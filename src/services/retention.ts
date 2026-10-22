import { lstatSync, realpathSync, rmSync } from "node:fs";
import { isAbsolute, resolve } from "node:path";
import { Config, ConfigError, Effect, Either, Schedule } from "effect";
import { isWithin } from "../util/paths.js";
import { GatewayConfig } from "./config.js";
import { NixEnvironment } from "./nix-environment.js";
import { SqliteClient, tryDb } from "./store/sqlite-client.js";

const positiveInteger = (name: string, fallback: number) =>
  Config.string(name).pipe(
    Config.withDefault(String(fallback)),
    Config.mapOrFail((value) => {
      const parsed = /^\d+$/.test(value.trim()) ? Number(value) : Number.NaN;
      return Number.isSafeInteger(parsed) && parsed > 0
        ? Either.right(parsed)
        : Either.left(
            ConfigError.InvalidData(
              [name],
              `${name} must be a positive integer`,
            ),
          );
    }),
  );

const day = 24 * 60 * 60 * 1000;
export const retentionConfig = Config.all({
  intervalMs: positiveInteger("RETENTION_INTERVAL_MS", 60 * 60 * 1000),
  workspaceTtlMs: positiveInteger("RETENTION_WORKSPACE_TTL_MS", day),
  deliveryTtlMs: positiveInteger("RETENTION_DELIVERY_TTL_MS", 7 * day),
  eventTtlMs: positiveInteger("RETENTION_EVENT_TTL_MS", 7 * day),
  projectionTtlMs: positiveInteger("RETENTION_PROJECTION_TTL_MS", 30 * day),
  authGraceMs: positiveInteger("RETENTION_AUTH_GRACE_MS", day),
  nixStoreMaxBytes: positiveInteger("NIX_STORE_MAX_BYTES", 20 * 1024 ** 3),
});
export type RetentionPolicy = Config.Config.Success<typeof retentionConfig>;

const terminal = "state IN ('succeeded','failed','canceled')";

export const makeRetention = (input: {
  readonly db: import("bun:sqlite").Database;
  readonly workspaceRoot: string;
  readonly policy: RetentionPolicy;
  readonly webhookReplayWindowMs: number;
}) => {
  const { db, policy } = input;
  const sweep = (now: number) =>
    tryDb(() => {
      // One synchronous SQLite transaction fences reopening and shared-path checks
      // against deletion. Never await between checking a run and removing its files.
      return db
        .transaction(() => {
          let workspaces = 0;
          let skippedWorkspaces = 0;
          const root = resolve(input.workspaceRoot);
          const candidates = db.query<
            { session_id: string; workspace_path: string },
            [number, number]
          >(
            `SELECT session_id, workspace_path FROM agent_run WHERE ${terminal}
         AND updated_at<? AND workspace_path IS NOT NULL
         AND (lease_expires_at IS NULL OR lease_expires_at<=?)`,
          );
          // Bind both cutoffs explicitly; the workspace grace is based on terminal update time.
          const rows = candidates.all(now - policy.workspaceTtlMs, now);
          for (const row of rows) {
            const path = resolve(row.workspace_path);
            const references = db
              .query<
                {
                  workspace_path: string | null;
                  canonical_path: string | null;
                },
                [string]
              >(
                `SELECT r.workspace_path, w.canonical_path FROM agent_run r LEFT JOIN workspace w ON w.session_id=r.session_id WHERE r.session_id<>?`,
              )
              .all(row.session_id);
            const shared = references.some((reference) =>
              [reference.workspace_path, reference.canonical_path].some(
                (other) =>
                  other !== null &&
                  (isWithin(path, resolve(other)) ||
                    isWithin(resolve(other), path)),
              ),
            );
            if (
              !isAbsolute(row.workspace_path) ||
              path === root ||
              !isWithin(root, path) ||
              shared
            ) {
              skippedWorkspaces++;
              continue;
            }
            try {
              const stat = lstatSync(path);
              const canonicalRoot = realpathSync(root);
              if (
                !stat.isDirectory() ||
                stat.isSymbolicLink() ||
                realpathSync(path) !== path ||
                !isWithin(canonicalRoot, path) ||
                canonicalRoot === path
              ) {
                skippedWorkspaces++;
                continue;
              }
              rmSync(path, { recursive: true, force: true });
            } catch (error) {
              if (
                !(
                  error instanceof Error &&
                  "code" in error &&
                  error.code === "ENOENT"
                )
              )
                throw error;
            }
            db.query("DELETE FROM workspace WHERE session_id=?").run(
              row.session_id,
            );
            db.query(
              "UPDATE agent_run SET workspace_path=NULL, omp_session_file=NULL WHERE session_id=?",
            ).run(row.session_id);
            workspaces++;
          }
          const terminalSessions = `SELECT session_id FROM agent_run WHERE ${terminal} AND updated_at<? AND (lease_expires_at IS NULL OR lease_expires_at<=?)`;
          // Retryable projections have a longer TTL than raw events. Once that
          // TTL expires for a terminal run, expire them too, unless a projector
          // still owns a live lease.
          const outbox = db
            .query(
              `DELETE FROM projection_outbox WHERE session_id IN (${terminalSessions}) AND updated_at<?
             AND (lease_expires_at IS NULL OR lease_expires_at<=?)`,
            )
            .run(
              now - policy.projectionTtlMs,
              now,
              now - policy.projectionTtlMs,
              now,
            ).changes;
          const projections =
            outbox +
            db
              .query(
                `DELETE FROM activity_projection WHERE session_id IN (${terminalSessions}) AND updated_at<?
             AND NOT EXISTS (SELECT 1 FROM projection_outbox o WHERE o.source_key=activity_projection.source_key)`,
              )
              .run(
                now - policy.projectionTtlMs,
                now,
                now - policy.projectionTtlMs,
              ).changes;
          const events = db
            .query(
              `DELETE FROM run_event WHERE session_id IN (${terminalSessions}) AND updated_at<?
         AND NOT EXISTS (SELECT 1 FROM activity_projection p WHERE p.source_key=run_event.source_key AND p.status<>'completed')
         AND NOT EXISTS (SELECT 1 FROM projection_outbox o WHERE o.source_key=run_event.source_key AND o.status<>'completed')`,
            )
            .run(now - policy.eventTtlMs, now, now - policy.eventTtlMs).changes;
          const inputs = db
            .query(
              `DELETE FROM run_input WHERE session_id IN (${terminalSessions}) AND processed_at IS NOT NULL AND processed_at<?`,
            )
            .run(now - policy.eventTtlMs, now, now - policy.eventTtlMs).changes;
          // Preserve delivery identities for deduplication. Drop their large raw payloads.
          const deliveries = db
            .query(
              "UPDATE webhook_delivery SET payload_json='null' WHERE status='processed' AND received_at<? AND payload_json<>'null'",
            )
            .run(
              now - Math.max(policy.deliveryTtlMs, input.webhookReplayWindowMs),
            ).changes;
          let auth = 0;
          for (const table of [
            "oauth_state",
            "mcp_oauth_state",
            "admin_session",
          ] as const) {
            auth += db
              .query(`DELETE FROM ${table} WHERE expires_at<?`)
              .run(now - policy.authGraceMs).changes;
          }
          return {
            workspaces,
            skippedWorkspaces,
            events,
            projections,
            inputs,
            deliveries,
            auth,
          };
        })
        .immediate();
    }, "Retention.sweep");
  return { sweep };
};

export class Retention extends Effect.Service<Retention>()("Retention", {
  dependencies: [GatewayConfig.Default, NixEnvironment.Default],
  effect: Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const policy = yield* Config.unwrap(retentionConfig);
    const { db } = yield* SqliteClient;
    const nix = yield* NixEnvironment;
    const { sweep } = makeRetention({
      db,
      workspaceRoot: config.workspaceRoot,
      policy,
      webhookReplayWindowMs: config.webhookReplayWindowMs,
    });
    const tick = Effect.gen(function* () {
      const result = yield* sweep(Date.now());
      yield* Effect.logInfo("Retention sweep finished").pipe(
        Effect.annotateLogs({ event: "retention.swept", ...result }),
      );
      yield* nix.enforceBudget(policy.nixStoreMaxBytes);
    }).pipe(
      Effect.catchAllCause((cause) =>
        Effect.logError("Retention sweep failed", cause),
      ),
    );
    return {
      sweep,
      run: Effect.repeat(tick, Schedule.spaced(policy.intervalMs)),
    };
  }),
}) {}
