import { lstatSync, realpathSync } from "node:fs";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import {
  Config,
  ConfigProvider,
  Context,
  Effect,
  Layer,
  Schedule,
} from "effect";
import { isWithin } from "../util/paths.js";
import { GatewayConfig } from "./config.js";
import { NixEnvironment } from "./nix-environment.js";
import { SqliteClient, tryDb } from "./store/sqlite-client.js";
import { retireWorkerIdentitySync } from "./store/worker-identity-repo.js";
import {
  stopWorkerUidProcessesSync,
  workerUidRange,
} from "./worker-security.js";
import { removeWorkerWorkspaceSync } from "./workspace-process.js";

const positiveInteger = (name: string, fallback: number) =>
  Config.String(name).pipe(
    Config.withDefault(String(fallback)),
    Config.mapEffect((value) => {
      const parsed = /^\d+$/.test(value.trim()) ? Number(value) : Number.NaN;
      return Number.isSafeInteger(parsed) && parsed > 0
        ? Effect.succeed(parsed)
        : Effect.fail(
            new Config.ConfigError(
              new ConfigProvider.SourceError({
                message: `${name} must be a positive integer`,
              }),
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
export type RetentionPolicy = Config.Success<typeof retentionConfig>;

export const MAX_WORKSPACES_PER_SWEEP = 25;

const terminal = "state IN ('succeeded','failed','canceled')";

const canonicalReference = (path: string): string | undefined => {
  // Resolve existing ancestors as well: a not-yet-created child under an alias
  // still references the canonical parent's workspace.
  const suffix: string[] = [];
  let current = path;
  for (;;) {
    try {
      return join(realpathSync(current), ...suffix);
    } catch (error) {
      if (
        !(
          error instanceof Error &&
          "code" in error &&
          error.code === "ENOENT"
        ) ||
        dirname(current) === current
      )
        return undefined;
      suffix.unshift(basename(current));
      current = dirname(current);
    }
  }
};

export const makeRetention = (input: {
  readonly db: import("bun:sqlite").Database;
  readonly workspaceRoot: string;
  readonly policy: RetentionPolicy;
  readonly webhookReplayWindowMs: number;
  readonly removeWorkspace?: (path: string) => void;
}) => {
  const { db, policy } = input;
  const sweep = (now: number) =>
    tryDb(() => {
      // One synchronous SQLite transaction fences reopening and shared-path checks
      // against deletion. Never await between checking a run and removing its files.
      const workspaceResult = db
        .transaction(() => {
          let workspaces = 0;
          let skippedWorkspaces = 0;
          // Match Workspace.materialize, which persists canonical paths. A missing
          // root means there is no workspace to remove, but data may still expire.
          let root: string | undefined;
          try {
            root = realpathSync(resolve(input.workspaceRoot));
          } catch {
            root = undefined;
          }
          const references = db
            .query<
              {
                session_id: string;
                workspace_path: string | null;
                canonical_path: string | null;
              },
              []
            >(
              `SELECT r.session_id, r.workspace_path, w.canonical_path FROM agent_run r LEFT JOIN workspace w ON w.session_id=r.session_id`,
            )
            .all();
          const referencePaths = references.map((reference) => ({
            sessionId: reference.session_id,
            paths: [reference.workspace_path, reference.canonical_path].flatMap(
              (path) => {
                if (path === null) return [];
                const absolute = resolve(path);
                return [absolute, canonicalReference(absolute)];
              },
            ),
          }));
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
            if (workspaces >= MAX_WORKSPACES_PER_SWEEP) break;
            const shared = referencePaths.some(
              (reference) =>
                reference.sessionId !== row.session_id &&
                reference.paths.some(
                  (other) =>
                    other === undefined ||
                    isWithin(path, other) ||
                    isWithin(other, path),
                ),
            );
            if (
              root === undefined ||
              !isAbsolute(row.workspace_path) ||
              path === root ||
              !isWithin(root, path) ||
              shared
            ) {
              skippedWorkspaces++;
              continue;
            }
            const identity = db
              .query<{ uid: number }, [string]>(
                "SELECT uid FROM worker_identity WHERE session_id=?",
              )
              .get(row.session_id);
            const uid = identity?.uid ?? workerUidRange(process.env)?.base;
            // A released uid may be serving another session. Never reopen an
            // old workspace to that worker, even briefly during deletion.
            if (
              uid !== undefined &&
              db
                .query(
                  "SELECT 1 FROM worker_identity WHERE uid=? AND active=1 AND session_id<>?",
                )
                .get(uid, row.session_id) !== null
            ) {
              skippedWorkspaces++;
              continue;
            }
            try {
              const stat = lstatSync(path);
              if (
                !stat.isDirectory() ||
                stat.isSymbolicLink() ||
                realpathSync(path) !== path ||
                !isWithin(root, path) ||
                root === path
              ) {
                skippedWorkspaces++;
                continue;
              }
              if (uid !== undefined) stopWorkerUidProcessesSync(uid);
              if (input.removeWorkspace === undefined)
                removeWorkerWorkspaceSync(
                  path,
                  root,
                  uid === undefined ? undefined : String(uid),
                );
              else input.removeWorkspace(path);
            } catch (error) {
              if (
                !(
                  error instanceof Error &&
                  "code" in error &&
                  error.code === "ENOENT"
                )
              ) {
                skippedWorkspaces++;
                continue;
              }
            }
            if (
              identity !== null &&
              !retireWorkerIdentitySync(db, row.session_id, identity.uid, root)
            ) {
              skippedWorkspaces++;
              continue;
            }
            db.query("DELETE FROM workspace WHERE session_id=?").run(
              row.session_id,
            );
            db.query(
              "UPDATE agent_run SET workspace_path=NULL, omp_session_file=NULL WHERE session_id=?",
            ).run(row.session_id);
            workspaces++;
          }
          // A crash can leave an allocation after terminal state was committed,
          // including a failed clone with no published workspace path.
          const retired = db
            .query<{ session_id: string; uid: number }, [number]>(
              `SELECT i.session_id, i.uid FROM worker_identity i JOIN agent_run r ON r.session_id=i.session_id
             WHERE i.active=1 AND r.state IN ('succeeded','failed','canceled')
             AND (r.lease_expires_at IS NULL OR r.lease_expires_at<=?)`,
            )
            .all(now);
          for (const identity of retired) {
            // Without the configured root, fail closed until a later sweep.
            if (root === undefined) continue;
            retireWorkerIdentitySync(
              db,
              identity.session_id,
              identity.uid,
              root,
            );
          }
          return { workspaces, skippedWorkspaces };
        })
        .immediate();
      // Commit workspace records before unrelated data cleanup, which can fail
      // without bringing already-deleted workspace paths back into the database.
      return db
        .transaction(() => {
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
          // Catch-up replays the entire Linear history. Keep processed input
          // identities and their original resume context until a durable catch-up
          // watermark exists; deleting them would execute historical prompts again.
          const inputs = 0;
          // Preserve delivery identities for deduplication. Drop their large raw payloads.
          const deliveries = db
            .query(
              "UPDATE webhook_delivery SET payload_json='null' WHERE status IN ('processed','failed') AND received_at<? AND payload_json<>'null'",
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
            ...workspaceResult,
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

export class Retention extends Context.Service<Retention>()("Retention", {
  make: Effect.gen(function* () {
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
    const sweepTick = Effect.gen(function* () {
      const result = yield* sweep(Date.now());
      yield* Effect.logInfo("Retention sweep finished").pipe(
        Effect.annotateLogs({ event: "retention.swept", ...result }),
      );
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logError("Retention sweep failed", cause),
      ),
    );
    const budgetTick = nix
      .enforceBudget(policy.nixStoreMaxBytes)
      .pipe(
        Effect.catchCause((cause) =>
          Effect.logError("Nix store budget enforcement failed", cause),
        ),
      );
    return {
      sweep,
      run: Effect.repeat(
        Effect.andThen(sweepTick, budgetTick),
        Schedule.spaced(policy.intervalMs),
      ),
    };
  }),
}) {
  static readonly layerWithoutDependencies = Layer.effect(this, this.make);
  static readonly layer = this.layerWithoutDependencies.pipe(
    Layer.provide([GatewayConfig.layer, NixEnvironment.layer]),
  );
}
