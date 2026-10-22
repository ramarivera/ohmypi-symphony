import { existsSync } from "node:fs";
import { symlink, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { it } from "@effect/vitest";
import { Config, ConfigProvider, Effect, Either, Layer, Schema } from "effect";
import { describe, expect } from "vitest";
import { SessionId, SourceKey } from "../src/domain/ids.js";
import {
  makeRetention,
  type RetentionPolicy,
  retentionConfig,
} from "../src/services/retention.js";
import { RunEventRepo } from "../src/services/store/run-event-repo.js";
import {
  SqliteClient,
  SqliteClientLive,
} from "../src/services/store/sqlite-client.js";
import { testPaths } from "./fixtures/paths.js";

const testPath = testPaths();
const policy: RetentionPolicy = {
  intervalMs: 100,
  workspaceTtlMs: 100,
  deliveryTtlMs: 100,
  eventTtlMs: 100,
  projectionTtlMs: 100,
  authGraceMs: 100,
  nixStoreMaxBytes: 1000,
};
const now = 1000;
const insertRun = (
  db: SqliteClient["Type"]["db"],
  id: string,
  state: string,
  path: string | null = null,
  updated = 1,
) => {
  db.query(
    "INSERT INTO agent_run (session_id, organization_id, state, workspace_path, created_at, updated_at) VALUES (?, 'org', ?, ?, 1, ?)",
  ).run(id, state, path, updated);
};

const withDatabase = <A, E>(effect: Effect.Effect<A, E, SqliteClient>) =>
  effect.pipe(Effect.provide(SqliteClientLive(":memory:")));

describe("retention", () => {
  it.scopedLive(
    "deletes expired terminal workspaces and data while preserving active runs and recent data",
    () =>
      Effect.gen(function* () {
        const { db } = yield* SqliteClient;
        const root = testPath("workspaces");
        const expired = testPath("workspaces/expired");
        const active = testPath("workspaces/active");
        const recent = testPath("workspaces/recent");
        const orphaned = testPath("workspaces/orphaned");
        const leased = testPath("workspaces/leased");
        for (const [id, state, path, updated] of [
          ["expired", "succeeded", expired, 1],
          ["active", "running", active, 1],
          ["recent", "failed", recent, 950],
          ["orphaned", "orphaned", orphaned, 1],
          ["leased", "failed", leased, 1],
        ] as const)
          insertRun(db, id, state, path, updated);
        db.query(
          "UPDATE agent_run SET lease_expires_at=2000 WHERE session_id='leased'",
        ).run();
        for (const id of [
          "expired",
          "active",
          "recent",
          "orphaned",
          "leased",
        ]) {
          db.query(
            "INSERT INTO run_event VALUES (?, ?, 'rpc', 'info', NULL, '{}', NULL, NULL, 1, 1)",
          ).run(id, id);
          db.query(
            "INSERT INTO activity_projection VALUES (?, ?, 'thought', 'hash', NULL, 'completed', 1, 1)",
          ).run(id, id);
          db.query(
            "INSERT INTO projection_outbox (source_key, session_id, activity_type, payload_json, status, created_at, updated_at) VALUES (?, ?, 'thought', '{}', 'completed', 1, 1)",
          ).run(id, id);
          db.query(
            "INSERT INTO run_input VALUES (?, ?, 'prompted', 'body', '{}', 1, 2)",
          ).run(id, id);
        }
        db.query(
          "INSERT INTO webhook_delivery VALUES ('old', 'org', 1, 'hash', '{}', 'processed', NULL), ('pending', 'org', 1, 'hash', '{}', 'pending', NULL), ('new', 'org', 950, 'hash', '{}', 'processed', NULL)",
        ).run();
        db.query(
          "INSERT INTO oauth_state VALUES ('old', 1, 1, NULL), ('new', 1, 950, NULL)",
        ).run();
        db.query(
          "INSERT INTO mcp_oauth_state VALUES ('old', 'org', 'server', 'url', NULL, 'verifier', 'redirect', '{}', 1, NULL)",
        ).run();
        db.query(
          "INSERT INTO admin_session VALUES ('old', 'org', 'csrf', 1, 1), ('valid', 'org', 'csrf', 2000, 1)",
        ).run();
        const service = makeRetention({
          db,
          workspaceRoot: root,
          policy,
          webhookReplayWindowMs: 100,
        });
        const result = yield* service.sweep(now);
        expect(result).toEqual({
          workspaces: 1,
          skippedWorkspaces: 0,
          events: 1,
          projections: 2,
          inputs: 1,
          deliveries: 1,
          auth: 3,
        });
        expect(existsSync(expired)).toBe(false);
        for (const path of [active, recent, orphaned, leased])
          expect(existsSync(path)).toBe(true);
        for (const table of [
          "run_event",
          "activity_projection",
          "projection_outbox",
          "run_input",
        ]) {
          expect(db.query(`SELECT COUNT(*) AS n FROM ${table}`).get()).toEqual({
            n: 4,
          });
        }
        expect(
          db
            .query(
              "SELECT workspace_path, omp_session_file FROM agent_run WHERE session_id='expired'",
            )
            .get(),
        ).toEqual({ workspace_path: null, omp_session_file: null });
        expect(
          db
            .query(
              "SELECT payload_json FROM webhook_delivery WHERE delivery_id='old'",
            )
            .get(),
        ).toEqual({ payload_json: "null" });
        expect(
          db
            .query(
              "SELECT payload_json FROM webhook_delivery WHERE delivery_id='pending'",
            )
            .get(),
        ).toEqual({ payload_json: "{}" });
        expect((yield* service.sweep(now)).workspaces).toBe(0);
      }).pipe(withDatabase),
  );

  it.scopedLive("refuses root, external, shared and symlinked workspaces", () =>
    Effect.gen(function* () {
      const { db } = yield* SqliteClient;
      const root = testPath("root");
      const external = testPath("external");
      const shared = testPath("root/shared");
      const linked = join(root, "linked");
      yield* Effect.promise(() => symlink(external, linked));
      yield* Effect.promise(() =>
        writeFile(join(external, "sentinel"), "keep"),
      );
      insertRun(db, "root", "failed", root);
      insertRun(db, "external", "failed", external);
      insertRun(db, "shared", "failed", shared);
      insertRun(db, "shared-active", "running", shared);
      insertRun(db, "linked", "failed", linked);
      const result = yield* makeRetention({
        db,
        workspaceRoot: root,
        policy,
        webhookReplayWindowMs: 100,
      }).sweep(now);
      expect(result.workspaces).toBe(0);
      expect(result.skippedWorkspaces).toBe(4);
      expect(existsSync(join(external, "sentinel"))).toBe(true);
      expect(existsSync(shared)).toBe(true);
    }).pipe(withDatabase),
  );

  it.scopedLive(
    "retains projections inside their TTL and delivery payloads inside replay window",
    () =>
      Effect.gen(function* () {
        const { db } = yield* SqliteClient;
        insertRun(db, "terminal", "succeeded");
        db.query(
          "INSERT INTO run_event VALUES ('pending', 'terminal', 'rpc', 'info', NULL, '{}', 'pending', NULL, 1, 1)",
        ).run();
        db.query(
          "INSERT INTO activity_projection VALUES ('pending', 'terminal', 'thought', 'hash', NULL, 'pending', 1, 1)",
        ).run();
        db.query(
          "INSERT INTO webhook_delivery VALUES ('old', 'org', 1, 'hash', '{}', 'processed', NULL)",
        ).run();
        const result = yield* makeRetention({
          db,
          workspaceRoot: testPath("root"),
          policy: { ...policy, projectionTtlMs: 2000 },
          webhookReplayWindowMs: 2000,
        }).sweep(now);
        expect(result.events).toBe(0);
        expect(result.projections).toBe(0);
        expect(result.deliveries).toBe(0);
      }).pipe(withDatabase),
  );

  it.scopedLive(
    "expires failed projections after their TTL but respects projector leases",
    () =>
      Effect.gen(function* () {
        const { db } = yield* SqliteClient;
        insertRun(db, "terminal", "failed");
        for (const key of ["failed", "leased"]) {
          db.query(
            "INSERT INTO run_event VALUES (?, 'terminal', 'rpc', 'info', NULL, '{}', 'failed', NULL, 1, 1)",
          ).run(key);
          db.query(
            "INSERT INTO activity_projection VALUES (?, 'terminal', 'thought', 'hash', NULL, 'failed', 1, 1)",
          ).run(key);
          db.query(
            "INSERT INTO projection_outbox (source_key, session_id, activity_type, payload_json, status, created_at, updated_at) VALUES (?, 'terminal', 'thought', '{}', 'failed', 1, 1)",
          ).run(key);
        }
        db.query(
          "UPDATE projection_outbox SET status='processing', lease_expires_at=2000 WHERE source_key='leased'",
        ).run();
        const result = yield* makeRetention({
          db,
          workspaceRoot: testPath("root"),
          policy,
          webhookReplayWindowMs: 100,
        }).sweep(now);
        expect(result.projections).toBe(2);
        expect(result.events).toBe(1);
        expect(
          db.query("SELECT source_key FROM projection_outbox").all(),
        ).toEqual([{ source_key: "leased" }]);
      }).pipe(withDatabase),
  );

  it("validates retention configuration and provides production defaults", async () => {
    const defaults = await Effect.runPromise(
      Config.unwrap(retentionConfig).pipe(
        Effect.withConfigProvider(ConfigProvider.fromMap(new Map())),
      ),
    );
    expect(defaults.workspaceTtlMs).toBe(86400000);
    expect(defaults.nixStoreMaxBytes).toBe(20 * 1024 ** 3);
    for (const value of ["0", "-1", "1e3", "1.5", "invalid"]) {
      const result = await Effect.runPromise(
        Effect.either(
          Config.unwrap(retentionConfig).pipe(
            Effect.withConfigProvider(
              ConfigProvider.fromMap(
                new Map([["RETENTION_EVENT_TTL_MS", value]]),
              ),
            ),
          ),
        ),
      );
      expect(Either.isLeft(result)).toBe(true);
    }
  });
});

describe("run event pagination", () => {
  it.scopedLive(
    "bounds pages and uses source keys to disambiguate equal timestamps",
    () =>
      Effect.gen(function* () {
        const repo = yield* RunEventRepo;
        const { db } = yield* SqliteClient;
        insertRun(db, "session", "running");
        const sessionId = Schema.decodeUnknownSync(SessionId)("session");
        // 105 events prove the default page is bounded even without route changes.
        for (let index = 0; index < 105; index++)
          yield* repo.upsert({
            sessionId,
            sourceKey: Schema.decodeUnknownSync(SourceKey)(
              `event-${String(index).padStart(3, "0")}`,
            ),
            kind: "rpc",
            level: "info",
            now: 1,
          });
        const first = yield* repo.list(sessionId);
        expect(first).toHaveLength(100);
        const last = first.at(-1);
        if (last === undefined) throw new Error("Expected a first page");
        const second = yield* repo.list(sessionId, {
          limit: 3,
          cursor: { createdAt: last.createdAt, sourceKey: last.sourceKey },
        });
        expect(second.map((event) => event.sourceKey)).toEqual([
          "event-100",
          "event-101",
          "event-102",
        ]);
        expect(
          yield* repo.list(sessionId, {
            cursor: { createdAt: 2, sourceKey: "" },
          }),
        ).toEqual([]);
        expect(yield* repo.list(sessionId, { limit: 0 })).toHaveLength(1);
      }).pipe(
        Effect.provide(
          RunEventRepo.Default.pipe(
            Layer.provideMerge(SqliteClientLive(":memory:")),
          ),
        ),
      ),
  );
});
