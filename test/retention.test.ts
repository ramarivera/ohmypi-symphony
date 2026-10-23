import { existsSync, rmSync } from "node:fs";
import { link, readFile, symlink, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { it } from "@effect/vitest";
import {
  Config,
  ConfigProvider,
  Deferred,
  Effect,
  Either,
  Fiber,
  Layer,
  Schema,
} from "effect";
import { describe, expect } from "vitest";
import { NixEnvironmentError } from "../src/domain/errors.js";
import { InputId, SessionId, SourceKey } from "../src/domain/ids.js";
import { GatewayConfig } from "../src/services/config.js";
import { LinearGateway } from "../src/services/linear-gateway.js";
import { NixEnvironment } from "../src/services/nix-environment.js";
import { Reconciler } from "../src/services/reconciler.js";
import {
  MAX_WORKSPACES_PER_SWEEP,
  makeRetention,
  Retention,
  type RetentionPolicy,
  retentionConfig,
} from "../src/services/retention.js";
import { SessionAuthority } from "../src/services/session-authority.js";
import { RunEventRepo } from "../src/services/store/run-event-repo.js";
import { RunInputRepo } from "../src/services/store/run-input-repo.js";
import { RunRepo } from "../src/services/store/run-repo.js";
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
          "INSERT INTO webhook_delivery VALUES ('old', 'org', 1, 'hash', '{}', 'processed', NULL), ('pending', 'org', 1, 'hash', '{}', 'pending', NULL), ('failed', 'org', 1, 'hash', '{}', 'failed', 'error'), ('new', 'org', 950, 'hash', '{}', 'processed', NULL)",
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
          inputs: 0,
          deliveries: 2,
          auth: 3,
        });
        expect(existsSync(expired)).toBe(false);
        for (const path of [active, recent, orphaned, leased])
          expect(existsSync(path)).toBe(true);
        for (const table of [
          "run_event",
          "activity_projection",
          "projection_outbox",
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
        expect(
          db
            .query(
              "SELECT payload_json, payload_hash, status FROM webhook_delivery WHERE delivery_id='failed'",
            )
            .get(),
        ).toEqual({
          payload_json: "null",
          payload_hash: "hash",
          status: "failed",
        });
        expect(db.query("SELECT COUNT(*) AS n FROM run_input").get()).toEqual({
          n: 5,
        });
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
    "removes nested workspace links without deleting or rewriting their external targets",
    () =>
      Effect.gen(function* () {
        const { db } = yield* SqliteClient;
        const root = testPath("root");
        const expired = testPath("root/expired");
        const external = testPath("external");
        const sentinel = join(external, "sentinel");
        yield* Effect.promise(async () => {
          await writeFile(sentinel, "keep");
          await symlink(external, join(expired, "directory-link"));
          await symlink(sentinel, join(expired, "file-link"));
          await link(sentinel, join(expired, "hardlink"));
        });
        insertRun(db, "expired", "succeeded", expired);
        const result = yield* makeRetention({
          db,
          workspaceRoot: root,
          policy,
          webhookReplayWindowMs: 100,
        }).sweep(now);
        expect(result.workspaces).toBe(1);
        expect(result.skippedWorkspaces).toBe(0);
        expect(existsSync(expired)).toBe(false);
        expect(yield* Effect.promise(() => readFile(sentinel, "utf8"))).toBe(
          "keep",
        );
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

  it.scopedLive(
    "bounds successful removals without starving candidates behind skipped rows",
    () =>
      Effect.gen(function* () {
        const { db } = yield* SqliteClient;
        const root = testPath("root");
        for (let index = 0; index < MAX_WORKSPACES_PER_SWEEP + 1; index++)
          insertRun(
            db,
            `skipped-${index}`,
            "failed",
            testPath(`outside-${index}`),
          );
        const paths = Array.from(
          { length: MAX_WORKSPACES_PER_SWEEP + 2 },
          (_, index) => testPath(`root/expired-${index}`),
        );
        for (const [index, path] of paths.entries())
          insertRun(db, `expired-${index}`, "failed", path);
        const service = makeRetention({
          db,
          workspaceRoot: root,
          policy,
          webhookReplayWindowMs: 100,
        });
        const first = yield* service.sweep(now);
        expect(first.workspaces).toBe(MAX_WORKSPACES_PER_SWEEP);
        expect(first.skippedWorkspaces).toBe(MAX_WORKSPACES_PER_SWEEP + 1);
        expect(paths.filter(existsSync)).toHaveLength(2);
        expect((yield* service.sweep(now)).workspaces).toBe(2);
        expect(paths.some(existsSync)).toBe(false);
      }).pipe(withDatabase),
  );

  it.scopedLive(
    "keeps earlier deletion records and continues after a failed workspace removal",
    () =>
      Effect.gen(function* () {
        const { db } = yield* SqliteClient;
        const root = testPath("root");
        const paths = ["first", "blocked", "last"].map((id) =>
          testPath(`root/${id}`),
        );
        for (const [index, path] of paths.entries()) {
          insertRun(db, `run-${index}`, "failed", path);
          db.query(
            "UPDATE agent_run SET omp_session_file='old-session' WHERE session_id=?",
          ).run(`run-${index}`);
          db.query(
            "INSERT INTO workspace VALUES (?, ?, 'repo', 'url', 'main', 'ready', 1, 1)",
          ).run(`run-${index}`, path);
        }
        db.query(
          "INSERT INTO oauth_state VALUES ('expired', 1, 1, NULL)",
        ).run();
        const result = yield* makeRetention({
          db,
          workspaceRoot: root,
          policy,
          webhookReplayWindowMs: 100,
          removeWorkspace: (path) => {
            if (path.endsWith("/blocked"))
              throw Object.assign(new Error("permission denied"), {
                code: "EACCES",
              });
            rmSync(path, { recursive: true, force: true });
          },
        }).sweep(now);
        expect(result.workspaces).toBe(2);
        expect(result.skippedWorkspaces).toBe(1);
        expect(result.auth).toBe(1);
        expect(paths.map(existsSync)).toEqual([false, true, false]);
        expect(
          db
            .query(
              "SELECT session_id, workspace_path, omp_session_file FROM agent_run ORDER BY session_id",
            )
            .all(),
        ).toEqual([
          { session_id: "run-0", workspace_path: null, omp_session_file: null },
          {
            session_id: "run-1",
            workspace_path: paths[1],
            omp_session_file: "old-session",
          },
          { session_id: "run-2", workspace_path: null, omp_session_file: null },
        ]);
        expect(db.query("SELECT session_id FROM workspace").all()).toEqual([
          { session_id: "run-1" },
        ]);
      }).pipe(withDatabase),
  );

  it.scopedLive(
    "commits workspace cleanup even if subsequent data cleanup fails",
    () =>
      Effect.gen(function* () {
        const { db } = yield* SqliteClient;
        const path = testPath("root/expired");
        insertRun(db, "expired", "failed", path);
        db.query(
          "CREATE TRIGGER fail_auth BEFORE DELETE ON oauth_state BEGIN SELECT RAISE(ABORT, 'broken auth cleanup'); END",
        ).run();
        db.query(
          "INSERT INTO oauth_state VALUES ('expired', 1, 1, NULL)",
        ).run();
        expect(
          Either.isLeft(
            yield* Effect.either(
              makeRetention({
                db,
                workspaceRoot: testPath("root"),
                policy,
                webhookReplayWindowMs: 100,
              }).sweep(now),
            ),
          ),
        ).toBe(true);
        expect(existsSync(path)).toBe(false);
        expect(db.query("SELECT workspace_path FROM agent_run").get()).toEqual({
          workspace_path: null,
        });
      }).pipe(withDatabase),
  );

  it.scopedLive(
    "canonicalizes configured root ancestors but rejects candidate aliases",
    () =>
      Effect.gen(function* () {
        const { db } = yield* SqliteClient;
        const actual = testPath("actual");
        const root = testPath("actual/workspaces");
        const alias = join(testPath("parent"), "alias");
        yield* Effect.promise(() => symlink(actual, alias));
        const expired = testPath("actual/workspaces/expired");
        const keep = testPath("actual/workspaces/keep");
        const linked = join(root, "linked");
        yield* Effect.promise(() => symlink(keep, linked));
        insertRun(db, "expired", "failed", expired);
        insertRun(db, "linked", "failed", linked);
        const result = yield* makeRetention({
          db,
          workspaceRoot: join(alias, "workspaces"),
          policy,
          webhookReplayWindowMs: 100,
        }).sweep(now);
        expect(result.workspaces).toBe(1);
        expect(result.skippedWorkspaces).toBe(1);
        expect(existsSync(expired)).toBe(false);
        expect(existsSync(keep)).toBe(true);
      }).pipe(withDatabase),
  );

  it.scopedLive(
    "preserves active shared paths reached through symlinks and workspace metadata",
    () =>
      Effect.gen(function* () {
        const { db } = yield* SqliteClient;
        const root = testPath("root");
        const expired = testPath("root/expired");
        const child = testPath("root/expired/child");
        const alias = join(testPath("aliases"), "active");
        yield* Effect.promise(() => symlink(expired, alias));
        insertRun(db, "expired", "failed", expired);
        insertRun(db, "active", "running", join(alias, "child"));
        const service = makeRetention({
          db,
          workspaceRoot: root,
          policy,
          webhookReplayWindowMs: 100,
        });
        expect((yield* service.sweep(now)).workspaces).toBe(0);
        expect(existsSync(child)).toBe(true);
        db.query(
          "UPDATE agent_run SET workspace_path=NULL WHERE session_id='active'",
        ).run();
        db.query(
          "INSERT INTO workspace VALUES ('active', ?, 'repo', 'url', 'main', 'ready', 1, 1)",
        ).run(join(alias, "not-yet-created"));
        expect((yield* service.sweep(now)).workspaces).toBe(0);
        expect(existsSync(expired)).toBe(true);
        db.query("DELETE FROM workspace WHERE session_id='active'").run();
        expect((yield* service.sweep(now)).workspaces).toBe(1);
      }).pipe(withDatabase),
  );

  it.scopedLive(
    "continues enforcing the Nix budget across sweep and budget failures",
    () =>
      Effect.gen(function* () {
        const { db } = yield* SqliteClient;
        db.query("DROP TABLE agent_run").run();
        let calls = 0;
        const retried = yield* Deferred.make<void>();
        const nix = NixEnvironment.make({
          prepare: () => Effect.die("Unexpected build"),
          list: () => Effect.succeed([]),
          prune: () => Effect.succeed(false),
          enforceBudget: (maxBytes) =>
            Effect.gen(function* () {
              expect(maxBytes).toBe(1000);
              calls++;
              if (calls === 1)
                return yield* Effect.fail(
                  new NixEnvironmentError({
                    reason: "gc_failed",
                    message: "Transient GC failure",
                  }),
                );
              yield* Deferred.succeed(retried, undefined);
              return { removed: 0, sizeBytes: 50, withinBudget: true };
            }),
        });
        const values = new Map([
          ["LINEAR_CLIENT_ID", "client"],
          ["LINEAR_CLIENT_SECRET", "secret"],
          ["LINEAR_WEBHOOK_SECRET", "secret"],
          ["TOKEN_ENCRYPTION_KEY", Buffer.alloc(32, 7).toString("base64")],
          ["PUBLIC_URL", "http://localhost:3000"],
          ["RETENTION_INTERVAL_MS", "1"],
          ["NIX_STORE_MAX_BYTES", "1000"],
        ]);
        yield* Effect.gen(function* () {
          const retention = yield* Retention;
          const fiber = yield* Effect.forkScoped(retention.run);
          yield* Deferred.await(retried);
          yield* Fiber.interrupt(fiber);
        }).pipe(
          Effect.provide(
            Retention.DefaultWithoutDependencies.pipe(
              Layer.provide(
                Layer.mergeAll(
                  GatewayConfig.Default,
                  Layer.succeed(NixEnvironment, nix),
                ),
              ),
            ),
          ),
          Effect.withConfigProvider(ConfigProvider.fromMap(values)),
        );
        expect(calls).toBe(2);
      }).pipe(Effect.scoped, withDatabase),
  );

  it.scopedLive(
    "deduplicates historical prompts and stops after retention and session resumption",
    () =>
      Effect.gen(function* () {
        const { db } = yield* SqliteClient;
        const inputs = yield* RunInputRepo;
        const sessionId = Schema.decodeUnknownSync(SessionId)("session");
        insertRun(db, sessionId, "running");
        for (const [activity, kind] of [
          ["old-prompt", "prompted"],
          ["old-stop", "stop"],
        ] as const) {
          const id = Schema.decodeUnknownSync(InputId)(
            `${sessionId}:${kind}:${activity}`,
          );
          yield* inputs.enqueue({
            id,
            sessionId,
            kind,
            body: "original context",
            payload: {},
            createdAt: 1,
          });
          yield* inputs.markProcessed(id, 2);
        }
        db.query(
          "UPDATE agent_run SET state='succeeded', desired_state='running', updated_at=1",
        ).run();
        expect(
          (yield* makeRetention({
            db,
            workspaceRoot: testPath("root"),
            policy,
            webhookReplayWindowMs: 100,
          }).sweep(now)).inputs,
        ).toBe(0);
        expect(db.query("SELECT COUNT(*) AS n FROM run_input").get()).toEqual({
          n: 2,
        });
        db.query("UPDATE agent_run SET state='running', updated_at=1").run();
        const gateway = {
          listSessionActivities: () =>
            Effect.succeed([
              {
                id: "old-prompt",
                type: "prompt" as const,
                body: "original context",
                title: null,
                signal: null,
                createdAt: new Date(1).toISOString(),
              },
              {
                id: "old-stop",
                type: "prompt" as const,
                body: "stop",
                title: null,
                signal: "stop",
                createdAt: new Date(1).toISOString(),
              },
            ]),
        } as unknown as LinearGateway;
        const authority = SessionAuthority.make({
          processSession: () => Effect.void,
          processRunnable: () => Effect.void,
          shutdown: () => Effect.void,
          activeWorkerCount: () => Effect.succeed(0),
        });
        yield* Effect.gen(function* () {
          yield* (yield* Reconciler).catchup();
        }).pipe(
          Effect.provide(
            Reconciler.DefaultWithoutDependencies.pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.succeed(SessionAuthority, authority),
                  Layer.succeed(LinearGateway, gateway),
                ),
              ),
            ),
          ),
        );
        expect(yield* inputs.pending(sessionId)).toEqual([]);
        expect(
          db.query("SELECT state, desired_state FROM agent_run").get(),
        ).toEqual({ state: "running", desired_state: "running" });
        expect(db.query("SELECT COUNT(*) AS n FROM run_input").get()).toEqual({
          n: 2,
        });
      }).pipe(
        Effect.provide(
          Layer.mergeAll(RunInputRepo.Default, RunRepo.Default).pipe(
            Layer.provideMerge(SqliteClientLive(":memory:")),
          ),
        ),
      ),
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
        // Legacy admin detail callers still receive the complete history.
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
        expect(yield* repo.list(sessionId)).toHaveLength(105);
        expect(yield* repo.list(sessionId, {})).toHaveLength(105);
        const first = yield* repo.list(sessionId, { limit: 100 });
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
