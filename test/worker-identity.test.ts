import { Database } from "bun:sqlite";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { Effect } from "effect";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  SqliteClient,
  SqliteClientLive,
} from "../src/services/store/sqlite-client.js";
import { makeWorkerIdentityRepo } from "../src/services/store/worker-identity-repo.js";
import * as workerSecurity from "../src/services/worker-security.js";
import {
  sessionWorkerEnvironment,
  workerUidRange,
} from "../src/services/worker-security.js";
import * as workspaceProcess from "../src/services/workspace-process.js";

const roots: string[] = [];
afterEach(async () => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
  await Promise.all(
    roots.splice(0).map((root) => rm(root, { recursive: true, force: true })),
  );
});
const withIdentities = async (
  test: (
    repo: ReturnType<typeof makeWorkerIdentityRepo>,
    db: SqliteClient["Service"]["db"],
    root: string,
  ) => Effect.Effect<void, unknown>,
) => {
  const parent =
    process.env.SECURITY_TEST_TMPDIR ??
    join(homedir(), "dev/agentic-scratchpads/ohmypi-symphony/security/tests");
  await mkdir(parent, { recursive: true });
  const root = await mkdtemp(join(parent, "allocation-"));
  roots.push(root);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const { db } = yield* SqliteClient;
        for (const id of ["a", "b", "c"])
          db.query(
            "INSERT INTO agent_run(session_id, organization_id, state, created_at, updated_at) VALUES (?, 'org', 'running', 1, 1)",
          ).run(id);
        yield* test(makeWorkerIdentityRepo(db), db, root);
      }).pipe(Effect.provide(SqliteClientLive(join(root, "gateway.sqlite")))),
    ),
  );
};
const range = (count = "2") => {
  vi.stubEnv("WORKER_UID_BASE", "21001");
  vi.stubEnv("WORKER_UID_COUNT", count);
};

describe("worker identities", () => {
  it("allocates distinct uids atomically and recovers the persisted uid from a new repo", async () => {
    range();
    await withIdentities((repo, db) =>
      Effect.gen(function* () {
        const identities = yield* Effect.all(
          [repo.acquire("a"), repo.acquire("b")],
          { concurrency: "unbounded" },
        );
        expect(identities).toEqual(["21001", "21002"]);
        expect(yield* makeWorkerIdentityRepo(db).acquire("a")).toBe("21001");
        expect(
          db
            .query(
              "SELECT COUNT(*) AS count FROM worker_identity WHERE active=1",
            )
            .get(),
        ).toEqual({ count: 2 });
      }),
    );
  });
  it("fails clearly on exhaustion without stealing an allocation", async () => {
    range("1");
    await withIdentities((repo) =>
      Effect.gen(function* () {
        yield* repo.acquire("a");
        const result = yield* Effect.result(repo.acquire("b"));
        expect(result._tag).toBe("Failure");
        if (result._tag !== "Failure") throw new Error("Expected exhaustion");
        expect(result.failure.message).toContain("Worker uid range exhausted");
        expect(yield* repo.acquire("a")).toBe("21001");
      }),
    );
  });
  it("releases on end, reuses slots, and requires the original uid when resuming", async () => {
    range("1");
    await withIdentities((repo, db, root) =>
      Effect.gen(function* () {
        expect(yield* repo.acquire("a")).toBe("21001");
        db.query(
          "UPDATE agent_run SET state='succeeded' WHERE session_id='a'",
        ).run();
        yield* repo.release("a", join(root, "workspaces"));
        expect(yield* repo.acquire("b")).toBe("21001");
        const busy = yield* Effect.result(repo.acquire("a"));
        expect(busy._tag).toBe("Failure");
        if (busy._tag !== "Failure")
          throw new Error("Expected busy resume uid");
        expect(busy.failure.message).toContain("for resume is busy");
        db.query(
          "UPDATE agent_run SET state='succeeded' WHERE session_id='b'",
        ).run();
        yield* repo.release("b", join(root, "workspaces"));
        expect(yield* repo.acquire("a")).toBe("21001");
      }),
    );
  });
  it("denies a retained session's resume on a second connection while its uid is reused", async () => {
    range("1");
    await withIdentities((repo, db, root) =>
      Effect.gen(function* () {
        yield* repo.acquire("a");
        db.query(
          "UPDATE agent_run SET state='succeeded' WHERE session_id='a'",
        ).run();
        yield* repo.release("a", join(root, "workspaces"));
        yield* repo.acquire("b");
        const other = new Database(join(root, "gateway.sqlite"));
        try {
          const result = yield* Effect.result(
            makeWorkerIdentityRepo(other).acquire(
              "a",
              join(root, "workspaces"),
            ),
          );
          expect(result._tag).toBe("Failure");
          expect(
            db
              .query("SELECT session_id FROM worker_identity WHERE active=1")
              .all(),
          ).toEqual([{ session_id: "b" }]);
        } finally {
          other.close();
        }
      }),
    );
  });
  it.each(["process", "shared", "seal"] as const)(
    "quarantines a failed %s retirement and reclaims it on allocation",
    async (failure) => {
      range("1");
      await withIdentities((repo, db, root) =>
        Effect.gen(function* () {
          yield* repo.acquire("a");
          db.query(
            "UPDATE agent_run SET state='succeeded' WHERE session_id='a'",
          ).run();
          const fail = () => {
            throw new Error("injected retirement failure");
          };
          const spy =
            failure === "process"
              ? vi
                  .spyOn(workerSecurity, "stopWorkerUidProcessesSync")
                  .mockImplementationOnce(fail)
              : failure === "shared"
                ? vi
                    .spyOn(workspaceProcess, "cleanupWorkerSharedFilesSync")
                    .mockImplementationOnce(fail)
                : vi
                    .spyOn(workerSecurity, "sealWorkerUidWorkspacesSync")
                    .mockImplementationOnce(fail);
          yield* repo.release("a", join(root, "workspaces"));
          expect(
            db
              .query("SELECT active FROM worker_identity WHERE session_id='a'")
              .get(),
          ).toEqual({ active: 1 });
          expect(
            db.query("SELECT kind FROM run_event WHERE session_id='a'").get(),
          ).toEqual({ kind: "worker_uid_quarantined" });
          spy.mockRestore();
          expect(yield* repo.acquire("b", join(root, "workspaces"))).toBe(
            "21001",
          );
          expect(
            db
              .query("SELECT active FROM worker_identity WHERE session_id='a'")
              .get(),
          ).toEqual({ active: 0 });
        }),
      );
    },
  );
  it("allocates identities for legacy sessions and migrates the single uid env", async () => {
    vi.stubEnv("WORKER_UID", "1001");
    vi.stubEnv("WORKER_UID_BASE", "");
    vi.stubEnv("WORKER_UID_COUNT", "");
    expect(workerUidRange(process.env)).toEqual({ base: 1001, count: 1 });
    await withIdentities((repo) =>
      Effect.gen(function* () {
        expect(yield* repo.acquire("a")).toBe("1001");
        const exhausted = yield* Effect.result(repo.acquire("b"));
        expect(exhausted._tag).toBe("Failure");
      }),
    );
  });
  it("rejects invalid ranges and a persisted uid outside a changed range", async () => {
    for (const env of [
      { WORKER_UID_BASE: "0" },
      { WORKER_UID_BASE: "-1" },
      { WORKER_UID_BASE: "1001", WORKER_UID_COUNT: "0" },
      { WORKER_UID_BASE: "4294967294", WORKER_UID_COUNT: "2" },
      { WORKER_UID_COUNT: "5" },
    ])
      expect(() => workerUidRange(env)).toThrow();
    range();
    await withIdentities((repo) =>
      Effect.gen(function* () {
        yield* repo.acquire("a");
        vi.stubEnv("WORKER_UID_BASE", "22000");
        const result = yield* Effect.result(repo.acquire("a"));
        expect(result._tag).toBe("Failure");
      }),
    );
  });
  it("keeps HOME and XDG caches in the session regardless of gateway settings", () => {
    expect(
      sessionWorkerEnvironment("/work/a", {
        HOME: "/home/shared",
        XDG_CACHE_HOME: "/shared/cache",
        TOKEN_ENCRYPTION_KEY: "secret",
      }),
    ).toMatchObject({
      HOME: "/work/a/.omp-home",
      TMPDIR: "/work/a/.omp-tmp",
      XDG_CACHE_HOME: "/work/a/.omp-home/.cache",
      XDG_CONFIG_HOME: "/work/a/.omp-home/.config",
      XDG_DATA_HOME: "/work/a/.omp-home/.local/share",
    });
    expect(
      sessionWorkerEnvironment("/work/a", { TOKEN_ENCRYPTION_KEY: "secret" }),
    ).not.toHaveProperty("TOKEN_ENCRYPTION_KEY");
  });
});
