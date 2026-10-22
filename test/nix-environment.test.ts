import { it } from "@effect/vitest";
import { Effect, Either, Fiber, Option, Schema } from "effect";
import * as fc from "effect/FastCheck";
import { describe, expect } from "vitest";
import { OrganizationId, WorkspaceId } from "../src/domain/ids.js";
import {
  type NixCacheEntry,
  NixPackageName,
  type RepositoryRecord,
} from "../src/domain/models.js";
import {
  makeNixEnvironment,
  type NixEnvironmentDependencies,
  nixCacheKey,
  protectedNixCacheKeys,
} from "../src/services/nix-environment.js";
import {
  SqliteClient,
  SqliteClientLive,
} from "../src/services/store/sqlite-client.js";

const closureSizeOutput = JSON.stringify({
  "/nix/store/abc-tool": { closureSize: 123 },
});

const packageName = (value: string) =>
  Schema.decodeUnknownSync(NixPackageName)(value);
const workspaceId = (value: string) =>
  Schema.decodeUnknownSync(WorkspaceId)(value);
const organizationId = (value: string) =>
  Schema.decodeUnknownSync(OrganizationId)(value);

const repository = (packages: ReadonlyArray<string>): RepositoryRecord => ({
  id: workspaceId("workspace"),
  organizationId: organizationId("organization"),
  url: "https://example.test/repository.git",
  ref: "main",
  teamIds: [],
  projectIds: [],
  labels: [],
  nixPackages: packages.map(packageName),
  isDefault: false,
  createdAt: 0,
  updatedAt: 0,
});

const fixture = () => {
  const entries = new Map<string, NixCacheEntry>();
  const commands: string[][] = [];
  const removals: string[] = [];
  let currentTime = 0;
  const dependencies: NixEnvironmentDependencies = {
    nixBinaryPath: "nix",
    nixpkgsFlakeRef:
      "github:NixOS/nixpkgs/0123456789abcdef0123456789abcdef01234567",
    nixRootsDir: "/roots",
    nixGcMaxBytes: 1234,
    repo: {
      get: (key) => Effect.succeed(Option.fromNullable(entries.get(key))),
      upsert: (entry) =>
        Effect.sync(() => void entries.set(entry.cacheKey, entry)),
      list: () => Effect.succeed([...entries.values()]),
      remove: (key) => Effect.sync(() => entries.delete(key)),
    },
    filesystem: {
      mkdir: async () => undefined,
      realpath: async (path) => path,
      remove: async (path) => void removals.push(path),
    },
    run: async (argv) => {
      commands.push([...argv]);
      return argv[1] === "path-info"
        ? { exitCode: 0, stdout: closureSizeOutput, stderr: "" }
        : {
            exitCode: 0,
            stdout: "/nix/store/abc-tool\nignored output",
            stderr: "",
          };
    },
    now: () => ++currentTime,
  };
  return { commands, dependencies, entries, removals };
};

describe("NixEnvironment", () => {
  it.prop(
    "derives deterministic canonical keys from package sets",
    {
      packages: fc.array(
        fc.constantFrom("git", "nodejs_22", "python3.pkgs.foo"),
      ),
    },
    ({ packages }) => {
      const ref =
        "github:NixOS/nixpkgs/0123456789abcdef0123456789abcdef01234567";
      const reversed = [...packages].reverse();
      const unique = [...new Set(packages)];
      expect(nixCacheKey(ref, packages)).toBe(nixCacheKey(ref, reversed));
      expect(nixCacheKey(ref, packages)).toBe(nixCacheKey(ref, unique));
    },
  );

  it("builds once for concurrent callers and reuses persisted metadata", async () => {
    const state = fixture();
    const service = await Effect.runPromise(
      makeNixEnvironment(state.dependencies),
    );
    const repo = repository(["git", "nodejs_22"]);
    const [first, second] = await Effect.runPromise(
      Effect.all([service.prepare(repo), service.prepare(repo)], {
        concurrency: "unbounded",
      }),
    );
    expect(
      state.commands.filter((command) => command[1] === "build"),
    ).toHaveLength(1);
    expect([first.reused, second.reused]).toContain(false);
    const reused = await Effect.runPromise(service.prepare(repo));
    expect(reused.reused).toBe(true);
    const cached = state.entries.get(first.cacheKey);
    expect(cached).toBeDefined();
    if (cached !== undefined) {
      expect(cached.updatedAt).toBeGreaterThan(cached.createdAt);
    }
    expect(
      state.commands.filter((command) => command[1] === "build"),
    ).toHaveLength(1);
  });

  it("keeps a shared build alive when its first caller is interrupted", async () => {
    const state = fixture();
    let buildCalls = 0;
    let notifyStarted: (() => void) | undefined;
    let releaseBuild: (() => void) | undefined;
    const started = new Promise<void>((resolve) => {
      notifyStarted = resolve;
    });
    const buildReleased = new Promise<void>((resolve) => {
      releaseBuild = resolve;
    });
    const interruptedDependencies: NixEnvironmentDependencies = {
      ...state.dependencies,
      run: async (argv) => {
        state.commands.push([...argv]);
        if (argv[1] === "path-info") {
          return { exitCode: 0, stdout: closureSizeOutput, stderr: "" };
        }
        if (argv[1] === "build") {
          buildCalls += 1;
          notifyStarted?.();
          await buildReleased;
        }
        return {
          exitCode: 0,
          stdout: "/nix/store/abc-tool",
          stderr: "",
        };
      },
    };
    const service = await Effect.runPromise(
      makeNixEnvironment(interruptedDependencies),
    );
    const selectedRepository = repository(["git"]);
    const leader = Effect.runFork(service.prepare(selectedRepository));
    await started;
    const follower = Effect.runPromise(service.prepare(selectedRepository));
    Effect.runFork(Fiber.interrupt(leader));
    releaseBuild?.();
    const prepared = await follower;
    expect(prepared.reused).toBe(false);
    expect(buildCalls).toBe(1);
  });

  it.effect("clears a failed build so a later caller can retry", () => {
    const state = fixture();
    let buildCalls = 0;
    const retryDependencies: NixEnvironmentDependencies = {
      ...state.dependencies,
      run: async (argv) => {
        state.commands.push([...argv]);
        if (argv[1] === "build") {
          buildCalls += 1;
          return buildCalls === 1
            ? { exitCode: 1, stdout: "", stderr: "first build failed" }
            : {
                exitCode: 0,
                stdout: "/nix/store/abc-tool",
                stderr: "",
              };
        }
        return {
          exitCode: 0,
          stdout:
            argv[1] === "path-info" ? closureSizeOutput : "/nix/store/abc-tool",
          stderr: "",
        };
      },
    };

    return Effect.gen(function* () {
      const service = yield* makeNixEnvironment(retryDependencies);
      const selectedRepository = repository(["git"]);
      const first = yield* Effect.either(service.prepare(selectedRepository));
      expect(Either.isLeft(first)).toBe(true);
      const retried = yield* service.prepare(selectedRepository);
      expect(retried.reused).toBe(false);
      expect(buildCalls).toBe(2);
    });
  });

  it("rejects unsafe package names in the typed error channel", async () => {
    const state = fixture();
    const service = await Effect.runPromise(
      makeNixEnvironment(state.dependencies),
    );
    const unsafe = { ...repository([]), nixPackages: ["../escape"] as never };
    const result = await Effect.runPromise(
      Effect.either(service.prepare(unsafe)),
    );
    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result))
      expect(result.left._tag).toBe("@Gateway/NixEnvironmentError");
    expect(state.commands).toHaveLength(0);
  });

  it("rejects unsafe cache keys without touching a root", async () => {
    const state = fixture();
    const service = await Effect.runPromise(
      makeNixEnvironment(state.dependencies),
    );
    const result = await Effect.runPromise(
      Effect.either(service.prune("../../escape")),
    );
    expect(Either.isLeft(result)).toBe(true);
    expect(state.removals).toEqual([]);
    expect(state.commands).toEqual([]);
  });

  it("prunes only its validated root then bounds garbage collection", async () => {
    const state = fixture();
    const service = await Effect.runPromise(
      makeNixEnvironment(state.dependencies),
    );
    const key = nixCacheKey(state.dependencies.nixpkgsFlakeRef, ["git"]);
    state.entries.set(key, {
      cacheKey: key,
      nixpkgsFlakeRef: state.dependencies.nixpkgsFlakeRef,
      packages: [packageName("git")],
      storePaths: ["/nix/store/abc-tool"],
      pathEntries: ["/nix/store/abc-tool/bin"],
      sizeBytes: 123,
      createdAt: 1,
      updatedAt: 1,
    });
    await Effect.runPromise(service.prune(key));
    expect(state.removals).toEqual([`/roots/${key}`]);
    expect(state.commands).toEqual([
      ["nix", "store", "gc", "--max-freed", "1234"],
    ]);
    expect(state.entries.has(key)).toBe(false);
  });

  it("returns a typed process failure", async () => {
    const state = fixture();
    const failingDependencies: NixEnvironmentDependencies = {
      ...state.dependencies,
      run: async () => ({
        exitCode: 1,
        stdout: "",
        stderr: "untrusted failure output",
      }),
    };
    const service = await Effect.runPromise(
      makeNixEnvironment(failingDependencies),
    );
    const result = await Effect.runPromise(
      Effect.either(service.prepare(repository(["git"]))),
    );
    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result))
      expect(result.left._tag).toBe("@Gateway/NixEnvironmentError");
  });
  it("evicts LRU out-links until the measured store meets its budget and preserves active keys", async () => {
    const state = fixture();
    const activeKey = nixCacheKey(state.dependencies.nixpkgsFlakeRef, ["git"]);
    let size = 300;
    const service = await Effect.runPromise(
      makeNixEnvironment({
        ...state.dependencies,
        protectedCacheKeys: () => Effect.succeed(new Set([activeKey])),
        run: async (argv) => {
          if (argv.includes("--all"))
            return {
              exitCode: 0,
              stdout: JSON.stringify({ "/nix/store/all": { narSize: size } }),
              stderr: "",
            };
          if (argv[1] === "store") {
            state.commands.push([...argv]);
            size -= 100;
            return { exitCode: 0, stdout: "", stderr: "" };
          }
          return state.dependencies.run(argv);
        },
      }),
    );
    const active = await Effect.runPromise(
      service.prepare(repository(["git"])),
    );
    const oldest = await Effect.runPromise(
      service.prepare(repository(["nodejs"])),
    );
    const newest = await Effect.runPromise(
      service.prepare(repository(["python3"])),
    );
    const result = await Effect.runPromise(service.enforceBudget(100));
    expect(result).toEqual({ removed: 2, sizeBytes: 100, withinBudget: true });
    expect(state.removals).toEqual([
      `/roots/${oldest.cacheKey}`,
      `/roots/${newest.cacheKey}`,
    ]);
    expect(state.entries.has(active.cacheKey)).toBe(true);
    expect(state.commands.filter((argv) => argv[1] === "store")).toEqual([
      ["nix", "store", "gc", "--max-freed", "1234"],
      ["nix", "store", "gc", "--max-freed", "1234"],
    ]);
  });

  it("does not evict below budget and reports budgets held by active roots", async () => {
    const state = fixture();
    let size = 50;
    const service = await Effect.runPromise(
      makeNixEnvironment({
        ...state.dependencies,
        protectedCacheKeys: () => Effect.succeed(new Set(state.entries.keys())),
        run: async (argv) =>
          argv.includes("--all")
            ? {
                exitCode: 0,
                stdout: JSON.stringify([{ narSize: size }]),
                stderr: "",
              }
            : state.dependencies.run(argv),
      }),
    );
    await Effect.runPromise(service.prepare(repository(["git"])));
    expect(await Effect.runPromise(service.enforceBudget(100))).toEqual({
      removed: 0,
      sizeBytes: 50,
      withinBudget: true,
    });
    size = 200;
    expect(await Effect.runPromise(service.enforceBudget(100))).toEqual({
      removed: 0,
      sizeBytes: 200,
      withinBudget: false,
    });
    expect(state.removals).toEqual([]);
  });

  it("fails closed on invalid store measurements", async () => {
    const state = fixture();
    const service = await Effect.runPromise(
      makeNixEnvironment({
        ...state.dependencies,
        run: async (argv) =>
          argv.includes("--all")
            ? {
                exitCode: 0,
                stdout: JSON.stringify({ path: { narSize: -1 } }),
                stderr: "",
              }
            : state.dependencies.run(argv),
      }),
    );
    await Effect.runPromise(service.prepare(repository(["git"])));
    const result = await Effect.runPromise(
      Effect.either(service.enforceBudget(100)),
    );
    expect(Either.isLeft(result)).toBe(true);
    expect(state.removals).toEqual([]);
  });
  it.scopedLive(
    "protects persisted active references, live terminal leases and unknown configuration history",
    () =>
      Effect.gen(function* () {
        const { db } = yield* SqliteClient;
        const state = fixture();
        const service = yield* makeNixEnvironment(state.dependencies);
        const git = yield* service.prepare(repository(["git"]));
        const node = yield* service.prepare(repository(["nodejs"]));
        const entries = [...state.entries.values()];
        db.query(`INSERT INTO repository (organization_id, id, url, ref, team_ids_json, project_ids_json, labels_json, nix_packages_json, created_at, updated_at)
      VALUES ('organization', 'workspace', 'https://example.test/repo', 'main', '[]', '[]', '[]', '["git"]', 1, 1)`).run();
        db.query(`INSERT INTO agent_run (session_id, organization_id, repository_id, state, created_at, updated_at)
      VALUES ('active', 'organization', 'workspace', 'running', 2, 2)`).run();
        expect(yield* protectedNixCacheKeys(db, entries)).toEqual(
          new Set([git.cacheKey]),
        );
        db.query("UPDATE agent_run SET state='succeeded'").run();
        expect(yield* protectedNixCacheKeys(db, entries)).toEqual(new Set());
        db.query("UPDATE agent_run SET lease_expires_at=?").run(
          Date.now() + 60000,
        );
        expect(yield* protectedNixCacheKeys(db, entries)).toEqual(
          new Set([git.cacheKey]),
        );
        db.query("UPDATE repository SET updated_at=3").run();
        expect(yield* protectedNixCacheKeys(db, entries)).toEqual(
          new Set([git.cacheKey, node.cacheKey]),
        );
        db.query("DELETE FROM repository").run();
        expect(yield* protectedNixCacheKeys(db, entries)).toEqual(
          new Set([git.cacheKey, node.cacheKey]),
        );
      }).pipe(Effect.provide(SqliteClientLive(":memory:"))),
  );

  it("refuses explicit pruning of active roots", async () => {
    const state = fixture();
    const key = nixCacheKey(state.dependencies.nixpkgsFlakeRef, ["git"]);
    const service = await Effect.runPromise(
      makeNixEnvironment({
        ...state.dependencies,
        protectedCacheKeys: () => Effect.succeed(new Set([key])),
      }),
    );
    const result = await Effect.runPromise(Effect.either(service.prune(key)));
    expect(Either.isLeft(result)).toBe(true);
    expect(state.removals).toEqual([]);
    expect(state.commands).toEqual([]);
  });
  it("retries failed GC even after the last out-link and metadata were removed", async () => {
    const state = fixture();
    let gcCalls = 0;
    let size = 300;
    const service = await Effect.runPromise(
      makeNixEnvironment({
        ...state.dependencies,
        run: async (argv) => {
          if (argv.includes("--all"))
            return {
              exitCode: 0,
              stdout: JSON.stringify([{ narSize: size }]),
              stderr: "",
            };
          if (argv[1] === "store") {
            gcCalls++;
            if (gcCalls === 1)
              return {
                exitCode: 1,
                stdout: "",
                stderr: "GC temporarily unavailable",
              };
            size = 50;
            return { exitCode: 0, stdout: "", stderr: "" };
          }
          return state.dependencies.run(argv);
        },
      }),
    );
    const prepared = await Effect.runPromise(
      service.prepare(repository(["git"])),
    );
    expect(
      Either.isLeft(
        await Effect.runPromise(Effect.either(service.enforceBudget(100))),
      ),
    ).toBe(true);
    expect(state.entries.size).toBe(0);
    expect(await Effect.runPromise(service.enforceBudget(100))).toEqual({
      removed: 0,
      sizeBytes: 50,
      withinBudget: true,
    });
    expect(state.removals).toEqual([`/roots/${prepared.cacheKey}`]);
    expect(gcCalls).toBe(2);
  });
});
