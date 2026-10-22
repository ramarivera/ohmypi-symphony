import { createHash } from "node:crypto";
import { mkdir, realpath, rm } from "node:fs/promises";
import { join, resolve } from "node:path";
import { Deferred, Effect, Option } from "effect";
import {
  type DatabaseError,
  NixEnvironmentError,
  type RowDecodeError,
} from "../domain/errors.js";
import type {
  NixCacheEntry,
  NixPackageName,
  RepositoryRecord,
} from "../domain/models.js";
import { isWithin } from "../util/paths.js";
import { GatewayConfig } from "./config.js";
import { detachedProcessGroup, signalProcessTree } from "./process.js";
import { NixCacheRepo } from "./store/repositories.js";
import { SqliteClient, tryDb } from "./store/sqlite-client.js";

const CACHE_KEY = /^[a-f0-9]{64}$/;
const NIX_PACKAGE = /^[A-Za-z0-9_+-]+(?:\.[A-Za-z0-9_+-]+)*$/;
const STORE_PATH = /^\/nix\/store\/[A-Za-z0-9][A-Za-z0-9+._?-]*$/;

type NixRepo = {
  readonly get: (
    cacheKey: string,
  ) => Effect.Effect<
    Option.Option<NixCacheEntry>,
    DatabaseError | RowDecodeError
  >;
  readonly upsert: (
    entry: NixCacheEntry,
  ) => Effect.Effect<void, DatabaseError | RowDecodeError>;
  readonly list: () => Effect.Effect<
    ReadonlyArray<NixCacheEntry>,
    DatabaseError | RowDecodeError
  >;
  readonly remove: (cacheKey: string) => Effect.Effect<boolean, DatabaseError>;
};

export interface NixProcessResult {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
}

export interface NixEnvironmentFileSystem {
  readonly mkdir: (path: string) => Promise<void>;
  readonly realpath: (path: string) => Promise<string>;
  readonly remove: (path: string) => Promise<void>;
}

export interface NixEnvironmentDependencies {
  readonly nixBinaryPath: string;
  readonly nixpkgsFlakeRef: string;
  readonly nixRootsDir: string;
  readonly nixGcMaxBytes: number;
  readonly repo: NixRepo;
  readonly protectedCacheKeys?: () => Effect.Effect<
    ReadonlySet<string>,
    DatabaseError | RowDecodeError
  >;
  readonly timeoutMs?: number;
  readonly run: (
    argv: ReadonlyArray<string>,
    signal?: AbortSignal,
  ) => Promise<NixProcessResult>;
  readonly filesystem?: NixEnvironmentFileSystem;
  readonly now?: () => number;
}

export interface PreparedNixEnvironment {
  readonly cacheKey: string;
  readonly storePaths: ReadonlyArray<string>;
  readonly pathEntries: ReadonlyArray<string>;
  readonly reused: boolean;
}

const systemFileSystem: NixEnvironmentFileSystem = {
  mkdir: async (path) => {
    await mkdir(path, { recursive: true });
  },
  realpath,
  remove: (path) => rm(path, { force: true, recursive: true }),
};

const failure = (
  reason: NixEnvironmentError["reason"],
  message: string,
  cause?: unknown,
): NixEnvironmentError =>
  new NixEnvironmentError({
    reason,
    message,
    ...(cause === undefined
      ? {}
      : { cause: cause instanceof Error ? cause.message : String(cause) }),
  });

export const nixCacheKey = (
  nixpkgsFlakeRef: string,
  packages: ReadonlyArray<string>,
): string =>
  createHash("sha256")
    .update(
      JSON.stringify({
        version: 1,
        nixpkgsFlakeRef,
        packages: [...new Set(packages)].sort(),
      }),
    )
    .digest("hex");

const normalizePackages = (
  packages: ReadonlyArray<NixPackageName>,
): ReadonlyArray<string> | NixEnvironmentError => {
  const normalized = [...new Set(packages.map(String))].sort();
  return normalized.every((item) => NIX_PACKAGE.test(item))
    ? normalized
    : failure(
        "invalid_package",
        "Nix package name is not a safe Nix attribute path",
      );
};

const parseStorePaths = (output: string): ReadonlyArray<string> =>
  [
    ...new Set(
      output
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => STORE_PATH.test(line)),
    ),
  ].sort();

const parseClosureSize = (output: string): number | NixEnvironmentError => {
  try {
    const parsed: unknown = JSON.parse(output);
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      Array.isArray(parsed)
    ) {
      return failure("process_failed", "Nix path metadata is not an object");
    }
    const values = Object.values(parsed);
    if (values.length === 0) {
      return failure("process_failed", "Nix path metadata is empty");
    }
    let total = 0;
    for (const value of values) {
      if (typeof value !== "object" || value === null) {
        return failure("process_failed", "Nix path metadata entry is invalid");
      }
      const closureSize = Reflect.get(value, "closureSize");
      if (
        typeof closureSize !== "number" ||
        !Number.isSafeInteger(closureSize) ||
        closureSize < 0
      ) {
        return failure("process_failed", "Nix closure size is invalid");
      }
      total += closureSize;
      if (!Number.isSafeInteger(total)) {
        return failure(
          "process_failed",
          "Nix closure size exceeds safe limits",
        );
      }
    }
    return total;
  } catch (cause) {
    return failure(
      "process_failed",
      "Nix path metadata could not be decoded",
      cause,
    );
  }
};

const nativeRunner = async (
  argv: ReadonlyArray<string>,
  signal?: AbortSignal,
): Promise<NixProcessResult> => {
  const process = Bun.spawn([...argv], {
    stdout: "pipe",
    stderr: "pipe",
    detached: detachedProcessGroup,
  });
  const abort = () => signalProcessTree(process, "SIGKILL");
  signal?.addEventListener("abort", abort, { once: true });
  const [exitCode, stdout, stderr] = await Promise.all([
    process.exited,
    new Response(process.stdout).text(),
    new Response(process.stderr).text(),
  ]).finally(() => signal?.removeEventListener("abort", abort));
  return { exitCode, stdout, stderr };
};

export const makeNixEnvironment = (input: NixEnvironmentDependencies) =>
  Effect.gen(function* () {
    const filesystem = input.filesystem ?? systemFileSystem;
    const now = input.now ?? Date.now;
    const inflight = new Map<
      string,
      Deferred.Deferred<
        PreparedNixEnvironment,
        NixEnvironmentError | DatabaseError | RowDecodeError
      >
    >();
    const inflightMutex = yield* Effect.makeSemaphore(1);
    // Builds share the lock; pruning and GC acquire every permit exclusively.
    const cachePermits = Number.MAX_SAFE_INTEGER;
    const cacheMutex = yield* Effect.makeSemaphore(cachePermits);

    const rootFor = Effect.fn("NixEnvironment.rootFor")(function* (
      cacheKey: string,
    ) {
      if (!CACHE_KEY.test(cacheKey))
        return yield* Effect.fail(
          failure("root_escape", "Nix cache key is invalid"),
        );
      const configuredRoot = resolve(input.nixRootsDir);
      const canonicalRoot = yield* Effect.tryPromise({
        try: async () => {
          await filesystem.mkdir(configuredRoot);
          return filesystem.realpath(configuredRoot);
        },
        catch: (cause) =>
          failure("metadata_failed", "Nix cache root is unavailable", cause),
      });
      const candidate = join(canonicalRoot, cacheKey);
      if (!isWithin(canonicalRoot, candidate))
        return yield* Effect.fail(
          failure(
            "root_escape",
            "Nix cache root resolves outside its configured directory",
          ),
        );
      return candidate;
    });

    const prepareFresh = Effect.fn("NixEnvironment.prepareFresh")(function* (
      cacheKey: string,
      packages: ReadonlyArray<string>,
    ) {
      const root = yield* rootFor(cacheKey);
      yield* Effect.tryPromise({
        try: () => filesystem.mkdir(root),
        catch: (cause) =>
          failure(
            "metadata_failed",
            "Nix cache root could not be created",
            cause,
          ),
      });
      const existing = yield* input.repo.get(cacheKey);
      if (Option.isSome(existing)) {
        const reusedEntry = { ...existing.value, updatedAt: now() };
        yield* input.repo.upsert(reusedEntry);
        yield* Effect.logInfo("Nix environment ready").pipe(
          Effect.annotateLogs({
            event: "nix.ready",
            cacheKey,
            reused: true,
            count: reusedEntry.packages.length,
          }),
        );
        return {
          cacheKey: reusedEntry.cacheKey,
          storePaths: reusedEntry.storePaths,
          pathEntries: reusedEntry.pathEntries,
          reused: true,
        };
      }
      const startedAt = now();
      yield* Effect.logInfo("Nix environment build started").pipe(
        Effect.annotateLogs({
          event: "nix.started",
          cacheKey,
          reused: false,
          count: packages.length,
        }),
      );
      const result = yield* Effect.tryPromise({
        try: (signal) =>
          input.run(
            [
              input.nixBinaryPath,
              "build",
              "--no-write-lock-file",
              "--print-out-paths",
              "--out-link",
              join(root, "result"),
              ...packages.map((pkg) => `${input.nixpkgsFlakeRef}#${pkg}`),
            ],
            signal,
          ),
        catch: (cause) =>
          failure("process_failed", "Nix build could not be started", cause),
      }).pipe(
        Effect.timeoutFail({
          duration: input.timeoutMs ?? 600_000,
          onTimeout: () =>
            failure(
              "process_failed",
              `Nix command timed out after ${input.timeoutMs ?? 600_000}ms`,
            ),
        }),
      );
      if (result.exitCode !== 0) {
        yield* Effect.logWarning("Nix environment build failed").pipe(
          Effect.annotateLogs({
            event: "nix.failed",
            cacheKey,
            reused: false,
            count: packages.length,
            durationMs: now() - startedAt,
          }),
        );
        return yield* Effect.fail(
          failure(
            "process_failed",
            `Nix build failed: ${result.stderr.trim().slice(-4096)}`,
          ),
        );
      }
      const storePaths = parseStorePaths(result.stdout);
      if (storePaths.length === 0)
        return yield* Effect.fail(
          failure(
            "process_failed",
            "Nix build did not produce safe store paths",
          ),
        );
      const sizeResult = yield* Effect.tryPromise({
        try: (signal) =>
          input.run(
            [
              input.nixBinaryPath,
              "path-info",
              "--json",
              "--closure-size",
              ...storePaths,
            ],
            signal,
          ),
        catch: (cause) =>
          failure(
            "process_failed",
            "Nix closure size query could not be started",
            cause,
          ),
      }).pipe(
        Effect.timeoutFail({
          duration: input.timeoutMs ?? 600_000,
          onTimeout: () =>
            failure(
              "process_failed",
              `Nix command timed out after ${input.timeoutMs ?? 600_000}ms`,
            ),
        }),
      );
      if (sizeResult.exitCode !== 0) {
        return yield* Effect.fail(
          failure("process_failed", "Nix closure size query failed"),
        );
      }
      const sizeBytes = parseClosureSize(sizeResult.stdout);
      if (sizeBytes instanceof NixEnvironmentError) {
        return yield* Effect.fail(sizeBytes);
      }
      const timestamp = now();
      const entry: NixCacheEntry = {
        cacheKey,
        nixpkgsFlakeRef: input.nixpkgsFlakeRef,
        packages: packages as ReadonlyArray<NixPackageName>,
        storePaths,
        pathEntries: storePaths.map((path) => `${path}/bin`),
        sizeBytes,
        createdAt: timestamp,
        updatedAt: timestamp,
      };
      yield* input.repo.upsert(entry);
      yield* Effect.logInfo("Nix environment ready").pipe(
        Effect.annotateLogs({
          event: "nix.ready",
          cacheKey,
          reused: false,
          count: packages.length,
          durationMs: now() - startedAt,
        }),
      );
      return {
        cacheKey,
        storePaths,
        pathEntries: entry.pathEntries,
        reused: false,
      };
    });

    const prepare = Effect.fn("NixEnvironment.prepare")(function* (
      repository: RepositoryRecord,
    ) {
      const packages = normalizePackages(repository.nixPackages);
      if (packages instanceof NixEnvironmentError) {
        return yield* Effect.fail(packages);
      }
      if (packages.length === 0) {
        return {
          cacheKey: nixCacheKey(input.nixpkgsFlakeRef, []),
          storePaths: [],
          pathEntries: [],
          reused: true,
        };
      }

      const cacheKey = nixCacheKey(input.nixpkgsFlakeRef, packages);
      const claim = yield* inflightMutex.withPermits(1)(
        Effect.gen(function* () {
          const running = inflight.get(cacheKey);
          if (running !== undefined) {
            return { leader: false as const, deferred: running };
          }
          const deferred = yield* Deferred.make<
            PreparedNixEnvironment,
            NixEnvironmentError | DatabaseError | RowDecodeError
          >();
          inflight.set(cacheKey, deferred);
          return { leader: true as const, deferred };
        }),
      );
      if (claim.leader) {
        yield* Effect.forkDaemon(
          Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const completed = yield* Effect.exit(
                restore(
                  cacheMutex.withPermits(1)(prepareFresh(cacheKey, packages)),
                ),
              );
              yield* Deferred.done(claim.deferred, completed);
              yield* inflightMutex.withPermits(1)(
                Effect.sync(() => {
                  inflight.delete(cacheKey);
                }),
              );
            }),
          ),
        );
      }
      return yield* Deferred.await(claim.deferred);
    });

    const list = Effect.fn("NixEnvironment.list")(function* () {
      return yield* input.repo.list();
    });

    const collectGarbage = Effect.gen(function* () {
      const gc = yield* Effect.tryPromise({
        try: (signal) =>
          input.run(
            [
              input.nixBinaryPath,
              "store",
              "gc",
              "--max-freed",
              String(input.nixGcMaxBytes),
            ],
            signal,
          ),
        catch: (cause) =>
          failure(
            "gc_failed",
            "Nix garbage collection could not be started",
            cause,
          ),
      }).pipe(
        Effect.timeoutFail({
          duration: input.timeoutMs ?? 600_000,
          onTimeout: () =>
            failure(
              "process_failed",
              `Nix command timed out after ${input.timeoutMs ?? 600_000}ms`,
            ),
        }),
      );
      if (gc.exitCode !== 0)
        return yield* Effect.fail(
          failure("gc_failed", "Nix garbage collection failed"),
        );
    });

    const pruneUnlocked = Effect.fn("NixEnvironment.prune")(function* (
      cacheKey: string,
    ) {
      const protectedKeys =
        input.protectedCacheKeys === undefined
          ? new Set<string>()
          : yield* input.protectedCacheKeys();
      if (protectedKeys.has(cacheKey) || inflight.has(cacheKey)) {
        return yield* Effect.fail(
          failure(
            "gc_failed",
            "Cannot prune a Nix environment referenced by an active run or build",
          ),
        );
      }
      const root = yield* rootFor(cacheKey);
      yield* Effect.tryPromise({
        try: () => filesystem.remove(root),
        catch: (cause) =>
          failure(
            "metadata_failed",
            "Nix cache root could not be removed",
            cause,
          ),
      });
      const removed = yield* input.repo.remove(cacheKey);
      yield* collectGarbage;
      return removed;
    });

    const storeSize = Effect.gen(function* () {
      const result = yield* Effect.tryPromise({
        try: (signal) =>
          input.run(
            [input.nixBinaryPath, "path-info", "--all", "--json"],
            signal,
          ),
        catch: (cause) =>
          failure(
            "gc_failed",
            "Nix store size query could not be started",
            cause,
          ),
      }).pipe(
        Effect.timeoutFail({
          duration: input.timeoutMs ?? 600_000,
          onTimeout: () =>
            failure(
              "process_failed",
              `Nix command timed out after ${input.timeoutMs ?? 600_000}ms`,
            ),
        }),
      );
      if (result.exitCode !== 0)
        return yield* Effect.fail(
          failure("gc_failed", "Nix store size query failed"),
        );
      return yield* Effect.try({
        try: () => {
          const parsed: unknown = JSON.parse(result.stdout);
          if (typeof parsed !== "object" || parsed === null)
            throw new Error("Invalid Nix store metadata");
          let total = 0;
          // narSize counts each registered path once, unlike overlapping closure sizes.
          for (const value of Object.values(parsed)) {
            if (typeof value !== "object" || value === null)
              throw new Error("Invalid Nix store path metadata");
            const size: unknown = Reflect.get(value, "narSize");
            if (
              typeof size !== "number" ||
              !Number.isSafeInteger(size) ||
              size < 0
            )
              throw new Error("Invalid Nix store path size");
            total += size;
            if (!Number.isSafeInteger(total))
              throw new Error("Nix store size exceeds safe limits");
          }
          return total;
        },
        catch: (cause) =>
          failure("gc_failed", "Nix store size metadata is invalid", cause),
      });
    });

    const enforceBudget = (maxBytes: number) =>
      cacheMutex.withPermits(cachePermits)(
        Effect.gen(function* () {
          if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0)
            return yield* Effect.fail(
              failure(
                "gc_failed",
                "Nix store budget must be a positive integer",
              ),
            );
          const entries = [...(yield* input.repo.list())].sort(
            (a, b) =>
              a.updatedAt - b.updatedAt || a.cacheKey.localeCompare(b.cacheKey),
          );
          let sizeBytes = yield* storeSize;
          let removed = 0;
          const collectUntilStable = Effect.gen(function* () {
            while (sizeBytes > maxBytes) {
              const previous = sizeBytes;
              yield* collectGarbage;
              sizeBytes = yield* storeSize;
              if (sizeBytes >= previous) break;
            }
          });
          // Drain already-unrooted paths first, even if all entries are protected.
          yield* collectUntilStable;
          for (const entry of entries) {
            if (sizeBytes <= maxBytes) break;
            const protectedKeys =
              input.protectedCacheKeys === undefined
                ? new Set<string>()
                : yield* input.protectedCacheKeys();
            if (
              protectedKeys.has(entry.cacheKey) ||
              inflight.has(entry.cacheKey)
            )
              continue;
            // prune removes only the validated managed out-link, then runs bounded GC.
            yield* pruneUnlocked(entry.cacheKey);
            removed++;
            sizeBytes = yield* storeSize;
            // Each GC pass is bounded. Drain this eviction before dropping another root.
            yield* collectUntilStable;
          }
          const result = {
            removed,
            sizeBytes,
            withinBudget: sizeBytes <= maxBytes,
          };
          yield* (
            result.withinBudget
              ? Effect.logInfo("Nix store budget checked")
              : Effect.logWarning(
                  "Nix store remains above budget; referenced roots were preserved",
                )
          ).pipe(
            Effect.annotateLogs({ event: "nix.budget", maxBytes, ...result }),
          );
          return result;
        }),
      );

    const prune = (cacheKey: string) =>
      cacheMutex.withPermits(cachePermits)(pruneUnlocked(cacheKey));
    return { prepare, list, prune, enforceBudget };
  });

export const protectedNixCacheKeys = (
  db: import("bun:sqlite").Database,
  entries: ReadonlyArray<NixCacheEntry>,
) =>
  Effect.gen(function* () {
    const active = yield* tryDb(
      () =>
        db
          .query<
            {
              nix_packages_json: string | null;
              repository_updated_at: number | null;
              run_created_at: number;
            },
            []
          >(`SELECT p.nix_packages_json, p.updated_at AS repository_updated_at, r.created_at AS run_created_at
          FROM agent_run r LEFT JOIN repository p ON p.id=r.repository_id AND p.organization_id=r.organization_id
          WHERE r.state NOT IN ('succeeded','failed','canceled') OR r.lease_expires_at>strftime('%s','now')*1000`)
          .all(),
      "NixEnvironment.activeReferences",
    );
    const protectedKeys = new Set<string>();
    for (const run of active) {
      // Older workers may use a previous repository configuration. If its
      // history is unknown, retain all roots instead of guessing at safety.
      if (
        run.nix_packages_json === null ||
        run.repository_updated_at === null ||
        run.repository_updated_at > run.run_created_at
      ) {
        for (const entry of entries) protectedKeys.add(entry.cacheKey);
        continue;
      }
      let packages: unknown;
      try {
        packages = JSON.parse(run.nix_packages_json);
      } catch {
        packages = null;
      }
      if (
        !Array.isArray(packages) ||
        !packages.every((item): item is string => typeof item === "string")
      ) {
        for (const entry of entries) protectedKeys.add(entry.cacheKey);
        continue;
      }
      for (const entry of entries) {
        if (entry.cacheKey === nixCacheKey(entry.nixpkgsFlakeRef, packages))
          protectedKeys.add(entry.cacheKey);
      }
    }
    return protectedKeys;
  });

export class NixEnvironment extends Effect.Service<NixEnvironment>()(
  "NixEnvironment",
  {
    accessors: true,
    dependencies: [GatewayConfig.Default, NixCacheRepo.Default],
    effect: Effect.gen(function* () {
      const config = yield* GatewayConfig;
      const repo = yield* NixCacheRepo;
      const { db } = yield* SqliteClient;
      const protectedCacheKeys = () =>
        Effect.flatMap(repo.list(), (entries) =>
          protectedNixCacheKeys(db, entries),
        );
      return yield* makeNixEnvironment({
        nixBinaryPath: config.nixBinaryPath,
        nixpkgsFlakeRef: config.nixpkgsFlakeRef,
        nixRootsDir: config.nixRootsDir,
        nixGcMaxBytes: config.nixGcMaxBytes,
        timeoutMs: config.nixTimeoutMs ?? 600_000,
        repo,
        protectedCacheKeys,
        run: nativeRunner,
      });
    }),
  },
) {}
