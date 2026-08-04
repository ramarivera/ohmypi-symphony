import { createHash } from "node:crypto";
import { mkdir, realpath, rm } from "node:fs/promises";
import { join, relative, resolve, sep } from "node:path";
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
import { GatewayConfig } from "./config.js";
import { NixCacheRepo } from "./store/repositories.js";

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
  readonly run: (argv: ReadonlyArray<string>) => Promise<NixProcessResult>;
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

const isWithin = (root: string, candidate: string): boolean => {
  const rel = relative(root, candidate);
  return (
    rel === "" ||
    (!rel.startsWith(`..${sep}`) && rel !== ".." && !rel.startsWith(sep))
  );
};

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
): Promise<NixProcessResult> => {
  const process = Bun.spawn([...argv], { stdout: "pipe", stderr: "pipe" });
  const [exitCode, stdout, stderr] = await Promise.all([
    process.exited,
    new Response(process.stdout).text(),
    new Response(process.stderr).text(),
  ]);
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
        try: () =>
          input.run([
            input.nixBinaryPath,
            "build",
            "--no-write-lock-file",
            "--print-out-paths",
            "--out-link",
            join(root, "result"),
            ...packages.map((pkg) => `${input.nixpkgsFlakeRef}#${pkg}`),
          ]),
        catch: (cause) =>
          failure("process_failed", "Nix build could not be started", cause),
      });
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
          failure("process_failed", "Nix build failed"),
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
        try: () =>
          input.run([
            input.nixBinaryPath,
            "path-info",
            "--json",
            "--json-format",
            "1",
            "--closure-size",
            ...storePaths,
          ]),
        catch: (cause) =>
          failure(
            "process_failed",
            "Nix closure size query could not be started",
            cause,
          ),
      });
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
                restore(prepareFresh(cacheKey, packages)),
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

    const prune = Effect.fn("NixEnvironment.prune")(function* (
      cacheKey: string,
    ) {
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
      const gc = yield* Effect.tryPromise({
        try: () =>
          input.run([
            input.nixBinaryPath,
            "store",
            "gc",
            "--max-freed",
            String(input.nixGcMaxBytes),
          ]),
        catch: (cause) =>
          failure(
            "gc_failed",
            "Nix garbage collection could not be started",
            cause,
          ),
      });
      if (gc.exitCode !== 0)
        return yield* Effect.fail(
          failure("gc_failed", "Nix garbage collection failed"),
        );
      return removed;
    });

    return { prepare, list, prune };
  });

export class NixEnvironment extends Effect.Service<NixEnvironment>()(
  "NixEnvironment",
  {
    accessors: true,
    dependencies: [GatewayConfig.Default, NixCacheRepo.Default],
    effect: Effect.gen(function* () {
      const config = yield* GatewayConfig;
      const repo = yield* NixCacheRepo;
      return yield* makeNixEnvironment({
        nixBinaryPath: config.nixBinaryPath,
        nixpkgsFlakeRef: config.nixpkgsFlakeRef,
        nixRootsDir: config.nixRootsDir,
        nixGcMaxBytes: config.nixGcMaxBytes,
        repo,
        run: nativeRunner,
      });
    }),
  },
) {}
