import { createHash } from "node:crypto";
import { constants } from "node:fs";
import {
  lstat,
  mkdir,
  mkdtemp,
  open,
  opendir,
  readFile,
  realpath,
  rename,
  writeFile,
} from "node:fs/promises";
import { hostname } from "node:os";
import { join } from "node:path";
import { Clock, Effect, Option } from "effect";
import {
  type DatabaseError,
  type RowDecodeError,
  WorkspaceError,
} from "../domain/errors.js";
import type { OrganizationId } from "../domain/ids.js";
import type { RepositoryRecord } from "../domain/models.js";
import { isWithin } from "../util/paths.js";
import { GatewayConfig } from "./config.js";
import {
  buildGitHubExtraHeader,
  GitHubApp,
  type GitHubAppTokenService,
} from "./github-app.js";
import { detachedProcessGroup, signalProcessTree } from "./process.js";
import { WorkspaceRepo } from "./store/repositories.js";
import {
  prepareWorkerWorkspace,
  workerIdentityCommand,
} from "./worker-security.js";
import {
  cleanupWorkspaceProcess,
  gitEnvironment,
  needsWorkspaceIdentity,
  removeWorkerWorkspace,
  workspaceProcess,
} from "./workspace-process.js";

export { isWithin } from "../util/paths.js";

export type RepositoryResolution =
  | { readonly kind: "match"; readonly repository: RepositoryRecord }
  | { readonly kind: "none" }
  | {
      readonly kind: "ambiguous";
      readonly repositories: ReadonlyArray<RepositoryRecord>;
    };

interface WorkspaceMarker {
  readonly repositoryId: string;
  readonly url: string;
  readonly ref: string;
}

interface ResolveContext {
  readonly organizationId: string | null;
  readonly teamId: string | null;
  readonly projectId: string | null;
  readonly repositoryId: string | null;
  readonly issueLabels: ReadonlyArray<string>;
  readonly projectLabels: ReadonlyArray<string>;
}

export interface WorkspaceRepoShape {
  readonly listRepositories: (
    organizationId: OrganizationId,
  ) => Effect.Effect<
    ReadonlyArray<RepositoryRecord>,
    DatabaseError | RowDecodeError
  >;
}

export function safeSessionKey(sessionId: string): string {
  return createHash("sha256").update(sessionId).digest("hex").slice(0, 32);
}

function hasIntersection(
  haystack: ReadonlyArray<string>,
  needles: ReadonlyArray<string>,
): boolean {
  const set = new Set(haystack);
  for (const needle of needles) {
    if (set.has(needle)) return true;
  }
  return false;
}

function parseMarker(value: unknown): Option.Option<WorkspaceMarker> {
  if (typeof value !== "object" || value === null) return Option.none();
  const record = value as Record<string, unknown>;
  if (
    typeof record.repositoryId === "string" &&
    typeof record.url === "string" &&
    typeof record.ref === "string"
  ) {
    return Option.some({
      repositoryId: record.repositoryId,
      url: record.url,
      ref: record.ref,
    });
  }
  return Option.none();
}

function normalizeLabels(value: unknown): ReadonlyArray<string> {
  if (!Array.isArray(value)) return [];
  return value
    .filter((item): item is string => typeof item === "string")
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);
}

function optionalString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function parseContext(context: unknown): Option.Option<ResolveContext> {
  if (typeof context !== "object" || context === null) return Option.none();
  const record = context as Record<string, unknown>;
  return Option.some({
    organizationId: optionalString(record.organizationId),
    teamId: optionalString(record.teamId),
    projectId: optionalString(record.projectId),
    repositoryId: optionalString(record.repositoryId),
    issueLabels: normalizeLabels(record.issueLabels),
    projectLabels: normalizeLabels(record.projectLabels),
  });
}

export interface RepositorySuggestionCandidate {
  readonly hostname: string;
  readonly repositoryFullName: string;
}

export const parseRepositorySuggestionCandidate = (
  repositoryUrl: string,
): RepositorySuggestionCandidate | null => {
  let hostname: string;
  let pathname: string;
  try {
    const parsed = new URL(repositoryUrl);
    hostname = parsed.hostname;
    pathname = parsed.pathname;
  } catch {
    const scp = /^git@([^:]+):(.+)$/u.exec(repositoryUrl);
    const scpHost = scp?.[1];
    const scpPath = scp?.[2];
    if (scpHost === undefined || scpPath === undefined) return null;
    hostname = scpHost;
    pathname = `/${scpPath}`;
  }
  const repositoryFullName = pathname
    .replace(/^\/+/u, "")
    .replace(/\/+$/u, "")
    .replace(/\.git$/u, "");
  if (
    hostname.length === 0 ||
    !/^[^/]+(?:\/[^/]+)+$/u.test(repositoryFullName)
  ) {
    return null;
  }
  return { hostname, repositoryFullName };
};

export const isHttpsGitHubRepositoryUrl = (repositoryUrl: string): boolean => {
  if (repositoryUrl.startsWith("-") || repositoryUrl.includes("\\"))
    return false;
  // Match the raw string too: URL canonicalization must not hide traversal,
  // credentials, encoded path separators, explicit ports, queries or fragments.
  return (
    /^https:\/\/github\.com\/[A-Za-z0-9][A-Za-z0-9-]*\/[A-Za-z0-9_.-]+(?:\.git)?$/u.test(
      repositoryUrl,
    ) && ![".", "..", ".git"].includes(repositoryUrl.split("/").at(-1) ?? "")
  );
};

const mintGitHubExtraHeader = (
  githubApp: GitHubAppTokenService,
  repository: RepositoryRecord,
  sessionId: string,
) =>
  Effect.gen(function* () {
    const candidate = parseRepositorySuggestionCandidate(repository.url);
    if (
      candidate === null ||
      candidate.hostname.toLowerCase() !== "github.com" ||
      !isHttpsGitHubRepositoryUrl(repository.url)
    ) {
      return undefined;
    }
    const [owner, repositoryName] = candidate.repositoryFullName.split("/");
    if (owner === undefined || repositoryName === undefined) {
      return yield* Effect.fail(
        workspaceFailure(
          "Repository URL did not yield an owner and name",
          "git_failed",
          sessionId,
        )(new Error("invalid repository full name")),
      );
    }
    const token = yield* githubApp
      .getInstallationToken(owner, repositoryName, repository.organizationId)
      .pipe(
        // App not installed on this repo: clone anonymously instead of
        // failing (public repos, or repos outside the App's installations).
        Effect.catchIf(
          (error) =>
            error._tag === "@Gateway/GitHubAppError" &&
            error.reason === "not_installed",
          (_error) =>
            Effect.logInfo("workspace.github_credentials_not_installed").pipe(
              Effect.annotateLogs({
                event: "workspace.github_credentials_not_installed",
                sessionId,
                repositoryId: repository.id,
              }),
              Effect.as(undefined),
            ),
        ),
        Effect.mapError(
          workspaceFailure(
            "GitHub credentials could not be minted",
            "git_failed",
            sessionId,
          ),
        ),
      );
    if (token === undefined) return undefined;
    return buildGitHubExtraHeader(token);
  });
function onlyItem<A>(items: ReadonlyArray<A>): A | undefined {
  return items.length === 1 ? items[0] : undefined;
}

function resolveFromRepositories(
  context: ResolveContext,
  repositories: ReadonlyArray<RepositoryRecord>,
): RepositoryResolution {
  if (context.organizationId === null || repositories.length === 0) {
    return { kind: "none" };
  }

  const repositoryId = context.repositoryId;
  if (repositoryId !== null) {
    const match = repositories.find(
      (repository) =>
        repository.id.toLowerCase() === repositoryId.toLowerCase(),
    );
    return match ? { kind: "match", repository: match } : { kind: "none" };
  }

  if (context.issueLabels.length > 0) {
    const matches = repositories.filter((repository) =>
      hasIntersection(repository.labels, context.issueLabels),
    );
    const match = onlyItem(matches);
    if (match !== undefined) return { kind: "match", repository: match };
    if (matches.length > 1) return { kind: "ambiguous", repositories: matches };
  }

  if (context.projectLabels.length > 0) {
    const matches = repositories.filter((repository) =>
      hasIntersection(repository.labels, context.projectLabels),
    );
    const match = onlyItem(matches);
    if (match !== undefined) return { kind: "match", repository: match };
    if (matches.length > 1) return { kind: "ambiguous", repositories: matches };
  }

  const projectId =
    context.projectId !== null ? context.projectId.toLowerCase() : null;
  if (projectId !== null) {
    const matches = repositories.filter((repository) =>
      repository.projectIds.some((id) => id.toLowerCase() === projectId),
    );
    const match = onlyItem(matches);
    if (match !== undefined) return { kind: "match", repository: match };
    if (matches.length > 1) return { kind: "ambiguous", repositories: matches };
  }

  const teamId = context.teamId !== null ? context.teamId.toLowerCase() : null;
  if (teamId !== null) {
    const matches = repositories.filter((repository) =>
      repository.teamIds.some((id) => id.toLowerCase() === teamId),
    );
    const match = onlyItem(matches);
    if (match !== undefined) return { kind: "match", repository: match };
    if (matches.length > 1) return { kind: "ambiguous", repositories: matches };
  }

  const defaultRepository = repositories.find(
    (repository) => repository.isDefault,
  );
  if (defaultRepository)
    return { kind: "match", repository: defaultRepository };
  return { kind: "none" };
}

const enoent = (error: unknown): error is Error & { code: "ENOENT" } =>
  error instanceof Error && "code" in error && error.code === "ENOENT";

const workspaceFailure =
  (message: string, reason: WorkspaceError["reason"], sessionId: string) =>
  (cause: unknown) =>
    new WorkspaceError({
      message,
      sessionId,
      reason,
      cause: cause instanceof Error ? cause.message : String(cause),
    });

const GITHUB_EXTRA_HEADER_KEY = "http.https://github.com/.extraheader";

const redactGitOutput = (
  value: string,
  args: ReadonlyArray<string> = [],
): string => {
  let redacted = value;
  for (const arg of args) {
    if (
      arg.includes("AUTHORIZATION: basic") ||
      arg.includes(`${GITHUB_EXTRA_HEADER_KEY}=`)
    ) {
      redacted = redacted.replaceAll(arg, "<redacted>");
    }
  }
  return redacted
    .replace(
      /(http\.https:\/\/github\.com\/\.extraheader=)\S+/giu,
      "$1<redacted>",
    )
    .replace(/(AUTHORIZATION:\s*basic\s+)\S+/giu, "$1<redacted>");
};

const gitSubcommand = (args: ReadonlyArray<string>): string => {
  for (const command of ["clone", "config", "fetch", "checkout"]) {
    if (args.includes(command)) return command;
  }
  return "command";
};

const runGit = (
  args: ReadonlyArray<string>,
  cwd: string | undefined,
  sessionId: string,
  timeoutMs = 120_000,
  extraHeader?: string,
) =>
  runGitOutput(args, cwd, sessionId, timeoutMs, extraHeader).pipe(
    Effect.asVoid,
  );

export const persistGitHubExtraHeader = (
  target: string,
  sessionId: string,
  header: string,
): Effect.Effect<void, WorkspaceError> => {
  if (needsWorkspaceIdentity())
    return Effect.tryPromise({
      try: async () => {
        await prepareWorkerWorkspace(target, process.env.WORKER_UID);
        await workspaceProcess(target, "persist-git", header);
      },
      catch: workspaceFailure(
        "Could not persist GitHub credential",
        "git_failed",
        sessionId,
      ),
    });
  return removeGitHubExtraHeaders(target, sessionId).pipe(
    Effect.zipRight(
      Effect.tryPromise({
        try: async () => {
          if (!isGatewayGitHubExtraHeader(header))
            throw new Error("Invalid gateway credential");
          const gitDir = await open(
            join(target, ".git"),
            constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW,
          );
          try {
            const configPath =
              process.platform === "linux"
                ? `/proc/self/fd/${gitDir.fd}/config`
                : join(target, ".git", "config");
            const config = await open(
              configPath,
              constants.O_WRONLY | constants.O_APPEND | constants.O_NOFOLLOW,
            );
            try {
              const stat = await config.stat();
              if (!stat.isFile() || stat.nlink !== 1)
                throw new Error("Git config must be a regular file");
              await config.chmod(0o600);
              await config.writeFile(
                `\n[http "https://github.com/"]\n\textraheader = ${header}\n`,
              );
            } finally {
              await config.close();
            }
          } finally {
            await gitDir.close();
          }
        },
        catch: workspaceFailure(
          "Could not persist GitHub credential",
          "git_failed",
          sessionId,
        ),
      }),
    ),
  );
};

const isGatewayGitHubExtraHeader = (value: string): boolean => {
  const match = /^AUTHORIZATION: basic ([A-Za-z0-9+/]+={0,2})$/u.exec(value);
  if (match === null) return false;
  const encoded = match[1];
  if (encoded === undefined) return false;
  const decoded = Buffer.from(encoded, "base64").toString("utf8");
  return (
    decoded.startsWith("x-access-token:") &&
    Buffer.from(decoded, "utf8").toString("base64") === encoded
  );
};

export const runGitOutput = (
  args: ReadonlyArray<string>,
  cwd: string | undefined,
  sessionId: string,
  timeoutMs = 120_000,
  extraHeader?: string,
  allowMissingConfig = false,
) =>
  Effect.tryPromise({
    try: async (signal) => {
      const command = [
        "git",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "credential.helper=",
        ...args,
      ];
      const process = Bun.spawn(
        cwd === undefined
          ? command
          : workerIdentityCommand(command, globalThis.process.env.WORKER_UID),
        {
          env: {
            ...gitEnvironment(globalThis.process.env),
            ...(extraHeader === undefined
              ? {}
              : {
                  GIT_CONFIG_COUNT: "1",
                  GIT_CONFIG_KEY_0: GITHUB_EXTRA_HEADER_KEY,
                  GIT_CONFIG_VALUE_0: extraHeader,
                }),
          },
          detached: detachedProcessGroup,
          ...(cwd ? { cwd } : {}),
          stdout: "pipe",
          stderr: "pipe",
        },
      );
      const abort = () => signalProcessTree(process, "SIGKILL");
      signal.addEventListener("abort", abort, { once: true });
      const [exitCode, stdout, stderr] = await Promise.all([
        process.exited,
        new Response(process.stdout).text(),
        new Response(process.stderr).text(),
      ]).finally(() => signal.removeEventListener("abort", abort));
      if (exitCode !== 0 && !(allowMissingConfig && exitCode === 1)) {
        throw new Error(
          `git ${gitSubcommand(args)} failed: ${redactGitOutput(stderr.trim(), args)}`,
        );
      }
      return stdout;
    },
    catch: (cause) =>
      new WorkspaceError({
        message:
          cause instanceof Error
            ? redactGitOutput(cause.message, args)
            : "git command failed",
        sessionId,
        reason: "git_failed",
      }),
  }).pipe(
    Effect.timeoutFail({
      duration: timeoutMs,
      onTimeout: () =>
        new WorkspaceError({
          message: `git ${gitSubcommand(args)} timed out after ${timeoutMs}ms`,
          sessionId,
          reason: "git_failed",
        }),
    }),
  );

export const unsetGitHubExtraHeader = (
  target: string,
  sessionId: string,
  timeoutMs = 120_000,
): Effect.Effect<void, never> => {
  if (needsWorkspaceIdentity())
    return cleanupWorkspaceProcess(
      target,
      "clear-git",
      "workspace.github_cleanup_failed",
      { sessionId },
    );
  return removeGitHubExtraHeaders(target, sessionId, timeoutMs).pipe(
    Effect.catchAll((error) =>
      Effect.logWarning("workspace.github_cleanup_failed").pipe(
        Effect.annotateLogs({ sessionId, error: error.message }),
      ),
    ),
  );
};

export const removeGitHubExtraHeaders = (
  target: string,
  sessionId: string,
  timeoutMs = 120_000,
): Effect.Effect<void, WorkspaceError> =>
  Effect.gen(function* () {
    // --file works for a repository owned by the worker and never consults
    // repository discovery/safe.directory or executes repository hooks.
    const configuredHeaders = yield* runGitOutput(
      [
        "config",
        "--file",
        join(target, ".git", "config"),
        "--get-all",
        "--",
        GITHUB_EXTRA_HEADER_KEY,
      ],
      target,
      sessionId,
      timeoutMs,
      undefined,
      true,
    );
    if (configuredHeaders.split(/\r?\n/u).some(isGatewayGitHubExtraHeader)) {
      yield* runGit(
        [
          "config",
          "--file",
          join(target, ".git", "config"),
          "--unset-all",
          "--",
          GITHUB_EXTRA_HEADER_KEY,
          "^AUTHORIZATION: basic eC1hY2Nlc3MtdG9rZW46",
        ],
        target,
        sessionId,
      );
    }
  });

const lstatOrMissing = (
  path: string,
  sessionId: string,
  reason: WorkspaceError["reason"],
) =>
  Effect.tryPromise({
    try: () =>
      lstat(path).then(
        (stats) => Option.some(stats),
        (error) => {
          if (enoent(error)) return Option.none();
          throw error;
        },
      ),
    catch: workspaceFailure(`lstat failed for ${path}`, reason, sessionId),
  });

const realpathOrFail = (
  path: string,
  sessionId: string,
  reason: WorkspaceError["reason"],
) =>
  Effect.tryPromise({
    try: () => realpath(path),
    catch: workspaceFailure(`realpath failed for ${path}`, reason, sessionId),
  });

const mkdirOrFail = (path: string, sessionId: string) =>
  Effect.tryPromise({
    try: () => mkdir(path, { recursive: true }),
    catch: workspaceFailure(
      `mkdir failed for ${path}`,
      "root_not_directory",
      sessionId,
    ),
  });

const readFileOrFail = (path: string, sessionId: string) =>
  Effect.tryPromise({
    try: () => readFile(path, "utf8"),
    catch: workspaceFailure(
      `read marker failed for ${path}`,
      "marker_mismatch",
      sessionId,
    ),
  });

const writeMarker = (
  markerPath: string,
  repository: RepositoryRecord,
  sessionId: string,
) =>
  Effect.tryPromise({
    try: () =>
      writeFile(
        markerPath,
        JSON.stringify({
          repositoryId: repository.id,
          url: repository.url,
          ref: repository.ref,
        }),
        { encoding: "utf8", mode: 0o600, flag: "wx" },
      ),
    catch: workspaceFailure(
      "write workspace marker failed",
      "git_failed",
      sessionId,
    ),
  });

const ensureRoot = (root: string, sessionId: string) =>
  Effect.gen(function* () {
    const existing = yield* lstatOrMissing(
      root,
      sessionId,
      "root_not_directory",
    );
    if (Option.isSome(existing)) {
      if (existing.value.isSymbolicLink() || !existing.value.isDirectory()) {
        return yield* Effect.fail(
          new WorkspaceError({
            message: "Workspace root must be a real directory",
            sessionId,
            reason: "root_not_directory",
          }),
        );
      }
      return yield* realpathOrFail(root, sessionId, "root_not_directory");
    }
    yield* mkdirOrFail(root, sessionId);
    return yield* realpathOrFail(root, sessionId, "root_not_directory");
  });

const validateExistingTarget = (
  canonicalRoot: string,
  target: string,
  sessionId: string,
) =>
  Effect.gen(function* () {
    const canonicalTarget = yield* realpathOrFail(
      target,
      sessionId,
      "path_escapes_root",
    );
    if (!isWithin(canonicalRoot, canonicalTarget)) {
      return yield* Effect.fail(
        new WorkspaceError({
          message: "Existing workspace resolves outside configured root",
          sessionId,
          reason: "path_escapes_root",
        }),
      );
    }
    return canonicalTarget;
  });

const validateMarker = (
  markerPath: string,
  repository: RepositoryRecord,
  sessionId: string,
) =>
  Effect.gen(function* () {
    const raw = yield* readFileOrFail(markerPath, sessionId);
    const parsed = yield* Effect.try({
      try: () => parseMarker(JSON.parse(raw)),
      catch: workspaceFailure(
        "invalid workspace marker",
        "marker_mismatch",
        sessionId,
      ),
    });
    if (
      Option.isNone(parsed) ||
      parsed.value.repositoryId !== repository.id ||
      parsed.value.url !== repository.url
    ) {
      return yield* Effect.fail(
        new WorkspaceError({
          message: "Workspace repository identity mismatch",
          sessionId,
          reason: "marker_mismatch",
        }),
      );
    }
  });

export const makeWorkspace = (input: {
  readonly workspaceRoot: string;
  readonly gitTimeoutMs?: number;
  readonly repo: WorkspaceRepoShape;
  readonly githubApp: GitHubAppTokenService | undefined;
}) =>
  Effect.gen(function* () {
    const hostKey = createHash("sha256")
      .update(hostname())
      .digest("hex")
      .slice(0, 16);
    // Ownership lives in the name, so even a crash between mkdir and the first
    // write leaves enough information for recovery. Never infer death from age.
    const stagingRecoveryGate = yield* Effect.makeSemaphore(1);
    let stagingRecovered = false;
    let stagingRetryAt = 0;
    const recoverStaging = stagingRecoveryGate.withPermits(1)(
      Effect.gen(function* () {
        if (
          stagingRecovered ||
          (yield* Clock.currentTimeMillis) < stagingRetryAt
        )
          return;
        yield* ensureRoot(input.workspaceRoot, "startup").pipe(
          Effect.flatMap((root) =>
            Effect.tryPromise({
              try: async () => {
                const entries = await opendir(root);
                let scanned = 0;
                let removed = 0;
                for await (const entry of entries) {
                  if (++scanned > 10_000 || removed >= 100) break;
                  const owner =
                    /^\.[a-f0-9]{32}-clone-([a-f0-9]{16})-([1-9][0-9]*)-[A-Za-z0-9]+$/u.exec(
                      entry.name,
                    );
                  if (
                    owner === null ||
                    owner[1] !== hostKey ||
                    !entry.isDirectory()
                  )
                    continue;
                  const pid = Number(owner[2]);
                  if (!Number.isSafeInteger(pid)) continue;
                  try {
                    process.kill(pid, 0);
                    continue;
                  } catch (error) {
                    if (
                      !(
                        error instanceof Error &&
                        "code" in error &&
                        error.code === "ESRCH"
                      )
                    )
                      continue;
                  }
                  // Dead owners cannot publish these random, per-clone names.
                  // Forced removal is idempotent when multiple gateways clean up.
                  await removeWorkerWorkspace(join(root, entry.name), root);
                  removed++;
                }
              },
              catch: workspaceFailure(
                "recover abandoned clones",
                "git_failed",
                "startup",
              ),
            }),
          ),
          Effect.tap(() =>
            Effect.sync(() => {
              stagingRecovered = true;
            }),
          ),
          Effect.catchAll((error) =>
            Effect.gen(function* () {
              stagingRetryAt = (yield* Clock.currentTimeMillis) + 30_000;
              yield* Effect.logWarning(
                "workspace.staging_recovery_failed",
              ).pipe(
                Effect.annotateLogs({
                  event: "workspace.staging_recovery_failed",
                  error: String(error),
                }),
              );
            }),
          ),
        );
      }),
    );
    const git = (
      args: ReadonlyArray<string>,
      cwd: string | undefined,
      sessionId: string,
    ) => runGit(args, cwd, sessionId, input.gitTimeoutMs);
    const resolve = Effect.fn("Workspace.resolve")(function* (
      context: unknown,
    ): Effect.fn.Return<RepositoryResolution, DatabaseError | RowDecodeError> {
      const parsed = parseContext(context);
      if (Option.isNone(parsed)) {
        yield* Effect.logWarning(
          "Workspace resolve rejected: invalid context",
        ).pipe(
          Effect.annotateLogs({
            reason: "invalid_context",
          }),
        );
        return { kind: "none" };
      }

      if (parsed.value.organizationId === null) {
        return { kind: "none" };
      }

      const repositories = yield* input.repo.listRepositories(
        parsed.value.organizationId as OrganizationId,
      );
      const resolution = resolveFromRepositories(parsed.value, repositories);

      yield* Effect.annotateCurrentSpan({
        "workspace.organization_id": parsed.value.organizationId,
        "workspace.resolution_kind": resolution.kind,
      });

      if (resolution.kind === "match") {
        yield* Effect.logInfo("Repository resolved").pipe(
          Effect.annotateLogs({
            event: "repository.resolved",
            organizationId: parsed.value.organizationId,
            repositoryId: resolution.repository.id,
            repositoryUrl: resolution.repository.url,
          }),
        );
      } else if (resolution.kind === "ambiguous") {
        yield* Effect.logInfo("Repository resolution ambiguous").pipe(
          Effect.annotateLogs({
            event: "repository.ambiguous",
            organizationId: parsed.value.organizationId,
            repositoryIds: resolution.repositories.map((r) => r.id),
          }),
        );
      }

      return resolution;
    });
    const refreshGitHubExtraHeader = Effect.fn(
      "Workspace.refreshGitHubExtraHeader",
    )(function* (
      sessionId: string,
      repository: RepositoryRecord,
      target: string,
    ): Effect.fn.Return<void, WorkspaceError> {
      const githubExtraHeader =
        input.githubApp === undefined
          ? undefined
          : yield* mintGitHubExtraHeader(
              input.githubApp,
              repository,
              sessionId,
            );
      if (githubExtraHeader === undefined) {
        yield* unsetGitHubExtraHeader(target, sessionId, input.gitTimeoutMs);
        return;
      }
      yield* persistGitHubExtraHeader(target, sessionId, githubExtraHeader);
    });

    const clearGitHubExtraHeader = (
      sessionId: string,
      target: string,
    ): Effect.Effect<void, never> =>
      unsetGitHubExtraHeader(target, sessionId, input.gitTimeoutMs);

    const materialize = Effect.fn("Workspace.materialize")(function* (
      sessionId: string,
      repository: RepositoryRecord,
    ): Effect.fn.Return<string, WorkspaceError> {
      yield* Effect.annotateCurrentSpan({
        "workspace.session_id": sessionId,
        "workspace.repository_id": repository.id,
      });

      if (
        !isHttpsGitHubRepositoryUrl(repository.url) ||
        repository.ref.startsWith("-") ||
        /[\r\n\0]/u.test(repository.ref)
      ) {
        return yield* Effect.fail(
          new WorkspaceError({
            message: "Repository must be an HTTPS GitHub URL with a valid ref",
            sessionId,
            reason: "git_failed",
          }),
        );
      }
      yield* recoverStaging;
      const canonicalRoot = yield* ensureRoot(input.workspaceRoot, sessionId);
      const target = join(canonicalRoot, safeSessionKey(sessionId));

      if (!isWithin(canonicalRoot, target)) {
        return yield* Effect.fail(
          new WorkspaceError({
            message: "Workspace path escapes configured root",
            sessionId,
            reason: "path_escapes_root",
          }),
        );
      }

      const targetStats = yield* lstatOrMissing(
        target,
        sessionId,
        "target_not_directory",
      );
      const markerPath = join(target, ".linear-gateway-workspace.json");

      if (Option.isSome(targetStats)) {
        if (
          targetStats.value.isSymbolicLink() ||
          !targetStats.value.isDirectory()
        ) {
          return yield* Effect.fail(
            new WorkspaceError({
              message: "Workspace target is not a real directory",
              sessionId,
              reason: "target_not_directory",
            }),
          );
        }

        const canonicalTarget = yield* validateExistingTarget(
          canonicalRoot,
          target,
          sessionId,
        );
        const marker = yield* lstatOrMissing(
          markerPath,
          sessionId,
          "marker_mismatch",
        );
        if (Option.isSome(marker)) {
          if (marker.value.isSymbolicLink() || !marker.value.isFile()) {
            return yield* Effect.fail(
              new WorkspaceError({
                message: "Workspace marker is not a real file",
                sessionId,
                reason: "marker_mismatch",
              }),
            );
          }
          yield* validateMarker(markerPath, repository, sessionId);
          yield* refreshGitHubExtraHeader(
            sessionId,
            repository,
            canonicalTarget,
          );

          yield* Effect.logInfo("Workspace ready (reused)").pipe(
            Effect.annotateLogs({
              event: "workspace.ready",
              repositoryId: repository.id,
              path: canonicalTarget,
              reused: true,
            }),
          );

          return canonicalTarget;
        }
        // A missing marker can also mean an agent removed an untracked file.
        // Preserve user work even when the old checkout cannot be reused.
        yield* Effect.tryPromise({
          try: () =>
            rename(
              canonicalTarget,
              join(
                canonicalRoot,
                `.stale-${safeSessionKey(sessionId)}-${crypto.randomUUID()}`,
              ),
            ),
          catch: workspaceFailure(
            "preserve marker-less workspace",
            "git_failed",
            sessionId,
          ),
        });
      }

      let githubExtraHeader: string | undefined;
      const repositoryCandidate = parseRepositorySuggestionCandidate(
        repository.url,
      );
      if (input.githubApp !== undefined) {
        if (isHttpsGitHubRepositoryUrl(repository.url)) {
          githubExtraHeader = yield* mintGitHubExtraHeader(
            input.githubApp,
            repository,
            sessionId,
          );
        } else if (
          repositoryCandidate?.hostname.toLowerCase() === "github.com"
        ) {
          yield* Effect.logDebug(
            "GitHub credentials skipped for SSH repository URL; SSH repositories are unsupported for GitHub App credentials",
          );
        } else {
          yield* Effect.logDebug(
            "GitHub credentials skipped for non-GitHub repository",
          );
        }
      }

      yield* Effect.scoped(
        Effect.gen(function* () {
          const staging = yield* Effect.acquireRelease(
            Effect.tryPromise({
              try: () =>
                mkdtemp(
                  join(
                    canonicalRoot,
                    `.${safeSessionKey(sessionId)}-clone-${hostKey}-${process.pid}-`,
                  ),
                ),
              catch: workspaceFailure(
                "create clone directory",
                "git_failed",
                sessionId,
              ),
            }),
            (path) =>
              Effect.tryPromise({
                try: () => removeWorkerWorkspace(path, canonicalRoot),
                catch: workspaceFailure(
                  "remove clone staging",
                  "git_failed",
                  sessionId,
                ),
              }).pipe(
                Effect.catchAll((error) =>
                  Effect.logWarning("workspace.staging_cleanup_failed").pipe(
                    Effect.annotateLogs({ sessionId, error: error.message }),
                  ),
                ),
              ),
          );
          const cloneArgs = [
            "clone",
            "--no-checkout",
            "--filter=blob:none",
            "--",
            repository.url,
            staging,
          ];
          yield* runGit(
            cloneArgs,
            undefined,
            sessionId,
            input.gitTimeoutMs,
            githubExtraHeader,
          );
          if (process.env.WORKER_UID)
            yield* Effect.tryPromise({
              try: () =>
                prepareWorkerWorkspace(staging, process.env.WORKER_UID),
              catch: workspaceFailure(
                "Could not assign workspace ownership",
                "git_failed",
                sessionId,
              ),
            });
          if (githubExtraHeader !== undefined) {
            yield* persistGitHubExtraHeader(
              staging,
              sessionId,
              githubExtraHeader,
            );
          }
          // If anything after persisting the header fails, drop it so the next
          // attempt doesn't inherit a stale/expired credential.
          yield* Effect.gen(function* () {
            yield* git(
              ["fetch", "--depth=1", "--", "origin", repository.ref],
              staging,
              sessionId,
            );
            yield* git(
              ["checkout", "--detach", "--force", "FETCH_HEAD", "--"],
              staging,
              sessionId,
            );
            yield* writeMarker(
              join(staging, ".linear-gateway-workspace.json"),
              repository,
              sessionId,
            );
          }).pipe(
            Effect.onError(() =>
              githubExtraHeader === undefined
                ? Effect.void
                : unsetGitHubExtraHeader(
                    staging,
                    sessionId,
                    input.gitTimeoutMs,
                  ),
            ),
          );

          yield* Effect.tryPromise({
            try: () => rename(staging, target),
            catch: workspaceFailure(
              "publish cloned workspace",
              "git_failed",
              sessionId,
            ),
          });
        }),
      );

      const finalTarget = yield* realpathOrFail(
        target,
        sessionId,
        "path_escapes_root",
      );
      if (!isWithin(canonicalRoot, finalTarget)) {
        return yield* Effect.fail(
          new WorkspaceError({
            message: "Materialized workspace resolves outside configured root",
            sessionId,
            reason: "path_escapes_root",
          }),
        );
      }

      yield* Effect.logInfo("Workspace ready").pipe(
        Effect.annotateLogs({
          event: "workspace.ready",
          repositoryId: repository.id,
          path: finalTarget,
          reused: false,
        }),
      );

      return finalTarget;
    });

    return {
      resolve,
      materialize,
      refreshGitHubExtraHeader,
      clearGitHubExtraHeader,
    };
  });
export class Workspace extends Effect.Service<Workspace>()("Workspace", {
  accessors: true,
  dependencies: [
    GatewayConfig.Default,
    WorkspaceRepo.Default,
    GitHubApp.Default,
  ],
  effect: Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const repo = yield* WorkspaceRepo;
    const githubApp =
      config.githubAppId !== undefined &&
      config.githubAppPrivateKey !== undefined
        ? yield* GitHubApp
        : undefined;
    return yield* makeWorkspace({
      workspaceRoot: config.workspaceRoot,
      gitTimeoutMs: config.gitTimeoutMs ?? 120_000,
      repo,
      githubApp,
    });
  }),
}) {}
