import { createHash } from "node:crypto";
import { lstat, mkdir, readFile, realpath, writeFile } from "node:fs/promises";
import { join, relative, sep } from "node:path";
import { Effect, Option } from "effect";
import {
  type DatabaseError,
  type RowDecodeError,
  WorkspaceError,
} from "../domain/errors.js";
import type { OrganizationId } from "../domain/ids.js";
import type { RepositoryRecord } from "../domain/models.js";
import { GatewayConfig } from "./config.js";
import { WorkspaceRepo } from "./store/repositories.js";

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

export function isWithin(root: string, candidate: string): boolean {
  const path = relative(root, candidate);
  return path === "" || (!path.startsWith(`..${sep}`) && path !== "..");
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

const runGit = (
  args: ReadonlyArray<string>,
  cwd: string | undefined,
  sessionId: string,
) =>
  Effect.tryPromise({
    try: async () => {
      const process = Bun.spawn(["git", ...args], {
        ...(cwd ? { cwd } : {}),
        stdout: "pipe",
        stderr: "pipe",
      });
      const [exitCode, stderr] = await Promise.all([
        process.exited,
        new Response(process.stderr).text(),
      ]);
      if (exitCode !== 0) {
        throw new Error(`git ${args[0] ?? "command"} failed: ${stderr.trim()}`);
      }
    },
    catch: workspaceFailure("git command failed", "git_failed", sessionId),
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
    const parsed = parseMarker(JSON.parse(raw));
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
  readonly repo: WorkspaceRepoShape;
}) =>
  Effect.gen(function* () {
    const resolve = Effect.fn("Workspace.resolve")(function* (
      context: unknown,
    ): Effect.fn.Return<RepositoryResolution, DatabaseError | RowDecodeError> {
      const parsed = parseContext(context);
      if (Option.isNone(parsed)) {
        yield* Effect.logWarning(
          "Workspace resolve rejected: invalid context",
          {
            reason: "invalid_context",
          },
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
        yield* Effect.logInfo("Repository resolved", {
          event: "repository.resolved",
          organizationId: parsed.value.organizationId,
          repositoryId: resolution.repository.id,
          repositoryUrl: resolution.repository.url,
        });
      } else if (resolution.kind === "ambiguous") {
        yield* Effect.logInfo("Repository resolution ambiguous", {
          event: "repository.ambiguous",
          organizationId: parsed.value.organizationId,
          repositoryIds: resolution.repositories.map((r) => r.id),
        });
      }

      return resolution;
    });

    const materialize = Effect.fn("Workspace.materialize")(function* (
      sessionId: string,
      repository: RepositoryRecord,
    ): Effect.fn.Return<string, WorkspaceError> {
      yield* Effect.annotateCurrentSpan({
        "workspace.session_id": sessionId,
        "workspace.repository_id": repository.id,
      });

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
        yield* validateMarker(markerPath, repository, sessionId);

        yield* Effect.logInfo("Workspace ready (reused)", {
          event: "workspace.ready",
          repositoryId: repository.id,
          path: canonicalTarget,
          reused: true,
        });

        return canonicalTarget;
      }

      yield* runGit(
        [
          "clone",
          "--no-checkout",
          "--filter=blob:none",
          repository.url,
          target,
        ],
        undefined,
        sessionId,
      );
      yield* runGit(
        ["fetch", "--depth=1", "origin", repository.ref],
        target,
        sessionId,
      );
      yield* runGit(
        ["checkout", "--detach", "--force", "FETCH_HEAD"],
        target,
        sessionId,
      );
      yield* writeMarker(markerPath, repository, sessionId);

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

      yield* Effect.logInfo("Workspace ready", {
        event: "workspace.ready",
        repositoryId: repository.id,
        path: finalTarget,
        reused: false,
      });

      return finalTarget;
    });

    return { resolve, materialize };
  });

export class Workspace extends Effect.Service<Workspace>()("Workspace", {
  accessors: true,
  dependencies: [GatewayConfig.Default, WorkspaceRepo.Default],
  effect: Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const repo = yield* WorkspaceRepo;
    return yield* makeWorkspace({ workspaceRoot: config.workspaceRoot, repo });
  }),
}) {}
