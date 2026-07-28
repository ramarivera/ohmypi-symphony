import { lstat, mkdir, readFile, realpath, writeFile } from "node:fs/promises";
import { join, relative, resolve, sep } from "node:path";
import type { RepositoryRecord } from "./domain";
import type { Logger } from "./logger";
import { createLogger } from "./logger";
import type { RepositoryResolution, WorkspacePort } from "./session-authority";
import type { GatewayStore } from "./store";

interface WorkspaceMarker {
  repositoryId: string;
  url: string;
  ref: string;
}

function isWithin(root: string, candidate: string): boolean {
  const path = relative(root, candidate);
  return path === "" || (!path.startsWith(`..${sep}`) && path !== "..");
}

function safeSessionKey(sessionId: string): string {
  const digest = new Bun.CryptoHasher("sha256").update(sessionId).digest("hex");
  return digest.slice(0, 32);
}

async function runGit(args: readonly string[], cwd?: string): Promise<void> {
  const process = Bun.spawn(["git", ...args], {
    ...(cwd ? { cwd } : {}),
    env: { PATH: Bun.env.PATH ?? "/usr/bin:/bin" },
    stdout: "pipe",
    stderr: "pipe",
  });
  const [exitCode, stderr] = await Promise.all([
    process.exited,
    new Response(process.stderr).text(),
  ]);
  if (exitCode !== 0)
    throw new Error(`git ${args[0] ?? "command"} failed: ${stderr.trim()}`);
}

function marker(value: unknown): WorkspaceMarker | null {
  if (typeof value !== "object" || value === null) return null;
  if (!("repositoryId" in value) || !("url" in value) || !("ref" in value))
    return null;
  return typeof value.repositoryId === "string" &&
    typeof value.url === "string" &&
    typeof value.ref === "string"
    ? { repositoryId: value.repositoryId, url: value.url, ref: value.ref }
    : null;
}

function hasIntersection(
  haystack: readonly string[],
  needles: readonly string[],
): boolean {
  const set = new Set(haystack);
  for (const needle of needles) {
    if (set.has(needle)) return true;
  }
  return false;
}

export class WorkspaceManager implements WorkspacePort {
  readonly #root: string;
  readonly #store: GatewayStore;
  readonly #logger: Logger;

  constructor(root: string, store: GatewayStore, logger?: Logger) {
    this.#root = resolve(root);
    this.#store = store;
    this.#logger =
      logger ??
      createLogger({ name: "workspace" }).child({
        component: "workspace",
      });
  }

  resolve(context: {
    organizationId: string | null;
    teamId: string | null;
    projectId: string | null;
    repositoryId: string | null;
    issueLabels: readonly string[];
    projectLabels: readonly string[];
  }): RepositoryResolution {
    const resolution = this.#resolveImpl(context);
    if (resolution.kind === "match") {
      this.#logger.info({
        event: "repository.resolved",
        organizationId: context.organizationId,
        repositoryId: resolution.repository.id,
        repositoryUrl: resolution.repository.url,
      });
    } else if (resolution.kind === "ambiguous") {
      this.#logger.info({
        event: "repository.ambiguous",
        organizationId: context.organizationId,
        repositoryIds: resolution.repositories.map((r) => r.id),
      });
    }
    return resolution;
  }

  #resolveImpl(context: {
    organizationId: string | null;
    teamId: string | null;
    projectId: string | null;
    repositoryId: string | null;
    issueLabels: readonly string[];
    projectLabels: readonly string[];
  }): RepositoryResolution {
    if (context.organizationId === null) return { kind: "none" };
    const repositories = this.#store.listRepositories(context.organizationId);
    if (repositories.length === 0) return { kind: "none" };

    const repositoryId =
      context.repositoryId !== null ? context.repositoryId.trim() : null;
    if (repositoryId !== null && repositoryId.length > 0) {
      const match = repositories.find(
        (repository) =>
          repository.id.toLowerCase() === repositoryId.toLowerCase(),
      );
      return match ? { kind: "match", repository: match } : { kind: "none" };
    }

    const issueLabels = context.issueLabels;
    if (issueLabels.length > 0) {
      const matches = repositories.filter((repository) =>
        hasIntersection(repository.labels, issueLabels),
      );
      const [repository] = matches;
      if (matches.length === 1 && repository)
        return { kind: "match", repository };
      if (matches.length > 1)
        return { kind: "ambiguous", repositories: matches };
    }

    const projectLabels = context.projectLabels;
    if (projectLabels.length > 0) {
      const matches = repositories.filter((repository) =>
        hasIntersection(repository.labels, projectLabels),
      );
      const [repository] = matches;
      if (matches.length === 1 && repository)
        return { kind: "match", repository };
      if (matches.length > 1)
        return { kind: "ambiguous", repositories: matches };
    }

    const projectId =
      context.projectId !== null
        ? context.projectId.trim().toLowerCase()
        : null;
    if (projectId !== null && projectId.length > 0) {
      const matches = repositories.filter((repository) =>
        repository.projectIds.includes(projectId),
      );
      const [repository] = matches;
      if (matches.length === 1 && repository)
        return { kind: "match", repository };
      if (matches.length > 1)
        return { kind: "ambiguous", repositories: matches };
    }

    const teamId =
      context.teamId !== null ? context.teamId.trim().toLowerCase() : null;
    if (teamId !== null && teamId.length > 0) {
      const matches = repositories.filter((repository) =>
        repository.teamIds.includes(teamId),
      );
      const [repository] = matches;
      if (matches.length === 1 && repository)
        return { kind: "match", repository };
      if (matches.length > 1)
        return { kind: "ambiguous", repositories: matches };
    }

    const defaultRepository = this.#store.getDefaultRepository(
      context.organizationId,
    );
    if (defaultRepository)
      return { kind: "match", repository: defaultRepository };
    return { kind: "none" };
  }

  async materialize(
    sessionId: string,
    repository: RepositoryRecord,
  ): Promise<string> {
    try {
      const rootMetadata = await lstat(this.#root);
      if (rootMetadata.isSymbolicLink() || !rootMetadata.isDirectory())
        throw new Error("Workspace root must be a real directory");
    } catch (error) {
      if (
        !(error instanceof Error) ||
        !("code" in error) ||
        error.code !== "ENOENT"
      )
        throw error;
      await mkdir(this.#root, { recursive: true });
    }
    const canonicalRoot = await realpath(this.#root);
    const target = join(canonicalRoot, safeSessionKey(sessionId));
    if (!isWithin(canonicalRoot, target))
      throw new Error("Workspace path escapes configured root");

    let exists = false;
    try {
      const metadata = await lstat(target);
      if (metadata.isSymbolicLink() || !metadata.isDirectory())
        throw new Error("Workspace target is not a real directory");
      exists = true;
    } catch (error) {
      if (
        !(error instanceof Error) ||
        !("code" in error) ||
        error.code !== "ENOENT"
      )
        throw error;
    }

    const markerPath = join(target, ".linear-gateway-workspace.json");
    if (exists) {
      const canonicalTarget = await realpath(target);
      if (!isWithin(canonicalRoot, canonicalTarget))
        throw new Error("Existing workspace resolves outside configured root");
      const parsed: unknown = JSON.parse(await readFile(markerPath, "utf8"));
      const existing = marker(parsed);
      if (
        !existing ||
        existing.repositoryId !== repository.id ||
        existing.url !== repository.url
      ) {
        throw new Error("Workspace repository identity mismatch");
      }
      this.#logger.info({
        event: "workspace.ready",
        repositoryId: repository.id,
        path: canonicalTarget,
        reused: true,
      });
      return canonicalTarget;
    }

    await runGit([
      "clone",
      "--no-checkout",
      "--filter=blob:none",
      repository.url,
      target,
    ]);
    await runGit(["fetch", "--depth=1", "origin", repository.ref], target);
    await runGit(["checkout", "--detach", "--force", "FETCH_HEAD"], target);
    await writeFile(
      markerPath,
      JSON.stringify({
        repositoryId: repository.id,
        url: repository.url,
        ref: repository.ref,
      }),
      {
        encoding: "utf8",
        mode: 0o600,
        flag: "wx",
      },
    );
    const canonicalTarget = await realpath(target);
    if (!isWithin(canonicalRoot, canonicalTarget))
      throw new Error(
        "Materialized workspace resolves outside configured root",
      );
    this.#logger.info({
      event: "workspace.ready",
      repositoryId: repository.id,
      path: canonicalTarget,
      reused: false,
    });
    return canonicalTarget;
  }
}
