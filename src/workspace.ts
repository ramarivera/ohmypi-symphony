import { lstat, mkdir, readFile, realpath, writeFile } from "node:fs/promises";
import { join, relative, resolve, sep } from "node:path";
import type { RepositoryDefinition, RepositoryMap } from "./domain";
import type { RepositoryResolution, WorkspacePort } from "./session-authority";

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

export class WorkspaceManager implements WorkspacePort {
  readonly #root: string;
  readonly #repositories: readonly RepositoryDefinition[];

  constructor(root: string, repositoryMap: RepositoryMap) {
    this.#root = resolve(root);
    this.#repositories = repositoryMap.repositories;
  }

  resolve(context: {
    teamId: string | null;
    projectId: string | null;
    repositoryId: string | null;
  }): RepositoryResolution {
    if (context.repositoryId) {
      const repository = this.#repositories.find(
        (item) => item.id === context.repositoryId,
      );
      return repository ? { kind: "match", repository } : { kind: "none" };
    }
    const projectId = context.projectId;
    const projectMatches = projectId
      ? this.#repositories.filter((item) => item.projectIds.includes(projectId))
      : [];
    const teamId = context.teamId;
    const matches =
      projectMatches.length > 0
        ? projectMatches
        : teamId
          ? this.#repositories.filter((item) => item.teamIds.includes(teamId))
          : [];
    if (matches.length === 0) return { kind: "none" };
    if (matches.length > 1) return { kind: "ambiguous", repositories: matches };
    const repository = matches[0];
    return repository ? { kind: "match", repository } : { kind: "none" };
  }

  async materialize(
    sessionId: string,
    repository: RepositoryDefinition,
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
      await runGit(["fetch", "--depth=1", "origin", repository.ref], target);
      await runGit(["checkout", "--detach", "--force", "FETCH_HEAD"], target);
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
    return canonicalTarget;
  }
}
