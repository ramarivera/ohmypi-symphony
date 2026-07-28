import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { WorkspaceManager } from "../src/workspace";

let root: string;
let source: string;

async function git(args: string[], cwd: string): Promise<void> {
  const process = Bun.spawn(["git", ...args], {
    cwd,
    stdout: "pipe",
    stderr: "pipe",
  });
  const code = await process.exited;
  if (code !== 0) throw new Error(await new Response(process.stderr).text());
}

beforeEach(async () => {
  await mkdir(join(process.cwd(), "data"), { recursive: true });
  root = await mkdtemp(join(process.cwd(), "data", "workspace-test-"));
  source = join(root, "source");
  await mkdir(source);
  await git(["init", "-b", "main"], source);
  await git(["config", "user.email", "test@example.com"], source);
  await git(["config", "user.name", "Gateway Test"], source);
  await writeFile(join(source, "README.txt"), "fixture");
  await git(["add", "README.txt"], source);
  await git(["commit", "-m", "fixture"], source);
});

afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

function manager(workspaceRoot = join(root, "workspaces")) {
  return new WorkspaceManager(workspaceRoot, {
    repositories: [
      {
        id: "one",
        url: source,
        ref: "main",
        teamIds: ["team"],
        projectIds: ["project"],
      },
      {
        id: "two",
        url: source,
        ref: "main",
        teamIds: ["team"],
        projectIds: [],
      },
    ],
  });
}

describe("WorkspaceManager", () => {
  test("prefers project mapping and reports team ambiguity", () => {
    expect(
      manager().resolve({
        teamId: "team",
        projectId: "project",
        repositoryId: null,
      }),
    ).toMatchObject({ kind: "match", repository: { id: "one" } });
    expect(
      manager().resolve({
        teamId: "team",
        projectId: null,
        repositoryId: null,
      }),
    ).toMatchObject({ kind: "ambiguous" });
    expect(
      manager().resolve({
        teamId: null,
        projectId: null,
        repositoryId: "missing",
      }),
    ).toEqual({ kind: "none" });
  });

  test("materializes a deterministic repository-bound workspace", async () => {
    const workspaces = manager();
    const repository = {
      id: "one",
      url: source,
      ref: "main",
      teamIds: ["team"],
      projectIds: ["project"],
    };
    const first = await workspaces.materialize("session", repository);
    const second = await workspaces.materialize("session", repository);
    expect(first).toBe(second);
    expect(await Bun.file(join(first, "README.txt")).text()).toBe("fixture");
  }, 15_000);

  test("rejects a symlink workspace target", async () => {
    const workspaceRoot = join(root, "linked-workspaces");
    await symlink(source, workspaceRoot);
    await expect(
      manager(workspaceRoot).materialize("session", {
        id: "one",
        url: source,
        ref: "main",
        teamIds: [],
        projectIds: [],
      }),
    ).rejects.toThrow();
  });
});
