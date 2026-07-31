import { afterEach, beforeEach, describe, expect, it, test } from "@effect/vitest"
import * as fc from "effect/FastCheck";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { GatewayStore } from "../src/store";
import { WorkspaceManager } from "../src/workspace";

let root: string;
let source: string;
let store: GatewayStore;
let workspaceManager: WorkspaceManager;

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
  store = await GatewayStore.open(":memory:", new Uint8Array(32).fill(1));
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

  store.createRepository({
    organizationId: "org",
    id: "one",
    url: source,
    ref: "main",
    teamIds: ["team"],
    projectIds: ["project"],
    labels: [],
    isDefault: false,
  });
  store.createRepository({
    organizationId: "org",
    id: "two",
    url: source,
    ref: "main",
    teamIds: ["team"],
    projectIds: [],
    labels: [],
    isDefault: false,
  });

  workspaceManager = new WorkspaceManager(join(root, "workspaces"), store);
});

afterEach(async () => {
  store.close();
  await rm(root, { recursive: true, force: true });
});

describe("WorkspaceManager", () => {
  test("prefers project mapping and reports team ambiguity", () => {
    expect(
      workspaceManager.resolve({
        organizationId: "org",
        teamId: "team",
        projectId: "project",
        repositoryId: null,
        issueLabels: [],
        projectLabels: [],
      }),
    ).toMatchObject({ kind: "match", repository: { id: "one" } });
    expect(
      workspaceManager.resolve({
        organizationId: "org",
        teamId: "team",
        projectId: null,
        repositoryId: null,
        issueLabels: [],
        projectLabels: [],
      }),
    ).toMatchObject({ kind: "ambiguous" });
    expect(
      workspaceManager.resolve({
        organizationId: "org",
        teamId: null,
        projectId: null,
        repositoryId: "missing",
        issueLabels: [],
        projectLabels: [],
      }),
    ).toEqual({ kind: "none" });
  });

  test("materializes a deterministic repository-bound workspace", async () => {
    const repository = store.getRepository("org", "one");
    if (!repository) throw new Error("Repository not found");
    const first = await workspaceManager.materialize("session", repository);
    const second = await workspaceManager.materialize("session", repository);
    expect(first).toBe(second);
    expect(await Bun.file(join(first, "README.txt")).text()).toBe("fixture");
  }, 15_000);

  test("rejects a symlink workspace target", async () => {
    const workspaceRoot = join(root, "linked-workspaces");
    await symlink(source, workspaceRoot);
    const repository = store.createRepository({
      organizationId: "org",
      id: "symlink-repo",
      url: source,
      ref: "main",
      teamIds: [],
      projectIds: [],
      labels: [],
      isDefault: false,
    });
    await expect(
      new WorkspaceManager(workspaceRoot, store).materialize(
        "session",
        repository,
      ),
    ).rejects.toThrow();
  });
});

describe("WorkspaceManager invariants", () => {
  it.prop(
    "resolve is deterministic for any context",
    {
      organizationId: fc.string({ maxLength: 20 }),
      repositoryId: fc.string({ maxLength: 20 }),
      issueLabels: fc.array(fc.string({ maxLength: 20 })),
      projectLabels: fc.array(fc.string({ maxLength: 20 })),
      projectId: fc.string({ maxLength: 20 }),
      teamId: fc.string({ maxLength: 20 }),
    },
    ({
      organizationId,
      repositoryId,
      issueLabels,
      projectLabels,
      projectId,
      teamId,
    }) => {
      const context = {
        organizationId,
        repositoryId,
        issueLabels,
        projectLabels,
        projectId,
        teamId,
      };
      const first = workspaceManager.resolve(context);
      const second = workspaceManager.resolve(context);
      expect(second).toEqual(first);
    },
  );
});
