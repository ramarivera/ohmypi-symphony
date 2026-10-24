import { createHash } from "node:crypto";
import * as filesystem from "node:fs/promises";
import {
  lstat,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { hostname } from "node:os";
import { join } from "node:path";
import { it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Logger, Result, Schema } from "effect";
import { TestClock } from "effect/testing";
import * as fc from "fast-check";
import { describe, expect, vi } from "vitest";
import { WorkspaceError } from "../src/domain/errors.js";
import {
  OrganizationId,
  ProjectId,
  SessionId,
  TeamId,
  WorkspaceId,
} from "../src/domain/ids.js";
import type { RepositoryRecord } from "../src/domain/models.js";
import { GatewayConfig } from "../src/services/config.js";
import { buildGitHubExtraHeader } from "../src/services/github-app.js";
import { WorkspaceRepo } from "../src/services/store/repositories.js";
import { SqliteClientLive } from "../src/services/store/sqlite-client.js";
import {
  isWithin,
  makeWorkspace,
  runGitOutput,
  safeSessionKey,
  Workspace,
} from "../src/services/workspace.js";

vi.mock("node:fs/promises", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs/promises")>();
  return { ...actual, rm: vi.fn(actual.rm) };
});

const MARKER_FILE = ".linear-gateway-workspace.json";
const TEST_KEY_BASE64 = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=";

const organizationId = (value: string) =>
  Schema.decodeUnknownSync(OrganizationId)(value);
const projectId = (value: string) => Schema.decodeUnknownSync(ProjectId)(value);
const sessionId = (value: string) => Schema.decodeUnknownSync(SessionId)(value);
const teamId = (value: string) => Schema.decodeUnknownSync(TeamId)(value);
const workspaceId = (value: string) =>
  Schema.decodeUnknownSync(WorkspaceId)(value);

const fixtureFailure = (operation: string) => (cause: unknown) =>
  new WorkspaceError({
    message: `${operation} failed`,
    sessionId: "workspace-fixture",
    reason: "git_failed",
    cause: cause instanceof Error ? cause.message : String(cause),
  });

const fixtureIo = <A>(operation: string, run: () => Promise<A>) =>
  Effect.tryPromise({
    try: run,
    catch: fixtureFailure(operation),
  });

const runGit = (args: ReadonlyArray<string>, cwd: string) =>
  Effect.tryPromise({
    try: async () => {
      const process = Bun.spawn(["git", ...args], {
        cwd,
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
    catch: fixtureFailure("git command"),
  });

const readGitConfig = (key: string, cwd: string) =>
  Effect.tryPromise({
    try: async () => {
      const process = Bun.spawn(["git", "config", "--local", "--get", key], {
        cwd,
        stdout: "pipe",
        stderr: "pipe",
      });
      const [exitCode, stdout] = await Promise.all([
        process.exited,
        new Response(process.stdout).text(),
      ]);
      return exitCode === 0 ? stdout.trim() : undefined;
    },
    catch: fixtureFailure("git config read"),
  });
const readGitConfigAll = (key: string, cwd: string) =>
  Effect.tryPromise({
    try: async () => {
      const process = Bun.spawn(
        ["git", "config", "--local", "--get-all", key],
        {
          cwd,
          stdout: "pipe",
          stderr: "pipe",
        },
      );
      const [exitCode, stdout] = await Promise.all([
        process.exited,
        new Response(process.stdout).text(),
      ]);
      return exitCode === 0
        ? stdout
            .split(/\r?\n/u)
            .map((value) => value.trim())
            .filter((value) => value.length > 0)
        : [];
    },
    catch: fixtureFailure("git config read"),
  });

interface GitFixture {
  readonly root: string;
  readonly source: string;
  readonly url: string;
  readonly previousGitConfigGlobal: string | undefined;
  readonly workspaceRoot: string;
}

const gitFixture = Effect.acquireRelease(
  Effect.gen(function* () {
    yield* fixtureIo("fixture parent mkdir", () =>
      mkdir(join(process.cwd(), "data"), { recursive: true }),
    );
    const root = yield* fixtureIo("fixture directory creation", () =>
      mkdtemp(join(process.cwd(), "data", "workspace-test-")),
    );
    const source = join(root, "source");
    yield* fixtureIo("source directory creation", () => mkdir(source));
    yield* runGit(["init", "-b", "main"], source);
    yield* runGit(["config", "user.email", "test@example.com"], source);
    yield* runGit(["config", "user.name", "Gateway Test"], source);
    yield* fixtureIo("fixture write", () =>
      writeFile(join(source, "README.txt"), "fixture\n"),
    );
    yield* runGit(["add", "README.txt"], source);
    yield* runGit(["commit", "-m", "fixture"], source);

    const previousGitConfigGlobal = process.env.GIT_CONFIG_GLOBAL;
    const url = `https://github.com/fixture/repo-${crypto.randomUUID()}.git`;
    const gitConfig = join(root, "gitconfig");
    yield* fixtureIo("git transport config", () =>
      writeFile(
        gitConfig,
        `[url "${source}"]\n\tinsteadOf = ${url}\n[url "${root}/missing"]\n\tinsteadOf = https://github.com/octo-org/repository-that-does-not-exist.git\n`,
      ),
    );
    process.env.GIT_CONFIG_GLOBAL = gitConfig;
    return {
      root,
      source,
      url,
      previousGitConfigGlobal,
      workspaceRoot: join(root, "workspaces"),
    };
  }),
  (fixture) =>
    fixtureIo("fixture cleanup", async () => {
      for (const [name, value] of [
        ["GIT_CONFIG_GLOBAL", fixture.previousGitConfigGlobal],
      ] as const) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      await rm(fixture.root, { recursive: true, force: true });
    }).pipe(
      Effect.catchTag("@Gateway/WorkspaceError", (error) => Effect.die(error)),
    ),
);

const gatewayConfigLayer = (workspaceRoot: string) =>
  GatewayConfig.layer.pipe(
    Layer.provide(
      ConfigProvider.layer(
        ConfigProvider.fromUnknown(
          Object.fromEntries(
            new Map([
              ["LINEAR_CLIENT_ID", "workspace-test-client"],
              ["LINEAR_CLIENT_SECRET", "workspace-test-client-secret"],
              ["LINEAR_WEBHOOK_SECRET", "workspace-test-webhook-secret"],
              ["TOKEN_ENCRYPTION_KEY", TEST_KEY_BASE64],
              ["PUBLIC_URL", "http://localhost:3000"],
              ["WORKSPACE_ROOT", workspaceRoot],
            ]),
          ),
          { preserveEmptyStrings: true },
        ),
      ),
    ),
  );

const workspaceLayer = (workspaceRoot: string) =>
  Layer.mergeAll(WorkspaceRepo.layer, Workspace.layer).pipe(
    Layer.provide(
      Layer.mergeAll(
        SqliteClientLive(":memory:"),
        gatewayConfigLayer(workspaceRoot),
      ),
    ),
  );

const withWorkspace = <A, E>(
  workspaceRoot: string,
  effect: Effect.Effect<A, E, Workspace | WorkspaceRepo>,
) => effect.pipe(Effect.provide(workspaceLayer(workspaceRoot)));

const repositoryRecord = (input: {
  readonly id: string;
  readonly organization?: string;
  readonly url?: string;
  readonly ref?: string;
  readonly labels?: ReadonlyArray<string>;
  readonly projectIds?: ReadonlyArray<string>;
  readonly teamIds?: ReadonlyArray<string>;
  readonly isDefault?: boolean;
}): RepositoryRecord => ({
  organizationId: organizationId(input.organization ?? "org"),
  id: workspaceId(input.id),
  url: input.url ?? "https://example.test/repository.git",
  ref: input.ref ?? "main",
  labels: [...(input.labels ?? [])],
  projectIds: (input.projectIds ?? []).map(projectId),
  teamIds: (input.teamIds ?? []).map(teamId),
  nixPackages: [],
  isDefault: input.isDefault ?? false,
  createdAt: 0,
  updatedAt: 0,
});

const resolveWith = (
  repositories: ReadonlyArray<RepositoryRecord>,
  context: unknown,
) =>
  Effect.runSync(
    Effect.gen(function* () {
      const workspace = yield* makeWorkspace({
        workspaceRoot: "/workspace-test",
        repo: { listRepositories: () => Effect.succeed(repositories) },
        githubApp: undefined,
      });
      return yield* workspace.resolve(context);
    }),
  );

const expectWorkspaceFailure = <A>(
  result: Result.Result<A, WorkspaceError>,
  reason: WorkspaceError["reason"],
) => {
  expect(Result.isFailure(result)).toBe(true);
  if (Result.isSuccess(result)) {
    throw new Error("Expected WorkspaceError");
  }
  expect(result.failure._tag).toBe("@Gateway/WorkspaceError");
  expect(result.failure.reason).toBe(reason);
};

const createRepository = (
  repo: WorkspaceRepo["Service"],
  fixture: GitFixture,
  id: string,
  overrides: Partial<{
    readonly labels: ReadonlyArray<string>;
    readonly projectIds: ReadonlyArray<string>;
    readonly teamIds: ReadonlyArray<string>;
    readonly isDefault: boolean;
    readonly ref: string;
  }> = {},
) =>
  repo.createRepository({
    organizationId: organizationId("org"),
    id: workspaceId(id),
    url: fixture.url,
    ref: overrides.ref ?? "main",
    labels: overrides.labels ?? [],
    projectIds: (overrides.projectIds ?? []).map(projectId),
    teamIds: (overrides.teamIds ?? []).map(teamId),
    isDefault: overrides.isDefault ?? false,
  });

describe("Workspace", () => {
  it.live(
    "routes explicit repository, labels, project, team, and default mappings by precedence",
    () =>
      Effect.gen(function* () {
        const fixture = yield* gitFixture;
        yield* withWorkspace(
          fixture.workspaceRoot,
          Effect.gen(function* () {
            const repository = yield* WorkspaceRepo;
            const workspace = yield* Workspace;
            yield* createRepository(repository, fixture, "explicit");
            yield* createRepository(repository, fixture, "issue", {
              labels: ["issue-label"],
            });
            yield* createRepository(repository, fixture, "project-label", {
              labels: ["project-label"],
            });
            yield* createRepository(repository, fixture, "project", {
              projectIds: ["project"],
            });
            yield* createRepository(repository, fixture, "team", {
              teamIds: ["team"],
            });
            yield* createRepository(repository, fixture, "default", {
              isDefault: true,
            });

            expect(
              yield* workspace.resolve({
                organizationId: organizationId("org"),
                repositoryId: workspaceId("explicit"),
                issueLabels: ["issue-label"],
                projectLabels: ["project-label"],
                projectId: projectId("project"),
                teamId: teamId("team"),
              }),
            ).toMatchObject({ kind: "match", repository: { id: "explicit" } });
            expect(
              yield* workspace.resolve({
                organizationId: organizationId("org"),
                repositoryId: null,
                issueLabels: [" ISSUE-LABEL "],
                projectLabels: ["project-label"],
                projectId: projectId("project"),
                teamId: teamId("team"),
              }),
            ).toMatchObject({ kind: "match", repository: { id: "issue" } });
            expect(
              yield* workspace.resolve({
                organizationId: organizationId("org"),
                repositoryId: null,
                issueLabels: [],
                projectLabels: ["PROJECT-LABEL"],
                projectId: projectId("project"),
                teamId: teamId("team"),
              }),
            ).toMatchObject({
              kind: "match",
              repository: { id: "project-label" },
            });
            expect(
              yield* workspace.resolve({
                organizationId: organizationId("org"),
                repositoryId: null,
                issueLabels: [],
                projectLabels: [],
                projectId: projectId("project"),
                teamId: teamId("team"),
              }),
            ).toMatchObject({ kind: "match", repository: { id: "project" } });
            expect(
              yield* workspace.resolve({
                organizationId: organizationId("org"),
                repositoryId: null,
                issueLabels: [],
                projectLabels: [],
                projectId: null,
                teamId: teamId("team"),
              }),
            ).toMatchObject({ kind: "match", repository: { id: "team" } });
            expect(
              yield* workspace.resolve({
                organizationId: organizationId("org"),
                repositoryId: null,
                issueLabels: [],
                projectLabels: [],
                projectId: null,
                teamId: null,
              }),
            ).toMatchObject({ kind: "match", repository: { id: "default" } });
            expect(
              yield* workspace.resolve({
                organizationId: organizationId("org"),
                repositoryId: workspaceId("missing"),
                issueLabels: [],
                projectLabels: [],
                projectId: null,
                teamId: null,
              }),
            ).toEqual({ kind: "none" });
          }),
        );
      }),
  );

  it.live("reports all competing repository mappings as an ambiguity", () =>
    Effect.gen(function* () {
      const fixture = yield* gitFixture;
      yield* withWorkspace(
        fixture.workspaceRoot,
        Effect.gen(function* () {
          const repository = yield* WorkspaceRepo;
          const workspace = yield* Workspace;
          yield* createRepository(repository, fixture, "team-one", {
            teamIds: ["team"],
          });
          yield* createRepository(repository, fixture, "team-two", {
            teamIds: ["team"],
          });

          expect(
            yield* workspace.resolve({
              organizationId: organizationId("org"),
              repositoryId: null,
              issueLabels: [],
              projectLabels: [],
              projectId: null,
              teamId: teamId("team"),
            }),
          ).toMatchObject({
            kind: "ambiguous",
            repositories: [{ id: "team-one" }, { id: "team-two" }],
          });
        }),
      );
    }),
  );

  it.live(
    "materializes a detached checkout once and safely reuses its repository-bound target",
    () =>
      Effect.gen(function* () {
        const fixture = yield* gitFixture;
        yield* withWorkspace(
          fixture.workspaceRoot,
          Effect.gen(function* () {
            const repository = yield* WorkspaceRepo;
            const workspace = yield* Workspace;
            const record = yield* createRepository(repository, fixture, "one");
            const id = sessionId("session-one");

            const first = yield* workspace.materialize(id, record);
            const second = yield* workspace.materialize(id, record);
            expect(second).toBe(first);
            expect(
              yield* fixtureIo("workspace read", () =>
                readFile(join(first, "README.txt"), "utf8"),
              ),
            ).toBe("fixture\n");
            expect(
              JSON.parse(
                yield* fixtureIo("workspace marker read", () =>
                  readFile(join(first, MARKER_FILE), "utf8"),
                ),
              ),
            ).toEqual({
              repositoryId: "one",
              url: fixture.url,
              ref: "main",
            });
            expect(
              yield* fixtureIo("workspace git head", async () => {
                const process = Bun.spawn(
                  ["git", "status", "--porcelain=v2", "--branch"],
                  {
                    cwd: first,
                    stdout: "pipe",
                    stderr: "pipe",
                  },
                );
                const [exitCode, stdout] = await Promise.all([
                  process.exited,
                  new Response(process.stdout).text(),
                ]);
                if (exitCode !== 0)
                  throw new Error("workspace HEAD is not inspectable");
                return stdout;
              }),
            ).toContain("# branch.head (detached)");
          }),
        );
      }),
  );

  it.live("refreshes GitHub credentials when reusing a workspace", () =>
    Effect.gen(function* () {
      const fixture = yield* gitFixture;
      const id = sessionId("github-reuse-refresh");
      const record = repositoryRecord({
        id: "github-reuse-refresh",
        url: "https://github.com/octo-org/private-repo.git",
      });
      yield* fixtureIo("workspace root creation", () =>
        mkdir(fixture.workspaceRoot, { recursive: true }),
      );
      const target = join(fixture.workspaceRoot, safeSessionKey(id));
      yield* fixtureIo("workspace target creation", () => mkdir(target));
      yield* runGit(["init"], target);
      yield* runGit(
        [
          "config",
          "--local",
          "http.https://github.com/.extraheader",
          buildGitHubExtraHeader("stale-token"),
        ],
        target,
      );
      yield* runGit(
        [
          "config",
          "--local",
          "--add",
          "http.https://github.com/.extraheader",
          "X-User: retained",
        ],
        target,
      );
      yield* fixtureIo("workspace marker creation", () =>
        writeFile(
          join(target, MARKER_FILE),
          JSON.stringify({
            repositoryId: record.id,
            url: record.url,
            ref: record.ref,
          }),
        ),
      );
      let tokenCalls = 0;
      const workspace = yield* makeWorkspace({
        workspaceRoot: fixture.workspaceRoot,
        repo: { listRepositories: () => Effect.succeed([]) },
        githubApp: {
          getBotIdentity: () =>
            Effect.succeed({
              name: "test-app[bot]",
              email: "98765+test-app[bot]@users.noreply.github.com",
            }),
          getInstallationToken: () => {
            tokenCalls += 1;
            return Effect.succeed("fresh-token");
          },
        },
      });
      yield* workspace.materialize(id, record);
      expect(yield* readGitConfig("user.name", target)).toBe("test-app[bot]");
      expect(yield* readGitConfig("user.email", target)).toBe(
        "98765+test-app[bot]@users.noreply.github.com",
      );
      yield* runGit(["commit", "--allow-empty", "-m", "worker commit"], target);
      yield* runGit(["config", "--local", "--unset", "user.email"], target);
      yield* workspace.materialize(id, record);
      expect(yield* readGitConfig("user.email", target)).toBe(
        "98765+test-app[bot]@users.noreply.github.com",
      );
      yield* runGit(["config", "--local", "--unset", "user.name"], target);
      yield* workspace.refreshGitHubExtraHeader(id, record, target);
      expect(yield* readGitConfig("user.name", target)).toBe("test-app[bot]");
      expect(tokenCalls).toBe(3);
      expect(
        yield* readGitConfigAll("http.https://github.com/.extraheader", target),
      ).toEqual(["X-User: retained", buildGitHubExtraHeader("fresh-token")]);
      yield* workspace.clearGitHubExtraHeader(id, target);
      expect(
        yield* readGitConfigAll("http.https://github.com/.extraheader", target),
      ).toEqual(["X-User: retained"]);
    }),
  );

  it.live(
    "never writes the bot identity into a repository enclosing a non-checkout target",
    () =>
      Effect.gen(function* () {
        const fixture = yield* gitFixture;
        const outer = join(fixture.root, "enclosing-repo");
        const inner = join(outer, "not-a-checkout");
        yield* fixtureIo("enclosing repo creation", () =>
          mkdir(inner, { recursive: true }),
        );
        yield* runGit(["init"], outer);
        const id = sessionId("identity-scope");
        const record = repositoryRecord({
          id: "identity-scope",
          url: "https://github.com/octo-org/private-repo.git",
        });
        const workspace = yield* makeWorkspace({
          workspaceRoot: fixture.workspaceRoot,
          repo: { listRepositories: () => Effect.succeed([]) },
          githubApp: {
            getBotIdentity: () =>
              Effect.succeed({
                name: "test-app[bot]",
                email: "98765+test-app[bot]@users.noreply.github.com",
              }),
            getInstallationToken: () => Effect.succeed("fresh-token"),
          },
        });
        yield* Effect.result(
          workspace.refreshGitHubExtraHeader(id, record, inner),
        );
        expect(yield* readGitConfig("user.name", outer)).toBeUndefined();
        expect(yield* readGitConfig("user.email", outer)).toBeUndefined();
      }),
  );

  it.live("unsets GitHub credentials when reusing without configuration", () =>
    Effect.gen(function* () {
      const fixture = yield* gitFixture;
      const id = sessionId("github-reuse-unset");
      const record = repositoryRecord({
        id: "github-reuse-unset",
        url: "https://github.com/octo-org/private-repo.git",
      });
      yield* fixtureIo("workspace root creation", () =>
        mkdir(fixture.workspaceRoot, { recursive: true }),
      );
      const target = join(fixture.workspaceRoot, safeSessionKey(id));
      yield* fixtureIo("workspace target creation", () => mkdir(target));
      yield* runGit(["init"], target);
      yield* runGit(
        [
          "config",
          "--local",
          "http.https://github.com/.extraheader",
          buildGitHubExtraHeader("stale-token"),
        ],
        target,
      );

      yield* fixtureIo("workspace marker creation", () =>
        writeFile(
          join(target, MARKER_FILE),
          JSON.stringify({
            repositoryId: record.id,
            url: record.url,
            ref: record.ref,
          }),
        ),
      );
      const workspace = yield* makeWorkspace({
        workspaceRoot: fixture.workspaceRoot,
        repo: { listRepositories: () => Effect.succeed([]) },
        githubApp: undefined,
      });
      yield* workspace.materialize(id, record);
      expect(
        yield* readGitConfig("http.https://github.com/.extraheader", target),
      ).toBeUndefined();
    }),
  );

  it.live(
    "preserves a user-installed GitHub extraheader when clearing credentials",
    () =>
      Effect.gen(function* () {
        const fixture = yield* gitFixture;
        const id = sessionId("github-reuse-preserve-foreign");
        const record = repositoryRecord({
          id: "github-reuse-preserve-foreign",
          url: "https://github.com/octo-org/private-repo.git",
        });
        yield* fixtureIo("workspace root creation", () =>
          mkdir(fixture.workspaceRoot, { recursive: true }),
        );
        const target = join(fixture.workspaceRoot, safeSessionKey(id));
        yield* fixtureIo("workspace target creation", () => mkdir(target));
        yield* runGit(["init"], target);
        const foreignHeader = "Authorization: Bearer user-installed";
        yield* runGit(
          [
            "config",
            "--local",
            "http.https://github.com/.extraheader",
            foreignHeader,
          ],
          target,
        );
        yield* fixtureIo("workspace marker creation", () =>
          writeFile(
            join(target, MARKER_FILE),
            JSON.stringify({
              repositoryId: record.id,
              url: record.url,
              ref: record.ref,
            }),
          ),
        );
        const workspace = yield* makeWorkspace({
          workspaceRoot: fixture.workspaceRoot,
          repo: { listRepositories: () => Effect.succeed([]) },
          githubApp: undefined,
        });
        yield* workspace.materialize(id, record);
        expect(
          yield* readGitConfigAll(
            "http.https://github.com/.extraheader",
            target,
          ),
        ).toEqual([foreignHeader]);
      }),
  );

  it.live(
    "preserves foreign GitHub extraheaders alongside gateway credentials",
    () =>
      Effect.gen(function* () {
        const fixture = yield* gitFixture;
        const id = sessionId("github-reuse-preserve-mixed");
        const record = repositoryRecord({
          id: "github-reuse-preserve-mixed",
          url: "https://github.com/octo-org/private-repo.git",
        });
        yield* fixtureIo("workspace root creation", () =>
          mkdir(fixture.workspaceRoot, { recursive: true }),
        );
        const target = join(fixture.workspaceRoot, safeSessionKey(id));
        yield* fixtureIo("workspace target creation", () => mkdir(target));
        yield* runGit(["init"], target);
        const gatewayHeader = buildGitHubExtraHeader("stale-token");
        const foreignHeader = "Authorization: Bearer user-installed";
        yield* runGit(
          [
            "config",
            "--local",
            "--add",
            "http.https://github.com/.extraheader",
            gatewayHeader,
          ],
          target,
        );
        yield* runGit(
          [
            "config",
            "--local",
            "--add",
            "http.https://github.com/.extraheader",
            foreignHeader,
          ],
          target,
        );
        yield* fixtureIo("workspace marker creation", () =>
          writeFile(
            join(target, MARKER_FILE),
            JSON.stringify({
              repositoryId: record.id,
              url: record.url,
              ref: record.ref,
            }),
          ),
        );
        const workspace = yield* makeWorkspace({
          workspaceRoot: fixture.workspaceRoot,
          repo: { listRepositories: () => Effect.succeed([]) },
          githubApp: undefined,
        });
        yield* workspace.materialize(id, record);
        expect(
          yield* readGitConfigAll(
            "http.https://github.com/.extraheader",
            target,
          ),
        ).toEqual([foreignHeader]);
      }),
  );

  it.live("scrubs GitHub credentials from clone failures", () =>
    Effect.gen(function* () {
      const fixture = yield* gitFixture;
      const token = "credential-token";
      const workspace = yield* makeWorkspace({
        workspaceRoot: fixture.workspaceRoot,
        repo: { listRepositories: () => Effect.succeed([]) },
        githubApp: {
          getBotIdentity: () =>
            Effect.succeed({
              name: "test-app[bot]",
              email: "98765+test-app[bot]@users.noreply.github.com",
            }),
          getInstallationToken: () => Effect.succeed(token),
        },
      });
      const result = yield* Effect.result(
        workspace.materialize(
          sessionId("github-clone-failure"),
          repositoryRecord({
            id: "github-clone-failure",
            url: "https://github.com/octo-org/repository-that-does-not-exist.git",
          }),
        ),
      );
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isSuccess(result)) return;
      expect(result.failure.message).toContain("git clone");
      expect(result.failure.message).not.toContain(token);
      expect(result.failure.message).not.toContain(
        buildGitHubExtraHeader(token),
      );
    }),
  );

  it.live("cleans GitHub credentials from a terminal workspace", () =>
    Effect.gen(function* () {
      const fixture = yield* gitFixture;
      const id = sessionId("github-terminal-cleanup");
      yield* fixtureIo("workspace root creation", () =>
        mkdir(fixture.workspaceRoot, { recursive: true }),
      );
      const target = join(fixture.workspaceRoot, safeSessionKey(id));
      yield* fixtureIo("workspace target creation", () => mkdir(target));
      yield* runGit(["init"], target);
      yield* runGit(
        [
          "config",
          "--local",
          "http.https://github.com/.extraheader",
          buildGitHubExtraHeader("terminal-token"),
        ],
        target,
      );
      const workspace = yield* makeWorkspace({
        workspaceRoot: fixture.workspaceRoot,
        repo: { listRepositories: () => Effect.succeed([]) },
        githubApp: undefined,
      });
      yield* workspace.clearGitHubExtraHeader(id, target);
      expect(
        yield* readGitConfig("http.https://github.com/.extraheader", target),
      ).toBeUndefined();
    }),
  );

  it.live("rejects SSH-style GitHub URLs before minting credentials", () =>
    Effect.gen(function* () {
      const fixture = yield* gitFixture;
      const id = sessionId("github-ssh-reuse");
      const record = repositoryRecord({
        id: "github-ssh-reuse",
        url: "git@github.com:octo-org/private-repo.git",
      });
      yield* fixtureIo("workspace root creation", () =>
        mkdir(fixture.workspaceRoot, { recursive: true }),
      );
      const target = join(fixture.workspaceRoot, safeSessionKey(id));
      yield* fixtureIo("workspace target creation", () => mkdir(target));
      yield* runGit(["init"], target);
      yield* fixtureIo("workspace marker creation", () =>
        writeFile(
          join(target, MARKER_FILE),
          JSON.stringify({
            repositoryId: record.id,
            url: record.url,
            ref: record.ref,
          }),
        ),
      );
      let tokenCalls = 0;
      const workspace = yield* makeWorkspace({
        workspaceRoot: fixture.workspaceRoot,
        repo: { listRepositories: () => Effect.succeed([]) },
        githubApp: {
          getBotIdentity: () =>
            Effect.succeed({
              name: "test-app[bot]",
              email: "98765+test-app[bot]@users.noreply.github.com",
            }),
          getInstallationToken: () => {
            tokenCalls += 1;
            return Effect.succeed("unexpected-token");
          },
        },
      });
      const rejected = yield* Effect.result(workspace.materialize(id, record));
      expect(Result.isFailure(rejected)).toBe(true);
      expect(tokenCalls).toBe(0);
      expect(
        yield* readGitConfig("http.https://github.com/.extraheader", target),
      ).toBeUndefined();
    }),
  );

  it.live(
    "rejects invalid repository records with their typed repository error",
    () =>
      Effect.gen(function* () {
        const fixture = yield* gitFixture;
        yield* withWorkspace(
          fixture.workspaceRoot,
          Effect.gen(function* () {
            const repository = yield* WorkspaceRepo;
            const result = yield* Effect.result(
              repository.createRepository({
                organizationId: organizationId("org"),
                id: workspaceId("unsafe-repository"),
                url: "--upload-pack=unsafe",
                ref: "main",
              }),
            );
            expect(Result.isFailure(result)).toBe(true);
            if (Result.isSuccess(result))
              throw new Error("Expected RowDecodeError");
            if (result.failure._tag !== "@Gateway/RowDecodeError") {
              throw new Error(
                `Expected RowDecodeError, got ${result.failure._tag}`,
              );
            }
            expect(result.failure.entity).toBe("RepositoryRecord");
          }),
        );
      }),
  );

  it.live("rejects root and target symlinks without following them", () =>
    Effect.gen(function* () {
      const fixture = yield* gitFixture;
      yield* withWorkspace(
        fixture.workspaceRoot,
        Effect.gen(function* () {
          const repository = yield* WorkspaceRepo;
          const record = yield* createRepository(repository, fixture, "one");
          const linkedRoot = join(fixture.root, "linked-workspaces");
          yield* fixtureIo("workspace root symlink", () =>
            symlink(fixture.source, linkedRoot),
          );

          const linkedWorkspace = yield* makeWorkspace({
            workspaceRoot: linkedRoot,
            repo: repository,
            githubApp: undefined,
          });
          expectWorkspaceFailure(
            yield* Effect.result(
              linkedWorkspace.materialize(sessionId("linked-root"), record),
            ),
            "root_not_directory",
          );

          yield* fixtureIo("workspace root creation", () =>
            mkdir(fixture.workspaceRoot),
          );
          const target = join(
            fixture.workspaceRoot,
            safeSessionKey("linked-target"),
          );
          yield* fixtureIo("workspace target symlink", () =>
            symlink(fixture.source, target),
          );
          const workspace = yield* makeWorkspace({
            workspaceRoot: fixture.workspaceRoot,
            repo: repository,
            githubApp: undefined,
          });
          expectWorkspaceFailure(
            yield* Effect.result(
              workspace.materialize(sessionId("linked-target"), record),
            ),
            "target_not_directory",
          );
        }),
      );
    }),
  );

  it.live(
    "rejects mismatched reuse and failed checkouts without creating a valid marker",
    () =>
      Effect.gen(function* () {
        const fixture = yield* gitFixture;
        yield* withWorkspace(
          fixture.workspaceRoot,
          Effect.gen(function* () {
            const repository = yield* WorkspaceRepo;
            const workspace = yield* Workspace;
            const firstRepository = yield* createRepository(
              repository,
              fixture,
              "one",
            );
            const secondRepository = yield* createRepository(
              repository,
              fixture,
              "two",
            );
            const reusedSession = sessionId("mismatched-reuse");
            yield* workspace.materialize(reusedSession, firstRepository);
            expectWorkspaceFailure(
              yield* Effect.result(
                workspace.materialize(reusedSession, secondRepository),
              ),
              "marker_mismatch",
            );

            const failingRepository = yield* createRepository(
              repository,
              fixture,
              "missing-ref",
              {
                ref: "missing-ref",
              },
            );
            const failingSession = sessionId("failed-checkout");
            expectWorkspaceFailure(
              yield* Effect.result(
                workspace.materialize(failingSession, failingRepository),
              ),
              "git_failed",
            );
            const failedTarget = join(
              fixture.workspaceRoot,
              safeSessionKey("failed-checkout"),
            );
            expect(
              yield* Effect.result(
                fixtureIo("failed workspace marker read", () =>
                  readFile(join(failedTarget, MARKER_FILE), "utf8"),
                ),
              ),
            ).toMatchObject({ _tag: "Failure" });
          }),
        );
      }),
  );

  it.live("releases every bounded git fixture when its scope closes", () =>
    Effect.gen(function* () {
      const root = yield* Effect.scoped(
        Effect.gen(function* () {
          const fixture = yield* gitFixture;
          return fixture.root;
        }),
      );
      const metadata = yield* Effect.result(
        fixtureIo("post-scope fixture stat", () => lstat(root)),
      );
      expect(Result.isFailure(metadata)).toBe(true);
    }),
  );
});

describe("Workspace properties", () => {
  it("generated repository contexts retain explicit, label, project, team, and default precedence", () =>
    fc.assert(
      fc.property(
        fc.record({ suffix: fc.integer({ min: 1, max: 1_000_000 }) }),
        ({ suffix }) => {
          const org = `org-${suffix}`;
          const issueLabel = `issue-${suffix}`;
          const projectLabel = `project-label-${suffix}`;
          const project = `project-${suffix}`;
          const team = `team-${suffix}`;
          const repositories = [
            repositoryRecord({ id: `explicit-${suffix}`, organization: org }),
            repositoryRecord({
              id: `issue-${suffix}`,
              organization: org,
              labels: [issueLabel],
            }),
            repositoryRecord({
              id: `project-label-${suffix}`,
              organization: org,
              labels: [projectLabel],
            }),
            repositoryRecord({
              id: `project-${suffix}`,
              organization: org,
              projectIds: [project],
            }),
            repositoryRecord({
              id: `team-${suffix}`,
              organization: org,
              teamIds: [team],
            }),
            repositoryRecord({
              id: `default-${suffix}`,
              organization: org,
              isDefault: true,
            }),
          ];
          const context = {
            organizationId: org,
            repositoryId: null,
            issueLabels: [issueLabel],
            projectLabels: [projectLabel],
            projectId: project,
            teamId: team,
          };

          expect(
            resolveWith(repositories, {
              ...context,
              repositoryId: `explicit-${suffix}`,
            }),
          ).toMatchObject({
            kind: "match",
            repository: { id: `explicit-${suffix}` },
          });
          expect(resolveWith(repositories, context)).toMatchObject({
            kind: "match",
            repository: { id: `issue-${suffix}` },
          });
          expect(
            resolveWith(repositories, { ...context, issueLabels: [] }),
          ).toMatchObject({
            kind: "match",
            repository: { id: `project-label-${suffix}` },
          });
          expect(
            resolveWith(repositories, {
              ...context,
              issueLabels: [],
              projectLabels: [],
            }),
          ).toMatchObject({
            kind: "match",
            repository: { id: `project-${suffix}` },
          });
          expect(
            resolveWith(repositories, {
              ...context,
              issueLabels: [],
              projectLabels: [],
              projectId: null,
            }),
          ).toMatchObject({
            kind: "match",
            repository: { id: `team-${suffix}` },
          });
          expect(
            resolveWith(repositories, {
              ...context,
              issueLabels: [],
              projectLabels: [],
              projectId: null,
              teamId: null,
            }),
          ).toMatchObject({
            kind: "match",
            repository: { id: `default-${suffix}` },
          });
        },
      ),
    ));

  it("generated competing mappings remain explicit ambiguities with a stable candidate order", () =>
    fc.assert(
      fc.property(
        fc.record({ suffix: fc.integer({ min: 1, max: 1_000_000 }) }),
        ({ suffix }) => {
          const label = `shared-${suffix}`;
          const repositories = [
            repositoryRecord({ id: `first-${suffix}`, labels: [label] }),
            repositoryRecord({ id: `second-${suffix}`, labels: [label] }),
          ];
          const context = {
            organizationId: "org",
            repositoryId: null,
            issueLabels: [label],
            projectLabels: [],
            projectId: null,
            teamId: null,
          };
          const first = resolveWith(repositories, context);
          const second = resolveWith(repositories, context);
          expect(first).toEqual({ kind: "ambiguous", repositories });
          expect(second).toEqual(first);
        },
      ),
    ));

  it("normalized nested paths stay contained while normalized traversals are rejected", () =>
    fc.assert(
      fc.property(
        fc.record({
          segments: fc.array(
            fc.constantFrom("alpha", "beta", "gamma", "delta"),
            {
              maxLength: 8,
            },
          ),
          outsideSegment: fc.constantFrom("outside", "other", "escape"),
        }),
        ({ segments, outsideSegment }) => {
          const root = "/workspace-root";
          expect(isWithin(root, join(root, ...segments))).toBe(true);
          expect(
            isWithin(root, join(root, "safe", "..", "..", outsideSegment)),
          ).toBe(false);
          expect(isWithin(root, join(root, "..", outsideSegment))).toBe(false);
        },
      ),
    ));

  it("workspace identity is deterministic, fixed-width, and collision-resistant for generated session IDs", () =>
    fc.assert(
      fc.property(
        fc.record({ first: fc.uuid(), second: fc.uuid() }),
        ({ first, second }) => {
          const firstKey = safeSessionKey(first);
          expect(safeSessionKey(first)).toBe(firstKey);
          expect(firstKey).toMatch(/^[a-f0-9]{32}$/u);
          if (first !== second)
            expect(safeSessionKey(second)).not.toBe(firstKey);
        },
      ),
    ));
});

it.live(
  "keeps GitHub credentials off every Git argv while cloning and checking out a real fixture",
  () =>
    Effect.gen(function* () {
      const fixture = yield* gitFixture;
      const uid = String(process.getuid?.() ?? 1000);
      yield* Effect.acquireRelease(
        Effect.sync(() => vi.stubEnv("WORKER_UID", uid)),
        () => Effect.sync(() => vi.unstubAllEnvs()),
      );
      const nativeSpawn = Bun.spawn;
      const spy = yield* Effect.acquireRelease(
        Effect.sync(() =>
          vi.spyOn(Bun, "spawn").mockImplementation((command, options) => {
            // Native tests can't setgroups. Verify the uid wrapper while executing
            // the real Git operations as the checkout owner.
            if (Array.isArray(command) && command[0] === "/usr/bin/setpriv")
              return nativeSpawn(
                command.slice(command.indexOf("--") + 1),
                options,
              );
            return nativeSpawn(command, options);
          }),
        ),
        (value) => Effect.sync(() => value.mockRestore()),
      );
      const token = "ghs_argv_secret";
      const header = buildGitHubExtraHeader(token);
      const workspace = yield* makeWorkspace({
        workspaceRoot: fixture.workspaceRoot,
        repo: { listRepositories: () => Effect.succeed([]) },
        githubApp: {
          getBotIdentity: () =>
            Effect.succeed({
              name: "test-app[bot]",
              email: "98765+test-app[bot]@users.noreply.github.com",
            }),
          getInstallationToken: () => Effect.succeed(token),
        },
      });
      const globalConfigBefore = yield* fixtureIo("read global config", () =>
        readFile(join(fixture.root, "gitconfig"), "utf8"),
      );
      const target = yield* workspace.materialize(
        "secret-argv-test",
        repositoryRecord({ id: "secret-argv-test", url: fixture.url }),
        uid,
      );
      expect(yield* readGitConfig("user.name", target)).toBe("test-app[bot]");
      expect(yield* readGitConfig("user.email", target)).toBe(
        "98765+test-app[bot]@users.noreply.github.com",
      );
      yield* runGit(["commit", "--allow-empty", "-m", "worker commit"], target);
      expect(
        yield* fixtureIo("read global config", () =>
          readFile(join(fixture.root, "gitconfig"), "utf8"),
        ),
      ).toBe(globalConfigBefore);
      const calls = spy.mock.calls;
      const commands = calls
        .map(([command]) => command)
        .filter((command): command is string[] => Array.isArray(command));
      for (const key of ["user.name", "user.email"]) {
        const configCommand = commands.find((command) => command.includes(key));
        expect(configCommand?.slice(0, 5)).toEqual([
          "/usr/bin/setpriv",
          "--reuid",
          uid,
          "--regid",
          uid,
        ]);
        expect(configCommand).toContain("--local");
      }
      expect(JSON.stringify(commands)).not.toContain(token);
      expect(JSON.stringify(commands)).not.toContain(header);
      const clone = calls.find(
        ([command]) => Array.isArray(command) && command.includes("clone"),
      );
      expect(clone?.[0]).toContain("--");
      expect(clone?.[1]?.env?.GIT_CONFIG_VALUE_0).toBe(header);
      expect(clone?.[1]?.env?.GIT_TERMINAL_PROMPT).toBe("0");
      expect(clone?.[1]?.env?.GIT_ASKPASS).toBe("");
      expect(clone?.[1]?.env).not.toHaveProperty("LINEAR_CLIENT_SECRET");
      expect(
        yield* readGitConfig("http.https://github.com/.extraheader", target),
      ).toBe(header);
    }),
);

describe("Workspace recovery regressions", () => {
  it.live("retries cleanly when fetch fails after cloning", () =>
    Effect.gen(function* () {
      const fixture = yield* gitFixture;
      const workspace = yield* makeWorkspace({
        workspaceRoot: fixture.workspaceRoot,
        repo: { listRepositories: () => Effect.succeed([]) },
        githubApp: undefined,
      });
      const repository = repositoryRecord({
        id: "atomic-clone",
        url: fixture.url,
        ref: "missing-ref",
      });
      const result = yield* workspace
        .materialize("atomic-session", repository)
        .pipe(Effect.result);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(result.failure.reason).toBe("git_failed");
      expect(
        yield* fixtureIo("list workspace root", () =>
          readdir(fixture.workspaceRoot),
        ),
      ).toEqual([]);
      const target = yield* workspace.materialize("atomic-session", {
        ...repository,
        ref: "HEAD",
      });
      expect(
        yield* fixtureIo("read clone", () =>
          readFile(join(target, "README.txt"), "utf8"),
        ),
      ).toBe("fixture\n");
      expect(
        yield* fixtureIo("read marker", () =>
          readFile(join(target, MARKER_FILE), "utf8"),
        ),
      ).toContain("atomic-clone");
    }),
  );

  it.live(
    "preserves a marker-less checkout while creating its replacement",
    () =>
      Effect.gen(function* () {
        const fixture = yield* gitFixture;
        const target = join(
          fixture.workspaceRoot,
          safeSessionKey("partial-session"),
        );
        yield* fixtureIo("create partial clone", async () => {
          await mkdir(target, { recursive: true });
          await writeFile(join(target, "partial.txt"), "incomplete");
        });
        const workspace = yield* makeWorkspace({
          workspaceRoot: fixture.workspaceRoot,
          repo: { listRepositories: () => Effect.succeed([]) },
          githubApp: undefined,
        });
        expect(
          yield* workspace.materialize(
            "partial-session",
            repositoryRecord({
              id: "partial",
              url: fixture.url,
              ref: "HEAD",
            }),
          ),
        ).toBe(target);
        expect(
          yield* fixtureIo("read recovered clone", () => readdir(target)),
        ).not.toContain("partial.txt");
        const siblings = yield* fixtureIo("find preserved checkout", () =>
          readdir(fixture.workspaceRoot),
        );
        const stale = siblings.find((name) =>
          name.startsWith(`.stale-${safeSessionKey("partial-session")}-`),
        );
        if (stale === undefined) throw new Error("preserved workspace missing");
        expect(
          yield* fixtureIo("read preserved work", () =>
            readFile(join(fixture.workspaceRoot, stale, "partial.txt"), "utf8"),
          ),
        ).toBe("incomplete");
      }),
  );

  it.live(
    "reclaims dead local clone owners and preserves live, foreign and legacy staging",
    () =>
      Effect.gen(function* () {
        const fixture = yield* gitFixture;
        const dead = Bun.spawn(["bun", "-e", "process.exit(0)"], {
          stdout: "ignore",
          stderr: "ignore",
        });
        yield* Effect.promise(() => dead.exited);
        const host = createHash("sha256")
          .update(hostname())
          .digest("hex")
          .slice(0, 16);
        const prefix = `.${safeSessionKey("abandoned")}-clone-`;
        const abandoned = `${prefix}${host}-${dead.pid}-dead00`;
        const live = `${prefix}${host}-${process.pid}-active`;
        const foreign = `${prefix}${host === "0000000000000000" ? "1111111111111111" : "0000000000000000"}-${dead.pid}-remote`;
        const legacy = `${prefix}legacy`;
        yield* fixtureIo("seed staging", async () => {
          for (const name of [abandoned, live, foreign, legacy])
            await mkdir(join(fixture.workspaceRoot, name), { recursive: true });
        });
        const workspace = yield* makeWorkspace({
          workspaceRoot: fixture.workspaceRoot,
          repo: { listRepositories: () => Effect.succeed([]) },
          githubApp: undefined,
        });
        yield* workspace.materialize(
          "recovered",
          repositoryRecord({
            id: "recovered",
            url: fixture.url,
            ref: "HEAD",
          }),
        );
        const names = yield* fixtureIo("read staging", () =>
          readdir(fixture.workspaceRoot),
        );
        expect(names).not.toContain(abandoned);
        expect(names).toEqual(expect.arrayContaining([live, foreign, legacy]));
      }),
  );

  it.effect(
    "backs off a failed staging scan and retries after its deadline",
    () =>
      Effect.gen(function* () {
        const fixture = yield* gitFixture;
        const dead = Bun.spawn([process.execPath, "-e", "process.exit(0)"], {
          stdout: "ignore",
          stderr: "ignore",
        });
        yield* Effect.promise(() => dead.exited);
        const host = createHash("sha256")
          .update(hostname())
          .digest("hex")
          .slice(0, 16);
        const abandoned = `.${safeSessionKey("scan-retry")}-clone-${host}-${dead.pid}-dead00`;
        yield* fixtureIo("seed abandoned clone", () =>
          mkdir(join(fixture.workspaceRoot, abandoned), { recursive: true }),
        );
        const workspace = yield* makeWorkspace({
          workspaceRoot: fixture.workspaceRoot,
          repo: { listRepositories: () => Effect.succeed([]) },
          githubApp: undefined,
        });
        const abandonedPath = join(fixture.workspaceRoot, abandoned);
        const originalRemove = vi.mocked(filesystem.rm).getMockImplementation();
        if (originalRemove === undefined)
          throw new Error("filesystem implementation missing");
        let attempts = 0;
        const removeSpy = yield* Effect.acquireRelease(
          Effect.sync(() =>
            vi.spyOn(filesystem, "rm").mockImplementation((path, options) => {
              if (path === abandonedPath && ++attempts === 1)
                return Promise.reject(new Error("controlled cleanup denial"));
              return originalRemove(path, options);
            }),
          ),
          (spy) => Effect.sync(() => spy.mockRestore()),
        );
        const messages: string[] = [];
        const logger = Logger.make((options) =>
          messages.push(String(options.message)),
        );
        const repository = repositoryRecord({
          id: "scan-retry",
          url: fixture.url,
          ref: "HEAD",
        });
        yield* Effect.gen(function* () {
          const path = yield* workspace.materialize("first", repository);
          expect(
            yield* fixtureIo("read workspace", () =>
              readFile(join(path, "README.txt"), "utf8"),
            ),
          ).toBe("fixture\n");
          expect(messages).toContain("workspace.staging_recovery_failed");
          expect(
            yield* fixtureIo("read staging", () =>
              readdir(fixture.workspaceRoot),
            ),
          ).toContain(abandoned);
        }).pipe(Effect.provide(Logger.layer([logger])));
        expect(attempts).toBe(1);
        yield* workspace.materialize("second", repository);
        expect(attempts).toBe(1);
        expect(removeSpy).toHaveBeenCalled();
        yield* TestClock.adjust(30_000);
        yield* workspace.materialize("third", repository);
        expect(attempts).toBe(2);
        expect(
          yield* fixtureIo("read staging", () =>
            readdir(fixture.workspaceRoot),
          ),
        ).not.toContain(abandoned);
      }),
  );

  it.live("bounds a hanging git subprocess and kills its process group", () =>
    Effect.gen(function* () {
      const fixture = yield* gitFixture;
      const pidFile = join(fixture.root, "descendant.pid");
      const result = yield* runGitOutput(
        [
          "-c",
          `alias.hang=!sleep 60 & child=$!; echo $child > '${pidFile}'; wait $child`,
          "hang",
        ],
        fixture.root,
        "git-timeout",
        300,
      ).pipe(Effect.result);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(result.failure.message).toContain("timed out after 300ms");
      const pid = Number(
        yield* fixtureIo("read descendant pid", () =>
          readFile(pidFile, "utf8"),
        ),
      );
      expect(Number.isSafeInteger(pid) && pid > 0).toBe(true);
      const gone = yield* fixtureIo("await descendant exit", async () => {
        const deadline = Date.now() + 3000;
        while (Date.now() < deadline) {
          try {
            process.kill(pid, 0);
          } catch (error) {
            if (
              error instanceof Error &&
              "code" in error &&
              error.code === "ESRCH"
            )
              return true;
            throw error;
          }
          await Bun.sleep(20);
        }
        return false;
      });
      expect(gone).toBe(true);
    }),
  );
});
