import {
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { join } from "node:path";
import { it } from "@effect/vitest";
import { ConfigProvider, Effect, Either, Layer, Schema } from "effect";
import * as fc from "effect/FastCheck";
import { describe, expect } from "vitest";
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
import { WorkspaceRepo } from "../src/services/store/repositories.js";
import { SqliteClientLive } from "../src/services/store/sqlite-client.js";
import {
  isWithin,
  makeWorkspace,
  safeSessionKey,
  Workspace,
  workspaceBranchName,
} from "../src/services/workspace.js";

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

interface GitFixture {
  readonly root: string;
  readonly source: string;
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

    return { root, source, workspaceRoot: join(root, "workspaces") };
  }),
  (fixture) =>
    fixtureIo("fixture cleanup", () =>
      rm(fixture.root, { recursive: true, force: true }),
    ).pipe(
      Effect.catchTag("@Gateway/WorkspaceError", (error) => Effect.die(error)),
    ),
);

const gatewayConfigLayer = (workspaceRoot: string) =>
  GatewayConfig.Default.pipe(
    Layer.provide(
      Layer.setConfigProvider(
        ConfigProvider.fromMap(
          new Map([
            ["LINEAR_CLIENT_ID", "workspace-test-client"],
            ["LINEAR_CLIENT_SECRET", "workspace-test-client-secret"],
            ["LINEAR_WEBHOOK_SECRET", "workspace-test-webhook-secret"],
            ["LINEAR_ALLOWED_ORGANIZATION_IDS", "workspace-org"],
            ["TOKEN_ENCRYPTION_KEY", TEST_KEY_BASE64],
            ["PUBLIC_URL", "http://localhost:3000"],
            ["WORKSPACE_ROOT", workspaceRoot],
          ]),
        ),
      ),
    ),
  );

const workspaceLayer = (workspaceRoot: string) =>
  Layer.mergeAll(WorkspaceRepo.Default, Workspace.Default).pipe(
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
      });
      return yield* workspace.resolve(context);
    }),
  );

const expectWorkspaceFailure = <A>(
  result: Either.Either<A, WorkspaceError>,
  reason: WorkspaceError["reason"],
) => {
  expect(Either.isLeft(result)).toBe(true);
  if (Either.isRight(result)) {
    throw new Error("Expected WorkspaceError");
  }
  expect(result.left._tag).toBe("@Gateway/WorkspaceError");
  expect(result.left.reason).toBe(reason);
};

const createRepository = (
  repo: WorkspaceRepo,
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
    url: fixture.source,
    ref: overrides.ref ?? "main",
    labels: overrides.labels ?? [],
    projectIds: (overrides.projectIds ?? []).map(projectId),
    teamIds: (overrides.teamIds ?? []).map(teamId),
    isDefault: overrides.isDefault ?? false,
  });

describe("Workspace", () => {
  it.scopedLive(
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

  it.scopedLive(
    "reports all competing repository mappings as an ambiguity",
    () =>
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

  it.scopedLive(
    "materializes a deterministic branch once and safely reuses its repository-bound target",
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
              url: fixture.source,
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
            ).toContain(`# branch.head ${workspaceBranchName(id)}`);
            expect(
              yield* fixtureIo("workspace publication base", async () => {
                const process = Bun.spawn(
                  ["git", "rev-list", "--count", "FETCH_HEAD..HEAD"],
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
                  throw new Error(
                    "workspace publication base is not inspectable",
                  );
                return stdout;
              }),
            ).toBe("0\n");
          }),
        );
      }),
  );

  it.scopedLive(
    "rejects invalid repository records with their typed repository error",
    () =>
      Effect.gen(function* () {
        const fixture = yield* gitFixture;
        yield* withWorkspace(
          fixture.workspaceRoot,
          Effect.gen(function* () {
            const repository = yield* WorkspaceRepo;
            const result = yield* Effect.either(
              repository.createRepository({
                organizationId: organizationId("org"),
                id: workspaceId("unsafe-repository"),
                url: "--upload-pack=unsafe",
                ref: "main",
              }),
            );
            expect(Either.isLeft(result)).toBe(true);
            if (Either.isRight(result))
              throw new Error("Expected RowDecodeError");
            if (result.left._tag !== "@Gateway/RowDecodeError") {
              throw new Error(
                `Expected RowDecodeError, got ${result.left._tag}`,
              );
            }
            expect(result.left.entity).toBe("RepositoryRecord");
          }),
        );
      }),
  );

  it.scopedLive("rejects root and target symlinks without following them", () =>
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
          });
          expectWorkspaceFailure(
            yield* Effect.either(
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
          });
          expectWorkspaceFailure(
            yield* Effect.either(
              workspace.materialize(sessionId("linked-target"), record),
            ),
            "target_not_directory",
          );
        }),
      );
    }),
  );

  it.scopedLive(
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
              yield* Effect.either(
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
              yield* Effect.either(
                workspace.materialize(failingSession, failingRepository),
              ),
              "git_failed",
            );
            const failedTarget = join(
              fixture.workspaceRoot,
              safeSessionKey("failed-checkout"),
            );
            expect(
              yield* Effect.either(
                fixtureIo("failed workspace marker read", () =>
                  readFile(join(failedTarget, MARKER_FILE), "utf8"),
                ),
              ),
            ).toMatchObject({ _tag: "Left" });
          }),
        );
      }),
  );

  it.scopedLive(
    "releases every bounded git fixture when its scope closes",
    () =>
      Effect.gen(function* () {
        const root = yield* Effect.scoped(
          Effect.gen(function* () {
            const fixture = yield* gitFixture;
            return fixture.root;
          }),
        );
        const metadata = yield* Effect.either(
          fixtureIo("post-scope fixture stat", () => lstat(root)),
        );
        expect(Either.isLeft(metadata)).toBe(true);
      }),
  );
});

describe("Workspace properties", () => {
  it.prop(
    "generated repository contexts retain explicit, label, project, team, and default precedence",
    { suffix: fc.integer({ min: 1, max: 1_000_000 }) },
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
      ).toMatchObject({ kind: "match", repository: { id: `team-${suffix}` } });
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
  );

  it.prop(
    "generated competing mappings remain explicit ambiguities with a stable candidate order",
    { suffix: fc.integer({ min: 1, max: 1_000_000 }) },
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
  );

  it.prop(
    "normalized nested paths stay contained while normalized traversals are rejected",
    {
      segments: fc.array(fc.constantFrom("alpha", "beta", "gamma", "delta"), {
        maxLength: 8,
      }),
      outsideSegment: fc.constantFrom("outside", "other", "escape"),
    },
    ({ segments, outsideSegment }) => {
      const root = "/workspace-root";
      expect(isWithin(root, join(root, ...segments))).toBe(true);
      expect(
        isWithin(root, join(root, "safe", "..", "..", outsideSegment)),
      ).toBe(false);
      expect(isWithin(root, join(root, "..", outsideSegment))).toBe(false);
    },
  );

  it.prop(
    "workspace identity is deterministic, fixed-width, and collision-resistant for generated session IDs",
    {
      first: fc.uuid(),
      second: fc.uuid(),
    },
    ({ first, second }) => {
      const firstKey = safeSessionKey(first);
      expect(safeSessionKey(first)).toBe(firstKey);
      expect(firstKey).toMatch(/^[a-f0-9]{32}$/u);
      if (first !== second) expect(safeSessionKey(second)).not.toBe(firstKey);
    },
  );
});
