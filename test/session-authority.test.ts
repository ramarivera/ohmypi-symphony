import { mkdir, writeFile } from "node:fs/promises";
import { describe, expect, it } from "@effect/vitest";
import {
  ConfigProvider,
  Context,
  Deferred,
  Effect,
  Either,
  Layer,
  Option,
  Schema,
  type Scope,
} from "effect";
import {
  DatabaseError,
  GitHubAppApiError,
  type GitHubAppConfigurationError,
  type GitHubAppRemoteError,
  InterruptedRunNoActionableInputError,
} from "../src/domain/errors.js";
import {
  AppUserId,
  InputId,
  OrganizationId,
  SessionId,
  SourceKey,
  WorkspaceId,
} from "../src/domain/ids.js";
import type { Installation } from "../src/domain/models.js";
import { GatewayConfig } from "../src/services/config.js";
import {
  type CreatedPullRequest,
  GitHubApp,
  type PublishPullRequestInput,
} from "../src/services/github-app.js";
import { ActivityProjector } from "../src/services/projector.js";
import {
  type RpcEvent,
  RpcWorker,
  type RpcWorkerHandle,
} from "../src/services/rpc-worker.js";
import { SessionAuthority } from "../src/services/session-authority.js";
import {
  InstallationRepo,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
  WorkspaceRepo,
} from "../src/services/store/repositories.js";
import {
  SqliteClient,
  SqliteClientLive,
  type SqliteClientShape,
} from "../src/services/store/sqlite-client.js";
import { TokenCrypto } from "../src/services/token-crypto.js";
import {
  safeSessionKey,
  workspaceBranchName,
} from "../src/services/workspace.js";

describe("SessionAuthority behavior invariants", () => {
  it.effect.prop(
    "persists each worker event once with distinct sequence keys",
    {
      sessionId: Schema.UUID,
      type: Schema.Literal(
        "agent_start",
        "turn_start",
        "turn_end",
        "tool_execution_start",
        "tool_execution_end",
        "message_end",
        "prompt_result",
      ),
    },
    ({ sessionId, type }) =>
      withAuthority(() =>
        Effect.gen(function* () {
          const sid = Schema.decodeUnknownSync(SessionId)(sessionId);
          const organizationId = Schema.decodeUnknownSync(OrganizationId)(
            `organization-${sessionId}`,
          );
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          const installationRepo = yield* InstallationRepo;
          const runEventRepo = yield* RunEventRepo;
          yield* runRepo.create({
            sessionId: sid,
            organizationId,
            issueId: Option.none(),
          });
          yield* installationRepo.put(install(organizationId));
          yield* runRepo.update(sid, {
            state: "orphaned",
            workspacePath: Option.some("/tmp"),
            ompSessionFile: Option.some("/tmp/session.jsonl"),
          });
          yield* authority.processSession(sid);

          const projected = yield* Deferred.make<void>();
          projectionWaiter = projected;
          projectionExpected = 2;
          const event: RpcEvent = { type };
          yield* Effect.sync(() => {
            if (workerEventListener === undefined)
              throw new Error("worker event listener was not registered");
            workerEventListener(event);
            workerEventListener(event);
          });
          yield* Deferred.await(projected);

          const persisted = (yield* runEventRepo.list(sid)).filter(
            (item) =>
              typeof item.payload === "object" &&
              item.payload !== null &&
              "type" in item.payload &&
              item.payload.type === type,
          );
          expect(persisted).toHaveLength(2);
          expect(new Set(persisted.map((item) => item.sourceKey)).size).toBe(2);
          expect(persisted.map((item) => item.payload)).toEqual([event, event]);
        }),
      ),
    { timeout: 15_000, fastCheck: { numRuns: 20 } },
  );

  it.effect.prop(
    "records UI requests and transitions the run to waiting",
    {
      sessionId: Schema.UUID,
      requestId: Schema.String.pipe(Schema.minLength(1)),
    },
    ({ sessionId, requestId }) =>
      withAuthority(() =>
        Effect.gen(function* () {
          const sid = Schema.decodeUnknownSync(SessionId)(sessionId);
          const organizationId = Schema.decodeUnknownSync(OrganizationId)(
            `organization-${sessionId}`,
          );
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          const installationRepo = yield* InstallationRepo;
          const runEventRepo = yield* RunEventRepo;
          yield* runRepo.create({
            sessionId: sid,
            organizationId,
            issueId: Option.none(),
          });
          yield* installationRepo.put(install(organizationId));
          yield* runRepo.update(sid, {
            state: "orphaned",
            workspacePath: Option.some("/tmp"),
            ompSessionFile: Option.some("/tmp/session.jsonl"),
          });
          yield* authority.processSession(sid);

          const elicitationObserved = yield* Deferred.make<void>();
          elicitationWaiter = elicitationObserved;
          const event: RpcEvent = {
            type: "extension_ui_request",
            id: requestId,
            method: "confirm",
            title: "Continue?",
            message: "Proceed with the task?",
            options: ["yes", "no"],
          };
          yield* Effect.sync(() => {
            if (workerEventListener === undefined)
              throw new Error("worker event listener was not registered");
            workerEventListener(event);
          });
          yield* Deferred.await(elicitationObserved);

          const run = yield* runRepo.get(sid);
          const persisted = yield* runEventRepo.list(sid);
          const uiEvent = persisted.find(
            (item) =>
              typeof item.payload === "object" &&
              item.payload !== null &&
              "type" in item.payload &&
              item.payload.type === "extension_ui_request",
          );
          expect(Option.isSome(run)).toBe(true);
          if (Option.isSome(run)) expect(run.value.state).toBe("waiting");
          expect(uiEvent).toMatchObject({
            sourceKey: `rpc-ui:${requestId}`,
            kind: "extension_ui_request",
            level: "warn",
            payload: event,
          });
          expect(projectorElicitations).toContainEqual({
            sessionId: sid,
            sourceKey: `rpc-ui:${requestId}`,
            text: "Continue?\n\nProceed with the task?",
            options: ["yes", "no"],
          });
        }),
      ),
    { timeout: 15_000, fastCheck: { numRuns: 20 } },
  );

  it.effect.prop(
    "round-trips generated worker events through event and projector layers",
    {
      sessionId: Schema.UUID,
      type: Schema.Literal(
        "agent_start",
        "turn_start",
        "turn_end",
        "agent_end",
        "tool_execution_start",
        "message_end",
        "prompt_result",
        "progress",
      ),
    },
    ({ sessionId, type }) =>
      withAuthority(() =>
        Effect.gen(function* () {
          const sid = Schema.decodeUnknownSync(SessionId)(sessionId);
          const organizationId = Schema.decodeUnknownSync(OrganizationId)(
            `organization-${sessionId}`,
          );
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          const installationRepo = yield* InstallationRepo;
          const runEventRepo = yield* RunEventRepo;
          yield* runRepo.create({
            sessionId: sid,
            organizationId,
            issueId: Option.none(),
          });
          yield* installationRepo.put(install(organizationId));
          yield* runRepo.update(sid, {
            state: "orphaned",
            workspacePath: Option.some("/tmp"),
            ompSessionFile: Option.some("/tmp/session.jsonl"),
          });
          yield* authority.processSession(sid);

          const projected = yield* Deferred.make<void>();
          projectionWaiter = projected;
          projectionExpected = 1;
          const event: RpcEvent = { type };
          yield* Effect.sync(() => {
            if (workerEventListener === undefined)
              throw new Error("worker event listener was not registered");
            workerEventListener(event);
          });
          yield* Deferred.await(projected);

          const persisted = (yield* runEventRepo.list(sid)).filter(
            (item) =>
              typeof item.payload === "object" &&
              item.payload !== null &&
              "type" in item.payload &&
              item.payload.type === type,
          );
          expect(persisted).toHaveLength(1);
          expect(persisted[0]?.payload).toEqual(event);
          expect(persisted[0]?.sessionId).toBe(sid);
          expect(persisted[0]?.level).toMatch(
            /^(debug|info|warn|result|error)$/,
          );
          expect(projectorEvents).toContainEqual({
            sessionId: sid,
            sequence: expect.any(Number),
            event,
          });
        }),
      ),
    { timeout: 15_000, fastCheck: { numRuns: 20 } },
  );

  it.effect.prop(
    "persists cancellation outcomes for both stop and canceled desired state",
    {
      sessionId: Schema.UUID,
      desiredState: Schema.Literal("running", "canceled"),
      inputKind: Schema.Literal("prompted", "stop"),
    },
    ({ sessionId, desiredState, inputKind }) =>
      withAuthority((db) =>
        Effect.gen(function* () {
          const sid = Schema.decodeUnknownSync(SessionId)(sessionId);
          const organizationId = Schema.decodeUnknownSync(OrganizationId)(
            `organization-${sessionId}`,
          );
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          const runInputRepo = yield* RunInputRepo;
          const runEventRepo = yield* RunEventRepo;
          const installationRepo = yield* InstallationRepo;
          const inputId = Schema.decodeUnknownSync(InputId)(
            `input-${sessionId}`,
          );
          yield* runRepo.create({
            sessionId: sid,
            organizationId,
            issueId: Option.none(),
          });
          yield* installationRepo.put(install(organizationId));
          yield* runInputRepo.enqueue({
            id: inputId,
            sessionId: sid,
            kind: inputKind,
            body: inputKind === "stop" ? "stop" : "cancel this run",
            payload: { source: "property" },
          });
          if (desiredState === "canceled" && inputKind === "prompted") {
            yield* Effect.sync(() =>
              db
                .query(
                  "UPDATE agent_run SET desired_state='canceled' WHERE session_id=?",
                )
                .run(sid),
            );
          }
          yield* authority.processSession(sid);

          const run = yield* runRepo.get(sid);
          const persisted = yield* runEventRepo.list(sid);
          const shouldCancel =
            desiredState === "canceled" || inputKind === "stop";
          expect(Option.isSome(run)).toBe(true);
          if (Option.isSome(run)) {
            expect(run.value.state).toBe(shouldCancel ? "canceled" : "waiting");
            expect(run.value.desiredState).toBe(
              shouldCancel ? "canceled" : "running",
            );
          }
          expect(
            persisted.some(
              (item) =>
                item.kind === "state" &&
                typeof item.payload === "object" &&
                item.payload !== null &&
                "to" in item.payload &&
                item.payload.to === (shouldCancel ? "canceled" : "waiting"),
            ),
          ).toBe(true);
          if (shouldCancel) {
            expect(projectorTerminals).toContainEqual({
              sessionId: sid,
              sourceKey: `stop:${sid}`,
              kind: "response",
              text: "Stopped as requested.",
            });
          }
        }),
      ),
    { timeout: 15_000, fastCheck: { numRuns: 20 } },
  );
});

const testConfigProvider = ConfigProvider.fromMap(
  new Map([
    ["LINEAR_CLIENT_ID", "test-client"],
    ["LINEAR_CLIENT_SECRET", "test-secret"],
    ["LINEAR_WEBHOOK_SECRET", "test-webhook-secret"],
    ["LINEAR_ALLOWED_ORGANIZATION_IDS", "authority-organization"],
    ["TOKEN_ENCRYPTION_KEY", "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc="],
    ["PUBLIC_URL", "http://localhost:3000"],
    ["WORKSPACE_ROOT", "/tmp/authority-tests"],
  ]),
);
const TEST_WORKSPACE_PATH = `/tmp/authority-tests/${safeSessionKey("authority-session")}`;
const prepareMaterializedWorkspace = Effect.tryPromise(async () => {
  await mkdir(`${TEST_WORKSPACE_PATH}/.git`, { recursive: true });
  await writeFile(
    `${TEST_WORKSPACE_PATH}/.git/linear-gateway-base.json`,
    JSON.stringify({
      repositoryId: "authority-repository",
      repositoryUrl: "git@github.com:octo/example.git",
      base: "main",
      baseCommit: "0000000000000000000000000000000000000000",
    }),
  );
  await writeFile(
    `${TEST_WORKSPACE_PATH}/.linear-gateway-workspace.json`,
    JSON.stringify({
      repositoryId: "authority-repository",
      url: "git@github.com:octo/example.git",
      ref: "main",
    }),
  );
});
let workerEventListener: ((event: RpcEvent) => void) | undefined;
let projectionWaiter: Deferred.Deferred<void, never> | undefined;
let terminalWaiter: Deferred.Deferred<void, never> | undefined;
let projectionExpected = 0;
const projectorEvents: Array<{
  readonly sessionId: string;
  readonly sequence: number;
  readonly event: RpcEvent;
}> = [];
const projectorElicitations: Array<{
  readonly sessionId: string;
  readonly sourceKey: string;
  readonly text: string;
  readonly options: ReadonlyArray<string> | undefined;
}> = [];
const projectorTerminals: Array<{
  readonly sessionId: string;
  readonly sourceKey: string;
  readonly kind: string;
  readonly text: string;
}> = [];
let elicitationWaiter: Deferred.Deferred<void, never> | undefined;

const signalProjection = (): void => {
  if (
    projectionWaiter !== undefined &&
    projectorEvents.length >= projectionExpected
  ) {
    Deferred.unsafeDone(projectionWaiter, Effect.void);
    projectionWaiter = undefined;
  }
};

const signalElicitation = (): void => {
  if (elicitationWaiter !== undefined) {
    Deferred.unsafeDone(elicitationWaiter, Effect.void);
    elicitationWaiter = undefined;
  }
};

let terminalFailure: DatabaseError | undefined;
const projectorExternalUrls: Array<{
  readonly sessionId: string;
  readonly sourceKey: string;
  readonly urls: ReadonlyArray<{
    readonly label: string;
    readonly url: string;
  }>;
}> = [];
const mockProjector = ActivityProjector.make({
  thought: () => Effect.succeed(true),
  elicitation: (sessionId, sourceKey, text, options) =>
    Effect.sync(() => {
      projectorElicitations.push({ sessionId, sourceKey, text, options });
      signalElicitation();
      return true;
    }),
  terminal: (sessionId, sourceKey, kind, text) =>
    Effect.gen(function* () {
      projectorTerminals.push({ sessionId, sourceKey, kind, text });
      if (terminalWaiter !== undefined) {
        Deferred.unsafeDone(terminalWaiter, Effect.void);
        terminalWaiter = undefined;
      }
      if (terminalFailure !== undefined) yield* Effect.fail(terminalFailure);
      return true;
    }),
  plan: () => Effect.succeed(true),
  externalUrls: (sessionId, sourceKey, urls) =>
    Effect.sync(() => {
      projectorExternalUrls.push({ sessionId, sourceKey, urls });
      return true;
    }),
  flushPending: () => Effect.succeed(0),
  projectRpcEvent: (sessionId, sequence, event) =>
    Effect.sync(() => {
      projectorEvents.push({ sessionId, sequence, event: event as RpcEvent });
      signalProjection();
    }),
});

const mockWorker: RpcWorkerHandle = {
  sessionId: Effect.succeed(Option.none()),
  sessionFile: Effect.succeed(Option.none()),
  isStreaming: Effect.succeed(false),
  start: () => Effect.void,
  stop: () => Effect.void,
  prompt: () => Effect.succeed(true),
  steer: () => Effect.void,
  followUp: () => Effect.void,
  abort: () => Effect.void,
  getState: () => Effect.succeed({}),
  respondToUi: () => Effect.void,
  onEvent: (listener) =>
    Effect.sync(() => {
      workerEventListener = listener;
      return () =>
        Effect.sync(() => {
          if (workerEventListener === listener) workerEventListener = undefined;
        });
    }),
};

const mockRpcWorker = RpcWorker.make({
  spawn: () => Effect.succeed(mockWorker),
});
const makeGitHubApp = (
  enabled: boolean,
  publish: (
    input: PublishPullRequestInput,
  ) => Effect.Effect<
    CreatedPullRequest | undefined,
    GitHubAppConfigurationError | GitHubAppRemoteError | GitHubAppApiError
  >,
) =>
  GitHubApp.make({
    enabled,
    isEnabled: () => enabled,
    createPullRequest: () =>
      Effect.succeed({ url: "https://github.com/example/pull/1", number: 1 }),
    publishPullRequest: publish,
  });

const mockGitHubApp = makeGitHubApp(false, () => Effect.succeed(undefined));
const withAuthority = <A, E>(
  effect: (
    db: SqliteClientShape["db"],
  ) => Effect.Effect<
    A,
    E,
    | SessionAuthority
    | RunRepo
    | RunInputRepo
    | RunEventRepo
    | InstallationRepo
    | WorkspaceRepo
    | Scope.Scope
  >,
  githubApp: GitHubApp = mockGitHubApp,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      terminalFailure = undefined;
      workerEventListener = undefined;
      terminalWaiter = undefined;
      projectionWaiter = undefined;
      elicitationWaiter = undefined;
      projectionExpected = 0;
      projectorEvents.length = 0;
      projectorElicitations.length = 0;
      projectorTerminals.length = 0;
      projectorExternalUrls.length = 0;
      const sqliteContext = yield* Layer.build(SqliteClientLive(":memory:"));
      const sqlite = Context.get(sqliteContext, SqliteClient);
      const dependencies = Layer.mergeAll(
        GatewayConfig.Default,
        TokenCrypto.Default,
        InstallationRepo.Default,
        RunEventRepo.Default,
        RunInputRepo.Default,
        Layer.succeed(GitHubApp, githubApp),
        RunRepo.Default,
        WorkspaceRepo.Default,
        Layer.succeed(ActivityProjector, mockProjector),
        Layer.succeed(RpcWorker, mockRpcWorker),
      ).pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(SqliteClient, sqlite),
            Layer.setConfigProvider(testConfigProvider),
          ),
        ),
      );
      const authority = SessionAuthority.DefaultWithoutDependencies.pipe(
        Layer.provide(dependencies),
      );
      return yield* effect(sqlite.db).pipe(
        Effect.provide(Layer.mergeAll(dependencies, authority)),
      );
    }),
  );

const testSessionId = Schema.decodeUnknownSync(SessionId)("authority-session");
const testOrganizationId = Schema.decodeUnknownSync(OrganizationId)(
  "authority-organization",
);
const install = (organizationId: OrganizationId): Installation => ({
  organizationId,
  appUserId: Schema.decodeUnknownSync(AppUserId)("authority-app-user"),
  accessToken: "access-token",
  refreshToken: "refresh-token",
  expiresAt: Date.now() + 3_600_000,
  scopes: ["read"],
  revokedAt: Option.none(),
  accessibleTeamIds: Option.none(),
  canAccessAllPublicTeams: Option.none(),
});

describe("SessionAuthority GitHub publication", () => {
  it.scopedLive(
    "publishes before succeeding and projects the PR URL",
    () => {
      const published: PublishPullRequestInput[] = [];
      const githubApp = makeGitHubApp(true, (input) =>
        Effect.sync(() => {
          published.push(input);
          return {
            url: "https://github.com/octo/example/pull/7",
            number: 7,
          };
        }),
      );
      return withAuthority(
        () =>
          Effect.gen(function* () {
            const authority = yield* SessionAuthority;
            const runRepo = yield* RunRepo;
            const installationRepo = yield* InstallationRepo;
            const workspaceRepo = yield* WorkspaceRepo;
            const repositoryId = Schema.decodeUnknownSync(WorkspaceId)(
              "authority-repository",
            );

            yield* installationRepo.put(install(testOrganizationId));
            yield* workspaceRepo.createRepository({
              organizationId: testOrganizationId,
              id: repositoryId,
              url: "git@github.com:octo/example.git",
              ref: "main",
            });
            yield* runRepo.create({
              sessionId: testSessionId,
              organizationId: testOrganizationId,
              issueId: Option.none(),
            });
            yield* runRepo.update(testSessionId, {
              state: "orphaned",
              repositoryId: Option.some(repositoryId),
              workspacePath: Option.some(TEST_WORKSPACE_PATH),
              ompSessionFile: Option.some("/tmp/authority-session.jsonl"),
            });
            yield* prepareMaterializedWorkspace;
            yield* authority.processSession(testSessionId);
            yield* workspaceRepo.updateRepository(
              testOrganizationId,
              repositoryId,
              {
                url: "https://example.invalid/replaced.git",
                ref: "release-tag",
              },
            );

            const projected = yield* Deferred.make<void>();
            projectionWaiter = projected;
            projectionExpected = 1;
            yield* Effect.sync(() => {
              if (workerEventListener === undefined)
                throw new Error("worker event listener was not registered");
              workerEventListener({ type: "agent_end" });
            });
            yield* Deferred.await(projected);

            const run = yield* runRepo.get(testSessionId);
            expect(Option.isSome(run) && run.value.state).toBe("succeeded");
            expect(published[0]).toMatchObject({
              repositoryUrl: "git@github.com:octo/example.git",
              base: "main",
              branch: workspaceBranchName(testSessionId),
            });
            expect(projectorExternalUrls).toContainEqual({
              sessionId: testSessionId,
              sourceKey: `github-pr:${testSessionId}`,
              urls: [
                {
                  label: "GitHub pull request",
                  url: "https://github.com/octo/example/pull/7",
                },
              ],
            });
          }),
        githubApp,
      );
    },
    15_000,
  );

  it.scopedLive(
    "marks the run failed when GitHub publication fails",
    () => {
      let publishCalls = 0;
      const githubApp = makeGitHubApp(true, () => {
        publishCalls += 1;
        return Effect.fail(
          new GitHubAppApiError({
            message: "GitHub pull request creation failed",
            operation: "pull request creation",
          }),
        );
      });
      return withAuthority(
        () =>
          Effect.gen(function* () {
            const authority = yield* SessionAuthority;
            const runRepo = yield* RunRepo;
            const installationRepo = yield* InstallationRepo;
            const workspaceRepo = yield* WorkspaceRepo;
            const repositoryId = Schema.decodeUnknownSync(WorkspaceId)(
              "authority-repository",
            );

            yield* installationRepo.put(install(testOrganizationId));
            yield* workspaceRepo.createRepository({
              organizationId: testOrganizationId,
              id: repositoryId,
              url: "https://github.com/octo/example",
              ref: "main",
            });
            yield* runRepo.create({
              sessionId: testSessionId,
              organizationId: testOrganizationId,
              issueId: Option.none(),
            });
            for (let attempt = 0; attempt < 6; attempt += 1) {
              yield* runRepo.update(testSessionId, { incrementAttempt: true });
            }
            yield* runRepo.update(testSessionId, {
              state: "orphaned",
              repositoryId: Option.some(repositoryId),
              workspacePath: Option.some(TEST_WORKSPACE_PATH),
              ompSessionFile: Option.some("/tmp/authority-session.jsonl"),
            });
            yield* prepareMaterializedWorkspace;
            yield* authority.processSession(testSessionId);

            const terminal = yield* Deferred.make<void>();
            terminalWaiter = terminal;
            yield* Effect.sync(() => {
              if (workerEventListener === undefined)
                throw new Error("worker event listener was not registered");
              workerEventListener({ type: "agent_end" });
            });
            yield* Deferred.await(terminal);

            expect(publishCalls).toBe(1);
            const run = yield* runRepo.get(testSessionId);
            expect(Option.isSome(run) && run.value.state).toBe("failed");
          }),
        githubApp,
      );
    },
    15_000,
  );
});

describe("SessionAuthority infrastructure failures", () => {
  it.scopedLive(
    "keeps a run lookup database failure distinct from an absent run",
    () =>
      withAuthority((db) =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          yield* Effect.sync(() => db.exec("DROP TABLE agent_run"));

          const result = yield* Effect.either(
            authority.processSession(testSessionId),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left._tag).toBe("@Gateway/DatabaseError");
          }
        }),
      ),
  );

  it.scopedLive(
    "surfaces terminal state persistence failure instead of reporting terminal handling",
    () =>
      withAuthority((db) =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* Effect.sync(() =>
            db.exec(
              "CREATE TRIGGER reject_terminal_update BEFORE UPDATE ON agent_run WHEN NEW.state = 'failed' BEGIN SELECT RAISE(FAIL, 'terminal persistence failed'); END",
            ),
          );

          const result = yield* Effect.either(
            authority.processSession(testSessionId),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left._tag).toBe("@Gateway/DatabaseError");
          }
        }),
      ),
  );

  it.scopedLive(
    "propagates projection persistence failure after terminal state persistence",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          terminalFailure = new DatabaseError({ message: "projection failed" });

          const result = yield* Effect.either(
            authority.processSession(testSessionId),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left._tag).toBe("@Gateway/DatabaseError");
          }
        }),
      ),
  );

  it.scopedLive(
    "returns a typed error for an interrupted run without actionable input",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          yield* installationRepo.put(install(testOrganizationId));
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            workspacePath: Option.some(
              "/Volumes/ExtSSD/SCRATCHPADS_FOR_AGENTS/authority-tests",
            ),
          });

          const result = yield* Effect.either(
            authority.processSession(testSessionId),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left).toBeInstanceOf(
              InterruptedRunNoActionableInputError,
            );
          }
        }),
      ),
  );

  it.scopedLive("surfaces a run-event persistence failure", () =>
    withAuthority((db) =>
      Effect.gen(function* () {
        const runEventRepo = yield* RunEventRepo;
        const runRepo = yield* RunRepo;
        yield* runRepo.create({
          sessionId: testSessionId,
          organizationId: testOrganizationId,
          issueId: Option.none(),
        });
        yield* Effect.sync(() =>
          db.exec(
            "CREATE TRIGGER reject_run_event BEFORE INSERT ON run_event BEGIN SELECT RAISE(FAIL, 'event persistence failed'); END",
          ),
        );

        const result = yield* Effect.either(
          runEventRepo.upsert({
            sourceKey: Schema.decodeUnknownSync(SourceKey)("authority-event"),
            sessionId: testSessionId,
            kind: "agent",
            level: "info",
            text: "event",
            payload: {},
            status: "observed",
            now: 0,
          }),
        );
        expect(Either.isLeft(result)).toBe(true);
        if (Either.isLeft(result)) {
          expect(result.left._tag).toBe("@Gateway/DatabaseError");
        }
      }),
    ),
  );

  it.scopedLive(
    "keeps the primary projection failure when lease cleanup fails",
    () =>
      withAuthority((db) =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* Effect.sync(() =>
            db.exec(
              "CREATE TRIGGER reject_lease_release BEFORE UPDATE ON agent_run WHEN NEW.lease_owner IS NULL BEGIN SELECT RAISE(FAIL, 'lease release failed'); END",
            ),
          );
          terminalFailure = new DatabaseError({
            message: "primary projection failure",
          });

          const result = yield* Effect.either(
            authority.processSession(testSessionId),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left.message).toBe("primary projection failure");
          }
        }),
      ),
  );
});
