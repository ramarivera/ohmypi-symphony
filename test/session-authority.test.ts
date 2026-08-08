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
} from "effect";
import {
  DatabaseError,
  InterruptedRunNoActionableInputError,
  NixEnvironmentError,
} from "../src/domain/errors.js";
import {
  AppUserId,
  InputId,
  OrganizationId,
  SessionId,
  SourceKey,
  WorkspaceId,
} from "../src/domain/ids.js";
import { type Installation, NixPackageName } from "../src/domain/models.js";
import { GatewayConfig } from "../src/services/config.js";
import { NixEnvironment } from "../src/services/nix-environment.js";
import { ActivityProjector } from "../src/services/projector.js";
import {
  type RpcEvent,
  RpcWorker,
  type RpcWorkerHandle,
} from "../src/services/rpc-worker.js";
import {
  deviationFromRpcEvent,
  linearWorkerPrompt,
  resolveDeviationExtensionPath,
  SessionAuthority,
} from "../src/services/session-authority.js";
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

describe("Linear worker prompt contract", () => {
  it("adds the Linear activity contract to a created session", () => {
    const prompt = linearWorkerPrompt("created", "Fix the webhook race");

    expect(prompt).toContain("Use OMP todos");
    expect(prompt).toContain("Linear agent plan");
    expect(prompt).toContain("Linear elicitations");
    expect(prompt).toContain("thought and action activities");
    expect(prompt).toContain("include relevant artifact URLs");
    expect(prompt).toContain("rromp_report_deviation");
    expect(prompt).toContain("cease work immediately");
    expect(prompt.endsWith("Linear task:\nFix the webhook race")).toBe(true);
  });

  it.each(["prompted", "stop"] as const)(
    "leaves %s input unchanged",
    (kind) => {
      expect(linearWorkerPrompt(kind, "Continue from here")).toBe(
        "Continue from here",
      );
    },
  );

  it("extracts only valid deviation tool calls", () => {
    expect(
      deviationFromRpcEvent({
        type: "tool_execution_start",
        toolName: "rromp_report_deviation",
        args: { deviation: "  Used a narrower migration scope.  " },
      }),
    ).toBe("Used a narrower migration scope.");
    expect(
      deviationFromRpcEvent({
        type: "tool_execution_start",
        toolName: "read",
        args: { deviation: "not a report" },
      }),
    ).toBeNull();
  });

  it("resolves the bundled deviation extension entrypoint", () => {
    expect(resolveDeviationExtensionPath()).not.toBeNull();
  });
});

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
    { timeout: 15_000, fastCheck: { numRuns: 10 } },
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

  it.effect(
    "resumes a user-stopped run when a follow-up prompt arrives",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          const runInputRepo = yield* RunInputRepo;
          const installationRepo = yield* InstallationRepo;
          const workspaceRepo = yield* WorkspaceRepo;
          const repositoryId =
            Schema.decodeUnknownSync(WorkspaceId)("resume-repository");
          yield* installationRepo.put(install(testOrganizationId));
          yield* workspaceRepo.createRepository({
            organizationId: testOrganizationId,
            id: repositoryId,
            url: "https://example.com/repository.git",
            ref: "main",
            nixPackages: [],
          });
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* runRepo.update(testSessionId, {
            state: "running",
            repositoryId: Option.some(repositoryId),
            workspacePath: Option.some("/tmp/resume-workspace"),
            ompSessionFile: Option.some("/tmp/resume-workspace/session.jsonl"),
          });

          yield* runInputRepo.enqueue({
            id: Schema.decodeUnknownSync(InputId)("stop-input"),
            sessionId: testSessionId,
            kind: "stop",
            body: "stop",
            payload: {},
          });
          yield* authority.processSession(testSessionId);

          const stopped = yield* runRepo.get(testSessionId);
          expect(Option.isSome(stopped)).toBe(true);
          if (Option.isSome(stopped)) {
            expect(stopped.value.state).toBe("canceled");
            expect(stopped.value.desiredState).toBe("canceled");
          }
          expect(projectorTerminals).toContainEqual({
            sessionId: testSessionId,
            sourceKey: `stop:${testSessionId}`,
            kind: "response",
            text: "Stopped as requested.",
          });

          yield* runInputRepo.enqueue({
            id: Schema.decodeUnknownSync(InputId)("resume-input"),
            sessionId: testSessionId,
            kind: "prompted",
            body: "please continue the task",
            payload: {},
          });
          yield* authority.processSession(testSessionId);

          const resumed = yield* runRepo.get(testSessionId);
          expect(Option.isSome(resumed)).toBe(true);
          if (Option.isSome(resumed)) {
            // startWorker marks the run running once the worker accepts the
            // prompt.
            expect(resumed.value.state).toBe("running");
            expect(resumed.value.desiredState).toBe("running");
            expect(resumed.value.terminalReason).toEqual(Option.none());
          }
          expect(workerSpawnInputs).toHaveLength(1);
          expect(workerSpawnInputs[0]?.command).toContain("--session");
          expect(workerSpawnInputs[0]?.command).toContain(
            "/tmp/resume-workspace/session.jsonl",
          );
          expect(workerSpawnInputs[0]?.cwd).toBe("/tmp/resume-workspace");
          expect(workerPrompts).toEqual(["please continue the task"]);
        }),
      ),
    { timeout: 15_000 },
  );
});

const testConfigProvider = ConfigProvider.fromMap(
  new Map([
    ["LINEAR_CLIENT_ID", "test-client"],
    ["LINEAR_CLIENT_SECRET", "test-secret"],
    ["LINEAR_WEBHOOK_SECRET", "test-webhook-secret"],
    ["TOKEN_ENCRYPTION_KEY", "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc="],
    ["PUBLIC_URL", "http://localhost:3000"],
    [
      "NIXPKGS_FLAKE_REF",
      "github:NixOS/nixpkgs/0123456789012345678901234567890123456789",
    ],
    [
      "WORKSPACE_ROOT",
      "/Volumes/ExtSSD/SCRATCHPADS_FOR_AGENTS/authority-tests",
    ],
  ]),
);
let workerEventListener: ((event: RpcEvent) => void) | undefined;
let projectionWaiter: Deferred.Deferred<void, never> | undefined;
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
const mockProjector = ActivityProjector.make({
  thought: () => Effect.succeed(true),
  deviation: () => Effect.succeed(true),
  elicitation: (sessionId, sourceKey, text, options) =>
    Effect.sync(() => {
      projectorElicitations.push({ sessionId, sourceKey, text, options });
      signalElicitation();
      return true;
    }),
  terminal: (sessionId, sourceKey, kind, text) =>
    Effect.gen(function* () {
      projectorTerminals.push({ sessionId, sourceKey, kind, text });
      if (terminalFailure !== undefined) yield* Effect.fail(terminalFailure);
      return true;
    }),
  plan: () => Effect.succeed(true),
  externalUrls: () => Effect.succeed(true),
  flushPending: () => Effect.succeed(0),
  projectRpcEvent: (sessionId, sequence, event) =>
    Effect.sync(() => {
      projectorEvents.push({ sessionId, sequence, event: event as RpcEvent });
      signalProjection();
    }),
});

const workerPrompts: Array<string> = [];
const mockWorker: RpcWorkerHandle = {
  sessionId: Effect.succeed(Option.none()),
  sessionFile: Effect.succeed(Option.none()),
  isStreaming: Effect.succeed(false),
  start: () => Effect.void,
  stop: () => Effect.void,
  prompt: (message) =>
    Effect.sync(() => {
      workerPrompts.push(message);
      return true;
    }),
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
  spawn: (input) =>
    Effect.sync(() => {
      workerSpawnInputs.push(input);
      return mockWorker;
    }),
});
const workerSpawnInputs: Array<{
  readonly command: ReadonlyArray<string>;
  readonly cwd: string;
  readonly env?: Record<string, string | undefined>;
}> = [];
let nixPathEntries: ReadonlyArray<string> = [];
let nixPrepareError: NixEnvironmentError | undefined;
const nixPreparedRepositories: Array<string> = [];
const mockNixEnvironment = NixEnvironment.make({
  prepare: (repository) => {
    nixPreparedRepositories.push(repository.id);
    if (nixPrepareError !== undefined) return Effect.fail(nixPrepareError);
    return Effect.succeed({
      cacheKey: "test-cache",
      storePaths: [],
      pathEntries: nixPathEntries,
      reused: false,
    });
  },
  list: () => Effect.succeed([]),
  prune: () => Effect.succeed(false),
});

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
    | NixEnvironment
  >,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      terminalFailure = undefined;
      workerEventListener = undefined;
      projectionWaiter = undefined;
      elicitationWaiter = undefined;
      projectionExpected = 0;
      projectorEvents.length = 0;
      projectorElicitations.length = 0;
      workerSpawnInputs.length = 0;
      workerPrompts.length = 0;
      nixPreparedRepositories.length = 0;
      nixPathEntries = [];
      nixPrepareError = undefined;
      projectorTerminals.length = 0;
      const sqliteContext = yield* Layer.build(SqliteClientLive(":memory:"));
      const sqlite = Context.get(sqliteContext, SqliteClient);
      const dependencies = Layer.mergeAll(
        GatewayConfig.Default,
        TokenCrypto.Default,
        InstallationRepo.Default,
        RunEventRepo.Default,
        RunInputRepo.Default,
        RunRepo.Default,
        WorkspaceRepo.Default,
        Layer.succeed(ActivityProjector, mockProjector),
        Layer.succeed(RpcWorker, mockRpcWorker),
        Layer.succeed(NixEnvironment, mockNixEnvironment),
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
describe("SessionAuthority Nix environment preparation", () => {
  it.scopedLive(
    "prepares repository dependencies before the first worker and its orphan retry",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          const workspaceRepo = yield* WorkspaceRepo;
          const repositoryId = Schema.decodeUnknownSync(WorkspaceId)(
            "nix-authority-repository",
          );
          yield* installationRepo.put(install(testOrganizationId));
          yield* workspaceRepo.createRepository({
            organizationId: testOrganizationId,
            id: repositoryId,
            url: "https://example.com/repository.git",
            ref: "main",
            nixPackages: [Schema.decodeUnknownSync(NixPackageName)("nodejs")],
          });
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            repositoryId: Option.some(repositoryId),
            workspacePath: Option.some("/tmp/nix-authority"),
            ompSessionFile: Option.some("/tmp/nix-authority/session.jsonl"),
          });
          nixPathEntries = ["/nix/store/test-node/bin"];

          yield* authority.processSession(testSessionId);
          expect(nixPreparedRepositories).toEqual([repositoryId]);
          expect(workerSpawnInputs).toHaveLength(1);
          expect(workerSpawnInputs[0]?.env?.PATH).toBe(
            `/nix/store/test-node/bin${process.env.PATH ? `:${process.env.PATH}` : ""}`,
          );
          expect(workerSpawnInputs[0]?.env?.HOME).toBe(process.env.HOME);
          expect(workerSpawnInputs[0]?.command).toContain("--extension");
          expect(workerSpawnInputs[0]?.command).toContain(
            resolveDeviationExtensionPath(),
          );

          yield* authority.shutdown();
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            ompSessionFile: Option.some("/tmp/nix-authority/session.jsonl"),
          });
          yield* authority.processSession(testSessionId);
          expect(nixPreparedRepositories).toEqual([repositoryId, repositoryId]);
          expect(workerSpawnInputs).toHaveLength(2);
        }),
      ),
  );

  it.scopedLive(
    "keeps the inherited environment unchanged for an empty dependency result",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          const workspaceRepo = yield* WorkspaceRepo;
          const repositoryId = Schema.decodeUnknownSync(WorkspaceId)(
            "nix-empty-dependencies",
          );
          yield* installationRepo.put(install(testOrganizationId));
          yield* workspaceRepo.createRepository({
            organizationId: testOrganizationId,
            id: repositoryId,
            url: "https://example.com/empty.git",
            ref: "main",
            nixPackages: [],
          });
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            repositoryId: Option.some(repositoryId),
            workspacePath: Option.some("/tmp/nix-empty"),
            ompSessionFile: Option.some("/tmp/nix-empty/session.jsonl"),
          });

          yield* authority.processSession(testSessionId);
          expect(nixPreparedRepositories).toEqual([repositoryId]);
          expect(workerSpawnInputs).toHaveLength(1);
          expect(workerSpawnInputs[0]?.env?.PATH).toBe(process.env.PATH);
          expect(workerSpawnInputs[0]?.env?.HOME).toBe(process.env.HOME);
        }),
      ),
  );

  it.scopedLive(
    "records a typed preparation failure without spawning an RPC worker",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          const workspaceRepo = yield* WorkspaceRepo;
          const repositoryId = Schema.decodeUnknownSync(WorkspaceId)(
            "nix-failing-dependencies",
          );
          yield* installationRepo.put(install(testOrganizationId));
          yield* workspaceRepo.createRepository({
            organizationId: testOrganizationId,
            id: repositoryId,
            url: "https://example.com/failing.git",
            ref: "main",
            nixPackages: [Schema.decodeUnknownSync(NixPackageName)("nodejs")],
          });
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            repositoryId: Option.some(repositoryId),
            workspacePath: Option.some("/tmp/nix-failing"),
            ompSessionFile: Option.some("/tmp/nix-failing/session.jsonl"),
          });
          nixPrepareError = new NixEnvironmentError({
            message: "Nix preparation failed",
            reason: "process_failed",
          });

          const result = yield* Effect.either(
            authority.processSession(testSessionId),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left._tag).toBe("@Gateway/NixEnvironmentError");
          }
          expect(workerSpawnInputs).toHaveLength(0);
          const run = yield* runRepo.get(testSessionId);
          expect(Option.isSome(run)).toBe(true);
          if (Option.isSome(run)) {
            expect(run.value.state).toBe("orphaned");
            expect(run.value.terminalReason).toEqual(
              Option.some("Nix preparation failed"),
            );
          }
        }),
      ),
  );
});
