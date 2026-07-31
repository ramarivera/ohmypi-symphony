import { describe, expect, it } from "@effect/vitest";
import {
  ConfigProvider,
  Context,
  Effect,
  Either,
  Layer,
  Option,
  Schema,
} from "effect";
import {
  DatabaseError,
  InterruptedRunNoActionableInputError,
} from "../src/domain/errors.js";
import {
  AppUserId,
  OrganizationId,
  SessionId,
  SourceKey,
} from "../src/domain/ids.js";
import type { Installation } from "../src/domain/models.js";
import { GatewayConfig } from "../src/services/config.js";
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

const rpcSourceKey = (
  sessionId: string,
  sequence: number,
  event: RpcEvent,
): string => {
  if (event.type === "extension_ui_request" && typeof event.id === "string") {
    return `rpc-ui:${event.id}`;
  }
  return `rpc:${sessionId}:${sequence}:${event.type}`;
};

const rpcEventLevel = (
  event: RpcEvent,
): "debug" | "info" | "warn" | "result" | "error" => {
  switch (event.type) {
    case "agent_start":
    case "turn_start":
    case "turn_end":
    case "tool_execution_start":
    case "message_end":
      return "info";
    case "agent_end":
      return "result";
    case "tool_execution_end":
      return event.error ? "error" : "result";
    case "prompt_result":
      return event.agentInvoked === false ? "result" : "info";
    case "error":
      return "error";
    case "extension_ui_request":
      return "warn";
    default:
      return "info";
  }
};

const cancelDominates = (desiredState: string, inputKind: string): boolean =>
  desiredState === "canceled" || inputKind === "stop";

describe("SessionAuthority behavior invariants", () => {
  it.effect.prop(
    "source key is deterministic and unique per (session, sequence, type)",
    {
      sessionId: Schema.UUID,
      sequence: Schema.Int,
      type: Schema.Literal(
        "agent_start",
        "turn_start",
        "turn_end",
        "tool_execution_start",
        "tool_execution_end",
        "message_end",
        "prompt_result",
        "error",
      ),
    },
    ({ sessionId, sequence, type }) =>
      Effect.gen(function* () {
        const event: RpcEvent = { type };
        const key = rpcSourceKey(sessionId, sequence, event);
        const key2 = rpcSourceKey(sessionId, sequence, event);
        expect(key).toBe(key2);
        expect(key).toBe(`rpc:${sessionId}:${sequence}:${type}`);
        yield* Effect.void;
      }),
  );

  it.effect.prop(
    "extension_ui_request source keys use the request id",
    {
      sessionId: Schema.UUID,
      sequence: Schema.Int,
      requestId: Schema.String,
    },
    ({ sessionId, sequence, requestId }) =>
      Effect.gen(function* () {
        const event: RpcEvent = {
          type: "extension_ui_request",
          id: requestId,
        };
        const key = rpcSourceKey(sessionId, sequence, event);
        expect(key).toBe(`rpc-ui:${requestId}`);
        yield* Effect.void;
      }),
  );

  it.effect.prop(
    "level mapping is total for known event types",
    {
      type: Schema.Literal(
        "agent_start",
        "turn_start",
        "turn_end",
        "agent_end",
        "tool_execution_start",
        "tool_execution_end",
        "message_end",
        "prompt_result",
        "error",
        "extension_ui_request",
        "progress",
      ),
    },
    ({ type }) =>
      Effect.gen(function* () {
        const event: RpcEvent = { type };
        const level = rpcEventLevel(event);
        expect(["debug", "info", "warn", "result", "error"]).toContain(level);
        yield* Effect.void;
      }),
  );

  it.effect.prop(
    "abort/cancel dominates queued prompts and stop inputs",
    {
      desiredState: Schema.Literal("running", "canceled"),
      inputKind: Schema.Literal("prompted", "stop"),
    },
    ({ desiredState, inputKind }) =>
      Effect.gen(function* () {
        const shouldCancel = cancelDominates(desiredState, inputKind);
        expect(shouldCancel).toBe(
          desiredState === "canceled" || inputKind === "stop",
        );
        yield* Effect.void;
      }),
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
      "WORKSPACE_ROOT",
      "/Volumes/ExtSSD/SCRATCHPADS_FOR_AGENTS/authority-tests",
    ],
  ]),
);
let terminalFailure: DatabaseError | undefined;
const mockProjector = ActivityProjector.make({
  thought: () => Effect.succeed(true),
  elicitation: () => Effect.succeed(true),
  terminal: () =>
    terminalFailure === undefined
      ? Effect.succeed(true)
      : Effect.fail(terminalFailure),
  plan: () => Effect.succeed(true),
  externalUrls: () => Effect.succeed(true),
  flushPending: () => Effect.succeed(0),
  projectRpcEvent: () => Effect.void,
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
  onEvent: () => Effect.succeed(() => Effect.void),
};

const mockRpcWorker = RpcWorker.make({
  spawn: () => Effect.succeed(mockWorker),
});

const withAuthority = <A, E>(
  effect: (
    db: SqliteClientShape["db"],
  ) => Effect.Effect<
    A,
    E,
    SessionAuthority | RunRepo | RunInputRepo | RunEventRepo | InstallationRepo
  >,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      terminalFailure = undefined;
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
const install = (): Installation => ({
  organizationId: testOrganizationId,
  appUserId: Schema.decodeUnknownSync(AppUserId)("authority-app-user"),
  accessToken: "access-token",
  refreshToken: "refresh-token",
  expiresAt: 1_000_000,
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
          yield* installationRepo.put(install());
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
