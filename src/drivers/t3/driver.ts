/**
 * T3 Code driver: one Linear agent session = one T3 thread, driven over the
 * WebSocket RPC (orchestration protocol 2).
 *
 * Idempotency: every id the driver sends (thread, command, message) is
 * derived from the session key or the caller's clientRequestId. T3 records a
 * receipt per commandId and replays it for a duplicate, so retrying with the
 * same ids never creates a second thread or message. On a lost response the
 * driver still reconciles through the projection first and adopts the work it
 * finds there (the shell-network lesson) instead of resending blind.
 */
import { createHash } from "node:crypto";
import {
  Duration,
  Effect,
  Exit,
  Option,
  Queue,
  type Redacted,
  Ref,
  Result,
  Schedule,
  Schema,
  Scope,
  Semaphore,
  Stream,
  SubscriptionRef,
  type Take,
} from "effect";
import type { T3Connection, T3DriverConfig } from "../../services/config.js";
import {
  type AgentDriver,
  type AgentEvent,
  type AgentSession,
  type DriverCapabilities,
  DriverError,
  type OpenSessionInput,
  type RequestResponse,
  type RuntimeMode,
  type SessionSnapshot,
  type TurnHandle,
  type TurnInput,
  type TurnOutcome,
  type TurnStatus,
  type WorkspaceSpec,
} from "../driver.js";
import { type RunView, T3EventMapper } from "./mapping.js";
import { ensureT3Project } from "./projects.js";
import {
  connectT3,
  fetchEnvironmentDescriptor,
  fetchWebSocketTicket,
  type T3RpcConnection,
  type T3RpcError,
  tokenFromSource,
} from "./rpc-client.js";
import {
  ACTIVE_RUN_STATUSES,
  DispatchCommand,
  type DispatchMode,
  LaunchThreadInput,
  LaunchThreadResult,
  type ModelSelection,
  ORCHESTRATION_PROTOCOL_VERSION,
  T3_METHODS,
  TERMINAL_RUN_STATUSES,
  type ThreadProjection,
  ThreadProjection as ThreadProjectionSchema,
  ThreadStreamItem,
  type WorkspaceStrategy,
} from "./wire.js";

export interface T3DriverOptions {
  readonly baseUrl: URL;
  readonly token: Effect.Effect<Redacted.Redacted<string>, T3RpcError>;
  readonly projectId?: string;
  readonly modelSelection: ModelSelection;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: "default" | "plan";
  readonly idNamespace: string;
  /** Consecutive failed resubscribes before the event stream gives up. */
  readonly maxReconnectAttempts?: number;
  readonly reconnectBaseDelayMs?: number;
  readonly reconnectMaxDelayMs?: number;
  readonly pingIntervalMs?: number;
  readonly requestTimeoutMs?: number;
  readonly launchTimeoutMs?: number;
  readonly connectTimeoutMs?: number;
}

/** Where one T3 daemon listens and how to get its bearer before each connect. */
export interface T3Endpoint {
  readonly baseUrl: URL;
  readonly token: Effect.Effect<Redacted.Redacted<string>, T3RpcError>;
}

export const remoteT3Endpoint = (
  connection: Extract<T3Connection, { readonly _tag: "Remote" }>,
): T3Endpoint => ({
  baseUrl: connection.baseUrl,
  token: tokenFromSource(connection.token),
});

export const t3DriverOptionsFromConfig = (
  config: T3DriverConfig,
  endpoint: T3Endpoint,
): T3DriverOptions => ({
  baseUrl: endpoint.baseUrl,
  token: endpoint.token,
  ...(config.projectId === undefined ? {} : { projectId: config.projectId }),
  modelSelection: {
    instanceId: config.providerInstanceId,
    model: config.model,
    ...(config.modelOptions.length > 0 ? { options: config.modelOptions } : {}),
  },
  runtimeMode: config.runtimeMode,
  interactionMode: config.interactionMode,
  idNamespace: config.idNamespace,
});

export const T3_CAPABILITIES: DriverCapabilities = {
  steer: true,
  queue: true,
  hostTools: "mcp",
  resumableStream: true,
  approvals: true,
  questions: true,
  workspaces: "driver",
};

const SAFE_KEY = /^[A-Za-z0-9_-]{1,80}$/u;

/** Deterministic T3 ids. Every id is scoped by namespace and thread. */
export const t3Ids = (namespace: string) => {
  const messagePrefix = (threadId: string) =>
    `${namespace}:${threadId}:message:`;
  return {
    threadId: (sessionKey: string): string =>
      `${namespace}-${
        SAFE_KEY.test(sessionKey)
          ? sessionKey
          : createHash("sha256").update(sessionKey).digest("hex").slice(0, 32)
      }`,
    launchCommandId: (threadId: string) => `${namespace}:${threadId}:launch`,
    messageId: (threadId: string, clientRequestId: string) =>
      `${messagePrefix(threadId)}${encodeURIComponent(clientRequestId)}`,
    dispatchCommandId: (threadId: string, clientRequestId: string) =>
      `${namespace}:${threadId}:dispatch:${encodeURIComponent(clientRequestId)}`,
    interruptCommandId: (threadId: string, runId: string) =>
      `${namespace}:${threadId}:interrupt:${runId}`,
    respondCommandId: (threadId: string, requestId: string) =>
      `${namespace}:${threadId}:respond:${requestId}`,
    /** Recovers the clientRequestId from a message this gateway sent. */
    clientRequestIdOf: (threadId: string, messageId: string): string | null => {
      const prefix = messagePrefix(threadId);
      if (!messageId.startsWith(prefix)) return null;
      try {
        return decodeURIComponent(messageId.slice(prefix.length));
      } catch {
        return null;
      }
    },
  };
};

const fromRpcError =
  (operation: string, mutating: boolean) =>
  (error: T3RpcError): DriverError =>
    new DriverError({
      driver: "t3",
      operation,
      reason: error.reason,
      // A rejected command was refused by T3's orchestrator, so nothing
      // committed; any other failure after the frame left may have landed.
      submission:
        mutating && error.sent && error.reason !== "rejected"
          ? "possibly-submitted"
          : "not-submitted",
      message: error.message,
    });

const withSubmission =
  (submission: DriverError["submission"]) =>
  (error: DriverError): DriverError =>
    new DriverError({
      driver: error.driver,
      operation: error.operation,
      reason: error.reason,
      submission,
      message: error.message,
    });

const protocolError = (operation: string, message: string) =>
  new DriverError({
    driver: "t3",
    operation,
    reason: "protocol",
    submission: "not-submitted",
    message,
  });

const isTransient = (error: DriverError): boolean =>
  error.reason === "lost" ||
  error.reason === "timeout" ||
  error.reason === "unavailable";

const workspaceStrategy = (workspace: WorkspaceSpec): WorkspaceStrategy => {
  switch (workspace._tag) {
    case "Directory":
      return { type: "existing_worktree", worktreePath: workspace.path };
    case "ProjectRoot":
      return workspace.branch === undefined
        ? { type: "root" }
        : { type: "root", branch: workspace.branch };
    case "DriverWorktree":
      return workspace.branch === undefined
        ? { type: "worktree", baseRef: workspace.baseRef }
        : {
            type: "worktree",
            baseRef: workspace.baseRef,
            branch: workspace.branch,
          };
  }
};

/** The newest root run that holds the thread (not queued, not finished). */
const activeRun = (projection: ThreadProjection) =>
  projection.runs
    .filter(
      (run) =>
        ACTIVE_RUN_STATUSES.has(run.status) &&
        run.delegatedTaskId === undefined,
    )
    .sort((a, b) => b.ordinal - a.ordinal)[0];

type Located =
  | {
      readonly _tag: "Run";
      readonly turnId: string;
      readonly delivery: TurnHandle["delivery"];
    }
  | { readonly _tag: "Unlinked" };

/** Finds the run T3 linked to our message (its own run, or the steered one). */
const locateTurn = (
  projection: ThreadProjection,
  messageId: string,
): Located | null => {
  const own = projection.runs.find((run) => run.userMessageId === messageId);
  if (own !== undefined) {
    return {
      _tag: "Run",
      turnId: own.id,
      delivery: own.status === "queued" ? "queued" : "started",
    };
  }
  const message = projection.messages.find((entry) => entry.id === messageId);
  if (message === undefined) return null;
  return message.runId === null
    ? { _tag: "Unlinked" }
    : { _tag: "Run", turnId: message.runId, delivery: "steered" };
};

interface OutcomeState {
  readonly runs: ReadonlyMap<string, RunView>;
  readonly failure: DriverError | null;
}

const isTerminal = (status: string): status is TurnStatus =>
  TERMINAL_RUN_STATUSES.has(status);

/** Terminal outcome of `turnId`, following restart continuations. */
const resolveOutcome = (
  state: OutcomeState,
  turnId: string,
): Option.Option<Result.Result<TurnOutcome, DriverError>> => {
  if (state.failure !== null) return Option.some(Result.fail(state.failure));
  const seen = new Set<string>();
  let current = turnId;
  while (!seen.has(current)) {
    seen.add(current);
    const run = state.runs.get(current);
    if (run === undefined || !isTerminal(run.status)) return Option.none();
    const continuation = [...state.runs.values()].find(
      (candidate) => candidate.restartContinuationOfRunId === current,
    );
    if (
      continuation !== undefined &&
      (run.status === "cancelled" || run.status === "interrupted")
    ) {
      current = continuation.id;
      continue;
    }
    return Option.some(
      Result.succeed({
        turnId: run.id,
        status: run.status,
        finalText: run.finalText,
        error:
          run.error ??
          (run.status === "failed"
            ? "The T3 run failed without an error item"
            : null),
      }),
    );
  }
  return Option.none();
};

const decodeProjection = (operation: string) => (value: unknown) =>
  Schema.decodeUnknownEffect(ThreadProjectionSchema)(value).pipe(
    Effect.mapError(() =>
      protocolError(
        operation,
        "T3 thread projection did not match the pinned wire schema",
      ),
    ),
  );

export const makeT3Driver = (options: T3DriverOptions): AgentDriver => {
  const ids = t3Ids(options.idNamespace);
  const maxReconnectAttempts = options.maxReconnectAttempts ?? 8;
  const reconnectBaseDelayMs = options.reconnectBaseDelayMs ?? 250;
  const reconnectMaxDelayMs = options.reconnectMaxDelayMs ?? 15_000;
  const connectOptions = {
    baseUrl: options.baseUrl,
    token: options.token,
    ...(options.pingIntervalMs === undefined
      ? {}
      : { pingIntervalMs: options.pingIntervalMs }),
    ...(options.requestTimeoutMs === undefined
      ? {}
      : { requestTimeoutMs: options.requestTimeoutMs }),
    ...(options.connectTimeoutMs === undefined
      ? {}
      : { connectTimeoutMs: options.connectTimeoutMs }),
  };

  const probe: AgentDriver["probe"] = Effect.gen(function* () {
    const descriptor = yield* fetchEnvironmentDescriptor(options.baseUrl).pipe(
      Effect.mapError(fromRpcError("probe", false)),
    );
    // The descriptor omits the field on protocol 1 servers.
    const protocol = descriptor.orchestrationProtocolVersion ?? 1;
    if (protocol !== ORCHESTRATION_PROTOCOL_VERSION) {
      return yield* new DriverError({
        driver: "t3",
        operation: "probe",
        reason: "unsupported",
        submission: "not-submitted",
        message: `T3 ${descriptor.serverVersion} speaks orchestration protocol ${protocol}; this driver needs ${ORCHESTRATION_PROTOCOL_VERSION}`,
      });
    }
    yield* fetchWebSocketTicket(connectOptions).pipe(
      Effect.mapError(fromRpcError("probe", false)),
    );
    return {
      detail: `T3 ${descriptor.serverVersion}, orchestration protocol ${protocol}, credential accepted`,
    };
  });

  const open: AgentDriver["open"] = (input: OpenSessionInput) =>
    Effect.gen(function* () {
      const sessionScope = yield* Effect.scope;
      const threadId =
        input.resume?.ref.nativeId ?? ids.threadId(input.sessionKey);
      const projectId =
        input.resume?.ref.locator.projectId ??
        (input.workspace._tag === "Directory"
          ? yield* ensureT3Project({
              ...connectOptions,
              workspaceRoot: input.workspace.path,
              title: input.title,
              idNamespace: options.idNamespace,
            }).pipe(Effect.mapError(fromRpcError("provisionProject", true)))
          : (input.project ?? options.projectId));
      if (projectId === undefined)
        return yield* protocolError(
          "open",
          "T3 project is required for driver-managed workspaces",
        );

      // -- connection management: one live socket, replaced when it dies.
      const lock = yield* Semaphore.make(1);
      const current = yield* Ref.make<
        Option.Option<{
          readonly connection: T3RpcConnection;
          readonly scope: Scope.Closeable;
        }>
      >(Option.none());
      const connection = lock.withPermits(1)(
        Effect.gen(function* () {
          const existing = yield* Ref.get(current);
          if (Option.isSome(existing)) {
            if (existing.value.connection.isOpen()) {
              return existing.value.connection;
            }
            yield* Scope.close(existing.value.scope, Exit.void);
            yield* Ref.set(current, Option.none());
          }
          const scope = yield* Scope.fork(sessionScope, "sequential");
          const fresh = yield* connectT3(connectOptions).pipe(
            Scope.provide(scope),
            Effect.tapError(() => Scope.close(scope, Exit.void)),
            Effect.mapError(fromRpcError("connect", false)),
          );
          yield* Ref.set(current, Option.some({ connection: fresh, scope }));
          return fresh;
        }),
      );

      const call = (
        method: string,
        payload: unknown,
        operation: string,
        mutating: boolean,
        timeoutMs?: number,
      ) =>
        connection.pipe(
          Effect.flatMap((conn) =>
            conn
              .request(
                method,
                payload,
                timeoutMs === undefined ? undefined : { timeoutMs },
              )
              .pipe(Effect.mapError(fromRpcError(operation, mutating))),
          ),
        );

      const encode = <A, I>(
        schema: Schema.Codec<A, I>,
        value: A,
        operation: string,
      ) =>
        Schema.encodeEffect(schema)(value).pipe(
          Effect.mapError(() =>
            protocolError(
              operation,
              `T3 ${operation} payload failed the pinned wire schema`,
            ),
          ),
        );

      const fetchProjection = (operation: string) =>
        call(
          T3_METHODS.getThreadProjection,
          { threadId },
          operation,
          false,
        ).pipe(Effect.flatMap(decodeProjection(operation)));

      /** Projection read that rides out a reconnect. */
      const fetchProjectionSettled = (operation: string) =>
        fetchProjection(operation).pipe(
          Effect.retry({
            times: 4,
            while: isTransient,
            schedule: Schedule.exponential(
              Duration.millis(reconnectBaseDelayMs),
            ),
          }),
        );

      /** For commands T3 dedupes by commandId: a resend replays the receipt. */
      const dispatchIdempotent = (
        command: DispatchCommand,
        operation: string,
      ) =>
        Effect.gen(function* () {
          const payload = yield* encode(DispatchCommand, command, operation);
          const send = call(
            T3_METHODS.dispatchCommand,
            payload,
            operation,
            true,
          );
          return yield* send.pipe(
            Effect.catchIf(
              (error) => error.submission === "possibly-submitted",
              () => send,
            ),
          );
        });

      // -- create or re-attach the thread.
      if (input.resume === undefined) {
        const launchInput: LaunchThreadInput = {
          commandId: ids.launchCommandId(threadId),
          creationSource: "web",
          threadId,
          projectId,
          title: input.title.trim().length > 0 ? input.title.trim() : threadId,
          modelSelection: options.modelSelection,
          runtimeMode: input.runtimeMode ?? options.runtimeMode,
          interactionMode: options.interactionMode,
          workspaceStrategy: workspaceStrategy(input.workspace),
        };
        const payload = yield* encode(
          LaunchThreadInput,
          launchInput,
          "launchThread",
        );
        const launch = call(
          T3_METHODS.launchThread,
          payload,
          "launchThread",
          true,
          options.launchTimeoutMs ?? 60_000,
        ).pipe(
          Effect.flatMap((value) =>
            Schema.decodeUnknownEffect(LaunchThreadResult)(value).pipe(
              Effect.mapError(() =>
                withSubmission("submitted")(
                  protocolError(
                    "launchThread",
                    "T3 launch result did not match the pinned wire schema",
                  ),
                ),
              ),
            ),
          ),
          Effect.asVoid,
        );
        yield* launch.pipe(
          Effect.catchIf(
            (error) => error.submission === "possibly-submitted",
            // Lost response: if the thread exists the launch landed. If not,
            // the same commandId is safe to send again.
            () =>
              fetchProjectionSettled("launchThread").pipe(
                Effect.asVoid,
                Effect.catch(() => launch),
              ),
          ),
        );
      } else {
        yield* fetchProjectionSettled("open");
      }

      // -- event pump: subscribe, map, resubscribe from the cursor on loss.
      const resumeCursor =
        input.resume?.cursor === undefined
          ? null
          : Number.parseInt(input.resume.cursor, 10);
      const mapper = new T3EventMapper(
        threadId,
        resumeCursor !== null && Number.isSafeInteger(resumeCursor)
          ? resumeCursor
          : null,
      );
      const queue =
        yield* Queue.unbounded<Take.Take<AgentEvent, DriverError>>();
      yield* Effect.addFinalizer(() => Queue.shutdown(queue));
      const outcomes = yield* SubscriptionRef.make<OutcomeState>({
        runs: new Map(),
        failure: null,
      });
      const received = yield* Ref.make(0);

      const subscribeOnce = Effect.gen(function* () {
        const conn = yield* connection;
        const cursor = mapper.cursor;
        yield* conn
          .stream(
            T3_METHODS.subscribeThread,
            cursor === null
              ? { threadId }
              : { threadId, afterSequence: cursor },
          )
          .pipe(
            Stream.mapError(fromRpcError("subscribeThread", false)),
            Stream.mapEffect((raw) =>
              Schema.decodeUnknownEffect(ThreadStreamItem)(raw).pipe(
                Effect.mapError(() =>
                  protocolError(
                    "subscribeThread",
                    "T3 thread stream item did not match the pinned wire schema",
                  ),
                ),
              ),
            ),
            Stream.runForEach((item) =>
              Effect.gen(function* () {
                yield* Ref.update(received, (count) => count + 1);
                for (const event of mapper.apply(item)) {
                  yield* Queue.offer(queue, [event]);
                }
                yield* SubscriptionRef.set(outcomes, {
                  runs: new Map(mapper.runViews()),
                  failure: null,
                });
              }),
            ),
          );
        return yield* new DriverError({
          driver: "t3",
          operation: "subscribeThread",
          reason: "lost",
          submission: "not-submitted",
          message: "T3 ended the thread subscription",
        });
      });

      const pump = Effect.gen(function* () {
        let failures = 0;
        while (true) {
          const before = yield* Ref.get(received);
          const error = yield* Effect.flip(subscribeOnce);
          if (!isTransient(error)) return yield* error;
          const progressed = (yield* Ref.get(received)) > before;
          failures = progressed ? 1 : failures + 1;
          if (failures > maxReconnectAttempts) {
            return yield* new DriverError({
              driver: "t3",
              operation: "subscribeThread",
              reason: "lost",
              submission: "not-submitted",
              message: `Gave up after ${maxReconnectAttempts} reconnect attempts: ${error.message}`,
            });
          }
          yield* Effect.sleep(
            Duration.millis(
              Math.min(
                reconnectMaxDelayMs,
                reconnectBaseDelayMs * 2 ** (failures - 1),
              ),
            ),
          );
        }
      }).pipe(
        Effect.catch((error: DriverError) =>
          Effect.andThen(
            SubscriptionRef.update(outcomes, (state) => ({
              ...state,
              failure: error,
            })),
            Queue.offer(queue, Exit.fail(error)),
          ),
        ),
      );
      yield* Effect.forkIn(pump, sessionScope);

      const awaitOutcome = (turnId: string) =>
        SubscriptionRef.changes(outcomes).pipe(
          Stream.filterMap((state) =>
            Result.fromOption(resolveOutcome(state, turnId), () => undefined),
          ),
          Stream.runHead,
          Effect.flatMap(
            Option.match({
              onNone: () =>
                Effect.fail(
                  new DriverError({
                    driver: "t3",
                    operation: "outcome",
                    reason: "lost",
                    submission: "submitted",
                    message: "The T3 session closed before the turn ended",
                  }),
                ),
              onSome: (result) => Effect.fromResult(result),
            }),
          ),
        );

      const handleFor = (
        clientRequestId: string,
        located: Located,
        adopted: boolean,
      ): Effect.Effect<TurnHandle, DriverError> =>
        located._tag === "Unlinked"
          ? Effect.fail(
              new DriverError({
                driver: "t3",
                operation: "sendTurn",
                reason: "protocol",
                submission: "submitted",
                message: "T3 accepted the message but linked it to no run",
              }),
            )
          : Effect.succeed({
              clientRequestId,
              turnId: located.turnId,
              delivery: located.delivery,
              adopted,
              outcome: awaitOutcome(located.turnId),
            });

      const sendTurn: AgentSession["sendTurn"] = (turn: TurnInput) =>
        Effect.gen(function* () {
          const messageId = ids.messageId(threadId, turn.clientRequestId);
          const before = yield* fetchProjectionSettled("sendTurn");
          const existing = locateTurn(before, messageId);
          if (existing !== null) {
            return yield* handleFor(turn.clientRequestId, existing, true);
          }
          const active = activeRun(before);
          const dispatchMode: DispatchMode =
            turn.delivery === "queue" ||
            (turn.delivery === "auto" && active !== undefined)
              ? { type: "queue_after_active" }
              : turn.delivery === "steer" && active !== undefined
                ? { type: "steer_active", targetRunId: active.id }
                : { type: "start_immediately" };
          const payload = yield* encode(
            DispatchCommand,
            {
              type: "message.dispatch",
              createdBy: "user",
              creationSource: "web",
              commandId: ids.dispatchCommandId(threadId, turn.clientRequestId),
              threadId,
              messageId,
              text: turn.text,
              attachments: [],
              dispatchMode,
            },
            "sendTurn",
          );
          const dispatch = call(
            T3_METHODS.dispatchCommand,
            payload,
            "sendTurn",
            true,
          );
          const adopted = yield* dispatch.pipe(
            Effect.as(false),
            Effect.catchIf(
              (error) => error.submission === "possibly-submitted",
              (error) =>
                fetchProjectionSettled("sendTurn").pipe(
                  Effect.mapError(() => error),
                  Effect.flatMap((projection) =>
                    locateTurn(projection, messageId) !== null
                      ? Effect.succeed(true)
                      : dispatch.pipe(Effect.as(false)),
                  ),
                ),
            ),
          );
          const after = yield* fetchProjectionSettled("sendTurn").pipe(
            Effect.mapError(withSubmission("submitted")),
          );
          const located = locateTurn(after, messageId);
          if (located === null) {
            return yield* new DriverError({
              driver: "t3",
              operation: "sendTurn",
              reason: "protocol",
              submission: "submitted",
              message:
                "T3 accepted the message but it is missing from the thread",
            });
          }
          return yield* handleFor(turn.clientRequestId, located, adopted);
        });

      const interrupt: AgentSession["interrupt"] = (turnId) =>
        Effect.gen(function* () {
          const target =
            turnId ?? activeRun(yield* fetchProjectionSettled("interrupt"))?.id;
          if (target === undefined) return "no_active_turn" as const;
          yield* dispatchIdempotent(
            {
              type: "run.interrupt",
              commandId: ids.interruptCommandId(threadId, target),
              threadId,
              runId: target,
              reason: "Interrupted by the Linear gateway",
            },
            "interrupt",
          );
          return "requested" as const;
        });

      const respond: AgentSession["respond"] = (
        requestId: string,
        response: RequestResponse,
      ) => {
        const commandId = ids.respondCommandId(threadId, requestId);
        const command: DispatchCommand =
          response._tag === "Dismiss"
            ? {
                type: "thread.user-input.dismiss",
                commandId,
                threadId,
                requestId,
              }
            : {
                type: "runtime-request.respond",
                commandId,
                threadId,
                requestId,
                ...(response._tag === "Answer"
                  ? { answers: response.answers }
                  : {
                      decision:
                        response._tag === "Deny"
                          ? "decline"
                          : response._tag === "Cancel"
                            ? "cancel"
                            : response.scope === "session"
                              ? "acceptForSession"
                              : response.scope === "always"
                                ? "acceptAlways"
                                : "accept",
                    }),
              };
        return dispatchIdempotent(command, "respond").pipe(Effect.asVoid);
      };

      const snapshot: AgentSession["snapshot"] = fetchProjectionSettled(
        "snapshot",
      ).pipe(
        Effect.map((projection): SessionSnapshot => {
          const active = activeRun(projection);
          return {
            activeTurnId: active?.id ?? null,
            turns: [...projection.runs]
              .sort((a, b) => a.ordinal - b.ordinal)
              .map((run) => ({
                turnId: run.id,
                status: run.status,
                clientRequestId: ids.clientRequestIdOf(
                  threadId,
                  run.userMessageId,
                ),
              })),
            queueHeld: projection.runs.some(
              (run) => run.status === "queued" && run.queueHeld === true,
            ),
            pendingRequests: projection.runtimeRequests
              .filter(
                (request) =>
                  request.status === "pending" &&
                  request.kind !== "dynamic_tool_call" &&
                  request.kind !== "auth_refresh",
              )
              .map((request) => ({
                requestId: request.id,
                kind:
                  request.kind === "user_input"
                    ? ("question" as const)
                    : ("approval" as const),
              })),
            acceptedClientRequestIds: projection.messages.flatMap((message) => {
              const id = ids.clientRequestIdOf(threadId, message.id);
              return id === null ? [] : [id];
            }),
            cursor: mapper.cursor === null ? null : String(mapper.cursor),
          };
        }),
      );

      return {
        ref: {
          driver: "t3",
          nativeId: threadId,
          locator: { projectId, origin: options.baseUrl.origin },
        },
        capabilities: T3_CAPABILITIES,
        events: Stream.fromQueue(queue).pipe(Stream.flattenTake),
        sendTurn,
        interrupt,
        respond,
        snapshot,
      } satisfies AgentSession;
    });

  return { kind: "t3", capabilities: T3_CAPABILITIES, probe, open };
};
