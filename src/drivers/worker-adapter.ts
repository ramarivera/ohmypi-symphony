/** Keeps the authority's host-tool/cancellation machinery while all turn
 * execution and event consumption go through AgentDriver. */
import { randomUUID } from "node:crypto";
import {
  Clock,
  Effect,
  Exit,
  Fiber,
  Option,
  Schema,
  Scope,
  Stream,
} from "effect";
import { RpcProtocolError } from "../domain/errors.js";
import type { RpcEvent, RpcWorkerHandle } from "../services/rpc-worker.js";
import type {
  AgentDriver,
  AgentSession,
  OpenSessionInput,
  TurnInput,
  UserQuestion,
} from "./driver.js";
import { agentEventToRpc } from "./events.js";

export interface DriverWorkerHandle extends RpcWorkerHandle {
  readonly driverSession: AgentSession;
}
const protocolError = (cause: { readonly message: string }) =>
  new RpcProtocolError({ method: "driver", message: cause.message });

export const openDriverWorker = (
  driver: AgentDriver,
  input: OpenSessionInput,
  legacy?: RpcWorkerHandle,
  stallTimeoutMs = 300_000,
) =>
  Effect.gen(function* () {
    const parentScope = yield* Effect.scope;
    const scope = yield* Scope.fork(parentScope, "sequential");
    const session = yield* driver.open(input).pipe(
      Scope.provide(scope),
      Effect.onError(() => Scope.close(scope, Exit.void)),
      Effect.mapError(protocolError),
    );
    let lastProgressAt = yield* Clock.currentTimeMillis;
    let awaitingOutcome = false;
    let ownedTurnId: string | undefined;
    const listeners = new Set<(event: RpcEvent) => void>();
    const questions = new Map<string, ReadonlyArray<UserQuestion>>();
    const emit = (event: RpcEvent) => {
      for (const listener of listeners) listener(event);
    };
    const send = (
      text: string,
      delivery: TurnInput["delivery"],
      clientRequestId: string = randomUUID(),
    ) =>
      session.sendTurn({ text, delivery, clientRequestId }).pipe(
        Effect.mapError(protocolError),
        Effect.tap(() =>
          Effect.gen(function* () {
            lastProgressAt = yield* Clock.currentTimeMillis;
            awaitingOutcome = true;
          }),
        ),
        Effect.tap((handle) => {
          ownedTurnId = handle.turnId;
          return session.ref.driver !== "t3"
            ? Effect.void
            : Effect.forkIn(
                handle.outcome.pipe(
                  Effect.tap((outcome) =>
                    Effect.gen(function* () {
                      const snapshot = yield* session.snapshot.pipe(
                        Effect.option,
                      );
                      if (
                        outcome.status === "completed" &&
                        Option.isSome(snapshot) &&
                        snapshot.value.turns.some(
                          (turn) =>
                            turn.turnId !== outcome.turnId &&
                            [
                              "queued",
                              "preparing",
                              "starting",
                              "running",
                              "waiting",
                            ].includes(turn.status),
                        )
                      )
                        return;
                      yield* Effect.sync(() => {
                        awaitingOutcome = false;
                        emit(
                          agentEventToRpc({
                            _tag: "TurnEnded",
                            key: `driver:${session.ref.nativeId}:${handle.turnId}:outcome`,
                            cursor: null,
                            outcome,
                          }),
                        );
                      });
                    }),
                  ),
                  Effect.catch((error) =>
                    Effect.sync(() =>
                      emit({ type: "error", message: error.message }),
                    ),
                  ),
                ),
                scope,
              ).pipe(Effect.asVoid);
        }),
      );
    if (session.ref.driver === "t3")
      yield* Effect.forkIn(
        Effect.forever(
          Effect.gen(function* () {
            yield* Effect.sleep(Math.min(1_000, stallTimeoutMs));
            if (
              awaitingOutcome &&
              (yield* Clock.currentTimeMillis) - lastProgressAt >=
                stallTimeoutMs
            ) {
              awaitingOutcome = false;
              if (ownedTurnId !== undefined)
                yield* session
                  .interrupt(ownedTurnId)
                  .pipe(Effect.catch(() => Effect.void));
              emit({
                type: "error",
                message: `T3 stalled for ${stallTimeoutMs}ms`,
              });
            }
          }),
        ),
        scope,
      );
    let pump: Fiber.Fiber<void> | undefined;
    const handle: DriverWorkerHandle = {
      driverSession: session,
      sessionId: legacy?.sessionId ?? Effect.succeed(Option.none()),
      sessionFile: legacy?.sessionFile ?? Effect.succeed(Option.none()),
      isStreaming:
        legacy?.isStreaming ??
        session.snapshot.pipe(
          Effect.map((state) => state.activeTurnId !== null),
          Effect.catch(() => Effect.succeed(false)),
        ),
      start: () => Effect.void,
      stop: () =>
        Effect.gen(function* () {
          listeners.clear();
          yield* Scope.close(scope, Exit.void);
        }),
      prompt: (text, id) =>
        send(text, "auto", id).pipe(
          Effect.map((turn) => turn.agentInvoked !== false),
        ),
      steer: (text, id) => send(text, "steer", id).pipe(Effect.asVoid),
      followUp: (text, id) => send(text, "queue", id).pipe(Effect.asVoid),
      abort: () =>
        session.interrupt().pipe(Effect.asVoid, Effect.mapError(protocolError)),
      getState:
        legacy?.getState ??
        (() =>
          session.snapshot.pipe(Effect.as({}), Effect.mapError(protocolError))),
      setHostTools: legacy?.setHostTools ?? (() => Effect.void),
      onHostToolCall:
        legacy?.onHostToolCall ?? (() => Effect.succeed(() => Effect.void)),
      respondToUi:
        legacy?.respondToUi ??
        ((requestId, response) => {
          const form = questions.get(requestId) ?? [];
          return Effect.gen(function* () {
            if ("cancelled" in response) {
              yield* session.respond(requestId, { _tag: "Dismiss" });
              return;
            }
            const value =
              "value" in response ? response.value : String(response.confirmed);
            const structured =
              form.length > 1 || form.some((question) => question.multiSelect);
            const answers = structured
              ? yield* Effect.try({
                  try: () =>
                    Schema.decodeUnknownSync(
                      Schema.Record(
                        Schema.String,
                        Schema.Union([
                          Schema.String,
                          Schema.Array(Schema.String),
                        ]),
                      ),
                    )(JSON.parse(value)),
                  catch: () =>
                    new RpcProtocolError({
                      method: "driver",
                      message:
                        "Reply with a JSON object keyed by question id; multi-select answers must be arrays.",
                    }),
                })
              : Object.fromEntries(
                  form.map((question) => [question.id, value]),
                );
            if (
              Object.keys(answers).length !== form.length ||
              form.some((question) => {
                const answer = answers[question.id];
                return (
                  !Object.hasOwn(answers, question.id) ||
                  (question.multiSelect
                    ? !Array.isArray(answer)
                    : typeof answer !== "string")
                );
              })
            )
              return yield* new RpcProtocolError({
                method: "driver",
                message:
                  "Answer every question id once, using arrays only for multi-select questions.",
              });
            yield* session.respond(requestId, { _tag: "Answer", answers });
          }).pipe(Effect.asVoid, Effect.mapError(protocolError));
        }),
      onEvent: (listener) =>
        Effect.gen(function* () {
          listeners.add(listener);
          if (pump === undefined)
            pump = yield* session.events.pipe(
              Stream.runForEach((event) =>
                Effect.gen(function* () {
                  lastProgressAt = yield* Clock.currentTimeMillis;
                  // Outcomes are watched from sendTurn so historical terminal runs in a
                  // resumed snapshot cannot finish the new authority attempt.
                  if (session.ref.driver === "t3" && event._tag === "TurnEnded")
                    return;
                  if (event._tag === "RequestOpened") {
                    if (event.kind === "question") {
                      questions.set(event.requestId, event.questions);
                      awaitingOutcome = false;
                    }
                  }
                  if (event._tag === "RequestResolved") awaitingOutcome = true;
                  emit(agentEventToRpc(event));
                }),
              ),
              Effect.catch((error) =>
                Effect.sync(() =>
                  emit({ type: "error", message: error.message }),
                ),
              ),
              Effect.forkIn(scope),
            );
          return () =>
            Effect.gen(function* () {
              listeners.delete(listener);
              if (listeners.size === 0 && pump !== undefined) {
                yield* Fiber.interrupt(pump);
                pump = undefined;
              }
            });
        }),
    };
    return handle;
  });
