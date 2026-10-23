import { Deferred, Effect, Option, Queue, Stream } from "effect";
import type { RpcWorkerHandle } from "../../services/rpc-worker.js";
import {
  type AgentDriver,
  type AgentEvent,
  DriverError,
  type TurnHandle,
  type TurnOutcome,
} from "../driver.js";
import { ompEvent, rpcAssistantText } from "../events.js";

export const OMP_CAPABILITIES = {
  steer: true,
  queue: true,
  hostTools: "inline",
  resumableStream: false,
  approvals: false,
  questions: true,
  workspaces: "gateway",
} as const;

const error = (operation: string) => (cause: { readonly message: string }) =>
  new DriverError({
    driver: "omp",
    operation,
    reason: "unavailable",
    submission: "possibly-submitted",
    message: cause.message,
  });

/** Uses the already prepared, allowlisted, uid-isolated RPC worker. */
export const makeOmpDriver = (worker: RpcWorkerHandle): AgentDriver => ({
  kind: "omp",
  capabilities: OMP_CAPABILITIES,
  probe: Effect.succeed({ detail: "omp stdio RPC adapter" }),
  open: (input) =>
    Effect.gen(function* () {
      if (input.workspace._tag !== "Directory")
        return yield* new DriverError({
          driver: "omp",
          operation: "open",
          reason: "unsupported",
          submission: "not-submitted",
          message: "omp requires a gateway workspace",
        });
      const queue = yield* Queue.unbounded<AgentEvent>();
      const accepted = new Map<string, TurnHandle>();
      const outcomes = new Map<
        string,
        Deferred.Deferred<TurnOutcome, DriverError>
      >();
      let sequence = 0;
      let activeTurnId: string | null = null;
      let assistantDraft: string | null = null;
      let interrupted = false;
      const unsubscribe = yield* worker.onEvent((event) => {
        const key = `omp:${input.sessionKey}:${++sequence}`;
        const turnIdBefore = activeTurnId;
        const finalText = rpcAssistantText(event);
        if (finalText.length > 0) assistantDraft = finalText;
        let terminal: TurnOutcome | undefined;
        if (
          (event.type === "agent_end" && event.willContinue !== true) ||
          event.type === "error" ||
          event.type === "worker_exited"
        ) {
          const pending = [...outcomes];
          outcomes.clear();
          activeTurnId = null;
          for (const [turnId, outcome] of pending) {
            const result: TurnOutcome = {
              turnId,
              status: interrupted
                ? "interrupted"
                : event.type === "agent_end"
                  ? "completed"
                  : "failed",
              finalText: assistantDraft,
              error: typeof event.message === "string" ? event.message : null,
            };
            terminal = result;
            Deferred.doneUnsafe(outcome, Effect.succeed(result));
          }
        }
        Queue.offerUnsafe(queue, ompEvent(event, key, turnIdBefore, terminal));
      });
      yield* Effect.addFinalizer(() =>
        unsubscribe().pipe(
          Effect.andThen(worker.stop()),
          Effect.andThen(Queue.shutdown(queue)),
        ),
      );
      yield* worker.start().pipe(Effect.mapError(error("open")));
      const sessionFile = yield* worker.sessionFile;
      return {
        ref: {
          driver: "omp" as const,
          nativeId: Option.getOrElse(sessionFile, () => input.sessionKey),
          locator: {},
        },
        capabilities: OMP_CAPABILITIES,
        events: Stream.fromQueue(queue),
        sendTurn: (turn) =>
          Effect.gen(function* () {
            const existing = accepted.get(turn.clientRequestId);
            if (existing !== undefined) return { ...existing, adopted: true };
            const busy = yield* worker.isStreaming;
            if (turn.delivery === "start" && busy)
              return yield* new DriverError({
                driver: "omp",
                operation: "sendTurn",
                reason: "rejected",
                submission: "not-submitted",
                message: "omp session already has an active turn",
              });
            const delivery =
              turn.delivery === "steer" && busy
                ? ("steered" as const)
                : turn.delivery === "queue" ||
                    (turn.delivery === "auto" && busy)
                  ? ("queued" as const)
                  : ("started" as const);
            if (!busy) {
              assistantDraft = null;
              interrupted = false;
            }
            const completion = yield* Deferred.make<TurnOutcome, DriverError>();
            const turnId = turn.clientRequestId;
            outcomes.set(turnId, completion);
            activeTurnId = turnId;
            const invoked =
              delivery === "steered"
                ? yield* worker
                    .steer(turn.text)
                    .pipe(Effect.as(true), Effect.mapError(error("steer")))
                : delivery === "queued"
                  ? yield* worker
                      .followUp(turn.text)
                      .pipe(Effect.as(true), Effect.mapError(error("queue")))
                  : yield* worker
                      .prompt(turn.text)
                      .pipe(Effect.mapError(error("prompt")));
            if (!invoked) {
              activeTurnId = null;
              outcomes.delete(turnId);
              yield* Deferred.succeed(completion, {
                turnId,
                status: "completed",
                finalText: null,
                error: null,
              });
            }
            const handle: TurnHandle = {
              clientRequestId: turn.clientRequestId,
              turnId,
              delivery,
              adopted: false,
              agentInvoked: invoked,
              outcome: Deferred.await(completion),
            };
            accepted.set(turn.clientRequestId, handle);
            return handle;
          }),
        interrupt: () =>
          Effect.sync(() => {
            interrupted = true;
          }).pipe(
            Effect.andThen(
              worker
                .abort()
                .pipe(
                  Effect.as("requested" as const),
                  Effect.mapError(error("interrupt")),
                ),
            ),
          ),
        respond: (requestId, response) =>
          worker
            .respondToUi(
              requestId,
              response._tag === "Answer"
                ? { value: String(Object.values(response.answers)[0] ?? "") }
                : response._tag === "Approve"
                  ? { confirmed: true }
                  : response._tag === "Deny"
                    ? { confirmed: false }
                    : { cancelled: true },
            )
            .pipe(Effect.mapError(error("respond"))),
        snapshot: worker.getState().pipe(
          Effect.map(() => ({
            activeTurnId,
            turns: [],
            queueHeld: false,
            pendingRequests: [],
            acceptedClientRequestIds: [...accepted.keys()],
            cursor: null,
          })),
          Effect.mapError(error("snapshot")),
        ),
      };
    }),
});
