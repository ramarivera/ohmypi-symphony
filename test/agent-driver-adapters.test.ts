import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Option, Queue, Result, Stream } from "effect";
import { TestClock } from "effect/testing";
import type {
  AgentDriver,
  AgentEvent,
  AgentSession,
  TurnOutcome,
} from "../src/drivers/driver.js";
import { DriverError } from "../src/drivers/driver.js";
import { makeOmpDriver } from "../src/drivers/omp/driver.js";
import { openDriverWorker } from "../src/drivers/worker-adapter.js";
import type { RpcEvent, RpcWorkerHandle } from "../src/services/rpc-worker.js";

const ompWorker = () => {
  const inputs: Array<{ kind: string; text: string }> = [];
  let listener: ((event: RpcEvent) => void) | undefined;
  let stops = 0;
  let streaming = false;
  const worker: RpcWorkerHandle = {
    sessionId: Effect.succeed(Option.some("omp-native")),
    sessionFile: Effect.succeed(Option.some("/workspace/session.jsonl")),
    isStreaming: Effect.sync(() => streaming),
    start: () => Effect.void,
    stop: () =>
      Effect.sync(() => {
        stops++;
      }),
    prompt: (text) =>
      Effect.sync(() => {
        inputs.push({ kind: "prompt", text });
        streaming = text !== "/help";
        return streaming;
      }),
    steer: (text) =>
      Effect.sync(() => {
        inputs.push({ kind: "steer", text });
      }),
    followUp: (text) =>
      Effect.sync(() => {
        inputs.push({ kind: "queue", text });
      }),
    getState: () => Effect.succeed({}),
    abort: () => Effect.void,
    setHostTools: () => Effect.void,
    onHostToolCall: () => Effect.succeed(() => Effect.void),
    respondToUi: () => Effect.void,
    onEvent: (callback) =>
      Effect.sync(() => {
        listener = callback;
        return () =>
          Effect.sync(() => {
            listener = undefined;
          });
      }),
  };
  return {
    worker,
    inputs,
    emit: (event: RpcEvent) => {
      if (event.type === "agent_end") streaming = false;
      listener?.(event);
    },
    stops: () => stops,
  };
};
const input = {
  sessionKey: "linear-session",
  title: "Fix it",
  workspace: { _tag: "Directory" as const, path: "/workspace" },
};

describe("omp AgentDriver adapter", () => {
  it.effect(
    "executes prompt/steer/queue through the interface and adopts duplicate inputs",
    () =>
      Effect.gen(function* () {
        const raw = ompWorker();
        const session = yield* makeOmpDriver(raw.worker).open(input);
        const first = yield* session.sendTurn({
          clientRequestId: "input-1",
          text: "fix",
          delivery: "start",
        });
        const adopted = yield* session.sendTurn({
          clientRequestId: "input-1",
          text: "fix",
          delivery: "start",
        });
        yield* session.sendTurn({
          clientRequestId: "input-2",
          text: "next",
          delivery: "queue",
        });
        yield* session.sendTurn({
          clientRequestId: "input-3",
          text: "change",
          delivery: "steer",
        });
        expect(first.adopted).toBe(false);
        expect(adopted.adopted).toBe(true);
        expect(raw.inputs).toEqual([
          { kind: "prompt", text: "fix" },
          { kind: "queue", text: "next" },
          { kind: "steer", text: "change" },
        ]);
        raw.emit({
          type: "message_end",
          message: { role: "assistant", content: "Created the PR" },
        });
        raw.emit({ type: "agent_end" });
        expect(yield* first.outcome).toMatchObject({
          status: "completed",
          finalText: "Created the PR",
        });
        const idleSteer = yield* session.sendTurn({
          clientRequestId: "input-4",
          text: "continue",
          delivery: "steer",
        });
        expect(idleSteer.delivery).toBe("started");
        yield* session.interrupt();
        raw.emit({ type: "agent_end" });
        expect((yield* idleSteer.outcome).status).toBe("interrupted");
      }),
  );
  it.effect(
    "preserves extension events and local commands across the authority compatibility boundary",
    () =>
      Effect.gen(function* () {
        const raw = ompWorker();
        const worker = yield* openDriverWorker(
          makeOmpDriver(raw.worker),
          input,
          raw.worker,
        );
        const events: RpcEvent[] = [];
        yield* worker.onEvent((event) => events.push(event));
        expect(yield* worker.prompt("/help", "input-help")).toBe(false);
        const event = {
          type: "extension_ui_request",
          id: "question",
          method: "input",
          title: "What branch?",
        };
        raw.emit(event);
        yield* Effect.yieldNow;
        expect(events).toEqual([event]);
        yield* worker.stop();
        expect(raw.stops()).toBe(1);
      }),
  );
});

const controlledT3 = Effect.gen(function* () {
  const queue = yield* Queue.unbounded<AgentEvent>();
  const completion = yield* Deferred.make<TurnOutcome, DriverError>();
  const responses: Array<unknown> = [];
  let interruptions = 0;
  let closed = 0;
  const targets: Array<string | undefined> = [];
  const capabilities = {
    steer: true,
    queue: true,
    hostTools: "mcp" as const,
    resumableStream: true,
    approvals: true,
    questions: true,
    workspaces: "gateway" as const,
  };
  const session: AgentSession = {
    ref: { driver: "t3", nativeId: "thread-1", locator: {} },
    capabilities,
    events: Stream.fromQueue(queue),
    snapshot: Effect.succeed({
      activeTurnId: "run-1",
      turns: [
        { turnId: "run-1", status: "running", clientRequestId: "input-1" },
      ],
      queueHeld: false,
      pendingRequests: [],
      acceptedClientRequestIds: ["input-1"],
      cursor: null,
    }),
    interrupt: (turnId) =>
      Effect.sync(() => {
        targets.push(turnId);
        interruptions++;
        return "requested" as const;
      }),
    respond: (requestId, response) =>
      Effect.sync(() => {
        responses.push({ requestId, response });
      }),
    sendTurn: (turn) =>
      Effect.succeed({
        clientRequestId: turn.clientRequestId,
        turnId: "run-1",
        delivery: "started",
        adopted: false,
        outcome: Deferred.await(completion),
      }),
  };
  const driver: AgentDriver = {
    kind: "t3",
    capabilities,
    probe: Effect.succeed({ detail: "test" }),
    open: () =>
      Effect.acquireRelease(Effect.succeed(session), () =>
        Effect.sync(() => {
          closed++;
        }),
      ),
  };
  return {
    driver,
    session,
    closed: () => closed,
    targets,
    queue,
    completion,
    responses,
    interruptions: () => interruptions,
  };
});

describe("T3 authority compatibility boundary", () => {
  it.effect("detaches without interrupting a new owner's active turn", () =>
    Effect.gen(function* () {
      const t3 = yield* controlledT3;
      const worker = yield* openDriverWorker(t3.driver, input);
      yield* worker.prompt("work", "input-1");
      yield* worker.stop();
      yield* worker.stop();
      expect(t3.interruptions()).toBe(0);
      expect(t3.closed()).toBe(1);
    }),
  );
  it.effect(
    "waits for another active queued turn after a completed outcome",
    () =>
      Effect.gen(function* () {
        const t3 = yield* controlledT3;
        const worker = yield* openDriverWorker(
          {
            ...t3.driver,
            open: () =>
              Effect.succeed({
                ...t3.session,
                snapshot: Effect.succeed({
                  activeTurnId: "run-2",
                  turns: [
                    {
                      turnId: "run-2",
                      status: "running",
                      clientRequestId: "input-2",
                    },
                  ],
                  queueHeld: false,
                  pendingRequests: [],
                  acceptedClientRequestIds: [],
                  cursor: null,
                }),
              }),
          },
          input,
        );
        const events: RpcEvent[] = [];
        yield* worker.onEvent((event) => events.push(event));
        yield* worker.prompt("work", "input-1");
        yield* Deferred.succeed(t3.completion, {
          turnId: "run-1",
          status: "completed",
          finalText: "Done",
          error: null,
        });
        yield* Effect.yieldNow;
        expect(events.some((event) => event.type === "agent_end")).toBe(false);
        yield* worker.stop();
      }),
  );
  it.effect(
    "keeps completed outcomes when snapshot reads fail, but surfaces outcome failures",
    () =>
      Effect.gen(function* () {
        for (const failOutcome of [false, true]) {
          const t3 = yield* controlledT3;
          const error = new DriverError({
            driver: "t3",
            operation: "snapshot",
            reason: "lost",
            submission: "submitted",
            message: "offline",
          });
          const worker = yield* openDriverWorker(
            {
              ...t3.driver,
              open: () =>
                Effect.succeed({ ...t3.session, snapshot: Effect.fail(error) }),
            },
            input,
          );
          const seen = yield* Deferred.make<RpcEvent>();
          yield* worker.onEvent((event) => {
            if (event.type === "agent_end" || event.type === "error")
              Deferred.doneUnsafe(seen, Effect.succeed(event));
          });
          yield* worker.prompt("work", "input-1");
          if (failOutcome) yield* Deferred.fail(t3.completion, error);
          else
            yield* Deferred.succeed(t3.completion, {
              turnId: "run-1",
              status: "completed",
              finalText: "Done",
              error: null,
            });
          expect((yield* Deferred.await(seen)).type).toBe(
            failOutcome ? "error" : "agent_end",
          );
          yield* worker.stop();
        }
      }),
  );
  it.effect(
    "preserves per-question and multi-select answers and rejects fabricated replies",
    () =>
      Effect.gen(function* () {
        const t3 = yield* controlledT3;
        const worker = yield* openDriverWorker(t3.driver, input);
        const seen = yield* Deferred.make<RpcEvent>();
        yield* worker.onEvent((event) => {
          Deferred.doneUnsafe(seen, Effect.succeed(event));
        });
        yield* Queue.offer(t3.queue, {
          _tag: "RequestOpened",
          key: "form",
          cursor: null,
          turnId: "run-1",
          kind: "question",
          requestId: "form",
          prompt: "Configure the run",
          options: [],
          questions: [
            {
              id: "branch",
              header: "Branch",
              question: "Which branch?",
              options: [],
              multiSelect: false,
              allowCustomAnswer: true,
            },
            {
              id: "suites",
              header: "Suites",
              question: "Which tests?",
              options: [{ label: "unit", value: "unit" }],
              multiSelect: true,
              allowCustomAnswer: true,
            },
          ],
        });
        const event = yield* Deferred.await(seen);
        expect(event.title).toContain('"id":"suites"');
        expect(event.title).toContain("JSON object");
        for (const value of [
          "main",
          '{"branch":"main"}',
          '{"branch":"main","suites":"unit"}',
          '{"branch":"main","suites":[],"extra":"oops"}',
        ]) {
          expect(
            Result.isFailure(
              yield* worker.respondToUi("form", { value }).pipe(Effect.result),
            ),
          ).toBe(true);
        }
        expect(t3.responses).toEqual([]);
        yield* worker.respondToUi("form", {
          value: '{"branch":"main","suites":["unit"]}',
        });
        expect(t3.responses).toEqual([
          {
            requestId: "form",
            response: {
              _tag: "Answer",
              answers: { branch: "main", suites: ["unit"] },
            },
          },
        ]);
        yield* worker.respondToUi("form", { cancelled: true });
        expect(t3.responses.at(-1)).toEqual({
          requestId: "form",
          response: { _tag: "Dismiss" },
        });
      }),
  );
  it.effect(
    "interrupts a stalled run and surfaces a retryable authority failure",
    () =>
      Effect.gen(function* () {
        const t3 = yield* controlledT3;
        const worker = yield* openDriverWorker(
          t3.driver,
          input,
          undefined,
          1_000,
        );
        const events: RpcEvent[] = [];
        yield* worker.onEvent((event) => events.push(event));
        yield* worker.prompt("work", "input-1");
        yield* TestClock.adjust(1_000);
        expect(t3.interruptions()).toBe(1);
        expect(t3.targets).toEqual(["run-1"]);
        expect(events).toContainEqual({
          type: "error",
          message: "T3 stalled for 1000ms",
        });
      }),
  );
  it.effect(
    "surfaces approvals for the authority and pauses the watchdog during human questions",
    () =>
      Effect.gen(function* () {
        const t3 = yield* controlledT3;
        const worker = yield* openDriverWorker(
          t3.driver,
          input,
          undefined,
          1_000,
        );
        const events: RpcEvent[] = [];
        yield* worker.onEvent((event) => events.push(event));
        yield* worker.prompt("work", "input-1");
        yield* Queue.offer(t3.queue, {
          _tag: "RequestOpened",
          key: "approval-1",
          cursor: null,
          turnId: "run-1",
          kind: "approval",
          requestId: "a-1",
          prompt: "Run tests?",
          options: [],
          questions: [],
        });
        yield* Queue.offer(t3.queue, {
          _tag: "RequestOpened",
          key: "question-1",
          cursor: null,
          turnId: "run-1",
          kind: "question",
          requestId: "q-1",
          prompt: "Which branch?",
          options: [],
          questions: [
            {
              id: "branch",
              header: "Branch",
              question: "Which branch?",
              options: [],
              multiSelect: false,
              allowCustomAnswer: true,
            },
          ],
        });
        yield* Effect.yieldNow;
        yield* TestClock.adjust(5_000);
        expect(t3.responses).toEqual([]);
        expect(
          events.some(
            (event) => event.type === "driver_approval" && event.id === "a-1",
          ),
        ).toBe(true);
        expect(t3.interruptions()).toBe(0);
        expect(
          events.some(
            (event) =>
              event.type === "extension_ui_request" && event.id === "q-1",
          ),
        ).toBe(true);
        yield* worker.respondToUi("q-1", { value: "main" });
        expect(t3.responses.at(-1)).toEqual({
          requestId: "q-1",
          response: { _tag: "Answer", answers: { branch: "main" } },
        });
      }),
  );
});
