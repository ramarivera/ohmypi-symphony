import { describe, expect, it } from "@effect/vitest";
import {
  Deferred,
  Effect,
  Either,
  Fiber,
  HashMap,
  Logger,
  LogLevel,
  Option,
  Schema,
} from "effect";
import { RpcWorker } from "../src/services/rpc-worker.js";
import { testPaths } from "./fixtures/paths.js";

const fixture = new URL("./fixtures/fake-rpc.ts", import.meta.url).pathname;
const uiResponseFixture = String.raw`
const send = (value) => process.stdout.write(JSON.stringify(value) + "\n");
send({
  type: "ready",
  protocolVersion: 2,
  supportedProtocolVersions: [1, 2],
});
const reader = Bun.stdin.stream().getReader();
const decoder = new TextDecoder();
let buffer = "";
while (true) {
  const result = await reader.read();
  if (result.done) break;
  buffer += decoder.decode(result.value, { stream: true });
  let newline = buffer.indexOf("\n");
  while (newline >= 0) {
    const line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    if (line) {
      const input = JSON.parse(line);
      if (input.type === "negotiate_protocol" && typeof input.id === "string") {
        send({ type: "response", id: input.id, success: true });
      } else if (
        input.type === "extension_ui_response" &&
        typeof input.id === "string"
      ) {
        send({
          type: "extension_ui_response_ack",
          id: input.id,
          confirmed: input.confirmed,
          value: input.value,
          cancelled: input.cancelled,
        });
      }
    }
    newline = buffer.indexOf("\n");
  }
}
`;

const ignoresStdinCloseFixture = String.raw`
const marker = Bun.env.STOP_MARKER;
process.stdout.write(JSON.stringify({
  type: "ready",
  protocolVersion: 1,
  supportedProtocolVersions: [1],
}) + "\n");
process.on("SIGTERM", async () => {
  if (marker) await Bun.write(marker, "signal");
  await Bun.sleep(100);
  if (marker) await Bun.write(marker, "exited");
  process.exit(0);
});
await Bun.sleep(60_000);
`;
const testPath = testPaths();

describe("RpcWorker", () => {
  const Live = RpcWorker.Default;

  it.effect("negotiates v2 and exposes session id/file/state", () =>
    Effect.gen(function* () {
      const rpc = yield* RpcWorker;
      const worker = yield* rpc.spawn({
        command: ["bun", "run", fixture],
        cwd: process.cwd(),
      });
      yield* worker.start();

      const state = yield* worker.getState();
      const sessionId = yield* worker.sessionId;
      const sessionFile = yield* worker.sessionFile;
      const isStreaming = yield* worker.isStreaming;

      expect(Option.getOrElse(sessionId, () => null)).toBe("omp-test");
      expect(Option.getOrElse(sessionFile, () => null)).toBe(
        "/safe/session.jsonl",
      );
      expect(isStreaming).toBe(false);
      expect(state).toMatchObject({
        sessionId: "omp-test",
        sessionFile: "/safe/session.jsonl",
      });

      yield* worker.stop();
    }).pipe(Effect.provide(Live)),
  );

  it.effect("prompt emits prompt_result for local command", () =>
    Effect.gen(function* () {
      const rpc = yield* RpcWorker;
      const worker = yield* rpc.spawn({
        command: ["bun", "run", fixture],
        cwd: process.cwd(),
      });
      yield* worker.start();

      const events: Array<Record<string, unknown>> = [];
      const seen = yield* Deferred.make<void>();
      yield* worker.onEvent((event) => {
        events.push(event);
        if (
          event.type === "prompt_result" &&
          (event as { agentInvoked?: boolean }).agentInvoked === false
        ) {
          Deferred.unsafeDone(seen, Effect.void);
        }
      });

      const agentInvoked = yield* worker.prompt("local command");
      expect(agentInvoked).toBe(true);

      yield* Deferred.await(seen);

      const promptResult = events.find(
        (event) => event.type === "prompt_result",
      );
      expect(promptResult).toMatchObject({
        type: "prompt_result",
        agentInvoked: false,
      });

      yield* worker.stop();
    }).pipe(Effect.provide(Live)),
  );

  it.effect("prompt returns true and streams agent events", () =>
    Effect.gen(function* () {
      const rpc = yield* RpcWorker;
      const worker = yield* rpc.spawn({
        command: ["bun", "run", fixture],
        cwd: process.cwd(),
      });
      yield* worker.start();

      const events: Array<Record<string, unknown>> = [];
      const seen = yield* Deferred.make<void>();
      const needed = ["agent_start", "message_end", "agent_end"];
      yield* worker.onEvent((event) => {
        events.push(event);
        if (needed.every((type) => events.some((e) => e.type === type))) {
          Deferred.unsafeDone(seen, Effect.void);
        }
      });

      const agentInvoked = yield* worker.prompt("perform fixture task");
      expect(agentInvoked).toBe(true);

      yield* Deferred.await(seen);

      expect(events.some((event) => event.type === "agent_start")).toBe(true);
      expect(events.some((event) => event.type === "message_end")).toBe(true);
      expect(events.some((event) => event.type === "agent_end")).toBe(true);

      const agentStart = events.find((event) => event.type === "agent_start");
      expect(agentStart).toMatchObject({
        linearSecretPresent: false,
      });

      yield* worker.stop();
    }).pipe(Effect.provide(Live)),
  );

  it.effect("steer and followUp are accepted while running", () =>
    Effect.gen(function* () {
      const rpc = yield* RpcWorker;
      const worker = yield* rpc.spawn({
        command: ["bun", "run", fixture],
        cwd: process.cwd(),
      });
      yield* worker.start();

      yield* worker.prompt("perform fixture task");
      yield* worker.steer("continue");

      yield* worker.followUp("and another");

      yield* worker.stop();
    }).pipe(Effect.provide(Live)),
  );

  it.effect("abort and stop are idempotent", () =>
    Effect.gen(function* () {
      const rpc = yield* RpcWorker;
      const worker = yield* rpc.spawn({
        command: ["bun", "run", fixture],
        cwd: process.cwd(),
      });
      yield* worker.start();

      yield* worker.abort();
      yield* worker.stop();
      yield* worker.stop();
    }).pipe(Effect.provide(Live)),
  );
  it.effect("escalates when stdin close does not stop the process", () =>
    Effect.gen(function* () {
      const rpc = yield* RpcWorker;
      const marker = testPath(`omp-rpc-stop-${Date.now()}-${Math.random()}`);
      const worker = yield* rpc.spawn({
        command: ["bun", "-e", ignoresStdinCloseFixture],
        cwd: process.cwd(),
        env: { PATH: process.env.PATH },
        injectedEnv: { STOP_MARKER: marker },
      });
      yield* worker.start();
      yield* worker.stop();
      const markerValue = yield* Effect.tryPromise(() =>
        Bun.file(marker).text(),
      );
      expect(markerValue).toBe("exited");
    }).pipe(Effect.provide(Live)),
  );

  it.effect("respondToUi and getState work after start", () =>
    Effect.gen(function* () {
      const rpc = yield* RpcWorker;
      const worker = yield* rpc.spawn({
        command: ["bun", "run", fixture],
        cwd: process.cwd(),
      });
      yield* worker.start();

      yield* worker.respondToUi("ui-1", { confirmed: true });
      yield* worker.respondToUi("ui-2", { value: "answer" });
      yield* worker.respondToUi("ui-3", { cancelled: true });

      const state = yield* worker.getState();
      expect(state).toMatchObject({
        sessionId: "omp-test",
        sessionFile: "/safe/session.jsonl",
      });

      yield* worker.stop();
    }).pipe(Effect.provide(Live)),
  );

  it.scopedLive.prop(
    "generated UI responses are emitted and acknowledged over the protocol",
    {
      requestId: Schema.String.pipe(Schema.minLength(1)),
      confirmed: Schema.Boolean,
    },
    ({ requestId, confirmed }) =>
      Effect.gen(function* () {
        const rpc = yield* RpcWorker;
        const worker = yield* rpc.spawn({
          command: ["bun", "-e", uiResponseFixture],
          cwd: process.cwd(),
        });
        yield* worker.start();
        return yield* Effect.gen(function* () {
          const acknowledged = yield* Deferred.make<{
            readonly type: string;
            readonly id: string;
            readonly confirmed: boolean;
          }>();
          yield* worker.onEvent((event) => {
            if (
              event.type === "extension_ui_response_ack" &&
              event.id === requestId &&
              typeof event.confirmed === "boolean"
            ) {
              Deferred.unsafeDone(
                acknowledged,
                Effect.succeed(
                  event as {
                    readonly type: string;
                    readonly id: string;
                    readonly confirmed: boolean;
                  },
                ),
              );
            }
          });
          yield* worker.respondToUi(requestId, { confirmed });
          const event = yield* Deferred.await(acknowledged);
          expect(event).toEqual({
            type: "extension_ui_response_ack",
            id: requestId,
            confirmed,
          });
        }).pipe(Effect.ensuring(worker.stop()));
      }).pipe(Effect.provide(Live)),
    { fastCheck: { numRuns: 20 }, timeout: 120_000 },
  );
});

describe("RPC liveness regressions", () => {
  it.live("times out an unacknowledged command", () =>
    Effect.gen(function* () {
      const rpc = yield* RpcWorker;
      const worker = yield* rpc.spawn({
        command: [process.execPath, "run", fixture],
        cwd: process.cwd(),
        env: { FAKE_RPC_HANG_COMMAND: "get_state" },
        requestTimeoutMs: 30,
      });
      yield* worker.start();
      const result = yield* worker.getState().pipe(Effect.either);
      expect(Either.isLeft(result)).toBe(true);
      if (Either.isLeft(result)) {
        expect(result.left).toMatchObject({
          _tag: "@Gateway/RpcTimeoutError",
          method: "get_state",
        });
      }
      yield* worker.stop();
    }).pipe(Effect.provide(RpcWorker.Default)),
  );

  it.live.each([
    "crash after ready",
    "crash with inherited stdout",
    "close stdout",
  ])("emits a failure event when the worker does %s", (message) =>
    Effect.gen(function* () {
      const rpc = yield* RpcWorker;
      const worker = yield* rpc.spawn({
        command:
          message === "close stdout"
            ? [
                "sh",
                "-c",
                `printf '%s\\n' '{"type":"ready","supportedProtocolVersions":[1]}'; read request; exec 1>&-; sleep 60`,
              ]
            : [process.execPath, "run", fixture],
        cwd: process.cwd(),
      });
      const failed = yield* Deferred.make<void>();
      yield* worker.onEvent((event) => {
        if (event.type === "worker_exited")
          Deferred.unsafeDone(failed, Effect.void);
      });
      yield* worker.start();
      const prompt = yield* Effect.fork(
        worker.prompt(message).pipe(Effect.ignore),
      );
      yield* Deferred.await(failed).pipe(Effect.timeout("2 seconds"));
      yield* Fiber.join(prompt);
      const state = yield* worker.getState().pipe(Effect.either);
      expect(Either.isLeft(state)).toBe(true);
      yield* worker.stop();
    }).pipe(Effect.provide(RpcWorker.Default)),
  );
});

describe("Worker process tree cleanup", () => {
  it.live("stops a tool child along with its OMP parent", () =>
    Effect.gen(function* () {
      const rpc = yield* RpcWorker;
      const worker = yield* rpc.spawn({
        command: [
          "sh",
          "-c",
          `trap 'wait; exit 0' TERM; printf '%s\\n' '{"type":"ready","supportedProtocolVersions":[1]}'; sleep 60 & child=$!; printf '{"type":"child_spawned","pid":%s}\\n' "$child"; while read request; do :; done; wait`,
        ],
        cwd: process.cwd(),
      });
      const child = yield* Deferred.make<number>();
      yield* worker.onEvent((event) => {
        if (event.type === "child_spawned" && typeof event.pid === "number")
          Deferred.unsafeDone(child, Effect.succeed(event.pid));
      });
      yield* worker.start();
      const childPid = yield* Deferred.await(child).pipe(
        Effect.timeout("1 second"),
      );
      yield* worker.stop();
      const alive = yield* Effect.try(() => process.kill(childPid, 0)).pipe(
        Effect.either,
      );
      expect(Either.isLeft(alive)).toBe(true);
    }).pipe(Effect.provide(RpcWorker.Default)),
  );

  it.live("logs the gateway session on worker failure", () => {
    const sessionIds: unknown[] = [];
    const logger = Logger.make((options) => {
      if (String(options.message).includes("worker.failed"))
        sessionIds.push(
          Option.getOrNull(HashMap.get(options.annotations, "sessionId")),
        );
    });
    return Effect.gen(function* () {
      const rpc = yield* RpcWorker;
      const worker = yield* rpc.spawn({
        command: [process.execPath, "run", fixture],
        cwd: process.cwd(),
        sessionId: "gateway-session",
      });
      const failed = yield* Deferred.make<void>();
      yield* worker.onEvent((event) => {
        if (event.type === "worker_exited")
          Deferred.unsafeDone(failed, Effect.void);
      });
      yield* worker.start();
      yield* worker.prompt("crash after ready").pipe(Effect.ignore);
      yield* Deferred.await(failed).pipe(Effect.timeout("1 second"));
      expect(sessionIds).toEqual(["gateway-session"]);
    }).pipe(
      Effect.provide(RpcWorker.Default),
      Effect.provide(Logger.replace(Logger.defaultLogger, logger)),
    );
  });
});

describe("Worker fiber ownership", () => {
  it.live("keeps worker IO alive after the caller's startup fiber exits", () =>
    Effect.gen(function* () {
      const worker = yield* RpcWorker.spawn({
        command: [process.execPath, "run", fixture],
        cwd: process.cwd(),
      });
      const startup = yield* Effect.fork(worker.start());
      yield* Fiber.join(startup);
      expect(yield* worker.getState()).toMatchObject({ sessionId: "omp-test" });
      yield* worker.stop();
    }).pipe(Effect.provide(RpcWorker.Default)),
  );
});

describe("Bounded worker shutdown registrations", () => {
  it.live("keeps only active workers in gateway shutdown cleanup", () => {
    let scopeCleanups = 0;
    const logger = Logger.make((options) => {
      if (String(options.message) === "worker.scope_cleanup") scopeCleanups++;
    });
    return Effect.gen(function* () {
      yield* Effect.gen(function* () {
        for (let i = 0; i < 10; i++) {
          const worker = yield* RpcWorker.spawn({
            command: [process.execPath, "run", fixture],
            cwd: process.cwd(),
          });
          yield* worker.start();
          yield* worker.stop();
        }
        const active = yield* RpcWorker.spawn({
          command: [process.execPath, "run", fixture],
          cwd: process.cwd(),
        });
        yield* active.start();
      }).pipe(Effect.provide(RpcWorker.Default));
      expect(scopeCleanups).toBe(1);
    }).pipe(
      Effect.provide(Logger.replace(Logger.defaultLogger, logger)),
      Logger.withMinimumLogLevel(LogLevel.Debug),
    );
  });
});
