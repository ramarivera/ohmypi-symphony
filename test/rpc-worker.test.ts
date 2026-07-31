import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Option, Schema } from "effect";
import { RpcWorker } from "../src/services/rpc-worker.js";

const fixture = new URL("./fixtures/fake-rpc.ts", import.meta.url).pathname;
const uiResponseFixture = String.raw`
const send = (value) => process.stdout.write(JSON.stringify(value) + "\n");
send({
  type: "ready",
  protocolVersion: 1,
  supportedProtocolVersions: [1],
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
      if (
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
