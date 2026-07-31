import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Option } from "effect";
import { RpcWorker } from "../src/services/rpc-worker.js";

const fixture = new URL("./fixtures/fake-rpc.ts", import.meta.url).pathname;

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
});
