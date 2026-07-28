import { describe, expect, test } from "bun:test";
import { join } from "node:path";
import type { RpcEvent } from "../src/domain";
import { OhMyPiRpcWorker } from "../src/rpc-worker";

describe("OhMyPiRpcWorker", () => {
  test("negotiates, correlates commands, streams events, and strips Linear credentials", async () => {
    const worker = new OhMyPiRpcWorker({
      command: [
        process.execPath,
        join(import.meta.dir, "fixtures/fake-rpc.ts"),
      ],
      cwd: process.cwd(),
      env: {
        PATH: Bun.env.PATH,
        HOME: Bun.env.HOME,
        LINEAR_CLIENT_SECRET: "must-not-leak",
      },
      startTimeoutMs: 2_000,
    });
    const events: Array<Record<string, unknown>> = [];
    const messageEnded = Promise.withResolvers<void>();
    worker.onEvent((event) => {
      events.push(event);
      if (event.type === "message_end") messageEnded.resolve();
    });
    await worker.start();
    await worker.prompt("perform fixture task");
    await messageEnded.promise;
    expect(events.some((event) => event.type === "agent_start")).toBeTrue();
    expect(
      events.find((event) => event.type === "agent_start")?.linearSecretPresent,
    ).toBeFalse();
    expect(events.some((event) => event.type === "message_end")).toBeTrue();
    expect(await worker.getState()).toMatchObject({
      sessionId: "omp-test",
      sessionFile: "/safe/session.jsonl",
    });
    expect(worker.sessionId).toBe("omp-test");
    await worker.stop();
  });
  test("surfaces a late prompt scheduling failure as an agent error event", async () => {
    const worker = new OhMyPiRpcWorker({
      command: [
        process.execPath,
        join(import.meta.dir, "fixtures/fake-rpc.ts"),
      ],
      cwd: process.cwd(),
      env: Bun.env,
    });
    const failure = Promise.withResolvers<RpcEvent>();
    worker.onEvent((event) => {
      if (event.type === "error") failure.resolve(event);
    });
    await worker.start();
    expect(await worker.prompt("late failure")).toBeTrue();
    expect(await failure.promise).toMatchObject({
      type: "error",
      message: "late scheduling failure",
    });
    await worker.stop();
  });
});
