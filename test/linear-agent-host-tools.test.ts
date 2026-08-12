import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect } from "effect";
import { type RpcHostToolCall, RpcWorker } from "../src/services/rpc-worker.js";
import { extractPullRequestUrls } from "../src/services/session-authority.js";

const hostToolFixture = (mode: "valid" | "malformed") => {
  const hostToolCalls =
    mode === "malformed"
      ? `send({ type: "host_tool_call", id: "bad-1", toolCallId: "tool-bad" });
        send({ type: "host_tool_call", id: "call-2", toolCallId: "tool-2", toolName: "linear_test", arguments: {} });`
      : `send({ type: "host_tool_call", id: "call-1", toolCallId: "tool-1", toolName: "linear_test", arguments: { value: "ok" } });`;
  const resultSeen =
    mode === "malformed"
      ? `send({ type: "host_tool_result_seen", id: input.id, isError: input.isError === true, result: input.result });`
      : `send({ type: "host_tool_result_seen", id: input.id, result: input.result });`;
  return String.raw`
const send = (value) => process.stdout.write(JSON.stringify(value) + "\n");
send({ type: "ready", protocolVersion: 2, supportedProtocolVersions: [1, 2] });
const reader = Bun.stdin.stream().getReader();
const decoder = new TextDecoder();
let buffer = "";
while (true) {
  const result = await reader.read();
  if (result.done) break;
  buffer += decoder.decode(result.value, { stream: true });
  let newline = buffer.indexOf("\n");
  while (newline >= 0) {
    const line = buffer.slice(0, newline); buffer = buffer.slice(newline + 1);
    if (line) {
      const input = JSON.parse(line);
      if (input.type === "negotiate_protocol") send({ type: "response", id: input.id, success: true });
      if (input.type === "set_host_tools") {
        send({ type: "response", id: input.id, command: input.type, success: true, data: { toolNames: input.tools.map((t) => t.name) } });
        ${hostToolCalls}
      }
      if (input.type === "host_tool_result") {
        ${resultSeen}
      }
    }
    newline = buffer.indexOf("\n");
  }
}
`;
};

describe("Linear host tools", () => {
  it.effect("round-trips host tool calls with correlation id and result", () =>
    Effect.gen(function* () {
      const rpc = yield* RpcWorker;
      const worker = yield* rpc.spawn({
        command: ["bun", "-e", hostToolFixture("valid")],
        cwd: process.cwd(),
      });
      yield* Effect.gen(function* () {
        yield* worker.start();
        const seen = yield* Deferred.make<void>();
        const requests: RpcHostToolCall[] = [];
        const echoed: Array<{
          readonly type: string;
          readonly id?: unknown;
          readonly result?: unknown;
        }> = [];
        yield* worker.onEvent((event) => {
          if (event.type === "host_tool_result_seen") {
            echoed.push(event);
            if (event.id === "call-1") Deferred.unsafeDone(seen, Effect.void);
          }
        });
        yield* worker.onHostToolCall((request) =>
          Effect.sync(() => {
            requests.push(request);
            return { content: [{ type: "text", text: "done" }] };
          }),
        );
        yield* worker.setHostTools([
          {
            name: "linear_test",
            description: "test",
            parameters: { type: "object", additionalProperties: true },
          },
        ]);
        yield* Deferred.await(seen).pipe(Effect.timeout("5 seconds"));
        expect(requests).toHaveLength(1);
        expect(requests[0]).toMatchObject({
          id: "call-1",
          toolCallId: "tool-1",
          arguments: { value: "ok" },
        });
        expect(echoed).toEqual([
          expect.objectContaining({
            type: "host_tool_result_seen",
            id: "call-1",
          }),
        ]);
      }).pipe(Effect.ensuring(worker.stop()));
    }).pipe(Effect.provide(RpcWorker.Default)),
  );

  it("extracts only https PR/MR URLs, deduping and capping", () => {
    expect(
      extractPullRequestUrls(
        "https://github.com/acme/repo/pull/12 https://github.com/acme/repo/issues/3 http://github.com/acme/repo/pull/9 https://gitlab.com/a/b/-/merge_requests/4 https://gitlab.com/a/b/-/merge_requests/4",
      ),
    ).toEqual([
      "https://github.com/acme/repo/pull/12",
      "https://gitlab.com/a/b/-/merge_requests/4",
    ]);
  });

  it.effect(
    "malformed host_tool_call gets an error result and the worker survives",
    () =>
      Effect.gen(function* () {
        const rpc = yield* RpcWorker;
        const worker = yield* rpc.spawn({
          command: ["bun", "-e", hostToolFixture("malformed")],
          cwd: process.cwd(),
        });
        yield* Effect.gen(function* () {
          yield* worker.start();
          const seen = yield* Deferred.make<void>();
          const echoed: Array<{
            readonly type: string;
            readonly id?: unknown;
            readonly isError?: unknown;
          }> = [];
          yield* worker.onEvent((event) => {
            if (event.type === "host_tool_result_seen") {
              echoed.push(event);
              // Await BOTH echoes: the malformed bad-1 result and the valid
              // call-2 result may complete in either order.
              if (
                echoed.some((entry) => entry.id === "bad-1") &&
                echoed.some((entry) => entry.id === "call-2")
              ) {
                Deferred.unsafeDone(seen, Effect.void);
              }
            }
          });
          yield* worker.onHostToolCall(() =>
            Effect.succeed({ content: [{ type: "text", text: "done" }] }),
          );
          yield* worker.setHostTools([
            {
              name: "linear_test",
              description: "test",
              parameters: { type: "object", additionalProperties: true },
            },
          ]);
          yield* Deferred.await(seen).pipe(Effect.timeout("5 seconds"));
          const bad = echoed.find((event) => event.id === "bad-1");
          const good = echoed.find((event) => event.id === "call-2");
          expect(bad?.isError).toBe(true);
          expect(good?.isError).toBe(false);
        }).pipe(Effect.ensuring(worker.stop()));
      }).pipe(Effect.provide(RpcWorker.Default)),
  );
});
