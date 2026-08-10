import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect } from "effect";
import { RpcWorker } from "../src/services/rpc-worker.js";
import { extractPullRequestUrls } from "../src/services/session-authority.js";

const hostToolFixture = String.raw`
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
        send({ type: "host_tool_call", id: "call-1", toolCallId: "tool-1", toolName: "linear_test", arguments: { value: "ok" } });
      }
      if (input.type === "host_tool_result") send({ type: "host_tool_result_seen", id: input.id, result: input.result });
    }
    newline = buffer.indexOf("\n");
  }
}
`;

describe("Linear host tools", () => {
  it.effect("round-trips host tool calls with correlation id and result", () =>
    Effect.gen(function* () {
      const rpc = yield* RpcWorker;
      const worker = yield* rpc.spawn({
        command: ["bun", "-e", hostToolFixture],
        cwd: process.cwd(),
      });
      yield* worker.start();
      const seen = yield* Deferred.make<void>();
      yield* worker.onEvent((event) => {
        if (event.type === "host_tool_result_seen" && event.id === "call-1")
          Deferred.unsafeDone(seen, Effect.void);
      });
      yield* worker.onHostToolCall((request) => {
        expect(request.id).toBe("call-1");
        expect(request.toolCallId).toBe("tool-1");
        expect(request.arguments).toEqual({ value: "ok" });
        return Effect.succeed({ content: [{ type: "text", text: "done" }] });
      });
      yield* worker.setHostTools([
        {
          name: "linear_test",
          description: "test",
          parameters: { type: "object" },
        },
      ]);
      yield* Deferred.await(seen);
      yield* worker.stop();
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
});
