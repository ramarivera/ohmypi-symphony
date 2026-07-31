import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { RpcEvent } from "../src/services/rpc-worker.js";

const rpcSourceKey = (
  sessionId: string,
  sequence: number,
  event: RpcEvent,
): string => {
  if (event.type === "extension_ui_request" && typeof event.id === "string") {
    return `rpc-ui:${event.id}`;
  }
  return `rpc:${sessionId}:${sequence}:${event.type}`;
};

const rpcEventLevel = (
  event: RpcEvent,
): "debug" | "info" | "warn" | "result" | "error" => {
  switch (event.type) {
    case "agent_start":
    case "turn_start":
    case "turn_end":
    case "tool_execution_start":
    case "message_end":
      return "info";
    case "agent_end":
      return "result";
    case "tool_execution_end":
      return event.error ? "error" : "result";
    case "prompt_result":
      return event.agentInvoked === false ? "result" : "info";
    case "error":
      return "error";
    case "extension_ui_request":
      return "warn";
    default:
      return "info";
  }
};

const cancelDominates = (desiredState: string, inputKind: string): boolean =>
  desiredState === "canceled" || inputKind === "stop";

describe("SessionAuthority behavior invariants", () => {
  it.effect.prop(
    "source key is deterministic and unique per (session, sequence, type)",
    {
      sessionId: Schema.UUID,
      sequence: Schema.Int,
      type: Schema.Literal(
        "agent_start",
        "turn_start",
        "turn_end",
        "tool_execution_start",
        "tool_execution_end",
        "message_end",
        "prompt_result",
        "error",
      ),
    },
    ({ sessionId, sequence, type }) =>
      Effect.gen(function* () {
        const event: RpcEvent = { type };
        const key = rpcSourceKey(sessionId, sequence, event);
        const key2 = rpcSourceKey(sessionId, sequence, event);
        expect(key).toBe(key2);
        expect(key).toBe(`rpc:${sessionId}:${sequence}:${type}`);
        yield* Effect.void;
      }),
  );

  it.effect.prop(
    "extension_ui_request source keys use the request id",
    {
      sessionId: Schema.UUID,
      sequence: Schema.Int,
      requestId: Schema.String,
    },
    ({ sessionId, sequence, requestId }) =>
      Effect.gen(function* () {
        const event: RpcEvent = {
          type: "extension_ui_request",
          id: requestId,
        };
        const key = rpcSourceKey(sessionId, sequence, event);
        expect(key).toBe(`rpc-ui:${requestId}`);
        yield* Effect.void;
      }),
  );

  it.effect.prop(
    "level mapping is total for known event types",
    {
      type: Schema.Literal(
        "agent_start",
        "turn_start",
        "turn_end",
        "agent_end",
        "tool_execution_start",
        "tool_execution_end",
        "message_end",
        "prompt_result",
        "error",
        "extension_ui_request",
        "progress",
      ),
    },
    ({ type }) =>
      Effect.gen(function* () {
        const event: RpcEvent = { type };
        const level = rpcEventLevel(event);
        expect(["debug", "info", "warn", "result", "error"]).toContain(level);
        yield* Effect.void;
      }),
  );

  it.effect.prop(
    "abort/cancel dominates queued prompts and stop inputs",
    {
      desiredState: Schema.Literal("running", "canceled"),
      inputKind: Schema.Literal("prompted", "stop"),
    },
    ({ desiredState, inputKind }) =>
      Effect.gen(function* () {
        const shouldCancel = cancelDominates(desiredState, inputKind);
        expect(shouldCancel).toBe(
          desiredState === "canceled" || inputKind === "stop",
        );
        yield* Effect.void;
      }),
  );
});
