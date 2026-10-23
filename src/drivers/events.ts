/** Compatibility boundary for the authority's existing omp lifecycle and projector. */
import type { RpcEvent } from "../services/rpc-worker.js";
import { isRecord } from "../util/guards.js";
import type { AgentEvent, TurnOutcome } from "./driver.js";

export const agentEventToRpc = (event: AgentEvent): RpcEvent => {
  if (event.runtimeEvent !== undefined) return event.runtimeEvent;
  const base = { driverEventKey: event.key };
  switch (event._tag) {
    case "RuntimeEvent":
      return event.event;
    case "TurnStarted":
      return { ...base, type: "agent_start" };
    case "Thinking":
      return { ...base, type: "thinking", message: event.text };
    case "ToolStarted":
      return {
        ...base,
        type: "tool_execution_start",
        toolName: event.name,
        toolCallId: event.toolCallId,
        args: event.input,
      };
    case "ToolEnded":
      return {
        ...base,
        type: "tool_execution_end",
        toolName: event.name,
        toolCallId: event.toolCallId,
        result: event.result,
        error: event.isError,
      };
    case "AssistantMessage":
      return {
        ...base,
        type: "message_end",
        message: { role: "assistant", content: event.text },
      };
    case "Plan":
      return { ...base, type: "driver_plan", items: event.steps };
    case "RequestOpened":
      if (event.kind === "approval")
        return {
          ...base,
          type: "driver_approval",
          id: event.requestId,
          title: event.prompt,
        };
      return {
        ...base,
        type: "extension_ui_request",
        id: event.requestId,
        method: "input",
        title: event.prompt,
        options: event.options.map((option) => option.label),
      };
    case "RequestResolved":
      return { ...base, type: "driver_request_resolved", id: event.requestId };
    case "Error":
      return { ...base, type: "driver_warning", message: event.message };
    case "QueueHeld":
      return {
        ...base,
        type: event.held ? "error" : "driver_queue_released",
        message: "T3 queue stopped advancing",
      };
    case "TurnEnded":
      return event.outcome.status === "completed"
        ? {
            ...base,
            type: "agent_end",
            messages: [
              {
                role: "assistant",
                content: event.outcome.finalText ?? "Agent run completed.",
              },
            ],
          }
        : {
            ...base,
            type: "error",
            message:
              event.outcome.error ?? `Agent turn ${event.outcome.status}`,
          };
  }
};

const text = (value: unknown): string =>
  typeof value === "string" ? value : JSON.stringify(value ?? {});
export const ompEvent = (
  event: RpcEvent,
  key: string,
  turnId: string | null,
  outcome?: TurnOutcome,
): AgentEvent => {
  const base = { key, cursor: null, runtimeEvent: event };
  if (outcome !== undefined) return { ...base, _tag: "TurnEnded", outcome };
  switch (event.type) {
    case "message_end":
      return {
        ...base,
        _tag: "AssistantMessage",
        turnId,
        messageId: key,
        text: rpcAssistantText(event),
      };
    case "agent_start":
      return { ...base, _tag: "TurnStarted", turnId: turnId ?? key };
    case "tool_execution_start":
      return {
        ...base,
        _tag: "ToolStarted",
        turnId,
        toolCallId: text(event.toolCallId ?? key),
        name: text(event.toolName ?? event.tool),
        input: text(event.args),
      };
    case "tool_execution_end":
      return {
        ...base,
        _tag: "ToolEnded",
        turnId,
        toolCallId: text(event.toolCallId ?? key),
        name: text(event.toolName ?? event.tool),
        result: text(event.result),
        isError: event.error === true,
      };
    case "error":
      return {
        ...base,
        _tag: "Error",
        turnId,
        message: text(event.message),
        retryable: false,
      };
    default:
      return { ...base, _tag: "RuntimeEvent", event };
  }
};

export const rpcAssistantText = (event: RpcEvent): string => {
  const collect = (value: unknown): string => {
    if (typeof value === "string") return value;
    if (Array.isArray(value))
      return value.map(collect).filter(Boolean).join("\n");
    if (!isRecord(value)) return "";
    if (typeof value.text === "string") return value.text;
    return collect(value.content ?? value.message ?? value.messages);
  };
  const candidates = [
    ...(Array.isArray(event.messages) ? event.messages : []),
    event.message,
  ];
  return candidates
    .filter(
      (candidate) => !isRecord(candidate) || candidate.role === "assistant",
    )
    .map(collect)
    .filter(Boolean)
    .join("\n");
};
