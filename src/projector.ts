import type {
  LinearActivityContent,
  LinearGatewayPort,
  RpcEvent,
} from "./domain";
import type { GatewayStore } from "./store";

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

async function sha256(value: string): Promise<string> {
  return Buffer.from(
    await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value)),
  ).toString("hex");
}

function assistantText(event: RpcEvent): string | null {
  if (!record(event.message)) return null;
  const role = text(event.message.role);
  if (role !== "assistant") return null;
  const content = event.message.content;
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return null;
  const parts: string[] = [];
  for (const item of content) {
    if (record(item) && item.type === "text" && typeof item.text === "string")
      parts.push(item.text);
  }
  return parts.length > 0 ? parts.join("\n") : null;
}

export class ActivityProjector {
  readonly #store: GatewayStore;
  readonly #linear: LinearGatewayPort;

  constructor(store: GatewayStore, linear: LinearGatewayPort) {
    this.#store = store;
    this.#linear = linear;
  }

  async thought(
    sessionId: string,
    sourceKey: string,
    body: string,
    ephemeral = true,
  ): Promise<boolean> {
    return this.#project(
      sessionId,
      sourceKey,
      { type: "thought", body },
      ephemeral,
    );
  }

  async elicitation(
    sessionId: string,
    sourceKey: string,
    body: string,
    options?: readonly string[],
  ): Promise<boolean> {
    const signalMetadata = options ? { options: [...options] } : undefined;
    return this.#project(
      sessionId,
      sourceKey,
      { type: "elicitation", body },
      false,
      options ? "select" : undefined,
      signalMetadata,
    );
  }

  async terminal(
    sessionId: string,
    sourceKey: string,
    type: "response" | "error",
    body: string,
  ): Promise<boolean> {
    if (
      this.#store.projectionCount(sessionId, "response") +
        this.#store.projectionCount(sessionId, "error") >
      0
    )
      return false;
    return this.#project(sessionId, sourceKey, { type, body }, false);
  }

  async projectRpcEvent(
    sessionId: string,
    sequence: number,
    event: RpcEvent,
  ): Promise<void> {
    const sourceKey = `rpc:${sessionId}:${sequence}:${event.type}`;
    if (event.type === "agent_start" || event.type === "turn_start") {
      await this.thought(
        sessionId,
        sourceKey,
        event.type === "agent_start"
          ? "OhMyPi worker started"
          : "Starting the next agent turn",
      );
      return;
    }
    if (event.type === "tool_execution_start") {
      const toolName = text(event.toolName) ?? text(event.tool) ?? "tool";
      const parameter =
        typeof event.args === "string"
          ? event.args
          : JSON.stringify(event.args ?? {});
      await this.#project(
        sessionId,
        sourceKey,
        { type: "action", action: toolName, parameter },
        true,
      );
      return;
    }
    if (event.type === "tool_execution_end") {
      const toolName = text(event.toolName) ?? text(event.tool) ?? "tool";
      const result =
        typeof event.result === "string"
          ? event.result
          : JSON.stringify(event.result ?? {});
      await this.#project(
        sessionId,
        sourceKey,
        { type: "action", action: toolName, parameter: "completed", result },
        false,
      );
      return;
    }
    if (event.type === "extension_ui_request") {
      const title =
        text(event.title) ?? text(event.message) ?? "Input required";
      const options = Array.isArray(event.options)
        ? event.options.filter(
            (item): item is string => typeof item === "string",
          )
        : undefined;
      await this.elicitation(sessionId, sourceKey, title, options);
      return;
    }
    if (event.type === "message_end") {
      const body = assistantText(event);
      if (body) await this.terminal(sessionId, sourceKey, "response", body);
      return;
    }
    if (event.type === "error") {
      await this.terminal(
        sessionId,
        sourceKey,
        "error",
        text(event.message) ?? "OhMyPi worker failed",
      );
    }
  }

  async #project(
    sessionId: string,
    sourceKey: string,
    content: LinearActivityContent,
    ephemeral: boolean,
    signal?: "auth" | "continue" | "select" | "stop",
    signalMetadata?: Record<string, unknown>,
  ): Promise<boolean> {
    const serialized = JSON.stringify({
      content,
      ephemeral,
      signal,
      signalMetadata,
    });
    const payloadHash = await sha256(serialized);
    if (
      !this.#store.reserveProjection({
        sourceKey,
        sessionId,
        activityType: content.type,
        payloadHash,
      })
    )
      return false;
    const request: Parameters<LinearGatewayPort["createActivity"]>[0] = {
      sessionId,
      content,
      ephemeral,
    };
    if (signal !== undefined) request.signal = signal;
    if (signalMetadata !== undefined) request.signalMetadata = signalMetadata;
    const activityId = await this.#linear.createActivity(request);
    this.#store.completeProjection(sourceKey, activityId);
    this.#store.updateRun(sessionId, { lastActivityAt: Date.now() });
    return true;
  }
}
