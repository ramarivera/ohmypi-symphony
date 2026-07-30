import { randomUUID } from "node:crypto";
import type {
  LinearActivityContent,
  LinearGatewayPort,
  RpcEvent,
} from "./domain";
import type { GatewayStore, ProjectionJob } from "./store";

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

export interface LinearPlanItem {
  readonly content: string;
  readonly status: "pending" | "inProgress" | "completed" | "canceled";
}

const PROJECTION_LEASE_MS = 30_000;
const MAX_PROJECTION_BACKOFF_MS = 5 * 60_000;

function boundedText(value: string, maxLength = 8_000): string {
  return value.length <= maxLength
    ? value
    : `${value.slice(0, maxLength)}\n[truncated]`;
}

function projectionError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isActivityType(
  value: unknown,
): value is LinearActivityContent["type"] {
  return (
    value === "thought" ||
    value === "action" ||
    value === "elicitation" ||
    value === "response" ||
    value === "error"
  );
}

function isActivitySignal(
  value: unknown,
): value is "auth" | "continue" | "select" | "stop" {
  return (
    value === "auth" ||
    value === "continue" ||
    value === "select" ||
    value === "stop"
  );
}

function decodeActivityRequest(
  job: ProjectionJob,
): Parameters<LinearGatewayPort["createActivity"]>[0] {
  if (!record(job.payload))
    throw new Error(`Projection ${job.sourceKey} payload is not an object`);
  const request = job.payload.request;
  if (!record(request) || !record(request.content)) {
    throw new Error(`Projection ${job.sourceKey} activity request is invalid`);
  }
  const type = request.content.type;
  if (!isActivityType(type)) {
    throw new Error(`Projection ${job.sourceKey} activity type is invalid`);
  }
  const body = text(request.content.body);
  const action = text(request.content.action);
  const parameter = text(request.content.parameter);
  const result = text(request.content.result);
  const content: LinearActivityContent = {
    type,
    ...(body === null ? {} : { body }),
    ...(action === null ? {} : { action }),
    ...(parameter === null ? {} : { parameter }),
    ...(result === null ? {} : { result }),
  };
  const signal = isActivitySignal(request.signal) ? request.signal : undefined;
  const signalMetadata = record(request.signalMetadata)
    ? request.signalMetadata
    : undefined;
  return {
    sessionId: job.sessionId,
    content,
    ephemeral: request.ephemeral === true,
    ...(signal === undefined ? {} : { signal }),
    ...(signalMetadata === undefined ? {} : { signalMetadata }),
  };
}

function decodeSessionUpdate(
  job: ProjectionJob,
): Parameters<LinearGatewayPort["updateSession"]>[0] {
  if (!record(job.payload) || !record(job.payload.request)) {
    throw new Error(`Projection ${job.sourceKey} session update is invalid`);
  }
  const raw = job.payload.request;
  const plan = Array.isArray(raw.plan)
    ? raw.plan.map((entry) => {
        if (!record(entry)) {
          throw new Error(`Projection ${job.sourceKey} plan item is invalid`);
        }
        const content = text(entry.content);
        const status = text(entry.status);
        if (content === null || status === null) {
          throw new Error(`Projection ${job.sourceKey} plan item is invalid`);
        }
        return { content, status };
      })
    : undefined;
  const externalUrls = Array.isArray(raw.externalUrls)
    ? raw.externalUrls.map((entry) => {
        if (!record(entry)) {
          throw new Error(
            `Projection ${job.sourceKey} external URL is invalid`,
          );
        }
        const label = text(entry.label);
        const url = text(entry.url);
        if (label === null || url === null) {
          throw new Error(
            `Projection ${job.sourceKey} external URL is invalid`,
          );
        }
        return { label, url };
      })
    : undefined;
  if (plan === undefined && externalUrls === undefined) {
    throw new Error(`Projection ${job.sourceKey} session update is empty`);
  }
  return {
    sessionId: job.sessionId,
    ...(plan === undefined ? {} : { plan }),
    ...(externalUrls === undefined ? {} : { externalUrls }),
  };
}

export class ActivityProjector {
  readonly #store: GatewayStore;
  readonly #linear: LinearGatewayPort;
  readonly #owner = `projector:${randomUUID()}`;
  readonly #assistantDraft = new Map<string, string>();

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
    return this.#activity(
      sessionId,
      sourceKey,
      { type: "thought", body: boundedText(body) },
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
    return this.#activity(
      sessionId,
      sourceKey,
      { type: "elicitation", body: boundedText(body) },
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
    return this.#activity(
      sessionId,
      `terminal:${sessionId}:${sourceKey}`,
      { type, body: boundedText(body) },
      false,
      undefined,
      undefined,
      true,
    );
  }

  async plan(
    sessionId: string,
    sourceKey: string,
    items: readonly LinearPlanItem[],
  ): Promise<boolean> {
    const normalized = items.map((item) => ({
      content: item.content,
      status: item.status,
    }));
    return this.#sessionUpdate(sessionId, sourceKey, "plan", {
      sessionId,
      plan: normalized,
    });
  }

  async externalUrls(
    sessionId: string,
    sourceKey: string,
    urls: readonly { label: string; url: string }[],
  ): Promise<boolean> {
    const normalized = urls.map((entry) => {
      const parsed = new URL(entry.url);
      if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
        throw new Error(`Unsupported external URL protocol ${parsed.protocol}`);
      }
      return { label: entry.label, url: parsed.toString() };
    });
    return this.#sessionUpdate(sessionId, sourceKey, "externalUrls", {
      sessionId,
      externalUrls: normalized,
    });
  }

  async flushPending(limit = 50, now = Date.now()): Promise<number> {
    let completed = 0;
    for (const sourceKey of this.#store.listDueProjectionKeys(now, limit)) {
      if (await this.#dispatch(sourceKey, now)) completed += 1;
    }
    return completed;
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
      await this.#activity(
        sessionId,
        sourceKey,
        {
          type: "action",
          action: toolName,
          parameter: boundedText(parameter, 4_000),
        },
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
      await this.#activity(
        sessionId,
        sourceKey,
        {
          type: "action",
          action: toolName,
          parameter: "completed",
          result: boundedText(result, 8_000),
        },
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
      if (body) this.#assistantDraft.set(sessionId, body);
      return;
    }
    if (event.type === "agent_end" && event.willContinue !== true) {
      const body =
        this.#assistantDraft.get(sessionId) ?? "OhMyPi run completed.";
      this.#assistantDraft.delete(sessionId);
      await this.terminal(sessionId, sourceKey, "response", body);
      return;
    }
    if (event.type === "error") {
      this.#assistantDraft.delete(sessionId);
      await this.terminal(
        sessionId,
        sourceKey,
        "error",
        text(event.message) ?? "OhMyPi worker failed",
      );
    }
  }

  async #activity(
    sessionId: string,
    sourceKey: string,
    content: LinearActivityContent,
    ephemeral: boolean,
    signal?: "auth" | "continue" | "select" | "stop",
    signalMetadata?: Record<string, unknown>,
    firstWriteWins = false,
  ): Promise<boolean> {
    const request: Parameters<LinearGatewayPort["createActivity"]>[0] = {
      sessionId,
      content,
      ephemeral,
    };
    if (signal !== undefined) request.signal = signal;
    if (signalMetadata !== undefined) request.signalMetadata = signalMetadata;
    return this.#enqueueAndDispatch(
      sessionId,
      sourceKey,
      content.type,
      { request },
      firstWriteWins,
    );
  }

  async #sessionUpdate(
    sessionId: string,
    sourceKey: string,
    activityType: "plan" | "externalUrls",
    request: Parameters<LinearGatewayPort["updateSession"]>[0],
  ): Promise<boolean> {
    return this.#enqueueAndDispatch(sessionId, sourceKey, activityType, {
      request,
    });
  }

  async #enqueueAndDispatch(
    sessionId: string,
    sourceKey: string,
    activityType: string,
    payload: unknown,
    firstWriteWins = false,
  ): Promise<boolean> {
    const serialized = JSON.stringify(payload);
    const payloadHash = await sha256(serialized);
    this.#store.enqueueProjection({
      sourceKey,
      sessionId,
      activityType,
      payloadHash,
      payload,
      firstWriteWins,
    });
    return this.#dispatch(sourceKey);
  }

  async #dispatch(sourceKey: string, now = Date.now()): Promise<boolean> {
    const job = this.#store.claimProjection(
      sourceKey,
      this.#owner,
      PROJECTION_LEASE_MS,
      now,
    );
    if (!job) return false;
    try {
      let activityId: string | null = null;
      if (job.activityType === "plan" || job.activityType === "externalUrls") {
        await this.#linear.updateSession(decodeSessionUpdate(job));
      } else {
        activityId = await this.#linear.createActivity(
          decodeActivityRequest(job),
        );
      }
      this.#store.completeProjection(sourceKey, this.#owner, activityId);
      this.#store.updateRun(job.sessionId, { lastActivityAt: Date.now() });
      return true;
    } catch (error) {
      const delay = Math.min(
        MAX_PROJECTION_BACKOFF_MS,
        1_000 * 2 ** Math.min(job.attempt - 1, 8),
      );
      this.#store.failProjection(
        sourceKey,
        this.#owner,
        projectionError(error),
        now + delay,
      );
      return false;
    }
  }
}
