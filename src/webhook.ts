import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import type { GatewayConfig, InputKind } from "./domain";
import type { GatewayStore } from "./store";

const LINEAR_SIGNATURE_HEADER = "linear-signature";
const LINEAR_TIMESTAMP_HEADER = "linear-timestamp";
const LINEAR_DELIVERY_HEADER = "linear-delivery";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function isStringArray(value: unknown): value is readonly string[] {
  return Array.isArray(value) && value.every(isString);
}

function isBoolean(value: unknown): value is boolean {
  return typeof value === "boolean";
}

function requireString(value: Record<string, unknown>, key: string): string {
  const field = value[key];
  if (!isString(field))
    throw new Error(`Webhook payload missing or invalid ${key}`);
  return field;
}

function optionalStringField(
  value: Record<string, unknown>,
  key: string,
): string | undefined {
  const field = value[key];
  if (field === undefined || field === null) return undefined;
  if (!isString(field))
    throw new Error(`Webhook payload field ${key} is not a string`);
  return field;
}

function optionalStringOrNullField(
  value: Record<string, unknown>,
  key: string,
): string | null {
  const field = value[key];
  if (field === undefined || field === null) return null;
  if (!isString(field))
    throw new Error(`Webhook payload field ${key} is not a string or null`);
  return field;
}

function extractBody(content: unknown): string {
  if (!isRecord(content))
    throw new Error("Webhook activity content is not an object");
  const body = content.body;
  if (body === undefined || body === null) return "";
  if (!isString(body))
    throw new Error("Webhook activity content body is not a string");
  return body;
}

type AgentSessionEventPayload = {
  readonly action: string;
  readonly raw: Record<string, unknown>;
  readonly agentSession: {
    readonly id: string;
    readonly organizationId: string;
    readonly issueId: string | null;
    readonly teamId: string | null;
    readonly projectId: string | null;
    readonly summary?: string | undefined;
    readonly raw: Record<string, unknown>;
  };
  readonly agentActivity?:
    | {
        readonly id: string;
        readonly agentSessionId: string;
        readonly content: unknown;
        readonly signal: string | null;
        readonly raw: Record<string, unknown>;
      }
    | undefined;
  readonly promptContext?: string | undefined;
  readonly organizationId: string;
  readonly webhookId: string;
};

type OAuthAppPayload = {
  readonly action: string;
  readonly oauthClientId: string;
  readonly organizationId: string;
  readonly webhookId: string;
};

type PermissionChangePayload = {
  readonly action: string;
  readonly oauthClientId: string;
  readonly appUserId: string;
  readonly addedTeamIds: readonly string[];
  readonly removedTeamIds: readonly string[];
  readonly canAccessAllPublicTeams: boolean;
  readonly organizationId: string;
  readonly webhookId: string;
};

function asAgentSessionEvent(
  value: Record<string, unknown>,
): AgentSessionEventPayload {
  const action = requireString(value, "action");
  const organizationId = requireString(value, "organizationId");
  const webhookId = requireString(value, "webhookId");

  const rawSession = value.agentSession;
  if (!isRecord(rawSession))
    throw new Error("AgentSessionEvent missing agentSession");

  const sessionId = requireString(rawSession, "id");
  const sessionOrganizationId = requireString(rawSession, "organizationId");
  const issueId = optionalStringOrNullField(rawSession, "issueId");
  const summary = optionalStringField(rawSession, "summary");
  const promptContext = optionalStringField(value, "promptContext");
  const rawIssue = rawSession.issue;
  const teamId = isRecord(rawIssue)
    ? optionalStringOrNullField(rawIssue, "teamId")
    : null;
  const projectId = isRecord(rawIssue)
    ? optionalStringOrNullField(rawIssue, "projectId")
    : null;

  const rawActivity = value.agentActivity;
  let agentActivity: AgentSessionEventPayload["agentActivity"] | undefined;
  if (rawActivity !== undefined && rawActivity !== null) {
    if (!isRecord(rawActivity))
      throw new Error("AgentSessionEvent agentActivity is not an object");
    const activityId = requireString(rawActivity, "id");
    const activitySessionId = requireString(rawActivity, "agentSessionId");
    const content = rawActivity.content;
    const signal = optionalStringOrNullField(rawActivity, "signal");
    agentActivity = {
      id: activityId,
      agentSessionId: activitySessionId,
      content,
      signal,
      raw: rawActivity,
    };
  }

  return {
    action,
    raw: value,
    agentSession: {
      id: sessionId,
      organizationId: sessionOrganizationId,
      issueId,
      summary,
      teamId,
      projectId,
      raw: rawSession,
    },
    agentActivity,
    promptContext,
    organizationId,
    webhookId,
  };
}

function asOAuthAppPayload(value: Record<string, unknown>): OAuthAppPayload {
  return {
    action: requireString(value, "action"),
    oauthClientId: requireString(value, "oauthClientId"),
    organizationId: requireString(value, "organizationId"),
    webhookId: requireString(value, "webhookId"),
  };
}

function asPermissionChangePayload(
  value: Record<string, unknown>,
): PermissionChangePayload {
  const added = value.addedTeamIds;
  const removed = value.removedTeamIds;
  if (!isStringArray(added) || !isStringArray(removed)) {
    throw new Error("PermissionChange missing or invalid team id arrays");
  }
  const canAccess = value.canAccessAllPublicTeams;
  if (!isBoolean(canAccess))
    throw new Error("PermissionChange missing canAccessAllPublicTeams");

  return {
    action: requireString(value, "action"),
    oauthClientId: requireString(value, "oauthClientId"),
    appUserId: requireString(value, "appUserId"),
    addedTeamIds: added,
    removedTeamIds: removed,
    canAccessAllPublicTeams: canAccess,
    organizationId: requireString(value, "organizationId"),
    webhookId: requireString(value, "webhookId"),
  };
}

function verifySignature(
  rawBody: Buffer,
  signature: string,
  secret: string,
): boolean {
  const expected = createHmac("sha256", secret).update(rawBody).digest("hex");
  const normalized = signature.toLowerCase();
  if (expected.length !== normalized.length) return false;

  const a = Buffer.from(expected, "utf-8");
  const b = Buffer.from(normalized, "utf-8");
  if (a.length !== b.length) return false;

  try {
    return timingSafeEqual(a, b);
  } catch {
    return false;
  }
}

function payloadHash(rawBody: Buffer): string {
  return createHash("sha256").update(rawBody).digest("hex");
}

function createInputBodyForCreated(event: AgentSessionEventPayload): string {
  if (isString(event.promptContext)) return event.promptContext;
  if (isString(event.agentSession.summary)) return event.agentSession.summary;
  return "";
}

function handleAgentSessionEvent(
  event: AgentSessionEventPayload,
  store: GatewayStore,
  now: number,
): void {
  const sessionId = event.agentSession.id;
  const organizationId = event.agentSession.organizationId;
  const issueId = event.agentSession.issueId;
  const teamId = event.agentSession.teamId;
  const projectId = event.agentSession.projectId;

  if (event.action === "created") {
    store.createRun({
      sessionId,
      organizationId,
      issueId,
      teamId,
      projectId,
      now,
    });
    const body = createInputBodyForCreated(event);
    store.enqueueInput({
      id: event.webhookId,
      sessionId,
      kind: "created",
      body,
      payload: event.raw,
      createdAt: now,
    });
    return;
  }

  if (event.action === "prompted") {
    if (!event.agentActivity)
      throw new Error("AgentSessionEvent prompted missing agentActivity");
    if (event.agentActivity.agentSessionId !== sessionId) {
      throw new Error("AgentSessionEvent prompted activity session mismatch");
    }
    store.createRun({
      sessionId,
      organizationId,
      issueId,
      teamId,
      projectId,
      now,
    });
    const body = extractBody(event.agentActivity.content);
    const kind: InputKind =
      event.agentActivity.signal === "stop" ? "stop" : "prompted";
    store.enqueueInput({
      id: event.webhookId,
      sessionId,
      kind,
      body,
      payload: event.raw,
      createdAt: now,
    });
    return;
  }

  if (event.action === "stop") {
    store.createRun({
      sessionId,
      organizationId,
      issueId,
      teamId,
      projectId,
      now,
    });
    const body = createInputBodyForCreated(event);
    store.enqueueInput({
      id: event.webhookId,
      sessionId,
      kind: "stop",
      body,
      payload: event.raw,
      createdAt: now,
    });
    return;
  }
}

function handleOAuthAppRevoked(
  payload: OAuthAppPayload,
  store: GatewayStore,
  config: GatewayConfig,
  now: number,
): void {
  if (payload.oauthClientId !== config.linearClientId) return;
  if (payload.action !== "revoked" && payload.action !== "revoke") return;
  store.revokeInstallation(payload.organizationId, now);
}

async function handlePermissionChange(
  payload: PermissionChangePayload,
  store: GatewayStore,
  config: GatewayConfig,
  now: number,
): Promise<void> {
  if (payload.oauthClientId !== config.linearClientId) return;
  if (payload.action !== "teamAccessChanged") return;
  await store.applyPermissionChange(
    payload.organizationId,
    payload.appUserId,
    payload.addedTeamIds,
    payload.removedTeamIds,
    payload.canAccessAllPublicTeams,
    now,
  );
}

export async function handleWebhook(
  request: Request,
  config: GatewayConfig,
  store: GatewayStore,
): Promise<Response> {
  if (request.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }

  let rawBody: Buffer;
  try {
    rawBody = Buffer.from(await request.arrayBuffer());
  } catch {
    return new Response("Unable to read body", { status: 400 });
  }

  const headers = request.headers;
  const signature = headers.get(LINEAR_SIGNATURE_HEADER);
  const timestampHeader = headers.get(LINEAR_TIMESTAMP_HEADER);
  const deliveryId = headers.get(LINEAR_DELIVERY_HEADER);
  if (!signature) return new Response("Missing signature", { status: 400 });
  if (!verifySignature(rawBody, signature, config.linearWebhookSecret)) {
    return new Response("Invalid signature", { status: 401 });
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(rawBody.toString("utf-8"));
  } catch {
    return new Response("Invalid JSON", { status: 400 });
  }
  if (!isRecord(parsed))
    return new Response("Payload is not an object", { status: 400 });

  const payloadTimestamp = parsed.webhookTimestamp;
  const timestamp =
    typeof payloadTimestamp === "number"
      ? payloadTimestamp
      : Number(timestampHeader);
  if (!Number.isSafeInteger(timestamp))
    return new Response("Invalid timestamp", { status: 400 });
  const now = Date.now();
  if (Math.abs(now - timestamp) > config.webhookReplayWindowMs) {
    return new Response("Webhook timestamp outside replay window", {
      status: 401,
    });
  }

  const organizationId = parsed.organizationId;
  const webhookId = parsed.webhookId;
  const eventType = parsed.type;
  if (
    !isString(organizationId) ||
    !isString(webhookId) ||
    !isString(eventType)
  ) {
    return new Response("Payload missing organizationId, webhookId, or type", {
      status: 400,
    });
  }

  const delivery = deliveryId ?? webhookId;
  const hash = payloadHash(rawBody);
  const accepted = store.acceptDelivery({
    id: delivery,
    organizationId,
    payloadHash: hash,
    payload: parsed,
    receivedAt: now,
  });
  if (!accepted) {
    return new Response("Duplicate delivery", { status: 200 });
  }

  try {
    switch (eventType) {
      case "AgentSessionEvent": {
        const event = asAgentSessionEvent(parsed);
        handleAgentSessionEvent(event, store, now);
        break;
      }
      case "OAuthApp": {
        const payload = asOAuthAppPayload(parsed);
        handleOAuthAppRevoked(payload, store, config, now);
        break;
      }
      case "PermissionChange": {
        const payload = asPermissionChangePayload(parsed);
        await handlePermissionChange(payload, store, config, now);
        break;
      }
    }
    store.markDelivery(delivery, "processed");
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Webhook processing failed";
    store.markDelivery(delivery, "failed", message);
    return new Response(message, { status: 500 });
  }

  return new Response("OK", { status: 200 });
}
