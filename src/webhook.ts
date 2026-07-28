import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import type { GatewayConfig, InputKind } from "./domain";
import type { GatewayStore } from "./store";

const LINEAR_SIGNATURE_HEADER = "linear-signature";
const LINEAR_TIMESTAMP_HEADER = "linear-timestamp";
const LINEAR_DELIVERY_HEADER = "linear-delivery";

class WebhookError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
    Object.setPrototypeOf(this, WebhookError.prototype);
  }
}

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

function isNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function requireString(value: Record<string, unknown>, key: string): string {
  const field = value[key];
  if (!isString(field))
    throw new WebhookError(400, `Webhook payload missing or invalid ${key}`);
  return field;
}

function optionalStringOrNullField(
  value: Record<string, unknown>,
  key: string,
): string | null {
  const field = value[key];
  if (field === undefined || field === null) return null;
  if (!isString(field))
    throw new WebhookError(
      400,
      `Webhook payload field ${key} is not a string or null`,
    );
  return field;
}

function coerceOptionalString(
  value: Record<string, unknown>,
  key: string,
): string | null {
  const field = value[key];
  if (field === undefined || field === null) return null;
  if (isString(field)) return field;
  return null;
}

function optionalRecord(
  value: Record<string, unknown>,
  key: string,
): Record<string, unknown> | null {
  const field = value[key];
  if (field === undefined || field === null) return null;
  if (isRecord(field)) return field;
  throw new WebhookError(400, `Webhook payload field ${key} is not an object`);
}

function optionalArray(
  value: Record<string, unknown>,
  key: string,
): readonly unknown[] {
  const field = value[key];
  if (field === undefined || field === null) return [];
  if (Array.isArray(field)) return field;
  throw new WebhookError(400, `Webhook payload field ${key} is not an array`);
}

function requireRecord(
  value: Record<string, unknown>,
  key: string,
): Record<string, unknown> {
  const field = value[key];
  if (!isRecord(field))
    throw new WebhookError(400, `Webhook payload missing ${key}`);
  return field;
}

type Comment = {
  readonly id: string;
  readonly body: string;
};

type Guidance = {
  readonly body: string;
};

type Issue = {
  readonly id: string;
  readonly title: string;
  readonly description: string | null;
  readonly identifier: string | null;
  readonly url: string | null;
  readonly teamId: string | null;
  readonly projectId: string | null;
};

type AgentActivity = {
  readonly id: string;
  readonly agentSessionId: string;
  readonly content: Record<string, unknown>;
  readonly signal: string | null;
  readonly raw: Record<string, unknown>;
};

type AgentSession = {
  readonly id: string;
  readonly appUserId: string;
  readonly organizationId: string;
  readonly status: string;
  readonly type: string;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly issueId: string | null;
  readonly commentId: string | null;
  readonly sourceCommentId: string | null;
  readonly summary: string | null;
  readonly url: string | null;
  readonly archivedAt: string | null;
  readonly startedAt: string | null;
  readonly endedAt: string | null;
  readonly comment: Comment | null;
  readonly issue: Issue | null;
};

type AgentSessionEventPayload = {
  readonly action: string;
  readonly organizationId: string;
  readonly appUserId: string;
  readonly oauthClientId: string;
  readonly webhookId: string;
  readonly promptContext: string | null;
  readonly guidance: readonly Guidance[];
  readonly previousComments: readonly Comment[];
  readonly agentActivity: AgentActivity | null;
  readonly agentSession: AgentSession;
  readonly raw: Record<string, unknown>;
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

function extractComment(value: Record<string, unknown>): Comment | null {
  const body = coerceOptionalString(value, "body");
  if (body === null) return null;
  const id = coerceOptionalString(value, "id") ?? "";
  return { id, body };
}

function extractComments(value: unknown): readonly Comment[] {
  if (!Array.isArray(value)) return [];
  const result: Comment[] = [];
  for (const item of value) {
    if (!isRecord(item)) continue;
    const comment = extractComment(item);
    if (comment) result.push(comment);
  }
  return result;
}

function extractGuidance(value: unknown): readonly Guidance[] {
  if (!Array.isArray(value)) return [];
  const result: Guidance[] = [];
  for (const item of value) {
    if (!isRecord(item)) continue;
    const body = coerceOptionalString(item, "body");
    if (body) result.push({ body });
  }
  return result;
}

function extractIssue(value: Record<string, unknown>): Issue | null {
  const title = coerceOptionalString(value, "title");
  if (title === null) return null;
  const id = coerceOptionalString(value, "id") ?? "";
  const description = coerceOptionalString(value, "description") ?? null;
  const identifier = coerceOptionalString(value, "identifier") ?? null;
  const url = coerceOptionalString(value, "url") ?? null;
  const teamIdValue = coerceOptionalString(value, "teamId");
  const team = optionalRecord(value, "team");
  const teamId = teamIdValue ?? coerceOptionalString(team ?? {}, "id") ?? null;
  const projectIdValue = coerceOptionalString(value, "projectId");
  const project = optionalRecord(value, "project");
  const projectId =
    projectIdValue ?? coerceOptionalString(project ?? {}, "id") ?? null;
  return { id, title, description, identifier, url, teamId, projectId };
}

function asAgentSessionActivity(value: Record<string, unknown>): AgentActivity {
  const id = requireString(value, "id");
  const agentSessionId = requireString(value, "agentSessionId");
  const content = value.content;
  if (!isRecord(content))
    throw new WebhookError(400, "Agent activity content is not an object");
  const signal = optionalStringOrNullField(value, "signal");
  return { id, agentSessionId, content, signal, raw: value };
}

function asAgentSession(value: Record<string, unknown>): AgentSession {
  const id = requireString(value, "id");
  const appUserId = requireString(value, "appUserId");
  const organizationId = requireString(value, "organizationId");
  const status = requireString(value, "status");
  const type = requireString(value, "type");
  const createdAt = requireString(value, "createdAt");
  const updatedAt = requireString(value, "updatedAt");
  const issueId = optionalStringOrNullField(value, "issueId");
  const commentId = optionalStringOrNullField(value, "commentId");
  const sourceCommentId = optionalStringOrNullField(value, "sourceCommentId");
  const summary = optionalStringOrNullField(value, "summary");
  const url = optionalStringOrNullField(value, "url");
  const archivedAt = optionalStringOrNullField(value, "archivedAt");
  const startedAt = optionalStringOrNullField(value, "startedAt");
  const endedAt = optionalStringOrNullField(value, "endedAt");
  const comment = optionalRecord(value, "comment");
  const issue = optionalRecord(value, "issue");
  return {
    id,
    appUserId,
    organizationId,
    status,
    type,
    createdAt,
    updatedAt,
    issueId,
    commentId,
    sourceCommentId,
    summary,
    url,
    archivedAt,
    startedAt,
    endedAt,
    comment: comment ? extractComment(comment) : null,
    issue: issue ? extractIssue(issue) : null,
  };
}

function asAgentSessionEvent(
  value: Record<string, unknown>,
): AgentSessionEventPayload {
  const action = requireString(value, "action");
  const organizationId = requireString(value, "organizationId");
  const appUserId = requireString(value, "appUserId");
  const oauthClientId = requireString(value, "oauthClientId");
  const webhookId = requireString(value, "webhookId");
  const promptContext = optionalStringOrNullField(value, "promptContext");
  const guidance = extractGuidance(optionalArray(value, "guidance"));
  const previousComments = extractComments(
    optionalArray(value, "previousComments"),
  );

  const rawActivity = value.agentActivity;
  let agentActivity: AgentActivity | null = null;
  if (rawActivity !== undefined && rawActivity !== null) {
    if (!isRecord(rawActivity))
      throw new WebhookError(400, "Agent activity is not an object");
    agentActivity = asAgentSessionActivity(rawActivity);
  }

  const rawSession = requireRecord(value, "agentSession");
  const agentSession = asAgentSession(rawSession);

  return {
    action,
    organizationId,
    appUserId,
    oauthClientId,
    webhookId,
    promptContext,
    guidance,
    previousComments,
    agentActivity,
    agentSession,
    raw: value,
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
    throw new WebhookError(400, "PermissionChange missing or invalid team ids");
  }
  const canAccess = value.canAccessAllPublicTeams;
  if (!isBoolean(canAccess))
    throw new WebhookError(
      400,
      "PermissionChange missing canAccessAllPublicTeams",
    );

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

function buildFallbackDeliveryId(parsed: Record<string, unknown>): string {
  const webhookId = isString(parsed.webhookId) ? parsed.webhookId : "unknown";
  const timestamp = isNumber(parsed.webhookTimestamp)
    ? String(parsed.webhookTimestamp)
    : "0";
  const action = isString(parsed.action) ? parsed.action : "unknown";
  const organizationId = isString(parsed.organizationId)
    ? parsed.organizationId
    : "unknown";
  return `linear:${webhookId}:${timestamp}:${action}:${organizationId}`;
}

function extractPromptBody(activity: AgentActivity | null): string {
  if (!activity) return "";
  const { content } = activity;
  const rawBody = content.body;
  const rawTitle = content.title;
  const body = isString(rawBody) ? rawBody : "";
  const title = isString(rawTitle) ? rawTitle : "";
  if (title && body) return `# ${title}\n\n${body}`;
  if (body) return body;
  if (title) return title;
  return "";
}

function buildCreatedInputBody(event: AgentSessionEventPayload): string {
  const sections: string[] = [];

  const userPrompt =
    event.promptContext?.trim() || event.agentSession.summary?.trim() || "";
  if (userPrompt) {
    sections.push(`User request:\n${userPrompt}`);
  } else if (
    event.agentSession.issue !== null ||
    event.agentSession.comment !== null ||
    event.previousComments.length > 0 ||
    event.guidance.length > 0
  ) {
    sections.push("User request:\nWork on the issue below.");
  }

  const issue = event.agentSession.issue;
  if (issue) {
    const lines: string[] = [
      `Issue: ${issue.title}${issue.identifier ? ` (${issue.identifier})` : ""}`,
    ];
    if (issue.url) lines.push(`URL: ${issue.url}`);
    if (issue.teamId) lines.push(`Team: ${issue.teamId}`);
    if (issue.projectId) lines.push(`Project: ${issue.projectId}`);
    if (issue.description) lines.push(`\nDescription:\n${issue.description}`);
    sections.push(`Issue context:\n${lines.join("\n")}`);
  }

  const comment = event.agentSession.comment;
  if (comment) {
    sections.push(`Thread comment:\n${comment.body}`);
  }

  if (event.previousComments.length > 0) {
    const list = event.previousComments
      .map((c, index) => `${index + 1}. ${c.body}`)
      .join("\n");
    sections.push(`Previous comments:\n${list}`);
  }

  if (event.guidance.length > 0) {
    const list = event.guidance
      .map((g, index) => `${index + 1}. ${g.body}`)
      .join("\n");
    sections.push(`Guidance:\n${list}`);
  }

  return sections.join("\n\n");
}

function buildInputId(
  event: AgentSessionEventPayload,
  kind: InputKind,
): string {
  const activityId = event.agentActivity?.id;
  if (activityId) return `${event.agentSession.id}:${kind}:${activityId}`;
  return `${event.agentSession.id}:${kind}`;
}

async function validateAgentSessionIdentity(
  event: AgentSessionEventPayload,
  store: GatewayStore,
  config: GatewayConfig,
): Promise<void> {
  if (event.oauthClientId !== config.linearClientId) {
    throw new WebhookError(401, "OAuth client identity mismatch");
  }
  if (event.organizationId !== event.agentSession.organizationId) {
    throw new WebhookError(401, "Organization identity mismatch");
  }
  if (event.appUserId !== event.agentSession.appUserId) {
    throw new WebhookError(401, "App user identity mismatch");
  }
  const installation = await store.getInstallation(event.organizationId);
  if (!installation) {
    throw new WebhookError(401, "No installation for organization");
  }
  if (installation.revokedAt !== null) {
    throw new WebhookError(401, "Installation is revoked");
  }
  if (installation.appUserId !== event.appUserId) {
    throw new WebhookError(401, "Installation app user mismatch");
  }
}

async function validatePermissionChangeIdentity(
  payload: PermissionChangePayload,
  store: GatewayStore,
  config: GatewayConfig,
): Promise<void> {
  if (payload.oauthClientId !== config.linearClientId) {
    throw new WebhookError(401, "OAuth client identity mismatch");
  }
  const installation = await store.getInstallation(payload.organizationId);
  if (!installation) {
    throw new WebhookError(401, "No installation for organization");
  }
  if (installation.revokedAt !== null) {
    throw new WebhookError(401, "Installation is revoked");
  }
  if (installation.appUserId !== payload.appUserId) {
    throw new WebhookError(401, "App user mismatch");
  }
}

function handleAgentSessionEvent(
  event: AgentSessionEventPayload,
  store: GatewayStore,
  timestamp: number,
): void {
  const issue = event.agentSession.issue;
  const issueId = event.agentSession.issueId;
  const teamId = issue?.teamId ?? null;
  const projectId = issue?.projectId ?? null;

  store.createRun({
    sessionId: event.agentSession.id,
    organizationId: event.agentSession.organizationId,
    issueId,
    teamId,
    projectId,
    now: timestamp,
  });

  if (event.action === "created") {
    const body = buildCreatedInputBody(event);
    store.enqueueInput({
      id: buildInputId(event, "created"),
      sessionId: event.agentSession.id,
      kind: "created",
      body,
      payload: event.raw,
      createdAt: timestamp,
    });
    return;
  }

  if (event.action === "prompted") {
    if (!event.agentActivity) {
      throw new WebhookError(
        400,
        "AgentSessionEvent prompted missing agentActivity",
      );
    }
    if (event.agentActivity.agentSessionId !== event.agentSession.id) {
      throw new WebhookError(
        401,
        "AgentSessionEvent prompted activity session mismatch",
      );
    }
    const body = extractPromptBody(event.agentActivity);
    const kind: InputKind =
      event.agentActivity.signal === "stop" ? "stop" : "prompted";
    store.enqueueInput({
      id: buildInputId(event, kind),
      sessionId: event.agentSession.id,
      kind,
      body,
      payload: event.raw,
      createdAt: timestamp,
    });
    return;
  }

  if (event.action === "stop") {
    const body = event.agentActivity
      ? extractPromptBody(event.agentActivity)
      : "";
    store.enqueueInput({
      id: buildInputId(event, "stop"),
      sessionId: event.agentSession.id,
      kind: "stop",
      body,
      payload: event.raw,
      createdAt: timestamp,
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
  if (payload.oauthClientId !== config.linearClientId) {
    throw new WebhookError(401, "OAuth client identity mismatch");
  }
  if (payload.action !== "revoked" && payload.action !== "revoke") return;
  store.revokeInstallation(payload.organizationId, now);
}

async function handlePermissionChange(
  payload: PermissionChangePayload,
  store: GatewayStore,
  config: GatewayConfig,
  now: number,
): Promise<void> {
  if (payload.oauthClientId !== config.linearClientId) {
    throw new WebhookError(401, "OAuth client identity mismatch");
  }
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

  const signature = request.headers.get(LINEAR_SIGNATURE_HEADER);
  if (!signature) {
    return new Response("Missing signature", { status: 400 });
  }
  if (!verifySignature(rawBody, signature, config.linearWebhookSecret)) {
    return new Response("Invalid signature", { status: 401 });
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(rawBody.toString("utf-8"));
  } catch {
    return new Response("Invalid JSON", { status: 400 });
  }
  if (!isRecord(parsed)) {
    return new Response("Payload is not an object", { status: 400 });
  }

  const payloadTimestamp = parsed.webhookTimestamp;
  const timestampHeader = request.headers.get(LINEAR_TIMESTAMP_HEADER);
  const timestamp =
    typeof payloadTimestamp === "number" &&
    Number.isSafeInteger(payloadTimestamp)
      ? payloadTimestamp
      : typeof timestampHeader === "string" && timestampHeader.length > 0
        ? Number(timestampHeader)
        : NaN;
  if (!Number.isSafeInteger(timestamp) || timestamp <= 0) {
    return new Response("Invalid timestamp", { status: 400 });
  }

  const receivedAt = Date.now();
  if (Math.abs(receivedAt - timestamp) > config.webhookReplayWindowMs) {
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

  const hash = payloadHash(rawBody);
  const deliveryId =
    request.headers.get(LINEAR_DELIVERY_HEADER) ??
    buildFallbackDeliveryId(parsed);

  const claim = store.claimDelivery({
    id: deliveryId,
    organizationId,
    payloadHash: hash,
    payload: parsed,
    receivedAt,
  });
  if (claim === "duplicate") {
    return new Response("Duplicate delivery", { status: 200 });
  }
  if (claim === "conflict") {
    return new Response("Delivery id reused with different payload", {
      status: 409,
    });
  }

  try {
    switch (eventType) {
      case "AgentSessionEvent": {
        const event = asAgentSessionEvent(parsed);
        await validateAgentSessionIdentity(event, store, config);
        handleAgentSessionEvent(event, store, timestamp);
        break;
      }
      case "OAuthApp": {
        const payload = asOAuthAppPayload(parsed);
        handleOAuthAppRevoked(payload, store, config, timestamp);
        break;
      }
      case "PermissionChange": {
        const payload = asPermissionChangePayload(parsed);
        await validatePermissionChangeIdentity(payload, store, config);
        await handlePermissionChange(payload, store, config, timestamp);
        break;
      }
    }
    store.markDelivery(deliveryId, "processed");
    return new Response("OK", { status: 200 });
  } catch (error) {
    store.markDelivery(
      deliveryId,
      "failed",
      error instanceof Error ? error.message : "Webhook processing failed",
    );
    const message =
      error instanceof WebhookError
        ? error.message
        : error instanceof Error
          ? error.message
          : "Webhook processing failed";
    const status = error instanceof WebhookError ? error.status : 500;
    return new Response(message, { status });
  }
}
