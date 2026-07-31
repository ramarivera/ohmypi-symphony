import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { Effect, Option, Redacted, Schema } from "effect";
import {
  type DatabaseError,
  type RowDecodeError,
  type TokenCipherError,
  WebhookIdentityError,
  WebhookPayloadError,
} from "../domain/errors.js";
import {
  AppUserId,
  DeliveryId,
  InputId,
  IssueId,
  OrganizationId,
  ProjectId,
  SessionId,
  TeamId,
} from "../domain/ids.js";
import type {
  AgentSessionActivity as AgentSessionActivityType,
  AgentSessionEvent as AgentSessionEventType,
  AgentSessionIssue as AgentSessionIssueType,
} from "../domain/models.js";
import { AgentSessionEvent } from "../domain/models.js";
import { GatewayConfig } from "./config.js";
import {
  isNumber,
  isRecord,
  isString,
  redactStringValues,
} from "./linear-helpers.js";
import {
  DeliveryRepo,
  InstallationRepo,
  RunInputRepo,
  RunRepo,
} from "./store/repositories.js";

const LINEAR_SIGNATURE_HEADER = "linear-signature";
const LINEAR_TIMESTAMP_HEADER = "linear-timestamp";
const LINEAR_DELIVERY_HEADER = "linear-delivery";

type WebhookProcessingError =
  | DatabaseError
  | RowDecodeError
  | TokenCipherError
  | WebhookIdentityError
  | WebhookPayloadError;

const isStringArray = (value: unknown): value is ReadonlyArray<string> =>
  Array.isArray(value) && value.every(isString);

const isBoolean = (value: unknown): value is boolean =>
  typeof value === "boolean";

export const verifySignature = (
  rawBody: Uint8Array,
  signature: string,
  secret: Redacted.Redacted<string>,
): boolean => {
  try {
    const expected = createHmac("sha256", Redacted.value(secret))
      .update(rawBody)
      .digest("hex");
    const normalized = signature.toLowerCase();
    if (expected.length !== normalized.length) return false;
    const a = Buffer.from(expected, "utf-8");
    const b = Buffer.from(normalized, "utf-8");
    if (a.length !== b.length) return false;
    return timingSafeEqual(a, b);
  } catch {
    return false;
  }
};

export const payloadHash = (rawBody: Uint8Array): string =>
  createHash("sha256").update(rawBody).digest("hex");

const buildFallbackDeliveryId = (
  parsed: Record<string, unknown>,
  timestamp: number,
): string => {
  const webhookId = isString(parsed.webhookId) ? parsed.webhookId : "unknown";
  const action = isString(parsed.action) ? parsed.action : "unknown";
  const organizationId = isString(parsed.organizationId)
    ? parsed.organizationId
    : "unknown";
  return `linear:${webhookId}:${timestamp}:${action}:${organizationId}`;
};

const extractPromptBody = (activity: AgentSessionActivityType): string => {
  const content = activity.content;
  if (!isRecord(content)) return "";
  const rawBody = content.body;
  const rawTitle = content.title;
  const body = isString(rawBody) ? rawBody : "";
  const title = isString(rawTitle) ? rawTitle : "";
  if (title && body) return `# ${title}\n\n${body}`;
  if (body) return body;
  if (title) return title;
  return "";
};

const buildCreatedInputBody = (event: AgentSessionEventType): string => {
  const sections: string[] = [];

  const promptContext = event.promptContext.pipe(
    Option.map((s) => s.trim()),
    Option.flatMap((s) => (s.length > 0 ? Option.some(s) : Option.none())),
  );
  const summary = event.agentSession.summary.pipe(
    Option.map((s) => s.trim()),
    Option.flatMap((s) => (s.length > 0 ? Option.some(s) : Option.none())),
  );
  const userPrompt = Option.orElse(promptContext, () =>
    Option.orElse(summary, () => Option.none()),
  ).pipe(Option.getOrElse(() => ""));

  if (userPrompt) {
    sections.push(`User request:\n${userPrompt}`);
  } else if (
    Option.isSome(event.agentSession.issue) ||
    Option.isSome(event.agentSession.comment) ||
    Option.getOrElse(event.previousComments, () => []).length > 0 ||
    Option.getOrElse(event.guidance, () => []).length > 0
  ) {
    sections.push("User request:\nWork on the issue below.");
  }

  const issue = event.agentSession.issue.pipe(Option.getOrElse(() => null));
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

  const comment = event.agentSession.comment.pipe(Option.getOrElse(() => null));
  if (comment) {
    sections.push(`Thread comment:\n${comment.body}`);
  }

  const previousComments = Option.getOrElse(event.previousComments, () => []);
  if (previousComments.length > 0) {
    const list = previousComments
      .map((c, index) => `${index + 1}. ${c.body}`)
      .join("\n");
    sections.push(`Previous comments:\n${list}`);
  }

  const guidance = Option.getOrElse(event.guidance, () => []);
  if (guidance.length > 0) {
    const list = guidance
      .map((g, index) => `${index + 1}. ${g.body}`)
      .join("\n");
    sections.push(`Guidance:\n${list}`);
  }

  return sections.join("\n\n");
};

const buildInputId = (
  event: AgentSessionEventType,
  kind: "created" | "prompted" | "stop",
): string => {
  const activityId = event.agentActivity.pipe(
    Option.map((a) => a.id),
    Option.getOrElse(() => ""),
  );
  if (activityId) return `${event.agentSession.id}:${kind}:${activityId}`;
  return `${event.agentSession.id}:${kind}`;
};

const resolveRawTeamProjectIds = (
  rawIssue: unknown,
): { teamId: string | null; projectId: string | null } => {
  if (!isRecord(rawIssue)) return { teamId: null, projectId: null };
  const teamValue = rawIssue.team;
  const projectValue = rawIssue.project;
  const teamId =
    isRecord(teamValue) && isString(teamValue.id) ? teamValue.id : null;
  const projectId =
    isRecord(projectValue) && isString(projectValue.id)
      ? projectValue.id
      : null;
  return { teamId, projectId };
};

const decodeOptionalBrand = <A>(
  value: string | null,
  schema: Schema.Schema<A, string, never>,
): Effect.Effect<Option.Option<A>, WebhookPayloadError> => {
  if (value === null) return Effect.succeed(Option.none());
  return Schema.decodeUnknown(schema)(value).pipe(
    Effect.map((v) => Option.some(v)),
    Effect.mapError(
      () =>
        new WebhookPayloadError({
          message: `Invalid identifier ${value}`,
          status: 400,
        }),
    ),
  );
};

const resolveTeamAndProject = (
  issue: AgentSessionIssueType | null,
  rawIssue: unknown,
): { teamId: string | null; projectId: string | null } => {
  const fallback = resolveRawTeamProjectIds(rawIssue);
  const teamId = issue?.teamId ?? fallback.teamId ?? null;
  const projectId = issue?.projectId ?? fallback.projectId ?? null;
  return { teamId, projectId };
};

const validateAgentSessionIdentity = (
  event: AgentSessionEventType,
  config: { readonly linearClientId: string },
  installationRepo: InstallationRepo,
): Effect.Effect<
  void,
  WebhookIdentityError | DatabaseError | TokenCipherError | RowDecodeError,
  never
> =>
  Effect.gen(function* () {
    if (event.oauthClientId !== config.linearClientId) {
      return yield* Effect.fail(
        new WebhookIdentityError({
          message: "OAuth client identity mismatch",
        }),
      );
    }
    if (event.organizationId !== event.agentSession.organizationId) {
      return yield* Effect.fail(
        new WebhookIdentityError({ message: "Organization identity mismatch" }),
      );
    }
    if (event.appUserId !== event.agentSession.appUserId) {
      return yield* Effect.fail(
        new WebhookIdentityError({ message: "App user identity mismatch" }),
      );
    }

    const organizationId = yield* Schema.decodeUnknown(OrganizationId)(
      event.organizationId,
    ).pipe(
      Effect.mapError(
        () =>
          new WebhookIdentityError({
            message: "Organization identity is not a valid identifier",
          }),
      ),
    );

    const installation = yield* installationRepo.get(organizationId);
    if (Option.isNone(installation)) {
      return yield* Effect.fail(
        new WebhookIdentityError({
          message: "No installation for organization",
        }),
      );
    }
    if (Option.isSome(installation.value.revokedAt)) {
      return yield* Effect.fail(
        new WebhookIdentityError({ message: "Installation is revoked" }),
      );
    }
    if (installation.value.appUserId !== event.appUserId) {
      return yield* Effect.fail(
        new WebhookIdentityError({
          message: "Installation app user mismatch",
        }),
      );
    }
  });

const handleAgentSessionEvent = (
  event: AgentSessionEventType,
  rawAgentSession: unknown,
  timestamp: number,
  runRepo: RunRepo,
  runInputRepo: RunInputRepo,
): Effect.Effect<
  void,
  WebhookPayloadError | WebhookIdentityError | DatabaseError | RowDecodeError,
  never
> =>
  Effect.gen(function* () {
    const issue = event.agentSession.issue.pipe(Option.getOrElse(() => null));
    const rawIssue =
      isRecord(rawAgentSession) && isRecord(rawAgentSession.issue)
        ? rawAgentSession.issue
        : undefined;
    const { teamId, projectId } = resolveTeamAndProject(issue, rawIssue);

    const sessionId = yield* Schema.decodeUnknown(SessionId)(
      event.agentSession.id,
    ).pipe(
      Effect.mapError(
        () =>
          new WebhookPayloadError({
            message: `Invalid session id ${event.agentSession.id}`,
            status: 400,
          }),
      ),
    );
    const organizationId = yield* Schema.decodeUnknown(OrganizationId)(
      event.agentSession.organizationId,
    ).pipe(
      Effect.mapError(
        () =>
          new WebhookPayloadError({
            message: `Invalid organization id ${event.agentSession.organizationId}`,
            status: 400,
          }),
      ),
    );
    const issueId = yield* decodeOptionalBrand(
      event.agentSession.issueId.pipe(Option.getOrElse(() => null)),
      IssueId,
    );
    const teamIdOption = yield* decodeOptionalBrand(teamId, TeamId);
    const projectIdOption = yield* decodeOptionalBrand(projectId, ProjectId);

    yield* runRepo.create({
      sessionId,
      organizationId,
      issueId,
      teamId: teamIdOption,
      projectId: projectIdOption,
      now: timestamp,
    });

    if (event.action === "created") {
      const body = buildCreatedInputBody(event);
      const id = yield* Schema.decodeUnknown(InputId)(
        buildInputId(event, "created"),
      ).pipe(
        Effect.mapError(
          () =>
            new WebhookPayloadError({
              message: `Invalid input id ${buildInputId(event, "created")}`,
              status: 400,
            }),
        ),
      );
      yield* runInputRepo.enqueue({
        id,
        sessionId,
        kind: "created",
        body,
        payload: event,
        createdAt: timestamp,
      });
      return;
    }

    if (event.action === "prompted") {
      const activity = event.agentActivity.pipe(Option.getOrElse(() => null));
      if (activity === null) {
        return yield* Effect.fail(
          new WebhookPayloadError({
            message: "AgentSessionEvent prompted missing agentActivity",
            status: 400,
          }),
        );
      }
      if (activity.agentSessionId !== event.agentSession.id) {
        return yield* Effect.fail(
          new WebhookIdentityError({
            message: "AgentSessionEvent prompted activity session mismatch",
          }),
        );
      }
      const body = extractPromptBody(activity);
      const kind: "prompted" | "stop" =
        Option.getOrElse(activity.signal, () => "") === "stop"
          ? "stop"
          : "prompted";
      const id = yield* Schema.decodeUnknown(InputId)(
        buildInputId(event, kind),
      ).pipe(
        Effect.mapError(
          () =>
            new WebhookPayloadError({
              message: `Invalid input id ${buildInputId(event, kind)}`,
              status: 400,
            }),
        ),
      );
      yield* runInputRepo.enqueue({
        id,
        sessionId,
        kind,
        body,
        payload: event,
        createdAt: timestamp,
      });
      return;
    }

    if (event.action === "stop") {
      const body = event.agentActivity.pipe(
        Option.map(extractPromptBody),
        Option.getOrElse(() => ""),
      );
      const id = yield* Schema.decodeUnknown(InputId)(
        buildInputId(event, "stop"),
      ).pipe(
        Effect.mapError(
          () =>
            new WebhookPayloadError({
              message: `Invalid input id ${buildInputId(event, "stop")}`,
              status: 400,
            }),
        ),
      );
      yield* runInputRepo.enqueue({
        id,
        sessionId,
        kind: "stop",
        body,
        payload: event,
        createdAt: timestamp,
      });
      return;
    }
  });

const validateOAuthAppPayload = (
  parsed: Record<string, unknown>,
  config: { readonly linearClientId: string },
): Effect.Effect<
  { action: string; organizationId: OrganizationId },
  WebhookPayloadError | WebhookIdentityError
> =>
  Effect.gen(function* () {
    const action = parsed.action;
    const oauthClientId = parsed.oauthClientId;
    const organizationId = parsed.organizationId;
    if (
      !isString(action) ||
      !isString(oauthClientId) ||
      !isString(organizationId)
    ) {
      return yield* Effect.fail(
        new WebhookPayloadError({
          message: "OAuthApp payload missing required fields",
          status: 400,
        }),
      );
    }
    if (oauthClientId !== config.linearClientId) {
      return yield* Effect.fail(
        new WebhookIdentityError({
          message: "OAuth client identity mismatch",
        }),
      );
    }
    const decoded = yield* Schema.decodeUnknown(OrganizationId)(
      organizationId,
    ).pipe(
      Effect.mapError(
        () =>
          new WebhookIdentityError({
            message: "Organization identity is not a valid identifier",
          }),
      ),
    );
    return { action, organizationId: decoded };
  });

const handleOAuthAppRevoked = (
  action: string,
  organizationId: OrganizationId,
  installationRepo: InstallationRepo,
  timestamp: number,
): Effect.Effect<void, DatabaseError, never> =>
  Effect.gen(function* () {
    if (action !== "revoked" && action !== "revoke") return;
    yield* installationRepo.revoke(organizationId, timestamp);
  });

const validatePermissionChangePayload = (
  parsed: Record<string, unknown>,
  config: { readonly linearClientId: string },
): Effect.Effect<
  {
    action: string;
    organizationId: OrganizationId;
    appUserId: AppUserId;
    addedTeamIds: ReadonlyArray<TeamId>;
    removedTeamIds: ReadonlyArray<TeamId>;
    canAccessAllPublicTeams: boolean;
  },
  WebhookPayloadError | WebhookIdentityError
> =>
  Effect.gen(function* () {
    const action = parsed.action;
    const oauthClientId = parsed.oauthClientId;
    const organizationId = parsed.organizationId;
    const appUserId = parsed.appUserId;
    const added = parsed.addedTeamIds;
    const removed = parsed.removedTeamIds;
    const canAccess = parsed.canAccessAllPublicTeams;

    if (
      !isString(action) ||
      !isString(oauthClientId) ||
      !isString(organizationId) ||
      !isString(appUserId) ||
      !isStringArray(added) ||
      !isStringArray(removed) ||
      !isBoolean(canAccess)
    ) {
      return yield* Effect.fail(
        new WebhookPayloadError({
          message: "PermissionChange missing or invalid fields",
          status: 400,
        }),
      );
    }

    if (oauthClientId !== config.linearClientId) {
      return yield* Effect.fail(
        new WebhookIdentityError({
          message: "OAuth client identity mismatch",
        }),
      );
    }

    const decodedOrganizationId = yield* Schema.decodeUnknown(OrganizationId)(
      organizationId,
    ).pipe(
      Effect.mapError(
        () =>
          new WebhookIdentityError({
            message: "Organization identity is not a valid identifier",
          }),
      ),
    );

    const decodedAppUserId = yield* Schema.decodeUnknown(AppUserId)(
      appUserId,
    ).pipe(
      Effect.mapError(
        () =>
          new WebhookPayloadError({
            message: `Invalid app user id ${appUserId}`,
            status: 400,
          }),
      ),
    );

    const addedTeamIds = yield* Effect.forEach(added, (teamId) =>
      Schema.decodeUnknown(TeamId)(teamId).pipe(
        Effect.mapError(
          () =>
            new WebhookPayloadError({
              message: `Invalid team id ${teamId}`,
              status: 400,
            }),
        ),
      ),
    );

    const removedTeamIds = yield* Effect.forEach(removed, (teamId) =>
      Schema.decodeUnknown(TeamId)(teamId).pipe(
        Effect.mapError(
          () =>
            new WebhookPayloadError({
              message: `Invalid team id ${teamId}`,
              status: 400,
            }),
        ),
      ),
    );

    return {
      action,
      organizationId: decodedOrganizationId,
      appUserId: decodedAppUserId,
      addedTeamIds,
      removedTeamIds,
      canAccessAllPublicTeams: canAccess,
    };
  });

const handlePermissionChange = (
  action: string,
  organizationId: OrganizationId,
  appUserId: AppUserId,
  addedTeamIds: ReadonlyArray<TeamId>,
  removedTeamIds: ReadonlyArray<TeamId>,
  canAccessAllPublicTeams: boolean,
  installationRepo: InstallationRepo,
  timestamp: number,
): Effect.Effect<void, DatabaseError | RowDecodeError, never> =>
  Effect.gen(function* () {
    if (action !== "teamAccessChanged") return;
    yield* installationRepo.applyPermissionChange(
      organizationId,
      appUserId,
      addedTeamIds,
      removedTeamIds,
      canAccessAllPublicTeams,
      timestamp,
    );
  });

const decodeDeliveryId = (
  raw: string,
): Effect.Effect<DeliveryId, WebhookPayloadError> =>
  Schema.decodeUnknown(DeliveryId)(raw).pipe(
    Effect.mapError(
      () =>
        new WebhookPayloadError({
          message: "Invalid delivery id",
          status: 400,
        }),
    ),
  );

const markDelivery = (
  deliveryRepo: DeliveryRepo,
  deliveryId: DeliveryId | undefined,
  status: "processed" | "failed",
  error?: string,
): Effect.Effect<void, never, never> =>
  deliveryId === undefined
    ? Effect.void
    : deliveryRepo.mark(deliveryId, status, error).pipe(Effect.ignore);

export class WebhookPipeline extends Effect.Service<WebhookPipeline>()(
  "WebhookPipeline",
  {
    accessors: true,
    dependencies: [
      GatewayConfig.Default,
      InstallationRepo.Default,
      RunRepo.Default,
      RunInputRepo.Default,
      DeliveryRepo.Default,
    ],
    effect: Effect.gen(function* () {
      const config = yield* GatewayConfig;
      const installationRepo = yield* InstallationRepo;
      const runRepo = yield* RunRepo;
      const runInputRepo = yield* RunInputRepo;
      const deliveryRepo = yield* DeliveryRepo;

      const handle = Effect.fn("WebhookPipeline.handle")(function* (
        request: Request,
      ) {
        let deliveryId: DeliveryId | undefined;

        const process: Effect.Effect<Response, WebhookProcessingError, never> =
          Effect.gen(function* () {
            const url = new URL(request.url);

            yield* Effect.annotateCurrentSpan({
              "webhook.path": url.pathname,
              "webhook.method": request.method,
            });

            if (request.method !== "POST") {
              yield* Effect.logWarning("webhook.rejected", {
                reason: "method not allowed",
                method: request.method,
                path: url.pathname,
                status: 405,
              });
              return new Response("Method not allowed", { status: 405 });
            }

            const rawBody = yield* Effect.tryPromise({
              try: () => request.arrayBuffer(),
              catch: (error) =>
                new WebhookPayloadError({
                  message: "Unable to read body",
                  status: 400,
                  cause: String(error),
                }),
            }).pipe(Effect.map((buffer) => new Uint8Array(buffer)));

            const signature = request.headers.get(LINEAR_SIGNATURE_HEADER);
            if (signature === null) {
              yield* Effect.logWarning("webhook.rejected", {
                reason: "missing signature",
                path: url.pathname,
                status: 400,
              });
              return new Response("Missing signature", { status: 400 });
            }
            if (
              !verifySignature(rawBody, signature, config.linearWebhookSecret)
            ) {
              yield* Effect.logWarning("webhook.rejected", {
                reason: "invalid signature",
                path: url.pathname,
                status: 401,
              });
              return new Response("Invalid signature", { status: 401 });
            }

            const parsed = yield* Effect.try({
              try: () => {
                const value = JSON.parse(new TextDecoder().decode(rawBody));
                return isRecord(value) ? value : null;
              },
              catch: () =>
                new WebhookPayloadError({
                  message: "Invalid JSON",
                  status: 400,
                }),
            });
            if (parsed === null) {
              yield* Effect.logWarning("webhook.rejected", {
                reason: "payload is not an object",
                path: url.pathname,
                status: 400,
              });
              return new Response("Payload is not an object", { status: 400 });
            }

            const payloadTimestamp = parsed.webhookTimestamp;
            const timestampHeader = request.headers.get(
              LINEAR_TIMESTAMP_HEADER,
            );
            const timestamp = isNumber(payloadTimestamp)
              ? payloadTimestamp
              : timestampHeader !== null && timestampHeader.length > 0
                ? Number(timestampHeader)
                : NaN;
            if (!Number.isSafeInteger(timestamp) || timestamp <= 0) {
              yield* Effect.logWarning("webhook.rejected", {
                reason: "invalid timestamp",
                path: url.pathname,
                status: 400,
              });
              return new Response("Invalid timestamp", { status: 400 });
            }

            const receivedAt = yield* Effect.clockWith(
              (clock) => clock.currentTimeMillis,
            );
            if (
              Math.abs(receivedAt - timestamp) > config.webhookReplayWindowMs
            ) {
              yield* Effect.logWarning("webhook.rejected", {
                reason: "timestamp outside replay window",
                path: url.pathname,
                status: 401,
                timestamp,
                receivedAt,
              });
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
              yield* Effect.logWarning("webhook.rejected", {
                reason: "missing organizationId, webhookId, or type",
                path: url.pathname,
                status: 400,
              });
              return new Response(
                "Payload missing organizationId, webhookId, or type",
                { status: 400 },
              );
            }

            const hash = payloadHash(rawBody);
            const rawDeliveryId =
              request.headers.get(LINEAR_DELIVERY_HEADER) ??
              buildFallbackDeliveryId(parsed, timestamp);
            deliveryId = yield* decodeDeliveryId(rawDeliveryId);

            // Normalize webhookTimestamp on the decoded payload so the schema
            // sees a number even when the header was the source.
            parsed.webhookTimestamp = timestamp;

            yield* Effect.logInfo("webhook.received", {
              deliveryId,
              organizationId,
              webhookId,
              eventType,
              path: url.pathname,
            });
            yield* Effect.logDebug("webhook.payload", {
              deliveryId,
              organizationId,
              payload: redactStringValues(parsed),
            });

            const claim = yield* deliveryRepo.claim({
              id: deliveryId,
              organizationId,
              payloadHash: hash,
              payload: parsed,
              receivedAt,
            });
            if (claim === "duplicate") {
              yield* Effect.logInfo("webhook.deduplicated", {
                deliveryId,
                organizationId,
                eventType,
              });
              return new Response("Duplicate delivery", { status: 200 });
            }
            if (claim === "conflict") {
              yield* Effect.logWarning("webhook.rejected", {
                reason: "delivery id reused with different payload",
                deliveryId,
                organizationId,
                eventType,
                status: 409,
              });
              return new Response("Delivery id reused with different payload", {
                status: 409,
              });
            }

            yield* Effect.logInfo("webhook.verified", {
              deliveryId,
              organizationId,
              eventType,
            });

            switch (eventType) {
              case "AgentSessionEvent": {
                const rawAgentSession = isRecord(parsed.agentSession)
                  ? parsed.agentSession
                  : undefined;
                const event = yield* Schema.decodeUnknown(AgentSessionEvent)(
                  parsed,
                ).pipe(
                  Effect.mapError(
                    (error) =>
                      new WebhookPayloadError({
                        message: `AgentSessionEvent payload is invalid: ${error}`,
                        status: 400,
                        cause: String(error),
                      }),
                  ),
                );
                yield* validateAgentSessionIdentity(
                  event,
                  config,
                  installationRepo,
                );
                yield* handleAgentSessionEvent(
                  event,
                  rawAgentSession,
                  timestamp,
                  runRepo,
                  runInputRepo,
                );
                break;
              }
              case "OAuthApp": {
                const { action, organizationId: orgId } =
                  yield* validateOAuthAppPayload(parsed, config);
                yield* handleOAuthAppRevoked(
                  action,
                  orgId,
                  installationRepo,
                  timestamp,
                );
                break;
              }
              case "PermissionChange": {
                const {
                  action,
                  organizationId: orgId,
                  appUserId,
                  addedTeamIds,
                  removedTeamIds,
                  canAccessAllPublicTeams,
                } = yield* validatePermissionChangePayload(parsed, config);

                const installation = yield* installationRepo.get(orgId);
                if (Option.isNone(installation)) {
                  return yield* Effect.fail(
                    new WebhookIdentityError({
                      message: "No installation for organization",
                    }),
                  );
                }
                if (Option.isSome(installation.value.revokedAt)) {
                  return yield* Effect.fail(
                    new WebhookIdentityError({
                      message: "Installation is revoked",
                    }),
                  );
                }
                if (installation.value.appUserId !== appUserId) {
                  return yield* Effect.fail(
                    new WebhookIdentityError({
                      message: "App user mismatch",
                    }),
                  );
                }

                yield* handlePermissionChange(
                  action,
                  orgId,
                  appUserId,
                  addedTeamIds,
                  removedTeamIds,
                  canAccessAllPublicTeams,
                  installationRepo,
                  timestamp,
                );
                break;
              }
              default:
                break;
            }

            yield* deliveryRepo.mark(deliveryId, "processed");
            yield* Effect.logInfo("webhook.processed", {
              deliveryId,
              organizationId,
              eventType,
            });
            return new Response("OK", { status: 200 });
          });

        return yield* process.pipe(
          Effect.catchTags({
            "@Gateway/WebhookPayloadError": (error) =>
              Effect.gen(function* () {
                yield* markDelivery(
                  deliveryRepo,
                  deliveryId,
                  "failed",
                  error.message,
                );
                return new Response(error.message, {
                  status: error.status,
                });
              }),
            "@Gateway/WebhookIdentityError": (error) =>
              Effect.gen(function* () {
                yield* markDelivery(
                  deliveryRepo,
                  deliveryId,
                  "failed",
                  error.message,
                );
                return new Response(error.message, { status: 401 });
              }),
            "@Gateway/DatabaseError": (error) =>
              Effect.gen(function* () {
                yield* markDelivery(
                  deliveryRepo,
                  deliveryId,
                  "failed",
                  error.message,
                );
                return new Response("Webhook processing failed", {
                  status: 500,
                });
              }),
            "@Gateway/RowDecodeError": (error) =>
              Effect.gen(function* () {
                yield* markDelivery(
                  deliveryRepo,
                  deliveryId,
                  "failed",
                  error.message,
                );
                return new Response("Webhook processing failed", {
                  status: 500,
                });
              }),
            "@Gateway/TokenCipherError": (error) =>
              Effect.gen(function* () {
                yield* markDelivery(
                  deliveryRepo,
                  deliveryId,
                  "failed",
                  error.message,
                );
                return new Response("Webhook processing failed", {
                  status: 500,
                });
              }),
          }),
        );
      });

      return { handle };
    }),
  },
) {}
