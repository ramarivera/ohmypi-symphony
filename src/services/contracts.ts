import { createHash, randomUUID } from "node:crypto";
import type { ActivityId, SessionId, SourceKey } from "../domain/ids.js";
import { AgentActivitySignal, LinearClient } from "@linear/sdk";
import { Cause, Clock, Deferred, Effect, Option, Redacted, Ref } from "effect";
import {
  DatabaseError,
  InstallationRevokedError,
  LinearApiError,
  LinearRateLimitError,
  RowDecodeError,
  RunLeaseError,
  TokenRefreshError,
  WorkspaceError,
} from "../domain/errors.js";
import type { ActivityType, AgentRun, Installation, ProjectionJob, RepositoryRecord, RunEvent, RunInput } from "../domain/models.js";
import { redact } from "../admin-ui/run-detail.js";
import {
  isActivitySignal,
  isActivityType,
  isNumber,
  isRecord,
  isString,
  redactStringValues,
} from "./linear-helpers.js";
import { GatewayConfig } from "./config.js";
import { makeWorkspace, type RepositoryResolution } from "./workspace.js";
import { InstallationRepo, ProjectionRepo, RunRepo, WorkspaceRepo } from "./store/repositories.js";

export {
  AdminSessionRepo,
  DeliveryRepo,
  InstallationRepo,
  ProjectionRepo,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
  WorkspaceRepo,
} from "./store/repositories.js";
export class LinearGateway extends Effect.Service<LinearGateway>()("LinearGateway", {
  accessors: true,
  dependencies: [InstallationRepo.Default, RunRepo.Default, GatewayConfig.Default],
  effect: Effect.gen(function* () {
    const installationRepo = yield* InstallationRepo;
    const runRepo = yield* RunRepo;
    const config = yield* GatewayConfig;
    const refreshing = yield* Ref.make(new Map<string, Deferred<Installation, TokenRefreshError>>());
    const queues = yield* Ref.make(new Map<string, Deferred<void, never>>());

    const text = (value: unknown): string | undefined => (isString(value) ? value : undefined);

    const parseTokenResponse = (value: unknown): TokenResponse => {
      if (!isRecord(value)) throw new Error("Linear token response is not an object");
      const accessToken = requireString(value, "access_token");
      const tokenType = requireString(value, "token_type");
      const expiresIn = parseExpiresIn(value.expires_in);
      const refreshToken = requireString(value, "refresh_token");
      const scopes = parseScopes(value.scope);
      if (tokenType.toLowerCase() !== "bearer") throw new Error(`Unexpected token type ${tokenType}`);
      return { accessToken, tokenType, expiresIn, refreshToken, scopes };
    };
    const requireString = (value: Record<string, unknown>, key: string): string => {
      const field = value[key];
      if (!isString(field)) throw new Error(`Linear token response missing or invalid ${key}`);
      return field;
    };
    const parseScopes = (raw: unknown): readonly string[] => {
      if (isString(raw)) return raw.split(/[,\s]+/).filter(Boolean);
      if (Array.isArray(raw)) {
        return raw.map((item, index) => {
          if (!isString(item)) throw new Error(`Linear token response scope[${index}] is not a string`);
          return item;
        });
      }
      throw new Error("Linear token response scope is not a string or array");
    };
    const parseExpiresIn = (raw: unknown): number => {
      if (isNumber(raw) && raw > 0) return raw;
      if (isString(raw)) {
        const parsed = Number.parseInt(raw, 10);
        if (Number.isSafeInteger(parsed) && parsed > 0) return parsed;
      }
      throw new Error("Linear token response missing or invalid expires_in");
    };

    const rateLimitDelay = (error: unknown): number | null => {
      if (!isRecord(error)) return null;
      const type = error.type;
      const status = error.status ?? error.statusCode;
      if (type !== "Ratelimited" && status !== 429) return null;
      const retryAfter = error.retryAfter;
      return isNumber(retryAfter)
        ? Math.min(60_000, Math.max(0, retryAfter * 1_000))
        : 1_000;
    };
    const mapLinearError = (operation: string, error: unknown): LinearApiError | LinearRateLimitError => {
      const delay = rateLimitDelay(error);
      if (delay !== null) {
        return new LinearRateLimitError({ message: "Linear rate limit exceeded", retryAfterMs: delay });
      }
      const status = isRecord(error) && isNumber(error.status) ? error.status : undefined;
      const message = error instanceof Error ? error.message : String(error);
      return new LinearApiError({ operation, message, status });
    };
    interface DecodedActivityContent {
      type: ActivityType;
      body?: string;
      action?: string;
      parameter?: string;
      result?: string;
    }

    const decodeActivityContent = (value: Record<string, unknown>): DecodedActivityContent => {
      const type = value.type;
      if (!isActivityType(type)) throw new Error(`Invalid activity type ${type}`);
      const content: DecodedActivityContent = { type };
      const body = text(value.body);
      const action = text(value.action);
      const parameter = text(value.parameter);
      const result = text(value.result);
      if (body !== undefined) content.body = body;
      if (action !== undefined) content.action = action;
      if (parameter !== undefined) content.parameter = parameter;
      if (result !== undefined) content.result = result;
      return content;
    };
    const mapActivityContent = (content: DecodedActivityContent): Record<string, unknown> => {
      switch (content.type) {
        case "thought":
          return { type: "thought", body: content.body ?? "" };
        case "action": {
          const action = content.action ?? "";
          const parameter = content.parameter ?? "";
          if (!action) throw new Error("Linear action activity requires an action");
          if (!parameter) throw new Error("Linear action activity requires a parameter");
          const payload: Record<string, unknown> = { type: "action", action, parameter };
          if (content.result !== undefined) payload.result = content.result;
          return payload;
        }
        case "elicitation":
          return { type: "elicitation", body: content.body ?? "" };
        case "response":
          return { type: "response", body: content.body ?? "" };
        case "error":
          return { type: "error", body: content.body ?? "" };
      }
    };
    const mapActivitySignal = (signal: "auth" | "continue" | "select" | "stop"): AgentActivitySignal => {
      switch (signal) {
        case "auth":
          return AgentActivitySignal.Auth;
        case "continue":
          return AgentActivitySignal.Continue;
        case "select":
          return AgentActivitySignal.Select;
        case "stop":
          return AgentActivitySignal.Stop;
      }
    };

    const exchangeToken = (
      input: { readonly grantType: string; readonly code?: string; readonly refreshToken?: string; readonly redirectUri?: string },
      organizationId: string,
    ) =>
      Effect.tryPromise({
        try: async () => {
          const body = new URLSearchParams();
          body.set("client_id", config.linearClientId);
          body.set("client_secret", Redacted.value(config.linearClientSecret));
          body.set("grant_type", input.grantType);
          if (input.code) body.set("code", input.code);
          if (input.refreshToken) body.set("refresh_token", input.refreshToken);
          if (input.redirectUri) body.set("redirect_uri", input.redirectUri);
          const response = await fetch(LINEAR_TOKEN_URL, {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: body.toString(),
          });
          if (!response.ok) {
            throw new Error(`Linear token exchange failed (${response.status}): ${await response.text()}`);
          }
          const raw: unknown = await response.json();
          return parseTokenResponse(raw);
        },
        catch: (error) =>
          new TokenRefreshError({
            organizationId,
            message: error instanceof Error ? error.message : String(error),
          }),
      });

    const performRefresh = Effect.fn("LinearGateway.performRefresh")(
      function* (record: Installation, now: number): Effect.fn.Return<Installation, TokenRefreshError> {
        yield* Effect.logInfo("linear.token.refresh", { organizationId: record.organizationId });
        const response = yield* exchangeToken({ grantType: "refresh_token", refreshToken: record.refreshToken }, record.organizationId);
        const updated: Installation = {
          ...record,
          accessToken: response.accessToken,
          refreshToken: response.refreshToken,
          expiresAt: now + response.expiresIn * 1000,
        };
        yield* installationRepo.put(updated);
        yield* Effect.logInfo("linear.token.refreshed", { organizationId: record.organizationId, expiresAt: updated.expiresAt });
        return updated;
      },
    );

    const refreshTokens = Effect.fn("LinearGateway.refreshTokens")(
      function* (record: Installation, now: number): Effect.fn.Return<Installation, TokenRefreshError> {
        const myDeferred = yield* Deferred.make<Installation, TokenRefreshError>();
        const claim = yield* Ref.modify(refreshing, (m) => {
          const existing = m.get(record.organizationId);
          if (existing) return [Option.some(existing), m] as const;
          const n = new Map(m);
          n.set(record.organizationId, myDeferred);
          return [Option.some(myDeferred), n] as const;
        });
        if (Option.isSome(claim) && claim.value !== myDeferred) {
          return yield* Deferred.await(claim.value);
        }
        const refreshed = yield* performRefresh(record, now).pipe(
          Effect.tap((value) => Deferred.succeed(myDeferred, value)),
          Effect.tapError((error) => Deferred.fail(myDeferred, error)),
          Effect.ensuring(
            Ref.update(refreshing, (m) => {
              const n = new Map(m);
              n.delete(record.organizationId);
              return n;
            }),
          ),
        );
        return refreshed;
      },
    );

    const ensureToken = Effect.fn("LinearGateway.ensureToken")(
      function* (organizationId: string): Effect.fn.Return<Installation, TokenRefreshError | InstallationRevokedError> {
        const now = yield* Clock.currentTimeMillis;
        const option = yield* installationRepo.get(organizationId);
        return yield* Option.match(option, {
          onNone: () =>
            Effect.fail(new TokenRefreshError({ organizationId, message: `No Linear installation for ${organizationId}` })),
          onSome: (record) =>
            Option.match(record.revokedAt, {
              onNone: () =>
                record.expiresAt - TOKEN_REFRESH_BUFFER_MS > now
                  ? Effect.succeed(record)
                  : refreshTokens(record, now),
              onSome: () =>
                Effect.fail(new InstallationRevokedError({ organizationId, message: `Linear installation for ${organizationId} is revoked` })),
            }),
        });
      },
    );

    const clientFor = (organizationId: string) =>
      Effect.gen(function* () {
        const installation = yield* ensureToken(organizationId);
        return new LinearClient({ accessToken: installation.accessToken });
      });

    const withRateLimitRetry = <A, E>(effect: Effect.Effect<A, E | LinearRateLimitError>): Effect.Effect<A, E | LinearRateLimitError | LinearApiError> =>
      effect.pipe(
        Effect.catchTag("LinearRateLimitError", (error) =>
          Effect.gen(function* () {
            yield* Effect.logDebug("linear.api.rateLimited", { retryAfterMs: error.retryAfterMs });
            yield* Effect.sleep(error.retryAfterMs ?? 1_000);
            return yield* effect;
          }),
        ),
      );

    const withOrgQueue = <A, E>(organizationId: string, effect: Effect.Effect<A, E>): Effect.Effect<A, E> =>
      Effect.gen(function* () {
        const myDone = yield* Deferred.make<void, never>();
        const previous = yield* Ref.modify(queues, (m) => {
          const prev = m.get(organizationId);
          const n = new Map(m);
          n.set(organizationId, myDone);
          return [prev, n] as const;
        });
        if (previous !== undefined) {
          yield* Deferred.await(previous);
        }
        const result = yield* effect;
        yield* Deferred.succeed(myDone, undefined);
        return result;
      }).pipe(
        Effect.ensuring(
          Effect.gen(function* () {
            yield* Ref.update(queues, (m) => {
              const n = new Map(m);
              n.delete(organizationId);
              return n;
            });
            yield* Deferred.succeed(myDone, undefined);
          }),
        ),
      );

    const createActivity = Effect.fn("LinearGateway.createActivity")(
      function* (input: { readonly sessionId: string; readonly content: unknown }): Effect.fn.Return<
        string,
        LinearApiError | LinearRateLimitError | TokenRefreshError | InstallationRevokedError
      > {
        yield* Effect.annotateCurrentSpan({ "linear.sessionId": input.sessionId });
        if (!isRecord(input.content)) {
          yield* Effect.logWarning("linear.createActivity.invalid", { sessionId: input.sessionId, reason: "content_not_record" });
          return yield* Effect.fail(new LinearApiError({ operation: "createActivity", message: "Activity content is not a record" }));
        }
        const runOption = yield* runRepo.get(input.sessionId);
        const run = yield* Option.match(runOption, {
          onNone: () => Effect.fail(new LinearApiError({ operation: "createActivity", message: `Unknown run ${input.sessionId}` })),
          onSome: Effect.succeed,
        });
        yield* Effect.annotateCurrentSpan({ "linear.organizationId": run.organizationId });

        const redactedRequest = redactStringValues(input.content) as Record<string, unknown>;
        const contentRecord = yield* Effect.try({
          try: () => mapActivityContent(decodeActivityContent(redactedRequest)),
          catch: (error) =>
            new LinearApiError({
              operation: "createActivity",
              message: error instanceof Error ? error.message : String(error),
            }),
        });

        const activityInput: Parameters<LinearClient["createAgentActivity"]>[0] = {
          agentSessionId: input.sessionId,
          content: contentRecord,
        };
        if (redactedRequest.ephemeral === true) activityInput.ephemeral = true;
        const signal = isActivitySignal(redactedRequest.signal) ? redactedRequest.signal : undefined;
        if (signal !== undefined) activityInput.signal = mapActivitySignal(signal);
        if (isRecord(redactedRequest.signalMetadata)) {
          activityInput.signalMetadata = redactStringValues(redactedRequest.signalMetadata) as Record<string, unknown>;
        }

        const id = yield* withOrgQueue(
          run.organizationId,
          Effect.gen(function* () {
            const client = yield* clientFor(run.organizationId);
            return yield* withRateLimitRetry(
              Effect.tryPromise({
                try: async () => {
                  const payload = await client.createAgentActivity(activityInput);
                  if (!payload.success) throw new Error("Linear failed to create agent activity");
                  const id = payload.agentActivityId ?? (payload.agentActivity ? (await payload.agentActivity).id : undefined);
                  if (!id) throw new Error("Linear did not return an agent activity id");
                  return id;
                },
                catch: (error) => mapLinearError("createActivity", error),
              }),
            );
          }),
        );

        yield* Effect.logInfo("linear.createActivity", { sessionId: input.sessionId, activityId: id });
        return id;
      },
    );

    const updateSession = Effect.fn("LinearGateway.updateSession")(
      function* (input: {
        readonly sessionId: string;
        readonly plan?: ReadonlyArray<{ readonly content: string; readonly status: string }>;
        readonly externalUrls?: ReadonlyArray<{ readonly label: string; readonly url: string }>;
      }): Effect.fn.Return<void, LinearApiError | LinearRateLimitError | TokenRefreshError | InstallationRevokedError> {
        yield* Effect.annotateCurrentSpan({ "linear.sessionId": input.sessionId });
        const runOption = yield* runRepo.get(input.sessionId);
        const run = yield* Option.match(runOption, {
          onNone: () => Effect.fail(new LinearApiError({ operation: "updateSession", message: `Unknown run ${input.sessionId}` })),
          onSome: Effect.succeed,
        });
        yield* Effect.annotateCurrentSpan({ "linear.organizationId": run.organizationId });

        const updateInput: Parameters<LinearClient["updateAgentSession"]>[1] = {};
        if (input.plan !== undefined) {
          updateInput.plan = input.plan.map((item) => ({ content: redact(item.content), status: item.status }));
        }
        if (input.externalUrls !== undefined) {
          updateInput.externalUrls = input.externalUrls.map((item) => ({
            label: redact(item.label),
            url: redact(item.url),
          }));
        }

        yield* withOrgQueue(
          run.organizationId,
          Effect.gen(function* () {
            const client = yield* clientFor(run.organizationId);
            return yield* withRateLimitRetry(
              Effect.tryPromise({
                try: async () => {
                  const payload = await client.updateAgentSession(input.sessionId, updateInput);
                  if (!payload.success) throw new Error("Linear failed to update agent session");
                },
                catch: (error) => mapLinearError("updateSession", error),
              }),
            );
          }),
        );

        yield* Effect.logInfo("linear.updateSession", { sessionId: input.sessionId });
      },
    );

    const refreshInstallation = Effect.fn("LinearGateway.refreshInstallation")(
      function* (organizationId: string): Effect.fn.Return<string, TokenRefreshError | InstallationRevokedError> {
        yield* Effect.annotateCurrentSpan({ "linear.organizationId": organizationId });
        const installation = yield* ensureToken(organizationId);
        yield* Effect.logInfo("linear.refreshInstallation", { organizationId });
        return installation.accessToken;
      },
    );

    return { createActivity, updateSession, refreshInstallation };
  }),
}) {}
type TokenResponse = {
  readonly accessToken: string;
  readonly tokenType: string;
  readonly expiresIn: number;
  readonly refreshToken: string;
  readonly scopes: readonly string[];
};
const LINEAR_TOKEN_URL = "https://api.linear.app/oauth/token";
const TOKEN_REFRESH_BUFFER_MS = 5 * 60 * 1000;

export class WebhookPipeline extends Effect.Service<WebhookPipeline>()("WebhookPipeline", {
  accessors: true,
  effect: Effect.gen(function* () {
    const handle = Effect.fn("WebhookPipeline.handle")(function* (request: Request): Effect.fn.Return<Response, never> { return yield* Effect.dieMessage("unimplemented: WebhookPipeline.handle"); });
    return { handle };
  }),
}) {}
export const projectionBackoff = (
  attempt: number,
  baseMs = 1_000,
  maxMs = 5 * 60_000,
  maxExponent = 8,
): number => Math.min(maxMs, baseMs * 2 ** Math.min(attempt - 1, maxExponent));

export const rpcEventActivityType = (
  event: { readonly type: string },
): ActivityType | "none" => {
  switch (event.type) {
    case "agent_start":
    case "turn_start":
      return "thought";
    case "tool_execution_start":
    case "tool_execution_end":
      return "action";
    case "agent_end":
      return "response";
    case "error":
      return "error";
    default:
      return "none";
  }
};

export class ActivityProjector extends Effect.Service<ActivityProjector>()("ActivityProjector", {
  accessors: true,
  effect: Effect.gen(function* () {
    const projectionRepo = yield* ProjectionRepo;
    const runRepo = yield* RunRepo;
    const linear = yield* LinearGateway;
    const owner = `projector:${randomUUID()}`;
    const assistantDraft = yield* Ref.make(new Map<string, string>());

    const PROJECTION_LEASE_MS = 30_000;
    const MAX_PROJECTION_BACKOFF_MS = 5 * 60_000;

    const text = (value: unknown): string | undefined =>
      isString(value) && value.length > 0 ? value : undefined;

    const boundedText = (value: string, maxLength = 8_000): string =>
      value.length <= maxLength ? value : `${value.slice(0, maxLength)}\n[truncated]`;

    const sha256 = (value: string): Effect.Effect<string, never, never> =>
      Effect.sync(() => createHash("sha256").update(value).digest("hex"));

    const stringify = (value: unknown): Effect.Effect<string, never, never> =>
      Effect.sync(() => JSON.stringify(value));

    const assistantText = (event: Record<string, unknown>): string | undefined => {
      if (!isRecord(event.message)) return undefined;
      const message = event.message;
      if (text(message.role) !== "assistant") return undefined;
      const content = message.content;
      if (isString(content)) return content;
      if (!Array.isArray(content)) return undefined;
      const parts: string[] = [];
      for (const item of content) {
        if (isRecord(item) && item.type === "text" && isString(item.text)) {
          parts.push(item.text);
        }
      }
      return parts.length > 0 ? parts.join("\n") : undefined;
    };

    interface LinearActivityContent {
      readonly type: ActivityType;
      readonly body?: string;
      readonly action?: string;
      readonly parameter?: string;
      readonly result?: string;
    }

    const decodeActivityRequest = (
      job: ProjectionJob,
    ): { readonly sessionId: string; readonly content: Record<string, unknown> } => {
      if (!isRecord(job.payload)) {
        throw new Error(`Projection ${job.sourceKey} payload is not an object`);
      }
      const request = job.payload.request;
      if (!isRecord(request) || !isRecord(request.content)) {
        throw new Error(`Projection ${job.sourceKey} activity request is invalid`);
      }
      const type = request.content.type;
      if (!isActivityType(type)) {
        throw new Error(`Projection ${job.sourceKey} activity type is invalid`);
      }
      const content: Record<string, unknown> = { type };
      const body = text(request.content.body);
      const action = text(request.content.action);
      const parameter = text(request.content.parameter);
      const result = text(request.content.result);
      if (body !== undefined) content.body = body;
      if (action !== undefined) content.action = action;
      if (parameter !== undefined) content.parameter = parameter;
      if (result !== undefined) content.result = result;
      if (request.ephemeral === true) content.ephemeral = true;
      const signal = isActivitySignal(request.signal) ? request.signal : undefined;
      if (signal !== undefined) content.signal = signal;
      const signalMetadata = isRecord(request.signalMetadata) ? request.signalMetadata : undefined;
      if (signalMetadata !== undefined) content.signalMetadata = signalMetadata;
      return { sessionId: job.sessionId, content };
    };

    interface LinearPlanItem {
      readonly content: string;
      readonly status: "pending" | "inProgress" | "completed" | "canceled";
    }

    const decodeSessionUpdate = (
      job: ProjectionJob,
    ): {
      readonly sessionId: string;
      readonly plan?: ReadonlyArray<LinearPlanItem>;
      readonly externalUrls?: ReadonlyArray<{ readonly label: string; readonly url: string }>;
    } => {
      if (!isRecord(job.payload) || !isRecord(job.payload.request)) {
        throw new Error(`Projection ${job.sourceKey} session update is invalid`);
      }
      const raw = job.payload.request;
      const planInput = Array.isArray(raw.plan)
        ? raw.plan
        : isRecord(raw.plan) && Array.isArray(raw.plan.items)
          ? raw.plan.items
          : undefined;
      const plan = planInput?.map((entry) => {
        if (!isRecord(entry)) {
          throw new Error(`Projection ${job.sourceKey} plan item is invalid`);
        }
        const content = text(entry.content);
        const status = text(entry.status);
        if (content === undefined || status === undefined) {
          throw new Error(`Projection ${job.sourceKey} plan item is invalid`);
        }
        return { content, status } as LinearPlanItem;
      });
      const externalUrls = Array.isArray(raw.externalUrls)
        ? raw.externalUrls.map((entry) => {
            if (!isRecord(entry)) {
              throw new Error(`Projection ${job.sourceKey} external URL is invalid`);
            }
            const label = text(entry.label);
            const url = text(entry.url);
            if (label === undefined || url === undefined) {
              throw new Error(`Projection ${job.sourceKey} external URL is invalid`);
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
    };

    const dispatch = Effect.fn("ActivityProjector.dispatch")(
      function* (
        sourceKey: SourceKey,
        now?: number,
      ): Effect.fn.Return<boolean, DatabaseError | RowDecodeError, never> {
        const at = now ?? (yield* Clock.currentTimeMillis);
        const jobOption = yield* projectionRepo.claim(sourceKey, owner, PROJECTION_LEASE_MS, at);
        if (Option.isNone(jobOption)) return false;
        const job = jobOption.value;

        const failWithBackoff = (
          message: string,
        ): Effect.Effect<void, DatabaseError | RowDecodeError, never> =>
          Effect.gen(function* () {
            const delay = projectionBackoff(job.attempt, 1_000, MAX_PROJECTION_BACKOFF_MS);
            const nextAttemptAt = at + delay;
            yield* projectionRepo.fail(sourceKey, owner, message, nextAttemptAt);
            yield* Effect.logWarning("projector.dispatch.failed", {
              sourceKey,
              error: message,
              nextAttemptAt,
            });
          });

        return yield* Effect.gen(function* () {
          if (job.activityType === "plan" || job.activityType === "externalUrls") {
            const update = yield* Effect.try({
              try: () => decodeSessionUpdate(job),
              catch: (error) =>
                new LinearApiError({
                  operation: "decodeSessionUpdate",
                  message: error instanceof Error ? error.message : String(error),
                }),
            });
            if (update.plan !== undefined && update.plan.length === 0) {
              yield* projectionRepo.complete(sourceKey, owner, Option.none());
              return true;
            }
            const redactedUpdate = redactStringValues({ ...update }) as {
              readonly sessionId: string;
              readonly plan?: ReadonlyArray<LinearPlanItem>;
              readonly externalUrls?: ReadonlyArray<{ readonly label: string; readonly url: string }>;
            };
            yield* linear.updateSession(redactedUpdate);
            yield* projectionRepo.complete(sourceKey, owner, Option.none());
          } else {
            const request = yield* Effect.try({
              try: () => decodeActivityRequest(job),
              catch: (error) =>
                new LinearApiError({
                  operation: "decodeActivityRequest",
                  message: error instanceof Error ? error.message : String(error),
                }),
            });
            const redactedContent = redactStringValues(request.content) as Record<string, unknown>;
            const activityId = yield* linear.createActivity({
              sessionId: request.sessionId,
              content: redactedContent,
            });
            yield* projectionRepo.complete(
              sourceKey,
              owner,
              Option.some(activityId as ActivityId),
            );
          }
          const updatedAt = yield* Clock.currentTimeMillis;
          yield* runRepo.update(job.sessionId, {
            lastActivityAt: Option.some(updatedAt),
          });
          return true;
        }).pipe(
          Effect.catchTags({
            "@Gateway/LinearApiError": (error: LinearApiError) =>
              Effect.gen(function* () {
                yield* failWithBackoff(error.message);
                return false;
              }),
            "@Gateway/LinearRateLimitError": (error: LinearRateLimitError) =>
              Effect.gen(function* () {
                yield* failWithBackoff(error.message);
                return false;
              }),
            "@Gateway/TokenRefreshError": (error: TokenRefreshError) =>
              Effect.gen(function* () {
                yield* failWithBackoff(error.message);
                return false;
              }),
            "@Gateway/InstallationRevokedError": (error: InstallationRevokedError) =>
              Effect.gen(function* () {
                yield* failWithBackoff(error.message);
                return false;
              }),
            "@Gateway/RunLeaseError": (error: RunLeaseError) =>
              Effect.gen(function* () {
                yield* failWithBackoff(error.message);
                return false;
              }),
          }),
        );
      },
    );

    const enqueueAndDispatch = Effect.fn("ActivityProjector.enqueueAndDispatch")(
      function* (
        sessionId: string,
        sourceKey: string,
        activityType: string,
        payload: unknown,
        firstWriteWins = false,
      ): Effect.fn.Return<boolean, DatabaseError | RowDecodeError, never> {
        const serialized = yield* stringify(payload);
        const payloadHash = yield* sha256(serialized);
        const enqueued = yield* projectionRepo.enqueue({
          sourceKey: sourceKey as SourceKey,
          sessionId: sessionId as SessionId,
          activityType,
          payloadHash,
          payload,
          firstWriteWins,
        });
        if (!enqueued) {
          yield* Effect.logDebug("projector.enqueue.duplicate", {
            sourceKey,
            payloadHash,
          });
        }
        return yield* dispatch(sourceKey as SourceKey, undefined);
      },
    );

    const activity = Effect.fn("ActivityProjector.activity")(
      function* (
        sessionId: string,
        sourceKey: string,
        content: LinearActivityContent,
        ephemeral: boolean,
        signal?: "auth" | "continue" | "select" | "stop",
        signalMetadata?: Record<string, unknown>,
        firstWriteWins = false,
      ): Effect.fn.Return<boolean, DatabaseError | RowDecodeError, never> {
        const request: Record<string, unknown> = { sessionId, content, ephemeral };
        if (signal !== undefined) request.signal = signal;
        if (signalMetadata !== undefined) request.signalMetadata = signalMetadata;
        return yield* enqueueAndDispatch(sessionId, sourceKey, content.type, { request }, firstWriteWins);
      },
    );

    const sessionUpdate = Effect.fn("ActivityProjector.sessionUpdate")(
      function* (
        sessionId: string,
        sourceKey: string,
        activityType: "plan" | "externalUrls",
        request:
          | { readonly sessionId: string; readonly plan?: ReadonlyArray<LinearPlanItem> }
          | { readonly sessionId: string; readonly externalUrls?: ReadonlyArray<{ readonly label: string; readonly url: string }> },
      ): Effect.fn.Return<boolean, DatabaseError | RowDecodeError, never> {
        return yield* enqueueAndDispatch(sessionId, sourceKey, activityType, { request });
      },
    );

    const thought = Effect.fn("ActivityProjector.thought")(
      function* (
        sessionId: string,
        sourceKey: string,
        body: string,
        ephemeral = true,
      ): Effect.fn.Return<boolean, DatabaseError | RowDecodeError, never> {
        yield* Effect.annotateCurrentSpan({
          "projector.sessionId": sessionId,
          "projector.sourceKey": sourceKey,
        });
        return yield* activity(
          sessionId,
          sourceKey,
          { type: "thought", body: boundedText(body) },
          ephemeral,
        );
      },
    );

    const elicitation = Effect.fn("ActivityProjector.elicitation")(
      function* (
        sessionId: string,
        sourceKey: string,
        body: string,
        options?: ReadonlyArray<string>,
      ): Effect.fn.Return<boolean, DatabaseError | RowDecodeError, never> {
        const signalMetadata = options ? { options: [...options] } : undefined;
        const signal: "select" | undefined = options ? "select" : undefined;
        return yield* activity(
          sessionId,
          sourceKey,
          { type: "elicitation", body: boundedText(body) },
          false,
          signal,
          signalMetadata,
        );
      },
    );

    const terminal = Effect.fn("ActivityProjector.terminal")(
      function* (
        sessionId: string,
        sourceKey: string,
        type: "response" | "error",
        body: string,
      ): Effect.fn.Return<boolean, DatabaseError | RowDecodeError, never> {
        return yield* activity(
          sessionId,
          `terminal:${sessionId}:${sourceKey}`,
          { type, body: boundedText(body) },
          false,
          undefined,
          undefined,
          true,
        );
      },
    );

    const plan = Effect.fn("ActivityProjector.plan")(
      function* (
        sessionId: string,
        sourceKey: string,
        items: ReadonlyArray<LinearPlanItem>,
      ): Effect.fn.Return<boolean, DatabaseError | RowDecodeError, never> {
        if (items.length === 0) return false;
        const normalized = items.map((item) => ({
          content: item.content,
          status: item.status,
        }));
        return yield* sessionUpdate(sessionId, sourceKey, "plan", {
          sessionId,
          plan: normalized,
        });
      },
    );

    const externalUrls = Effect.fn("ActivityProjector.externalUrls")(
      function* (
        sessionId: string,
        sourceKey: string,
        urls: ReadonlyArray<{ readonly label: string; readonly url: string }>,
      ): Effect.fn.Return<boolean, DatabaseError | RowDecodeError | LinearApiError, never> {
        const normalized = yield* Effect.forEach(urls, (entry) =>
          Effect.try({
            try: () => {
              const parsed = new URL(entry.url);
              if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
                throw new Error(`Unsupported external URL protocol ${parsed.protocol}`);
              }
              return { label: entry.label, url: parsed.toString() };
            },
            catch: (error) =>
              new LinearApiError({
                operation: "externalUrls",
                message: error instanceof Error ? error.message : String(error),
              }),
          }),
        );
        return yield* sessionUpdate(sessionId, sourceKey, "externalUrls", {
          sessionId,
          externalUrls: normalized,
        });
      },
    );

    const flushPending = Effect.fn("ActivityProjector.flushPending")(
      function* (
        limit = 50,
        now?: number,
      ): Effect.fn.Return<number, DatabaseError | RowDecodeError, never> {
        const at = now ?? (yield* Clock.currentTimeMillis);
        const keys = yield* projectionRepo.due(at, limit);
        let completed = 0;
        for (const sourceKey of keys) {
          const ok = yield* dispatch(sourceKey, at);
          if (ok) completed += 1;
        }
        return completed;
      },
    );

    const projectRpcEvent = Effect.fn("ActivityProjector.projectRpcEvent")(
      function* (
        sessionId: string,
        sequence: number,
        event: unknown,
      ): Effect.fn.Return<void, DatabaseError | RowDecodeError, never> {
        if (!isRecord(event)) return;
        yield* Effect.annotateCurrentSpan({
          "projector.sessionId": sessionId,
          "projector.sequence": sequence,
        });
        const sourceKey = `rpc:${sessionId}:${sequence}:${event.type}`;
        switch (event.type) {
          case "agent_start":
            yield* thought(sessionId, sourceKey, "OhMyPi worker started", true);
            return;
          case "turn_start":
            yield* thought(sessionId, sourceKey, "Starting the next agent turn", true);
            return;
          case "tool_execution_start": {
            const toolName = text(event.toolName) ?? text(event.tool) ?? "tool";
            const parameter = isString(event.args)
              ? event.args
              : (yield* stringify(event.args ?? {}));
            yield* activity(
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
          case "tool_execution_end": {
            const toolName = text(event.toolName) ?? text(event.tool) ?? "tool";
            const result = isString(event.result)
              ? event.result
              : (yield* stringify(event.result ?? {}));
            yield* activity(
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
          case "message_end": {
            const body = assistantText(event);
            if (body !== undefined) {
              yield* Ref.update(assistantDraft, (m) => {
                const n = new Map(m);
                n.set(sessionId, body);
                return n;
              });
            }
            return;
          }
          case "agent_end": {
            if (event.willContinue === true) return;
            const draft = yield* Ref.get(assistantDraft);
            const body = Option.match(Option.fromNullable(draft.get(sessionId)), {
              onNone: () => "OhMyPi run completed.",
              onSome: (value) => value,
            });
            yield* Ref.update(assistantDraft, (m) => {
              const n = new Map(m);
              n.delete(sessionId);
              return n;
            });
            yield* terminal(sessionId, sourceKey, "response", body);
            return;
          }
          case "error": {
            yield* Ref.update(assistantDraft, (m) => {
              const n = new Map(m);
              n.delete(sessionId);
              return n;
            });
            const message = text(event.message) ?? "OhMyPi worker failed";
            yield* terminal(sessionId, sourceKey, "error", message);
            return;
          }
          case "extension_ui_request":
            return;
          default:
            return;
        }
      },
    );

    return {
      thought,
      elicitation,
      terminal,
      plan,
      externalUrls,
      flushPending,
      projectRpcEvent,
    };
  }),
}) {}

export class SessionAuthority extends Effect.Service<SessionAuthority>()("SessionAuthority", {
  accessors: true,
  effect: Effect.gen(function* () {
    const processRunnable = Effect.fn("SessionAuthority.processRunnable")(function* (): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: SessionAuthority.processRunnable"); });
    const processSession = Effect.fn("SessionAuthority.processSession")(function* (sessionId: string): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: SessionAuthority.processSession"); });
    const shutdown = Effect.fn("SessionAuthority.shutdown")(function* (): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: SessionAuthority.shutdown"); });
    return { processRunnable, processSession, shutdown };
  }),
}) {}

export class RpcWorker extends Effect.Service<RpcWorker>()("RpcWorker", {
  accessors: true,
  effect: Effect.gen(function* () {
    const start = Effect.fn("RpcWorker.start")(function* (sessionId: string): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: RpcWorker.start"); });
    const stop = Effect.fn("RpcWorker.stop")(function* (sessionId: string): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: RpcWorker.stop"); });
    return { start, stop };
  }),
}) {}
export interface ReconcilerStatus {
  readonly running: boolean;
  readonly lastStartedAt: number | null;
  readonly lastCompletedAt: number | null;
  readonly lastError: string | null;
}

export class Reconciler extends Effect.Service<Reconciler>()("Reconciler", {
  accessors: true,
  dependencies: [SessionAuthority.Default],
  effect: Effect.gen(function* () {
    const statusRef = yield* Ref.make<ReconcilerStatus>({
      running: true,
      lastStartedAt: null,
      lastCompletedAt: null,
      lastError: null,
    });
    const inFlight = yield* Ref.make<Option.Option<Deferred<void, never>>>(Option.none());
    const authority = yield* SessionAuthority;

    const tick = Effect.fn("Reconciler.tick")(
      function* (): Effect.Effect<void, never, never> {
        const myDeferred = yield* Deferred.make<void, never>();
        const claim = yield* Ref.modify(inFlight, (current) => {
          if (Option.isSome(current)) return [current, current] as const;
          const next = Option.some(myDeferred);
          return [next, next] as const;
        });
        if (Option.isSome(claim) && claim.value !== myDeferred) {
          yield* Deferred.await(claim.value);
          return;
        }
        const now = yield* Clock.currentTimeMillis;
        yield* Ref.update(statusRef, (s) => ({ ...s, running: true, lastStartedAt: now }));

        const perform = authority.processRunnable().pipe(
          Effect.matchCauseEffect({
            onSuccess: () =>
              Effect.gen(function* () {
                const completedAt = yield* Clock.currentTimeMillis;
                return yield* Ref.update(statusRef, (s) => ({
                  ...s,
                  lastCompletedAt: completedAt,
                  lastError: null,
                }));
              }),
            onFailure: (cause) =>
              Effect.gen(function* () {
                const message = Cause.pretty(cause);
                yield* Ref.update(statusRef, (s) => ({ ...s, lastError: message }));
                yield* Effect.logWarning("reconciler.tick.error", { error: message });
              }),
          }),
          Effect.ensuring(
            Effect.gen(function* () {
              yield* Ref.set(inFlight, Option.none());
              yield* Deferred.succeed(myDeferred, undefined);
            }),
          ),
        );

        yield* perform;
      },
    );

    const status = Effect.fn("Reconciler.status")(
      function* (): Effect.Effect<ReconcilerStatus, never, never> {
        return yield* Ref.get(statusRef);
      },
    );

    return { tick, status };
  }),
}) {}
export type { RepositoryResolution } from "./workspace.js";
export class Workspace extends Effect.Service<Workspace>()("Workspace", {
  accessors: true,
  dependencies: [GatewayConfig.Default, WorkspaceRepo.Default],
  effect: Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const repo = yield* WorkspaceRepo;
    return yield* makeWorkspace({ workspaceRoot: config.workspaceRoot, repo });
  }),
}) {}

export class OAuth extends Effect.Service<OAuth>()("OAuth", {
  accessors: true,
  effect: Effect.gen(function* () {
    const startAuthorization = Effect.fn("OAuth.startAuthorization")(function* (): Effect.fn.Return<{ readonly state: string; readonly url: URL }, never> { return yield* Effect.dieMessage("unimplemented: OAuth.startAuthorization"); });
    const completeAuthorization = Effect.fn("OAuth.completeAuthorization")(function* (url: URL): Effect.fn.Return<Installation, never> { return yield* Effect.dieMessage("unimplemented: OAuth.completeAuthorization"); });
    return { startAuthorization, completeAuthorization };
  }),
}) {}

export class Admin extends Effect.Service<Admin>()("Admin", {
  accessors: true,
  effect: Effect.gen(function* () {
    const handle = Effect.fn("Admin.handle")(function* (request: Request): Effect.fn.Return<Option.Option<Response>, never> { return yield* Effect.dieMessage("unimplemented: Admin.handle"); });
    return { handle };
  }),
}) {}
