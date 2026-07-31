import { createAdminHandle } from "./admin.js";
import { createHash, randomUUID } from "node:crypto";
import type { ActivityId, SessionId, SourceKey } from "../domain/ids.js";
import { AgentActivitySignal, LinearClient } from "@linear/sdk";
import type { RpcEvent, RpcWorkerHandle } from "./rpc-worker.js";
import { RpcWorker } from "./rpc-worker.js";
import { Cause, Clock, Deferred, Effect, Fiber, Option, Queue, Redacted, Ref } from "effect";
import {
  DatabaseError,
  InstallationRevokedError,
  LinearApiError,
  LinearRateLimitError,
  RowDecodeError,
  RpcProtocolError,
  RpcSpawnError,
  RpcTimeoutError,
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
import { AdminSessionRepo, InstallationRepo, ProjectionRepo, RunEventRepo, RunInputRepo, RunRepo, WorkspaceRepo } from "./store/repositories.js";

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

export { WebhookPipeline } from "./webhook.js";
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

interface InputContext {
  readonly organizationId: string | null;
  readonly teamId: string | null;
  readonly projectId: string | null;
  readonly repositoryId: string | null;
  readonly issueLabels: ReadonlyArray<string>;
  readonly projectLabels: ReadonlyArray<string>;
}

interface WorkerState {
  readonly worker: RpcWorkerHandle;
  readonly queue: Queue.Queue<RpcEvent>;
  readonly consumer: Fiber.Fiber<never, never>;
  readonly unsubscribe: () => Effect.Effect<void, never, never>;
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function nullableString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function jsonValue(value: unknown): string {
  if (typeof value === "string") return value;
  if (value === undefined || value === null) return "{}";
  return JSON.stringify(value);
}

function labelSet(value: unknown): ReadonlyArray<string> {
  if (!Array.isArray(value)) return [];
  const seen = new Set<string>();
  const out: string[] = [];
  for (const item of value) {
    let raw: string | null = null;
    if (typeof item === "string") {
      raw = item;
    } else if (record(item) && typeof item.name === "string") {
      raw = item.name;
    }
    if (!raw) continue;
    const normalized = raw.trim().toLowerCase();
    if (!normalized) continue;
    if (seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(normalized);
  }
  return out.sort();
}

function inputContext(payload: unknown): InputContext {
  if (!record(payload)) {
    return {
      organizationId: null,
      teamId: null,
      projectId: null,
      repositoryId: null,
      issueLabels: [],
      projectLabels: [],
    };
  }
  const session = record(payload.agentSession) ? payload.agentSession : null;
  const issue = session && record(session.issue) ? session.issue : null;
  const project = issue && record(issue.project) ? issue.project : null;
  return {
    organizationId:
      nullableString(payload.organizationId) ??
      (session ? nullableString(session.organizationId) : null),
    teamId: issue ? nullableString(issue.teamId) : null,
    projectId: issue
      ? (nullableString(issue.projectId) ??
        (project ? nullableString(project.id) : null))
      : null,
    repositoryId: nullableString(payload.repositoryId),
    issueLabels: issue ? labelSet(issue.labels) : [],
    projectLabels: project ? labelSet(project.labels) : [],
  };
}

function planItems(
  value: unknown,
): Array<{
  content: string;
  status: "pending" | "inProgress" | "completed" | "canceled";
}> {
  if (!Array.isArray(value)) return [];
  const candidates = value.flatMap((phase) =>
    record(phase) && Array.isArray(phase.tasks) ? phase.tasks : [phase],
  );
  const items: Array<{
    content: string;
    status: "pending" | "inProgress" | "completed" | "canceled";
  }> = [];
  for (const candidate of candidates) {
    if (!record(candidate) || typeof candidate.content !== "string") continue;
    const status =
      candidate.status === "in_progress" || candidate.status === "inProgress"
        ? "inProgress"
        : candidate.status === "completed"
          ? "completed"
          : candidate.status === "canceled" || candidate.status === "cancelled"
            ? "canceled"
            : "pending";
    items.push({ content: candidate.content, status });
  }
  return items;
}

function failureCorrelationId(
  sessionId: string,
  attempt: number,
  message: string,
): string {
  return createHash("sha256")
    .update(`${sessionId}\0${attempt}\0${message}`)
    .digest("hex")
    .slice(0, 12);
}

export class SessionAuthority extends Effect.Service<SessionAuthority>()(
  "SessionAuthority",
  {
    accessors: true,
    dependencies: [
      ActivityProjector.Default,
      InstallationRepo.Default,
      RunEventRepo.Default,
      RunInputRepo.Default,
      RunRepo.Default,
      WorkspaceRepo.Default,
      GatewayConfig.Default,
      RpcWorker.Default,
    ],
    effect: Effect.gen(function* () {
      const runRepo = yield* RunRepo;
      const runInputRepo = yield* RunInputRepo;
      const installationRepo = yield* InstallationRepo;
      const runEventRepo = yield* RunEventRepo;
      const workspaceRepo = yield* WorkspaceRepo;
      const projector = yield* ActivityProjector;
      const rpc = yield* RpcWorker;
      const config = yield* GatewayConfig;

      const owner = `authority:${yield* Effect.sync(() =>
        crypto.randomUUID(),
      )}`;
      const leaseDurationMs = config.leaseDurationMs;
      const maxAttempts = 5;

      const runUrlForSession =
        config.publicUrl != null
          ? (sessionId: SessionId) =>
              new URL(
                `/runs/${encodeURIComponent(sessionId)}`,
                config.publicUrl,
              ).toString()
          : null;

      const workspace = yield* makeWorkspace({
        workspaceRoot: config.workspaceRoot,
        repo: workspaceRepo,
      });

      const workersRef =
        yield* Ref.make<ReadonlyMap<SessionId, WorkerState>>(new Map());
      const eventSequenceRef =
        yield* Ref.make<ReadonlyMap<SessionId, number>>(new Map());
      const pendingUiRef =
        yield* Ref.make<
          ReadonlyMap<SessionId, { readonly id: string; readonly method: string }>
        >(new Map());

      const getWorker = (
        sessionId: SessionId,
      ): Effect.Effect<Option.Option<WorkerState>, never, never> =>
        Ref.get(workersRef).pipe(
          Effect.map((workers) => Option.fromNullable(workers.get(sessionId))),
        );

      const releaseIfNoWorker = (
        sessionId: SessionId,
      ): Effect.Effect<void, never, never> =>
        Effect.gen(function* () {
          const workers = yield* Ref.get(workersRef);
          if (!workers.has(sessionId)) {
            yield* runRepo.releaseLease(sessionId, owner).pipe(Effect.ignore);
          }
        });

      const recordRunEvent = Effect.fn("SessionAuthority.recordRunEvent")(
        function* (sessionId: SessionId,
        sequence: number,
        event: RpcEvent,): Effect.fn.Return<void, never, never> { const type = event.type;
        let sourceKey = `rpc:${sessionId}:${sequence}:${type}`;
        let kind = type;
        let level: "debug" | "info" | "warn" | "result" | "error" = "info";
        let text: string | null = type;
        
        switch (type) {
          case "agent_start":
            kind = "agent";
            text = "OhMyPi agent started";
            level = "info";
            break;
          case "turn_start":
            kind = "turn";
            text = "OhMyPi agent turn started";
            level = "info";
            break;
          case "turn_end":
            kind = "turn";
            text = "OhMyPi agent turn ended";
            level = "info";
            break;
          case "agent_end":
            kind = "agent";
            text =
              event.willContinue === true
                ? "OhMyPi agent turn ended (will continue)"
                : "OhMyPi agent ended";
            level = "result";
            break;
          case "tool_execution_start":
            kind = "tool";
            text = `Tool started: ${jsonValue(event.toolName ?? event.tool)}`;
            level = "info";
            break;
          case "tool_execution_end":
            kind = "tool";
            text = `Tool completed: ${jsonValue(event.toolName ?? event.tool)}`;
            level = event.error ? "error" : "result";
            break;
          case "message_end":
            kind = "message";
            text = "OhMyPi message ended";
            level = "info";
            break;
          case "prompt_result":
            kind = "prompt";
            text =
              event.agentInvoked === false
                ? "OhMyPi command completed without agent"
                : "OhMyPi prompt result";
            level = event.agentInvoked === false ? "result" : "info";
            break;
          case "extension_ui_request": {
            if (typeof event.id === "string") {
              sourceKey = `rpc-ui:${event.id}`;
            }
            const title =
              typeof event.title === "string" ? event.title : "Input required";
            const message =
              typeof event.message === "string" ? event.message : "";
            text = [title, message].filter(Boolean).join("\n\n");
            level = "warn";
            break;
          }
          case "error":
            text =
              typeof event.message === "string"
                ? event.message
                : "OhMyPi worker failed";
            break;
          default:
            text = type;
        }
        
        const now = yield* Clock.currentTimeMillis;
        yield* runEventRepo
          .upsert({
            sourceKey: sourceKey as SessionId & { __tag: "SourceKey" },
            sessionId,
            kind,
            level,
            text,
            payload: event,
            status: "observed",
            now,
          })
          .pipe(Effect.ignore); },
      );

      const captureWorkerState = Effect.fn("SessionAuthority.captureWorkerState")(
        function* (sessionId: SessionId,
        worker: RpcWorkerHandle,): Effect.fn.Return<void, never, never> { const state = yield* worker.getState().pipe(
          Effect.orElseSucceed(() => ({} as Record<string, unknown>)),
        );
        
        const fromStateSessionId = isString(state.sessionId)
          ? Option.some(state.sessionId)
          : Option.none<string>();
        const fromStateSessionFile = isString(state.sessionFile)
          ? Option.some(state.sessionFile)
          : Option.none<string>();
        
        const workerSessionId = yield* worker.sessionId;
        const workerSessionFile = yield* worker.sessionFile;
        
        const ompSessionId = Option.isSome(fromStateSessionId)
          ? fromStateSessionId
          : workerSessionId;
        const ompSessionFile = Option.isSome(fromStateSessionFile)
          ? fromStateSessionFile
          : workerSessionFile;
        
        yield* runRepo
          .update(sessionId, { ompSessionId, ompSessionFile })
          .pipe(Effect.ignore);
        
        if (Array.isArray(state.todoPhases)) {
          const items = planItems(state.todoPhases);
          if (items.length > 0) {
            const fingerprint = yield* Effect.sync(() =>
              createHash("sha256")
                .update(JSON.stringify(items))
                .digest("hex")
                .slice(0, 16),
            );
            yield* projector
              .plan(sessionId, `plan:${sessionId}:${fingerprint}`, items)
              .pipe(Effect.ignore);
          }
        } },
      );

      const finishLocalCommand = Effect.fn("SessionAuthority.finishLocalCommand")(
        function* (sessionId: SessionId,
        worker: RpcWorkerHandle,
        sourceId: string,): Effect.fn.Return<void, never, never> { yield* captureWorkerState(sessionId, worker).pipe(Effect.ignore);
        yield* runRepo
          .update(sessionId, {
            state: "waiting",
            nextAttemptAt: Option.none(),
          })
          .pipe(Effect.ignore);
        yield* projector
          .thought(
            sessionId,
            `local-command:${sourceId}`,
            "The OhMyPi command completed without starting an agent turn.",
          )
          .pipe(Effect.ignore); },
      );

      const cancel = Effect.fn("SessionAuthority.cancel")(
        function* (run: AgentRun): Effect.fn.Return<void, never, never> { const state = yield* getWorker(run.sessionId);
        if (Option.isSome(state)) {
          yield* state.value.worker.abort().pipe(Effect.ignore);
          yield* state.value.worker.stop();
          yield* Ref.update(workersRef, (workers) => {
            const next = new Map(workers);
            next.delete(run.sessionId);
            return next;
          });
        }
        
        yield* Ref.update(pendingUiRef, (pending) => {
          const next = new Map(pending);
          next.delete(run.sessionId);
          return next;
        });
        
        if (
          run.state !== "succeeded" &&
          run.state !== "failed" &&
          run.state !== "canceled"
        ) {
          yield* runRepo
            .update(run.sessionId, {
              state: "canceled",
              terminalReason: Option.some("Stopped by Linear user"),
            })
            .pipe(Effect.ignore);
          yield* Effect.logInfo("run.canceled", {
            event: "run.canceled",
            sessionId: run.sessionId,
            attempt: run.attempt,
          });
          yield* projector
            .terminal(
              run.sessionId,
              `stop:${run.sessionId}`,
              "response",
              "Stopped as requested.",
            )
            .pipe(Effect.ignore);
        }
        
        yield* runRepo.releaseLease(run.sessionId, owner).pipe(Effect.ignore); },
      );

      const handleFailure = Effect.fn("SessionAuthority.handleFailure")(
        function* (sessionId: SessionId,
        error: unknown,): Effect.fn.Return<void, never, never> { const message = error instanceof Error ? error.message : String(error);
        
        const worker = yield* getWorker(sessionId);
        if (Option.isSome(worker)) {
          yield* worker.value.worker.stop();
          yield* Ref.update(workersRef, (workers) => {
            const next = new Map(workers);
            next.delete(sessionId);
            return next;
          });
        }
        
        yield* Effect.logWarning("authority.failure", {
          event: "authority.failure",
          sessionId,
          error: message,
        });
        
        const current = yield* runRepo
          .get(sessionId)
          .pipe(Effect.orElseSucceed(() => Option.none<AgentRun>()));
        
        if (
          Option.isNone(current) ||
          current.value.state === "succeeded" ||
          current.value.state === "canceled"
        ) {
          return;
        }
        
        const run = current.value;
        
        if (run.desiredState === "canceled") {
          yield* cancel(run);
          return;
        }
        
        if (run.attempt >= maxAttempts) {
          const correlationId = failureCorrelationId(
            sessionId,
            run.attempt,
            message,
          );
          yield* runRepo
            .update(run.sessionId, {
              state: "failed",
              terminalReason: Option.some(`${message} [${correlationId}]`),
              nextAttemptAt: Option.none(),
            })
            .pipe(Effect.ignore);
          yield* Effect.logInfo("run.failed", {
            event: "run.failed",
            sessionId,
            attempt: run.attempt,
            correlationId,
            terminalReason: `${message} [${correlationId}]`,
          });
          yield* projector
            .terminal(
              sessionId,
              `failure:${correlationId}`,
              "error",
              `The OhMyPi run failed after ${run.attempt} attempts. Reference: ${correlationId}`,
            )
            .pipe(Effect.ignore);
          return;
        }
        
        const delay = Math.min(
          300_000,
          10_000 * 2 ** Math.min(run.attempt, 5),
        );
        const jitter = yield* Effect.sync(() =>
          Math.floor(Math.random() * 1_000),
        );
        const now = yield* Clock.currentTimeMillis;
        const nextAttemptAt = now + delay + jitter;
        
        yield* runRepo
          .update(run.sessionId, {
            state: "orphaned",
            terminalReason: Option.some(message),
            nextAttemptAt: Option.some(nextAttemptAt),
          })
          .pipe(Effect.ignore);
        yield* Effect.logInfo("run.retried", {
          event: "run.retried",
          sessionId,
          attempt: run.attempt,
          delay,
          nextAttemptAt,
        }); },
      );

      const handleEvent = Effect.fn("SessionAuthority.handleEvent")(
        function* (sessionId: SessionId,
        event: RpcEvent,): Effect.fn.Return<void, RpcProtocolError, never> { const current = yield* runRepo
          .get(sessionId)
          .pipe(Effect.orElseSucceed(() => Option.none<AgentRun>()));
        
        if (
          Option.isNone(current) ||
          current.value.desiredState === "canceled" ||
          current.value.state === "canceled"
        ) {
          return;
        }
        
        const run = current.value;
        const sequence = yield* Ref.modify(eventSequenceRef, (m) => {
          const next = new Map(m);
          const value = (next.get(sessionId) ?? 0) + 1;
          next.set(sessionId, value);
          return [value, next] as const;
        });
        
        yield* recordRunEvent(sessionId, sequence, event);
        
        if (
          event.type === "extension_ui_request" &&
          typeof event.id === "string" &&
          typeof event.method === "string" &&
          ["select", "confirm", "input", "editor"].includes(event.method)
        ) {
          yield* Ref.update(pendingUiRef, (pending) => {
            const next = new Map(pending);
            next.set(sessionId, { id: event.id, method: event.method });
            return next;
          });
        
          const title =
            typeof event.title === "string" ? event.title : "Input required";
          const message =
            typeof event.message === "string" ? event.message : "";
          const options = Array.isArray(event.options)
            ? event.options.filter((option): option is string =>
                typeof option === "string",
              )
            : [];
        
          yield* projector
            .elicitation(
              sessionId,
              `rpc-ui:${event.id}`,
              [title, message].filter(Boolean).join("\n\n"),
              options.length > 0 ? options : undefined,
            )
            .pipe(Effect.ignore);
          yield* runRepo
            .update(sessionId, { state: "waiting" })
            .pipe(Effect.ignore);
          return;
        }
        
        if (event.type === "prompt_result" && event.agentInvoked === false) {
          const worker = yield* getWorker(sessionId);
          if (Option.isSome(worker)) {
            yield* finishLocalCommand(
              sessionId,
              worker.value.worker,
              typeof event.id === "string"
                ? event.id
                : `prompt-result:${sequence}`,
            );
          }
          return;
        }
        
        if (event.type === "error") {
          return yield* Effect.fail(
            new RpcProtocolError({
              method: "worker",
              message:
                typeof event.message === "string"
                  ? event.message
                  : "OhMyPi worker failed",
            }),
          );
        }
        
        const worker = yield* getWorker(sessionId);
        const terminalAgentEnd =
          event.type === "agent_end" && event.willContinue !== true;
        
        if (
          Option.isSome(worker) &&
          (event.type === "agent_start" ||
            event.type === "turn_end" ||
            event.type === "agent_end")
        ) {
          yield* captureWorkerState(sessionId, worker.value.worker).pipe(
            Effect.ignore,
          );
        }
        
        if (terminalAgentEnd) {
          yield* Effect.logInfo("run.completed", {
            event: "run.completed",
            sessionId,
            attempt: run.attempt,
          });
          yield* runRepo
            .update(sessionId, {
              state: "succeeded",
              nextAttemptAt: Option.none(),
            })
            .pipe(Effect.ignore);
        
          const project = projector
            .projectRpcEvent(sessionId, sequence, event)
            .pipe(
              Effect.ensuring(
                Effect.gen(function* () {
                  if (Option.isSome(worker)) {
                    yield* worker.value.worker.stop();
                  }
                  yield* Ref.update(workersRef, (workers) => {
                    const next = new Map(workers);
                    next.delete(sessionId);
                    return next;
                  });
                  yield* runRepo
                    .releaseLease(sessionId, owner)
                    .pipe(Effect.ignore);
                }),
              ),
            );
        
          yield* project;
          return;
        }
        
        yield* projector.projectRpcEvent(sessionId, sequence, event).pipe(
          Effect.ignore,
        ); },
      );

      const startWorker = Effect.fn("SessionAuthority.startWorker")(
        function* (run: AgentRun,
        cwd: string,): Effect.fn.Return<RpcWorkerHandle,
        RpcProtocolError | RpcSpawnError | RpcTimeoutError,
        never> { const command: string[] = [config.ompCliPath];
        if (Option.isSome(run.ompSessionFile)) {
          command.push("--session", run.ompSessionFile.value);
        }
        
        const worker = yield* rpc.spawn({
          command,
          cwd,
        });
        
        const queue = yield* Queue.unbounded<RpcEvent>();
        
        const unsubscribe = yield* worker.onEvent((event) => {
          Queue.unsafeOffer(queue, event);
        });
        
        const consumer = yield* Effect.fork(
          Effect.forever(
            Queue.take(queue).pipe(
              Effect.flatMap((event) => handleEvent(run.sessionId, event)),
              Effect.matchEffect({
                onSuccess: () => Effect.void,
                onFailure: (error) => handleFailure(run.sessionId, error),
              }),
            ),
          ),
        );
        
        yield* Ref.update(workersRef, (workers) => {
          const next = new Map(workers);
          next.set(run.sessionId, {
            worker,
            queue,
            consumer,
            unsubscribe,
          });
          return next;
        });
        
        yield* worker.start();
        yield* runRepo
          .update(run.sessionId, { state: "running" })
          .pipe(Effect.ignore);
        yield* captureWorkerState(run.sessionId, worker).pipe(Effect.ignore);
        
        const ompSessionId = yield* worker.sessionId;
        const ompSessionFile = yield* worker.sessionFile;
        
        yield* Effect.logInfo("work.ready", {
          event: "work.ready",
          sessionId: run.sessionId,
          attempt: run.attempt,
          cwd,
          ompSessionId: Option.getOrElse(ompSessionId, () => null),
          ompSessionFile: Option.getOrElse(ompSessionFile, () => null),
        });
        
        return worker; },
      );

      const processSession = Effect.fn("SessionAuthority.processSession")(
        function* (sessionId: SessionId): Effect.fn.Return<void, never, never> { return yield* Effect.gen(function* () {
          const initial = yield* runRepo
            .get(sessionId)
            .pipe(Effect.orElseSucceed(() => Option.none<AgentRun>()));
        
          if (
            Option.isSome(initial) &&
            initial.value.desiredState === "canceled"
          ) {
            yield* cancel(initial.value);
            return;
          }
        
          const workerState = yield* getWorker(sessionId);
          if (Option.isSome(workerState)) {
            const renewed = yield* runRepo.renewLease(
              sessionId,
              owner,
              leaseDurationMs,
            );
            if (!renewed) {
              return;
            }
          } else {
            const claimed = yield* runRepo.claimLease(
              sessionId,
              owner,
              leaseDurationMs,
            );
            if (!claimed) {
              return;
            }
          }
        
          const runOption = yield* runRepo.get(sessionId);
          if (Option.isNone(runOption)) {
            return;
          }
          const run = runOption.value;
        
          yield* Effect.logInfo("work.assigned", {
            event: "work.assigned",
            sessionId,
            attempt: run.attempt,
            state: run.state,
          });
        
          if (run.desiredState === "canceled") {
            yield* cancel(run);
            return;
          }
        
          const installation = yield* installationRepo
            .get(run.organizationId)
            .pipe(Effect.orElseSucceed(() => Option.none<Installation>()));
        
          if (
            Option.isNone(installation) ||
            Option.isSome(installation.value.revokedAt)
          ) {
            yield* runRepo
              .update(sessionId, {
                state: "failed",
                terminalReason: Option.some("Linear installation is unavailable"),
              })
              .pipe(Effect.ignore);
            yield* projector
              .terminal(
                sessionId,
                `installation-unavailable:${run.organizationId}`,
                "error",
                "The Linear installation is unavailable. Reinstall or reauthorize the app, then try again.",
              )
              .pipe(Effect.ignore);
            return;
          }
        
          const teamAccess = Option.match(
            installation.value.accessibleTeamIds,
            { onNone: () => [] as ReadonlyArray<string>, onSome: (ids) => ids },
          );
          const canAccessAll =
            Option.match(installation.value.canAccessAllPublicTeams, {
              onNone: () => false,
              onSome: (value) => value,
            });
        
          if (
            Option.isSome(run.teamId) &&
            !canAccessAll &&
            !teamAccess.includes(run.teamId.value)
          ) {
            yield* runRepo
              .update(sessionId, {
                state: "canceled",
                terminalReason: Option.some("Linear team access was removed"),
              })
              .pipe(Effect.ignore);
            yield* projector
              .terminal(
                sessionId,
                `team-access-removed:${run.teamId.value}`,
                "response",
                "Stopped because this Linear installation no longer has access to the issue's team.",
              )
              .pipe(Effect.ignore);
            return;
          }
        
          const inputs = yield* runInputRepo
            .pending(sessionId)
            .pipe(Effect.orElseSucceed(() => [] as ReadonlyArray<RunInput>));
        
          let worker: RpcWorkerHandle | undefined =
            Option.getOrElse(workerState, () => undefined);
        
          if (inputs.length === 0) {
            if (
              worker === undefined &&
              run.state === "orphaned" &&
              Option.isSome(run.workspacePath)
            ) {
              yield* runRepo
                .update(sessionId, {
                  state: "starting",
                  incrementAttempt: true,
                  nextAttemptAt: Option.none(),
                })
                .pipe(Effect.ignore);
        
              const resumedOption = yield* runRepo.get(sessionId);
              const resumed = Option.getOrElse(resumedOption, () => run);
        
              yield* Effect.logInfo("run.retried", {
                event: "run.retried",
                sessionId,
                attempt: resumed.attempt,
                workspacePath: run.workspacePath.value,
              });
        
              worker = yield* startWorker(resumed, run.workspacePath.value);
        
              yield* projector
                .thought(
                  sessionId,
                  `retry:${resumed.attempt}`,
                  `Retrying the interrupted OhMyPi run (attempt ${resumed.attempt}).`,
                )
                .pipe(Effect.ignore);
        
              if (Option.isSome(run.ompSessionFile)) {
                yield* worker
                  .followUp(
                    "Continue the interrupted Linear task from the saved session state.",
                  )
                  .pipe(Effect.ignore);
              } else {
                const latestActionable = yield* runInputRepo
                  .latestActionableInput(sessionId)
                  .pipe(Effect.orElseSucceed(() => Option.none()));
        
                if (Option.isNone(latestActionable)) {
                  return yield* Effect.fail(
                    new Error("Interrupted run has no input to resume"),
                  );
                }
        
                const agentInvoked = yield* worker.prompt(
                  latestActionable.value.body,
                );
                if (!agentInvoked) {
                  yield* finishLocalCommand(
                    sessionId,
                    worker,
                    `retry:${resumed.attempt}`,
                  );
                }
              }
            }
            return;
          }
        
          for (const input of inputs) {
            const latestOption = yield* runRepo
              .get(sessionId)
              .pipe(Effect.orElseSucceed(() => Option.none<AgentRun>()));
            const latest = Option.getOrElse(latestOption, () => run);
        
            if (
              latest.desiredState === "canceled" ||
              input.kind === "stop"
            ) {
              yield* cancel(latest);
              yield* runInputRepo.markProcessed(input.id).pipe(Effect.ignore);
              break;
            }
        
            if (worker === undefined) {
              if (runUrlForSession !== null) {
                yield* projector
                  .externalUrls(sessionId, `run-url:${sessionId}`, [
                    {
                      label: "OhMyPi run",
                      url: runUrlForSession(sessionId),
                    },
                  ])
                  .pipe(Effect.ignore);
              }
        
              yield* projector
                .thought(
                  sessionId,
                  `accepted:${input.id}`,
                  "Request accepted; preparing the OhMyPi worker.",
                )
                .pipe(Effect.ignore);
        
              const baseContext = inputContext(input.payload);
              const resolvedContext = {
                ...baseContext,
                organizationId:
                  baseContext.organizationId ??
                  (run.organizationId as string),
              };
        
              const context =
                input.kind === "prompted" &&
                latest.state === "waiting" &&
                resolvedContext.repositoryId === null
                  ? {
                      ...resolvedContext,
                      repositoryId: input.body.trim(),
                    }
                  : resolvedContext;
        
              const resolution = yield* workspace.resolve(context);
        
              if (resolution.kind === "none") {
                yield* projector
                  .elicitation(
                    sessionId,
                    `repo:none:${input.id}`,
                    "No repository is configured for this Linear issue.",
                  )
                  .pipe(Effect.ignore);
                yield* runRepo
                  .update(sessionId, { state: "waiting" })
                  .pipe(Effect.ignore);
                return;
              }
        
              if (resolution.kind === "ambiguous") {
                yield* projector
                  .elicitation(
                    sessionId,
                    `repo:ambiguous:${input.id}`,
                    "Select the repository for this issue.",
                    resolution.repositories.map((r) => r.id),
                  )
                  .pipe(Effect.ignore);
                yield* runRepo
                  .update(sessionId, { state: "waiting" })
                  .pipe(Effect.ignore);
                return;
              }
        
              const workspacePath = yield* workspace.materialize(
                sessionId,
                resolution.repository,
              );
        
              yield* runRepo
                .update(sessionId, {
                  state: "starting",
                  repositoryId: Option.some(resolution.repository.id),
                  workspacePath: Option.some(workspacePath),
                  incrementAttempt: true,
                })
                .pipe(Effect.ignore);
        
              const updatedOption = yield* runRepo.get(sessionId);
              const updated = Option.getOrElse(updatedOption, () => run);
        
              worker = yield* startWorker(updated, workspacePath);
        
              const agentInvoked = yield* worker.prompt(input.body);
              if (!agentInvoked) {
                yield* finishLocalCommand(sessionId, worker, input.id);
              }
            } else if (input.kind === "prompted") {
              const pendingUi = yield* Ref.get(pendingUiRef).pipe(
                Effect.map((m) => m.get(sessionId)),
              );
        
              if (pendingUi !== undefined) {
                const normalized = input.body.trim().toLowerCase();
                const response =
                  pendingUi.method === "confirm"
                    ? {
                        confirmed:
                          /^(?:y|yes|true|confirm|confirmed)$/u.test(
                            normalized,
                          ),
                      }
                    : { value: input.body };
                yield* worker
                  .respondToUi(pendingUi.id, response)
                  .pipe(Effect.ignore);
                yield* Ref.update(pendingUiRef, (m) => {
                  const next = new Map(m);
                  next.delete(sessionId);
                  return next;
                });
              } else {
                const streaming = yield* worker.isStreaming;
                if (streaming) {
                  yield* worker.steer(input.body).pipe(Effect.ignore);
                } else {
                  yield* worker.followUp(input.body).pipe(Effect.ignore);
                }
              }
        
              yield* runRepo
                .update(sessionId, { state: "running" })
                .pipe(Effect.ignore);
            }
        
            yield* runInputRepo.markProcessed(input.id).pipe(Effect.ignore);
          }
        }).pipe(
          Effect.matchEffect({
            onSuccess: () => Effect.void,
            onFailure: (error) => handleFailure(sessionId, error),
          }),
          Effect.ensuring(releaseIfNoWorker(sessionId)),
        ); },
      );

      const processRunnable = Effect.fn("SessionAuthority.processRunnable")(
        function* (): Effect.fn.Return<void,
        | DatabaseError
        | RowDecodeError
        | TokenCipherError,
        never> { yield* projector.flushPending().pipe(Effect.ignore);
        
        const workers = yield* Ref.get(workersRef);
        for (const [sessionId, workerState] of workers) {
          const run = yield* runRepo
            .get(sessionId)
            .pipe(Effect.orElseSucceed(() => Option.none<AgentRun>()));
          if (
            Option.isNone(run) ||
            run.value.desiredState === "canceled"
          ) {
            continue;
          }
        
          const renewed = yield* runRepo.renewLease(
            sessionId,
            owner,
            leaseDurationMs,
          );
          if (!renewed) {
            yield* workerState.worker.abort().pipe(Effect.ignore);
            yield* workerState.worker.stop();
            yield* Ref.update(workersRef, (m) => {
              const next = new Map(m);
              next.delete(sessionId);
              return next;
            });
          }
        }
        
        const cancellationPending =
          yield* runRepo.listCancellationPending();
        for (const run of cancellationPending) {
          yield* cancel(run);
        }
        
        const runnable = yield* runRepo.listRunnable();
        const sessionsWithInputs =
          yield* runInputRepo.listSessionsWithPendingInputs();
        
        const sessionIds = new Set<SessionId>([
          ...sessionsWithInputs,
          ...runnable.map((r) => r.sessionId),
        ]);
        
        for (const sessionId of sessionIds) {
          yield* processSession(sessionId);
        }
        
        yield* projector.flushPending().pipe(Effect.ignore); },
      );

      const shutdown = Effect.fn("SessionAuthority.shutdown")(
        function* (): Effect.fn.Return<void, never, never> { const workers = yield* Ref.get(workersRef);
        
        for (const [, workerState] of workers) {
          yield* Fiber.interrupt(workerState.consumer);
          yield* workerState.unsubscribe();
          yield* workerState.worker.stop();
        }
        
        yield* Ref.set(workersRef, new Map());
        yield* Ref.set(eventSequenceRef, new Map());
        yield* Ref.set(pendingUiRef, new Map());
        
        yield* projector.flushPending().pipe(Effect.ignore); },
      );

      const activeWorkerCount = Effect.fn("SessionAuthority.activeWorkerCount")(
        function* (): Effect.fn.Return<number, never, never> { const workers = yield* Ref.get(workersRef);
        return workers.size; },
      );

      return {
        processRunnable,
        processSession,
        shutdown,
        activeWorkerCount,
      };
    }),
  },
) {}

export { RpcWorker } from "./rpc-worker.js";
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

export { OAuth } from "./oauth.js";

export class Admin extends Effect.Service<Admin>()("Admin", {
  accessors: true,
  dependencies: [
    GatewayConfig.Default,
    AdminSessionRepo.Default,
    InstallationRepo.Default,
    RunRepo.Default,
    RunEventRepo.Default,
    WorkspaceRepo.Default,
    Workspace.Default,
    Reconciler.Default,
  ],
  effect: Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const adminSessionRepo = yield* AdminSessionRepo;
    const installationRepo = yield* InstallationRepo;
    const runRepo = yield* RunRepo;
    const runEventRepo = yield* RunEventRepo;
    const workspaceRepo = yield* WorkspaceRepo;
    const workspace = yield* Workspace;
    const reconciler = yield* Reconciler;
    const handle = createAdminHandle({
      config,
      adminSessionRepo,
      installationRepo,
      runRepo,
      runEventRepo,
      workspaceRepo,
      workspace,
      reconciler,
    });
    return { handle };
  }),
}) {}
