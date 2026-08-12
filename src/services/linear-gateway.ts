import { AgentActivitySignal, LinearClient } from "@linear/sdk";
import { Clock, Deferred, Effect, Option, Redacted, Ref, Schema } from "effect";
import {
  type DatabaseError,
  InstallationRevokedError,
  LinearApiError,
  LinearRateLimitError,
  type RowDecodeError,
  type TokenCipherError,
  TokenRefreshError,
} from "../domain/errors.js";
import { IssueId, OrganizationId, SessionId } from "../domain/ids.js";
import type { ActivityType, Installation } from "../domain/models.js";
import { GatewayConfig } from "./config.js";
import {
  isActivitySignal,
  isActivityType,
  isNumber,
  isRecord,
  isString,
} from "./linear-helpers.js";
import { InstallationRepo, RunRepo } from "./store/repositories.js";
export interface ListedSessionActivity {
  readonly id: string;
  readonly type:
    | "thought"
    | "action"
    | "elicitation"
    | "response"
    | "error"
    | "prompt"
    | "unknown";
  readonly body: string | null;
  readonly title: string | null;
  readonly signal: "auth" | "continue" | "select" | "stop" | null;
  readonly createdAt: string;
}

type TokenResponse = {
  readonly accessToken: string;
  readonly tokenType: string;
  readonly expiresIn: number;
  readonly refreshToken: string;
  readonly scopes: readonly string[];
};
const LINEAR_TOKEN_URL = "https://api.linear.app/oauth/token";
const TOKEN_REFRESH_BUFFER_MS = 5 * 60 * 1000;
const STARTED_STATES_CACHE_TTL_MS = 60 * 60 * 1000;
const STARTED_STATES_CACHE_MAX_TEAMS = 100;
const LIST_SESSION_ACTIVITIES_MAX_PAGES = 10;

export class LinearGateway extends Effect.Service<LinearGateway>()(
  "LinearGateway",
  {
    accessors: true,
    dependencies: [
      InstallationRepo.Default,
      RunRepo.Default,
      GatewayConfig.Default,
    ],
    effect: Effect.gen(function* () {
      const installationRepo = yield* InstallationRepo;
      const runRepo = yield* RunRepo;
      const config = yield* GatewayConfig;
      const refreshing = yield* Ref.make(
        new Map<string, Deferred.Deferred<Installation, TokenRefreshError>>(),
      );
      const queues = yield* Ref.make(
        new Map<string, Deferred.Deferred<void, never>>(),
      );
      const startedStatesCache = yield* Ref.make<
        Map<
          string,
          {
            readonly states: ReadonlyArray<{
              id: string;
              name: string;
              position: number;
            }>;
            readonly fetchedAt: number;
          }
        >
      >(new Map());

      const text = (value: unknown): string | undefined =>
        isString(value) ? value : undefined;

      const parseTokenResponse = (value: unknown): TokenResponse => {
        if (!isRecord(value))
          throw new Error("Linear token response is not an object");
        const accessToken = requireString(value, "access_token");
        const tokenType = requireString(value, "token_type");
        const expiresIn = parseExpiresIn(value.expires_in);
        const refreshToken = requireString(value, "refresh_token");
        const scopes = parseScopes(value.scope);
        if (tokenType.toLowerCase() !== "bearer")
          throw new Error(`Unexpected token type ${tokenType}`);
        return { accessToken, tokenType, expiresIn, refreshToken, scopes };
      };
      const requireString = (
        value: Record<string, unknown>,
        key: string,
      ): string => {
        const field = value[key];
        if (!isString(field))
          throw new Error(`Linear token response missing or invalid ${key}`);
        return field;
      };
      const parseScopes = (raw: unknown): readonly string[] => {
        if (isString(raw)) return raw.split(/[,\s]+/).filter(Boolean);
        if (Array.isArray(raw)) {
          return raw.map((item, index) => {
            if (!isString(item))
              throw new Error(
                `Linear token response scope[${index}] is not a string`,
              );
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
      const mapLinearError = (
        operation: string,
        error: unknown,
      ): LinearApiError | LinearRateLimitError => {
        const delay = rateLimitDelay(error);
        if (delay !== null) {
          return new LinearRateLimitError({
            message: "Linear rate limit exceeded",
            retryAfterMs: delay,
          });
        }
        const status =
          isRecord(error) && isNumber(error.status) ? error.status : undefined;
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

      const decodeActivityContent = (
        value: Record<string, unknown>,
      ): DecodedActivityContent => {
        const type = value.type;
        if (!isActivityType(type))
          throw new Error(`Invalid activity type ${type}`);
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
      const mapActivityContent = (
        content: DecodedActivityContent,
      ): Record<string, unknown> => {
        switch (content.type) {
          case "thought":
            return { type: "thought", body: content.body ?? "" };
          case "action": {
            const action = content.action ?? "";
            const parameter = content.parameter ?? "";
            if (!action)
              throw new Error("Linear action activity requires an action");
            if (!parameter)
              throw new Error("Linear action activity requires a parameter");
            const payload: Record<string, unknown> = {
              type: "action",
              action,
              parameter,
            };
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
      const mapActivitySignal = (
        signal: "auth" | "continue" | "select" | "stop",
      ): AgentActivitySignal => {
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
        input: {
          readonly grantType: string;
          readonly code?: string;
          readonly refreshToken?: string;
          readonly redirectUri?: string;
        },
        organizationId: string,
      ) =>
        Effect.tryPromise({
          try: async () => {
            const body = new URLSearchParams();
            body.set("client_id", config.linearClientId);
            body.set(
              "client_secret",
              Redacted.value(config.linearClientSecret),
            );
            body.set("grant_type", input.grantType);
            if (input.code) body.set("code", input.code);
            if (input.refreshToken)
              body.set("refresh_token", input.refreshToken);
            if (input.redirectUri) body.set("redirect_uri", input.redirectUri);
            const response = await fetch(LINEAR_TOKEN_URL, {
              method: "POST",
              headers: { "Content-Type": "application/x-www-form-urlencoded" },
              body: body.toString(),
            });
            if (!response.ok) {
              throw new Error(
                `Linear token exchange failed (${response.status}): ${await response.text()}`,
              );
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
        function* (
          record: Installation,
          now: number,
        ): Effect.fn.Return<Installation, TokenRefreshError> {
          yield* Effect.logInfo("linear.token.refresh").pipe(
            Effect.annotateLogs({
              organizationId: record.organizationId,
            }),
          );
          const response = yield* exchangeToken(
            { grantType: "refresh_token", refreshToken: record.refreshToken },
            record.organizationId,
          );
          const updated: Installation = {
            ...record,
            accessToken: response.accessToken,
            refreshToken: response.refreshToken,
            expiresAt: now + response.expiresIn * 1000,
          };
          yield* installationRepo.put(updated).pipe(
            Effect.catchTags({
              "@Gateway/DatabaseError": (error: DatabaseError) =>
                Effect.fail(
                  new TokenRefreshError({
                    organizationId: record.organizationId,
                    message: error.message,
                  }),
                ),
              "@Gateway/TokenCipherError": (error: TokenCipherError) =>
                Effect.fail(
                  new TokenRefreshError({
                    organizationId: record.organizationId,
                    message: error.message,
                  }),
                ),
            }),
          );
          yield* Effect.logInfo("linear.token.refreshed").pipe(
            Effect.annotateLogs({
              organizationId: record.organizationId,
              expiresAt: updated.expiresAt,
            }),
          );
          return updated;
        },
      );

      const refreshTokens = Effect.fn("LinearGateway.refreshTokens")(function* (
        record: Installation,
        now: number,
      ): Effect.fn.Return<Installation, TokenRefreshError> {
        const myDeferred = yield* Deferred.make<
          Installation,
          TokenRefreshError
        >();
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
      });

      const ensureToken = Effect.fn("LinearGateway.ensureToken")(function* (
        organizationId: string,
      ): Effect.fn.Return<
        Installation,
        InstallationRevokedError | TokenRefreshError
      > {
        const now = yield* Clock.currentTimeMillis;
        const orgId = yield* Schema.decodeUnknown(OrganizationId)(
          organizationId,
        ).pipe(
          Effect.catchTag(
            "ParseError",
            (error) =>
              new TokenRefreshError({
                organizationId,
                message: `Invalid organization id: ${error.message}`,
              }),
          ),
        );
        const option = yield* installationRepo.get(orgId).pipe(
          Effect.catchTags({
            "@Gateway/DatabaseError": (error: DatabaseError) =>
              Effect.fail(
                new TokenRefreshError({
                  organizationId,
                  message: error.message,
                }),
              ),
            "@Gateway/RowDecodeError": (error: RowDecodeError) =>
              Effect.fail(
                new TokenRefreshError({
                  organizationId,
                  message: error.message,
                }),
              ),
            "@Gateway/TokenCipherError": (error: TokenCipherError) =>
              Effect.fail(
                new TokenRefreshError({
                  organizationId,
                  message: error.message,
                }),
              ),
          }),
        );
        return yield* Option.match(option, {
          onNone: () =>
            Effect.fail(
              new TokenRefreshError({
                organizationId,
                message: `No Linear installation for ${organizationId}`,
              }),
            ),
          onSome: (record) =>
            Option.match(record.revokedAt, {
              onNone: () =>
                record.expiresAt - TOKEN_REFRESH_BUFFER_MS > now
                  ? Effect.succeed(record)
                  : refreshTokens(record, now),
              onSome: () =>
                Effect.fail(
                  new InstallationRevokedError({
                    organizationId,
                    message: `Linear installation for ${organizationId} is revoked`,
                  }),
                ),
            }),
        });
      });

      const clientFor = (organizationId: string) =>
        Effect.gen(function* () {
          const installation = yield* ensureToken(organizationId);
          return new LinearClient({ accessToken: installation.accessToken });
        });

      const withRateLimitRetry = <A>(
        effect: Effect.Effect<A, LinearApiError | LinearRateLimitError>,
      ): Effect.Effect<A, LinearApiError | LinearRateLimitError> => {
        const run = (
          retriesRemaining: number,
        ): Effect.Effect<A, LinearApiError | LinearRateLimitError> =>
          effect.pipe(
            Effect.catchSome((error) => {
              if (
                error._tag !== "@Gateway/LinearRateLimitError" ||
                retriesRemaining === 0
              ) {
                return Option.none();
              }
              return Option.some(
                Effect.gen(function* () {
                  yield* Effect.logDebug("linear.api.rateLimited").pipe(
                    Effect.annotateLogs({
                      retryAfterMs: error.retryAfterMs,
                    }),
                  );
                  yield* Effect.sleep(error.retryAfterMs ?? 1_000);
                  return yield* run(retriesRemaining - 1);
                }),
              );
            }),
          );
        return run(1);
      };

      const withOrgQueue = <A, E>(
        organizationId: string,
        effect: Effect.Effect<A, E>,
      ): Effect.Effect<A, E> =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const myDone = yield* Deferred.make<void, never>();
            const previous = yield* Ref.modify(queues, (m) => {
              const prev = m.get(organizationId);
              const next = new Map(m);
              next.set(organizationId, myDone);
              return [prev, next] as const;
            });
            if (previous !== undefined) {
              yield* Deferred.await(previous);
            }
            return yield* restore(effect).pipe(
              Effect.ensuring(
                Effect.gen(function* () {
                  yield* Ref.update(queues, (m) => {
                    if (m.get(organizationId) !== myDone) return m;
                    const next = new Map(m);
                    next.delete(organizationId);
                    return next;
                  });
                  yield* Deferred.succeed(myDone, undefined);
                }),
              ),
            );
          }),
        );

      const createActivity = Effect.fn("LinearGateway.createActivity")(
        function* (input: {
          readonly sessionId: string;
          readonly content: unknown;
        }): Effect.fn.Return<
          string,
          | InstallationRevokedError
          | LinearApiError
          | LinearRateLimitError
          | TokenRefreshError
        > {
          const sessionId = yield* Schema.decodeUnknown(SessionId)(
            input.sessionId,
          ).pipe(
            Effect.catchTag(
              "ParseError",
              (error) =>
                new LinearApiError({
                  operation: "createActivity",
                  message: `Invalid session id: ${error.message}`,
                }),
            ),
          );
          yield* Effect.annotateCurrentSpan({ "linear.sessionId": sessionId });
          if (!isRecord(input.content)) {
            yield* Effect.logWarning("linear.createActivity.invalid").pipe(
              Effect.annotateLogs({
                sessionId,
                reason: "content_not_record",
              }),
            );
            return yield* Effect.fail(
              new LinearApiError({
                operation: "createActivity",
                message: "Activity content is not a record",
              }),
            );
          }
          const content = input.content;
          const runOption = yield* runRepo.get(sessionId).pipe(
            Effect.catchTags({
              "@Gateway/DatabaseError": (error: DatabaseError) =>
                Effect.fail(
                  new LinearApiError({
                    operation: "createActivity",
                    message: error.message,
                  }),
                ),
              "@Gateway/RowDecodeError": (error: RowDecodeError) =>
                Effect.fail(
                  new LinearApiError({
                    operation: "createActivity",
                    message: error.message,
                  }),
                ),
            }),
          );
          const run = yield* Option.match(runOption, {
            onNone: () =>
              Effect.fail(
                new LinearApiError({
                  operation: "createActivity",
                  message: `Unknown run ${sessionId}`,
                }),
              ),
            onSome: Effect.succeed,
          });
          yield* Effect.annotateCurrentSpan({
            "linear.organizationId": run.organizationId,
          });

          const contentRecord = yield* Effect.try({
            try: () => mapActivityContent(decodeActivityContent(content)),
            catch: (error) =>
              new LinearApiError({
                operation: "createActivity",
                message: error instanceof Error ? error.message : String(error),
              }),
          });

          const activityInput: Parameters<
            LinearClient["createAgentActivity"]
          >[0] = {
            agentSessionId: sessionId,
            content: contentRecord,
          };
          if (content.ephemeral === true) activityInput.ephemeral = true;
          const signal = isActivitySignal(content.signal)
            ? content.signal
            : undefined;
          if (signal !== undefined)
            activityInput.signal = mapActivitySignal(signal);
          if (isRecord(content.signalMetadata)) {
            activityInput.signalMetadata = content.signalMetadata;
          }

          const id = yield* withOrgQueue(
            run.organizationId,
            Effect.gen(function* () {
              const client = yield* clientFor(run.organizationId);
              return yield* withRateLimitRetry(
                Effect.tryPromise({
                  try: async () => {
                    const payload =
                      await client.createAgentActivity(activityInput);
                    if (!payload.success)
                      throw new Error("Linear failed to create agent activity");
                    const id =
                      payload.agentActivityId ??
                      (payload.agentActivity
                        ? (await payload.agentActivity).id
                        : undefined);
                    if (!id)
                      throw new Error(
                        "Linear did not return an agent activity id",
                      );
                    return id;
                  },
                  catch: (error) => mapLinearError("createActivity", error),
                }),
              );
            }),
          );

          yield* Effect.logInfo("linear.createActivity").pipe(
            Effect.annotateLogs({
              sessionId,
              activityId: id,
            }),
          );
          return id;
        },
      );

      const listSessionActivities = Effect.fn(
        "LinearGateway.listSessionActivities",
      )(function* (input: {
        readonly sessionId: string;
      }): Effect.fn.Return<
        ReadonlyArray<ListedSessionActivity>,
        | InstallationRevokedError
        | LinearApiError
        | LinearRateLimitError
        | TokenRefreshError
      > {
        const sessionId = yield* Schema.decodeUnknown(SessionId)(
          input.sessionId,
        ).pipe(
          Effect.catchTag(
            "ParseError",
            (error) =>
              new LinearApiError({
                operation: "listSessionActivities",
                message: `Invalid session id: ${error.message}`,
              }),
          ),
        );
        yield* Effect.annotateCurrentSpan({ "linear.sessionId": sessionId });
        const runOption = yield* runRepo.get(sessionId).pipe(
          Effect.catchTags({
            "@Gateway/DatabaseError": (error: DatabaseError) =>
              Effect.fail(
                new LinearApiError({
                  operation: "listSessionActivities",
                  message: error.message,
                }),
              ),
            "@Gateway/RowDecodeError": (error: RowDecodeError) =>
              Effect.fail(
                new LinearApiError({
                  operation: "listSessionActivities",
                  message: error.message,
                }),
              ),
          }),
        );
        const run = yield* Option.match(runOption, {
          onNone: () =>
            Effect.fail(
              new LinearApiError({
                operation: "listSessionActivities",
                message: `Unknown run ${sessionId}`,
              }),
            ),
          onSome: Effect.succeed,
        });
        yield* Effect.annotateCurrentSpan({
          "linear.organizationId": run.organizationId,
        });

        const mapSignal = (value: unknown): ListedSessionActivity["signal"] => {
          if (typeof value !== "string") return null;
          const normalized = value.toLowerCase();
          return normalized === "auth" ||
            normalized === "continue" ||
            normalized === "select" ||
            normalized === "stop"
            ? normalized
            : null;
        };
        const mapActivity = (activity: {
          readonly id: unknown;
          readonly content: unknown;
          readonly signal?: unknown;
          readonly createdAt: unknown;
        }): ListedSessionActivity => {
          const content =
            typeof activity.content === "object" && activity.content !== null
              ? (activity.content as Record<string, unknown>)
              : {};
          const typename = content.__typename;
          const type: ListedSessionActivity["type"] =
            typename === "AgentActivityThoughtContent"
              ? "thought"
              : typename === "AgentActivityActionContent"
                ? "action"
                : typename === "AgentActivityElicitationContent"
                  ? "elicitation"
                  : typename === "AgentActivityResponseContent"
                    ? "response"
                    : typename === "AgentActivityErrorContent"
                      ? "error"
                      : typename === "AgentActivityPromptContent"
                        ? "prompt"
                        : "unknown";
          const body = typeof content.body === "string" ? content.body : null;
          const title =
            typeof content.title === "string" ? content.title : null;
          const createdAt =
            activity.createdAt instanceof Date
              ? activity.createdAt.toISOString()
              : typeof activity.createdAt === "string"
                ? activity.createdAt
                : String(activity.createdAt ?? "");
          return {
            id:
              typeof activity.id === "string"
                ? activity.id
                : String(activity.id),
            type,
            body,
            title,
            signal: mapSignal(activity.signal),
            createdAt,
          };
        };

        return yield* withOrgQueue(
          run.organizationId,
          Effect.gen(function* () {
            const client = yield* clientFor(run.organizationId);
            const result = yield* withRateLimitRetry(
              Effect.tryPromise({
                try: async () => {
                  const activities: Array<ListedSessionActivity> = [];
                  const session = await client.agentSession(sessionId);
                  let after: string | undefined;
                  let pages = 0;
                  let capped = false;
                  while (pages < LIST_SESSION_ACTIVITIES_MAX_PAGES) {
                    const connection = await session.activities({
                      after,
                      first: 100,
                      orderBy: "createdAt",
                    } as Parameters<typeof session.activities>[0]);
                    pages += 1;
                    for (const activity of connection.nodes) {
                      activities.push(mapActivity(activity));
                    }
                    if (!connection.pageInfo.hasNextPage) break;
                    const next = connection.pageInfo.endCursor;
                    if (!next || next === after) break;
                    after = next;
                    if (pages === LIST_SESSION_ACTIVITIES_MAX_PAGES) {
                      capped = true;
                    }
                  }
                  return { activities, capped };
                },
                catch: (error) =>
                  mapLinearError("listSessionActivities", error),
              }),
            );
            if (result.capped) {
              yield* Effect.logDebug(
                "linear.listSessionActivities.pagination_capped",
              ).pipe(
                Effect.annotateLogs({
                  sessionId,
                  maxPages: LIST_SESSION_ACTIVITIES_MAX_PAGES,
                }),
              );
            }
            return result.activities;
          }),
        );
      });

      const updateSession = Effect.fn("LinearGateway.updateSession")(
        function* (input: {
          readonly sessionId: string;
          readonly plan?: ReadonlyArray<{
            readonly content: string;
            readonly status: string;
          }>;
          readonly externalUrls?: ReadonlyArray<{
            readonly label: string;
            readonly url: string;
          }>;
        }): Effect.fn.Return<
          void,
          | InstallationRevokedError
          | LinearApiError
          | LinearRateLimitError
          | TokenRefreshError
        > {
          const sessionId = yield* Schema.decodeUnknown(SessionId)(
            input.sessionId,
          ).pipe(
            Effect.catchTag(
              "ParseError",
              (error) =>
                new LinearApiError({
                  operation: "updateSession",
                  message: `Invalid session id: ${error.message}`,
                }),
            ),
          );
          yield* Effect.annotateCurrentSpan({ "linear.sessionId": sessionId });
          const runOption = yield* runRepo.get(sessionId).pipe(
            Effect.catchTags({
              "@Gateway/DatabaseError": (error: DatabaseError) =>
                Effect.fail(
                  new LinearApiError({
                    operation: "updateSession",
                    message: error.message,
                  }),
                ),
              "@Gateway/RowDecodeError": (error: RowDecodeError) =>
                Effect.fail(
                  new LinearApiError({
                    operation: "updateSession",
                    message: error.message,
                  }),
                ),
            }),
          );
          const run = yield* Option.match(runOption, {
            onNone: () =>
              Effect.fail(
                new LinearApiError({
                  operation: "updateSession",
                  message: `Unknown run ${sessionId}`,
                }),
              ),
            onSome: Effect.succeed,
          });
          yield* Effect.annotateCurrentSpan({
            "linear.organizationId": run.organizationId,
          });

          const updateInput: Parameters<LinearClient["updateAgentSession"]>[1] =
            {};
          if (input.plan !== undefined) {
            updateInput.plan = input.plan.map((item) => ({
              content: item.content,
              status: item.status,
            }));
          }
          if (input.externalUrls !== undefined) {
            updateInput.externalUrls = input.externalUrls.map((item) => ({
              label: item.label,
              url: item.url,
            }));
          }

          yield* withOrgQueue(
            run.organizationId,
            Effect.gen(function* () {
              const client = yield* clientFor(run.organizationId);
              return yield* withRateLimitRetry(
                Effect.tryPromise({
                  try: async () => {
                    const payload = await client.updateAgentSession(
                      sessionId,
                      updateInput,
                    );
                    if (!payload.success)
                      throw new Error("Linear failed to update agent session");
                  },
                  catch: (error) => mapLinearError("updateSession", error),
                }),
              );
            }),
          );

          yield* Effect.logInfo("linear.updateSession").pipe(
            Effect.annotateLogs({ sessionId }),
          );
        },
      );

      const teamStartedStates = Effect.fn("LinearGateway.teamStartedStates")(
        function* (input: {
          readonly sessionId: string;
          readonly teamId: string;
        }): Effect.fn.Return<
          ReadonlyArray<{ id: string; name: string; position: number }>,
          | InstallationRevokedError
          | LinearApiError
          | LinearRateLimitError
          | TokenRefreshError
        > {
          const sessionId = yield* Schema.decodeUnknown(SessionId)(
            input.sessionId,
          ).pipe(
            Effect.catchTag(
              "ParseError",
              (error) =>
                new LinearApiError({
                  operation: "teamStartedStates",
                  message: `Invalid session id: ${error.message}`,
                }),
            ),
          );
          const runOption = yield* runRepo.get(sessionId).pipe(
            Effect.catchTags({
              "@Gateway/DatabaseError": (error: DatabaseError) =>
                Effect.fail(
                  new LinearApiError({
                    operation: "teamStartedStates",
                    message: error.message,
                  }),
                ),
              "@Gateway/RowDecodeError": (error: RowDecodeError) =>
                Effect.fail(
                  new LinearApiError({
                    operation: "teamStartedStates",
                    message: error.message,
                  }),
                ),
            }),
          );
          const run = yield* Option.match(runOption, {
            onNone: () =>
              Effect.fail(
                new LinearApiError({
                  operation: "teamStartedStates",
                  message: `Unknown run ${sessionId}`,
                }),
              ),
            onSome: Effect.succeed,
          });
          const now = yield* Clock.currentTimeMillis;
          const cached = yield* Ref.get(startedStatesCache);
          const hit = cached.get(input.teamId);
          if (
            hit !== undefined &&
            now - hit.fetchedAt < STARTED_STATES_CACHE_TTL_MS
          ) {
            return hit.states;
          }
          const states = yield* withOrgQueue(
            run.organizationId,
            Effect.gen(function* () {
              const client = yield* clientFor(run.organizationId);
              return yield* withRateLimitRetry(
                Effect.tryPromise({
                  try: async () => {
                    const team = await client.team(input.teamId);
                    if (!team)
                      throw new Error(`Linear team ${input.teamId} not found`);
                    const connection = await team.states({
                      filter: { type: { eq: "started" } },
                      first: 50,
                    });
                    return connection.nodes
                      .map(
                        (state: {
                          id: string;
                          name: string;
                          position: number;
                        }) => ({
                          id: state.id,
                          name: state.name,
                          position: state.position,
                        }),
                      )
                      .sort(
                        (a: { position: number }, b: { position: number }) =>
                          a.position - b.position,
                      );
                  },
                  catch: (error) => mapLinearError("teamStartedStates", error),
                }),
              );
            }),
          );
          const fetchedAt = yield* Clock.currentTimeMillis;
          yield* Ref.update(startedStatesCache, (current) => {
            const next = new Map(current);
            if (
              !next.has(input.teamId) &&
              next.size >= STARTED_STATES_CACHE_MAX_TEAMS
            ) {
              let oldestTeamId: string | undefined;
              let oldestFetchedAt = Number.POSITIVE_INFINITY;
              for (const [teamId, entry] of next) {
                if (entry.fetchedAt < oldestFetchedAt) {
                  oldestTeamId = teamId;
                  oldestFetchedAt = entry.fetchedAt;
                }
              }
              if (oldestTeamId !== undefined) next.delete(oldestTeamId);
            }
            next.set(input.teamId, { states, fetchedAt });
            return next;
          });
          return states;
        },
      );

      const repositorySuggestions = Effect.fn(
        "LinearGateway.repositorySuggestions",
      )(function* (input: {
        readonly sessionId: string;
        readonly issueId: string;
        readonly candidates: ReadonlyArray<{
          readonly hostname: string;
          readonly repositoryFullName: string;
        }>;
      }): Effect.fn.Return<
        ReadonlyArray<{
          hostname: string;
          repositoryFullName: string;
          confidence: number;
        }>,
        | InstallationRevokedError
        | LinearApiError
        | LinearRateLimitError
        | TokenRefreshError
      > {
        const sessionId = yield* Schema.decodeUnknown(SessionId)(
          input.sessionId,
        ).pipe(
          Effect.catchTag(
            "ParseError",
            (error) =>
              new LinearApiError({
                operation: "repositorySuggestions",
                message: `Invalid session id: ${error.message}`,
              }),
          ),
        );
        const runOption = yield* runRepo.get(sessionId).pipe(
          Effect.catchTags({
            "@Gateway/DatabaseError": (error: DatabaseError) =>
              Effect.fail(
                new LinearApiError({
                  operation: "repositorySuggestions",
                  message: error.message,
                }),
              ),
            "@Gateway/RowDecodeError": (error: RowDecodeError) =>
              Effect.fail(
                new LinearApiError({
                  operation: "repositorySuggestions",
                  message: error.message,
                }),
              ),
          }),
        );
        const run = yield* Option.match(runOption, {
          onNone: () =>
            Effect.fail(
              new LinearApiError({
                operation: "repositorySuggestions",
                message: `Unknown run ${sessionId}`,
              }),
            ),
          onSome: Effect.succeed,
        });
        return yield* withOrgQueue(
          run.organizationId,
          Effect.gen(function* () {
            const client = yield* clientFor(run.organizationId);
            return yield* withRateLimitRetry(
              Effect.tryPromise({
                try: async () => {
                  const payload = await client.issueRepositorySuggestions(
                    input.candidates.map((candidate) => ({
                      hostname: candidate.hostname,
                      repositoryFullName: candidate.repositoryFullName,
                    })),
                    input.issueId,
                  );
                  return payload.suggestions
                    .filter(
                      (
                        suggestion,
                      ): suggestion is typeof suggestion & {
                        hostname: string;
                      } => typeof suggestion.hostname === "string",
                    )
                    .map((suggestion) => ({
                      hostname: suggestion.hostname,
                      repositoryFullName: suggestion.repositoryFullName,
                      confidence: suggestion.confidence,
                    }))
                    .sort((a, b) => b.confidence - a.confidence);
                },
                catch: (error) =>
                  mapLinearError("repositorySuggestions", error),
              }),
            );
          }),
        );
      });

      const getIssue = Effect.fn("LinearGateway.getIssue")(function* (input: {
        readonly sessionId: string;
        readonly issueId: string;
      }): Effect.fn.Return<
        {
          readonly id: string;
          readonly identifier: string | null;
          readonly title: string;
          readonly description: string | null;
          readonly stateId: string | null;
          readonly stateName: string | null;
          readonly stateType: string | null;
          readonly delegateId: string | null;
          readonly url: string | null;
          readonly teamId: string | null;
          readonly labels: ReadonlyArray<string>;
        },
        | InstallationRevokedError
        | LinearApiError
        | LinearRateLimitError
        | TokenRefreshError
      > {
        const sessionId = yield* Schema.decodeUnknown(SessionId)(
          input.sessionId,
        ).pipe(
          Effect.catchTag(
            "ParseError",
            (error) =>
              new LinearApiError({
                operation: "getIssue",
                message: `Invalid session id: ${error.message}`,
              }),
          ),
        );
        const issueId = input.issueId.trim();
        if (!issueId) {
          return yield* Effect.fail(
            new LinearApiError({
              operation: "getIssue",
              message: "Issue id is required",
            }),
          );
        }
        const runOption = yield* runRepo.get(sessionId).pipe(
          Effect.catchTags({
            "@Gateway/DatabaseError": (error: DatabaseError) =>
              Effect.fail(
                new LinearApiError({
                  operation: "getIssue",
                  message: error.message,
                }),
              ),
            "@Gateway/RowDecodeError": (error: RowDecodeError) =>
              Effect.fail(
                new LinearApiError({
                  operation: "getIssue",
                  message: error.message,
                }),
              ),
          }),
        );
        const run = yield* Option.match(runOption, {
          onNone: () =>
            Effect.fail(
              new LinearApiError({
                operation: "getIssue",
                message: `Unknown run ${sessionId}`,
              }),
            ),
          onSome: Effect.succeed,
        });
        return yield* withOrgQueue(
          run.organizationId,
          Effect.gen(function* () {
            const client = yield* clientFor(run.organizationId);
            return yield* withRateLimitRetry(
              Effect.tryPromise({
                try: async () => {
                  const issue = await client.issue(issueId);
                  if (!issue)
                    throw new Error(`Linear issue ${issueId} not found`);
                  const state = issue.state ? await issue.state : null;
                  const labelsConnection = await issue.labels();
                  return {
                    id: issue.id,
                    identifier: issue.identifier ?? null,
                    title: issue.title,
                    description: issue.description ?? null,
                    stateId: issue.stateId ?? state?.id ?? null,
                    stateName: state?.name ?? null,
                    stateType: state?.type ?? null,
                    delegateId: issue.delegateId ?? null,
                    url: issue.url ?? null,
                    teamId: issue.teamId ?? null,
                    labels: Array.isArray(labelsConnection?.nodes)
                      ? labelsConnection.nodes.map((label) => label.name)
                      : [],
                  };
                },
                catch: (error) => mapLinearError("getIssue", error),
              }),
            );
          }),
        );
      });

      const updateIssue = Effect.fn("LinearGateway.updateIssue")(
        function* (input: {
          readonly sessionId: string;
          readonly issueId: string;
          readonly stateId?: string;
          readonly delegateId?: string | null;
        }): Effect.fn.Return<
          void,
          | InstallationRevokedError
          | LinearApiError
          | LinearRateLimitError
          | TokenRefreshError
        > {
          const sessionId = yield* Schema.decodeUnknown(SessionId)(
            input.sessionId,
          ).pipe(
            Effect.catchTag(
              "ParseError",
              (error) =>
                new LinearApiError({
                  operation: "updateIssue",
                  message: `Invalid session id: ${error.message}`,
                }),
            ),
          );
          const issueId = input.issueId.trim();
          if (!issueId) {
            return yield* Effect.fail(
              new LinearApiError({
                operation: "updateIssue",
                message: "Issue id is required",
              }),
            );
          }
          if (input.stateId === undefined && input.delegateId === undefined) {
            return yield* Effect.fail(
              new LinearApiError({
                operation: "updateIssue",
                message: "At least one issue field is required",
              }),
            );
          }
          const runOption = yield* runRepo.get(sessionId).pipe(
            Effect.catchTags({
              "@Gateway/DatabaseError": (error: DatabaseError) =>
                Effect.fail(
                  new LinearApiError({
                    operation: "updateIssue",
                    message: error.message,
                  }),
                ),
              "@Gateway/RowDecodeError": (error: RowDecodeError) =>
                Effect.fail(
                  new LinearApiError({
                    operation: "updateIssue",
                    message: error.message,
                  }),
                ),
            }),
          );
          const run = yield* Option.match(runOption, {
            onNone: () =>
              Effect.fail(
                new LinearApiError({
                  operation: "updateIssue",
                  message: `Unknown run ${sessionId}`,
                }),
              ),
            onSome: Effect.succeed,
          });
          const updateInput: Parameters<LinearClient["updateIssue"]>[1] = {};
          if (input.stateId !== undefined) updateInput.stateId = input.stateId;
          if (input.delegateId !== undefined)
            updateInput.delegateId = input.delegateId;
          yield* withOrgQueue(
            run.organizationId,
            Effect.gen(function* () {
              const client = yield* clientFor(run.organizationId);
              yield* withRateLimitRetry(
                Effect.tryPromise({
                  try: async () => {
                    const payload = await client.updateIssue(
                      issueId,
                      updateInput,
                    );
                    if (!payload.success)
                      throw new Error("Linear failed to update issue");
                  },
                  catch: (error) => mapLinearError("updateIssue", error),
                }),
              );
            }),
          );
        },
      );

      const addSessionExternalUrls = Effect.fn(
        "LinearGateway.addSessionExternalUrls",
      )(function* (input: {
        readonly sessionId: string;
        readonly urls: ReadonlyArray<{
          readonly label: string;
          readonly url: string;
        }>;
      }): Effect.fn.Return<
        void,
        | InstallationRevokedError
        | LinearApiError
        | LinearRateLimitError
        | TokenRefreshError
      > {
        const sessionId = yield* Schema.decodeUnknown(SessionId)(
          input.sessionId,
        ).pipe(
          Effect.catchTag(
            "ParseError",
            (error) =>
              new LinearApiError({
                operation: "addSessionExternalUrls",
                message: `Invalid session id: ${error.message}`,
              }),
          ),
        );
        if (input.urls.length === 0) return;
        const runOption = yield* runRepo.get(sessionId).pipe(
          Effect.catchTags({
            "@Gateway/DatabaseError": (error: DatabaseError) =>
              Effect.fail(
                new LinearApiError({
                  operation: "addSessionExternalUrls",
                  message: error.message,
                }),
              ),
            "@Gateway/RowDecodeError": (error: RowDecodeError) =>
              Effect.fail(
                new LinearApiError({
                  operation: "addSessionExternalUrls",
                  message: error.message,
                }),
              ),
          }),
        );
        const run = yield* Option.match(runOption, {
          onNone: () =>
            Effect.fail(
              new LinearApiError({
                operation: "addSessionExternalUrls",
                message: `Unknown run ${sessionId}`,
              }),
            ),
          onSome: Effect.succeed,
        });
        yield* withOrgQueue(
          run.organizationId,
          Effect.gen(function* () {
            const client = yield* clientFor(run.organizationId);
            yield* withRateLimitRetry(
              Effect.tryPromise({
                try: async () => {
                  const payload = await client.updateAgentSession(sessionId, {
                    addedExternalUrls: input.urls.map((item) => ({
                      label: item.label,
                      url: item.url,
                    })),
                  });
                  if (!payload.success)
                    throw new Error("Linear failed to update agent session");
                },
                catch: (error) =>
                  mapLinearError("addSessionExternalUrls", error),
              }),
            );
          }),
        );
      });

      const createIssueComment = Effect.fn("LinearGateway.createIssueComment")(
        function* (input: {
          readonly sessionId: string;
          readonly body: string;
        }): Effect.fn.Return<
          string,
          | InstallationRevokedError
          | LinearApiError
          | LinearRateLimitError
          | TokenRefreshError
        > {
          const sessionId = yield* Schema.decodeUnknown(SessionId)(
            input.sessionId,
          ).pipe(
            Effect.catchTag(
              "ParseError",
              (error) =>
                new LinearApiError({
                  operation: "createIssueComment",
                  message: `Invalid session id: ${error.message}`,
                }),
            ),
          );
          const runOption = yield* runRepo.get(sessionId).pipe(
            Effect.catchTags({
              "@Gateway/DatabaseError": (error: DatabaseError) =>
                Effect.fail(
                  new LinearApiError({
                    operation: "createIssueComment",
                    message: error.message,
                  }),
                ),
              "@Gateway/RowDecodeError": (error: RowDecodeError) =>
                Effect.fail(
                  new LinearApiError({
                    operation: "createIssueComment",
                    message: error.message,
                  }),
                ),
            }),
          );
          const run = yield* Option.match(runOption, {
            onNone: () =>
              Effect.fail(
                new LinearApiError({
                  operation: "createIssueComment",
                  message: `Unknown run ${sessionId}`,
                }),
              ),
            onSome: Effect.succeed,
          });
          const issueId = yield* Option.match(run.issueId, {
            onNone: () =>
              Effect.fail(
                new LinearApiError({
                  operation: "createIssueComment",
                  message: `Run ${sessionId} is not linked to a Linear issue`,
                }),
              ),
            onSome: Effect.succeed,
          });

          return yield* withOrgQueue(
            run.organizationId,
            Effect.gen(function* () {
              const client = yield* clientFor(run.organizationId);
              return yield* withRateLimitRetry(
                Effect.tryPromise({
                  try: async () => {
                    const payload = await client.createComment({
                      issueId,
                      body: input.body,
                    });
                    if (!payload.success)
                      throw new Error("Linear failed to create issue comment");
                    const comment = await payload.comment;
                    if (!comment?.id)
                      throw new Error("Linear did not return a comment id");
                    return comment.id;
                  },
                  catch: (error) => mapLinearError("createIssueComment", error),
                }),
              );
            }),
          );
        },
      );

      const refreshInstallation = Effect.fn(
        "LinearGateway.refreshInstallation",
      )(function* (
        organizationId: string,
      ): Effect.fn.Return<
        string,
        InstallationRevokedError | TokenRefreshError
      > {
        yield* Effect.annotateCurrentSpan({
          "linear.organizationId": organizationId,
        });
        const installation = yield* ensureToken(organizationId);
        yield* Effect.logInfo("linear.refreshInstallation").pipe(
          Effect.annotateLogs({ organizationId }),
        );
        return installation.accessToken;
      });

      const createSessionOnIssue = Effect.fn(
        "LinearGateway.createSessionOnIssue",
      )(function* (input: {
        readonly organizationId: string;
        readonly issueId: string;
      }): Effect.fn.Return<
        string,
        | InstallationRevokedError
        | LinearApiError
        | LinearRateLimitError
        | TokenRefreshError
      > {
        const issueId = yield* Schema.decodeUnknown(IssueId)(
          input.issueId,
        ).pipe(
          Effect.catchTag(
            "ParseError",
            (error) =>
              new LinearApiError({
                operation: "createSessionOnIssue",
                message: `Invalid issue id: ${error.message}`,
              }),
          ),
        );
        yield* Effect.annotateCurrentSpan({
          "linear.organizationId": input.organizationId,
          "linear.issueId": issueId,
        });

        const newSessionId = yield* withOrgQueue(
          input.organizationId,
          Effect.gen(function* () {
            const client = yield* clientFor(input.organizationId);
            return yield* withRateLimitRetry(
              Effect.tryPromise({
                try: async () => {
                  const payload = await client.agentSessionCreateOnIssue({
                    issueId,
                  });
                  if (!payload.success)
                    throw new Error(
                      "Linear failed to create agent session on issue",
                    );
                  const id = payload.agentSessionId;
                  if (!id)
                    throw new Error(
                      "Linear did not return an agent session id",
                    );
                  return id;
                },
                catch: (error) =>
                  mapLinearError("agentSessionCreateOnIssue", error),
              }),
            );
          }),
        );

        yield* Effect.logInfo("linear.createSessionOnIssue").pipe(
          Effect.annotateLogs({
            organizationId: input.organizationId,
            issueId,
            newSessionId,
          }),
        );
        return newSessionId;
      });

      return {
        createActivity,
        createSessionOnIssue,
        listSessionActivities,
        updateSession,
        getIssue,
        updateIssue,
        addSessionExternalUrls,
        createIssueComment,
        teamStartedStates,
        repositorySuggestions,
        refreshInstallation,
      };
    }),
  },
) {}
