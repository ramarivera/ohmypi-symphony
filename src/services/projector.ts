import { createHash, randomUUID } from "node:crypto";
import { Clock, Effect, Option, ParseResult, Ref, Schema } from "effect";
import {
  type InstallationRevokedError,
  LinearApiError,
  type LinearRateLimitError,
  RowDecodeError,
  type RunLeaseError,
  type TokenRefreshError,
} from "../domain/errors.js";
import { ActivityId, SessionId, SourceKey } from "../domain/ids.js";
import type { ActivityType, ProjectionJob } from "../domain/models.js";
import { LinearGateway } from "./linear-gateway.js";
import {
  isActivitySignal,
  isActivityType,
  isRecord,
  isString,
} from "./linear-helpers.js";
import { ProjectionRepo, RunRepo } from "./store/repositories.js";

export const projectionBackoff = (
  attempt: number,
  baseMs = 1_000,
  maxMs = 5 * 60_000,
  maxExponent = 8,
): number => Math.min(maxMs, baseMs * 2 ** Math.min(attempt - 1, maxExponent));

export const rpcEventActivityType = (event: {
  readonly type: string;
}): ActivityType | "none" => {
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

export class ActivityProjector extends Effect.Service<ActivityProjector>()(
  "ActivityProjector",
  {
    accessors: true,
    dependencies: [
      ProjectionRepo.Default,
      RunRepo.Default,
      LinearGateway.Default,
    ],
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
        value.length <= maxLength
          ? value
          : `${value.slice(0, maxLength)}\n[truncated]`;

      const sha256 = (value: string): Effect.Effect<string, never, never> =>
        Effect.sync(() => createHash("sha256").update(value).digest("hex"));

      const stringify = (value: unknown): Effect.Effect<string, never, never> =>
        Effect.sync(() => JSON.stringify(value));

      const decodeSourceKey = (value: string) =>
        Schema.decodeUnknown(SourceKey)(value).pipe(
          Effect.mapError(
            (error) =>
              new RowDecodeError({
                message: ParseResult.TreeFormatter.formatErrorSync(error),
                entity: "SourceKey",
                cause: String(error),
              }),
          ),
        );

      const decodeSessionId = (value: string) =>
        Schema.decodeUnknown(SessionId)(value).pipe(
          Effect.mapError(
            (error) =>
              new RowDecodeError({
                message: ParseResult.TreeFormatter.formatErrorSync(error),
                entity: "SessionId",
                cause: String(error),
              }),
          ),
        );

      const decodeActivityId = (value: string) =>
        Schema.decodeUnknown(ActivityId)(value).pipe(
          Effect.mapError(
            (error) =>
              new RowDecodeError({
                message: ParseResult.TreeFormatter.formatErrorSync(error),
                entity: "ActivityId",
                cause: String(error),
              }),
          ),
        );

      const assistantText = (
        event: Record<string, unknown>,
      ): string | undefined => {
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
      ): {
        readonly sessionId: string;
        readonly content: Record<string, unknown>;
      } => {
        if (!isRecord(job.payload)) {
          throw new Error(
            `Projection ${job.sourceKey} payload is not an object`,
          );
        }
        const request = job.payload.request;
        if (!isRecord(request) || !isRecord(request.content)) {
          throw new Error(
            `Projection ${job.sourceKey} activity request is invalid`,
          );
        }
        const type = request.content.type;
        if (!isActivityType(type)) {
          throw new Error(
            `Projection ${job.sourceKey} activity type is invalid`,
          );
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
        const signal = isActivitySignal(request.signal)
          ? request.signal
          : undefined;
        if (signal !== undefined) content.signal = signal;
        const signalMetadata = isRecord(request.signalMetadata)
          ? request.signalMetadata
          : undefined;
        if (signalMetadata !== undefined)
          content.signalMetadata = signalMetadata;
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
        readonly externalUrls?: ReadonlyArray<{
          readonly label: string;
          readonly url: string;
        }>;
      } => {
        if (!isRecord(job.payload) || !isRecord(job.payload.request)) {
          throw new Error(
            `Projection ${job.sourceKey} session update is invalid`,
          );
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
                throw new Error(
                  `Projection ${job.sourceKey} external URL is invalid`,
                );
              }
              const label = text(entry.label);
              const url = text(entry.url);
              if (label === undefined || url === undefined) {
                throw new Error(
                  `Projection ${job.sourceKey} external URL is invalid`,
                );
              }
              return { label, url };
            })
          : undefined;
        if (plan === undefined && externalUrls === undefined) {
          throw new Error(
            `Projection ${job.sourceKey} session update is empty`,
          );
        }
        return {
          sessionId: job.sessionId,
          ...(plan === undefined ? {} : { plan }),
          ...(externalUrls === undefined ? {} : { externalUrls }),
        };
      };

      const dispatch = Effect.fn("ActivityProjector.dispatch")(function* (
        sourceKey: SourceKey,
        now?: number,
      ) {
        const at = now ?? (yield* Clock.currentTimeMillis);
        const jobOption = yield* projectionRepo.claim(
          sourceKey,
          owner,
          PROJECTION_LEASE_MS,
          at,
        );
        if (Option.isNone(jobOption)) return false;
        const job = jobOption.value;

        const failWithBackoff = (message: string) =>
          Effect.gen(function* () {
            const delay = projectionBackoff(
              job.attempt,
              1_000,
              MAX_PROJECTION_BACKOFF_MS,
            );
            const nextAttemptAt = at + delay;
            yield* projectionRepo.fail(
              sourceKey,
              owner,
              message,
              nextAttemptAt,
            );
            yield* Effect.logWarning("projector.dispatch.failed", {
              sourceKey,
              error: message,
              nextAttemptAt,
            });
          });

        return yield* Effect.gen(function* () {
          if (
            job.activityType === "plan" ||
            job.activityType === "externalUrls"
          ) {
            const update = yield* Effect.try({
              try: () => decodeSessionUpdate(job),
              catch: (error) =>
                new LinearApiError({
                  operation: "decodeSessionUpdate",
                  message:
                    error instanceof Error ? error.message : String(error),
                }),
            });
            if (update.plan !== undefined && update.plan.length === 0) {
              yield* projectionRepo.complete(sourceKey, owner, Option.none());
              return true;
            }
            const sessionId = yield* Schema.decodeUnknown(SessionId)(
              update.sessionId,
            ).pipe(
              Effect.mapError(
                (error) =>
                  new LinearApiError({
                    operation: "updateSession",
                    message: ParseResult.TreeFormatter.formatErrorSync(error),
                  }),
              ),
            );
            yield* linear.updateSession({ ...update, sessionId });
            yield* projectionRepo.complete(sourceKey, owner, Option.none());
          } else {
            const request = yield* Effect.try({
              try: () => decodeActivityRequest(job),
              catch: (error) =>
                new LinearApiError({
                  operation: "decodeActivityRequest",
                  message:
                    error instanceof Error ? error.message : String(error),
                }),
            });
            const sessionId = yield* Schema.decodeUnknown(SessionId)(
              request.sessionId,
            ).pipe(
              Effect.mapError(
                (error) =>
                  new LinearApiError({
                    operation: "createActivity",
                    message: ParseResult.TreeFormatter.formatErrorSync(error),
                  }),
              ),
            );
            const activityId = yield* linear.createActivity({
              sessionId,
              content: request.content,
            });
            const activityIdDecoded = yield* decodeActivityId(activityId);
            yield* projectionRepo.complete(
              sourceKey,
              owner,
              Option.some(activityIdDecoded),
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
            "@Gateway/InstallationRevokedError": (
              error: InstallationRevokedError,
            ) =>
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
      });

      const enqueueAndDispatch = Effect.fn(
        "ActivityProjector.enqueueAndDispatch",
      )(function* (
        sessionId: string,
        sourceKey: string,
        activityType: string,
        payload: unknown,
        firstWriteWins = false,
      ) {
        const serialized = yield* stringify(payload);
        const payloadHash = yield* sha256(serialized);
        const decodedSourceKey = yield* decodeSourceKey(sourceKey);
        const decodedSessionId = yield* decodeSessionId(sessionId);
        const enqueued = yield* projectionRepo.enqueue({
          sourceKey: decodedSourceKey,
          sessionId: decodedSessionId,
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
        return yield* dispatch(decodedSourceKey, undefined);
      });

      const activity = Effect.fn("ActivityProjector.activity")(function* (
        sessionId: string,
        sourceKey: string,
        content: LinearActivityContent,
        ephemeral: boolean,
        signal?: "auth" | "continue" | "select" | "stop",
        signalMetadata?: Record<string, unknown>,
        firstWriteWins = false,
      ) {
        const request: Record<string, unknown> = {
          sessionId,
          content,
          ephemeral,
        };
        if (signal !== undefined) request.signal = signal;
        if (signalMetadata !== undefined)
          request.signalMetadata = signalMetadata;
        return yield* enqueueAndDispatch(
          sessionId,
          sourceKey,
          content.type,
          { request },
          firstWriteWins,
        );
      });

      const sessionUpdate = Effect.fn("ActivityProjector.sessionUpdate")(
        function* (
          sessionId: string,
          sourceKey: string,
          activityType: "plan" | "externalUrls",
          request:
            | {
                readonly sessionId: string;
                readonly plan?: ReadonlyArray<LinearPlanItem>;
              }
            | {
                readonly sessionId: string;
                readonly externalUrls?: ReadonlyArray<{
                  readonly label: string;
                  readonly url: string;
                }>;
              },
        ) {
          return yield* enqueueAndDispatch(sessionId, sourceKey, activityType, {
            request,
          });
        },
      );

      const thought = Effect.fn("ActivityProjector.thought")(function* (
        sessionId: string,
        sourceKey: string,
        body: string,
        ephemeral = true,
      ) {
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
      });

      const elicitation = Effect.fn("ActivityProjector.elicitation")(function* (
        sessionId: string,
        sourceKey: string,
        body: string,
        options?: ReadonlyArray<string>,
      ) {
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
      });

      const terminal = Effect.fn("ActivityProjector.terminal")(function* (
        sessionId: string,
        sourceKey: string,
        type: "response" | "error",
        body: string,
      ) {
        return yield* activity(
          sessionId,
          `terminal:${sessionId}:${sourceKey}`,
          { type, body: boundedText(body) },
          false,
          undefined,
          undefined,
          true,
        );
      });

      const plan = Effect.fn("ActivityProjector.plan")(function* (
        sessionId: string,
        sourceKey: string,
        items: ReadonlyArray<LinearPlanItem>,
      ) {
        if (items.length === 0) return false;
        const normalized = items.map((item) => ({
          content: item.content,
          status: item.status,
        }));
        return yield* sessionUpdate(sessionId, sourceKey, "plan", {
          sessionId,
          plan: normalized,
        });
      });

      const externalUrls = Effect.fn("ActivityProjector.externalUrls")(
        function* (
          sessionId: string,
          sourceKey: string,
          urls: ReadonlyArray<{ readonly label: string; readonly url: string }>,
        ) {
          const normalized = yield* Effect.forEach(urls, (entry) =>
            Effect.try({
              try: () => {
                const parsed = new URL(entry.url);
                if (
                  parsed.protocol !== "http:" &&
                  parsed.protocol !== "https:"
                ) {
                  throw new Error(
                    `Unsupported external URL protocol ${parsed.protocol}`,
                  );
                }
                return { label: entry.label, url: parsed.toString() };
              },
              catch: (error) =>
                new LinearApiError({
                  operation: "externalUrls",
                  message:
                    error instanceof Error ? error.message : String(error),
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
        function* (limit = 50, now?: number) {
          const at = now ?? (yield* Clock.currentTimeMillis);
          const keys = yield* projectionRepo.due(at, limit);
          let completed = 0;
          for (const sourceKey of keys) {
            const decoded = yield* decodeSourceKey(sourceKey);
            const ok = yield* dispatch(decoded, at);
            if (ok) completed += 1;
          }
          return completed;
        },
      );

      const projectRpcEvent = Effect.fn("ActivityProjector.projectRpcEvent")(
        function* (sessionId: string, sequence: number, event: unknown) {
          if (!isRecord(event)) return;
          yield* Effect.annotateCurrentSpan({
            "projector.sessionId": sessionId,
            "projector.sequence": sequence,
          });
          const sourceKey = `rpc:${sessionId}:${sequence}:${event.type}`;
          switch (event.type) {
            case "agent_start":
              yield* thought(
                sessionId,
                sourceKey,
                "OhMyPi worker started",
                true,
              );
              return;
            case "turn_start":
              yield* thought(
                sessionId,
                sourceKey,
                "Starting the next agent turn",
                true,
              );
              return;
            case "tool_execution_start": {
              const toolName =
                text(event.toolName) ?? text(event.tool) ?? "tool";
              const parameter = isString(event.args)
                ? event.args
                : yield* stringify(event.args ?? {});
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
              const toolName =
                text(event.toolName) ?? text(event.tool) ?? "tool";
              const result = isString(event.result)
                ? event.result
                : yield* stringify(event.result ?? {});
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
              const body = Option.match(
                Option.fromNullable(draft.get(sessionId)),
                {
                  onNone: () => "OhMyPi run completed.",
                  onSome: (value) => value,
                },
              );
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
  },
) {}
