import { Cause, Context, Effect, FiberRef, FiberRefs, HashMap, Layer, List, Logger, LogLevel, Option } from "effect";
import * as Tracer from "effect/Tracer";
import pino from "pino";
import { GatewayConfig } from "./config.js";

const secretKeys = [
  "Authorization", "authorization", "Cookie", "cookie", "accessToken", "access_token", "refreshToken", "refresh_token",
  "clientId", "client_id", "clientSecret", "client_secret", "clientToken", "client_token", "webhookSecret", "webhook_secret",
  "webhookToken", "webhook_token", "token", "Token", "TOKEN", "apiKey", "api_key", "apiToken", "api_token",
  "authToken", "auth_token", "bearerToken", "bearer_token", "tokenSecret", "token_secret", "secret", "Secret", "SECRET",
  "password", "Password", "PASSWORD", "x-api-key", "X-Api-Key", "x-authorization", "X-Authorization", "x-token", "X-Token",
] as const;

export const redactionPaths = secretKeys.flatMap((key) => ["", "*.", "*.*.", "*.*.*.", "*.*.*.*.", "*.*.*.*.*.", "*.*.*.*.*.*."].map((prefix) => `${prefix}${key}`));

export const makePinoEffectLogger = (logger: pino.Logger): Logger.Logger<unknown, void> =>
  Logger.make(({ logLevel, message, annotations, spans, cause, context }) => {
    const fields: Record<string, unknown> = Object.fromEntries(HashMap.entries(annotations));
    const spanLabels = List.toArray(spans).map((span) => span.label);
    if (spanLabels.length > 0) fields.spans = spanLabels;
    const parentSpan = Context.getOption(FiberRefs.getOrDefault(context, FiberRef.currentContext), Tracer.ParentSpan);
    if (Option.isSome(parentSpan)) {
      fields.trace_id = parentSpan.value.traceId;
      fields.span_id = parentSpan.value.spanId;
      if (parentSpan.value._tag === "Span") fields.span_name = parentSpan.value.name;
    } else fields.uncorrelated = true;
    if (cause !== undefined && !Cause.isEmpty(cause)) fields.cause = Cause.pretty(cause);
    const text = Array.isArray(message) ? message.map(String).join(" ") : String(message);
    if (LogLevel.greaterThanEqual(logLevel, LogLevel.Fatal)) logger.fatal(fields, text);
    else if (LogLevel.greaterThanEqual(logLevel, LogLevel.Error)) logger.error(fields, text);
    else if (LogLevel.greaterThanEqual(logLevel, LogLevel.Warning)) logger.warn(fields, text);
    else if (LogLevel.greaterThanEqual(logLevel, LogLevel.Info)) logger.info(fields, text);
    else if (LogLevel.greaterThanEqual(logLevel, LogLevel.Debug)) logger.debug(fields, text);
    else logger.trace(fields, text);
  });

export class GatewayLogger extends Effect.Service<GatewayLogger>()("GatewayLogger", {
  accessors: true,
  dependencies: [GatewayConfig.Default],
  effect: Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const logger = pino({
      name: "gateway",
      level: config.logLevel,
      redact: { paths: redactionPaths, censor: "[REDACTED]" },
    });
    return { logger };
  }),
}) {}

export const PinoLoggerLive = Layer.unwrapEffect(
  Effect.map(GatewayLogger, ({ logger }) =>
    Logger.replace(Logger.defaultLogger, makePinoEffectLogger(logger)),
  ),
);
