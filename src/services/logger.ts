import { fileURLToPath } from "node:url";
import {
  Cause,
  Context,
  Effect,
  Layer,
  Logger,
  LogLevel,
  Option,
  References,
} from "effect";
import * as Tracer from "effect/Tracer";
import pino from "pino";
import type { LogFileConfig } from "./config.js";
import { GatewayConfig } from "./config.js";

const pinoFileTarget = fileURLToPath(import.meta.resolve("pino/file"));
const pinoRollTarget = fileURLToPath(import.meta.resolve("pino-roll"));

const secretKeys = [
  "Authorization",
  "authorization",
  "Cookie",
  "cookie",
  "accessToken",
  "access_token",
  "refreshToken",
  "refresh_token",
  "clientId",
  "client_id",
  "clientSecret",
  "client_secret",
  "clientToken",
  "client_token",
  "webhookSecret",
  "webhook_secret",
  "webhookToken",
  "webhook_token",
  "token",
  "Token",
  "TOKEN",
  "apiKey",
  "api_key",
  "apiToken",
  "api_token",
  "authToken",
  "auth_token",
  "bearerToken",
  "bearer_token",
  "tokenSecret",
  "token_secret",
  "secret",
  "Secret",
  "SECRET",
  "password",
  "Password",
  "PASSWORD",
  "x-api-key",
  "X-Api-Key",
  "x-authorization",
  "X-Authorization",
  "x-token",
  "X-Token",
] as const;

export const redactionPaths = secretKeys.flatMap((key) =>
  ["", "*.", "*.*.", "*.*.*.", "*.*.*.*.", "*.*.*.*.*.", "*.*.*.*.*.*."].map(
    (prefix) => `${prefix}${key}`,
  ),
);

export interface PinoLoggerInput {
  readonly logLevel: string;
  readonly logFile: Option.Option<LogFileConfig>;
}

export interface PinoLoggerPlan {
  readonly options: pino.LoggerOptions;
  readonly transport: pino.TransportMultiOptions<unknown> | undefined;
}

/**
 * Builds the stdout-only or stdout-plus-rotated-file topology. Pino applies
 * `redact` before serializing each NDJSON line to either destination.
 */
export const buildPinoLoggerPlan = ({
  logLevel,
  logFile,
}: PinoLoggerInput): PinoLoggerPlan => ({
  options: {
    name: "gateway",
    level: logLevel,
    redact: { paths: redactionPaths, censor: "[REDACTED]" },
  },
  transport: Option.match(logFile, {
    onNone: () => undefined,
    onSome: (file): pino.TransportMultiOptions<unknown> => ({
      targets: [
        {
          target: pinoFileTarget,
          level: logLevel,
          options: { destination: 1 },
        },
        {
          target: pinoRollTarget,
          level: logLevel,
          options: {
            file: file.path,
            frequency: file.frequency,
            size: file.size,
            limit: { count: file.limit },
            mkdir: true,
          },
        },
      ],
    }),
  }),
});

export const makePinoEffectLogger = (
  logger: pino.Logger,
): Logger.Logger<unknown, void> =>
  Logger.make(({ logLevel, message, cause, fiber }) => {
    const fields: Record<string, unknown> = {
      ...fiber.getRef(References.CurrentLogAnnotations),
    };
    const spanLabels = fiber
      .getRef(References.CurrentLogSpans)
      .map(([label]) => label);
    if (spanLabels.length > 0) fields.spans = spanLabels;
    const parentSpan = Context.getOption(fiber.context, Tracer.ParentSpan);
    if (Option.isSome(parentSpan)) {
      fields.trace_id = parentSpan.value.traceId;
      fields.span_id = parentSpan.value.spanId;
      if (parentSpan.value._tag === "Span")
        fields.span_name = parentSpan.value.name;
    } else fields.uncorrelated = true;
    if (cause !== undefined && cause.reasons.length > 0)
      fields.cause = Cause.pretty(cause);
    const text = Array.isArray(message)
      ? message.map(String).join(" ")
      : String(message);
    if (LogLevel.isGreaterThanOrEqualTo(logLevel, "Fatal"))
      logger.fatal(fields, text);
    else if (LogLevel.isGreaterThanOrEqualTo(logLevel, "Error"))
      logger.error(fields, text);
    else if (LogLevel.isGreaterThanOrEqualTo(logLevel, "Warn"))
      logger.warn(fields, text);
    else if (LogLevel.isGreaterThanOrEqualTo(logLevel, "Info"))
      logger.info(fields, text);
    else if (LogLevel.isGreaterThanOrEqualTo(logLevel, "Debug"))
      logger.debug(fields, text);
    else logger.trace(fields, text);
  });

export interface ManagedPinoTransport {
  readonly ready?: boolean;
  readonly flushSync: () => void;
  readonly end: () => void;
  readonly once: {
    (event: "close" | "ready", listener: () => void): unknown;
    (event: "error", listener: (error: Error) => void): unknown;
  };
  readonly off: {
    (event: "close" | "ready", listener: () => void): unknown;
    (event: "error", listener: (error: Error) => void): unknown;
  };
}

export const closePinoTransport = (
  transport: ManagedPinoTransport,
): Effect.Effect<void> =>
  Effect.callback<void>((resume) => {
    let completed = false;
    const cleanup = () => {
      transport.off("ready", flushAndEnd);
      transport.off("close", onClose);
      transport.off("error", onError);
    };
    const complete = (effect: Effect.Effect<void>) => {
      if (completed) return;
      completed = true;
      cleanup();
      resume(effect);
    };
    const onClose = () => complete(Effect.void);
    const onError = (error: Error) => complete(Effect.die(error));

    const flushAndEnd = () => {
      try {
        transport.flushSync();
        transport.end();
      } catch (error) {
        onError(error instanceof Error ? error : new Error(String(error)));
      }
    };
    transport.once("close", onClose);
    transport.once("error", onError);
    // v4 can finalize before Bun has delivered the worker's ready event.
    // flushSync would block that event loop while waiting for the worker.
    if (transport.ready === false) transport.once("ready", flushAndEnd);
    else flushAndEnd();

    return Effect.sync(cleanup);
  });

export class GatewayLogger extends Context.Service<GatewayLogger>()(
  "GatewayLogger",
  {
    make: Effect.gen(function* () {
      const config = yield* GatewayConfig;
      const plan = buildPinoLoggerPlan(config);
      const resource = yield* Effect.acquireRelease(
        Effect.sync(() => {
          const transport =
            plan.transport === undefined
              ? undefined
              : pino.transport(plan.transport);
          return { logger: pino(plan.options, transport), transport };
        }),
        ({ logger, transport }) =>
          transport === undefined
            ? Effect.sync(() => logger.flush())
            : closePinoTransport(transport),
      );
      return { logger: resource.logger };
    }),
  },
) {
  static readonly layerWithoutDependencies = Layer.effect(this, this.make);
  static readonly layer = this.layerWithoutDependencies.pipe(
    Layer.provide([GatewayConfig.layer]),
  );
}

export const PinoLoggerLive = Layer.unwrap(
  Effect.map(GatewayLogger, ({ logger }) =>
    Logger.layer([makePinoEffectLogger(logger)]),
  ),
);
