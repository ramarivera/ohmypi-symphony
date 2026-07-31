import { resolve } from "node:path";
import { Config, ConfigError, Effect, Either, Option, Redacted } from "effect";
import type { LogLevel } from "../domain/models.js";
export const LOG_FILE_FREQUENCIES = ["daily", "hourly"] as const;
export type LogFileFrequency = (typeof LOG_FILE_FREQUENCIES)[number];

const isLogFileFrequency = (value: string): value is LogFileFrequency =>
  LOG_FILE_FREQUENCIES.some((frequency) => frequency === value);

export interface LogFileConfig {
  readonly path: string;
  readonly frequency: LogFileFrequency;
  readonly size: string;
  readonly limit: number;
}
export interface GatewayConfigShape {
  readonly linearClientId: string;
  readonly linearClientSecret: Redacted.Redacted<string>;
  readonly linearWebhookSecret: Redacted.Redacted<string>;
  readonly tokenEncryptionKey: Redacted.Redacted<string>;
  readonly publicUrl: URL;
  readonly logLevel: LogLevel;
  readonly logFile: Option.Option<LogFileConfig>;
  readonly databasePath: string;
  readonly workspaceRoot: string;
  readonly ompCliPath: string;
  readonly port: number;
  readonly leaseDurationMs: number;
  readonly reconcilerIntervalMs: number;
  readonly webhookReplayWindowMs: number;
}

const positiveInteger = (name: string, fallback: number) =>
  Config.string(name).pipe(
    Config.withDefault(String(fallback)),
    Config.mapOrFail((value) => {
      const parsed = Number.parseInt(value, 10);
      return Number.isSafeInteger(parsed) && parsed > 0
        ? Either.right(parsed)
        : Either.left(
            ConfigError.InvalidData(
              [name],
              `${name} must be a positive integer`,
            ),
          );
    }),
  );

const LOG_LEVELS: ReadonlySet<string> = new Set([
  "trace",
  "debug",
  "info",
  "warn",
  "error",
  "fatal",
  "silent",
]);

const logLevel = Config.string("LOG_LEVEL").pipe(
  Config.withDefault("info"),
  Config.map((value): LogLevel => {
    const normalized = value.trim().toLowerCase();
    return LOG_LEVELS.has(normalized) ? (normalized as LogLevel) : "info";
  }),
);

const logFilePath = Config.string("LOG_FILE").pipe(
  Config.withDefault(""),
  Config.map((value) => {
    const trimmed = value.trim();
    return trimmed.length === 0
      ? Option.none<LogFileConfig["path"]>()
      : Option.some(resolve(trimmed));
  }),
);

const logFileFrequency = Config.string("LOG_FILE_FREQUENCY").pipe(
  Config.withDefault("daily"),
  Config.mapOrFail((value) => {
    const normalized = value.trim().toLowerCase();
    return isLogFileFrequency(normalized)
      ? Either.right(normalized)
      : Either.left(
          ConfigError.InvalidData(
            ["LOG_FILE_FREQUENCY"],
            "LOG_FILE_FREQUENCY must be daily or hourly",
          ),
        );
  }),
);

const logFileSize = Config.string("LOG_FILE_SIZE").pipe(
  Config.withDefault("25m"),
  Config.mapOrFail((value) => {
    const normalized = value.trim().toLowerCase();
    const match = /^(\d+(?:\.\d+)?)([bkmg]?)$/.exec(normalized);
    return match !== null && Number(match[1]) > 0
      ? Either.right(normalized)
      : Either.left(
          ConfigError.InvalidData(
            ["LOG_FILE_SIZE"],
            "LOG_FILE_SIZE must be a positive number with an optional b, k, m, or g suffix",
          ),
        );
  }),
);

const logFileLimit = positiveInteger("LOG_FILE_LIMIT", 14);

const logFile = Config.all({
  path: logFilePath,
  frequency: logFileFrequency,
  size: logFileSize,
  limit: logFileLimit,
}).pipe(
  Config.map(({ path, frequency, size, limit }) =>
    Option.map(path, (filePath) => ({
      path: filePath,
      frequency,
      size,
      limit,
    })),
  ),
);

const GatewayConfigValues = Config.all({
  logLevel,
  logFile,
  databasePath: Config.string("DATABASE_PATH").pipe(
    Config.withDefault("./data/gateway.sqlite"),
    Config.map((value) => (value === ":memory:" ? value : resolve(value))),
  ),
  workspaceRoot: Config.string("WORKSPACE_ROOT").pipe(
    Config.withDefault("./data/workspaces"),
    Config.map(resolve),
  ),
  ompCliPath: Config.string("OMP_CLI_PATH").pipe(
    Config.withDefault("omp"),
    Config.map((value) => value.trim() || "omp"),
  ),
  port: positiveInteger("PORT", 3000),
  leaseDurationMs: positiveInteger("LEASE_DURATION_MS", 60_000),
  reconcilerIntervalMs: positiveInteger("RECONCILER_INTERVAL_MS", 1_000),
  webhookReplayWindowMs: positiveInteger("WEBHOOK_REPLAY_WINDOW_MS", 60_000),
});

const requiredValue = Effect.fn("GatewayConfig.requiredValue")(function* (
  name: string,
): Effect.fn.Return<string, ConfigError.ConfigError> {
  const direct = yield* Config.option(Config.string(name));
  if (Option.isSome(direct)) {
    const value = direct.value.trim();
    if (value.length > 0) return value;
  }

  const filePath = yield* Config.option(Config.string(`${name}_FILE`));
  if (Option.isSome(filePath) && filePath.value.trim().length > 0) {
    const path = filePath.value.trim();
    const value = yield* Effect.tryPromise({
      try: () => Bun.file(path).text(),
      catch: (error) =>
        ConfigError.InvalidData(
          [`${name}_FILE`],
          `Could not read ${name} from ${path}: ${String(error)}`,
        ),
    });
    const trimmed = value.trim();
    if (trimmed.length > 0) return trimmed;
  }

  return yield* Effect.fail(
    ConfigError.MissingData(
      [name],
      `Missing required environment variable ${name} or ${name}_FILE`,
    ),
  );
});

export class GatewayConfig extends Effect.Service<GatewayConfig>()(
  "GatewayConfig",
  {
    accessors: true,
    effect: Effect.gen(function* () {
      const values = yield* Config.unwrap(GatewayConfigValues);
      const linearClientId = yield* requiredValue("LINEAR_CLIENT_ID");
      const linearClientSecret = yield* requiredValue("LINEAR_CLIENT_SECRET");
      const linearWebhookSecret = yield* requiredValue("LINEAR_WEBHOOK_SECRET");
      const tokenEncryptionKey = yield* requiredValue("TOKEN_ENCRYPTION_KEY");
      const publicUrlValue = yield* requiredValue("PUBLIC_URL");
      const publicUrl = yield* Effect.try({
        try: () => new URL(publicUrlValue),
        catch: () =>
          ConfigError.InvalidData(
            ["PUBLIC_URL"],
            "PUBLIC_URL must be a valid URL",
          ),
      });
      if (
        publicUrl.protocol !== "https:" &&
        publicUrl.hostname !== "localhost"
      ) {
        return yield* Effect.fail(
          ConfigError.InvalidData(
            ["PUBLIC_URL"],
            "PUBLIC_URL must use HTTPS except on localhost",
          ),
        );
      }
      return {
        ...values,
        linearClientId,
        linearClientSecret: Redacted.make(linearClientSecret),
        linearWebhookSecret: Redacted.make(linearWebhookSecret),
        tokenEncryptionKey: Redacted.make(tokenEncryptionKey),
        publicUrl,
      };
    }),
  },
) {}
