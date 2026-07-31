import { Config, ConfigError, Effect, Either, Redacted } from "effect";
import type { LogLevel } from "../domain/models.js";

export interface GatewayConfigShape {
  readonly linearClientId: string;
  readonly linearClientSecret: Redacted.Redacted<string>;
  readonly linearWebhookSecret: Redacted.Redacted<string>;
  readonly tokenEncryptionKey: Redacted.Redacted<string>;
  readonly publicUrl: URL;
  readonly logLevel: LogLevel;
  readonly databasePath: string;
  readonly workspaceRoot: string;
  readonly ompCliPath: string;
  readonly port: number;
  readonly leaseDurationMs: number;
  readonly reconcilerIntervalMs: number;
  readonly webhookReplayWindowMs: number;
}

const positiveInteger = (name: string, fallback: number) =>
  Config.integer(name).pipe(
    Config.withDefault(fallback),
    Config.validate({ message: `${name} must be a positive integer`, validation: (value) => Number.isSafeInteger(value) && value > 0 }),
  );

const urlConfig = Config.string("PUBLIC_URL").pipe(
  Config.mapOrFail((value) => {
    try {
      const url = new URL(value);
      return url.protocol === "https:" || url.hostname === "localhost"
        ? Either.right(url)
        : Either.left(ConfigError.InvalidData(["PUBLIC_URL"], "PUBLIC_URL must use HTTPS except on localhost"));
    } catch {
      return Either.left(ConfigError.InvalidData(["PUBLIC_URL"], "PUBLIC_URL must be a valid URL"));
    }
  }),
);

const logLevel = Config.string("LOG_LEVEL").pipe(
  Config.withDefault("info"),
  Config.validate({
    message: "LOG_LEVEL must be a supported Pino level",
    validation: (value): value is LogLevel => ["trace", "debug", "info", "warn", "error", "fatal", "silent"].includes(value),
  }),
);

const GatewayConfigValues = Config.all({
  linearClientId: Config.nonEmptyString("LINEAR_CLIENT_ID"),
  linearClientSecret: Config.redacted("LINEAR_CLIENT_SECRET"),
  linearWebhookSecret: Config.redacted("LINEAR_WEBHOOK_SECRET"),
  tokenEncryptionKey: Config.redacted("TOKEN_ENCRYPTION_KEY"),
  publicUrl: urlConfig,
  logLevel,
  databasePath: Config.string("DATABASE_PATH").pipe(Config.withDefault("./data/gateway.sqlite")),
  workspaceRoot: Config.string("WORKSPACE_ROOT").pipe(Config.withDefault("./data/workspaces")),
  ompCliPath: Config.string("OMP_CLI_PATH").pipe(Config.withDefault("omp")),
  port: positiveInteger("PORT", 3000),
  leaseDurationMs: positiveInteger("LEASE_DURATION_MS", 60_000),
  reconcilerIntervalMs: positiveInteger("RECONCILER_INTERVAL_MS", 1_000),
  webhookReplayWindowMs: positiveInteger("WEBHOOK_REPLAY_WINDOW_MS", 60_000),
});

export class GatewayConfig extends Effect.Service<GatewayConfig>()("GatewayConfig", {
  accessors: true,
  effect: Effect.gen(function* () {
    return yield* Config.unwrap(GatewayConfigValues);
  }),
}) {}
