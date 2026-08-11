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
  readonly githubAppId: string | undefined;
  readonly githubAppPrivateKey: Redacted.Redacted<string> | undefined;
  readonly publicUrl: URL;
  readonly logLevel: LogLevel;
  readonly logFile: Option.Option<LogFileConfig>;
  readonly nixBinaryPath: string;
  readonly nixpkgsFlakeRef: string;
  readonly nixRootsDir: string;
  readonly nixGcMaxBytes: number;
  readonly databasePath: string;
  readonly workspaceRoot: string;
  readonly ompCliPath: string;
  readonly port: number;
  readonly leaseDurationMs: number;
  readonly reconcilerIntervalMs: number;
  readonly reconcilerCatchupIntervalMs: number;
  readonly reconcilerCatchupMinAgeMs: number;
  readonly webhookReplayWindowMs: number;
  readonly repositorySuggestionConfidenceThreshold: number;
}

const positiveInteger = (name: string, fallback: number) =>
  Config.string(name).pipe(
    Config.withDefault(String(fallback)),
    Config.mapOrFail((value) => {
      const trimmed = value.trim();
      // Digit-only: Number.parseInt would silently truncate "5m", "1.5",
      // and "1e3" into wrong-but-accepted values.
      const parsed = /^\d+$/u.test(trimmed) ? Number(trimmed) : Number.NaN;
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
const decimalBetween = (name: string, fallback: number) =>
  Config.option(Config.string(name)).pipe(
    Config.mapOrFail((value) => {
      const raw = Option.match(value, {
        onNone: () => String(fallback),
        onSome: (configured) => configured.trim(),
      });
      // Blank values must fail: Number("") is 0, which would silently
      // disable the repository-suggestion confidence gate.
      const parsed = raw.length === 0 ? Number.NaN : Number(raw);
      return Number.isFinite(parsed) && parsed >= 0 && parsed <= 1
        ? Either.right(parsed)
        : Either.left(
            ConfigError.InvalidData(
              [name],
              `${name} must be a number between 0 and 1`,
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

const nixpkgsFlakeRef = Config.string("NIXPKGS_FLAKE_REF").pipe(
  Config.withDefault(
    "github:NixOS/nixpkgs/ac62194c3917d5f474c1a844b6fd6da2db95077d",
  ),
  Config.mapOrFail((value) => {
    const normalized = value.trim();
    return /^github:[A-Za-z0-9._-]+\/[A-Za-z0-9._-]+\/[0-9a-fA-F]{40}$/.test(
      normalized,
    )
      ? Either.right(normalized)
      : Either.left(
          ConfigError.InvalidData(
            ["NIXPKGS_FLAKE_REF"],
            "NIXPKGS_FLAKE_REF must be an immutable github owner/repository commit reference",
          ),
        );
  }),
);

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
  reconcilerCatchupIntervalMs: positiveInteger(
    "RECONCILER_CATCHUP_INTERVAL_MS",
    5 * 60_000,
  ),
  reconcilerCatchupMinAgeMs: positiveInteger(
    "RECONCILER_CATCHUP_MIN_AGE_MS",
    2 * 60_000,
  ),
  webhookReplayWindowMs: positiveInteger("WEBHOOK_REPLAY_WINDOW_MS", 60_000),
  repositorySuggestionConfidenceThreshold: decimalBetween(
    "REPOSITORY_SUGGESTION_CONFIDENCE_THRESHOLD",
    0.8,
  ),
  nixBinaryPath: Config.string("NIX_BINARY_PATH").pipe(
    Config.withDefault("nix"),
    Config.map((value) => value.trim() || "nix"),
  ),
  nixpkgsFlakeRef,
  nixRootsDir: Config.string("NIX_ROOTS_DIR").pipe(
    Config.withDefault("/app/nix-roots"),
    Config.map(resolve),
  ),
  nixGcMaxBytes: positiveInteger("NIX_GC_MAX_BYTES", 10 * 1024 * 1024 * 1024),
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

const optionalValue = Effect.fn("GatewayConfig.optionalValue")(function* (
  name: string,
): Effect.fn.Return<string | undefined, ConfigError.ConfigError> {
  const direct = yield* Config.option(Config.string(name));
  if (Option.isSome(direct)) {
    const value = direct.value.trim();
    if (value.length > 0) return value;
  }

  const filePath = yield* Config.option(Config.string(`${name}_FILE`));
  if (Option.isNone(filePath) || filePath.value.trim().length === 0) {
    return undefined;
  }

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
  return trimmed.length > 0 ? trimmed : undefined;
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
      const githubAppId = yield* optionalValue("GITHUB_APP_ID");
      const githubAppPrivateKey = yield* optionalValue(
        "GITHUB_APP_PRIVATE_KEY",
      );
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
        githubAppId,
        githubAppPrivateKey:
          githubAppPrivateKey === undefined
            ? undefined
            : Redacted.make(githubAppPrivateKey),
        linearClientSecret: Redacted.make(linearClientSecret),
        linearWebhookSecret: Redacted.make(linearWebhookSecret),
        tokenEncryptionKey: Redacted.make(tokenEncryptionKey),
        publicUrl,
      };
    }),
  },
) {}
