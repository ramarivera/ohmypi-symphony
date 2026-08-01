import { resolve } from "node:path";
import { Config, ConfigError, Effect, Either, Option, Redacted } from "effect";
import type { ConfigError as ConfigErrorType } from "effect/ConfigError";
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
  readonly githubApp: GitHubAppConfigShape | undefined;
  readonly allowedOrganizationIds: Set<string>;
  readonly webhookReplayWindowMs: number;
}
export interface GitHubAppConfigShape {
  readonly appId: string;
  readonly privateKey: Redacted.Redacted<string>;
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

const parseAllowedOrganizationIds = (
  value: string,
): Either.Either<Set<string>, ConfigErrorType> => {
  const ids = value.split(",").map((id) => id.trim());
  return ids.some(
    (id) => id.length === 0 || !/^[A-Za-z0-9][A-Za-z0-9._-]*$/u.test(id),
  )
    ? Either.left(
        ConfigError.InvalidData(
          ["LINEAR_ALLOWED_ORGANIZATION_IDS"],
          "LINEAR_ALLOWED_ORGANIZATION_IDS must contain non-empty organization IDs",
        ),
      )
    : Either.right(new Set(ids));
};

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

const GatewayConfigValues = Config.all({
  logLevel,
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
const optionalValue = Effect.fn("GatewayConfig.optionalValue")(function* (
  name: string,
) {
  const direct = yield* Config.option(Config.string(name));
  if (Option.isSome(direct)) {
    const value = direct.value.trim();
    if (value.length > 0) return Option.some(value);
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
    if (trimmed.length > 0) return Option.some(trimmed);
  }

  return Option.none<string>();
});

const requiredValue = Effect.fn("GatewayConfig.requiredValue")(function* (
  name: string,
) {
  const value = yield* optionalValue(name);
  if (Option.isSome(value)) return value.value;

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
      const githubAppValues = {
        appId: yield* optionalValue("GITHUB_APP_ID"),
        privateKey: yield* optionalValue("GITHUB_APP_PRIVATE_KEY"),
      };
      const githubAppConfigured = Object.values(githubAppValues).some((value) =>
        Option.isSome(value),
      );
      const githubApp = githubAppConfigured
        ? {
            appId: Option.getOrElse(githubAppValues.appId, () => ""),
            privateKey: Redacted.make(
              Option.getOrElse(githubAppValues.privateKey, () => ""),
            ),
          }
        : undefined;
      const linearClientId = yield* requiredValue("LINEAR_CLIENT_ID");
      const parsedAllowedOrganizationIds = parseAllowedOrganizationIds(
        yield* requiredValue("LINEAR_ALLOWED_ORGANIZATION_IDS"),
      );
      if (Either.isLeft(parsedAllowedOrganizationIds)) {
        return yield* Effect.fail(parsedAllowedOrganizationIds.left);
      }
      const allowedOrganizationIds = parsedAllowedOrganizationIds.right;
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
        githubApp,
        allowedOrganizationIds,
      };
    }),
  },
) {}
