import { isAbsolute, resolve } from "node:path";
import {
  Config,
  ConfigProvider,
  Context,
  Effect,
  Layer,
  Option,
  Redacted,
} from "effect";
import type { LogLevel } from "../domain/models.js";
import { modelProvider } from "./worker-profile.js";
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
  readonly allowedOrganizationIds?: ReadonlySet<string> | undefined;
  readonly gatewayAdminOrganizationIds?: ReadonlySet<string> | undefined;
  readonly githubAppAllowedOwners?:
    | Readonly<Record<string, ReadonlyArray<string>>>
    | undefined;
  readonly mcpOAuthAllowLoopbackHttp?: boolean | undefined;
  readonly tokenEncryptionKeyId?: number | undefined;
  readonly tokenEncryptionPreviousKeys?: Redacted.Redacted<string> | undefined;
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
  readonly rpcRequestTimeoutMs?: number;
  readonly gitTimeoutMs?: number;
  readonly nixTimeoutMs?: number;
  readonly maxConcurrentWorkers?: number;
  readonly reconcilerIntervalMs: number;
  readonly reconcilerCatchupIntervalMs: number;
  readonly reconcilerCatchupMinAgeMs: number;
  readonly webhookReplayWindowMs: number;
  readonly repositorySuggestionConfidenceThreshold: number;
  /** Read-only, credential-free omp profile copied into each worker agent dir. */
  readonly workerProfileDir?: string | undefined;
  readonly workerProfileMaxBytes?: number;
  readonly workerProfileMaxEntries?: number;
  /** CLIProxyAPI origin; the only model endpoint workers are configured for. */
  readonly cliproxyApiBaseUrl?: string | undefined;
  /** Dedicated Symphony client key, passed only into worker environments. */
  readonly cliproxyApiKey?: Redacted.Redacted<string> | undefined;
  readonly workerDefaultModel?: string | undefined;
  /** Gateway-installed omp extensions loaded into every worker, e.g. pi-claude-cli. */
  readonly workerExtensionPaths?: ReadonlyArray<string>;
  readonly workerClaudeExecutable?: string;
}

const positiveInteger = (name: string, fallback: number) =>
  Config.String(name).pipe(
    Config.withDefault(String(fallback)),
    Config.mapEffect((value) => {
      const trimmed = value.trim();
      // Digit-only: Number.parseInt would silently truncate "5m", "1.5",
      // and "1e3" into wrong-but-accepted values.
      const parsed = /^\d+$/u.test(trimmed) ? Number(trimmed) : Number.NaN;
      return Number.isSafeInteger(parsed) && parsed > 0
        ? Effect.succeed(parsed)
        : Effect.fail(
            new Config.ConfigError(
              new ConfigProvider.SourceError({
                message: `${name} must be a positive integer`,
              }),
            ),
          );
    }),
  );
const timeoutMilliseconds = (name: string, fallback: number) =>
  positiveInteger(name, fallback).pipe(
    Config.mapEffect((value) =>
      value <= 2_147_483_647
        ? Effect.succeed(value)
        : Effect.fail(
            new Config.ConfigError(
              new ConfigProvider.SourceError({
                message: `${name} must not exceed 2147483647 ms`,
              }),
            ),
          ),
    ),
  );

const decimalBetween = (name: string, fallback: number) =>
  Config.option(Config.String(name)).pipe(
    Config.mapEffect((value) => {
      const raw = Option.match(value, {
        onNone: () => String(fallback),
        onSome: (configured) => configured.trim(),
      });
      // Blank values must fail: Number("") is 0, which would silently
      // disable the repository-suggestion confidence gate.
      const parsed = raw.length === 0 ? Number.NaN : Number(raw);
      return Number.isFinite(parsed) && parsed >= 0 && parsed <= 1
        ? Effect.succeed(parsed)
        : Effect.fail(
            new Config.ConfigError(
              new ConfigProvider.SourceError({
                message: `${name} must be a number between 0 and 1`,
              }),
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

const logLevel = Config.String("LOG_LEVEL").pipe(
  Config.withDefault("info"),
  Config.map((value): LogLevel => {
    const normalized = value.trim().toLowerCase();
    return LOG_LEVELS.has(normalized) ? (normalized as LogLevel) : "info";
  }),
);

const logFilePath = Config.String("LOG_FILE").pipe(
  Config.withDefault(""),
  Config.map((value) => {
    const trimmed = value.trim();
    return trimmed.length === 0
      ? Option.none<LogFileConfig["path"]>()
      : Option.some(resolve(trimmed));
  }),
);

const logFileFrequency = Config.String("LOG_FILE_FREQUENCY").pipe(
  Config.withDefault("daily"),
  Config.mapEffect((value) => {
    const normalized = value.trim().toLowerCase();
    return isLogFileFrequency(normalized)
      ? Effect.succeed(normalized)
      : Effect.fail(
          new Config.ConfigError(
            new ConfigProvider.SourceError({
              message: "LOG_FILE_FREQUENCY must be daily or hourly",
            }),
          ),
        );
  }),
);

const logFileSize = Config.String("LOG_FILE_SIZE").pipe(
  Config.withDefault("25m"),
  Config.mapEffect((value) => {
    const normalized = value.trim().toLowerCase();
    const match = /^(\d+(?:\.\d+)?)([bkmg]?)$/.exec(normalized);
    return match !== null && Number(match[1]) > 0
      ? Effect.succeed(normalized)
      : Effect.fail(
          new Config.ConfigError(
            new ConfigProvider.SourceError({
              message:
                "LOG_FILE_SIZE must be a positive number with an optional b, k, m, or g suffix",
            }),
          ),
        );
  }),
);

const logFileLimit = positiveInteger("LOG_FILE_LIMIT", 14);

const invalid = (message: string) =>
  new Config.ConfigError(new ConfigProvider.SourceError({ message }));

const absolutePath = (name: string, value: string) =>
  isAbsolute(value) && resolve(value) === value
    ? Effect.succeed(value)
    : Effect.fail(invalid(`${name} must be a normalized absolute path`));

const workerExtensionPaths = Config.String("WORKER_EXTENSION_PATHS").pipe(
  Config.withDefault(""),
  Config.mapEffect((value) =>
    Effect.forEach(
      value
        .split(",")
        .map((path) => path.trim())
        .filter(Boolean),
      (path) => absolutePath("WORKER_EXTENSION_PATHS", path),
    ),
  ),
);

const nixpkgsFlakeRef = Config.String("NIXPKGS_FLAKE_REF").pipe(
  Config.withDefault(
    "github:NixOS/nixpkgs/ac62194c3917d5f474c1a844b6fd6da2db95077d",
  ),
  Config.mapEffect((value) => {
    const normalized = value.trim();
    return /^github:[A-Za-z0-9._-]+\/[A-Za-z0-9._-]+\/[0-9a-fA-F]{40}$/.test(
      normalized,
    )
      ? Effect.succeed(normalized)
      : Effect.fail(
          new Config.ConfigError(
            new ConfigProvider.SourceError({
              message:
                "NIXPKGS_FLAKE_REF must be an immutable github owner/repository commit reference",
            }),
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
  mcpOAuthAllowLoopbackHttp: Config.Boolean(
    "MCP_OAUTH_ALLOW_LOOPBACK_HTTP",
  ).pipe(Config.withDefault(false)),
  tokenEncryptionKeyId: positiveInteger("TOKEN_ENCRYPTION_KEY_ID", 1),
  databasePath: Config.String("DATABASE_PATH").pipe(
    Config.withDefault("./data/gateway.sqlite"),
    Config.map((value) => (value === ":memory:" ? value : resolve(value))),
  ),
  workspaceRoot: Config.String("WORKSPACE_ROOT").pipe(
    Config.withDefault("./data/workspaces"),
    Config.map(resolve),
  ),
  ompCliPath: Config.String("OMP_CLI_PATH").pipe(
    Config.withDefault("omp"),
    Config.map((value) => value.trim() || "omp"),
  ),
  port: positiveInteger("PORT", 3000),
  leaseDurationMs: positiveInteger("LEASE_DURATION_MS", 60_000),
  rpcRequestTimeoutMs: timeoutMilliseconds("RPC_REQUEST_TIMEOUT_MS", 30_000),
  gitTimeoutMs: timeoutMilliseconds("GIT_TIMEOUT_MS", 120_000),
  nixTimeoutMs: timeoutMilliseconds("NIX_TIMEOUT_MS", 600_000),
  maxConcurrentWorkers: positiveInteger("MAX_CONCURRENT_WORKERS", 4),
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
  nixBinaryPath: Config.String("NIX_BINARY_PATH").pipe(
    Config.withDefault("nix"),
    Config.map((value) => value.trim() || "nix"),
  ),
  nixpkgsFlakeRef,
  nixRootsDir: Config.String("NIX_ROOTS_DIR").pipe(
    Config.withDefault("/app/nix-roots"),
    Config.map(resolve),
  ),
  nixGcMaxBytes: positiveInteger("NIX_GC_MAX_BYTES", 10 * 1024 * 1024 * 1024),
  workerProfileMaxBytes: positiveInteger(
    "WORKER_PROFILE_MAX_BYTES",
    64 * 1024 * 1024,
  ),
  workerProfileMaxEntries: positiveInteger(
    "WORKER_PROFILE_MAX_ENTRIES",
    10_000,
  ),
  workerExtensionPaths,
  workerClaudeExecutable: Config.String("WORKER_CLAUDE_EXECUTABLE").pipe(
    Config.withDefault("claude-cliproxy"),
    Config.map((value) => value.trim() || "claude-cliproxy"),
  ),
});

const requiredValue = Effect.fn("GatewayConfig.requiredValue")(function* (
  name: string,
): Effect.fn.Return<string, Config.ConfigError> {
  const direct = yield* Config.option(Config.String(name));
  if (Option.isSome(direct)) {
    const value = direct.value.trim();
    if (value.length > 0) return value;
  }

  const filePath = yield* Config.option(Config.String(`${name}_FILE`));
  if (Option.isSome(filePath) && filePath.value.trim().length > 0) {
    const path = filePath.value.trim();
    const value = yield* Effect.tryPromise({
      try: () => Bun.file(path).text(),
      catch: (error) =>
        new Config.ConfigError(
          new ConfigProvider.SourceError({
            message: `Could not read ${name} from ${path}: ${String(error)}`,
          }),
        ),
    });
    const trimmed = value.trim();
    if (trimmed.length > 0) return trimmed;
  }

  return yield* Effect.fail(
    new Config.ConfigError(
      new ConfigProvider.SourceError({
        message: `Missing required environment variable ${name} or ${name}_FILE`,
      }),
    ),
  );
});

const optionalValue = Effect.fn("GatewayConfig.optionalValue")(function* (
  name: string,
): Effect.fn.Return<string | undefined, Config.ConfigError> {
  const direct = yield* Config.option(Config.String(name));
  if (Option.isSome(direct)) {
    const value = direct.value.trim();
    if (value.length > 0) return value;
  }

  const filePath = yield* Config.option(Config.String(`${name}_FILE`));
  if (Option.isNone(filePath) || filePath.value.trim().length === 0) {
    return undefined;
  }

  const path = filePath.value.trim();
  const value = yield* Effect.tryPromise({
    try: () => Bun.file(path).text(),
    catch: (error) =>
      new Config.ConfigError(
        new ConfigProvider.SourceError({
          message: `Could not read ${name} from ${path}: ${String(error)}`,
        }),
      ),
  });
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
});

/** Codex through CLIProxyAPI's OpenAI-compatible endpoint, declared by the profile. */
export const DEFAULT_CLIPROXYAPI_WORKER_MODEL = "cliproxyapi/gpt-6-sol";

const workerModelRouting = Effect.fn("GatewayConfig.workerModelRouting")(
  function* (): Effect.fn.Return<
    Pick<
      GatewayConfigShape,
      | "workerProfileDir"
      | "cliproxyApiBaseUrl"
      | "cliproxyApiKey"
      | "workerDefaultModel"
    >,
    Config.ConfigError
  > {
    const profile = yield* Config.option(Config.String("WORKER_PROFILE_DIR"));
    const profileDir = Option.isSome(profile)
      ? profile.value.trim() || undefined
      : undefined;
    const workerProfileDir =
      profileDir === undefined
        ? undefined
        : yield* absolutePath("WORKER_PROFILE_DIR", profileDir);
    const rawBase = yield* Config.option(Config.String("CLIPROXYAPI_BASE_URL"));
    const base = Option.isSome(rawBase)
      ? rawBase.value.trim() || undefined
      : undefined;
    const key = yield* optionalValue("CLIPROXYAPI_API_KEY");
    if ((base === undefined) !== (key === undefined))
      return yield* Effect.fail(
        invalid(
          "CLIPROXYAPI_BASE_URL and CLIPROXYAPI_API_KEY (or _FILE) must be set together",
        ),
      );
    let cliproxyApiBaseUrl: string | undefined;
    if (base !== undefined) {
      const url = URL.canParse(base) ? new URL(base) : undefined;
      if (
        url === undefined ||
        (url.protocol !== "https:" && url.protocol !== "http:") ||
        url.username !== "" ||
        url.password !== "" ||
        url.search !== "" ||
        url.hash !== ""
      )
        return yield* Effect.fail(
          invalid(
            "CLIPROXYAPI_BASE_URL must be an http(s) URL without credentials, query, or fragment",
          ),
        );
      cliproxyApiBaseUrl = `${url.origin}${url.pathname.replace(/\/+$/u, "")}`;
    }
    const rawModel = yield* Config.option(
      Config.String("WORKER_DEFAULT_MODEL"),
    );
    const configuredModel = Option.isSome(rawModel)
      ? rawModel.value.trim() || undefined
      : undefined;
    const workerDefaultModel =
      configuredModel ??
      (cliproxyApiBaseUrl === undefined
        ? undefined
        : DEFAULT_CLIPROXYAPI_WORKER_MODEL);
    if (
      workerDefaultModel !== undefined &&
      modelProvider(workerDefaultModel) === undefined
    )
      return yield* Effect.fail(
        invalid(
          "WORKER_DEFAULT_MODEL must be provider/model with an optional :thinking suffix",
        ),
      );
    return {
      workerProfileDir,
      cliproxyApiBaseUrl,
      cliproxyApiKey: key === undefined ? undefined : Redacted.make(key),
      workerDefaultModel,
    };
  },
);

export class GatewayConfig extends Context.Service<GatewayConfig>()(
  "GatewayConfig",
  {
    make: Effect.gen(function* (): Effect.fn.Return<
      GatewayConfigShape,
      Config.ConfigError
    > {
      const values = yield* Config.unwrap(GatewayConfigValues);
      const allowedIds = yield* optionalValue(
        "LINEAR_ALLOWED_ORGANIZATION_IDS",
      );
      const production = yield* Config.String("NODE_ENV").pipe(
        Config.withDefault("development"),
      );
      if (production === "production" && allowedIds === undefined) {
        return yield* Effect.fail(
          new Config.ConfigError(
            new ConfigProvider.SourceError({
              message: "Production requires an explicit organization allowlist",
            }),
          ),
        );
      }
      const parseIds = (raw: string | undefined) =>
        raw === undefined
          ? undefined
          : new Set(
              raw
                .split(",")
                .map((id) => id.trim())
                .filter(Boolean),
            );
      const allowedOrganizationIds = parseIds(allowedIds);
      const gatewayAdminOrganizationIds = parseIds(
        yield* optionalValue("GATEWAY_ADMIN_ORGANIZATION_IDS"),
      );
      const ownersJson = yield* optionalValue("GITHUB_APP_ALLOWED_OWNERS_JSON");
      const githubAppAllowedOwners = yield* Effect.try({
        try: () => {
          if (ownersJson === undefined) return undefined;
          const value: unknown = JSON.parse(ownersJson);
          if (
            typeof value !== "object" ||
            value === null ||
            Array.isArray(value)
          )
            throw new Error("Expected org to GitHub owner array map");
          const result: Record<string, ReadonlyArray<string>> = {};
          for (const [org, owners] of Object.entries(value)) {
            if (
              !Array.isArray(owners) ||
              !owners.every(
                (owner): owner is string =>
                  typeof owner === "string" &&
                  /^[A-Za-z0-9][A-Za-z0-9-]*$/u.test(owner),
              )
            )
              throw new Error("Invalid GitHub owner binding");
            result[org] = owners.map((owner) => owner.toLowerCase());
          }
          return result;
        },
        catch: () =>
          new Config.ConfigError(
            new ConfigProvider.SourceError({
              message: "Invalid organization to GitHub owners map",
            }),
          ),
      });
      if (values.tokenEncryptionKeyId > 255)
        return yield* Effect.fail(
          new Config.ConfigError(
            new ConfigProvider.SourceError({
              message: "Key id must fit one byte",
            }),
          ),
        );
      const previousKeys = yield* optionalValue(
        "TOKEN_ENCRYPTION_PREVIOUS_KEYS_JSON",
      );
      const linearClientId = yield* requiredValue("LINEAR_CLIENT_ID");
      const linearClientSecret = yield* requiredValue("LINEAR_CLIENT_SECRET");
      const linearWebhookSecret = yield* requiredValue("LINEAR_WEBHOOK_SECRET");
      const tokenEncryptionKey = yield* requiredValue("TOKEN_ENCRYPTION_KEY");
      const githubAppId = yield* optionalValue("GITHUB_APP_ID");
      const githubAppPrivateKey = yield* optionalValue(
        "GITHUB_APP_PRIVATE_KEY",
      );
      if ((githubAppId === undefined) !== (githubAppPrivateKey === undefined)) {
        const missing =
          githubAppId === undefined
            ? "GITHUB_APP_ID"
            : "GITHUB_APP_PRIVATE_KEY";
        yield* Effect.logWarning(
          `GitHub App configuration is partial; missing ${missing}`,
        ).pipe(
          Effect.annotateLogs({
            event: "config.github_app.partial",
            missing,
          }),
        );
      }
      const publicUrlValue = yield* requiredValue("PUBLIC_URL");
      const publicUrl = yield* Effect.try({
        try: () => new URL(publicUrlValue),
        catch: () =>
          new Config.ConfigError(
            new ConfigProvider.SourceError({
              message: "PUBLIC_URL must be a valid URL",
            }),
          ),
      });
      if (
        publicUrl.protocol !== "https:" &&
        publicUrl.hostname !== "localhost"
      ) {
        return yield* Effect.fail(
          new Config.ConfigError(
            new ConfigProvider.SourceError({
              message: "PUBLIC_URL must use HTTPS except on localhost",
            }),
          ),
        );
      }
      const routing = yield* workerModelRouting();
      return {
        ...values,
        ...routing,
        allowedOrganizationIds,
        gatewayAdminOrganizationIds,
        githubAppAllowedOwners,
        tokenEncryptionPreviousKeys:
          previousKeys === undefined ? undefined : Redacted.make(previousKeys),
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
    }).pipe(Effect.map((value): GatewayConfigShape => value)),
  },
) {
  static readonly layerWithoutDependencies = Layer.effect(this, this.make);
  static readonly layer = this.layerWithoutDependencies;
}

export const organizationAllowed = (
  config: GatewayConfigShape,
  organizationId: string,
): boolean =>
  config.allowedOrganizationIds === undefined ||
  config.allowedOrganizationIds.has(organizationId);
