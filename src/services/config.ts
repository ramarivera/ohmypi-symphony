import { isAbsolute, resolve } from "node:path";
import {
  Config,
  ConfigProvider,
  Context,
  Effect,
  Layer,
  Option,
  Redacted,
  Result,
} from "effect";
import type { LogLevel } from "../domain/models.js";
import { RUNTIME_MODES, type RuntimeMode } from "../drivers/driver.js";
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
export type T3TokenSource =
  | { readonly _tag: "Inline"; readonly token: Redacted.Redacted<string> }
  /** Re-read on every connect so a rotated token file is picked up. */
  | { readonly _tag: "File"; readonly path: string };

/** Settings for the gateway-owned per-session `t3 serve` launcher. */
export interface T3LaunchConfig {
  readonly _tag: "Launch";
  readonly cliPath: string;
  /** First loopback port; a session's port is portBase + its slot. */
  readonly portBase: number;
  readonly portCount: number;
  readonly launchTimeoutMs: number;
  readonly codexExecutable: string;
  readonly claudeExecutable: string;
  readonly codexModel: string;
  readonly codexReasoningEffort: string;
  readonly textGenerationModel: string;
}

/**
 * Remote: one externally managed daemon shared by every T3 session. Launch:
 * the gateway starts a private daemon per session under its worker uid.
 */
export type T3Connection =
  | {
      readonly _tag: "Remote";
      readonly baseUrl: URL;
      readonly token: T3TokenSource;
    }
  | T3LaunchConfig;

export interface T3DriverConfig {
  readonly connection: T3Connection;
  readonly projectId?: string;
  readonly providerInstanceId: string;
  readonly model: string;
  readonly modelOptions: ReadonlyArray<{
    readonly id: string;
    readonly value: string | boolean;
  }>;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: "default" | "plan";
  /** Prefix for every id the driver derives (threads, commands, messages). */
  readonly idNamespace: string;
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
  /** Dedicated Partitura client key, passed only into worker environments. */
  readonly cliproxyApiKey?: Redacted.Redacted<string> | undefined;
  readonly workerDefaultModel?: string | undefined;
  /** Gateway-installed omp extensions loaded into every worker, e.g. pi-claude-cli. */
  readonly workerExtensionPaths?: ReadonlyArray<string>;
  readonly workerClaudeExecutable?: string;
  /** Present only when T3_MODE or T3_BASE_URL is set. */
  readonly t3Driver?: T3DriverConfig;
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
      (workerProfileDir === undefined || cliproxyApiBaseUrl === undefined
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

const isRuntimeMode = (value: string): value is RuntimeMode =>
  RUNTIME_MODES.some((mode) => mode === value);

const t3ModelOptions = (
  raw: string | undefined,
): Result.Result<T3DriverConfig["modelOptions"], Config.ConfigError> => {
  if (raw === undefined) return Result.succeed([]);
  const invalid = Result.fail(
    new Config.ConfigError(
      new ConfigProvider.SourceError({
        message:
          'T3_MODEL_OPTIONS must be a JSON object of string or boolean values, e.g. {"effort":"high"}',
      }),
    ),
  );
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return invalid;
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return invalid;
  }
  const options: Array<{ id: string; value: string | boolean }> = [];
  for (const [id, value] of Object.entries(parsed)) {
    if (id.trim().length === 0) return invalid;
    if (typeof value === "boolean") {
      options.push({ id: id.trim(), value });
    } else if (typeof value === "string" && value.trim().length > 0) {
      options.push({ id: id.trim(), value: value.trim() });
    } else {
      return invalid;
    }
  }
  return Result.succeed(options);
};

const configFailure = (message: string) =>
  Effect.fail(
    new Config.ConfigError(new ConfigProvider.SourceError({ message })),
  );

const t3RemoteConnection = Effect.fn("GatewayConfig.t3RemoteConnection")(
  function* (
    baseUrlValue: string | undefined,
  ): Effect.fn.Return<T3Connection, Config.ConfigError> {
    if (baseUrlValue === undefined)
      return yield* configFailure("T3_MODE=remote requires T3_BASE_URL");
    const baseUrl = yield* Effect.try({
      try: () => new URL(baseUrlValue),
      catch: () =>
        new Config.ConfigError(
          new ConfigProvider.SourceError({
            message: "T3_BASE_URL must be a valid URL",
          }),
        ),
    });
    if (
      (baseUrl.protocol !== "http:" && baseUrl.protocol !== "https:") ||
      baseUrl.username !== "" ||
      baseUrl.password !== "" ||
      baseUrl.search !== "" ||
      baseUrl.hash !== ""
    )
      return yield* configFailure(
        "T3_BASE_URL must use http(s) without credentials, query, or fragment",
      );
    const allowPrivateNetworkHttp = yield* Config.Boolean(
      "T3_ALLOW_PRIVATE_NETWORK_HTTP",
    ).pipe(Config.withDefault(false));
    if (baseUrl.protocol === "http:" && !allowPrivateNetworkHttp)
      return yield* configFailure(
        "T3_BASE_URL requires HTTPS unless T3_ALLOW_PRIVATE_NETWORK_HTTP=true on a trusted private network",
      );
    const tokenFile = yield* Config.option(Config.String("T3_TOKEN_FILE"));
    const directToken = yield* Config.option(Config.String("T3_TOKEN"));
    const hasDirectToken =
      Option.isSome(directToken) && directToken.value.trim().length > 0;
    // requiredValue validates that the file (or variable) yields a token now.
    const token = yield* requiredValue("T3_TOKEN");
    return {
      _tag: "Remote",
      baseUrl,
      token:
        !hasDirectToken &&
        Option.isSome(tokenFile) &&
        tokenFile.value.trim().length > 0
          ? { _tag: "File", path: tokenFile.value.trim() }
          : { _tag: "Inline", token: Redacted.make(token) },
    };
  },
);

const T3_SIMPLE_VALUE = /^[A-Za-z0-9._/-]{1,200}$/u;

/** The only provider instances a launched daemon defines; both use CLIProxyAPI. */
export const T3_LAUNCH_INSTANCES: ReadonlySet<string> = new Set([
  "codex-cliproxy",
  "claude-cliproxy",
]);

const t3LaunchConnection = Effect.fn("GatewayConfig.t3LaunchConnection")(
  function* (
    baseUrlValue: string | undefined,
  ): Effect.fn.Return<T3Connection, Config.ConfigError> {
    // A shared daemon endpoint or bearer would bypass per-session isolation.
    if (
      baseUrlValue !== undefined ||
      (yield* optionalValue("T3_TOKEN")) !== undefined
    )
      return yield* configFailure(
        "T3_MODE=launch starts a daemon per session; unset T3_BASE_URL, T3_TOKEN and T3_TOKEN_FILE",
      );
    const portBase = yield* positiveInteger("T3_PORT_BASE", 47100);
    const portCount = yield* positiveInteger(
      "T3_PORT_COUNT",
      yield* positiveInteger("WORKER_UID_COUNT", 64),
    );
    if (
      !Number.isSafeInteger(portBase) ||
      !Number.isSafeInteger(portCount) ||
      portBase < 1024 ||
      portCount < 1 ||
      portBase + portCount - 1 > 65535
    )
      return yield* configFailure(
        "T3_PORT_BASE and T3_PORT_COUNT must describe unprivileged TCP ports",
      );
    const launchTimeoutMs = yield* timeoutMilliseconds(
      "T3_LAUNCH_TIMEOUT_MS",
      60_000,
    );
    const value = (name: string, fallback: string) =>
      optionalValue(name).pipe(
        Effect.map((raw) => raw ?? fallback),
        Effect.filterOrElse(
          (raw) => T3_SIMPLE_VALUE.test(raw),
          () => configFailure(`${name} must be a plain path or identifier`),
        ),
      );
    return {
      _tag: "Launch",
      cliPath: yield* value("T3_CLI_PATH", "t3"),
      portBase,
      portCount,
      launchTimeoutMs,
      codexExecutable: yield* value("T3_CODEX_EXECUTABLE", "codex"),
      claudeExecutable: yield* value(
        "T3_CLAUDE_EXECUTABLE",
        (yield* optionalValue("WORKER_CLAUDE_EXECUTABLE")) ?? "claude-cliproxy",
      ),
      codexModel: yield* value("T3_CODEX_MODEL", "gpt-6-sol"),
      codexReasoningEffort: yield* value("T3_CODEX_REASONING_EFFORT", "high"),
      textGenerationModel: yield* value(
        "T3_TEXT_GENERATION_MODEL",
        "gpt-6-luna",
      ),
    };
  },
);

const t3DriverConfig = Effect.fn("GatewayConfig.t3Driver")(
  function* (): Effect.fn.Return<
    T3DriverConfig | undefined,
    Config.ConfigError
  > {
    const modeValue = yield* optionalValue("T3_MODE");
    const baseUrlValue = yield* optionalValue("T3_BASE_URL");
    if (modeValue === undefined && baseUrlValue === undefined) return undefined;
    const mode = modeValue ?? "remote";
    if (mode !== "launch" && mode !== "remote") {
      return yield* configFailure("T3_MODE must be launch or remote");
    }
    const connection: T3Connection =
      mode === "launch"
        ? yield* t3LaunchConnection(baseUrlValue)
        : yield* t3RemoteConnection(baseUrlValue);
    const runtimeModeValue =
      (yield* optionalValue("T3_RUNTIME_MODE")) ?? "auto";
    if (!isRuntimeMode(runtimeModeValue)) {
      return yield* Effect.fail(
        new Config.ConfigError(
          new ConfigProvider.SourceError({
            message: `T3_RUNTIME_MODE must be one of ${RUNTIME_MODES.join(", ")}`,
          }),
        ),
      );
    }
    const interactionMode =
      (yield* optionalValue("T3_INTERACTION_MODE")) ?? "default";
    if (interactionMode !== "default" && interactionMode !== "plan") {
      return yield* Effect.fail(
        new Config.ConfigError(
          new ConfigProvider.SourceError({
            message: "T3_INTERACTION_MODE must be default or plan",
          }),
        ),
      );
    }
    const idNamespace = (yield* optionalValue("T3_ID_NAMESPACE")) ?? "symphony";
    if (!/^[A-Za-z0-9_-]{1,32}$/u.test(idNamespace)) {
      return yield* Effect.fail(
        new Config.ConfigError(
          new ConfigProvider.SourceError({
            message: "T3_ID_NAMESPACE must be 1-32 letters, digits, '_' or '-'",
          }),
        ),
      );
    }
    const modelOptions = t3ModelOptions(
      yield* optionalValue("T3_MODEL_OPTIONS"),
    );
    if (Result.isFailure(modelOptions))
      return yield* Effect.fail(modelOptions.failure);
    const projectId = yield* optionalValue("T3_PROJECT_ID");
    return {
      connection,
      ...(projectId === undefined ? {} : { projectId }),
      providerInstanceId: yield* requiredValue("T3_PROVIDER_INSTANCE"),
      model: yield* requiredValue("T3_MODEL"),
      modelOptions: modelOptions.success,
      runtimeMode: runtimeModeValue,
      interactionMode,
      idNamespace,
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
      const t3Driver = yield* t3DriverConfig();
      if (t3Driver?.connection._tag === "Launch") {
        // Launched daemons only get the proxy client key, like OMP workers.
        if (routing.cliproxyApiBaseUrl === undefined)
          return yield* configFailure(
            "T3_MODE=launch requires CLIPROXYAPI_BASE_URL and CLIPROXYAPI_API_KEY",
          );
        if (!T3_LAUNCH_INSTANCES.has(t3Driver.providerInstanceId))
          return yield* configFailure(
            `T3_MODE=launch supports T3_PROVIDER_INSTANCE ${[...T3_LAUNCH_INSTANCES].join(" or ")}`,
          );
      }
      return {
        ...values,
        ...routing,
        allowedOrganizationIds,
        gatewayAdminOrganizationIds,
        githubAppAllowedOwners,
        tokenEncryptionPreviousKeys:
          previousKeys === undefined ? undefined : Redacted.make(previousKeys),
        // Spread so the service type keeps t3Driver optional.
        ...(t3Driver === undefined ? {} : { t3Driver }),
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
