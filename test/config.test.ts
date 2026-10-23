import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { afterEach, describe, expect, it, test } from "@effect/vitest";
import {
  ConfigProvider,
  Effect,
  Layer,
  Logger,
  Redacted,
  Result,
} from "effect";
import { GatewayConfig } from "../src/services/config.js";
import { testPaths } from "./fixtures/paths.js";

const temporaryDirectories: string[] = [];

const baseValues = new Map<string, string>([
  ["LINEAR_CLIENT_ID", "client"],
  ["LINEAR_CLIENT_SECRET", "secret"],
  ["LINEAR_WEBHOOK_SECRET", "webhook"],
  [
    "TOKEN_ENCRYPTION_KEY",
    Buffer.from(new Uint8Array(32).fill(4)).toString("base64"),
  ],
  ["PUBLIC_URL", "https://ohmypi-symphony.ai.roxasroot.net"],
]);

const configLayer = (values: Map<string, string>) =>
  GatewayConfig.layer.pipe(
    Layer.provide(
      ConfigProvider.layer(
        ConfigProvider.fromUnknown(Object.fromEntries(values), {
          preserveEmptyStrings: true,
        }),
      ),
    ),
  );

const configResult = (values: Map<string, string>) =>
  Effect.result(
    Effect.gen(function* () {
      return yield* GatewayConfig;
    }).pipe(Effect.provide(configLayer(values))),
  );

const valuesWith = (
  overrides: ReadonlyArray<readonly [string, string]>,
): Map<string, string> => new Map([...baseValues, ...overrides]);

const configValuesFromEnvironment = (
  environment: Record<string, string>,
): Map<string, string> => new Map(Object.entries(environment));

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((path) => rm(path, { recursive: true, force: true })),
  );
});

const testPath = testPaths();

describe("GatewayConfig", () => {
  test("loads required values from Docker secret files", async () => {
    const directory = await mkdtemp(
      join(testPath("temporary"), "gateway-secrets-"),
    );
    temporaryDirectories.push(directory);
    const values = {
      LINEAR_CLIENT_ID: "client",
      LINEAR_CLIENT_SECRET: "secret",
      LINEAR_WEBHOOK_SECRET: "webhook",
      TOKEN_ENCRYPTION_KEY: Buffer.from(new Uint8Array(32).fill(4)).toString(
        "base64",
      ),
    };
    const environment: Record<string, string> = {
      PUBLIC_URL: "https://ohmypi-symphony.ai.roxasroot.net",
    };

    for (const [name, value] of Object.entries(values)) {
      const path = join(directory, name.toLowerCase());
      await writeFile(path, `${value}\n`, { mode: 0o600 });
      environment[`${name}_FILE`] = path;
    }

    const config = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* GatewayConfig;
      }).pipe(
        Effect.provide(configLayer(configValuesFromEnvironment(environment))),
      ),
    );
    expect(config.linearClientId).toBe("client");
    expect(Redacted.value(config.linearClientSecret)).toBe("secret");
    expect(Redacted.value(config.linearWebhookSecret)).toBe("webhook");
    expect(Redacted.value(config.tokenEncryptionKey)).toBe(
      values.TOKEN_ENCRYPTION_KEY,
    );
    expect(config.publicUrl.toString()).toBe(
      "https://ohmypi-symphony.ai.roxasroot.net/",
    );
  });

  test("prefers a direct value over its file fallback", async () => {
    const directory = await mkdtemp(
      join(testPath("temporary"), "gateway-secrets-"),
    );
    temporaryDirectories.push(directory);
    const clientIdFile = join(directory, "linear-client-id");
    await writeFile(clientIdFile, "file-client\n", { mode: 0o600 });

    const values = configValuesFromEnvironment({
      PUBLIC_URL: "http://localhost:3000",
      LINEAR_CLIENT_ID: "direct-client",
      LINEAR_CLIENT_ID_FILE: clientIdFile,
      LINEAR_CLIENT_SECRET: "secret",
      LINEAR_WEBHOOK_SECRET: "webhook",
      TOKEN_ENCRYPTION_KEY: Buffer.from(new Uint8Array(32).fill(5)).toString(
        "base64",
      ),
    });
    const config = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* GatewayConfig;
      }).pipe(Effect.provide(configLayer(values))),
    );

    expect(config.linearClientId).toBe("direct-client");
  });

  test("loads optional GitHub App credentials and keeps the key redacted", async () => {
    const configured = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* GatewayConfig;
      }).pipe(
        Effect.provide(
          configLayer(
            valuesWith([
              ["GITHUB_APP_ID", "12345"],
              ["GITHUB_APP_PRIVATE_KEY", "private-key"],
            ]),
          ),
        ),
      ),
    );
    expect(configured.githubAppId).toBe("12345");
    if (configured.githubAppPrivateKey === undefined) {
      throw new Error("Expected GitHub App private key");
    }
    expect(Redacted.isRedacted(configured.githubAppPrivateKey)).toBe(true);
    expect(Redacted.value(configured.githubAppPrivateKey)).toBe("private-key");
    expect(String(configured.githubAppPrivateKey)).not.toContain("private-key");

    const absent = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* GatewayConfig;
      }).pipe(Effect.provide(configLayer(baseValues))),
    );
    expect(absent.githubAppId).toBeUndefined();
    expect(absent.githubAppPrivateKey).toBeUndefined();
  });

  test.each([
    [["GITHUB_APP_ID", "12345"] as const, "GITHUB_APP_PRIVATE_KEY"],
    [["GITHUB_APP_PRIVATE_KEY", "private-key"] as const, "GITHUB_APP_ID"],
  ])("warns when GitHub App config is partial", async (configured, missing) => {
    const logs: string[] = [];
    const logger = Logger.make(({ message }) => {
      logs.push(
        Array.isArray(message)
          ? message.map(String).join(" ")
          : String(message),
      );
    });
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* GatewayConfig;
      }).pipe(
        Effect.provide(
          configLayer(valuesWith([configured])).pipe(
            Layer.provideMerge(Logger.layer([logger])),
          ),
        ),
      ),
    );
    expect(logs.join("\n")).toContain("GitHub App configuration is partial");
    expect(logs.join("\n")).toContain(missing);
  });

  test("rejects an unreadable optional GitHub App secret file", async () => {
    const result = await Effect.runPromise(
      configResult(
        valuesWith([
          ["GITHUB_APP_ID", "12345"],
          ["GITHUB_APP_PRIVATE_KEY_FILE", "/missing/github-app-key"],
        ]),
      ),
    );
    expect(Result.isFailure(result)).toBe(true);
  });

  test("applies all operational defaults", async () => {
    const config = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* GatewayConfig;
      }).pipe(Effect.provide(configLayer(baseValues))),
    );

    expect(config.port).toBe(3000);
    expect(config.leaseDurationMs).toBe(60_000);
    expect(config.reconcilerIntervalMs).toBe(1_000);
    expect(config.webhookReplayWindowMs).toBe(60_000);
    expect(config.logLevel).toBe("info");
    expect(config.databasePath).toBe(resolve("./data/gateway.sqlite"));
    expect(config.workspaceRoot).toBe(resolve("./data/workspaces"));
    expect(config.ompCliPath).toBe("omp");
  });

  test("requires every mandatory setting", async () => {
    for (const name of [
      "LINEAR_CLIENT_ID",
      "LINEAR_CLIENT_SECRET",
      "LINEAR_WEBHOOK_SECRET",
      "TOKEN_ENCRYPTION_KEY",
      "PUBLIC_URL",
    ]) {
      const values = new Map(baseValues);
      values.delete(name);
      const result = await Effect.runPromise(configResult(values));
      expect(Result.isFailure(result), name).toBe(true);
    }
  });

  test("rejects an empty client id", async () => {
    const result = await Effect.runPromise(
      configResult(valuesWith([["LINEAR_CLIENT_ID", ""]])),
    );
    expect(Result.isFailure(result)).toBe(true);
  });
  test("rejects a spaces-only repository suggestion confidence threshold", async () => {
    const result = await Effect.runPromise(
      configResult(
        valuesWith([["REPOSITORY_SUGGESTION_CONFIDENCE_THRESHOLD", "   "]]),
      ),
    );
    expect(Result.isFailure(result)).toBe(true);
  });
  test("rejects empty and whitespace-only confidence thresholds", async () => {
    for (const blank of ["", "   "]) {
      const result = await Effect.runPromise(
        configResult(
          valuesWith([["REPOSITORY_SUGGESTION_CONFIDENCE_THRESHOLD", blank]]),
        ),
      );
      expect(Result.isFailure(result), JSON.stringify(blank)).toBe(true);
    }
  });

  test("rejects malformed positive-integer settings instead of truncating", async () => {
    for (const malformed of ["5m", "1e3", "1.5", "-3", ""]) {
      for (const name of [
        "RECONCILER_CATCHUP_INTERVAL_MS",
        "RECONCILER_CATCHUP_MIN_AGE_MS",
        "LEASE_DURATION_MS",
      ]) {
        const result = await Effect.runPromise(
          configResult(valuesWith([[name, malformed]])),
        );
        expect(Result.isFailure(result), `${name}=${malformed}`).toBe(true);
      }
    }
  });

  test("wraps all secret values in Redacted", async () => {
    const config = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* GatewayConfig;
      }).pipe(Effect.provide(configLayer(baseValues))),
    );

    for (const [redacted, value] of [
      [config.linearClientSecret, "secret"],
      [config.linearWebhookSecret, "webhook"],
      [config.tokenEncryptionKey, baseValues.get("TOKEN_ENCRYPTION_KEY")],
    ] as const) {
      expect(Redacted.isRedacted(redacted)).toBe(true);
      expect(Redacted.value(redacted)).toBe(value);
      expect(String(redacted)).not.toContain(value);
    }
  });

  test("rejects insecure and malformed public URLs", async () => {
    for (const value of ["http://example.com", "not a URL"]) {
      const result = await Effect.runPromise(
        configResult(valuesWith([["PUBLIC_URL", value]])),
      );
      expect(Result.isFailure(result), value).toBe(true);
    }
  });

  test("accepts HTTPS and localhost public URL boundaries", async () => {
    for (const value of [
      "https://example.com",
      "http://localhost:3000",
      "https://localhost",
    ]) {
      const result = await Effect.runPromise(
        configResult(valuesWith([["PUBLIC_URL", value]])),
      );
      expect(Result.isSuccess(result), value).toBe(true);
      if (Result.isSuccess(result))
        expect(result.success.publicUrl).toBeInstanceOf(URL);
    }
  });

  test("preserves configured path boundaries", async () => {
    const values = valuesWith([
      ["DATABASE_PATH", ":memory:"],
      ["WORKSPACE_ROOT", testPath("gateway-workspaces")],
      ["OMP_CLI_PATH", ""],
    ]);
    const config = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* GatewayConfig;
      }).pipe(Effect.provide(configLayer(values))),
    );

    expect(config.databasePath).toBe(":memory:");
    expect(config.workspaceRoot).toBe(testPath("gateway-workspaces"));
    expect(config.ompCliPath).toBe("omp");
  });

  test("normalizes supported log levels and falls back for unknown levels", async () => {
    for (const [value, expected] of [
      ["trace", "trace"],
      [" DEBUG ", "debug"],
      ["Info", "info"],
      ["warn", "warn"],
      ["ERROR", "error"],
      ["fatal", "fatal"],
      ["silent", "silent"],
      ["verbose", "info"],
    ] as const) {
      const result = await Effect.runPromise(
        configResult(valuesWith([["LOG_LEVEL", value]])),
      );
      expect(Result.isSuccess(result), value).toBe(true);
      if (Result.isSuccess(result)) {
        expect(result.success.logLevel).toBe(expected);
      }
    }
  });

  for (const field of [
    "PORT",
    "LEASE_DURATION_MS",
    "RECONCILER_INTERVAL_MS",
    "WEBHOOK_REPLAY_WINDOW_MS",
  ]) {
    it.effect.each([
      ["1", true],
      ["2", true],
      ["65535", true],
      ["0", false],
      ["-1", false],
      ["1.5", false],
      ["not-a-number", false],
      ["9007199254740992", false],
    ] as const)(
      `${field} validates positive safe integer %s`,
      ([value, valid]) =>
        Effect.gen(function* () {
          const result = yield* configResult(valuesWith([[field, value]]));
          expect(Result.isSuccess(result)).toBe(valid);
        }),
    );
  }

  it.effect.each([
    ["https://example.com", true],
    ["https://localhost", true],
    ["http://localhost:3000", true],
    ["ftp://localhost", true],
    ["http://example.com", false],
    ["file:///tmp/gateway", false],
    ["not-a-url", false],
  ] as const)("validates secure or localhost URL %s", ([value, valid]) =>
    Effect.gen(function* () {
      const result = yield* configResult(valuesWith([["PUBLIC_URL", value]]));
      expect(Result.isSuccess(result)).toBe(valid);
    }),
  );
});

describe("Recovery bounds configuration", () => {
  it.effect("loads safe worker and operation bounds by default", () =>
    Effect.gen(function* () {
      const result = yield* configResult(valuesWith([]));
      expect(Result.isSuccess(result)).toBe(true);
      if (Result.isSuccess(result))
        expect(result.success).toMatchObject({
          rpcRequestTimeoutMs: 30_000,
          gitTimeoutMs: 120_000,
          nixTimeoutMs: 600_000,
          maxConcurrentWorkers: 4,
        });
    }),
  );

  it.effect.each([
    "RPC_REQUEST_TIMEOUT_MS",
    "GIT_TIMEOUT_MS",
    "NIX_TIMEOUT_MS",
    "MAX_CONCURRENT_WORKERS",
  ] as const)("requires a positive integer for %s", (name) =>
    Effect.gen(function* () {
      expect(
        Result.isFailure(yield* configResult(valuesWith([[name, "0"]]))),
      ).toBe(true);
      const result = yield* configResult(valuesWith([[name, "7"]]));
      expect(Result.isSuccess(result)).toBe(true);
      if (Result.isSuccess(result)) {
        const key = {
          RPC_REQUEST_TIMEOUT_MS: "rpcRequestTimeoutMs",
          GIT_TIMEOUT_MS: "gitTimeoutMs",
          NIX_TIMEOUT_MS: "nixTimeoutMs",
          MAX_CONCURRENT_WORKERS: "maxConcurrentWorkers",
        }[name];
        expect(Reflect.get(result.success, key)).toBe(7);
      }
    }),
  );
});

describe("Finite operation deadlines", () => {
  it.effect.each([
    "RPC_REQUEST_TIMEOUT_MS",
    "GIT_TIMEOUT_MS",
    "NIX_TIMEOUT_MS",
  ] as const)("bounds %s to the timer range", (name) =>
    Effect.gen(function* () {
      expect(
        Result.isSuccess(
          yield* configResult(valuesWith([[name, "2147483647"]])),
        ),
      ).toBe(true);
      expect(
        Result.isFailure(
          yield* configResult(valuesWith([[name, "2147483648"]])),
        ),
      ).toBe(true);
    }),
  );
});

describe("Worker model routing", () => {
  it.effect("leaves routing and the default model unset by default", () =>
    Effect.gen(function* () {
      const result = yield* configResult(baseValues);
      if (!Result.isSuccess(result)) throw new Error("expected success");
      expect(result.success.cliproxyApiBaseUrl).toBeUndefined();
      expect(result.success.cliproxyApiKey).toBeUndefined();
      expect(result.success.workerDefaultModel).toBeUndefined();
      expect(result.success.workerProfileDir).toBeUndefined();
      expect(result.success.workerExtensionPaths).toEqual([]);
      expect(result.success.workerClaudeExecutable).toBe("claude-cliproxy");
    }),
  );

  test("reads the CLIProxyAPI key from a secret file and defaults to Codex", async () => {
    const directory = await mkdtemp(
      join(testPath("temporary"), "gateway-secrets-"),
    );
    temporaryDirectories.push(directory);
    const keyFile = join(directory, "cliproxyapi-key");
    await writeFile(keyFile, "symphony-client-key\n", { mode: 0o600 });
    const result = await Effect.runPromise(
      configResult(
        valuesWith([
          ["CLIPROXYAPI_BASE_URL", "http://cliproxyapi:8317/"],
          ["CLIPROXYAPI_API_KEY_FILE", keyFile],
          ["WORKER_PROFILE_DIR", "/app/worker-profile"],
          [
            "WORKER_EXTENSION_PATHS",
            "/opt/omp-extensions/a.ts, /opt/omp-extensions/b.ts",
          ],
        ]),
      ),
    );
    if (!Result.isSuccess(result)) throw new Error("expected success");
    const config = result.success;
    expect(config.cliproxyApiBaseUrl).toBe("http://cliproxyapi:8317");
    expect(config.cliproxyApiKey && Redacted.value(config.cliproxyApiKey)).toBe(
      "symphony-client-key",
    );
    expect(String(config.cliproxyApiKey)).not.toContain("symphony-client-key");
    expect(config.workerDefaultModel).toBe("cliproxyapi/gpt-6-sol");
    expect(config.workerProfileDir).toBe("/app/worker-profile");
    expect(config.workerExtensionPaths).toEqual([
      "/opt/omp-extensions/a.ts",
      "/opt/omp-extensions/b.ts",
    ]);
  });

  it.effect("accepts an explicit default model with a thinking level", () =>
    Effect.gen(function* () {
      const result = yield* configResult(
        valuesWith([
          ["CLIPROXYAPI_BASE_URL", "https://cliproxyapi.example.test/root"],
          ["CLIPROXYAPI_API_KEY", "key"],
          ["WORKER_DEFAULT_MODEL", "pi-claude-cli/claude-opus-5-5:high"],
        ]),
      );
      if (!Result.isSuccess(result)) throw new Error("expected success");
      expect(result.success.cliproxyApiBaseUrl).toBe(
        "https://cliproxyapi.example.test/root",
      );
      expect(result.success.workerDefaultModel).toBe(
        "pi-claude-cli/claude-opus-5-5:high",
      );
    }),
  );

  it.effect.each([
    [
      "base URL without a key",
      [["CLIPROXYAPI_BASE_URL", "http://cliproxyapi:8317"]],
    ],
    ["key without a base URL", [["CLIPROXYAPI_API_KEY", "key"]]],
    [
      "embedded URL credentials",
      [
        ["CLIPROXYAPI_BASE_URL", "http://user:pass@cliproxyapi:8317"],
        ["CLIPROXYAPI_API_KEY", "key"],
      ],
    ],
    [
      "a query string",
      [
        ["CLIPROXYAPI_BASE_URL", "http://cliproxyapi:8317?key=x"],
        ["CLIPROXYAPI_API_KEY", "key"],
      ],
    ],
    [
      "a non-HTTP URL",
      [
        ["CLIPROXYAPI_BASE_URL", "file:///etc/passwd"],
        ["CLIPROXYAPI_API_KEY", "key"],
      ],
    ],
    ["a model without a provider", [["WORKER_DEFAULT_MODEL", "gpt-6-sol"]]],
    ["a relative profile path", [["WORKER_PROFILE_DIR", "worker-profile"]]],
    [
      "a relative extension path",
      [["WORKER_EXTENSION_PATHS", "/opt/a.ts,ext/b.ts"]],
    ],
  ] as const)("rejects %s", ([, overrides]) =>
    Effect.gen(function* () {
      expect(Result.isFailure(yield* configResult(valuesWith(overrides)))).toBe(
        true,
      );
    }),
  );
});
