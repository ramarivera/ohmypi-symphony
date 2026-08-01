import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterEach, describe, expect, it, test } from "@effect/vitest";
import {
  ConfigProvider,
  Effect,
  Either,
  Layer,
  Redacted,
  Schema,
} from "effect";
import { GatewayConfig } from "../src/services/config.js";

const temporaryDirectories: string[] = [];

const baseValues = new Map<string, string>([
  ["LINEAR_CLIENT_ID", "client"],
  ["LINEAR_CLIENT_SECRET", "secret"],
  ["LINEAR_WEBHOOK_SECRET", "webhook"],
  ["LINEAR_ALLOWED_ORGANIZATION_IDS", "allowed-org"],
  [
    "TOKEN_ENCRYPTION_KEY",
    Buffer.from(new Uint8Array(32).fill(4)).toString("base64"),
  ],
  ["PUBLIC_URL", "https://ohmypi-symphony.ai.roxasroot.net"],
]);

const configLayer = (values: Map<string, string>) =>
  GatewayConfig.Default.pipe(
    Layer.provide(Layer.setConfigProvider(ConfigProvider.fromMap(values))),
  );

const configResult = (values: Map<string, string>) =>
  Effect.either(
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

describe("GatewayConfig", () => {
  test("loads required values from Docker secret files", async () => {
    const directory = await mkdtemp(join(tmpdir(), "gateway-secrets-"));
    temporaryDirectories.push(directory);
    const values = {
      LINEAR_CLIENT_ID: "client",
      LINEAR_CLIENT_SECRET: "secret",
      LINEAR_WEBHOOK_SECRET: "webhook",
      LINEAR_ALLOWED_ORGANIZATION_IDS: "allowed-org",
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
    const directory = await mkdtemp(join(tmpdir(), "gateway-secrets-"));
    temporaryDirectories.push(directory);
    const clientIdFile = join(directory, "linear-client-id");
    await writeFile(clientIdFile, "file-client\n", { mode: 0o600 });

    const values = configValuesFromEnvironment({
      PUBLIC_URL: "http://localhost:3000",
      LINEAR_CLIENT_ID: "direct-client",
      LINEAR_CLIENT_ID_FILE: clientIdFile,
      LINEAR_CLIENT_SECRET: "secret",
      LINEAR_WEBHOOK_SECRET: "webhook",
      LINEAR_ALLOWED_ORGANIZATION_IDS: "allowed-org",
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

  test("treats the GitHub App settings as an optional group", async () => {
    const disabled = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* GatewayConfig;
      }).pipe(Effect.provide(configLayer(baseValues))),
    );
    expect(disabled.githubApp).toBeUndefined();

    const blank = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* GatewayConfig;
      }).pipe(
        Effect.provide(
          configLayer(
            valuesWith([
              ["GITHUB_APP_ID", " \t"],
              ["GITHUB_APP_PRIVATE_KEY", "\n"],
            ]),
          ),
        ),
      ),
    );
    expect(blank.githubApp).toBeUndefined();

    const partial = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* GatewayConfig;
      }).pipe(
        Effect.provide(configLayer(valuesWith([["GITHUB_APP_ID", "123"]]))),
      ),
    );
    expect(partial.githubApp?.appId).toBe("123");
    expect(
      Redacted.value(partial.githubApp?.privateKey ?? Redacted.make("")),
    ).toBe("");
  });

  test("loads optional GitHub App settings from direct or file values", async () => {
    const directory = await mkdtemp(join(tmpdir(), "gateway-github-app-"));
    temporaryDirectories.push(directory);
    const appIdFile = join(directory, "app-id");
    const privateKeyFile = join(directory, "private-key");
    await writeFile(appIdFile, "456\n", { mode: 0o600 });
    await writeFile(privateKeyFile, "file-private-key\n", { mode: 0o600 });
    const fromFiles = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* GatewayConfig;
      }).pipe(
        Effect.provide(
          configLayer(
            valuesWith([
              ["GITHUB_APP_ID", " \t"],
              ["GITHUB_APP_PRIVATE_KEY", "\n"],
              ["GITHUB_APP_ID_FILE", appIdFile],
              ["GITHUB_APP_PRIVATE_KEY_FILE", privateKeyFile],
            ]),
          ),
        ),
      ),
    );
    expect(fromFiles.githubApp?.appId).toBe("456");
    expect(
      Redacted.value(fromFiles.githubApp?.privateKey ?? Redacted.make("")),
    ).toBe("file-private-key");

    const directValues = valuesWith([
      ["GITHUB_APP_ID", "789"],
      ["GITHUB_APP_PRIVATE_KEY", " direct-private-key "],
      ["GITHUB_APP_ID_FILE", appIdFile],
      ["GITHUB_APP_PRIVATE_KEY_FILE", privateKeyFile],
    ]);
    const direct = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* GatewayConfig;
      }).pipe(Effect.provide(configLayer(directValues))),
    );
    expect(direct.githubApp?.appId).toBe("789");
    expect(
      Redacted.value(direct.githubApp?.privateKey ?? Redacted.make("")),
    ).toBe("direct-private-key");
  });

  test("requires every mandatory setting", async () => {
    for (const name of [
      "LINEAR_CLIENT_ID",
      "LINEAR_CLIENT_SECRET",
      "LINEAR_WEBHOOK_SECRET",
      "TOKEN_ENCRYPTION_KEY",
      "LINEAR_ALLOWED_ORGANIZATION_IDS",
      "PUBLIC_URL",
    ]) {
      const values = new Map(baseValues);
      values.delete(name);
      const result = await Effect.runPromise(configResult(values));
      expect(Either.isLeft(result), name).toBe(true);
    }
  });

  test("parses and validates the allowed organization set", async () => {
    const config = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* GatewayConfig;
      }).pipe(
        Effect.provide(
          configLayer(
            valuesWith([
              ["LINEAR_ALLOWED_ORGANIZATION_IDS", " org-a,org-b,org-a "],
            ]),
          ),
        ),
      ),
    );
    expect([...config.allowedOrganizationIds]).toEqual(["org-a", "org-b"]);

    for (const value of ["", "org-a,,org-b", "org a"]) {
      const result = await Effect.runPromise(
        configResult(valuesWith([["LINEAR_ALLOWED_ORGANIZATION_IDS", value]])),
      );
      expect(Either.isLeft(result), value).toBe(true);
    }
  });

  test("rejects an empty client id", async () => {
    const result = await Effect.runPromise(
      configResult(valuesWith([["LINEAR_CLIENT_ID", ""]])),
    );
    expect(Either.isLeft(result)).toBe(true);
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
      expect(Either.isLeft(result), value).toBe(true);
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
      expect(Either.isRight(result), value).toBe(true);
      if (Either.isRight(result))
        expect(result.right.publicUrl).toBeInstanceOf(URL);
    }
  });

  test("preserves configured path boundaries", async () => {
    const values = valuesWith([
      ["DATABASE_PATH", ":memory:"],
      ["WORKSPACE_ROOT", "/tmp/gateway-workspaces"],
      ["OMP_CLI_PATH", ""],
    ]);
    const config = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* GatewayConfig;
      }).pipe(Effect.provide(configLayer(values))),
    );

    expect(config.databasePath).toBe(":memory:");
    expect(config.workspaceRoot).toBe("/tmp/gateway-workspaces");
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
      expect(Either.isRight(result), value).toBe(true);
      if (Either.isRight(result)) {
        expect(result.right.logLevel).toBe(expected);
      }
    }
  });

  it.effect.prop(
    "positive integer settings accept positive safe integers and reject boundaries",
    {
      field: Schema.Literal(
        "PORT",
        "LEASE_DURATION_MS",
        "RECONCILER_INTERVAL_MS",
        "WEBHOOK_REPLAY_WINDOW_MS",
      ),
      value: Schema.Literal(
        "1",
        "2",
        "65535",
        "0",
        "-1",
        "1.5",
        "not-a-number",
        "9007199254740992",
      ),
    },
    ({ field, value }) =>
      Effect.gen(function* () {
        const result = yield* configResult(valuesWith([[field, value]]));
        const parsed = Number.parseInt(value, 10);
        const valid = Number.isSafeInteger(parsed) && parsed > 0;
        expect(Either.isRight(result)).toBe(valid);
      }),
    { fastCheck: { numRuns: 20 } },
  );

  it.effect.prop(
    "URL parsing accepts only valid secure or localhost URLs",
    {
      value: Schema.Literal(
        "https://example.com",
        "https://localhost",
        "http://localhost:3000",
        "ftp://localhost",
        "http://example.com",
        "file:///tmp/gateway",
        "not-a-url",
      ),
    },
    ({ value }) =>
      Effect.gen(function* () {
        const result = yield* configResult(valuesWith([["PUBLIC_URL", value]]));
        let valid = false;
        try {
          const url = new URL(value);
          valid = url.protocol === "https:" || url.hostname === "localhost";
        } catch {
          valid = false;
        }
        expect(Either.isRight(result)).toBe(valid);
      }),
    { fastCheck: { numRuns: 20 } },
  );
});
