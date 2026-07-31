import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
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

const configValuesFromEnvironment = async (
  environment: Record<string, string>,
): Promise<Map<string, string>> => {
  const values = new Map<string, string>();
  for (const [name, value] of Object.entries(environment)) {
    if (!name.endsWith("_FILE")) values.set(name, value);
  }
  for (const [name, path] of Object.entries(environment)) {
    if (!name.endsWith("_FILE")) continue;
    const valueName = name.slice(0, -"_FILE".length);
    if (values.has(valueName)) continue;
    values.set(valueName, (await readFile(path, "utf8")).trim());
  }
  return values;
};

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
        Effect.provide(
          configLayer(await configValuesFromEnvironment(environment)),
        ),
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

    const values = await configValuesFromEnvironment({
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
    expect(config.databasePath).toBe("./data/gateway.sqlite");
    expect(config.workspaceRoot).toBe("./data/workspaces");
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
      expect(Either.isLeft(result), name).toBe(true);
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
    expect(config.ompCliPath).toBe("");
  });

  test("accepts supported log levels and rejects unknown levels", async () => {
    for (const level of [
      "trace",
      "debug",
      "info",
      "warn",
      "error",
      "fatal",
      "silent",
    ]) {
      const result = await Effect.runPromise(
        configResult(valuesWith([["LOG_LEVEL", level]])),
      );
      expect(Either.isRight(result), level).toBe(true);
    }
    const invalid = await Effect.runPromise(
      configResult(valuesWith([["LOG_LEVEL", "verbose"]])),
    );
    expect(Either.isLeft(invalid)).toBe(true);
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
        const parsed = Number(value);
        const valid =
          Number.isSafeInteger(parsed) &&
          parsed > 0 &&
          Number.isInteger(parsed);
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
