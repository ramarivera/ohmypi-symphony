import { writeFile } from "node:fs/promises";
import { describe, expect, it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Redacted, Result } from "effect";
import {
  remoteT3Endpoint,
  t3DriverOptionsFromConfig,
} from "../src/drivers/t3/driver.js";
import { GatewayConfig, type T3DriverConfig } from "../src/services/config.js";
import { testPaths } from "./fixtures/paths.js";

const pathFor = testPaths();

const baseValues: ReadonlyArray<readonly [string, string]> = [
  ["LINEAR_CLIENT_ID", "client"],
  ["LINEAR_CLIENT_SECRET", "secret"],
  ["LINEAR_WEBHOOK_SECRET", "webhook"],
  [
    "TOKEN_ENCRYPTION_KEY",
    Buffer.from(new Uint8Array(32).fill(4)).toString("base64"),
  ],
  ["PUBLIC_URL", "https://ohmypi-symphony.ai.roxasroot.net"],
];

const t3Values: ReadonlyArray<readonly [string, string]> = [
  ["T3_BASE_URL", "http://t3:3773"],
  ["T3_ALLOW_PRIVATE_NETWORK_HTTP", "true"],
  ["T3_TOKEN", "t3-bearer"],
  ["T3_PROJECT_ID", "project-1"],
  ["T3_PROVIDER_INSTANCE", "codex-cliproxy"],
  ["T3_MODEL", "gpt-6-sol"],
];

const remoteOptions = (config: T3DriverConfig) => {
  if (config.connection._tag !== "Remote")
    throw new Error("expected a remote T3 connection");
  return t3DriverOptionsFromConfig(config, remoteT3Endpoint(config.connection));
};

const launchValues: ReadonlyArray<readonly [string, string]> = [
  ["T3_MODE", "launch"],
  ["T3_PROVIDER_INSTANCE", "claude-cliproxy"],
  ["T3_MODEL", "claude-opus-5-5"],
  ["CLIPROXYAPI_BASE_URL", "https://cliproxyapi.example.test/"],
  ["CLIPROXYAPI_API_KEY", "proxy-client-key"],
];

const load = (values: ReadonlyArray<readonly [string, string]>) =>
  Effect.result(
    Effect.gen(function* () {
      return yield* GatewayConfig;
    }).pipe(
      Effect.provide(
        GatewayConfig.layer.pipe(
          Layer.provide(
            ConfigProvider.layer(
              ConfigProvider.fromUnknown(
                Object.fromEntries([...baseValues, ...values]),
                { preserveEmptyStrings: true },
              ),
            ),
          ),
        ),
      ),
    ),
  );

describe("T3 driver config", () => {
  it.effect("is absent unless T3_BASE_URL is set", () =>
    Effect.gen(function* () {
      const result = yield* load([]);
      expect(Result.isSuccess(result)).toBe(true);
      if (Result.isSuccess(result))
        expect(result.success.t3Driver).toBeUndefined();
    }),
  );

  it.effect("loads defaults and keeps the token redacted", () =>
    Effect.gen(function* () {
      const result = yield* load(t3Values);
      if (Result.isFailure(result)) throw result.failure;
      const t3 = result.success.t3Driver;
      expect(t3).toMatchObject({
        projectId: "project-1",
        providerInstanceId: "codex-cliproxy",
        model: "gpt-6-sol",
        modelOptions: [],
        runtimeMode: "auto",
        interactionMode: "default",
        idNamespace: "symphony",
      });
      const connection = t3?.connection;
      if (connection?._tag !== "Remote") throw new Error("expected remote");
      expect(connection.baseUrl.toString()).toBe("http://t3:3773/");
      expect(connection.token._tag).toBe("Inline");
      if (connection.token._tag === "Inline") {
        expect(Redacted.isRedacted(connection.token.token)).toBe(true);
        expect(String(connection.token.token)).not.toContain("t3-bearer");
        expect(Redacted.value(connection.token.token)).toBe("t3-bearer");
      }
    }),
  );

  it.effect(
    "requires HTTPS unless private-network HTTP is explicitly enabled",
    () =>
      Effect.gen(function* () {
        for (const url of [
          "http://t3:3773",
          "http://127.0.0.1:3773",
          "http://[::1]:3773",
          "http://public.example.test",
        ]) {
          for (const optIn of [undefined, "false", "true"]) {
            const result = yield* load([
              ...t3Values.filter(
                ([key]) => key !== "T3_ALLOW_PRIVATE_NETWORK_HTTP",
              ),
              ["T3_BASE_URL", url],
              ...(optIn === undefined
                ? []
                : [["T3_ALLOW_PRIVATE_NETWORK_HTTP", optIn] as const]),
            ]);
            expect(Result.isSuccess(result)).toBe(optIn === "true");
          }
        }
        const https = yield* load([
          ...t3Values.filter(
            ([key]) => key !== "T3_ALLOW_PRIVATE_NETWORK_HTTP",
          ),
          ["T3_BASE_URL", "https://t3.example.test"],
        ]);
        expect(Result.isSuccess(https)).toBe(true);
      }),
  );

  it.effect("rejects non-digit launch numbers and timer overflow", () =>
    Effect.gen(function* () {
      for (const key of [
        "T3_PORT_BASE",
        "T3_PORT_COUNT",
        "T3_LAUNCH_TIMEOUT_MS",
      ]) {
        for (const value of [
          "4e4",
          "0x9C40",
          "+40000",
          "40000.0",
          "",
          "0",
          "-1",
        ]) {
          const result = yield* load([...launchValues, [key, value]]);
          expect(Result.isFailure(result), `${key}=${value}`).toBe(true);
        }
      }
      const overflow = yield* load([
        ...launchValues,
        ["T3_LAUNCH_TIMEOUT_MS", "2147483648"],
      ]);
      expect(Result.isFailure(overflow)).toBe(true);
      const valid = yield* load([
        ...launchValues,
        ["T3_PORT_BASE", " 40000 "],
        ["T3_PORT_COUNT", " 8 "],
        ["T3_LAUNCH_TIMEOUT_MS", " 2147483647 "],
      ]);
      if (Result.isFailure(valid)) throw valid.failure;
      expect(valid.success.t3Driver?.connection).toMatchObject({
        portBase: 40000,
        portCount: 8,
        launchTimeoutMs: 2147483647,
      });
    }),
  );

  it.effect("parses model options and maps them onto the wire selection", () =>
    Effect.gen(function* () {
      const result = yield* load([
        ...t3Values,
        ["T3_MODEL_OPTIONS", '{"reasoningEffort":"high","fastMode":true}'],
        ["T3_RUNTIME_MODE", "auto-accept-edits"],
        ["T3_ID_NAMESPACE", "sym-prod"],
      ]);
      if (Result.isFailure(result)) throw result.failure;
      const t3 = result.success.t3Driver;
      if (t3 === undefined) throw new Error("expected T3 config");
      const options = remoteOptions(t3);
      expect(options.modelSelection).toEqual({
        instanceId: "codex-cliproxy",
        model: "gpt-6-sol",
        options: [
          { id: "reasoningEffort", value: "high" },
          { id: "fastMode", value: true },
        ],
      });
      expect(options.runtimeMode).toBe("auto-accept-edits");
      expect(options.idNamespace).toBe("sym-prod");
    }),
  );

  it.effect("keeps a token file path so rotation is picked up", () =>
    Effect.gen(function* () {
      const path = pathFor("secrets/t3.token");
      yield* Effect.promise(() => writeFile(path, "file-token\n"));
      const result = yield* load([
        ...t3Values.filter(([key]) => key !== "T3_TOKEN"),
        ["T3_TOKEN_FILE", path],
      ]);
      if (Result.isFailure(result)) throw result.failure;
      const t3 = result.success.t3Driver;
      if (t3 === undefined) throw new Error("expected T3 config");
      expect(t3.connection).toMatchObject({
        _tag: "Remote",
        token: { _tag: "File", path },
      });
      const token = yield* remoteOptions(t3).token;
      expect(Redacted.value(token)).toBe("file-token");
      yield* Effect.promise(() => writeFile(path, "rotated-token\n"));
      const rotated = yield* remoteOptions(t3).token;
      expect(Redacted.value(rotated)).toBe("rotated-token");
    }),
  );

  it.effect(
    "allows automatic workspace project provisioning without T3_PROJECT_ID",
    () =>
      Effect.gen(function* () {
        const result = yield* load(
          t3Values.filter(([key]) => key !== "T3_PROJECT_ID"),
        );
        if (Result.isFailure(result)) throw result.failure;
        expect(result.success.t3Driver?.projectId).toBeUndefined();
      }),
  );

  it.effect("rejects partial or invalid T3 settings", () =>
    Effect.gen(function* () {
      const cases: ReadonlyArray<ReadonlyArray<readonly [string, string]>> = [
        t3Values.filter(([key]) => key !== "T3_TOKEN"),
        [...t3Values, ["T3_RUNTIME_MODE", "yolo"]],
        [...t3Values, ["T3_INTERACTION_MODE", "chatty"]],
        [...t3Values, ["T3_MODEL_OPTIONS", '["effort"]']],
        [...t3Values, ["T3_MODEL_OPTIONS", '{"effort":3}']],
        [...t3Values, ["T3_ID_NAMESPACE", "has spaces"]],
        [["T3_BASE_URL", "ftp://t3"], ...t3Values.slice(1)],
        [["T3_BASE_URL", "https://user:secret@t3"], ...t3Values.slice(1)],
        [["T3_BASE_URL", "https://t3/?token=secret"], ...t3Values.slice(1)],
      ];
      for (const values of cases) {
        const result = yield* load(values);
        expect(Result.isFailure(result)).toBe(true);
      }
    }),
  );

  it.effect("launch mode needs no shared endpoint or bearer", () =>
    Effect.gen(function* () {
      const result = yield* load(launchValues);
      if (Result.isFailure(result)) throw result.failure;
      expect(result.success.t3Driver).toMatchObject({
        providerInstanceId: "claude-cliproxy",
        runtimeMode: "auto",
        connection: {
          _tag: "Launch",
          cliPath: "t3",
          portBase: 47100,
          portCount: 64,
          launchTimeoutMs: 60_000,
          codexExecutable: "codex",
          claudeExecutable: "claude-cliproxy",
          codexModel: "gpt-6-sol",
        },
      });
      const sized = yield* load([
        ...launchValues,
        ["WORKER_UID_COUNT", "8"],
        ["T3_PORT_BASE", "40000"],
      ]);
      if (Result.isFailure(sized)) throw sized.failure;
      expect(sized.success.t3Driver?.connection).toMatchObject({
        portBase: 40000,
        portCount: 8,
      });
    }),
  );

  it.effect("launch mode rejects settings that would share a daemon", () =>
    Effect.gen(function* () {
      const cases: ReadonlyArray<ReadonlyArray<readonly [string, string]>> = [
        [...launchValues, ["T3_BASE_URL", "http://t3:3773"]],
        [...launchValues, ["T3_TOKEN", "shared-bearer"]],
        [...launchValues, ["T3_PROVIDER_INSTANCE", "codex"]],
        launchValues.filter(([key]) => !key.startsWith("CLIPROXYAPI_")),
        [...launchValues, ["T3_MODE", "sidecar"]],
        [...launchValues, ["T3_PORT_BASE", "80"]],
        [...launchValues, ["T3_PORT_BASE", "65530"], ["T3_PORT_COUNT", "10"]],
        [...launchValues, ["T3_CLI_PATH", "t3; rm -rf /"]],
        [["T3_MODE", "remote"], ...launchValues.slice(1)],
      ];
      for (const values of cases) {
        const result = yield* load(values);
        expect(Result.isFailure(result)).toBe(true);
      }
    }),
  );
});
