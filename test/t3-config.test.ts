import { writeFile } from "node:fs/promises";
import { describe, expect, it } from "@effect/vitest";
import { ConfigProvider, Effect, Either, Layer, Redacted } from "effect";
import { t3DriverOptionsFromConfig } from "../src/drivers/t3/driver.js";
import { GatewayConfig } from "../src/services/config.js";
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
  ["T3_TOKEN", "t3-bearer"],
  ["T3_PROJECT_ID", "project-1"],
  ["T3_PROVIDER_INSTANCE", "codex-cliproxy"],
  ["T3_MODEL", "gpt-6-sol"],
];

const load = (values: ReadonlyArray<readonly [string, string]>) =>
  Effect.either(
    Effect.gen(function* () {
      return yield* GatewayConfig;
    }).pipe(
      Effect.provide(
        GatewayConfig.Default.pipe(
          Layer.provide(
            Layer.setConfigProvider(
              ConfigProvider.fromMap(new Map([...baseValues, ...values])),
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
      expect(Either.isRight(result)).toBe(true);
      if (Either.isRight(result)) expect(result.right.t3Driver).toBeUndefined();
    }),
  );

  it.effect("loads defaults and keeps the token redacted", () =>
    Effect.gen(function* () {
      const result = yield* load(t3Values);
      if (Either.isLeft(result)) throw result.left;
      const t3 = result.right.t3Driver;
      expect(t3).toMatchObject({
        projectId: "project-1",
        providerInstanceId: "codex-cliproxy",
        model: "gpt-6-sol",
        modelOptions: [],
        runtimeMode: "auto",
        interactionMode: "default",
        idNamespace: "symphony",
      });
      expect(t3?.baseUrl.toString()).toBe("http://t3:3773/");
      expect(t3?.token._tag).toBe("Inline");
      if (t3?.token._tag === "Inline") {
        expect(Redacted.isRedacted(t3.token.token)).toBe(true);
        expect(String(t3.token.token)).not.toContain("t3-bearer");
        expect(Redacted.value(t3.token.token)).toBe("t3-bearer");
      }
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
      if (Either.isLeft(result)) throw result.left;
      const t3 = result.right.t3Driver;
      if (t3 === undefined) throw new Error("expected T3 config");
      const options = t3DriverOptionsFromConfig(t3);
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
      if (Either.isLeft(result)) throw result.left;
      const t3 = result.right.t3Driver;
      expect(t3?.token).toEqual({ _tag: "File", path });
      if (t3 === undefined) throw new Error("expected T3 config");
      const token = yield* t3DriverOptionsFromConfig(t3).token;
      expect(Redacted.value(token)).toBe("file-token");
      yield* Effect.promise(() => writeFile(path, "rotated-token\n"));
      const rotated = yield* t3DriverOptionsFromConfig(t3).token;
      expect(Redacted.value(rotated)).toBe("rotated-token");
    }),
  );

  it.effect("rejects partial or invalid T3 settings", () =>
    Effect.gen(function* () {
      const cases: ReadonlyArray<ReadonlyArray<readonly [string, string]>> = [
        t3Values.filter(([key]) => key !== "T3_PROJECT_ID"),
        t3Values.filter(([key]) => key !== "T3_TOKEN"),
        [...t3Values, ["T3_RUNTIME_MODE", "yolo"]],
        [...t3Values, ["T3_INTERACTION_MODE", "chatty"]],
        [...t3Values, ["T3_MODEL_OPTIONS", '["effort"]']],
        [...t3Values, ["T3_MODEL_OPTIONS", '{"effort":3}']],
        [...t3Values, ["T3_ID_NAMESPACE", "has spaces"]],
        [["T3_BASE_URL", "ftp://t3"], ...t3Values.slice(1)],
      ];
      for (const values of cases) {
        const result = yield* load(values);
        expect(Either.isLeft(result)).toBe(true);
      }
    }),
  );
});
