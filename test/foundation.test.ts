import { it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Schema } from "effect";
import pino from "pino";
import { describe, expect } from "vitest";
import {
  AppUserId,
  DeliveryId,
  IssueId,
  OrganizationId,
  SessionId,
} from "../src/domain/ids.js";
import { GatewayConfig } from "../src/services/config.js";
import { redactionPaths } from "../src/services/logger.js";

const configValues = new Map<string, string>([
  ["LINEAR_CLIENT_ID", "client"],
  ["LINEAR_CLIENT_SECRET", "client-secret"],
  ["LINEAR_WEBHOOK_SECRET", "webhook-secret"],
  [
    "TOKEN_ENCRYPTION_KEY",
    Buffer.from(new Uint8Array(32).fill(7)).toString("base64"),
  ],
  ["PUBLIC_URL", "http://localhost:3000"],
]);

const configLayer = GatewayConfig.Default.pipe(
  Layer.provide(Layer.setConfigProvider(ConfigProvider.fromMap(configValues))),
);

describe("Effect foundation", () => {
  it.layer(configLayer)("configuration", (it) => {
    it.effect("applies operational defaults", () =>
      Effect.gen(function* () {
        const config = yield* GatewayConfig;
        expect(config.port).toBe(3000);
        expect(config.leaseDurationMs).toBe(60_000);
        expect(config.webhookReplayWindowMs).toBe(60_000);
        expect(config.publicUrl.toString()).toBe("http://localhost:3000/");
      }),
    );
  });

  it.effect("rejects invalid public URLs", () =>
    Effect.gen(function* () {
      const layer = GatewayConfig.Default.pipe(
        Layer.provide(
          Layer.setConfigProvider(
            ConfigProvider.fromMap(
              new Map([...configValues, ["PUBLIC_URL", "http://example.com"]]),
            ),
          ),
        ),
      );
      const result = yield* Effect.either(
        Effect.gen(function* () {
          return yield* GatewayConfig;
        }).pipe(Effect.provide(layer)),
      );
      expect(result._tag).toBe("Left");
    }),
  );

  it.effect.prop(
    "branded identifier schemas decode arbitrary UUID strings",
    { value: Schema.UUID },
    ({ value }) =>
      Effect.gen(function* () {
        for (const id of [
          SessionId,
          DeliveryId,
          OrganizationId,
          IssueId,
          AppUserId,
        ]) {
          expect(yield* Schema.decodeUnknown(id)(value)).toBe(value);
        }
      }),
  );

  it.prop(
    "logger redaction removes configured secret values",
    { secret: Schema.String.pipe(Schema.minLength(1)) },
    ({ secret }) => {
      let line = "";
      const logger = pino(
        { redact: { paths: redactionPaths, censor: "[REDACTED]" } },
        {
          write: (chunk) => {
            line += String(chunk);
          },
        },
      );
      logger.info(
        { nested: { token: secret }, clientSecret: secret },
        "redaction-check",
      );
      const fields = Schema.decodeUnknownSync(
        Schema.Struct({
          nested: Schema.Struct({ token: Schema.String }),
          clientSecret: Schema.String,
        }),
      )(JSON.parse(line));
      expect(fields.nested.token).toBe("[REDACTED]");
      expect(fields.clientSecret).toBe("[REDACTED]");
    },
  );
});
