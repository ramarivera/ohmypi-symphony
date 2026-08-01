import { describe, expect, it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer } from "effect";
import { GatewayServicesLive } from "../src/conductor.js";
import { GatewayConfig } from "../src/services/config.js";
import { Reconciler } from "../src/services/reconciler.js";

const configProvider = ConfigProvider.fromMap(
  new Map([
    ["LINEAR_CLIENT_ID", "test-client"],
    ["LINEAR_CLIENT_SECRET", "test-client-secret"],
    ["LINEAR_WEBHOOK_SECRET", "test-webhook-secret"],
    ["LINEAR_ALLOWED_ORGANIZATION_IDS", "test-organization"],
    [
      "TOKEN_ENCRYPTION_KEY",
      Buffer.from(new Uint8Array(32).fill(7)).toString("base64"),
    ],
    ["PUBLIC_URL", "http://localhost:3000"],
    ["DATABASE_PATH", ":memory:"],
  ]),
);

const Live = GatewayServicesLive.pipe(
  Layer.provide(Layer.setConfigProvider(configProvider)),
);

describe("Conductor", () => {
  it.scopedLive(
    "composes the production services without opening a listener",
    () =>
      Effect.gen(function* () {
        const config = yield* GatewayConfig;
        const reconciler = yield* Reconciler;

        expect(config.databasePath).toBe(":memory:");
        expect(config.port).toBe(3000);
        expect((yield* reconciler.status()).lastError).toBeNull();
      }).pipe(Effect.provide(Live)),
  );
});
