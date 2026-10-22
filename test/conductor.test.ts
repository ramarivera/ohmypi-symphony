import { describe, expect, it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Option, Schema } from "effect";
import { GatewayServicesLive, recoverGateway } from "../src/conductor.js";
import { DeliveryId, OrganizationId, SessionId } from "../src/domain/ids.js";
import { GatewayConfig } from "../src/services/config.js";
import { Reconciler } from "../src/services/reconciler.js";
import { DeliveryRepo, RunRepo } from "../src/services/store/repositories.js";

const configProvider = ConfigProvider.fromMap(
  new Map([
    ["LINEAR_CLIENT_ID", "test-client"],
    ["LINEAR_CLIENT_SECRET", "test-client-secret"],
    ["LINEAR_WEBHOOK_SECRET", "test-webhook-secret"],
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
        expect((yield* reconciler.status()).lastError).toEqual(Option.none());
      }).pipe(Effect.provide(Live)),
  );
});

describe("Gateway startup recovery", () => {
  it.scopedLive(
    "recovers interrupted runs and makes pending deliveries reclaimable",
    () =>
      Effect.gen(function* () {
        const sid = Schema.decodeUnknownSync(SessionId)("startup-run");
        const org = Schema.decodeUnknownSync(OrganizationId)("startup-org");
        yield* RunRepo.create({
          sessionId: sid,
          organizationId: org,
          issueId: Option.none(),
        });
        yield* RunRepo.update(sid, { state: "starting" });
        yield* RunRepo.claimLease(sid, "dead-process", 60_000);
        const delivery = {
          id: Schema.decodeUnknownSync(DeliveryId)("startup-delivery"),
          organizationId: org,
          payloadHash: "hash",
          payload: { type: "AgentSessionEvent" },
        };
        expect(yield* DeliveryRepo.claim(delivery)).toBe("claimed");
        expect(yield* DeliveryRepo.claim(delivery)).toBe("duplicate");
        yield* recoverGateway;
        const run = Option.getOrThrow(yield* RunRepo.get(sid));
        expect(run.state).toBe("orphaned");
        expect(run.leaseOwner).toEqual(Option.none());
        expect(yield* DeliveryRepo.claim(delivery)).toBe("claimed");
      }).pipe(Effect.provide(Live)),
  );
});
