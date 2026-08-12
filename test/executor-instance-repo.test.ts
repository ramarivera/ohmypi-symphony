import { describe, expect, it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Option, Schema } from "effect";
import { OrganizationId } from "../src/domain/ids.js";
import { ExecutorInstanceRepo } from "../src/services/store/executor-instance-repo.js";
import { SqliteClientLive } from "../src/services/store/sqlite-client.js";
import { TokenCrypto } from "../src/services/token-crypto.js";

const org = Schema.decodeUnknownSync(OrganizationId)("org_executor");
const key = Buffer.from(new Uint8Array(32).fill(0x42)).toString("base64");
const configProvider = ConfigProvider.fromMap(
  new Map<string, string>([["TOKEN_ENCRYPTION_KEY", key]]),
);
const live = ExecutorInstanceRepo.Default.pipe(
  Layer.provide(
    Layer.mergeAll(
      SqliteClientLive(":memory:"),
      TokenCrypto.Default.pipe(
        Layer.provide(Layer.setConfigProvider(configProvider)),
      ),
    ),
  ),
);

describe("ExecutorInstanceRepo", () => {
  it.layer(live)("stores encrypted credentials and supports CRUD", (it) => {
    it.effect("round trips, updates, and removes", () =>
      Effect.gen(function* () {
        const repo = yield* ExecutorInstanceRepo;
        expect(yield* repo.get(org)).toEqual(Option.none());
        const saved = yield* repo.put({
          organizationId: org,
          endpoint: "https://executor.example",
          token: "secret",
          updatedAt: 10,
        });
        expect(saved.token).toBe("secret");
        expect(yield* repo.get(org)).toEqual(Option.some(saved));
        const updated = yield* repo.put({
          organizationId: org,
          endpoint: "https://executor.example/v2",
          token: "next",
          updatedAt: 20,
        });
        expect(yield* repo.get(org)).toEqual(Option.some(updated));
        expect(yield* repo.remove(org)).toBe(true);
        expect(yield* repo.remove(org)).toBe(false);
      }),
    );
  });
});
