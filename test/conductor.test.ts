import type { Database } from "bun:sqlite";
import { describe, expect, it } from "@effect/vitest";
import {
  Clock,
  ConfigProvider,
  Deferred,
  Effect,
  Fiber,
  Layer,
  Option,
  Schema,
} from "effect";
import { GatewayServicesLive, recoverGateway } from "../src/conductor.js";
import { DeliveryId, OrganizationId, SessionId } from "../src/domain/ids.js";
import { GatewayConfig } from "../src/services/config.js";
import { Reconciler } from "../src/services/reconciler.js";
import { SessionAuthority } from "../src/services/session-authority.js";
import { DeliveryRepo, RunRepo } from "../src/services/store/repositories.js";
import {
  SqliteClient,
  SqliteClientLive,
  transact,
} from "../src/services/store/sqlite-client.js";

const configProvider = ConfigProvider.fromUnknown(
  Object.fromEntries(
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
  ),
  { preserveEmptyStrings: true },
);

const Live = GatewayServicesLive.pipe(
  Layer.provide(ConfigProvider.layer(configProvider)),
);

describe("Conductor", () => {
  it.live("composes the production services without opening a listener", () =>
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
  it.live(
    "recovers interrupted runs and makes pending deliveries reclaimable",
    () =>
      Effect.gen(function* () {
        const sid = Schema.decodeUnknownSync(SessionId)("startup-run");
        const org = Schema.decodeUnknownSync(OrganizationId)("startup-org");
        yield* RunRepo.use((service) =>
          service.create({
            sessionId: sid,
            organizationId: org,
            issueId: Option.none(),
          }),
        );
        yield* RunRepo.use((service) =>
          service.update(sid, { state: "starting" }),
        );
        yield* Effect.gen(function* () {
          const service = yield* RunRepo;
          return yield* service.claimLease(
            sid,
            "dead-process",
            60_000,
            (yield* Clock.currentTimeMillis) - 60_001,
          );
        });
        const delivery = {
          id: Schema.decodeUnknownSync(DeliveryId)("startup-delivery"),
          organizationId: org,
          payloadHash: "hash",
          payload: { type: "AgentSessionEvent" },
        };
        expect(
          yield* DeliveryRepo.use((service) => service.claim(delivery)),
        ).toBe("claimed");
        expect(
          yield* DeliveryRepo.use((service) => service.claim(delivery)),
        ).toBe("duplicate");
        yield* recoverGateway;
        const run = Option.getOrThrow(
          yield* RunRepo.use((service) => service.get(sid)),
        );
        expect(run.state).toBe("orphaned");
        expect(run.leaseOwner).toEqual(Option.none());
        expect(
          yield* DeliveryRepo.use((service) => service.claim(delivery)),
        ).toBe("claimed");
      }).pipe(Effect.provide(Live)),
  );
});

it.live(
  "Layer.launch shutdown interrupts reconciliation, rolls back, and closes SQLite",
  () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      let database: Database | undefined;
      let rowsAfterRollback: number | undefined;
      let reconciliationStopped = false;
      const authority = SessionAuthority.of({
        processSession: () => Effect.void,
        shutdown: () => Effect.void,
        activeWorkerCount: () => Effect.succeed(0),
        processRunnable: () =>
          Effect.gen(function* () {
            const db = database;
            if (db === undefined)
              return yield* Effect.die("SQLite wasn't initialized");
            return yield* transact(
              db,
              Effect.gen(function* () {
                db.exec("INSERT INTO lifecycle VALUES (2)");
                yield* Deferred.succeed(entered, undefined);
                return yield* Effect.never;
              }),
            ).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  reconciliationStopped = true;
                }),
              ),
            );
          }),
      });
      const startup = Layer.effectDiscard(
        Effect.gen(function* () {
          const { db } = yield* SqliteClient;
          database = db;
          db.exec("CREATE TABLE lifecycle (value INTEGER)");
          yield* transact(
            db,
            Effect.sync(() => db.exec("INSERT INTO lifecycle VALUES (1)")),
          );
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              rowsAfterRollback = db
                .query<{ count: number }, []>(
                  "SELECT COUNT(*) AS count FROM lifecycle",
                )
                .get()?.count;
            }),
          );
          const reconciler = yield* Reconciler;
          yield* Effect.forkScoped(reconciler.tick());
        }),
      ).pipe(
        Layer.provide(
          Layer.mergeAll(
            SqliteClientLive(":memory:"),
            Reconciler.layerWithoutDependencies.pipe(
              Layer.provide(Layer.succeed(SessionAuthority, authority)),
            ),
          ),
        ),
      );
      const launched = yield* Effect.forkChild(Layer.launch(startup));
      yield* Deferred.await(entered);
      yield* Fiber.interrupt(launched);
      expect(reconciliationStopped).toBe(true);
      expect(rowsAfterRollback).toBe(1);
      const db = database;
      if (db === undefined) throw new Error("Missing SQLite lifecycle fixture");
      expect(() => db.query("SELECT 1").get()).toThrow();
    }),
);
