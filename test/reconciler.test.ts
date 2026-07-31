import { describe, expect, it } from "@effect/vitest";
import { Duration, Effect, Layer, Schema } from "effect";
import { Reconciler } from "../src/services/reconciler.js";
import { SessionAuthority } from "../src/services/session-authority.js";

const authority = SessionAuthority.make({
  processRunnable: () => Effect.void,
  processSession: () => Effect.void,
  shutdown: () => Effect.void,
  activeWorkerCount: () => Effect.succeed(0),
});

const Live = Reconciler.DefaultWithoutDependencies.pipe(
  Layer.provide(Layer.succeed(SessionAuthority, authority)),
);

describe("Reconciler", () => {
  it.scopedLive.prop(
    "coalesces every generated trigger burst into one queued reconciliation",
    { burst: Schema.Number.pipe(Schema.int(), Schema.between(1, 50)) },
    ({ burst }) =>
      Effect.gen(function* () {
        const reconciler = yield* Reconciler;
        yield* Effect.forEach(
          Array.from({ length: burst }),
          () => reconciler.trigger(),
          { discard: true },
        );

        yield* reconciler.awaitTrigger();
        const duplicate = yield* Effect.race(
          reconciler.awaitTrigger().pipe(Effect.as(true)),
          Effect.sleep(Duration.millis(10)).pipe(Effect.as(false)),
        );
        expect(duplicate).toBe(false);
      }).pipe(Effect.provide(Live)),
    { fastCheck: { numRuns: 20 }, timeout: 15_000 },
  );
});
