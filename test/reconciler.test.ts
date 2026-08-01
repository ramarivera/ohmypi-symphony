import { describe, expect, it } from "@effect/vitest";
import {
  Deferred,
  Duration,
  Effect,
  Fiber,
  Layer,
  Option,
  Ref,
  Schema,
  TestClock,
} from "effect";
import { Reconciler } from "../src/services/reconciler.js";
import { SessionAuthority } from "../src/services/session-authority.js";

const noOpAuthority = {
  processSession: () => Effect.void,
  shutdown: () => Effect.void,
  activeWorkerCount: () => Effect.succeed(0),
};

const reconcilerLayer = (authority: SessionAuthority) =>
  Reconciler.DefaultWithoutDependencies.pipe(
    Layer.provide(Layer.succeed(SessionAuthority, authority)),
  );

describe("Reconciler", () => {
  it.effect(
    "records deterministic success timestamps and clears failures",
    () =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const authority = SessionAuthority.make({
          ...noOpAuthority,
          processRunnable: () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(release);
            }),
        });
        const clock = yield* TestClock.testClock();

        const status = yield* Effect.gen(function* () {
          const reconciler = yield* Reconciler;
          yield* clock.adjust(Duration.seconds(5));
          const tick = yield* Effect.fork(reconciler.tick());

          yield* Deferred.await(started);
          expect(yield* reconciler.status()).toMatchObject({
            running: true,
            lastStartedAt: 5_000,
            lastCompletedAt: null,
            lastError: null,
          });

          yield* clock.adjust(Duration.seconds(7));
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(tick);
          return yield* reconciler.status();
        }).pipe(Effect.provide(reconcilerLayer(authority)));

        expect(status).toMatchObject({
          running: true,
          lastStartedAt: 5_000,
          lastCompletedAt: 12_000,
          lastError: null,
        });
      }),
  );

  it.effect(
    "coalesces overlapping ticks behind one in-flight reconciliation",
    () =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const calls = yield* Ref.make(0);
        const authority = SessionAuthority.make({
          ...noOpAuthority,
          processRunnable: () =>
            Effect.gen(function* () {
              yield* Ref.update(calls, (count) => count + 1);
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(release);
            }),
        });

        const callCount = yield* Effect.gen(function* () {
          const reconciler = yield* Reconciler;
          const first = yield* Effect.fork(reconciler.tick());
          yield* Deferred.await(started);
          const second = yield* Effect.fork(reconciler.tick());
          expect(Option.isNone(yield* Fiber.poll(second))).toBe(true);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(first);
          yield* Fiber.join(second);
          return yield* Ref.get(calls);
        }).pipe(Effect.provide(reconcilerLayer(authority)));

        expect(callCount).toBe(1);
      }),
  );
  it.effect("records a failure without failing the tick", () =>
    Effect.gen(function* () {
      const authority = SessionAuthority.make({
        ...noOpAuthority,
        processRunnable: () => Effect.die("process failed"),
      });
      const clock = yield* TestClock.testClock();

      const status = yield* Effect.gen(function* () {
        const reconciler = yield* Reconciler;
        yield* clock.adjust(Duration.seconds(2));
        yield* reconciler.tick();
        return yield* reconciler.status();
      }).pipe(Effect.provide(reconcilerLayer(authority)));

      expect(status.running).toBe(true);
      expect(status.lastStartedAt).toBe(2_000);
      expect(status.lastCompletedAt).toBeNull();
      expect(status.lastError).toContain("process failed");
    }),
  );

  it.effect.prop(
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
        const duplicate = yield* Effect.fork(reconciler.awaitTrigger());
        expect(Option.isNone(yield* Fiber.poll(duplicate))).toBe(true);
        yield* Fiber.interrupt(duplicate);
      }).pipe(
        Effect.provide(
          reconcilerLayer(
            SessionAuthority.make({
              ...noOpAuthority,
              processRunnable: () => Effect.void,
            }),
          ),
        ),
      ),
    { fastCheck: { numRuns: 20 } },
  );
});
