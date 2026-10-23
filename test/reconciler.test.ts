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
} from "effect";
import { TestClock } from "effect/testing";
import * as fc from "fast-check";
import { Reconciler } from "../src/services/reconciler.js";
import { SessionAuthority } from "../src/services/session-authority.js";

const noOpAuthority = {
  processSession: () => Effect.void,
  shutdown: () => Effect.void,
  activeWorkerCount: () => Effect.succeed(0),
};

const reconcilerLayer = (authority: SessionAuthority["Service"]) =>
  Reconciler.layerWithoutDependencies.pipe(
    Layer.provide(Layer.succeed(SessionAuthority, authority)),
  );

describe("Reconciler", () => {
  it.effect(
    "records deterministic success timestamps and clears failures",
    () =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const authority = SessionAuthority.of({
          ...noOpAuthority,
          processRunnable: () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(release);
            }),
        });
        const clock = yield* TestClock.testClockWith(Effect.succeed);

        const status = yield* Effect.gen(function* () {
          const reconciler = yield* Reconciler;
          yield* clock.adjust(Duration.seconds(5));
          const tick = yield* Effect.forkChild(reconciler.tick(), {
            startImmediately: true,
          });

          yield* Deferred.await(started);
          expect(yield* reconciler.status()).toEqual({
            running: true,
            lastStartedAt: Option.some(5_000),
            lastCompletedAt: Option.none(),
            lastError: Option.none(),
          });

          yield* clock.adjust(Duration.seconds(7));
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(tick);
          return yield* reconciler.status();
        }).pipe(Effect.provide(reconcilerLayer(authority)));

        expect(status).toEqual({
          running: true,
          lastStartedAt: Option.some(5_000),
          lastCompletedAt: Option.some(12_000),
          lastError: Option.none(),
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
        const authority = SessionAuthority.of({
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
          const first = yield* Effect.forkChild(reconciler.tick(), {
            startImmediately: true,
          });
          yield* Deferred.await(started);
          const second = yield* Effect.forkChild(reconciler.tick(), {
            startImmediately: true,
          });
          expect(
            Option.isNone(
              yield* Effect.sync(() =>
                Option.fromUndefinedOr(second.pollUnsafe()),
              ),
            ),
          ).toBe(true);
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
      const authority = SessionAuthority.of({
        ...noOpAuthority,
        processRunnable: () => Effect.die("process failed"),
      });
      const clock = yield* TestClock.testClockWith(Effect.succeed);

      const status = yield* Effect.gen(function* () {
        const reconciler = yield* Reconciler;
        yield* clock.adjust(Duration.seconds(2));
        yield* reconciler.tick();
        return yield* reconciler.status();
      }).pipe(Effect.provide(reconcilerLayer(authority)));

      expect(status.running).toBe(true);
      expect(status.lastStartedAt).toEqual(Option.some(2_000));
      expect(status.lastCompletedAt).toEqual(Option.some(2_000));
      expect(Option.getOrNull(status.lastError)).toContain("process failed");
    }),
  );

  it.effect(
    "coalesces every generated trigger burst into one queued reconciliation",
    () =>
      Effect.gen(function* () {
        const cases = fc.record({
          burst: fc
            .integer({ min: 1, max: 50 })
            .filter(
              Schema.is(
                Schema.Number.pipe(
                  Schema.check(Schema.isInt()),
                  Schema.check(Schema.isBetween({ minimum: 1, maximum: 50 })),
                ),
              ),
            ),
        });
        const check = ({
          burst,
        }: typeof cases extends fc.Arbitrary<infer A> ? A : never) =>
          Effect.gen(function* () {
            const reconciler = yield* Reconciler;
            yield* Effect.forEach(
              Array.from({ length: burst }),
              () => reconciler.trigger(),
              { discard: true },
            );

            yield* reconciler.awaitTrigger();
            const duplicate = yield* Effect.forkChild(
              reconciler.awaitTrigger(),
            );
            expect(
              Option.isNone(
                yield* Effect.sync(() =>
                  Option.fromUndefinedOr(duplicate.pollUnsafe()),
                ),
              ),
            ).toBe(true);
            yield* Fiber.interrupt(duplicate);
          }).pipe(
            Effect.provide(
              reconcilerLayer(
                SessionAuthority.of({
                  ...noOpAuthority,
                  processRunnable: () => Effect.void,
                }),
              ),
            ),
          );
        const context =
          yield* Effect.context<Effect.Services<ReturnType<typeof check>>>();
        yield* Effect.promise(() =>
          fc.assert(
            fc.asyncProperty(cases, (sample) =>
              Effect.runPromiseWith(context)(check(sample)),
            ),
            { numRuns: 20 },
          ),
        );
      }),
  );

  it.live("models absent reconciliation lifecycle values as Options", () =>
    Effect.gen(function* () {
      const reconciler = yield* Reconciler;
      const initial = yield* reconciler.status();
      expect(initial.lastStartedAt._tag).toBe("None");
      expect(initial.lastCompletedAt._tag).toBe("None");
      expect(initial.lastError._tag).toBe("None");

      yield* reconciler.tick();
      const completed = yield* reconciler.status();
      expect(completed.lastStartedAt._tag).toBe("Some");
      expect(completed.lastCompletedAt._tag).toBe("Some");
      expect(completed.lastError._tag).toBe("None");
    }).pipe(
      Effect.provide(
        reconcilerLayer(
          SessionAuthority.of({
            ...noOpAuthority,
            processRunnable: () => Effect.void,
          }),
        ),
      ),
    ),
  );
});
