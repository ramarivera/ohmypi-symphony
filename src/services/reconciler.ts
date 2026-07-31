import { Cause, Clock, Deferred, Effect, Option, Queue, Ref } from "effect";
import { SessionAuthority } from "./session-authority.js";

export interface ReconcilerStatus {
  readonly running: boolean;
  readonly lastStartedAt: number | null;
  readonly lastCompletedAt: number | null;
  readonly lastError: string | null;
}

export class Reconciler extends Effect.Service<Reconciler>()("Reconciler", {
  accessors: true,
  dependencies: [SessionAuthority.Default],
  effect: Effect.gen(function* () {
    const statusRef = yield* Ref.make<ReconcilerStatus>({
      running: true,
      lastStartedAt: null,
      lastCompletedAt: null,
      lastError: null,
    });
    const inFlight = yield* Ref.make<
      Option.Option<Deferred.Deferred<void, never>>
    >(Option.none());
    const authority = yield* SessionAuthority;
    const triggers = yield* Queue.dropping<void>(1);

    const tick = Effect.fn("Reconciler.tick")(function* () {
      const myDeferred = yield* Deferred.make<void, never>();
      const claim = yield* Ref.modify(inFlight, (current) => {
        if (Option.isSome(current)) return [current, current] as const;
        const next = Option.some(myDeferred);
        return [next, next] as const;
      });
      if (Option.isSome(claim) && claim.value !== myDeferred) {
        yield* Deferred.await(claim.value);
        return;
      }
      const now = yield* Clock.currentTimeMillis;
      yield* Ref.update(statusRef, (s) => ({
        ...s,
        running: true,
        lastStartedAt: now,
      }));

      const perform = authority.processRunnable().pipe(
        Effect.matchCauseEffect({
          onSuccess: () =>
            Effect.gen(function* () {
              const completedAt = yield* Clock.currentTimeMillis;
              return yield* Ref.update(statusRef, (s) => ({
                ...s,
                lastCompletedAt: completedAt,
                lastError: null,
              }));
            }),
          onFailure: (cause) =>
            Effect.gen(function* () {
              const message = Cause.pretty(cause);
              yield* Ref.update(statusRef, (s) => ({
                ...s,
                lastError: message,
              }));
              yield* Effect.logWarning("reconciler.tick.error", {
                error: message,
              });
            }),
        }),
        Effect.ensuring(
          Effect.gen(function* () {
            yield* Ref.set(inFlight, Option.none());
            yield* Deferred.succeed(myDeferred, undefined);
          }),
        ),
      );

      yield* perform;
    });

    const trigger = Effect.fn("Reconciler.trigger")(function* () {
      yield* Queue.offer(triggers, undefined);
    });

    const awaitTrigger = Effect.fn("Reconciler.awaitTrigger")(function* () {
      yield* Queue.take(triggers);
    });

    const status = Effect.fn("Reconciler.status")(function* () {
      return yield* Ref.get(statusRef);
    });

    return { tick, trigger, awaitTrigger, status };
  }),
}) {}
