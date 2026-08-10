import {
  Cause,
  Clock,
  Deferred,
  Effect,
  Option,
  Queue,
  Ref,
  Schema,
} from "effect";
import { InputId } from "../domain/ids.js";
import { GatewayConfig } from "./config.js";
import { LinearGateway } from "./linear-gateway.js";
import { SessionAuthority } from "./session-authority.js";
import { RunInputRepo, RunRepo } from "./store/repositories.js";
export interface ReconcilerStatus {
  readonly running: boolean;
  readonly lastStartedAt: Option.Option<number>;
  readonly lastCompletedAt: Option.Option<number>;
  readonly lastError: Option.Option<string>;
}

export class Reconciler extends Effect.Service<Reconciler>()("Reconciler", {
  accessors: true,
  dependencies: [SessionAuthority.Default],
  effect: Effect.gen(function* () {
    const statusRef = yield* Ref.make<ReconcilerStatus>({
      running: true,
      lastStartedAt: Option.none(),
      lastCompletedAt: Option.none(),
      lastError: Option.none(),
    });
    const inFlight = yield* Ref.make<
      Option.Option<Deferred.Deferred<void, never>>
    >(Option.none());
    const authority = yield* SessionAuthority;
    const triggers = yield* Queue.dropping<void>(1);

    const catchupLastAt = yield* Ref.make<Option.Option<number>>(Option.none());
    const catchup = Effect.fn("Reconciler.catchup")(
      function* (): Effect.fn.Return<void, never> {
        const gatewayOption = yield* Effect.serviceOption(LinearGateway);
        const runRepoOption = yield* Effect.serviceOption(RunRepo);
        const runInputRepoOption = yield* Effect.serviceOption(RunInputRepo);
        const configOption = yield* Effect.serviceOption(GatewayConfig);
        if (
          Option.isNone(gatewayOption) ||
          Option.isNone(runRepoOption) ||
          Option.isNone(runInputRepoOption)
        ) {
          return;
        }
        const minAgeMs = Option.match(configOption, {
          onNone: () => 2 * 60_000,
          onSome: (config) => config.reconcilerCatchupMinAgeMs ?? 2 * 60_000,
        });
        const gateway = gatewayOption.value;
        const runRepo = runRepoOption.value;
        const runInputRepo = runInputRepoOption.value;
        const now = yield* Clock.currentTimeMillis;
        const candidatesResult = yield* Effect.all([
          runRepo.listRunnable(now, true),
          runRepo.listCancellationPending(),
        ]).pipe(
          Effect.matchCauseEffect({
            onSuccess: ([runnable, cancellationPending]) =>
              Effect.succeed([...runnable, ...cancellationPending]),
            onFailure: (cause) =>
              Effect.gen(function* () {
                yield* Effect.logWarning("reconciler.catchup.runs_failed").pipe(
                  Effect.annotateLogs({ error: Cause.pretty(cause) }),
                );
                return [];
              }),
          }),
        );
        const seen = new Set<string>();
        for (const run of candidatesResult) {
          if (seen.has(run.sessionId)) continue;
          seen.add(run.sessionId);
          if (
            run.state !== "queued" &&
            run.state !== "starting" &&
            run.state !== "running" &&
            run.state !== "waiting" &&
            run.state !== "stopping" &&
            run.state !== "canceled"
          ) {
            continue;
          }
          if (
            Option.isSome(run.lastActivityAt) &&
            now - run.lastActivityAt.value < minAgeMs
          ) {
            continue;
          }
          const activities = yield* gateway
            .listSessionActivities({
              sessionId: run.sessionId,
            })
            .pipe(
              Effect.matchCauseEffect({
                onSuccess: Effect.succeed,
                onFailure: (cause) =>
                  Effect.gen(function* () {
                    yield* Effect.logWarning(
                      "reconciler.catchup.activities_failed",
                    ).pipe(
                      Effect.annotateLogs({
                        sessionId: run.sessionId,
                        error: Cause.pretty(cause),
                      }),
                    );
                    return [];
                  }),
              }),
            );
          for (const activity of activities) {
            if (activity.type !== "prompt") continue;
            const kind = activity.signal === "stop" ? "stop" : "prompted";
            if (run.state === "canceled" && kind !== "prompted") continue;
            const id = Schema.decodeUnknownSync(InputId)(
              `${run.sessionId}:${kind}:${activity.id}`,
            );
            const inserted = yield* runInputRepo
              .enqueue({
                id,
                sessionId: run.sessionId,
                kind,
                body: activity.body ?? "",
                payload: {
                  source: "reconciler.catchup",
                  sessionId: run.sessionId,
                  activityId: activity.id,
                  activity,
                },
                createdAt: Date.parse(activity.createdAt) || now,
              })
              .pipe(
                Effect.matchCauseEffect({
                  onSuccess: Effect.succeed,
                  onFailure: (cause) =>
                    Effect.gen(function* () {
                      yield* Effect.logWarning(
                        "reconciler.catchup.enqueue_failed",
                      ).pipe(
                        Effect.annotateLogs({
                          sessionId: run.sessionId,
                          activityId: activity.id,
                          error: Cause.pretty(cause),
                        }),
                      );
                      return false;
                    }),
                }),
              );
            if (inserted) {
              yield* Effect.logInfo("reconciler.catchup.injected").pipe(
                Effect.annotateLogs({
                  sessionId: run.sessionId,
                  activityId: activity.id,
                }),
              );
            }
          }
        }
      },
    );

    const tick = Effect.fn("Reconciler.tick")(function* (): Effect.fn.Return<
      void,
      never
    > {
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
        lastStartedAt: Option.some(now),
      }));

      const configOption = yield* Effect.serviceOption(GatewayConfig);
      const catchupIntervalMs = Option.match(configOption, {
        onNone: () => 5 * 60_000,
        onSome: (config) => config.reconcilerCatchupIntervalMs ?? 5 * 60_000,
      });
      const lastCatchup = yield* Ref.get(catchupLastAt);
      if (
        Option.isNone(lastCatchup) ||
        now - lastCatchup.value >= catchupIntervalMs
      ) {
        yield* Ref.set(catchupLastAt, Option.some(now));
        yield* catchup();
      }

      const perform = authority.processRunnable().pipe(
        Effect.matchCauseEffect({
          onSuccess: () =>
            Effect.gen(function* () {
              const completedAt = yield* Clock.currentTimeMillis;
              return yield* Ref.update(statusRef, (s) => ({
                ...s,
                lastCompletedAt: Option.some(completedAt),
                lastError: Option.none(),
              }));
            }),
          onFailure: (cause) =>
            Effect.gen(function* () {
              const message = Cause.pretty(cause);
              yield* Ref.update(statusRef, (s) => ({
                ...s,
                lastError: Option.some(message),
              }));
              yield* Effect.logWarning("reconciler.tick.error").pipe(
                Effect.annotateLogs({
                  error: message,
                }),
              );
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

    const trigger = Effect.fn("Reconciler.trigger")(
      function* (): Effect.fn.Return<void, never> {
        yield* Queue.offer(triggers, undefined);
      },
    );

    const awaitTrigger = Effect.fn("Reconciler.awaitTrigger")(
      function* (): Effect.fn.Return<void, never> {
        yield* Queue.take(triggers);
      },
    );

    const status = Effect.fn("Reconciler.status")(
      function* (): Effect.fn.Return<ReconcilerStatus, never> {
        return yield* Ref.get(statusRef);
      },
    );

    return { tick, catchup, trigger, awaitTrigger, status };
  }),
}) {}
