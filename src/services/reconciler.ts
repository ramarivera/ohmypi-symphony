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
import { InputId, type SessionId } from "../domain/ids.js";
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

// Catch-up polls run inline before processRunnable and share the per-org
// Linear queue, so each sweep handles a bounded batch; the rest wait for
// the next sweep.
const MAX_CATCHUP_CANDIDATES_PER_SWEEP = 25;

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
    const catchupLastPolledAt = yield* Ref.make<Map<SessionId, number>>(
      new Map(),
    );
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
        const catchupIntervalMs = Option.match(configOption, {
          onNone: () => 5 * 60_000,
          onSome: (config) => config.reconcilerCatchupIntervalMs ?? 5 * 60_000,
        });
        const gateway = gatewayOption.value;
        const runRepo = runRepoOption.value;
        const runInputRepo = runInputRepoOption.value;
        const now = yield* Clock.currentTimeMillis;
        const candidatesResult = yield* runRepo.listCatchupCandidates(now).pipe(
          Effect.matchCauseEffect({
            onSuccess: Effect.succeed,
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
        // Prune polling history for sessions no longer eligible (terminal or
        // aged out of the canceled horizon) so the map can't grow forever.
        const eligible = new Set(candidatesResult.map((run) => run.sessionId));
        yield* Ref.update(catchupLastPolledAt, (lastPolled) => {
          const next = new Map(lastPolled);
          for (const sessionId of next.keys()) {
            if (!eligible.has(sessionId)) next.delete(sessionId);
          }
          return next;
        });
        let polled = 0;
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
          const lastPolledAt = (yield* Ref.get(catchupLastPolledAt)).get(
            run.sessionId,
          );
          if (
            lastPolledAt !== undefined &&
            now - lastPolledAt <= catchupIntervalMs
          ) {
            continue;
          }
          // Bound per-sweep Linear traffic: catchup runs inline before
          // processRunnable and shares the per-org API queue, so a large
          // backlog must not starve live runs. Oldest candidates first
          // (SQL orders by created_at); the rest are picked up next sweep.
          if (polled >= MAX_CATCHUP_CANDIDATES_PER_SWEEP) break;
          // Runs that never recorded activity (e.g. just created by a live
          // webhook) fall back to createdAt for the min-age guard, so fresh
          // sessions aren't pointlessly polled.
          const activityAt = Option.getOrElse(
            run.lastActivityAt,
            () => run.createdAt,
          );
          if (now - activityAt < minAgeMs) {
            continue;
          }
          polled += 1;
          yield* Ref.update(catchupLastPolledAt, (lastPolled) => {
            const next = new Map(lastPolled);
            next.set(run.sessionId, now);
            return next;
          });
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
            // Mirror the webhook's extractPromptBody: title-prefixed when the
            // activity carries a title, so a catch-up-first injection reads
            // identically to a webhook-delivered prompt.
            const body =
              activity.title !== null && activity.body !== null
                ? `# ${activity.title}\n\n${activity.body}`
                : (activity.body ?? activity.title ?? "");
            const activityCreatedAt = Date.parse(activity.createdAt);
            const inserted = yield* runInputRepo
              .enqueue({
                id,
                sessionId: run.sessionId,
                kind,
                body,
                payload: {
                  source: "reconciler.catchup",
                  sessionId: run.sessionId,
                  activityId: activity.id,
                  activity,
                },
                createdAt: Number.isFinite(activityCreatedAt)
                  ? activityCreatedAt
                  : now,
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
      const release = Effect.gen(function* () {
        yield* Ref.set(inFlight, Option.none());
        yield* Deferred.succeed(myDeferred, undefined);
      });
      yield* Effect.gen(function* () {
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
        );

        yield* perform;
      }).pipe(Effect.ensuring(release));
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
