import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Option, Schema, TestClock } from "effect";
import { LinearApiError } from "../src/domain/errors.js";
import { OrganizationId, SessionId } from "../src/domain/ids.js";
import type { AgentRun } from "../src/domain/models.js";
import type { ListedSessionActivity } from "../src/services/linear-gateway.js";
import { LinearGateway } from "../src/services/linear-gateway.js";
import { Reconciler } from "../src/services/reconciler.js";
import { SessionAuthority } from "../src/services/session-authority.js";
import { RunInputRepo, RunRepo } from "../src/services/store/repositories.js";

const sessionA = Schema.decodeUnknownSync(SessionId)(
  "22222222-2222-4222-8222-222222222222",
);
const sessionB = Schema.decodeUnknownSync(SessionId)(
  "33333333-3333-4333-8333-333333333333",
);
const organizationId = Schema.decodeUnknownSync(OrganizationId)(
  "11111111-1111-4111-8111-111111111111",
);

const run = (
  sessionId: SessionId,
  state: AgentRun["state"] = "running",
  lastActivityAt?: number,
): AgentRun => ({
  sessionId,
  organizationId,
  issueId: Option.none(),
  repositoryId: Option.none(),
  state,
  desiredState: state === "canceled" ? "canceled" : "running",
  ompSessionId: Option.none(),
  ompSessionFile: Option.none(),
  workspacePath: Option.none(),
  teamId: Option.none(),
  projectId: Option.none(),
  attempt: 0,
  leaseOwner: Option.none(),
  leaseExpiresAt: Option.none(),
  lastActivityAt: Option.fromNullable(lastActivityAt),
  terminalReason: Option.none(),
  nextAttemptAt: Option.none(),
  createdAt: 0,
  updatedAt: 0,
});

const authority = {
  processRunnable: () => Effect.void,
} as unknown as SessionAuthority;

const makeLayer = (
  runs: ReadonlyArray<AgentRun>,
  activities: (
    sessionId: string,
  ) => Effect.Effect<ReadonlyArray<ListedSessionActivity>, unknown>,
) => {
  const inserted = new Set<string>();
  const ids: string[] = [];
  const bodies = new Map<string, string>();
  const listRunnableCalls: Array<{
    readonly now: number;
    readonly includeUnleased: boolean;
  }> = [];
  const runRepo = {
    listRunnable: (now: number, includeUnleased: boolean) =>
      Effect.sync(() => {
        listRunnableCalls.push({ now, includeUnleased });
        return runs;
      }),
    listCancellationPending: () => Effect.succeed(runs),
  } as unknown as RunRepo;
  const runInputRepo = {
    enqueue: (input: { id: string; body?: string }) =>
      Effect.sync(() => {
        if (inserted.has(input.id)) return false;
        inserted.add(input.id);
        ids.push(input.id);
        bodies.set(input.id, input.body ?? "");
        return true;
      }),
  } as unknown as RunInputRepo;
  const gateway = {
    listSessionActivities: ({ sessionId }: { sessionId: string }) =>
      activities(sessionId),
  } as unknown as LinearGateway;
  const deps = Layer.mergeAll(
    Layer.succeed(RunRepo, runRepo),
    Layer.succeed(RunInputRepo, runInputRepo),
    Layer.succeed(LinearGateway, gateway),
  );
  const reconciler = Reconciler.DefaultWithoutDependencies.pipe(
    Layer.provide(Layer.succeed(SessionAuthority, authority)),
  );
  const layer = Layer.mergeAll(reconciler, deps);
  return { layer, ids, bodies, listRunnableCalls };
};

describe("Reconciler catch-up", () => {
  it.effect(
    "injects prompt and stop activities once, with unknown activities ignored",
    () =>
      Effect.gen(function* () {
        const { layer, ids, bodies, listRunnableCalls } = makeLayer(
          [run(sessionA)],
          () =>
            Effect.succeed([
              {
                id: "prompt-1",
                type: "prompt",
                body: "hello",
                title: "Follow-up on ENG-1",
                signal: null,
                createdAt: "2025-01-01T00:00:00.000Z",
              },
              {
                id: "stop-1",
                type: "prompt",
                body: "stop",
                title: null,
                signal: "stop",
                createdAt: "2025-01-01T00:00:00.000Z",
              },
              {
                id: "new-1",
                type: "unknown",
                body: "ignored",
                title: null,
                signal: null,
                createdAt: "2025-01-01T00:00:00.000Z",
              },
            ]),
        );
        yield* Effect.gen(function* () {
          const reconciler = yield* Reconciler;
          // TestClock starts at epoch: advance past the catch-up min-age so
          // the createdAt=0 fixtures are old enough to be polled.
          yield* TestClock.adjust("10 minutes");
          yield* reconciler.catchup();
          yield* reconciler.catchup();
        }).pipe(Effect.provide(layer));
        expect(listRunnableCalls.length).toBe(2);
        expect(listRunnableCalls[0]?.includeUnleased).toBe(true);
        expect(typeof listRunnableCalls[0]?.now).toBe("number");
        expect(ids).toEqual([
          `${sessionA}:prompted:prompt-1`,
          `${sessionA}:stop:stop-1`,
        ]);
        // Catch-up mirrors the webhook's extractPromptBody title prefix.
        expect(bodies.get(`${sessionA}:prompted:prompt-1`)).toBe(
          "# Follow-up on ENG-1\n\nhello",
        );
        expect(bodies.get(`${sessionA}:stop:stop-1`)).toBe("stop");
      }),
  );

  it.effect(
    "isolates activity fetch failures and preserves canceled prompt semantics",
    () =>
      Effect.gen(function* () {
        const { layer, ids } = makeLayer(
          [run(sessionA), run(sessionB, "canceled")],
          (sessionId) =>
            sessionId === sessionA
              ? Effect.fail(
                  new LinearApiError({
                    operation: "listSessionActivities",
                    message: "offline",
                  }),
                )
              : Effect.succeed([
                  {
                    id: "cancel-stop",
                    type: "prompt",
                    body: "stop",
                    title: null,
                    signal: "stop",
                    createdAt: "2025-01-01T00:00:00.000Z",
                  },
                  {
                    id: "cancel-prompt",
                    type: "prompt",
                    body: "resume",
                    title: null,
                    signal: null,
                    createdAt: "2025-01-01T00:00:00.000Z",
                  },
                ]),
        );
        yield* Effect.gen(function* () {
          const reconciler = yield* Reconciler;
          yield* TestClock.adjust("10 minutes");
          yield* reconciler.catchup();
        }).pipe(Effect.provide(layer));
        expect(ids).toEqual([`${sessionB}:prompted:cancel-prompt`]);
      }),
  );
  it.effect("skips runs with recent activity", () =>
    Effect.gen(function* () {
      const { layer, ids } = makeLayer(
        [run(sessionA, "running", Date.now())],
        () =>
          Effect.succeed([
            {
              id: "recent-prompt",
              type: "prompt",
              body: "ignored",
              title: null,
              signal: null,
              createdAt: "2025-01-01T00:00:00.000Z",
            },
          ]),
      );
      yield* Effect.gen(function* () {
        const reconciler = yield* Reconciler;
        yield* reconciler.catchup();
      }).pipe(Effect.provide(layer));
      expect(ids).toEqual([]);
    }),
  );
});
