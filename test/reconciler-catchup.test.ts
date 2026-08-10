import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Option, Schema } from "effect";
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
  lastActivityAt: Option.none(),
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
  const runRepo = {
    listRunnable: () => Effect.succeed(runs),
    listCancellationPending: () => Effect.succeed([]),
  } as unknown as RunRepo;
  const runInputRepo = {
    enqueue: (input: { id: string }) =>
      Effect.sync(() => {
        if (inserted.has(input.id)) return false;
        inserted.add(input.id);
        ids.push(input.id);
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
  return { layer, ids };
};

describe("Reconciler catch-up", () => {
  it.effect(
    "injects prompt and stop activities once, with unknown activities ignored",
    () =>
      Effect.gen(function* () {
        const { layer, ids } = makeLayer([run(sessionA)], () =>
          Effect.succeed([
            {
              id: "prompt-1",
              type: "prompt",
              body: "hello",
              signal: null,
              createdAt: "2025-01-01T00:00:00.000Z",
            },
            {
              id: "stop-1",
              type: "prompt",
              body: "stop",
              signal: "stop",
              createdAt: "2025-01-01T00:00:00.000Z",
            },
            {
              id: "new-1",
              type: "unknown",
              body: "ignored",
              signal: null,
              createdAt: "2025-01-01T00:00:00.000Z",
            },
          ]),
        );
        yield* Effect.gen(function* () {
          const reconciler = yield* Reconciler;
          yield* reconciler.catchup();
          yield* reconciler.catchup();
        }).pipe(Effect.provide(layer));
        expect(ids).toEqual([
          `${sessionA}:prompted:prompt-1`,
          `${sessionA}:stop:stop-1`,
        ]);
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
                    signal: "stop",
                    createdAt: "2025-01-01T00:00:00.000Z",
                  },
                  {
                    id: "cancel-prompt",
                    type: "prompt",
                    body: "resume",
                    signal: null,
                    createdAt: "2025-01-01T00:00:00.000Z",
                  },
                ]),
        );
        yield* Effect.gen(function* () {
          const reconciler = yield* Reconciler;
          yield* reconciler.catchup();
        }).pipe(Effect.provide(layer));
        expect(ids).toEqual([`${sessionB}:prompted:cancel-prompt`]);
      }),
  );
});
