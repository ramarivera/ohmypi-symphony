import { createHash } from "node:crypto";
import { it } from "@effect/vitest";
import { Clock, Effect, Fiber, Layer, Option } from "effect";
import * as fc from "effect/FastCheck";
import { describe, expect } from "vitest";
import { LinearApiError } from "../src/domain/errors.js";
import type { SessionId, SourceKey } from "../src/domain/ids.js";
import { LinearGateway } from "../src/services/linear-gateway.js";
import { redactStringValues } from "../src/services/linear-helpers.js";
import {
  ActivityProjector,
  projectionBackoff,
  rpcEventActivityType,
} from "../src/services/projector.js";
import { ProjectionRepo } from "../src/services/store/projection-repo.js";
import { RunEventRepo } from "../src/services/store/run-event-repo.js";
import { RunInputRepo } from "../src/services/store/run-input-repo.js";
import { RunRepo } from "../src/services/store/run-repo.js";
import { SqliteClientLive } from "../src/services/store/sqlite-client.js";

interface MockState {
  activities: Array<{ readonly sessionId: string; readonly content: unknown }>;
  comments: Array<{ readonly sessionId: string; readonly body: string }>;
  updates: Array<{
    readonly sessionId: string;
    readonly plan?: unknown;
    readonly externalUrls?: unknown;
  }>;
  failuresRemaining: number;
  block: Option.Option<Promise<void>>;
  activityStarted: Option.Option<() => void>;
}

const mockState: MockState = {
  activities: [],
  comments: [],
  updates: [],
  failuresRemaining: 0,
  block: Option.none(),
  activityStarted: Option.none(),
};

const resetMock = () => {
  mockState.activities.length = 0;
  mockState.comments.length = 0;
  mockState.updates.length = 0;
  mockState.failuresRemaining = 0;
  mockState.block = Option.none();
  mockState.activityStarted = Option.none();
};

let runCounter = 0;

const mockLinear = LinearGateway.make({
  createActivity: (input) =>
    Effect.gen(function* () {
      mockState.activities.push(input);
      Option.match(mockState.activityStarted, {
        onNone: () => undefined,
        onSome: (resolve) => resolve(),
      });
      yield* Option.match(mockState.block, {
        onNone: () => Effect.void,
        onSome: (promise) => Effect.promise(() => promise),
      });
      if (mockState.failuresRemaining > 0) {
        mockState.failuresRemaining -= 1;
        return yield* Effect.fail(
          new LinearApiError({
            operation: "createActivity",
            message: "temporary Linear failure",
          }),
        );
      }
      return `activity-${mockState.activities.length}`;
    }),
  updateSession: (input) =>
    Effect.gen(function* () {
      mockState.updates.push(input);
      return yield* Effect.void;
    }),
  createIssueComment: (input) =>
    Effect.sync(() => {
      mockState.comments.push(input);
      return `comment-${mockState.comments.length}`;
    }),
  refreshInstallation: () => Effect.succeed("token"),
});

const sqlite = SqliteClientLive(":memory:");
const repoDeps = Layer.mergeAll(
  sqlite,
  RunEventRepo.Default,
  RunRepo.Default,
  RunInputRepo.Default,
  ProjectionRepo.Default,
).pipe(Layer.provide(sqlite));
const deps = Layer.mergeAll(repoDeps, Layer.succeed(LinearGateway, mockLinear));
const ActivityProjectorLayer = Layer.mergeAll(
  ActivityProjector.DefaultWithoutDependencies.pipe(Layer.provide(deps)),
  deps,
);

const sha256 = (value: string): string =>
  createHash("sha256").update(value).digest("hex");

const makeSourceKey = (value: string): SourceKey => value as SourceKey;
const makeSessionId = (value: string): SessionId => value as SessionId;

describe("ActivityProjector", () => {
  it.layer(ActivityProjectorLayer)("projects activity to Linear", (it) => {
    it.effect(
      "retries a durably queued projection after a transient API failure",
      () =>
        Effect.gen(function* () {
          resetMock();
          mockState.failuresRemaining = 1;
          const projector = yield* ActivityProjector;
          const projectionRepo = yield* ProjectionRepo;
          const runRepo = yield* RunRepo;
          yield* runRepo.create({
            sessionId: makeSessionId("session"),
            organizationId: "org" as never,
            issueId: Option.none(),
            now: 0,
          });

          const first = yield* projector.thought(
            "session",
            "accepted",
            "Accepted",
          );
          expect(first).toBe(false);
          expect(mockState.activities).toHaveLength(1);
          expect(
            yield* projectionRepo.projectionCount(
              "session" as never,
              "thought",
            ),
          ).toBe(1);

          const flushed1 = yield* projector.flushPending(
            50,
            Date.now() + 2_000,
          );
          expect(flushed1).toBe(1);
          expect(mockState.activities).toHaveLength(2);

          const flushed2 = yield* projector.flushPending(
            50,
            Date.now() + 4_000,
          );
          expect(flushed2).toBe(0);
        }),
    );

    it.effect(
      "allows only one concurrent call for the same source projection",
      () =>
        Effect.gen(function* () {
          resetMock();
          const started = Promise.withResolvers<void>();
          const release = Promise.withResolvers<void>();
          mockState.activityStarted = Option.some(() => started.resolve());
          mockState.block = Option.some(release.promise);

          const projector = yield* ActivityProjector;
          const runRepo = yield* RunRepo;
          yield* runRepo.create({
            sessionId: makeSessionId("session"),
            organizationId: "org" as never,
            issueId: Option.none(),
            now: 0,
          });

          const firstFiber = yield* Effect.fork(
            projector.thought("session", "same-source", "Accepted"),
          );
          yield* Effect.promise(() => started.promise);

          const second = yield* projector.thought(
            "session",
            "same-source",
            "Accepted",
          );
          release.resolve();
          const first = yield* Fiber.join(firstFiber);

          expect([first, second]).toEqual([true, false]);
          expect(mockState.activities).toHaveLength(1);
        }),
    );

    it.effect("uses first-write-wins for a terminal outcome", () =>
      Effect.gen(function* () {
        resetMock();
        const projector = yield* ActivityProjector;
        const runRepo = yield* RunRepo;
        yield* runRepo.create({
          sessionId: makeSessionId("session"),
          organizationId: "org" as never,
          issueId: Option.none(),
          now: 0,
        });

        const first = yield* projector.terminal(
          "session",
          "done",
          "response",
          "Completed",
        );
        expect(first).toBe(true);

        const second = yield* projector.terminal(
          "session",
          "done",
          "error",
          "Late failure",
        );
        expect(second).toBe(false);

        expect(mockState.activities).toHaveLength(1);
        expect(mockState.activities[0]?.content).toEqual({
          type: "response",
          body: "Completed",
        });
      }),
    );

    it.effect(
      "waits for agent_end before projecting the final assistant response",
      () =>
        Effect.gen(function* () {
          resetMock();
          const projector = yield* ActivityProjector;

          yield* projector.projectRpcEvent("session", 1, {
            type: "message_end",
            message: { role: "assistant", content: "Final answer" },
          });
          expect(mockState.activities).toHaveLength(0);

          yield* projector.projectRpcEvent("session", 2, { type: "agent_end" });
          expect(mockState.activities.at(-1)?.content).toEqual({
            type: "response",
            body: "Final answer",
          });
        }),
    );

    it.effect(
      "does not project extension UI events as generic elicitations",
      () =>
        Effect.gen(function* () {
          resetMock();
          const projector = yield* ActivityProjector;

          yield* projector.projectRpcEvent("session", 1, {
            type: "extension_ui_request",
            method: "setStatus",
          });
          expect(mockState.activities).toHaveLength(0);
        }),
    );

    it.effect(
      "posts deviations as issue comments without duplicating them as activities",
      () =>
        Effect.gen(function* () {
          resetMock();
          const projector = yield* ActivityProjector;
          const runRepo = yield* RunRepo;
          yield* runRepo.create({
            sessionId: makeSessionId("session"),
            organizationId: "org" as never,
            issueId: Option.some("issue-1" as never),
            now: 0,
          });

          const posted = yield* projector.deviation(
            "session",
            "deviation:call-1",
            "Used a narrower migration scope to avoid destructive changes.",
          );
          expect(posted).toBe(true);
          expect(mockState.comments).toEqual([
            {
              sessionId: "session",
              body: expect.stringContaining(
                "Used a narrower migration scope to avoid destructive changes.",
              ),
            },
          ]);

          yield* projector.projectRpcEvent("session", 1, {
            type: "tool_execution_start",
            toolName: "rromp_report_deviation",
            args: { deviation: "same deviation" },
          });
          yield* projector.projectRpcEvent("session", 2, {
            type: "tool_execution_end",
            toolName: "rromp_report_deviation",
            result: "queued",
          });
          expect(mockState.activities).toHaveLength(0);
        }),
    );

    it.effect(
      "drops a queued deviation comment when the run is stopped before dispatch",
      () =>
        Effect.gen(function* () {
          resetMock();
          const projector = yield* ActivityProjector;
          const projectionRepo = yield* ProjectionRepo;
          const runRepo = yield* RunRepo;
          const sessionId = makeSessionId("stopped-session");
          yield* runRepo.create({
            sessionId,
            organizationId: "org" as never,
            issueId: Option.some("issue-1" as never),
            now: 0,
          });
          yield* projectionRepo.enqueue({
            sourceKey: makeSourceKey("deviation:stopped"),
            sessionId,
            activityType: "comment",
            payloadHash: sha256("stopped-deviation"),
            payload: {
              request: {
                sessionId,
                body: "### Agent deviation report\n\nToo late.",
              },
            },
            firstWriteWins: true,
            now: 0,
          });
          const runInputRepo = yield* RunInputRepo;
          yield* runInputRepo.enqueue({
            id: "stop-input" as never,
            sessionId,
            kind: "stop",
            body: "",
            payload: { signal: "stop" },
            createdAt: 1,
          });

          const flushed = yield* projector.flushPending(
            50,
            yield* Clock.currentTimeMillis,
          );

          expect(flushed).toBe(1);
          expect(mockState.comments).toHaveLength(0);
        }),
    );

    it.effect("ignores empty plans because Linear rejects them", () =>
      Effect.gen(function* () {
        resetMock();
        const projector = yield* ActivityProjector;
        const runRepo = yield* RunRepo;
        yield* runRepo.create({
          sessionId: makeSessionId("session"),
          organizationId: "org" as never,
          issueId: Option.none(),
          now: 0,
        });

        const result = yield* projector.plan("session", "plan:empty", []);
        expect(result).toBe(false);
        expect(mockState.updates).toHaveLength(0);
      }),
    );

    it.effect("migrates queued plans from the legacy items wrapper", () =>
      Effect.gen(function* () {
        resetMock();
        const projector = yield* ActivityProjector;
        const projectionRepo = yield* ProjectionRepo;

        const plan = [{ content: "Implement", status: "inProgress" }] as const;
        const payload = {
          request: { sessionId: "session", plan: { items: plan } },
        };
        const serialized = JSON.stringify(payload);
        yield* projectionRepo.enqueue({
          sourceKey: makeSourceKey("plan:legacy"),
          sessionId: makeSessionId("session"),
          activityType: "plan",
          payloadHash: sha256(serialized),
          payload,
        });

        const flushed = yield* projector.flushPending();
        expect(flushed).toBe(1);
        expect(mockState.updates).toEqual([{ sessionId: "session", plan }]);
      }),
    );

    it.effect("settles queued empty legacy plans without calling Linear", () =>
      Effect.gen(function* () {
        resetMock();
        const projector = yield* ActivityProjector;
        const projectionRepo = yield* ProjectionRepo;

        const payload = {
          request: { sessionId: "session", plan: { items: [] } },
        };
        const serialized = JSON.stringify(payload);
        yield* projectionRepo.enqueue({
          sourceKey: makeSourceKey("plan:legacy-empty"),
          sessionId: makeSessionId("session"),
          activityType: "plan",
          payloadHash: sha256(serialized),
          payload,
        });

        const flushed = yield* projector.flushPending();
        expect(flushed).toBe(1);
        expect(mockState.updates).toHaveLength(0);
      }),
    );

    it.effect(
      "projects full plans and mutation-side external URLs idempotently",
      () =>
        Effect.gen(function* () {
          resetMock();
          const projector = yield* ActivityProjector;
          const runRepo = yield* RunRepo;
          yield* runRepo.create({
            sessionId: makeSessionId("session"),
            organizationId: "org" as never,
            issueId: Option.none(),
            now: 0,
          });

          const plan = [
            { content: "Implement", status: "inProgress" },
          ] as const;
          expect(yield* projector.plan("session", "plan:v1", plan)).toBe(true);
          expect(yield* projector.plan("session", "plan:v1", plan)).toBe(false);

          expect(
            yield* projector.externalUrls("session", "urls:v1", [
              { label: "Run", url: "https://gateway.example/runs/session" },
            ]),
          ).toBe(true);

          expect(mockState.updates).toEqual([
            { sessionId: "session", plan },
            {
              sessionId: "session",
              externalUrls: [
                { label: "Run", url: "https://gateway.example/runs/session" },
              ],
            },
          ]);
        }),
    );
    it.effect("forwards persisted activity and session payloads verbatim", () =>
      Effect.gen(function* () {
        resetMock();
        const projector = yield* ActivityProjector;
        const projectionRepo = yield* ProjectionRepo;
        const runRepo = yield* RunRepo;
        yield* runRepo.create({
          sessionId: makeSessionId("session"),
          organizationId: "org" as never,
          issueId: Option.none(),
          now: 0,
        });

        const activityPayload = {
          request: {
            sessionId: "session",
            content: {
              type: "elicitation",
              body: "Bearer activity-secret",
            },
            ephemeral: true,
            signal: "select",
            signalMetadata: {
              options: ["Approve", "Reject"],
              detail: "preserve exactly",
            },
          },
        };
        const plan = [{ content: "Keep exact plan", status: "inProgress" }];
        const externalUrls = [
          { label: "Run", url: "https://gateway.example/run?token=keep" },
        ];
        const jobs = [
          {
            sourceKey: "activity:verbatim",
            activityType: "elicitation",
            payload: activityPayload,
          },
          {
            sourceKey: "plan:verbatim",
            activityType: "plan",
            payload: { request: { sessionId: "session", plan } },
          },
          {
            sourceKey: "urls:verbatim",
            activityType: "externalUrls",
            payload: { request: { sessionId: "session", externalUrls } },
          },
        ] as const;
        for (const job of jobs) {
          const serialized = JSON.stringify(job.payload);
          yield* projectionRepo.enqueue({
            sourceKey: makeSourceKey(job.sourceKey),
            sessionId: makeSessionId("session"),
            activityType: job.activityType,
            payloadHash: sha256(serialized),
            payload: job.payload,
          });
        }

        expect(yield* projector.flushPending()).toBe(3);
        expect(mockState.activities).toEqual([
          {
            sessionId: "session",
            content: {
              type: "elicitation",
              body: "Bearer activity-secret",
              ephemeral: true,
              signal: "select",
              signalMetadata: {
                options: ["Approve", "Reject"],
                detail: "preserve exactly",
              },
            },
          },
        ]);
        expect(mockState.updates).toEqual([
          { sessionId: "session", plan },
          { sessionId: "session", externalUrls },
        ]);
      }),
    );

    it.effect.prop(
      "enqueue is idempotent under duplicate sourceKey",
      {
        sourceKey: fc.uuid() as fc.Arbitrary<string>,
        body: fc.string({ maxLength: 120 }) as fc.Arbitrary<string>,
      },
      ({ sourceKey, body }) =>
        Effect.gen(function* () {
          resetMock();
          const projector = yield* ActivityProjector;
          const runRepo = yield* RunRepo;
          yield* runRepo.create({
            sessionId: makeSessionId("session"),
            organizationId: "org" as never,
            issueId: Option.none(),
            now: 0,
          });

          const runId = yield* Effect.sync(() => {
            runCounter += 1;
            return runCounter;
          });
          const uniqueSourceKey = `${runId}-${sourceKey}`;
          const first = yield* projector.terminal(
            "session",
            uniqueSourceKey,
            "response",
            body,
          );
          const second = yield* projector.terminal(
            "session",
            uniqueSourceKey,
            "response",
            body,
          );

          expect(first).toBe(true);
          expect(second).toBe(false);
        }),
    );
  });

  describe("pure invariants", () => {
    it.prop(
      "redaction never leaks bearer token values",
      {
        token: fc.string({
          unit: fc.constantFrom(
            "A",
            "B",
            "C",
            "1",
            "2",
            "-",
            "_",
            ".",
            "~",
            "+",
            "/",
            "=",
          ),
          minLength: 1,
        }) as fc.Arbitrary<string>,
      },
      ({ token }) => {
        const redacted = redactStringValues({
          body: `Authorization: Bearer ${token}`,
        }) as { body: string };
        expect(redacted.body).toContain("Bearer redacted");
        expect(redacted.body).not.toContain(`Bearer ${token}`);
      },
    );

    it.prop(
      "redaction never leaks key-value token values",
      {
        kind: fc.constantFrom(
          "token",
          "key",
          "secret",
          "signature",
          "password",
        ) as fc.Arbitrary<
          "token" | "key" | "secret" | "signature" | "password"
        >,
        value: fc
          .string({ minLength: 1 })
          .filter((s) => !/[,"'\s}\]]/.test(s)) as fc.Arbitrary<string>,
      },
      ({ kind, value }) => {
        const redacted = redactStringValues({ body: `${kind}=${value}` }) as {
          body: string;
        };
        expect(redacted.body).toBe(`${kind}=redacted`);
      },
    );
    it.prop(
      "redaction never leaks query token values",
      {
        kind: fc.constantFrom(
          "token",
          "key",
          "secret",
          "signature",
          "password",
        ) as fc.Arbitrary<
          "token" | "key" | "secret" | "signature" | "password"
        >,
        value: fc
          .string({ minLength: 1 })
          .filter((s) => !/[&#\s"'<]/.test(s)) as fc.Arbitrary<string>,
      },
      ({ kind, value }) => {
        const redacted = redactStringValues({ body: `?${kind}=${value}` }) as {
          body: string;
        };
        expect(redacted.body).toBe(`?${kind}=redacted`);
      },
    );
    it.prop(
      "projection backoff is monotone non-decreasing and capped",
      { attempts: fc.integer({ min: 1, max: 100 }) as fc.Arbitrary<number> },
      ({ attempts }) => {
        const base = 1_000;
        const max = 5 * 60_000;
        const values: number[] = [];
        for (let i = 1; i <= attempts; i++) {
          values.push(projectionBackoff(i, base, max));
        }
        for (let i = 1; i < values.length; i++) {
          const value = values[i];
          const previous = values[i - 1];
          if (value !== undefined && previous !== undefined) {
            expect(value).toBeGreaterThanOrEqual(previous);
          }
        }
        for (const v of values) {
          expect(v).toBeLessThanOrEqual(max);
        }
      },
    );

    it.prop(
      "rpc event activity mapping is total over known kinds",
      {
        kind: fc.constantFrom(
          "agent_start",
          "turn_start",
          "tool_execution_start",
          "tool_execution_end",
          "message_end",
          "agent_end",
          "error",
          "extension_ui_request",
        ) as fc.Arbitrary<
          | "agent_start"
          | "turn_start"
          | "tool_execution_start"
          | "tool_execution_end"
          | "message_end"
          | "agent_end"
          | "error"
          | "extension_ui_request"
        >,
      },
      ({ kind }) => {
        const result = rpcEventActivityType({ type: kind });
        expect(["thought", "action", "response", "error", "none"]).toContain(
          result,
        );
        if (kind === "agent_start" || kind === "turn_start")
          expect(result).toBe("thought");
        if (kind === "tool_execution_start" || kind === "tool_execution_end")
          expect(result).toBe("action");
        if (kind === "agent_end") expect(result).toBe("response");
        if (kind === "error") expect(result).toBe("error");
        if (kind === "message_end" || kind === "extension_ui_request")
          expect(result).toBe("none");
      },
    );

    it.prop(
      "rpc event activity mapping returns none for unknown kinds",
      { kind: fc.string() as fc.Arbitrary<string> },
      ({ kind }) => {
        const known = new Set([
          "agent_start",
          "turn_start",
          "tool_execution_start",
          "tool_execution_end",
          "message_end",
          "agent_end",
          "error",
          "extension_ui_request",
        ]);
        if (known.has(kind)) return;
        expect(rpcEventActivityType({ type: kind })).toBe("none");
      },
    );
  });
});
