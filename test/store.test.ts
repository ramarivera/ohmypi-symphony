import { it } from "@effect/vitest";
import {
  Clock,
  ConfigProvider,
  Effect,
  Either,
  Layer,
  Option,
  Schema,
} from "effect";
import { describe, expect } from "vitest";
import type {
  ActivityId,
  AppUserId,
  DeliveryId,
  InputId,
  IssueId,
  OrganizationId,
  SessionId,
  SourceKey,
  TeamId,
} from "../src/domain/ids.js";
import { type Installation, RunState } from "../src/domain/models.js";
import {
  AdminSessionRepo,
  DeliveryRepo,
  InstallationRepo,
  ProjectionRepo,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
} from "../src/services/store/repositories.js";
import { SqliteClientLive } from "../src/services/store/sqlite-client.js";
import { TokenCrypto } from "../src/services/token-crypto.js";

const testKeyBase64 = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=";
const configProvider = ConfigProvider.fromMap(
  new Map([["TOKEN_ENCRYPTION_KEY", testKeyBase64]]),
);

type RepoServices =
  | AdminSessionRepo
  | InstallationRepo
  | DeliveryRepo
  | RunRepo
  | RunInputRepo
  | RunEventRepo
  | ProjectionRepo
  | TokenCrypto;

const withRepos = <A, E>(effect: Effect.Effect<A, E, RepoServices>) =>
  Effect.gen(function* () {
    yield* Effect.scope;
    const sqlite = SqliteClientLive(":memory:");
    const repos = Layer.mergeAll(
      AdminSessionRepo.Default,
      InstallationRepo.Default,
      DeliveryRepo.Default,
      RunRepo.Default,
      RunInputRepo.Default,
      RunEventRepo.Default,
      ProjectionRepo.Default,
      TokenCrypto.Default,
    ).pipe(
      Layer.provide(
        Layer.mergeAll(sqlite, Layer.setConfigProvider(configProvider)),
      ),
    );
    return yield* effect.pipe(Effect.provide(repos));
  });

const makeSessionId = (value: string): SessionId => value as SessionId;
const makeDeliveryId = (value: string): DeliveryId => value as DeliveryId;
const makeOrganizationId = (value: string): OrganizationId =>
  value as OrganizationId;
const makeIssueId = (value: string): IssueId => value as IssueId;
const makeTeamId = (value: string): TeamId => value as TeamId;
const makeAppUserId = (value: string): AppUserId => value as AppUserId;
const makeSourceKey = (value: string): SourceKey => value as SourceKey;
const makeInputId = (value: string): InputId => value as InputId;
const makeActivityId = (value: string): ActivityId => value as ActivityId;

const makeInstallation = (
  overrides: Partial<Installation> = {},
): Installation => ({
  organizationId: makeOrganizationId("org-1"),
  appUserId: makeAppUserId("app-user-1"),
  accessToken: "access-secret",
  refreshToken: "refresh-secret",
  expiresAt: 1_000_000,
  scopes: ["read", "write"],
  revokedAt: Option.none(),
  accessibleTeamIds: Option.none(),
  canAccessAllPublicTeams: Option.none(),
  ...overrides,
});

const DistinctStrings = Schema.Struct({
  first: Schema.String,
  second: Schema.String,
}).pipe(Schema.filter((s) => s.first !== s.second));

const NonEmptyDistinctStrings = Schema.Struct({
  first: Schema.String.pipe(Schema.minLength(1)),
  second: Schema.String.pipe(Schema.minLength(1)),
}).pipe(Schema.filter((s) => s.first !== s.second));

describe("Store repositories", () => {
  it.scopedLive("encrypts tokens at rest and round-trips installations", () =>
    withRepos(
      Effect.gen(function* () {
        const repo = yield* InstallationRepo;
        const expected = makeInstallation();
        yield* repo.put(expected);

        const raw = yield* repo.getRawEncryptedAccessToken(
          expected.organizationId,
        );
        if (Option.isSome(raw)) {
          expect(raw.value).not.toContain("access-secret");
        }

        const actual = yield* repo.get(expected.organizationId);
        expect(actual).toEqual(Option.some(expected));
      }),
    ),
  );

  it.scopedLive("consumes OAuth state exactly once before expiry", () =>
    withRepos(
      Effect.gen(function* () {
        const repo = yield* InstallationRepo;
        yield* repo.createOAuthState("hash", 2_000, 1_000);
        expect(yield* repo.consumeOAuthState("hash", 1_500)).toBe(true);
        expect(yield* repo.consumeOAuthState("hash", 1_600)).toBe(false);
        yield* repo.createOAuthState("expired", 2_000, 1_000);
        expect(yield* repo.consumeOAuthState("expired", 2_001)).toBe(false);
      }),
    ),
  );

  it.scopedLive("deduplicates webhook delivery IDs", () =>
    withRepos(
      Effect.gen(function* () {
        const repo = yield* DeliveryRepo;
        const delivery = {
          id: makeDeliveryId("delivery-1"),
          organizationId: makeOrganizationId("org-1"),
          payloadHash: "hash",
          payload: { type: "AgentSessionEvent" },
        };
        expect(yield* repo.accept(delivery)).toBe(true);
        expect(yield* repo.accept(delivery)).toBe(false);
      }),
    ),
  );

  it.scopedLive(
    "reclaims a delivery interrupted before durable processing",
    () =>
      withRepos(
        Effect.gen(function* () {
          const repo = yield* DeliveryRepo;
          const delivery = {
            id: makeDeliveryId("delivery-1"),
            organizationId: makeOrganizationId("org-1"),
            payloadHash: "hash",
            payload: { type: "AgentSessionEvent" },
          };
          expect(yield* repo.claim(delivery)).toBe("claimed");
          expect(yield* repo.claim(delivery)).toBe("duplicate");
          expect(yield* repo.recoverPendingDeliveries()).toBe(1);
          expect(yield* repo.claim(delivery)).toBe("claimed");
        }),
      ),
  );

  it.scopedLive(
    "enforces one lease and permits takeover only after expiry",
    () =>
      withRepos(
        Effect.gen(function* () {
          const repo = yield* RunRepo;
          yield* repo.create({
            sessionId: makeSessionId("session-1"),
            organizationId: makeOrganizationId("org-1"),
            issueId: Option.none(),
            now: 1_000,
          });
          expect(
            yield* repo.claimLease(
              makeSessionId("session-1"),
              "worker-a",
              1_000,
              1_000,
            ),
          ).toBe(true);
          expect(
            yield* repo.claimLease(
              makeSessionId("session-1"),
              "worker-b",
              1_000,
              1_500,
            ),
          ).toBe(false);
          expect(
            yield* repo.claimLease(
              makeSessionId("session-1"),
              "worker-b",
              1_000,
              2_001,
            ),
          ).toBe(true);
        }),
      ),
  );

  it.scopedLive("recovers active runs and leases after a process restart", () =>
    withRepos(
      Effect.gen(function* () {
        const runRepo = yield* RunRepo;
        const inputRepo = yield* RunInputRepo;

        yield* runRepo.create({
          sessionId: makeSessionId("running-session"),
          organizationId: makeOrganizationId("org-1"),
          issueId: Option.some(makeIssueId("issue-1")),
          now: 1_000,
        });
        yield* runRepo.update(makeSessionId("running-session"), {
          state: "running",
          workspacePath: Option.some("/workspace"),
          ompSessionFile: Option.some("/workspace/session.jsonl"),
        });
        expect(
          yield* runRepo.claimLease(
            makeSessionId("running-session"),
            "dead-process",
            60_000,
            1_000,
          ),
        ).toBe(true);

        yield* runRepo.create({
          sessionId: makeSessionId("canceled-session"),
          organizationId: makeOrganizationId("org-1"),
          issueId: Option.some(makeIssueId("issue-2")),
          now: 1_000,
        });
        yield* runRepo.update(makeSessionId("canceled-session"), {
          state: "running",
        });
        yield* inputRepo.enqueue({
          id: makeInputId("stop"),
          sessionId: makeSessionId("canceled-session"),
          kind: "stop",
          body: "",
          payload: {},
          createdAt: 1_100,
        });

        expect(yield* runRepo.recoverInterruptedRuns(2_000)).toBe(2);

        const maybeRunning = yield* runRepo.get(
          makeSessionId("running-session"),
        );
        Option.match(maybeRunning, {
          onNone: () => expect.fail("running-session not found"),
          onSome: (run) => {
            expect(run).toMatchObject({
              state: "orphaned",
              nextAttemptAt: Option.some(2_000),
              leaseOwner: Option.none(),
              leaseExpiresAt: Option.none(),
            });
          },
        });

        const runnable = yield* runRepo.listRunnable(2_000);
        expect(runnable.map((run) => run.sessionId)).toContain(
          makeSessionId("running-session"),
        );

        const maybeCanceled = yield* runRepo.get(
          makeSessionId("canceled-session"),
        );
        Option.match(maybeCanceled, {
          onNone: () => expect.fail("canceled-session not found"),
          onSome: (run) => {
            expect(run).toMatchObject({
              state: "stopping",
              desiredState: "canceled",
              leaseOwner: Option.none(),
              leaseExpiresAt: Option.none(),
            });
          },
        });
      }),
    ),
  );

  it.scopedLive(
    "stop dominates later prompts and revocation cancels all live runs",
    () =>
      withRepos(
        Effect.gen(function* () {
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          const inputRepo = yield* RunInputRepo;

          yield* installationRepo.put(makeInstallation());
          yield* runRepo.create({
            sessionId: makeSessionId("session-1"),
            organizationId: makeOrganizationId("org-1"),
            issueId: Option.none(),
          });

          expect(
            yield* inputRepo.enqueue({
              id: makeInputId("stop"),
              sessionId: makeSessionId("session-1"),
              kind: "stop",
              body: "stop",
              payload: {},
            }),
          ).toBe(true);
          expect(
            yield* inputRepo.enqueue({
              id: makeInputId("late"),
              sessionId: makeSessionId("session-1"),
              kind: "prompted",
              body: "keep going",
              payload: {},
            }),
          ).toBe(false);

          const maybeRun = yield* runRepo.get(makeSessionId("session-1"));
          Option.match(maybeRun, {
            onNone: () => expect.fail("run not found"),
            onSome: (run) => expect(run.desiredState).toBe("canceled"),
          });

          const revokedAt = yield* Clock.currentTimeMillis;
          yield* installationRepo.revoke(
            makeOrganizationId("org-1"),
            revokedAt,
          );
          const maybeInstallation = yield* installationRepo.get(
            makeOrganizationId("org-1"),
          );
          Option.match(maybeInstallation, {
            onNone: () => expect.fail("installation not found"),
            onSome: (installation) =>
              expect(installation.revokedAt).not.toEqual(Option.none()),
          });
        }),
      ),
  );

  it.scopedLive(
    "updates permission snapshots without rewriting rotating tokens",
    () =>
      withRepos(
        Effect.gen(function* () {
          const repo = yield* InstallationRepo;
          yield* repo.put(
            makeInstallation({
              accessibleTeamIds: Option.some([makeTeamId("team-a")]),
            }),
          );

          const before = yield* repo.getRawEncryptedAccessToken(
            makeOrganizationId("org-1"),
          );

          const changed = yield* repo.applyPermissionChange(
            makeOrganizationId("org-1"),
            makeAppUserId("app-user-1"),
            [makeTeamId("team-b")],
            [makeTeamId("team-a")],
            false,
            0,
          );
          expect(changed).toBe(true);

          const after = yield* repo.getRawEncryptedAccessToken(
            makeOrganizationId("org-1"),
          );
          expect(after).toEqual(before);

          const maybeInstallation = yield* repo.get(
            makeOrganizationId("org-1"),
          );
          Option.match(maybeInstallation, {
            onNone: () => expect.fail("installation not found"),
            onSome: (installation) => {
              expect(installation.accessibleTeamIds).toEqual(
                Option.some([makeTeamId("team-b")]),
              );
            },
          });
        }),
      ),
  );

  it.scopedLive("reserves each projection and terminal outcome once", () =>
    withRepos(
      Effect.gen(function* () {
        const runRepo = yield* RunRepo;
        const projectionRepo = yield* ProjectionRepo;

        yield* runRepo.create({
          sessionId: makeSessionId("session-1"),
          organizationId: makeOrganizationId("org-1"),
          issueId: Option.none(),
        });

        const projection = {
          sourceKey: makeSourceKey("event-1"),
          sessionId: makeSessionId("session-1"),
          activityType: "response",
          payloadHash: "hash",
          payload: { body: "done" },
        };

        expect(yield* projectionRepo.enqueue(projection)).toBe(true);
        expect(yield* projectionRepo.enqueue(projection)).toBe(false);

        const job = yield* projectionRepo.claim(
          projection.sourceKey,
          "test-owner",
          60_000,
        );
        expect(job).not.toEqual(Option.none());

        yield* projectionRepo.complete(
          projection.sourceKey,
          "test-owner",
          Option.some(makeActivityId("linear-activity-1")),
        );
        expect(
          yield* projectionRepo.projectionCount(
            makeSessionId("session-1"),
            "response",
          ),
        ).toBe(1);
      }),
    ),
  );

  it.scopedLive(
    "persist inputs and projections as chronological run events",
    () =>
      withRepos(
        Effect.gen(function* () {
          const runRepo = yield* RunRepo;
          const inputRepo = yield* RunInputRepo;
          const projectionRepo = yield* ProjectionRepo;
          const eventRepo = yield* RunEventRepo;

          yield* runRepo.create({
            sessionId: makeSessionId("session-1"),
            organizationId: makeOrganizationId("org-1"),
            issueId: Option.none(),
          });

          yield* inputRepo.enqueue({
            id: makeInputId("input-1"),
            sessionId: makeSessionId("session-1"),
            kind: "created",
            body: "Implement the issue",
            payload: { agentSession: { issue: { teamId: "team" } } },
            createdAt: 1_000,
          });

          yield* projectionRepo.enqueue({
            sourceKey: makeSourceKey("event-1"),
            sessionId: makeSessionId("session-1"),
            activityType: "thought",
            payloadHash: "hash",
            payload: {
              request: { content: { type: "thought", body: "Thinking..." } },
            },
            now: 2_000,
          });

          const events = yield* eventRepo.list(makeSessionId("session-1"));
          expect(events.length).toBe(2);
          expect(events[0]).toMatchObject({
            sourceKey: makeSourceKey("input:input-1"),
            sessionId: makeSessionId("session-1"),
            kind: "input:created",
            level: "info",
            text: Option.some("Implement the issue"),
            status: Option.none(),
            createdAt: 1_000,
          });
          expect(events[1]).toMatchObject({
            sourceKey: makeSourceKey("event-1"),
            sessionId: makeSessionId("session-1"),
            kind: "thought",
            level: "debug",
            text: Option.some("Thinking..."),
            status: Option.some("pending"),
            createdAt: 2_000,
          });
        }),
      ),
  );

  it.scopedLive("records state transitions with a stable source key", () =>
    withRepos(
      Effect.gen(function* () {
        const runRepo = yield* RunRepo;
        const eventRepo = yield* RunEventRepo;

        yield* runRepo.create({
          sessionId: makeSessionId("session-transition"),
          organizationId: makeOrganizationId("org-1"),
          issueId: Option.none(),
        });
        yield* runRepo.update(makeSessionId("session-transition"), {
          state: "failed",
          terminalReason: Option.some("Worker exited"),
        });

        const events = yield* eventRepo.list(
          makeSessionId("session-transition"),
        );
        const transition = events.find((event) => event.kind === "state");
        expect(transition).toMatchObject({
          sourceKey: makeSourceKey("state:session-transition:0:failed"),
          level: "error",
          text: Option.some("queued → failed"),
          status: Option.some("observed"),
          error: Option.some("Worker exited"),
        });
      }),
    ),
  );

  it.scopedLive(
    "deduplicates run events by source key and preserves created at",
    () =>
      withRepos(
        Effect.gen(function* () {
          const runRepo = yield* RunRepo;
          const eventRepo = yield* RunEventRepo;

          yield* runRepo.create({
            sessionId: makeSessionId("session-1"),
            organizationId: makeOrganizationId("org-1"),
            issueId: Option.none(),
          });

          yield* eventRepo.upsert({
            sourceKey: makeSourceKey("rpc:session-1:1:agent_start"),
            sessionId: makeSessionId("session-1"),
            kind: "agent_start",
            level: "info",
            text: "Worker started",
            payload: { type: "agent_start" },
            status: "observed",
            now: 1_000,
          });
          yield* eventRepo.upsert({
            sourceKey: makeSourceKey("rpc:session-1:1:agent_start"),
            sessionId: makeSessionId("session-1"),
            kind: "thought",
            level: "debug",
            text: "Worker started",
            payload: {
              request: { content: { type: "thought", body: "Worker started" } },
            },
            status: "pending",
            now: 2_000,
          });

          const events = yield* eventRepo.list(makeSessionId("session-1"));
          expect(events.length).toBe(1);
          expect(events[0]).toMatchObject({
            sourceKey: makeSourceKey("rpc:session-1:1:agent_start"),
            kind: "thought",
            level: "debug",
            status: Option.some("pending"),
            createdAt: 1_000,
            updatedAt: 2_000,
          });
        }),
      ),
  );

  it.scopedLive("tracks projection lifecycle status and errors", () =>
    withRepos(
      Effect.gen(function* () {
        const runRepo = yield* RunRepo;
        const projectionRepo = yield* ProjectionRepo;
        const eventRepo = yield* RunEventRepo;

        yield* runRepo.create({
          sessionId: makeSessionId("session-1"),
          organizationId: makeOrganizationId("org-1"),
          issueId: Option.none(),
        });

        yield* projectionRepo.enqueue({
          sourceKey: makeSourceKey("event-1"),
          sessionId: makeSessionId("session-1"),
          activityType: "action",
          payloadHash: "hash",
          payload: {
            request: {
              content: {
                type: "action",
                action: "git",
                parameter: "status",
              },
            },
          },
        });
        const events1 = yield* eventRepo.list(makeSessionId("session-1"));
        expect(events1[0]?.status).toEqual(Option.some("pending"));

        const job = yield* projectionRepo.claim(
          makeSourceKey("event-1"),
          "owner",
          60_000,
        );
        expect(job).not.toEqual(Option.none());
        const events2 = yield* eventRepo.list(makeSessionId("session-1"));
        expect(events2[0]?.status).toEqual(Option.some("pending"));

        yield* projectionRepo.complete(
          makeSourceKey("event-1"),
          "owner",
          Option.some(makeActivityId("linear-1")),
        );
        const events3 = yield* eventRepo.list(makeSessionId("session-1"));
        const event1 = events3[0];
        expect(event1?.status).toEqual(Option.some("completed"));
        expect(event1?.error).toEqual(Option.none());

        yield* projectionRepo.enqueue({
          sourceKey: makeSourceKey("event-2"),
          sessionId: makeSessionId("session-1"),
          activityType: "response",
          payloadHash: "hash",
          payload: { request: { content: { type: "response", body: "Done" } } },
        });
        yield* projectionRepo.claim(makeSourceKey("event-2"), "owner", 60_000);
        yield* projectionRepo.fail(
          makeSourceKey("event-2"),
          "owner",
          "Linear API error",
          0,
        );

        const events4 = yield* eventRepo.list(makeSessionId("session-1"));
        const event2 = events4.find(
          (e) => e.sourceKey === makeSourceKey("event-2"),
        );
        expect(event2?.status).toEqual(Option.some("failed"));
        expect(event2?.error).toEqual(Option.some("Linear API error"));
      }),
    ),
  );

  it.scopedLive("does not overwrite completed run events on re-enqueue", () =>
    withRepos(
      Effect.gen(function* () {
        const runRepo = yield* RunRepo;
        const projectionRepo = yield* ProjectionRepo;
        const eventRepo = yield* RunEventRepo;

        yield* runRepo.create({
          sessionId: makeSessionId("session-1"),
          organizationId: makeOrganizationId("org-1"),
          issueId: Option.none(),
        });

        yield* projectionRepo.enqueue({
          sourceKey: makeSourceKey("terminal:session-1:stop"),
          sessionId: makeSessionId("session-1"),
          activityType: "response",
          payloadHash: "first-hash",
          payload: {
            request: {
              content: { type: "response", body: "Stopped as requested." },
            },
          },
          now: 1_000,
        });
        yield* projectionRepo.claim(
          makeSourceKey("terminal:session-1:stop"),
          "owner",
          60_000,
          1_000,
        );
        yield* projectionRepo.complete(
          makeSourceKey("terminal:session-1:stop"),
          "owner",
          Option.some(makeActivityId("linear-1")),
        );

        const requeued = yield* projectionRepo.enqueue({
          sourceKey: makeSourceKey("terminal:session-1:stop"),
          sessionId: makeSessionId("session-1"),
          activityType: "response",
          payloadHash: "other-hash",
          payload: {
            request: { content: { type: "response", body: "Other" } },
          },
          firstWriteWins: true,
          now: 2_000,
        });
        expect(requeued).toBe(false);

        const events = yield* eventRepo.list(makeSessionId("session-1"));
        const event = events[0];
        expect(event?.text).toEqual(Option.some("Stopped as requested."));
        expect(event?.status).toEqual(Option.some("completed"));
        expect(event?.createdAt).toBe(1_000);
        expect(event?.updatedAt).toBeGreaterThanOrEqual(2_000);
      }),
    ),
  );
});

describe("pure invariants", () => {
  it.scopedLive.prop(
    "DeliveryRepo.claim idempotency",
    {
      deliveryId: Schema.UUID,
      organizationId: Schema.UUID,
      payload: Schema.String,
      hashes: DistinctStrings,
    },
    ({ deliveryId, organizationId, payload, hashes: { first, second } }) =>
      withRepos(
        Effect.gen(function* () {
          const repo = yield* DeliveryRepo;
          const common = {
            id: makeDeliveryId(deliveryId),
            organizationId: makeOrganizationId(organizationId),
            payload,
          };
          expect(yield* repo.claim({ ...common, payloadHash: first })).toBe(
            "claimed",
          );
          expect(yield* repo.claim({ ...common, payloadHash: first })).toBe(
            "duplicate",
          );
          expect(yield* repo.claim({ ...common, payloadHash: second })).toBe(
            "conflict",
          );
        }),
      ),
  );

  it.scopedLive.prop(
    "RunRepo.claimLease exclusivity",
    {
      sessionId: Schema.UUID,
      organizationId: Schema.UUID,
      owner: Schema.String.pipe(Schema.minLength(1)),
      otherOwner: Schema.String.pipe(Schema.minLength(1)),
      duration: Schema.Number.pipe(Schema.int(), Schema.between(2, 1_000_000)),
      now: Schema.Number.pipe(Schema.int(), Schema.between(0, 1_000_000)),
    },
    ({ sessionId, organizationId, owner, otherOwner, duration, now }) =>
      withRepos(
        Effect.gen(function* () {
          const repo = yield* RunRepo;
          yield* repo.create({
            sessionId: makeSessionId(sessionId),
            organizationId: makeOrganizationId(organizationId),
            issueId: Option.none(),
            now,
          });
          expect(
            yield* repo.claimLease(
              makeSessionId(sessionId),
              owner,
              duration,
              now,
            ),
          ).toBe(true);
          expect(
            yield* repo.claimLease(
              makeSessionId(sessionId),
              owner,
              duration,
              now + 1,
            ),
          ).toBe(true);
          const other = yield* repo.claimLease(
            makeSessionId(sessionId),
            otherOwner,
            duration,
            now + 1,
          );
          if (owner === otherOwner) {
            expect(other).toBe(true);
          } else {
            expect(other).toBe(false);
          }
        }),
      ),
  );

  it.scopedLive.prop(
    "RunEventRepo.upsert source-key uniqueness",
    {
      sessionId: Schema.UUID,
      organizationId: Schema.UUID,
      sourceKey: Schema.UUID,
      texts: NonEmptyDistinctStrings,
      bodies: NonEmptyDistinctStrings,
    },
    ({
      sessionId,
      organizationId,
      sourceKey,
      texts: { first: text1, second: text2 },
      bodies: { first: body1, second: body2 },
    }) =>
      withRepos(
        Effect.gen(function* () {
          const runRepo = yield* RunRepo;
          const eventRepo = yield* RunEventRepo;

          yield* runRepo.create({
            sessionId: makeSessionId(sessionId),
            organizationId: makeOrganizationId(organizationId),
            issueId: Option.none(),
            now: 0,
          });

          const key = makeSourceKey(sourceKey);

          yield* eventRepo.upsert({
            sourceKey: key,
            sessionId: makeSessionId(sessionId),
            kind: "agent_start",
            level: "info",
            text: text1,
            payload: { body: body1 },
            status: "pending",
            now: 1_000,
          });
          yield* eventRepo.upsert({
            sourceKey: key,
            sessionId: makeSessionId(sessionId),
            kind: "thought",
            level: "debug",
            text: text2,
            payload: { body: body2 },
            status: "pending",
            now: 2_000,
          });

          const events = yield* eventRepo.list(makeSessionId(sessionId));
          expect(events.length).toBe(1);
          expect(events[0]).toMatchObject({
            sourceKey: key,
            kind: "thought",
            level: "debug",
            text: Option.some(text2),
            status: Option.some("pending"),
            createdAt: 1_000,
            updatedAt: 2_000,
          });

          yield* eventRepo.upsert({
            sourceKey: key,
            sessionId: makeSessionId(sessionId),
            kind: "thought",
            level: "debug",
            text: text2,
            payload: { body: body2 },
            status: "completed",
            now: 3_000,
          });
          yield* eventRepo.upsert({
            sourceKey: key,
            sessionId: makeSessionId(sessionId),
            kind: "response",
            level: "result",
            text: "overwritten",
            payload: { body: "overwritten" },
            status: "pending",
            now: 4_000,
          });

          const events2 = yield* eventRepo.list(makeSessionId(sessionId));
          expect(events2.length).toBe(1);
          expect(events2[0]).toMatchObject({
            sourceKey: key,
            kind: "thought",
            level: "debug",
            text: Option.some(text2),
            status: Option.some("completed"),
            createdAt: 1_000,
            updatedAt: 3_000,
          });
        }),
      ),
  );

  it.scopedLive.prop(
    "ProjectionRepo.enqueue firstWriteWins",
    {
      sessionId: Schema.UUID,
      organizationId: Schema.UUID,
      sourceKey: Schema.UUID,
      bodies: NonEmptyDistinctStrings,
      hashes: DistinctStrings,
      activityId: Schema.UUID,
    },
    ({
      sessionId,
      organizationId,
      sourceKey,
      bodies: { first: body, second: otherBody },
      hashes: { first: firstHash, second: secondHash },
      activityId,
    }) =>
      withRepos(
        Effect.gen(function* () {
          const runRepo = yield* RunRepo;
          const projectionRepo = yield* ProjectionRepo;
          const eventRepo = yield* RunEventRepo;

          yield* runRepo.create({
            sessionId: makeSessionId(sessionId),
            organizationId: makeOrganizationId(organizationId),
            issueId: Option.none(),
            now: 0,
          });

          const key = makeSourceKey(sourceKey);

          const first = yield* projectionRepo.enqueue({
            sourceKey: key,
            sessionId: makeSessionId(sessionId),
            activityType: "response",
            payloadHash: firstHash,
            payload: { request: { content: { type: "response", body } } },
            now: 1_000,
          });
          expect(first).toBe(true);

          const sameHash = yield* projectionRepo.enqueue({
            sourceKey: key,
            sessionId: makeSessionId(sessionId),
            activityType: "response",
            payloadHash: firstHash,
            payload: { request: { content: { type: "response", body } } },
            now: 1_000,
          });
          expect(sameHash).toBe(false);

          const job = yield* projectionRepo.claim(key, "owner", 60_000);
          expect(job).not.toEqual(Option.none());

          yield* projectionRepo.complete(
            key,
            "owner",
            Option.some(makeActivityId(activityId)),
          );

          const differentHash = yield* projectionRepo.enqueue({
            sourceKey: key,
            sessionId: makeSessionId(sessionId),
            activityType: "response",
            payloadHash: secondHash,
            payload: {
              request: { content: { type: "response", body: otherBody } },
            },
            now: 2_000,
            firstWriteWins: true,
          });
          expect(differentHash).toBe(false);

          const events = yield* eventRepo.list(makeSessionId(sessionId));
          const event = events[0];
          expect(event?.text).toEqual(Option.some(body));
          expect(event?.status).toEqual(Option.some("completed"));
        }),
      ),
  );

  it.scopedLive.prop(
    "RunRepo.update rejects transitions from terminal states",
    {
      sessionId: Schema.UUID,
      organizationId: Schema.UUID,
      firstState: RunState,
      secondState: RunState,
    },
    ({ sessionId, organizationId, firstState, secondState }) =>
      withRepos(
        Effect.gen(function* () {
          const repo = yield* RunRepo;
          const sid = makeSessionId(sessionId);
          const oid = makeOrganizationId(organizationId);

          yield* repo.create({
            sessionId: sid,
            organizationId: oid,
            issueId: Option.none(),
            now: 0,
          });

          yield* repo.update(sid, { state: firstState });
          const first = yield* repo.get(sid);
          expect(Option.isSome(first)).toBe(true);
          if (Option.isSome(first)) {
            expect(first.value.state).toBe(firstState);
          }

          const either = yield* Effect.either(
            repo.update(sid, { state: secondState }),
          );
          const terminal = ["succeeded", "failed", "canceled"] as const;
          const isTerminal = terminal.includes(
            firstState as (typeof terminal)[number],
          );
          const shouldFail = isTerminal && secondState !== firstState;

          if (shouldFail) {
            expect(Either.isLeft(either)).toBe(true);
            if (Either.isLeft(either)) {
              expect(either.left.message).toBe(
                "Terminal run state is immutable",
              );
            }
          } else {
            expect(Either.isRight(either)).toBe(true);
            const after = yield* repo.get(sid);
            expect(Option.isSome(after)).toBe(true);
            if (Option.isSome(after)) {
              expect(after.value.state).toBe(secondState);
            }
          }
        }),
      ),
  );
  it.scopedLive.prop(
    "InstallationRepo persists generated installation records and replaces them",
    {
      organizationId: Schema.UUID,
      appUserId: Schema.UUID,
      accessToken: Schema.String.pipe(Schema.minLength(1)),
      refreshToken: Schema.String.pipe(Schema.minLength(1)),
      expiresAt: Schema.Number.pipe(Schema.int(), Schema.between(0, 1_000_000_000)),
      scopes: Schema.Array(Schema.String),
      revokedAt: Schema.NullOr(
        Schema.Number.pipe(Schema.int(), Schema.between(0, 1_000_000_000)),
      ),
      accessibleTeamIds: Schema.NullOr(Schema.Array(Schema.UUID)),
      canAccessAllPublicTeams: Schema.NullOr(Schema.Boolean),
    },
    (input) =>
      withRepos(
        Effect.gen(function* () {
          const repo = yield* InstallationRepo;
          const expected: Installation = {
            organizationId: makeOrganizationId(input.organizationId),
            appUserId: makeAppUserId(input.appUserId),
            accessToken: input.accessToken,
            refreshToken: input.refreshToken,
            expiresAt: input.expiresAt,
            scopes: input.scopes,
            revokedAt:
              input.revokedAt === null
                ? Option.none()
                : Option.some(input.revokedAt),
            accessibleTeamIds:
              input.accessibleTeamIds === null
                ? Option.none()
                : Option.some(input.accessibleTeamIds.map(makeTeamId)),
            canAccessAllPublicTeams:
              input.canAccessAllPublicTeams === null
                ? Option.none()
                : Option.some(input.canAccessAllPublicTeams),
          };
          yield* repo.put(expected);
          expect(yield* repo.get(expected.organizationId)).toEqual(
            Option.some(expected),
          );

          const replacement = { ...expected, accessToken: `${input.accessToken}-replacement` };
          yield* repo.put(replacement);
          expect(yield* repo.get(expected.organizationId)).toEqual(
            Option.some(replacement),
          );
        }),
      ),
  );

  it.scopedLive.prop(
    "RunInputRepo keeps generated inputs pending exactly once until processed",
    {
      sessionId: Schema.UUID,
      organizationId: Schema.UUID,
      inputId: Schema.UUID,
      kind: Schema.Literal("created", "prompted", "stop"),
      body: Schema.String,
      marker: Schema.String,
      createdAt: Schema.Number.pipe(Schema.int(), Schema.between(0, 1_000_000_000)),
    },
    ({ sessionId, organizationId, inputId, kind, body, marker, createdAt }) =>
      withRepos(
        Effect.gen(function* () {
          const runRepo = yield* RunRepo;
          const repo = yield* RunInputRepo;
          const sid = makeSessionId(sessionId);
          yield* runRepo.create({
            sessionId: sid,
            organizationId: makeOrganizationId(organizationId),
            issueId: Option.none(),
            now: createdAt,
          });

          const input = {
            id: makeInputId(inputId),
            sessionId: sid,
            kind,
            body,
            payload: { marker },
            createdAt,
          };
          expect(yield* repo.enqueue(input)).toBe(true);
          expect(yield* repo.enqueue(input)).toBe(false);

          const pending = yield* repo.pending(sid);
          expect(pending).toHaveLength(1);
          expect(pending[0]).toEqual(input);
          expect(yield* repo.listSessionsWithPendingInputs()).toContain(sid);

          const latest = yield* repo.latestActionableInput(sid);
          if (kind === "stop") {
            expect(latest).toEqual(Option.none());
          } else {
            expect(latest).toEqual(Option.some({ body, kind }));
          }

          yield* repo.markProcessed(input.id, createdAt + 1);
          expect(yield* repo.pending(sid)).toEqual([]);
        }),
      ),
  );

  it.scopedLive.prop(
    "AdminSessionRepo enforces inclusive expiry and single deletion",
    {
      organizationId: Schema.UUID,
      tokenHash: Schema.String.pipe(Schema.minLength(1)),
      csrfTokenHash: Schema.String.pipe(Schema.minLength(1)),
      now: Schema.Number.pipe(Schema.int(), Schema.between(0, 1_000_000_000)),
      ttl: Schema.Number.pipe(Schema.int(), Schema.between(1, 1_000_000)),
    },
    ({ organizationId, tokenHash, csrfTokenHash, now, ttl }) =>
      withRepos(
        Effect.gen(function* () {
          const repo = yield* AdminSessionRepo;
          const organization = makeOrganizationId(organizationId);
          const expiresAt = now + ttl;
          yield* repo.create({
            organizationId: organization,
            tokenHash,
            csrfTokenHash,
            expiresAt,
            now,
          });

          expect(yield* repo.get(tokenHash, now)).toEqual(
            Option.some({ organizationId: organization, csrfTokenHash }),
          );
          expect(yield* repo.get(tokenHash, expiresAt)).toEqual(
            Option.some({ organizationId: organization, csrfTokenHash }),
          );
          expect(yield* repo.get(tokenHash, expiresAt + 1)).toEqual(
            Option.none(),
          );
          expect(yield* repo.deleteAdminSession(tokenHash)).toBe(true);
          expect(yield* repo.deleteAdminSession(tokenHash)).toBe(false);
          expect(yield* repo.get(tokenHash, now)).toEqual(Option.none());
        }),
      ),
  );

});
