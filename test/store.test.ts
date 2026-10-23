import { Database } from "bun:sqlite";
import { it } from "@effect/vitest";
import {
  Clock,
  ConfigProvider,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Result,
  Schema,
} from "effect";
import * as fc from "fast-check";
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
import {
  SqliteClient,
  SqliteClientLive,
  transact,
  tryDb,
} from "../src/services/store/sqlite-client.js";
import { TokenCrypto } from "../src/services/token-crypto.js";

import { suitePaths } from "./fixtures/paths.js";

const storeSuitePath = suitePaths();

const testKeyBase64 = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=";
const configProvider = ConfigProvider.fromUnknown(
  Object.fromEntries(
    new Map([
      ["LINEAR_CLIENT_ID", "test"],
      ["LINEAR_CLIENT_SECRET", "test"],
      ["LINEAR_WEBHOOK_SECRET", "test"],
      ["PUBLIC_URL", "http://localhost:3000"],
      ["TOKEN_ENCRYPTION_KEY", testKeyBase64],
    ]),
  ),
  { preserveEmptyStrings: true },
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
      AdminSessionRepo.layer,
      InstallationRepo.layer,
      DeliveryRepo.layer,
      RunRepo.layer,
      RunInputRepo.layer,
      RunEventRepo.layer,
      ProjectionRepo.layer,
      TokenCrypto.layer,
    ).pipe(
      Layer.provide(
        Layer.mergeAll(sqlite, ConfigProvider.layer(configProvider)),
      ),
    );
    return yield* effect.pipe(Effect.provide(repos));
  });

describe("SQLite transactions", () => {
  it.live("rolls back when the transaction fiber is interrupted", () =>
    Effect.gen(function* () {
      const db = yield* Effect.acquireRelease(
        Effect.sync(() => new Database(":memory:")),
        (database) => Effect.sync(() => database.close()),
      );
      yield* Effect.sync(() => db.exec("CREATE TABLE item (value TEXT)"));
      const entered = yield* Deferred.make<void>();
      const fiber = yield* Effect.forkChild(
        transact(
          db,
          Effect.gen(function* () {
            yield* Deferred.succeed(entered, undefined);
            return yield* Effect.never;
          }),
        ),
      );
      yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);

      yield* transact(
        db,
        Effect.sync(() => db.exec("INSERT INTO item VALUES ('usable')")),
      );
      const row = db.query("SELECT COUNT(*) AS count FROM item").get() as {
        count: number;
      };
      expect(row.count).toBe(1);
    }),
  );
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

const _DistinctStrings = Schema.Struct({
  first: Schema.String,
  second: Schema.String,
}).pipe(Schema.check(Schema.makeFilter((s) => s.first !== s.second)));

const _NonEmptyDistinctStrings = Schema.Struct({
  first: Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
  second: Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
}).pipe(Schema.check(Schema.makeFilter((s) => s.first !== s.second)));

it("rolls back after a commit failure so the next write can proceed", async () => {
  const statements: string[] = [];
  let failCommit = true;
  const db = {
    exec: (statement: string) => {
      statements.push(statement);
      if (statement === "COMMIT" && failCommit) {
        failCommit = false;
        throw new Error("busy");
      }
    },
  } as unknown as Database;

  const first = await Effect.runPromiseExit(
    transact(db, Effect.succeed("first")),
  );
  expect(Exit.isFailure(first)).toBe(true);
  await Effect.runPromise(transact(db, Effect.succeed("second")));
  expect(statements).toEqual([
    "BEGIN IMMEDIATE",
    "COMMIT",
    "ROLLBACK",
    "BEGIN IMMEDIATE",
    "COMMIT",
  ]);
});

describe("Store repositories", () => {
  it.live("encrypts tokens at rest and round-trips installations", () =>
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

  it.live("consumes OAuth state exactly once before expiry", () =>
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

  it.live("deduplicates webhook delivery IDs", () =>
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

  it.live("reclaims a delivery interrupted before durable processing", () =>
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

  it.live("enforces one lease and permits takeover only after expiry", () =>
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
  it.live(
    "catch-up candidates include live runs despite leases and retry delays",
    () =>
      withRepos(
        Effect.gen(function* () {
          const repo = yield* RunRepo;
          const sessionId = makeSessionId("catchup-leased");
          yield* repo.create({
            sessionId,
            organizationId: makeOrganizationId("org-1"),
            issueId: Option.none(),
            now: 1_000,
          });
          yield* repo.update(sessionId, {
            state: "running",
            nextAttemptAt: Option.some(9_000),
          });
          expect(
            yield* repo.claimLease(sessionId, "worker-a", 60_000, 1_000),
          ).toBe(true);

          const candidates = yield* repo.listCatchupCandidates(2_000);
          expect(candidates.map((run) => run.sessionId)).toEqual([sessionId]);
        }),
      ),
  );
  it.live("keeps canceled catch-up candidates for seven days", () =>
    withRepos(
      Effect.gen(function* () {
        const repo = yield* RunRepo;
        const sessionId = makeSessionId("catchup-canceled");
        yield* repo.create({
          sessionId,
          organizationId: makeOrganizationId("org-1"),
          issueId: Option.none(),
          now: 0,
        });
        yield* repo.update(sessionId, { state: "canceled" });
        const updatedAt = yield* Clock.currentTimeMillis;
        const sixDays = 6 * 24 * 60 * 60_000;
        const eightDays = 8 * 24 * 60 * 60_000;
        expect(
          (yield* repo.listCatchupCandidates(updatedAt + sixDays)).some(
            (run) => run.sessionId === sessionId,
          ),
        ).toBe(true);
        expect(
          (yield* repo.listCatchupCandidates(updatedAt + eightDays)).some(
            (run) => run.sessionId === sessionId,
          ),
        ).toBe(false);
      }),
    ),
  );

  it.live("startup preserves another gateway's live lease", () =>
    withRepos(
      Effect.gen(function* () {
        const repo = yield* RunRepo;
        const sid = makeSessionId("live-gateway");
        yield* repo.create({
          sessionId: sid,
          organizationId: makeOrganizationId("org"),
          issueId: Option.none(),
          now: 1000,
        });
        yield* repo.update(sid, { state: "starting" });
        yield* repo.claimLease(sid, "live-owner", 60000, 1000);
        expect(yield* repo.recoverInterruptedRuns(2000)).toBe(0);
        const preserved = Option.getOrThrow(yield* repo.get(sid));
        expect(preserved.state).toBe("starting");
        expect(preserved.leaseOwner).toEqual(Option.some("live-owner"));
        expect(yield* repo.claimLease(sid, "new-owner", 60000, 2000)).toBe(
          false,
        );
        expect(yield* repo.recoverInterruptedRuns(61001)).toBe(1);
        expect(yield* repo.claimLease(sid, "new-owner", 60000, 61001)).toBe(
          true,
        );
      }),
    ),
  );

  it.live("recovers active runs and leases after a process restart", () =>
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
            500,
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

  it.live(
    "prompts after stop are accepted for resume and revocation cancels all live runs",
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
          // A follow-up prompt is no longer rejected at enqueue time; the
          // session authority decides between honoring the cancellation and
          // resuming when it processes the input.
          expect(
            yield* inputRepo.enqueue({
              id: makeInputId("late"),
              sessionId: makeSessionId("session-1"),
              kind: "prompted",
              body: "keep going",
              payload: {},
            }),
          ).toBe(true);

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

  it.live(
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

  it.live("reserves each projection and terminal outcome once", () =>
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

  it.live("persist inputs and projections as chronological run events", () =>
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

  it.live("records state transitions with a stable source key", () =>
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

  it.live(
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

  it.live(
    "reads one event by source key and seeds RPC sequence numerically within a session",
    () =>
      withRepos(
        Effect.gen(function* () {
          const sid = makeSessionId("targeted-events-🚀");
          const other = makeSessionId("targeted-events-other");
          for (const sessionId of [sid, other])
            yield* RunRepo.use((service) =>
              service.create({
                sessionId,
                organizationId: makeOrganizationId("org-1"),
                issueId: Option.none(),
              }),
            );
          expect(
            yield* RunEventRepo.use((service) => service.maxRpcSequence(sid)),
          ).toBe(0);
          const key = makeSourceKey(`retry:${sid}:3`);
          yield* RunEventRepo.use((service) =>
            service.upsert({
              sourceKey: key,
              sessionId: sid,
              kind: "retry",
              level: "info",
              payload: { nextAttemptAt: 900, scheduledAt: 500 },
            }),
          );
          const event = Option.getOrThrow(
            yield* RunEventRepo.use((service) => service.getBySourceKey(key)),
          );
          expect(event.sourceKey).toBe(key);
          expect(event.payload).toEqual({
            nextAttemptAt: 900,
            scheduledAt: 500,
          });
          expect(
            yield* RunEventRepo.use((service) =>
              service.getBySourceKey(makeSourceKey("absent")),
            ),
          ).toEqual(Option.none());
          for (const sourceKey of [
            `rpc:${sid}:2:progress`,
            `rpc:${sid}:10:progress`,
            `rpc:${sid}:9:progress`,
            `rpc:${sid}:99garbage:progress`,
            `rpc:${sid}:9007199254740992:progress`,
            `rpc:${sid}:500`,
            `rpc:${sid}:1e4:progress`,
            `rpc:${sid}extra:1000:progress`,
          ])
            yield* RunEventRepo.use((service) =>
              service.upsert({
                sourceKey: makeSourceKey(sourceKey),
                sessionId: sid,
                kind: "progress",
                level: "info",
              }),
            );
          yield* RunEventRepo.use((service) =>
            service.upsert({
              sourceKey: makeSourceKey(`rpc:${other}:2000:progress`),
              sessionId: other,
              kind: "progress",
              level: "info",
            }),
          );
          expect(
            yield* RunEventRepo.use((service) => service.maxRpcSequence(sid)),
          ).toBe(10);
          expect(
            yield* RunEventRepo.use((service) => service.maxRpcSequence(other)),
          ).toBe(2000);
        }),
      ),
  );

  it.live("tracks projection lifecycle status and errors", () =>
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

  it.live("does not overwrite completed run events on re-enqueue", () =>
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
  it.live(
    "DeliveryRepo.claim idempotency",
    () =>
      Effect.gen(function* () {
        const cases = fc.record({
          deliveryId: fc.uuid(),
          organizationId: fc.uuid(),
          payload: fc.string(),
          hashes: fc.record({ first: fc.string(), second: fc.string() }).filter(
            Schema.is(
              Schema.Struct({
                first: Schema.String,
                second: Schema.String,
              }).pipe(
                Schema.check(Schema.makeFilter((s) => s.first !== s.second)),
              ),
            ),
          ),
        });
        const check = ({
          deliveryId,
          organizationId,
          payload,
          hashes: { first, second },
        }: typeof cases extends fc.Arbitrary<infer A> ? A : never) =>
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
              expect(
                yield* repo.claim({ ...common, payloadHash: second }),
              ).toBe("conflict");
            }),
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
    { timeout: 15_000 },
  );

  it.live(
    "RunRepo.claimLease exclusivity",
    () =>
      Effect.gen(function* () {
        const cases = fc.record({
          sessionId: fc.uuid(),
          organizationId: fc.uuid(),
          owner: fc
            .string()
            .filter(
              Schema.is(
                Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
              ),
            ),
          otherOwner: fc
            .string()
            .filter(
              Schema.is(
                Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
              ),
            ),
          duration: fc
            .integer({ min: 2, max: 1_000_000 })
            .filter(
              Schema.is(
                Schema.Number.pipe(
                  Schema.check(Schema.isInt()),
                  Schema.check(
                    Schema.isBetween({ minimum: 2, maximum: 1_000_000 }),
                  ),
                ),
              ),
            ),
          now: fc
            .integer({ min: 0, max: 1_000_000 })
            .filter(
              Schema.is(
                Schema.Number.pipe(
                  Schema.check(Schema.isInt()),
                  Schema.check(
                    Schema.isBetween({ minimum: 0, maximum: 1_000_000 }),
                  ),
                ),
              ),
            ),
        });
        const check = ({
          sessionId,
          organizationId,
          owner,
          otherOwner,
          duration,
          now,
        }: typeof cases extends fc.Arbitrary<infer A> ? A : never) =>
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
    { timeout: 15_000 },
  );

  it.live(
    "RunEventRepo.upsert source-key uniqueness",
    () =>
      Effect.gen(function* () {
        const cases = fc.record({
          sessionId: fc.uuid(),
          organizationId: fc.uuid(),
          sourceKey: fc.uuid(),
          texts: fc
            .record({
              first: fc
                .string()
                .filter(
                  Schema.is(
                    Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
                  ),
                ),
              second: fc
                .string()
                .filter(
                  Schema.is(
                    Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
                  ),
                ),
            })
            .filter(
              Schema.is(
                Schema.Struct({
                  first: Schema.String.pipe(
                    Schema.check(Schema.isMinLength(1)),
                  ),
                  second: Schema.String.pipe(
                    Schema.check(Schema.isMinLength(1)),
                  ),
                }).pipe(
                  Schema.check(Schema.makeFilter((s) => s.first !== s.second)),
                ),
              ),
            ),
          bodies: fc
            .record({
              first: fc
                .string()
                .filter(
                  Schema.is(
                    Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
                  ),
                ),
              second: fc
                .string()
                .filter(
                  Schema.is(
                    Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
                  ),
                ),
            })
            .filter(
              Schema.is(
                Schema.Struct({
                  first: Schema.String.pipe(
                    Schema.check(Schema.isMinLength(1)),
                  ),
                  second: Schema.String.pipe(
                    Schema.check(Schema.isMinLength(1)),
                  ),
                }).pipe(
                  Schema.check(Schema.makeFilter((s) => s.first !== s.second)),
                ),
              ),
            ),
        });
        const check = ({
          sessionId,
          organizationId,
          sourceKey,
          texts: { first: text1, second: text2 },
          bodies: { first: body1, second: body2 },
        }: typeof cases extends fc.Arbitrary<infer A> ? A : never) =>
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
    { timeout: 15_000 },
  );

  it.live(
    "ProjectionRepo.enqueue firstWriteWins",
    () =>
      Effect.gen(function* () {
        const cases = fc.record({
          sessionId: fc.uuid(),
          organizationId: fc.uuid(),
          sourceKey: fc.uuid(),
          bodies: fc
            .record({
              first: fc
                .string()
                .filter(
                  Schema.is(
                    Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
                  ),
                ),
              second: fc
                .string()
                .filter(
                  Schema.is(
                    Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
                  ),
                ),
            })
            .filter(
              Schema.is(
                Schema.Struct({
                  first: Schema.String.pipe(
                    Schema.check(Schema.isMinLength(1)),
                  ),
                  second: Schema.String.pipe(
                    Schema.check(Schema.isMinLength(1)),
                  ),
                }).pipe(
                  Schema.check(Schema.makeFilter((s) => s.first !== s.second)),
                ),
              ),
            ),
          hashes: fc.record({ first: fc.string(), second: fc.string() }).filter(
            Schema.is(
              Schema.Struct({
                first: Schema.String,
                second: Schema.String,
              }).pipe(
                Schema.check(Schema.makeFilter((s) => s.first !== s.second)),
              ),
            ),
          ),
          activityId: fc.uuid(),
        });
        const check = ({
          sessionId,
          organizationId,
          sourceKey,
          bodies: { first: body, second: otherBody },
          hashes: { first: firstHash, second: secondHash },
          activityId,
        }: typeof cases extends fc.Arbitrary<infer A> ? A : never) =>
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
    { timeout: 15_000 },
  );

  it.live(
    "RunRepo.update rejects transitions from terminal states",
    () =>
      Effect.gen(function* () {
        const cases = fc.record({
          sessionId: fc.uuid(),
          organizationId: fc.uuid(),
          firstState: fc.constantFrom(...RunState.literals),
          secondState: fc.constantFrom(...RunState.literals),
        });
        const check = ({
          sessionId,
          organizationId,
          firstState,
          secondState,
        }: typeof cases extends fc.Arbitrary<infer A> ? A : never) =>
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

              const either = yield* Effect.result(
                repo.update(sid, { state: secondState }),
              );
              const terminal = ["succeeded", "failed", "canceled"] as const;
              const isTerminal = terminal.includes(
                firstState as (typeof terminal)[number],
              );
              const shouldFail = isTerminal && secondState !== firstState;

              if (shouldFail) {
                expect(Result.isFailure(either)).toBe(true);
                if (Result.isFailure(either)) {
                  expect(either.failure.message).toBe(
                    "Terminal run state is immutable",
                  );
                }
              } else {
                expect(Result.isSuccess(either)).toBe(true);
                const after = yield* repo.get(sid);
                expect(Option.isSome(after)).toBe(true);
                if (Option.isSome(after)) {
                  expect(after.value.state).toBe(secondState);
                }
              }
            }),
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
    { timeout: 15_000 },
  );
  it.live(
    "InstallationRepo persists generated installation records and replaces them",
    () =>
      Effect.gen(function* () {
        const cases = fc.record({
          organizationId: fc.uuid(),
          appUserId: fc.uuid(),
          accessToken: fc
            .string()
            .filter(
              Schema.is(
                Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
              ),
            ),
          refreshToken: fc
            .string()
            .filter(
              Schema.is(
                Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
              ),
            ),
          expiresAt: fc
            .integer({ min: 0, max: 1_000_000_000 })
            .filter(
              Schema.is(
                Schema.Number.pipe(
                  Schema.check(Schema.isInt()),
                  Schema.check(
                    Schema.isBetween({ minimum: 0, maximum: 1_000_000_000 }),
                  ),
                ),
              ),
            ),
          scopes: fc.array(fc.string()),
          revokedAt: fc.option(
            fc
              .integer({ min: 0, max: 1_000_000_000 })
              .filter(
                Schema.is(
                  Schema.Number.pipe(
                    Schema.check(Schema.isInt()),
                    Schema.check(
                      Schema.isBetween({ minimum: 0, maximum: 1_000_000_000 }),
                    ),
                  ),
                ),
              ),
            { nil: null },
          ),
          accessibleTeamIds: fc.option(fc.array(fc.uuid()), { nil: null }),
          canAccessAllPublicTeams: fc.option(fc.boolean(), { nil: null }),
        });
        const check = (
          input: typeof cases extends fc.Arbitrary<infer A> ? A : never,
        ) =>
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

              const replacement = {
                ...expected,
                accessToken: `${input.accessToken}-replacement`,
              };
              yield* repo.put(replacement);
              expect(yield* repo.get(expected.organizationId)).toEqual(
                Option.some(replacement),
              );
            }),
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
    { timeout: 15_000 },
  );

  it.live(
    "RunInputRepo keeps generated inputs pending exactly once until processed",
    () =>
      Effect.gen(function* () {
        const cases = fc.record({
          sessionId: fc.uuid(),
          organizationId: fc.uuid(),
          inputId: fc.uuid(),
          kind: fc.constantFrom(...(["created", "prompted", "stop"] as const)),
          body: fc.string(),
          marker: fc.string(),
          createdAt: fc
            .integer({ min: 0, max: 1_000_000_000 })
            .filter(
              Schema.is(
                Schema.Number.pipe(
                  Schema.check(Schema.isInt()),
                  Schema.check(
                    Schema.isBetween({ minimum: 0, maximum: 1_000_000_000 }),
                  ),
                ),
              ),
            ),
        });
        const check = ({
          sessionId,
          organizationId,
          inputId,
          kind,
          body,
          marker,
          createdAt,
        }: typeof cases extends fc.Arbitrary<infer A> ? A : never) =>
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
              expect(yield* repo.listSessionsWithPendingInputs()).toContain(
                sid,
              );

              const latest = yield* repo.latestActionableInput(sid);
              if (kind === "stop") {
                expect(latest).toEqual(Option.none());
              } else {
                expect(latest).toEqual(
                  Option.some({ body, kind, payload: { marker } }),
                );
              }

              yield* repo.markProcessed(input.id, createdAt + 1);
              expect(yield* repo.pending(sid)).toEqual([]);
            }),
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
    { timeout: 15_000 },
  );

  it.live(
    "AdminSessionRepo enforces inclusive expiry and single deletion",
    () =>
      Effect.gen(function* () {
        const cases = fc.record({
          organizationId: fc.uuid(),
          tokenHash: fc
            .string()
            .filter(
              Schema.is(
                Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
              ),
            ),
          csrfTokenHash: fc
            .string()
            .filter(
              Schema.is(
                Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
              ),
            ),
          now: fc
            .integer({ min: 0, max: 1_000_000_000 })
            .filter(
              Schema.is(
                Schema.Number.pipe(
                  Schema.check(Schema.isInt()),
                  Schema.check(
                    Schema.isBetween({ minimum: 0, maximum: 1_000_000_000 }),
                  ),
                ),
              ),
            ),
          ttl: fc
            .integer({ min: 1, max: 1_000_000 })
            .filter(
              Schema.is(
                Schema.Number.pipe(
                  Schema.check(Schema.isInt()),
                  Schema.check(
                    Schema.isBetween({ minimum: 1, maximum: 1_000_000 }),
                  ),
                ),
              ),
            ),
        });
        const check = ({
          organizationId,
          tokenHash,
          csrfTokenHash,
          now,
          ttl,
        }: typeof cases extends fc.Arbitrary<infer A> ? A : never) =>
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
    { timeout: 15_000 },
  );
});

describe("Lease-fenced run writes", () => {
  it.live(
    "rejects completion and metadata writes from a replaced lease owner",
    () =>
      withRepos(
        Effect.gen(function* () {
          const repo = yield* RunRepo;
          const sid = makeSessionId("lease-fence");
          const now = yield* Clock.currentTimeMillis;
          yield* repo.create({
            sessionId: sid,
            organizationId: makeOrganizationId("org"),
            issueId: Option.none(),
          });
          yield* repo.claimLease(sid, "old-owner", 10, now - 100);
          expect(yield* repo.claimLease(sid, "new-owner", 60_000, now)).toBe(
            true,
          );
          for (const patch of [
            { state: "succeeded" as const },
            { ompSessionId: Option.some("stale-session") },
          ]) {
            const result = yield* repo
              .update(sid, { ...patch, leaseOwner: "old-owner" })
              .pipe(Effect.result);
            expect(Result.isFailure(result)).toBe(true);
          }
          const run = Option.getOrThrow(yield* repo.get(sid));
          expect(run.state).toBe("queued");
          expect(run.ompSessionId).toEqual(Option.none());
          expect(
            yield* RunEventRepo.use((service) => service.list(sid)),
          ).toEqual([]);
          yield* repo.update(sid, {
            leaseOwner: "new-owner",
            state: "succeeded",
          });
          expect(Option.getOrThrow(yield* repo.get(sid)).state).toBe(
            "succeeded",
          );
        }),
      ),
  );

  it.live("rejects writes after expiry even before another owner claims", () =>
    withRepos(
      Effect.gen(function* () {
        const sid = makeSessionId("expired-write");
        yield* RunRepo.use((service) =>
          service.create({
            sessionId: sid,
            organizationId: makeOrganizationId("org"),
            issueId: Option.none(),
          }),
        );
        const now = yield* Clock.currentTimeMillis;
        yield* RunRepo.use((service) =>
          service.claimLease(sid, "expired-owner", 10, now - 100),
        );
        expect(
          Result.isFailure(
            yield* RunRepo.use((service) =>
              service.update(sid, {
                leaseOwner: "expired-owner",
                state: "succeeded",
              }),
            ).pipe(Effect.result),
          ),
        ).toBe(true);
      }),
    ),
  );
});

it.live(
  "nested repository writes roll back together and exclude unrelated fibers",
  () =>
    Effect.gen(function* () {
      const db = yield* Effect.acquireRelease(
        Effect.sync(() => new Database(":memory:")),
        (database) => Effect.sync(() => database.close()),
      );
      db.exec("CREATE TABLE atomic_write (value TEXT)");
      const entered = yield* Deferred.make<void>();
      const outsideStarted = yield* Deferred.make<void>();
      const inside = yield* Effect.forkChild(
        transact(
          db,
          Effect.gen(function* () {
            yield* transact(
              db,
              tryDb(
                () =>
                  db.exec("INSERT INTO atomic_write VALUES ('rolled-back')"),
                "nested insert",
              ),
            );
            yield* Deferred.succeed(entered, undefined);
            return yield* Effect.never;
          }),
        ),
      );
      yield* Deferred.await(entered);
      const outside = yield* Effect.forkChild(
        Effect.gen(function* () {
          yield* Deferred.succeed(outsideStarted, undefined);
          yield* tryDb(
            () => db.exec("INSERT INTO atomic_write VALUES ('survived')"),
            "outside insert",
          );
        }),
      );
      yield* Deferred.await(outsideStarted);
      yield* Fiber.interrupt(inside);
      yield* Fiber.join(outside);
      expect(
        db.query<{ value: string }, []>("SELECT value FROM atomic_write").all(),
      ).toEqual([{ value: "survived" }]);
    }),
);

it.live(
  "additive delivery migration preserves legacy rows and is safe to reopen",
  () =>
    Effect.gen(function* () {
      const path = storeSuitePath("legacy-webhook.db");
      const legacy = new Database(path, { create: true, strict: true });
      legacy.exec(`
      CREATE TABLE webhook_delivery (
        delivery_id TEXT PRIMARY KEY, organization_id TEXT NOT NULL,
        received_at INTEGER NOT NULL, payload_hash TEXT NOT NULL,
        payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', error TEXT
      );
      INSERT INTO webhook_delivery VALUES ('legacy', 'org', 1, 'original-hash', '{"type":"AgentSessionEvent"}', 'pending', NULL);
    `);
      legacy.close();
      for (let open = 0; open < 2; open++) {
        yield* Effect.gen(function* () {
          const { db } = yield* SqliteClient;
          expect(
            db
              .query<
                {
                  payload_hash: string;
                  payload_json: string;
                  payload_version: number;
                  replay_hash: string | null;
                  claim_token: string | null;
                },
                []
              >(
                "SELECT payload_hash, payload_json, payload_version, replay_hash, claim_token FROM webhook_delivery",
              )
              .get(),
          ).toEqual({
            payload_hash: "original-hash",
            payload_json: '{"type":"AgentSessionEvent"}',
            payload_version: 0,
            replay_hash: null,
            claim_token: null,
          });
        }).pipe(Effect.provide(SqliteClientLive(path)));
      }
    }),
);
