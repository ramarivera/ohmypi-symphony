import { Clock, Effect, Option, Schema } from "effect";
import { DatabaseError, type RowDecodeError } from "../../domain/errors.js";
import type {
  IssueId,
  OrganizationId,
  ProjectId,
  SessionId,
  TeamId,
  WorkspaceId,
} from "../../domain/ids.js";
import { SourceKey } from "../../domain/ids.js";
import {
  type AgentRun,
  AgentRun as AgentRunSchema,
  type RunState,
} from "../../domain/models.js";
import { RunEventRepo } from "./run-event-repo.js";
import {
  decodeRow,
  decodeRows,
  runChanges,
  SqliteClient,
  tryDb,
} from "./sqlite-client.js";

const AgentRunRow = Schema.Struct({
  session_id: Schema.String,
  organization_id: Schema.String,
  issue_id: Schema.NullOr(Schema.String),
  repository_id: Schema.NullOr(Schema.String),
  state: Schema.String,
  desired_state: Schema.String,
  omp_session_id: Schema.NullOr(Schema.String),
  omp_session_file: Schema.NullOr(Schema.String),
  workspace_path: Schema.NullOr(Schema.String),
  attempt: Schema.Number,
  lease_owner: Schema.NullOr(Schema.String),
  lease_expires_at: Schema.NullOr(Schema.Number),
  last_activity_at: Schema.NullOr(Schema.Number),
  terminal_reason: Schema.NullOr(Schema.String),
  next_attempt_at: Schema.NullOr(Schema.Number),
  team_id: Schema.NullOr(Schema.String),
  project_id: Schema.NullOr(Schema.String),
  created_at: Schema.Number,
  updated_at: Schema.Number,
});

type AgentRunRow = Schema.Schema.Type<typeof AgentRunRow>;

const TERMINAL_STATES: ReadonlyArray<RunState> = [
  "succeeded",
  "failed",
  "canceled",
];

const rowToAgentRun = (
  row: AgentRunRow,
): Effect.Effect<AgentRun, RowDecodeError> =>
  decodeRow(
    AgentRunSchema,
    {
      sessionId: row.session_id,
      organizationId: row.organization_id,
      issueId: row.issue_id,
      repositoryId: row.repository_id,
      state: row.state,
      desiredState: row.desired_state,
      ompSessionId: row.omp_session_id,
      ompSessionFile: row.omp_session_file,
      workspacePath: row.workspace_path,
      attempt: row.attempt,
      teamId: row.team_id,
      projectId: row.project_id,
      leaseOwner: row.lease_owner,
      leaseExpiresAt: row.lease_expires_at,
      lastActivityAt: row.last_activity_at,
      terminalReason: row.terminal_reason,
      nextAttemptAt: row.next_attempt_at,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    },
    "AgentRun",
  );

const getNullable = <A>(
  current: Option.Option<A>,
  patch: Option.Option<A> | undefined,
): Option.Option<A> => (patch === undefined ? current : patch);

const optionToSql = <A>(value: Option.Option<A>): A | null =>
  Option.match(value, { onNone: () => null, onSome: (v) => v });

export class RunRepo extends Effect.Service<RunRepo>()("RunRepo", {
  accessors: true,
  dependencies: [RunEventRepo.Default],
  effect: Effect.gen(function* () {
    const { db } = yield* SqliteClient;
    const runEventRepo = yield* RunEventRepo;
    const get = Effect.fn("RunRepo.get")(function* (
      sessionId: SessionId,
    ): Effect.fn.Return<
      Option.Option<AgentRun>,
      DatabaseError | RowDecodeError
    > {
      yield* Effect.annotateCurrentSpan("sessionId", sessionId);
      const row = yield* tryDb(
        () =>
          db
            .query<AgentRunRow, [string]>(
              "SELECT * FROM agent_run WHERE session_id=?",
            )
            .get(sessionId),
        "RunRepo.get",
      );
      if (row === null) return Option.none();
      const decoded = yield* decodeRow(AgentRunRow, row, "AgentRun");
      const run = yield* rowToAgentRun(decoded);
      return Option.some(run);
    });

    const create = Effect.fn("RunRepo.create")(function* (input: {
      readonly sessionId: SessionId;
      readonly organizationId: OrganizationId;
      readonly issueId: Option.Option<IssueId>;
      readonly teamId?: Option.Option<TeamId>;
      readonly projectId?: Option.Option<ProjectId>;
      readonly now?: number;
    }): Effect.fn.Return<AgentRun, DatabaseError | RowDecodeError> {
      yield* Effect.annotateCurrentSpan("sessionId", input.sessionId);
      const now = input.now ?? (yield* Clock.currentTimeMillis);
      yield* tryDb(
        () =>
          db
            .query(`
              INSERT INTO agent_run (
                session_id, organization_id, issue_id, team_id, project_id, state, desired_state, created_at, updated_at
              ) VALUES (?, ?, ?, ?, ?, 'queued', 'running', ?, ?)
              ON CONFLICT(session_id) DO NOTHING
            `)
            .run(
              input.sessionId,
              input.organizationId,
              optionToSql(input.issueId),
              optionToSql(input.teamId ?? Option.none()),
              optionToSql(input.projectId ?? Option.none()),
              now,
              now,
            ),
        "RunRepo.create",
      );
      const run = yield* get(input.sessionId);
      return yield* Option.match(run, {
        onNone: () =>
          Effect.fail(
            new DatabaseError({
              message: `Failed to create run ${input.sessionId}`,
            }),
          ),
        onSome: Effect.succeed,
      });
    });

    const update = Effect.fn("RunRepo.update")(function* (
      sessionId: SessionId,
      patch: {
        readonly state?: RunState;
        readonly repositoryId?: Option.Option<WorkspaceId>;
        readonly workspacePath?: Option.Option<string>;
        readonly ompSessionId?: Option.Option<string>;
        readonly ompSessionFile?: Option.Option<string>;
        readonly terminalReason?: Option.Option<string>;
        readonly lastActivityAt?: Option.Option<number>;
        readonly nextAttemptAt?: Option.Option<number>;
        readonly incrementAttempt?: boolean;
      },
    ): Effect.fn.Return<void, DatabaseError | RowDecodeError> {
      yield* Effect.annotateCurrentSpan("sessionId", sessionId);
      const current = yield* get(sessionId);
      if (Option.isNone(current)) {
        return yield* Effect.fail(
          new DatabaseError({ message: `Unknown run ${sessionId}` }),
        );
      }
      const run = current.value;

      if (
        TERMINAL_STATES.includes(run.state) &&
        patch.state !== undefined &&
        patch.state !== run.state
      ) {
        return yield* Effect.fail(
          new DatabaseError({ message: "Terminal run state is immutable" }),
        );
      }

      const state = patch.state ?? run.state;
      const attempt = run.attempt + (patch.incrementAttempt ? 1 : 0);
      const now = yield* Clock.currentTimeMillis;

      const repositoryId = getNullable(run.repositoryId, patch.repositoryId);
      const workspacePath = getNullable(run.workspacePath, patch.workspacePath);
      const ompSessionId = getNullable(run.ompSessionId, patch.ompSessionId);
      const ompSessionFile = getNullable(
        run.ompSessionFile,
        patch.ompSessionFile,
      );
      const terminalReason = getNullable(
        run.terminalReason,
        patch.terminalReason,
      );
      const lastActivityAt = getNullable(
        run.lastActivityAt,
        patch.lastActivityAt,
      );
      const nextAttemptAt = getNullable(run.nextAttemptAt, patch.nextAttemptAt);

      yield* tryDb(
        () =>
          db
            .query(`
              UPDATE agent_run SET state=?, repository_id=?, workspace_path=?, omp_session_id=?, omp_session_file=?,
                terminal_reason=?, last_activity_at=?, next_attempt_at=?, attempt=attempt+?, updated_at=? WHERE session_id=?
            `)
            .run(
              state,
              optionToSql(repositoryId),
              optionToSql(workspacePath),
              optionToSql(ompSessionId),
              optionToSql(ompSessionFile),
              optionToSql(terminalReason),
              optionToSql(lastActivityAt),
              optionToSql(nextAttemptAt),
              patch.incrementAttempt ? 1 : 0,
              now,
              sessionId,
            ),
        "RunRepo.update",
      );
      if (state !== run.state) {
        const sourceKey = Schema.decodeUnknownSync(SourceKey)(
          `state:${sessionId}:${attempt}:${state}`,
        );
        yield* runEventRepo.upsert({
          sourceKey,
          sessionId,
          kind: "state",
          level: state === "failed" || state === "canceled" ? "error" : "info",
          text: `${run.state} → ${state}`,
          payload: { from: run.state, to: state, attempt },
          status: "observed",
          error: optionToSql(terminalReason),
          now,
        });
      }
    });

    const listRunnable = Effect.fn("RunRepo.listRunnable")(function* (
      now: number,
    ): Effect.fn.Return<
      ReadonlyArray<AgentRun>,
      DatabaseError | RowDecodeError
    > {
      const rows = yield* tryDb(
        () =>
          db
            .query<AgentRunRow, [number, number]>(`
              SELECT * FROM agent_run WHERE desired_state='running' AND state NOT IN ('succeeded','failed','canceled')
                AND (next_attempt_at IS NULL OR next_attempt_at<=?) AND (lease_owner IS NULL OR lease_expires_at<?)
              ORDER BY created_at, session_id
            `)
            .all(now, now),
        "RunRepo.listRunnable",
      );
      const decoded = yield* decodeRows(AgentRunRow, rows, "AgentRun");
      return yield* Effect.forEach(decoded, rowToAgentRun);
    });

    const listCancellationPending = Effect.fn(
      "RunRepo.listCancellationPending",
    )(function* (): Effect.fn.Return<
      ReadonlyArray<AgentRun>,
      DatabaseError | RowDecodeError
    > {
      const rows = yield* tryDb(
        () =>
          db
            .query<AgentRunRow, []>(
              "SELECT * FROM agent_run WHERE desired_state='canceled' AND state NOT IN ('succeeded','failed','canceled')",
            )
            .all(),
        "RunRepo.listCancellationPending",
      );
      const decoded = yield* decodeRows(AgentRunRow, rows, "AgentRun");
      return yield* Effect.forEach(decoded, rowToAgentRun);
    });

    const claimLease = Effect.fn("RunRepo.claimLease")(function* (
      sessionId: SessionId,
      owner: string,
      leaseDurationMs: number,
      now?: number,
    ): Effect.fn.Return<boolean, DatabaseError> {
      yield* Effect.annotateCurrentSpan("sessionId", sessionId);
      const at = now ?? (yield* Clock.currentTimeMillis);
      const result = yield* tryDb(
        () =>
          db
            .query(`
              UPDATE agent_run SET lease_owner=?, lease_expires_at=?, updated_at=?
              WHERE session_id=? AND desired_state='running' AND state NOT IN ('succeeded','failed','canceled')
                AND (lease_owner IS NULL OR lease_owner=? OR lease_expires_at<?)
            `)
            .run(owner, at + leaseDurationMs, at, sessionId, owner, at),
        "RunRepo.claimLease",
      );
      return (yield* runChanges(result, "RunRepo.claimLease")) === 1;
    });

    const renewLease = Effect.fn("RunRepo.renewLease")(function* (
      sessionId: SessionId,
      owner: string,
      leaseDurationMs: number,
      now?: number,
    ): Effect.fn.Return<boolean, DatabaseError> {
      yield* Effect.annotateCurrentSpan("sessionId", sessionId);
      const at = now ?? (yield* Clock.currentTimeMillis);
      const result = yield* tryDb(
        () =>
          db
            .query(
              "UPDATE agent_run SET lease_expires_at=?, updated_at=? WHERE session_id=? AND lease_owner=? AND lease_expires_at>=?",
            )
            .run(at + leaseDurationMs, at, sessionId, owner, at),
        "RunRepo.renewLease",
      );
      return (yield* runChanges(result, "RunRepo.renewLease")) === 1;
    });

    const releaseLease = Effect.fn("RunRepo.releaseLease")(function* (
      sessionId: SessionId,
      owner: string,
    ): Effect.fn.Return<void, DatabaseError> {
      yield* Effect.annotateCurrentSpan("sessionId", sessionId);
      const now = yield* Clock.currentTimeMillis;
      yield* tryDb(
        () =>
          db
            .query(
              "UPDATE agent_run SET lease_owner=NULL, lease_expires_at=NULL, updated_at=? WHERE session_id=? AND lease_owner=?",
            )
            .run(now, sessionId, owner),
        "RunRepo.releaseLease",
      );
    });

    const recoverInterruptedRuns = Effect.fn("RunRepo.recoverInterruptedRuns")(
      function* (now?: number): Effect.fn.Return<number, DatabaseError> {
        const at = now ?? (yield* Clock.currentTimeMillis);
        const result = yield* tryDb(
          () =>
            db
              .query(`
              UPDATE agent_run
              SET state=CASE
                    WHEN desired_state='canceled' THEN 'stopping'
                    WHEN state IN ('starting','running') THEN 'orphaned'
                    ELSE state
                  END,
                  next_attempt_at=CASE
                    WHEN desired_state='running' AND state IN ('starting','running') THEN ?
                    ELSE next_attempt_at
                  END,
                  lease_owner=NULL,
                  lease_expires_at=NULL,
                  updated_at=?
              WHERE state NOT IN ('succeeded','failed','canceled')
            `)
              .run(at, at),
          "RunRepo.recoverInterruptedRuns",
        );
        return yield* runChanges(result, "RunRepo.recoverInterruptedRuns");
      },
    );

    return {
      create,
      get,
      update,
      listRunnable,
      listCancellationPending,
      claimLease,
      renewLease,
      releaseLease,
      recoverInterruptedRuns,
    };
  }),
}) {}
