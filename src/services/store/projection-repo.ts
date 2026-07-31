import { Clock, Effect, Option, Schema } from "effect";
import type { ActivityId, SessionId, SourceKey } from "../../domain/ids.js";
import { ProjectionJob } from "../../domain/models.js";
import {
  DatabaseError,
  RowDecodeError,
  RunLeaseError,
} from "../../domain/errors.js";
import {
  SqliteClient,
  tryDb,
  runChanges,
  decodeRow,
  transact,
} from "./sqlite-client.js";
import { RunEventRepo, runEventLevelFromKind, runEventTextFromPayload } from "./run-event-repo.js";

const ProjectionJobRow = Schema.Struct({
  source_key: Schema.String,
  session_id: Schema.String,
  activity_type: Schema.String,
  payload_json: Schema.String,
  payload_hash: Schema.String,
  attempt: Schema.Number,
  next_attempt_at: Schema.Number,
  created_at: Schema.Number,
});

type ProjectionJobRow = Schema.Schema.Type<typeof ProjectionJobRow>;

const ProjectionIdentityRow = Schema.Struct({
  source_key: Schema.String,
  payload_hash: Schema.String,
  status: Schema.Literal("pending", "completed", "failed"),
});

type ProjectionIdentityRow = Schema.Schema.Type<typeof ProjectionIdentityRow>;

const optionToSql = <A>(value: Option.Option<A>): A | null =>
  Option.match(value, { onNone: () => null, onSome: (v) => v });

const rowToProjectionJob = (
  row: ProjectionJobRow,
): Effect.Effect<ProjectionJob, RowDecodeError> =>
  Effect.gen(function* () {
    const payload = yield* Effect.try({
      try: () => JSON.parse(row.payload_json) as unknown,
      catch: (error) =>
        new RowDecodeError({
          message: "Invalid JSON in projection_outbox.payload_json",
          entity: "ProjectionJob",
          cause: error instanceof Error ? error.message : String(error),
        }),
    });
    return yield* decodeRow(
      ProjectionJob,
      {
        sourceKey: row.source_key,
        sessionId: row.session_id,
        activityType: row.activity_type,
        payload,
        attempt: row.attempt,
        payloadHash: row.payload_hash,
        nextAttemptAt: row.next_attempt_at,
        createdAt: row.created_at,
      },
      "ProjectionJob",
    );
  });

export class ProjectionRepo extends Effect.Service<ProjectionRepo>()(
  "ProjectionRepo",
  {
    accessors: true,
    dependencies: [RunEventRepo.Default],
    effect: Effect.gen(function* () {
      const { db } = yield* SqliteClient;
      const runEventRepo = yield* RunEventRepo;

      const enqueue = Effect.fn("ProjectionRepo.enqueue")(
        function* (input: {
          readonly sourceKey: SourceKey;
          readonly sessionId: SessionId;
          readonly activityType: string;
          readonly payloadHash: string;
          readonly payload: unknown;
          readonly now?: number;
          readonly firstWriteWins?: boolean;
        }) { yield* Effect.annotateCurrentSpan("sourceKey", input.sourceKey);
        const now = input.now ?? (yield* Clock.currentTimeMillis);

        const tx = Effect.gen(function* () {
          const reserved = yield* tryDb(
            () =>
              db
                .query(`
                  INSERT INTO activity_projection (
                    source_key, session_id, activity_type, payload_hash, status, created_at, updated_at
                  ) VALUES (?, ?, ?, ?, 'pending', ?, ?)
                  ON CONFLICT DO NOTHING
                `)
                .run(
                  input.sourceKey,
                  input.sessionId,
                  input.activityType,
                  input.payloadHash,
                  now,
                  now,
                ),
            "ProjectionRepo.enqueue.reserve",
          );

          if ((yield* runChanges(reserved, "ProjectionRepo.enqueue.reserve")) === 1) {
            yield* tryDb(
              () =>
                db
                  .query(`
                    INSERT INTO projection_outbox (
                      source_key, session_id, activity_type, payload_json, status,
                      attempt, next_attempt_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, ?)
                  `)
                  .run(
                    input.sourceKey,
                    input.sessionId,
                    input.activityType,
                    JSON.stringify(input.payload),
                    now,
                    now,
                    now,
                  ),
              "ProjectionRepo.enqueue.outbox",
            );
            yield* upsertProjectionEvent(input, now);
            return true;
          }

          const existingBySource = yield* tryDb(
            () =>
              db
                .query<ProjectionIdentityRow, [string]>(`
                  SELECT source_key, payload_hash, status FROM activity_projection WHERE source_key=?
                `)
                .get(input.sourceKey),
            "ProjectionRepo.enqueue.existingBySource",
          );
          const existing =
            existingBySource ??
            (yield* tryDb(
              () =>
                db
                  .query<ProjectionIdentityRow, [string, string, string]>(`
                    SELECT source_key, payload_hash, status FROM activity_projection
                    WHERE session_id=? AND activity_type=? AND payload_hash=?
                  `)
                  .get(input.sessionId, input.activityType, input.payloadHash),
              "ProjectionRepo.enqueue.existingByHash",
            ));

          if (existing === null) {
            return yield* Effect.fail(new DatabaseError({ message: `Projection ${input.sourceKey} reservation disappeared` }))
          }

          const decoded = yield* decodeRow(
            ProjectionIdentityRow,
            existing,
            "Projection",
          );

          if (decoded.source_key !== input.sourceKey) {
            return false;
          }

          if (decoded.payload_hash !== input.payloadHash) {
            if (input.firstWriteWins) return false;
            return yield* Effect.fail(new DatabaseError({ message: `Projection ${input.sourceKey} was reused with a different payload` }))
          }

          if (decoded.status === "completed") {
            return false;
          }

          yield* tryDb(
            () =>
              db
                .query(`
                  INSERT INTO projection_outbox (
                    source_key, session_id, activity_type, payload_json, status,
                    attempt, next_attempt_at, created_at, updated_at
                  ) VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, ?)
                  ON CONFLICT(source_key) DO NOTHING
                `)
                .run(
                  input.sourceKey,
                  input.sessionId,
                  input.activityType,
                  JSON.stringify(input.payload),
                  now,
                  now,
                  now,
                ),
            "ProjectionRepo.enqueue.outbox",
          );
          yield* upsertProjectionEvent(input, now);
          return false;
        });

        return yield* transact(db, tx); },
      );

      const claimProjection = Effect.fn("ProjectionRepo.claimProjection")(
        function* (sourceKey: SourceKey,
        owner: string,
        leaseDurationMs: number,
        now?: number,) { yield* Effect.annotateCurrentSpan("sourceKey", sourceKey);
        const at = now ?? (yield* Clock.currentTimeMillis);

        const tx = Effect.gen(function* () {
          const claimed = yield* tryDb(
            () =>
              db
                .query(`
                  UPDATE projection_outbox
                  SET status='processing', attempt=attempt+1, lease_owner=?, lease_expires_at=?,
                    last_error=NULL, updated_at=?
                  WHERE source_key=? AND next_attempt_at<=?
                    AND (
                      status IN ('pending', 'failed')
                      OR (status='processing' AND lease_expires_at<?)
                    )
                `)
                .run(owner, at + leaseDurationMs, at, sourceKey, at, at),
            "ProjectionRepo.claimProjection",
          );

          if ((yield* runChanges(claimed, "ProjectionRepo.claimProjection")) !== 1) {
            return Option.none();
          }

          const row = yield* tryDb(
            () =>
              db
                .query<ProjectionJobRow, [string]>(`
                  SELECT o.source_key, o.session_id, o.activity_type, o.payload_json,
                    a.payload_hash, o.attempt, o.next_attempt_at, o.created_at
                  FROM projection_outbox o
                  JOIN activity_projection a ON a.source_key=o.source_key
                  WHERE o.source_key=?
                `)
                .get(sourceKey),
            "ProjectionRepo.claimProjection.select",
          );
          if (row === null) {
            return yield* Effect.fail(new DatabaseError({ message: `Projection ${sourceKey} disappeared after claim` }))
          }

          yield* tryDb(
            () =>
              db
                .query(
                  "UPDATE run_event SET status='pending', updated_at=? WHERE source_key=?",
                )
                .run(at, sourceKey),
            "ProjectionRepo.claimProjection.event",
          );

          const decoded = yield* decodeRow(ProjectionJobRow, row, "ProjectionJob");
          const job = yield* rowToProjectionJob(decoded);
          return Option.some(job);
        });

        return yield* transact(db, tx); },
      );

      const listDueProjectionKeys = Effect.fn(
        "ProjectionRepo.listDueProjectionKeys",
      )(
        function* (now?: number,
        limit = 50,) { const at = now ?? (yield* Clock.currentTimeMillis);
        const rows = yield* tryDb(
          () =>
            db
              .query<{ source_key: string }, [number, number, number]>(`
                SELECT source_key FROM projection_outbox
                WHERE next_attempt_at<=?
                  AND (
                    status IN ('pending', 'failed')
                    OR (status='processing' AND lease_expires_at<?)
                  )
                ORDER BY next_attempt_at, created_at, source_key
                LIMIT ?
              `)
              .all(at, at, limit),
          "ProjectionRepo.listDueProjectionKeys",
        );
        return yield* Effect.forEach(rows, (row) =>
          decodeRow(Schema.String, row.source_key, "SourceKey"),
        ); },
      );

      const failProjection = Effect.fn("ProjectionRepo.failProjection")(
        function* (sourceKey: SourceKey,
        owner: string,
        error: string,
        nextAttemptAt: number,) { yield* Effect.annotateCurrentSpan("sourceKey", sourceKey);
        const now = yield* Clock.currentTimeMillis;

        const tx = Effect.gen(function* () {
          yield* tryDb(
            () =>
              db
                .query(`
                  UPDATE projection_outbox
                  SET status='failed', next_attempt_at=?, lease_owner=NULL, lease_expires_at=NULL,
                    last_error=?, updated_at=?
                  WHERE source_key=? AND lease_owner=?
                `)
                .run(nextAttemptAt, error, now, sourceKey, owner),
            "ProjectionRepo.failProjection.outbox",
          );
          yield* tryDb(
            () =>
              db
                .query(
                  "UPDATE activity_projection SET status='failed', updated_at=? WHERE source_key=?",
                )
                .run(now, sourceKey),
            "ProjectionRepo.failProjection.activity",
          );
          yield* tryDb(
            () =>
              db
                .query(
                  "UPDATE run_event SET status='failed', error=?, updated_at=? WHERE source_key=?",
                )
                .run(error, now, sourceKey),
            "ProjectionRepo.failProjection.event",
          );
        });

        return yield* transact(db, tx); },
      );

      const completeProjection = Effect.fn("ProjectionRepo.completeProjection")(
        function* (sourceKey: SourceKey,
        owner: string,
        activityId: Option.Option<ActivityId>,) { yield* Effect.annotateCurrentSpan("sourceKey", sourceKey);
        const now = yield* Clock.currentTimeMillis;

        const tx = Effect.gen(function* () {
          const completed = yield* tryDb(
            () =>
              db
                .query(`
                  UPDATE projection_outbox
                  SET status='completed', lease_owner=NULL, lease_expires_at=NULL,
                    last_error=NULL, updated_at=?
                  WHERE source_key=? AND lease_owner=? AND status='processing'
                `)
                .run(now, sourceKey, owner),
            "ProjectionRepo.completeProjection.outbox",
          );

          if ((yield* runChanges(completed, "ProjectionRepo.completeProjection.outbox")) !== 1) {
            return yield* Effect.fail(
              new RunLeaseError({
                sessionId: sourceKey,
                message: `Projection ${sourceKey} lease was lost before completion`,
              }),
            );
          }

          yield* tryDb(
            () =>
              db
                .query(`
                  UPDATE activity_projection
                  SET status='completed', linear_activity_id=?, updated_at=? WHERE source_key=?
                `)
                .run(optionToSql(activityId), now, sourceKey),
            "ProjectionRepo.completeProjection.activity",
          );
          yield* tryDb(
            () =>
              db
                .query(
                  "UPDATE run_event SET status='completed', error=NULL, updated_at=? WHERE source_key=?",
                )
                .run(now, sourceKey),
            "ProjectionRepo.completeProjection.event",
          );
        });

        return yield* transact(db, tx); },
      );

      const projectionCount = Effect.fn("ProjectionRepo.projectionCount")(
        function* (sessionId: SessionId,
        activityType?: string,) { const row =
          activityType === undefined
            ? yield* tryDb(
                () =>
                  db
                    .query<{ count: number }, [string]>(
                      "SELECT COUNT(*) count FROM activity_projection WHERE session_id=?",
                    )
                    .get(sessionId),
                "ProjectionRepo.projectionCount",
              )
            : yield* tryDb(
                () =>
                  db
                    .query<{ count: number }, [string, string]>(
                      "SELECT COUNT(*) count FROM activity_projection WHERE session_id=? AND activity_type=?",
                    )
                    .get(sessionId, activityType),
                "ProjectionRepo.projectionCount",
              );
        return (row as { count: number }).count ?? 0; },
      );

      function upsertProjectionEvent(
        input: {
          readonly sourceKey: SourceKey;
          readonly sessionId: SessionId;
          readonly activityType: string;
          readonly payload: unknown;
        },
        now: number,
      ): Effect.Effect<void, DatabaseError> {
        return runEventRepo.upsert({
          sourceKey: input.sourceKey,
          sessionId: input.sessionId,
          kind: input.activityType,
          level: runEventLevelFromKind(input.activityType),
          text: runEventTextFromPayload(input.activityType, input.payload),
          payload: input.payload,
          status: "pending",
          now,
        });
      }

      const get = Effect.fn("ProjectionRepo.get")(
        function* (sourceKey: SourceKey,) { yield* Effect.annotateCurrentSpan("sourceKey", sourceKey);
        const row = yield* tryDb(
          () =>
            db
              .query<ProjectionJobRow, [string]>(`
                SELECT o.source_key, o.session_id, o.activity_type, o.payload_json,
                  a.payload_hash, o.attempt, o.next_attempt_at, o.created_at
                FROM projection_outbox o
                JOIN activity_projection a ON a.source_key=o.source_key
                WHERE o.source_key=?
              `)
              .get(sourceKey),
          "ProjectionRepo.get",
        );
        if (row === null) {
          return Option.none();
        }
        const decoded = yield* decodeRow(ProjectionJobRow, row, "ProjectionJob");
        const job = yield* rowToProjectionJob(decoded);
        return Option.some(job); },
      );

      return {
        enqueue,
        get,
        claimProjection,
        claim: claimProjection,
        listDueProjectionKeys,
        due: listDueProjectionKeys,
        failProjection,
        fail: failProjection,
        completeProjection,
        complete: completeProjection,
        projectionCount,
      };
    }),
  }
) {}
