import { Clock, Effect, Option, Schema } from "effect";
import type { InputId, SessionId, SourceKey } from "../../domain/ids.js";
import { InputKind, RunInput } from "../../domain/models.js";
import { DatabaseError, RowDecodeError } from "../../domain/errors.js";
import { SqliteClient, tryDb, decodeRow, decodeRows, transact } from "./sqlite-client.js";
import { RunEventRepo } from "./run-event-repo.js";

const RunInputRow = Schema.Struct({
  id: Schema.String,
  session_id: Schema.String,
  kind: InputKind,
  body: Schema.String,
  payload_json: Schema.String,
  created_at: Schema.Number,
});

type RunInputRow = Schema.Schema.Type<typeof RunInputRow>;

const RunRowState = Schema.Struct({
  state: Schema.Literal("queued", "starting", "running", "waiting", "stopping", "succeeded", "failed", "canceled", "orphaned"),
  desired_state: Schema.Literal("running", "canceled"),
});

type RunRowState = Schema.Schema.Type<typeof RunRowState>;

const rowToRunInput = (row: RunInputRow): Effect.Effect<RunInput, RowDecodeError> =>
  Effect.gen(function* () {
    const payload = yield* Effect.try({
      try: () => JSON.parse(row.payload_json) as unknown,
      catch: (error) =>
        new RowDecodeError({
          message: "Invalid JSON in run_input.payload_json",
          entity: "RunInput",
          cause: error instanceof Error ? error.message : String(error),
        }),
    });
    return yield* decodeRow(
      RunInput,
      {
        id: row.id,
        sessionId: row.session_id,
        kind: row.kind,
        body: row.body,
        payload,
        createdAt: row.created_at,
      },
      "RunInput",
    );
  });

export class RunInputRepo extends Effect.Service<RunInputRepo>()(
  "RunInputRepo",
  {
    accessors: true,
    dependencies: [RunEventRepo.Default],
    effect: Effect.gen(function* () {
      const { db } = yield* SqliteClient;

      const getRunState = (
        sessionId: SessionId,
      ): Effect.Effect<Option.Option<RunRowState>, DatabaseError | RowDecodeError> =>
        Effect.gen(function* () {
          const row = yield* tryDb(
            () =>
              db
                .query<RunRowState, [string]>(
                  "SELECT state, desired_state FROM agent_run WHERE session_id=?",
                )
                .get(sessionId),
            "RunInputRepo.getRunState",
          );
          if (row === null) return Option.none();
          return Option.some(yield* decodeRow(RunRowState, row, "AgentRun"));
        });

      const enqueue = Effect.fn("RunInputRepo.enqueue")(
        function* (input: {
          readonly id: InputId;
          readonly sessionId: SessionId;
          readonly kind: InputKind;
          readonly body: string;
          readonly payload: unknown;
          readonly createdAt?: number;
        }): Effect.Effect<boolean, DatabaseError | RowDecodeError> {
          yield* Effect.annotateCurrentSpan("sessionId", input.sessionId);

          const tx = Effect.gen(function* () {
            const run = yield* getRunState(input.sessionId);
            if (Option.isNone(run)) {
              return yield* Effect.die(new Error(`Unknown run ${input.sessionId}`));
            }
            if (input.kind !== "stop" && run.value.desired_state === "canceled") {
              return false;
            }

            const createdAt = input.createdAt ?? (yield* Clock.currentTimeMillis);
            const insertResult = yield* tryDb(
              () =>
                db
                  .query(`
                    INSERT INTO run_input (id, session_id, kind, body, payload_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING
                  `)
                  .run(
                    input.id,
                    input.sessionId,
                    input.kind,
                    input.body,
                    JSON.stringify(input.payload),
                    createdAt,
                  ),
              "RunInputRepo.enqueue.insert",
            );
            const inserted = (insertResult as { changes: number }).changes === 1;

            if (inserted) {
              const sourceKey = `input:${input.id}` as SourceKey;
              yield* RunEventRepo.upsert({
                sourceKey,
                sessionId: input.sessionId,
                kind: `input:${input.kind}`,
                level: input.kind === "stop" ? "warn" : "info",
                text: input.body || null,
                payload: input.payload,
                status: null,
                now: createdAt,
              });
            }

            if (inserted && input.kind !== "stop") {
              yield* tryDb(
                () =>
                  db
                    .query(`
                      UPDATE agent_run
                      SET state='queued', desired_state='running', attempt=0,
                        terminal_reason=NULL, next_attempt_at=NULL, updated_at=?
                      WHERE session_id=? AND state IN ('succeeded','failed')
                    `)
                    .run(createdAt, input.sessionId),
                "RunInputRepo.enqueue.restart",
              );
            }

            if (input.kind === "stop") {
              yield* tryDb(
                () =>
                  db
                    .query(`
                      UPDATE agent_run SET desired_state='canceled',
                        state=CASE WHEN state IN ('queued','waiting') THEN 'stopping' ELSE state END, updated_at=?
                      WHERE session_id=?
                    `)
                    .run(createdAt, input.sessionId),
                "RunInputRepo.enqueue.stop",
              );
            }

            return inserted;
          });

          return yield* transact(tx);
        },
      );

      const pending = Effect.fn("RunInputRepo.pending")(
        function* (
          sessionId: SessionId,
        ): Effect.Effect<ReadonlyArray<RunInput>, DatabaseError | RowDecodeError> {
          yield* Effect.annotateCurrentSpan("sessionId", sessionId);
          const rows = yield* tryDb(
            () =>
              db
                .query<RunInputRow, [string]>(
                  "SELECT id, session_id, kind, body, payload_json, created_at FROM run_input WHERE session_id=? AND processed_at IS NULL ORDER BY created_at, id",
                )
                .all(sessionId),
            "RunInputRepo.pending",
          );
          const decoded = yield* decodeRows(RunInputRow, rows, "RunInput");
          return yield* Effect.forEach(decoded, rowToRunInput);
        },
      );

      const latestActionableInput = Effect.fn(
        "RunInputRepo.latestActionableInput",
      )(
        function* (
          sessionId: SessionId,
        ): Effect.Effect<
          Option.Option<{ readonly body: string; readonly kind: InputKind }>,
          DatabaseError | RowDecodeError
        > {
          yield* Effect.annotateCurrentSpan("sessionId", sessionId);
          const row = yield* tryDb(
            () =>
              db
                .query<{ body: string; kind: InputKind }, [string]>(`
                  SELECT body, kind FROM run_input
                  WHERE session_id=? AND kind!='stop'
                  ORDER BY created_at DESC, id DESC
                  LIMIT 1
                `)
                .get(sessionId),
            "RunInputRepo.latestActionableInput",
          );
          if (row === null) return Option.none();
          return Option.some({ body: row.body, kind: row.kind });
        },
      );

      const listSessionsWithPendingInputs = Effect.fn(
        "RunInputRepo.listSessionsWithPendingInputs",
      )(
        function* (): Effect.Effect<
          ReadonlyArray<SessionId>,
          DatabaseError | RowDecodeError
        > {
          const rows = yield* tryDb(
            () =>
              db
                .query<{ session_id: string }, []>(`
                  SELECT DISTINCT r.session_id
                  FROM agent_run r
                  JOIN run_input i ON i.session_id=r.session_id AND i.processed_at IS NULL
                  WHERE r.state NOT IN ('succeeded','failed','canceled')
                  ORDER BY r.session_id
                `)
                .all(),
            "RunInputRepo.listSessionsWithPendingInputs",
          );
          return yield* Effect.forEach(rows, (row) =>
            decodeRow(Schema.String, row.session_id, "SessionId"),
          );
        },
      );

      const markProcessed = Effect.fn("RunInputRepo.markProcessed")(
        function* (
          id: InputId,
          at?: number,
        ): Effect.Effect<void, DatabaseError> {
          yield* Effect.annotateCurrentSpan("inputId", id);
          const processedAt = at ?? (yield* Clock.currentTimeMillis);
          return yield* tryDb(
            () =>
              db
                .query("UPDATE run_input SET processed_at=? WHERE id=?")
                .run(processedAt, id),
            "RunInputRepo.markProcessed",
          );
        },
      );

      return {
        enqueue,
        pending,
        latestActionableInput,
        listSessionsWithPendingInputs,
        markProcessed,
      };
    }),
  }
) {}
