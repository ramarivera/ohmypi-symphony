import { Clock, Effect, Schema } from "effect";
import { type DatabaseError, RowDecodeError } from "../../domain/errors.js";
import type { SessionId, SourceKey } from "../../domain/ids.js";
import { RunEvent } from "../../domain/models.js";
import { decodeRow, decodeRows, SqliteClient, tryDb } from "./sqlite-client.js";

const RunEventRow = Schema.Struct({
  source_key: Schema.String,
  session_id: Schema.String,
  kind: Schema.String,
  level: Schema.Literal("debug", "info", "warn", "result", "error"),
  text: Schema.NullOr(Schema.String),
  payload_json: Schema.String,
  status: Schema.NullOr(
    Schema.Literal("observed", "pending", "completed", "failed"),
  ),
  error: Schema.NullOr(Schema.String),
  created_at: Schema.Number,
  updated_at: Schema.Number,
});

type RunEventRow = Schema.Schema.Type<typeof RunEventRow>;

export const runEventLevelFromKind = (
  kind: string,
): "debug" | "info" | "warn" | "result" | "error" => {
  switch (kind) {
    case "thought":
      return "debug";
    case "action":
      return "info";
    case "elicitation":
      return "warn";
    case "response":
      return "result";
    case "error":
      return "error";
    default:
      return "info";
  }
};

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

export const runEventTextFromPayload = (
  kind: string,
  payload: unknown,
): string | null => {
  if (!record(payload)) return null;
  const request = record(payload.request) ? payload.request : payload;

  if (kind === "plan") {
    const plan = Array.isArray(request.plan) ? request.plan : [];
    if (plan.length === 0) return null;
    return plan
      .filter(
        (item): item is Record<string, unknown> & { content: string } =>
          record(item) && typeof item.content === "string",
      )
      .map(
        (item) =>
          `${item.content} (${
            typeof item.status === "string" ? item.status : "pending"
          })`,
      )
      .join("; ");
  }

  if (kind === "externalUrls") {
    const urls = Array.isArray(request.externalUrls)
      ? request.externalUrls
      : [];
    if (urls.length === 0) return null;
    return urls
      .filter(
        (
          entry,
        ): entry is Record<string, unknown> & { label: string; url: string } =>
          record(entry) &&
          typeof entry.label === "string" &&
          typeof entry.url === "string",
      )
      .map((entry) => `${entry.label}: ${entry.url}`)
      .join("; ");
  }

  const content = record(request.content) ? request.content : request;
  const body = text(content.body);
  if (body) return body;

  if (kind === "action" || text(content.action)) {
    const action = text(content.action) ?? "tool";
    const parameter = text(content.parameter);
    const result = text(content.result);
    if (parameter && result) return `${action}: ${parameter} → ${result}`;
    if (parameter) return `${action}: ${parameter}`;
    if (result) return `${action} → ${result}`;
    return action;
  }

  return null;
};

const rowToRunEvent = (
  row: RunEventRow,
): Effect.Effect<RunEvent, RowDecodeError> =>
  Effect.gen(function* () {
    const payload = yield* Effect.try({
      try: () => JSON.parse(row.payload_json) as unknown,
      catch: (error) =>
        new RowDecodeError({
          message: "Invalid JSON in run_event.payload_json",
          entity: "RunEvent",
          cause: error instanceof Error ? error.message : String(error),
        }),
    });
    return yield* decodeRow(
      RunEvent,
      {
        sourceKey: row.source_key,
        sessionId: row.session_id,
        kind: row.kind,
        level: row.level,
        status: row.status,
        text: row.text,
        payload,
        error: row.error,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      },
      "RunEvent",
    );
  });

export class RunEventRepo extends Effect.Service<RunEventRepo>()(
  "RunEventRepo",
  {
    accessors: true,
    effect: Effect.gen(function* () {
      const { db } = yield* SqliteClient;

      const upsert = Effect.fn("RunEventRepo.upsert")(function* (input: {
        readonly sourceKey: SourceKey;
        readonly sessionId: SessionId;
        readonly kind: string;
        readonly level: "debug" | "info" | "warn" | "result" | "error";
        readonly text?: string | null;
        readonly payload?: unknown;
        readonly status?:
          | "observed"
          | "pending"
          | "completed"
          | "failed"
          | null;
        readonly error?: string | null;
        readonly now?: number;
      }): Effect.fn.Return<void, DatabaseError> {
        yield* Effect.annotateCurrentSpan("sourceKey", input.sourceKey);
        const now = input.now ?? (yield* Clock.currentTimeMillis);
        const text = input.text ?? null;
        const payloadJson = JSON.stringify(input.payload ?? null);
        const status = input.status ?? null;
        const error = input.error ?? null;

        yield* tryDb(
          () =>
            db
              .query(`
              INSERT INTO run_event (
                source_key, session_id, kind, level, text, payload_json, status, error, created_at, updated_at
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT(source_key) DO UPDATE SET
                kind=?,
                level=?,
                text=?,
                payload_json=?,
                status=?,
                error=?,
                created_at=run_event.created_at,
                updated_at=?
              WHERE run_event.status IS NULL OR run_event.status NOT IN ('completed','failed')
            `)
              .run(
                input.sourceKey,
                input.sessionId,
                input.kind,
                input.level,
                text,
                payloadJson,
                status,
                error,
                now,
                now,
                input.kind,
                input.level,
                text,
                payloadJson,
                status,
                error,
                now,
              ),
          "RunEventRepo.upsert",
        );
      });

      const list = Effect.fn("RunEventRepo.list")(function* (
        sessionId: SessionId,
      ): Effect.fn.Return<
        ReadonlyArray<RunEvent>,
        DatabaseError | RowDecodeError
      > {
        yield* Effect.annotateCurrentSpan("sessionId", sessionId);
        const rows = yield* tryDb(
          () =>
            db
              .query<RunEventRow, [string]>(
                "SELECT * FROM run_event WHERE session_id=? ORDER BY created_at, source_key",
              )
              .all(sessionId),
          "RunEventRepo.list",
        );
        const decoded = yield* decodeRows(RunEventRow, rows, "RunEvent");
        return yield* Effect.forEach(decoded, rowToRunEvent);
      });

      return { upsert, list };
    }),
  },
) {}
