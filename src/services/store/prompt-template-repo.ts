import { Effect, Option, Schema } from "effect";
import type { DatabaseError, RowDecodeError } from "../../domain/errors.js";
import {
  PromptTemplate,
  PromptTemplateKind,
  type PromptTemplate as PromptTemplateRecord,
} from "../../domain/models.js";
import { decodeRow, decodeRows, SqliteClient, tryDb } from "./sqlite-client.js";

const PromptTemplateRow = Schema.Struct({
  organization_id: Schema.String,
  kind: PromptTemplateKind,
  body: Schema.String,
  updated_at: Schema.Number,
});
type PromptTemplateRow = Schema.Schema.Type<typeof PromptTemplateRow>;

const rowToPromptTemplate = (
  row: PromptTemplateRow,
): Effect.Effect<PromptTemplateRecord, RowDecodeError> =>
  decodeRow(
    PromptTemplate,
    {
      organizationId: row.organization_id,
      kind: row.kind,
      body: row.body,
      updatedAt: row.updated_at,
    },
    "PromptTemplate",
  );

export class PromptTemplateRepo extends Effect.Service<PromptTemplateRepo>()(
  "PromptTemplateRepo",
  {
    accessors: true,
    effect: Effect.gen(function* () {
      const { db } = yield* SqliteClient;

      const get = Effect.fn("PromptTemplateRepo.get")(function* (
        organizationId: string,
        kind: PromptTemplateKind,
      ): Effect.fn.Return<
        Option.Option<PromptTemplateRecord>,
        DatabaseError | RowDecodeError
      > {
        const row = yield* tryDb(
          () =>
            db
              .query<PromptTemplateRow, [string, PromptTemplateKind]>(
                "SELECT * FROM prompt_template WHERE organization_id=? AND kind=?",
              )
              .get(organizationId, kind),
          "PromptTemplateRepo.get",
        );
        if (row === null) return Option.none();
        return Option.some(
          yield* rowToPromptTemplate(
            yield* decodeRow(PromptTemplateRow, row, "PromptTemplate"),
          ),
        );
      });

      const list = Effect.fn("PromptTemplateRepo.list")(function* (
        organizationId: string,
      ): Effect.fn.Return<
        ReadonlyArray<PromptTemplateRecord>,
        DatabaseError | RowDecodeError
      > {
        const rows = yield* tryDb(
          () =>
            db
              .query<PromptTemplateRow, [string]>(
                "SELECT * FROM prompt_template WHERE organization_id=? ORDER BY kind",
              )
              .all(organizationId),
          "PromptTemplateRepo.list",
        );
        const decoded = yield* decodeRows(
          PromptTemplateRow,
          rows,
          "PromptTemplate",
        );
        return yield* Effect.forEach(decoded, rowToPromptTemplate);
      });

      const upsert = Effect.fn("PromptTemplateRepo.upsert")(function* (
        template: PromptTemplateRecord,
      ): Effect.fn.Return<void, DatabaseError | RowDecodeError> {
        const valid = yield* decodeRow(
          PromptTemplate,
          template,
          "PromptTemplate",
        );
        yield* tryDb(
          () =>
            db
              .query(`
                INSERT INTO prompt_template (organization_id, kind, body, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(organization_id, kind) DO UPDATE SET
                  body=excluded.body,
                  updated_at=excluded.updated_at
              `)
              .run(
                valid.organizationId,
                valid.kind,
                valid.body,
                valid.updatedAt,
              ),
          "PromptTemplateRepo.upsert",
        );
      });

      const remove = Effect.fn("PromptTemplateRepo.remove")(function* (
        organizationId: string,
        kind: PromptTemplateKind,
      ): Effect.fn.Return<boolean, DatabaseError> {
        const result = yield* tryDb(
          () =>
            db
              .query(
                "DELETE FROM prompt_template WHERE organization_id=? AND kind=?",
              )
              .run(organizationId, kind),
          "PromptTemplateRepo.remove",
        );
        return result.changes > 0;
      });

      return { get, list, upsert, remove };
    }),
  },
) {}
