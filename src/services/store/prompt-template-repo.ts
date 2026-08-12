import { Effect, Option, Schema } from "effect";
import type { DatabaseError, RowDecodeError } from "../../domain/errors.js";
import {
  PromptTemplate,
  PromptTemplateKind,
  type PromptTemplate as PromptTemplateRecord,
} from "../../domain/models.js";
import {
  decodeRow,
  decodeRows,
  runChanges,
  SqliteClient,
  tryDb,
} from "./sqlite-client.js";

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
      const cache = new Map<string, Option.Option<PromptTemplateRecord>>();
      const cacheKey = (organizationId: string, kind: PromptTemplateKind) =>
        `${organizationId}:${kind}`;

      const get = Effect.fn("PromptTemplateRepo.get")(function* (
        organizationId: string,
        kind: PromptTemplateKind,
      ): Effect.fn.Return<
        Option.Option<PromptTemplateRecord>,
        DatabaseError | RowDecodeError
      > {
        const key = cacheKey(organizationId, kind);
        const cached = cache.get(key);
        if (cached !== undefined) return cached;
        const row = yield* tryDb(
          () =>
            db
              .query<PromptTemplateRow, [string, PromptTemplateKind]>(
                "SELECT * FROM prompt_template WHERE organization_id=? AND kind=?",
              )
              .get(organizationId, kind),
          "PromptTemplateRepo.get",
        );
        if (row === null) {
          cache.set(key, Option.none());
          return Option.none();
        }
        const value = Option.some(
          yield* rowToPromptTemplate(
            yield* decodeRow(PromptTemplateRow, row, "PromptTemplate"),
          ),
        );
        cache.set(key, value);
        return value;
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
        cache.delete(cacheKey(valid.organizationId, valid.kind));
      });

      const remove = Effect.fn("PromptTemplateRepo.remove")(function* (
        organizationId: string,
        kind: PromptTemplateKind,
      ): Effect.fn.Return<boolean, DatabaseError> {
        const changes = yield* tryDb(
          () =>
            db
              .query(
                "DELETE FROM prompt_template WHERE organization_id=? AND kind=?",
              )
              .run(organizationId, kind),
          "PromptTemplateRepo.remove",
        ).pipe(
          Effect.flatMap((result) =>
            runChanges(result, "PromptTemplateRepo.remove"),
          ),
        );
        cache.delete(cacheKey(organizationId, kind));
        return changes > 0;
      });

      return { get, list, upsert, remove };
    }),
  },
) {}
