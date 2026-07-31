import { Clock, Effect, Option, Schema } from "effect";
import { OrganizationId } from "../../domain/ids.js";
import { DatabaseError, RowDecodeError } from "../../domain/errors.js";
import { SqliteClient, tryDb, runChanges, decodeRow } from "./sqlite-client.js";

const AdminSessionRow = Schema.Struct({
  token_hash: Schema.String,
  organization_id: OrganizationId,
  csrf_token_hash: Schema.String,
  expires_at: Schema.Number,
  created_at: Schema.Number,
});

type AdminSessionRow = Schema.Schema.Type<typeof AdminSessionRow>;

export class AdminSessionRepo extends Effect.Service<AdminSessionRepo>()(
  "AdminSessionRepo",
  {
    accessors: true,
    effect: Effect.gen(function* () {
      const { db } = yield* SqliteClient;

      const create = Effect.fn("AdminSessionRepo.create")(
        function* (input: {
          readonly organizationId: OrganizationId;
          readonly tokenHash: string;
          readonly csrfTokenHash: string;
          readonly expiresAt: number;
          readonly now?: number;
        }) { yield* Effect.annotateCurrentSpan("organizationId", input.organizationId);
        const now = input.now ?? (yield* Clock.currentTimeMillis);
        yield* tryDb(() =>
          db
            .query(`
              INSERT INTO admin_session (token_hash, organization_id, csrf_token_hash, expires_at, created_at)
              VALUES (?, ?, ?, ?, ?)
            `)
            .run(
              input.tokenHash,
              input.organizationId,
              input.csrfTokenHash,
              input.expiresAt,
              now,
            ), "AdminSessionRepo.create"); },
      );

      const get = Effect.fn("AdminSessionRepo.get")(
        function* (tokenHash: string,
        now?: number,) { yield* Effect.annotateCurrentSpan("tokenHash", tokenHash);
        const at = now ?? (yield* Clock.currentTimeMillis);
        const row = yield* tryDb(
          () =>
            db
              .query<AdminSessionRow, [string, number]>(
                "SELECT * FROM admin_session WHERE token_hash=? AND expires_at>=?",
              )
              .get(tokenHash, at),
          "AdminSessionRepo.get",
        );
        if (row === null) return Option.none();
        const decoded = yield* decodeRow(AdminSessionRow, row, "AdminSession");
        return Option.some({
          organizationId: decoded.organization_id,
          csrfTokenHash: decoded.csrf_token_hash,
        }); },
      );

      const deleteAdminSession = Effect.fn("AdminSessionRepo.deleteAdminSession")(
        function* (tokenHash: string) { yield* Effect.annotateCurrentSpan("tokenHash", tokenHash);
        const result = yield* tryDb(
          () =>
            db
              .query("DELETE FROM admin_session WHERE token_hash=?")
              .run(tokenHash),
          "AdminSessionRepo.deleteAdminSession",
        );
        return (yield* runChanges(result, "AdminSessionRepo.deleteAdminSession")) === 1; },
      );

      return { create, get, deleteAdminSession };
    }),
  }
) {}
