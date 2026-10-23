import type { Database } from "bun:sqlite";
import { Effect } from "effect";
import type { TokenCipherError } from "../domain/errors.js";
import { transact, tryDb } from "./store/sqlite-client.js";

export const tokenContext = (
  table: string,
  org: string,
  id: string,
  field: string,
): string => JSON.stringify([table, org, id, field]);
interface Cipher {
  encrypt: (
    value: string,
    context?: string,
  ) => Effect.Effect<string, TokenCipherError>;
  decrypt: (
    value: string,
    context?: string,
  ) => Effect.Effect<string, TokenCipherError>;
  needsReencrypt: (value: string) => boolean;
}
const PREFIX = "mcpenc:v1:";
/** Atomic startup migration. Retain old keys until every gateway has upgraded. */
export const migrateTokenRows = (db: Database, cipher: Cipher) =>
  transact(
    db,
    Effect.gen(function* () {
      for (const spec of [
        {
          table: "installation",
          id: "organization_id",
          fields: ["access_token", "refresh_token"],
          json: [],
        },
        {
          table: "executor_instance",
          id: "organization_id",
          fields: ["token"],
          json: [],
        },
        {
          table: "mcp_server",
          id: "id",
          fields: ["oauth_client_json"],
          json: ["env_json", "headers_json"],
        },
        {
          table: "mcp_oauth_state",
          id: "state_hash",
          fields: ["code_verifier", "client_json"],
          json: [],
        },
        {
          table: "mcp_oauth_credential",
          id: "server_id",
          fields: ["access_token", "refresh_token", "client_json"],
          json: [],
        },
      ]) {
        // Table and column identifiers are from this constant list, never user input.
        const rows = yield* tryDb(
          () =>
            db
              .query<Record<string, string | null>, []>(
                `SELECT * FROM ${spec.table}`,
              )
              .all(),
          "TokenMigration.read",
        );
        for (const row of rows) {
          const org = row.organization_id;
          const id = row[spec.id];
          if (!org || !id) continue;
          const migrateValue = (value: string, field: string) =>
            Effect.gen(function* () {
              const prefixed = value.startsWith(PREFIX);
              if (!prefixed && spec.table !== "installation") return value;
              const payload = prefixed ? value.slice(PREFIX.length) : value;
              if (!cipher.needsReencrypt(payload)) return value;
              const context = tokenContext(spec.table, org, id, field);
              const plaintext = yield* cipher.decrypt(payload, context);
              const next = yield* cipher.encrypt(plaintext, context);
              return prefixed ? `${PREFIX}${next}` : next;
            });
          for (const field of spec.fields) {
            const current = row[field];
            if (typeof current !== "string") continue;
            const next = yield* migrateValue(current, field);
            if (next !== current)
              yield* tryDb(
                () =>
                  db
                    .query(
                      `UPDATE ${spec.table} SET ${field} = ? WHERE organization_id = ? AND ${spec.id} = ?`,
                    )
                    .run(next, org, id),
                "TokenMigration.write",
              );
          }
          for (const field of spec.json) {
            const current = row[field];
            if (typeof current !== "string") continue;
            const values = yield* tryDb(
              () => JSON.parse(current) as Record<string, string>,
              "TokenMigration.json",
            );
            let changed = false;
            for (const [key, value] of Object.entries(values)) {
              const next = yield* migrateValue(value, `${field}:${key}`);
              if (next !== value) {
                values[key] = next;
                changed = true;
              }
            }
            if (changed)
              yield* tryDb(
                () =>
                  db
                    .query(
                      `UPDATE ${spec.table} SET ${field} = ? WHERE organization_id = ? AND ${spec.id} = ?`,
                    )
                    .run(JSON.stringify(values), org, id),
                "TokenMigration.writeJson",
              );
          }
        }
      }
    }),
  );
