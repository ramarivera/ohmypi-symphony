import { Clock, Effect, Option, Schema } from "effect";
import type {
  DatabaseError,
  RowDecodeError,
  TokenCipherError,
} from "../../domain/errors.js";
import {
  OrganizationId,
  type OrganizationId as OrganizationIdType,
} from "../../domain/ids.js";
import { TokenCrypto } from "../token-crypto.js";
import { decodeRow, runChanges, SqliteClient, tryDb } from "./sqlite-client.js";

const ExecutorInstanceRow = Schema.Struct({
  organization_id: Schema.String,
  endpoint: Schema.String,
  token: Schema.String,
  updated_at: Schema.Number,
});
type ExecutorInstanceRow = Schema.Schema.Type<typeof ExecutorInstanceRow>;

const PREFIX = "mcpenc:v1:";

export interface ExecutorInstance {
  readonly organizationId: OrganizationIdType;
  readonly endpoint: string;
  readonly token: string;
  readonly updatedAt: number;
}

const rowToInstance = (
  crypto: TokenCrypto,
  row: ExecutorInstanceRow,
): Effect.Effect<ExecutorInstance, RowDecodeError | TokenCipherError> =>
  Effect.gen(function* () {
    const organizationId = yield* decodeRow(
      OrganizationId,
      row.organization_id,
      "ExecutorInstance.organizationId",
    );
    const token = row.token.startsWith(PREFIX)
      ? yield* crypto.decrypt(row.token.slice(PREFIX.length))
      : row.token;
    return {
      organizationId,
      endpoint: row.endpoint,
      token,
      updatedAt: row.updated_at,
    };
  });

export class ExecutorInstanceRepo extends Effect.Service<ExecutorInstanceRepo>()(
  "ExecutorInstanceRepo",
  {
    accessors: true,
    dependencies: [TokenCrypto.Default],
    effect: Effect.gen(function* () {
      const { db } = yield* SqliteClient;
      const crypto = yield* TokenCrypto;

      const get = Effect.fn("ExecutorInstanceRepo.get")(function* (
        organizationId: OrganizationIdType,
      ): Effect.fn.Return<
        Option.Option<ExecutorInstance>,
        DatabaseError | RowDecodeError | TokenCipherError
      > {
        const row = yield* tryDb(
          () =>
            db
              .query<ExecutorInstanceRow, [string]>(
                "SELECT * FROM executor_instance WHERE organization_id=?",
              )
              .get(organizationId),
          "ExecutorInstanceRepo.get",
        );
        if (row === null) return Option.none();
        return Option.some(
          yield* rowToInstance(
            crypto,
            yield* decodeRow(ExecutorInstanceRow, row, "ExecutorInstance"),
          ),
        );
      });

      const put = Effect.fn("ExecutorInstanceRepo.put")(function* (input: {
        readonly organizationId: OrganizationIdType;
        readonly endpoint: string;
        readonly token: string;
        readonly updatedAt?: number;
      }): Effect.fn.Return<
        ExecutorInstance,
        DatabaseError | RowDecodeError | TokenCipherError
      > {
        const updatedAt = input.updatedAt ?? (yield* Clock.currentTimeMillis);
        const encrypted = yield* crypto.encrypt(input.token);
        yield* tryDb(
          () =>
            db
              .query(
                `INSERT INTO executor_instance (organization_id, endpoint, token, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(organization_id) DO UPDATE SET endpoint=excluded.endpoint, token=excluded.token, updated_at=excluded.updated_at`,
              )
              .run(
                input.organizationId,
                input.endpoint,
                `${PREFIX}${encrypted}`,
                updatedAt,
              ),
          "ExecutorInstanceRepo.put",
        );
        return {
          organizationId: input.organizationId,
          endpoint: input.endpoint,
          token: input.token,
          updatedAt,
        };
      });

      const remove = Effect.fn("ExecutorInstanceRepo.remove")(function* (
        organizationId: OrganizationIdType,
      ): Effect.fn.Return<boolean, DatabaseError> {
        const result = yield* tryDb(
          () =>
            db
              .query("DELETE FROM executor_instance WHERE organization_id=?")
              .run(organizationId),
          "ExecutorInstanceRepo.remove",
        );
        return (yield* runChanges(result, "ExecutorInstanceRepo.remove")) === 1;
      });

      return { get, put, remove };
    }),
  },
) {}
