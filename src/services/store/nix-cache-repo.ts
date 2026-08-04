import { Effect, Option, Schema } from "effect";
import { type DatabaseError, RowDecodeError } from "../../domain/errors.js";
import { NixCacheEntry } from "../../domain/models.js";
import {
  decodeRow,
  decodeRows,
  runChanges,
  SqliteClient,
  tryDb,
} from "./sqlite-client.js";

const NixCacheRow = Schema.Struct({
  cache_key: Schema.String,
  nixpkgs_flake_ref: Schema.String,
  packages_json: Schema.String,
  store_paths_json: Schema.String,
  path_entries_json: Schema.String,
  size_bytes: Schema.Number,
  created_at: Schema.Number,
  updated_at: Schema.Number,
});
type NixCacheRow = Schema.Schema.Type<typeof NixCacheRow>;

const parseStringArray = (
  json: string,
  field: string,
): Effect.Effect<ReadonlyArray<string>, RowDecodeError> =>
  Effect.try({
    try: () => {
      const value: unknown = JSON.parse(json);
      if (
        !Array.isArray(value) ||
        !value.every((item) => typeof item === "string")
      ) {
        throw new Error(`${field} must be a JSON string array`);
      }
      return value;
    },
    catch: (error) =>
      new RowDecodeError({
        message: `Invalid Nix cache ${field}`,
        entity: "NixCacheEntry",
        cause: error instanceof Error ? error.message : String(error),
      }),
  });

const rowToNixCacheEntry = (
  row: NixCacheRow,
): Effect.Effect<NixCacheEntry, RowDecodeError> =>
  Effect.gen(function* () {
    const packages = yield* parseStringArray(row.packages_json, "packages");
    const storePaths = yield* parseStringArray(
      row.store_paths_json,
      "store paths",
    );
    const pathEntries = yield* parseStringArray(
      row.path_entries_json,
      "path entries",
    );
    return yield* decodeRow(
      NixCacheEntry,
      {
        cacheKey: row.cache_key,
        nixpkgsFlakeRef: row.nixpkgs_flake_ref,
        packages,
        storePaths,
        pathEntries,
        sizeBytes: row.size_bytes,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      },
      "NixCacheEntry",
    );
  });

export class NixCacheRepo extends Effect.Service<NixCacheRepo>()(
  "NixCacheRepo",
  {
    accessors: true,
    effect: Effect.gen(function* () {
      const { db } = yield* SqliteClient;

      const get = Effect.fn("NixCacheRepo.get")(function* (
        cacheKey: string,
      ): Effect.fn.Return<
        Option.Option<NixCacheEntry>,
        DatabaseError | RowDecodeError
      > {
        const row = yield* tryDb(
          () =>
            db
              .query<NixCacheRow, [string]>(
                "SELECT * FROM nix_cache WHERE cache_key=?",
              )
              .get(cacheKey),
          "NixCacheRepo.get",
        );
        if (row === null) return Option.none();
        return Option.some(
          yield* rowToNixCacheEntry(
            yield* decodeRow(NixCacheRow, row, "NixCacheEntry"),
          ),
        );
      });

      const upsert = Effect.fn("NixCacheRepo.upsert")(function* (
        entry: NixCacheEntry,
      ): Effect.fn.Return<void, DatabaseError | RowDecodeError> {
        const validEntry = yield* decodeRow(
          NixCacheEntry,
          entry,
          "NixCacheEntry",
        );
        yield* tryDb(
          () =>
            db
              .query(`
              INSERT INTO nix_cache (
                cache_key, nixpkgs_flake_ref, packages_json, store_paths_json, path_entries_json, size_bytes, created_at, updated_at
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT(cache_key) DO UPDATE SET
                nixpkgs_flake_ref=excluded.nixpkgs_flake_ref,
                packages_json=excluded.packages_json,
                store_paths_json=excluded.store_paths_json,
                path_entries_json=excluded.path_entries_json,
                size_bytes=excluded.size_bytes,
                updated_at=excluded.updated_at
            `)
              .run(
                validEntry.cacheKey,
                validEntry.nixpkgsFlakeRef,
                JSON.stringify(validEntry.packages),
                JSON.stringify(validEntry.storePaths),
                JSON.stringify(validEntry.pathEntries),
                validEntry.sizeBytes,
                validEntry.createdAt,
                validEntry.updatedAt,
              ),
          "NixCacheRepo.upsert",
        );
      });

      const list = Effect.fn("NixCacheRepo.list")(
        function* (): Effect.fn.Return<
          ReadonlyArray<NixCacheEntry>,
          DatabaseError | RowDecodeError
        > {
          const rows = yield* tryDb(
            () =>
              db
                .query<NixCacheRow, []>(
                  "SELECT * FROM nix_cache ORDER BY updated_at DESC, cache_key",
                )
                .all(),
            "NixCacheRepo.list",
          );
          const decoded = yield* decodeRows(NixCacheRow, rows, "NixCacheEntry");
          return yield* Effect.forEach(decoded, rowToNixCacheEntry);
        },
      );

      const remove = Effect.fn("NixCacheRepo.remove")(function* (
        cacheKey: string,
      ): Effect.fn.Return<boolean, DatabaseError> {
        const result = yield* tryDb(
          () =>
            db.query("DELETE FROM nix_cache WHERE cache_key=?").run(cacheKey),
          "NixCacheRepo.remove",
        );
        return (yield* runChanges(result, "NixCacheRepo.remove")) === 1;
      });

      return { get, upsert, list, remove };
    }),
  },
) {}
