import { Effect, Layer, Option, Schema } from "effect";
import { describe, expect, it } from "vitest";
import {
  NixCacheEntry,
  NixPackageName,
  normalizeNixPackages,
} from "../src/domain/models.js";
import {
  NixCacheRepo,
  WorkspaceRepo,
} from "../src/services/store/repositories.js";
import {
  SqliteClient,
  SqliteClientLive,
} from "../src/services/store/sqlite-client.js";

const cacheKey = "a".repeat(64);
const storeLayer = Layer.mergeAll(
  NixCacheRepo.Default,
  WorkspaceRepo.Default,
).pipe(Layer.provideMerge(SqliteClientLive(":memory:")));

const withStore = <A, E>(
  effect: Effect.Effect<A, E, NixCacheRepo | WorkspaceRepo | SqliteClient>,
) => Effect.runPromise(effect.pipe(Effect.provide(storeLayer)));

const entry = Schema.decodeUnknownSync(NixCacheEntry)({
  cacheKey,
  nixpkgsFlakeRef:
    "github:NixOS/nixpkgs/0123456789abcdef0123456789abcdef01234567",
  packages: ["nodejs_22", "ripgrep"],
  storePaths: ["/nix/store/node", "/nix/store/rg"],
  pathEntries: ["/nix/store/node/bin", "/nix/store/rg/bin"],
  sizeBytes: 456,
  createdAt: 10,
  updatedAt: 20,
});

describe("Nix package domain", () => {
  it("accepts conservative attribute paths and rejects unsafe names", () => {
    expect(
      Schema.decodeUnknownEither(NixPackageName)("python3Packages.requests")
        ._tag,
    ).toBe("Right");
    for (const value of [
      "",
      "../nix",
      "pkg/name",
      "pkg:name",
      "pkg name",
      'pkg"name',
      ".hidden",
      "trailing.",
    ]) {
      expect(Schema.decodeUnknownEither(NixPackageName)(value)._tag).toBe(
        "Left",
      );
    }
  });

  it("canonicalizes packages by sorting and deduplicating", () => {
    expect(normalizeNixPackages(["ripgrep", "nodejs_22", "ripgrep"])).toEqual([
      "nodejs_22",
      "ripgrep",
    ]);
  });
});

describe("Nix cache repository", () => {
  it("round-trips cache metadata and removes it by cache key", async () => {
    await withStore(
      Effect.gen(function* () {
        const repo = yield* NixCacheRepo;
        yield* repo.upsert(entry);
        const found = yield* repo.get(cacheKey);
        expect(Option.isSome(found)).toBe(true);
        if (Option.isSome(found)) expect(found.value).toEqual(entry);
        expect(yield* repo.list()).toEqual([entry]);
        expect(yield* repo.remove(cacheKey)).toBe(true);
        expect(Option.isNone(yield* repo.get(cacheKey))).toBe(true);
      }),
    );
  });

  it("decodes legacy repository rows with the empty package default", async () => {
    await withStore(
      Effect.gen(function* () {
        const { db } = yield* SqliteClient;
        const repo = yield* WorkspaceRepo;
        db.query(`
          INSERT INTO repository (
            organization_id, id, url, ref, team_ids_json, project_ids_json, labels_json, is_default, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).run(
          "legacy-org",
          "legacy-repository",
          "https://example.test/legacy.git",
          "main",
          "[]",
          "[]",
          "[]",
          0,
          1,
          1,
        );
        const loaded = yield* repo.getRepository(
          "legacy-org" as never,
          "legacy-repository" as never,
        );
        expect(Option.isSome(loaded)).toBe(true);
        if (Option.isSome(loaded)) expect(loaded.value.nixPackages).toEqual([]);
      }),
    );
  });

  it("defaults omitted repository packages to an empty array", async () => {
    await withStore(
      Effect.gen(function* () {
        const repo = yield* WorkspaceRepo;
        const created = yield* repo.createRepository({
          organizationId: "org" as never,
          id: "repository" as never,
          url: "https://example.test/repository.git",
          ref: "main",
        });
        expect(created.nixPackages).toEqual([]);
      }),
    );
  });
});
