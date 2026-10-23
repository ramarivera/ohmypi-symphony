import { Database } from "bun:sqlite";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Option, Result, Schema } from "effect";
import { OrganizationId, SessionId, WorkspaceId } from "../src/domain/ids.js";
import { selectDriver } from "../src/drivers/selection.js";
import { RunEventRepo } from "../src/services/store/run-event-repo.js";
import { RunRepo } from "../src/services/store/run-repo.js";
import {
  SqliteClient,
  SqliteClientLive,
} from "../src/services/store/sqlite-client.js";
import { WorkspaceRepo } from "../src/services/store/workspace-repo.js";

import { testPaths } from "./fixtures/paths.js";

const testPath = testPaths();
const organizationId = Schema.decodeUnknownSync(OrganizationId)("org-1");
const id = Schema.decodeUnknownSync(WorkspaceId)("repo-1");
const sessionId = Schema.decodeUnknownSync(SessionId)("session-1");
const layer = Layer.mergeAll(
  WorkspaceRepo.layer,
  RunRepo.layer.pipe(Layer.provide(RunEventRepo.layer)),
).pipe(Layer.provideMerge(SqliteClientLive(":memory:")));

describe("repository driver selection", () => {
  it.live(
    "migrates legacy repository rows additively and reopens idempotently",
    () =>
      Effect.gen(function* () {
        const path = testPath("legacy.sqlite");
        yield* Effect.sync(() => {
          const db = new Database(path);
          try {
            db.exec(`CREATE TABLE repository (
          organization_id TEXT NOT NULL, id TEXT NOT NULL, url TEXT NOT NULL, ref TEXT NOT NULL,
          team_ids_json TEXT NOT NULL, project_ids_json TEXT NOT NULL, labels_json TEXT NOT NULL,
          nix_packages_json TEXT NOT NULL DEFAULT '[]', is_default INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY(organization_id,id)
        ); INSERT INTO repository VALUES ('org-1','repo-1','https://github.com/org/repo','main','[]','[]','[]','[]',0,1,1);`);
          } finally {
            db.close();
          }
        });
        const read = Effect.gen(function* () {
          const repository = Option.getOrThrow(
            yield* (yield* WorkspaceRepo).getRepository(organizationId, id),
          );
          expect(repository.defaultDriver).toBe("omp");
          expect(repository.createdAt).toBe(1);
        });
        for (let count = 0; count < 2; count++)
          yield* read.pipe(
            Effect.provide(
              WorkspaceRepo.layer.pipe(Layer.provide(SqliteClientLive(path))),
            ),
          );
      }),
  );

  it.effect(
    "uses repository defaults and exact, normalized issue label overrides",
    () =>
      Effect.gen(function* () {
        expect(yield* selectDriver("omp", [])).toBe("omp");
        expect(yield* selectDriver("t3", ["bug"])).toBe("t3");
        expect(yield* selectDriver("omp", [" Driver:T3 ", "driver:t3"])).toBe(
          "t3",
        );
        expect(yield* selectDriver("t3", ["driver:omp"])).toBe("omp");
        expect(yield* selectDriver("omp", ["driver:t3-extra"])).toBe("omp");
        expect(
          (yield* selectDriver("omp", ["driver:t3", "driver:omp"]).pipe(
            Effect.flip,
          )).reason,
        ).toBe("rejected");
      }),
  );
  it.effect(
    "persists a default and reads edits on the next run without rebuilding services",
    () =>
      Effect.gen(function* () {
        const repo = yield* WorkspaceRepo;
        const created = yield* repo.createRepository({
          organizationId,
          id,
          url: "https://github.com/org/repo",
          ref: "main",
        });
        expect(created.defaultDriver).toBe("omp");
        yield* repo.updateRepository(organizationId, id, {
          defaultDriver: "t3",
        });
        const current = yield* repo.getRepository(organizationId, id);
        if (Option.isNone(current)) throw new Error("repository missing");
        expect(
          yield* selectDriver(current.value.defaultDriver ?? "omp", []),
        ).toBe("t3");
        yield* repo.updateRepository(organizationId, id, { ref: "next" });
        expect(
          Option.getOrThrow(yield* repo.getRepository(organizationId, id))
            .defaultDriver,
        ).toBe("t3");
      }).pipe(Effect.provide(layer)),
  );
  it.effect(
    "retains JSON and schema causes for corrupt driver session rows",
    () =>
      Effect.gen(function* () {
        const repo = yield* WorkspaceRepo;
        const { db } = yield* SqliteClient;
        yield* (yield* RunRepo).create({
          sessionId,
          organizationId,
          issueId: Option.none(),
        });
        for (const ref of [
          "{broken",
          JSON.stringify({ driver: "t3", nativeId: 123, locator: {} }),
        ]) {
          yield* Effect.sync(() =>
            db
              .query(
                "INSERT INTO driver_session(session_id,ref_json) VALUES (?,?) ON CONFLICT(session_id) DO UPDATE SET ref_json=excluded.ref_json",
              )
              .run(sessionId, ref),
          );
          const result = yield* repo
            .getDriverSession(sessionId)
            .pipe(Effect.result);
          expect(Result.isFailure(result)).toBe(true);
          if (Result.isFailure(result)) {
            expect(result.failure._tag).toBe("@Gateway/RowDecodeError");
            expect(result.failure).toHaveProperty("entity", "driver_session");
            expect(result.failure).toHaveProperty("cause");
            if (result.failure._tag === "@Gateway/RowDecodeError")
              expect(result.failure.cause).toBeTruthy();
          }
        }
      }).pipe(Effect.provide(layer)),
  );
  it.effect(
    "persists native driver refs separately from omp session files",
    () =>
      Effect.gen(function* () {
        const repo = yield* WorkspaceRepo;
        const runs = yield* RunRepo;
        yield* runs.create({
          sessionId,
          organizationId,
          issueId: Option.none(),
        });
        expect(yield* repo.getDriverSession(sessionId)).toEqual(Option.none());
        const ref = {
          driver: "t3" as const,
          nativeId: "thread-1",
          locator: { projectId: "project-1", origin: "http://t3:3773" },
        };
        yield* repo.setDriverSession(sessionId, ref);
        expect(
          Option.getOrThrow(yield* repo.getDriverSession(sessionId)),
        ).toEqual(ref);
        expect(
          Option.getOrThrow(yield* runs.get(sessionId)).ompSessionFile,
        ).toEqual(Option.none());
      }).pipe(Effect.provide(layer)),
  );
});
