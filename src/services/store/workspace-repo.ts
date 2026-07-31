import { Clock, Effect, Option, Schema } from "effect";
import type { OrganizationId, SessionId, WorkspaceId } from "../../domain/ids.js";
import type { TeamId, ProjectId } from "../../domain/ids.js";
import { RepositoryRecord } from "../../domain/models.js";
import { DatabaseError, RowDecodeError } from "../../domain/errors.js";
import { SqliteClient, tryDb, runChanges, decodeRow, decodeRows, transact } from "./sqlite-client.js";

const RepositoryRow = Schema.Struct({
  organization_id: Schema.String,
  id: Schema.String,
  url: Schema.String,
  ref: Schema.String,
  team_ids_json: Schema.String,
  project_ids_json: Schema.String,
  labels_json: Schema.String,
  is_default: Schema.Number,
  created_at: Schema.Number,
  updated_at: Schema.Number,
});

type RepositoryRow = Schema.Schema.Type<typeof RepositoryRow>;

const SAFE_ID_RE = /^[a-zA-Z0-9_.-]+$/u;

function validateString(value: unknown, field: string): string {
  if (typeof value !== "string") throw new Error(`${field} must be a string`);
  return value.trim();
}

function isSafeArgument(value: string): boolean {
  if (value.startsWith("-") || /\s/u.test(value)) return false;
  return [...value].every((character) => {
    const code = character.codePointAt(0);
    return code !== undefined && code > 31 && code !== 127;
  });
}

function normalizeStringArray(
  value: unknown,
  field: string,
): ReadonlyArray<string> {
  if (!Array.isArray(value)) throw new Error(`${field} must be an array`);
  const seen = new Set<string>();
  const out: Array<string> = [];
  for (const item of value) {
    if (typeof item !== "string")
      throw new Error(`${field} must contain only strings`);
    const trimmed = item.trim().toLowerCase();
    if (!trimmed) continue;
    if (seen.has(trimmed)) continue;
    seen.add(trimmed);
    out.push(trimmed);
  }
  return out.sort();
}

const validateRepository = (
  input: {
    readonly organizationId: OrganizationId;
    readonly id: WorkspaceId;
    readonly url: string;
    readonly ref: string;
    readonly teamIds?: ReadonlyArray<TeamId>;
    readonly projectIds?: ReadonlyArray<ProjectId>;
    readonly labels?: ReadonlyArray<string>;
    readonly isDefault?: boolean;
  },
  now: number,
): Effect.Effect<RepositoryRecord, RowDecodeError> =>
  Effect.try({
    try: () => {
      const organizationId = validateString(input.organizationId, "organizationId");
      if (!organizationId) throw new Error("organizationId must not be empty");
      const id = validateString(input.id, "id");
      if (!id || !SAFE_ID_RE.test(id))
        throw new Error("id must be a non-empty safe identifier");
      const url = validateString(input.url, "url");
      if (!url || !isSafeArgument(url))
        throw new Error("url must be a non-empty safe URL");
      const ref = validateString(input.ref, "ref");
      if (!ref || !isSafeArgument(ref))
        throw new Error("ref must be a non-empty safe ref");
      return Schema.decodeUnknownSync(RepositoryRecord)({
        organizationId,
        id,
        url,
        ref,
        teamIds: normalizeStringArray(input.teamIds ?? [], "teamIds"),
        projectIds: normalizeStringArray(input.projectIds ?? [], "projectIds"),
        labels: normalizeStringArray(input.labels ?? [], "labels"),
        isDefault: input.isDefault === true,
        createdAt: now,
        updatedAt: now,
      });
    },
    catch: (error) =>
      new RowDecodeError({
        message: error instanceof Error ? error.message : String(error),
        entity: "RepositoryRecord",
        cause: String(error),
      }),
  });

const rowToRepositoryRecord = (
  row: RepositoryRow,
): Effect.Effect<RepositoryRecord, RowDecodeError> =>
  Effect.gen(function* () {
    const teamIds = yield* parseStringArray(
      row.team_ids_json,
      "repository team ids",
    );
    const projectIds = yield* parseStringArray(
      row.project_ids_json,
      "repository project ids",
    );
    const labels = yield* parseStringArray(row.labels_json, "repository labels");
    const validTeamIds = yield* decodeRow(
      Schema.Array(Schema.String),
      teamIds,
      "RepositoryRecord.teamIds",
    );
    const validProjectIds = yield* decodeRow(
      Schema.Array(Schema.String),
      projectIds,
      "RepositoryRecord.projectIds",
    );
    const validLabels = yield* decodeRow(
      Schema.Array(Schema.String),
      labels,
      "RepositoryRecord.labels",
    );
    return yield* decodeRow(
      RepositoryRecord,
      {
        organizationId: row.organization_id,
        id: row.id,
        url: row.url,
        ref: row.ref,
        teamIds: validTeamIds,
        projectIds: validProjectIds,
        labels: validLabels,
        isDefault: row.is_default === 1,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      },
      "RepositoryRecord",
    );
  });

const parseStringArray = (
  json: string,
  label: string,
): Effect.Effect<ReadonlyArray<string>, RowDecodeError> =>
  Effect.gen(function* () {
    const value = yield* Effect.try({
      try: () => JSON.parse(json) as unknown,
      catch: (error) =>
        new RowDecodeError({
          message: `Invalid JSON in ${label}`,
          entity: "RepositoryRecord",
          cause: error instanceof Error ? error.message : String(error),
        }),
    });
    if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) {
      return yield* Effect.fail(new RowDecodeError({
        message: `Invalid string array in ${label}`,
        entity: "RepositoryRecord",
      }));
    }
    return value;
  });

export class WorkspaceRepo extends Effect.Service<WorkspaceRepo>()(
  "WorkspaceRepo",
  {
    accessors: true,
    effect: Effect.gen(function* () {
      const { db } = yield* SqliteClient;

      const setWorkspace = Effect.fn("WorkspaceRepo.setWorkspace")(
        function* (input: {
          readonly sessionId: SessionId;
          readonly path: string;
          readonly repositoryId: WorkspaceId;
          readonly url: string;
          readonly ref: string;
          readonly state: string;
        }) { yield* Effect.annotateCurrentSpan("sessionId", input.sessionId);
        const now = yield* Clock.currentTimeMillis;
        yield* tryDb(() =>
          db
            .query(`
              INSERT INTO workspace (session_id, canonical_path, repository_id, repository_url, repository_ref, state, created_at, updated_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT(session_id) DO UPDATE SET canonical_path=excluded.canonical_path, repository_id=excluded.repository_id,
                repository_url=excluded.repository_url, repository_ref=excluded.repository_ref, state=excluded.state, updated_at=excluded.updated_at
            `)
            .run(
              input.sessionId,
              input.path,
              input.repositoryId,
              input.url,
              input.ref,
              input.state,
              now,
              now,
            ), "WorkspaceRepo.setWorkspace"); },
      );

      const createRepository = Effect.fn("WorkspaceRepo.createRepository")(
        function* (input: {
          readonly organizationId: OrganizationId;
          readonly id: WorkspaceId;
          readonly url: string;
          readonly ref: string;
          readonly teamIds?: ReadonlyArray<TeamId>;
          readonly projectIds?: ReadonlyArray<ProjectId>;
          readonly labels?: ReadonlyArray<string>;
          readonly isDefault?: boolean;
          readonly now?: number;
        }) { yield* Effect.annotateCurrentSpan("repositoryId", input.id);
        const now = input.now ?? (yield* Clock.currentTimeMillis);
        const record = yield* validateRepository(input, now);

        const tx = Effect.gen(function* () {
          const existing = yield* tryDb(
            () =>
              db
                .query<RepositoryRow, [string, string]>(
                  "SELECT organization_id FROM repository WHERE organization_id=? AND id=?",
                )
                .get(record.organizationId, record.id),
            "WorkspaceRepo.createRepository.select",
          );
          if (existing !== null) {
            return yield* Effect.fail(new DatabaseError({ message: `Repository ${record.id} already exists for ${record.organizationId}` }))
          }

          if (record.isDefault) {
            yield* tryDb(
              () =>
                db
                  .query(
                    "UPDATE repository SET is_default=0, updated_at=? WHERE organization_id=? AND is_default=1",
                  )
                  .run(now, record.organizationId),
              "WorkspaceRepo.createRepository.default",
            );
          }

          yield* tryDb(
            () =>
              db
                .query(`
                  INSERT INTO repository (
                    organization_id, id, url, ref, team_ids_json, project_ids_json, labels_json, is_default, created_at, updated_at
                  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                `)
                .run(
                  record.organizationId,
                  record.id,
                  record.url,
                  record.ref,
                  JSON.stringify(record.teamIds),
                  JSON.stringify(record.projectIds),
                  JSON.stringify(record.labels),
                  record.isDefault ? 1 : 0,
                  record.createdAt,
                  record.updatedAt,
                ),
            "WorkspaceRepo.createRepository.insert",
          );

          return record;
        });

        return yield* transact(db, tx); },
      );

      const updateRepository = Effect.fn("WorkspaceRepo.updateRepository")(
        function* (organizationId: OrganizationId,
        id: WorkspaceId,
        input: {
          readonly url?: string;
          readonly ref?: string;
          readonly teamIds?: ReadonlyArray<TeamId>;
          readonly projectIds?: ReadonlyArray<ProjectId>;
          readonly labels?: ReadonlyArray<string>;
          readonly isDefault?: boolean;
          readonly now?: number;
        },) { yield* Effect.annotateCurrentSpan("repositoryId", id);
        const current = yield* getRepository(organizationId, id);
        if (Option.isNone(current)) {
          return yield* Effect.fail(new DatabaseError({ message: `Repository ${id} not found for ${organizationId}` }))
        }
        const existing = current.value;
        const now = input.now ?? (yield* Clock.currentTimeMillis);

        const patch = yield* validateRepository(
          {
            organizationId,
            id,
            url: input.url ?? existing.url,
            ref: input.ref ?? existing.ref,
            teamIds: input.teamIds ?? existing.teamIds,
            projectIds: input.projectIds ?? existing.projectIds,
            labels: input.labels ?? existing.labels,
            isDefault: input.isDefault ?? existing.isDefault,
          },
          now,
        );

        const tx = Effect.gen(function* () {
          if (patch.isDefault && !existing.isDefault) {
            yield* tryDb(
              () =>
                db
                  .query(
                    "UPDATE repository SET is_default=0, updated_at=? WHERE organization_id=? AND is_default=1",
                  )
                  .run(now, organizationId),
              "WorkspaceRepo.updateRepository.default",
            );
          }

          yield* tryDb(
            () =>
              db
                .query(`
                  UPDATE repository
                  SET url=?, ref=?, team_ids_json=?, project_ids_json=?, labels_json=?, is_default=?, updated_at=?
                  WHERE organization_id=? AND id=?
                `)
                .run(
                  patch.url,
                  patch.ref,
                  JSON.stringify(patch.teamIds),
                  JSON.stringify(patch.projectIds),
                  JSON.stringify(patch.labels),
                  patch.isDefault ? 1 : 0,
                  now,
                  organizationId,
                  id,
                ),
            "WorkspaceRepo.updateRepository.update",
          );

          return yield* decodeRow(
            RepositoryRecord,
            {
              ...patch,
              createdAt: existing.createdAt,
              updatedAt: now,
            },
            "RepositoryRecord",
          );
        });

        return yield* transact(db, tx); },
      );

      const deleteRepository = Effect.fn("WorkspaceRepo.deleteRepository")(
        function* (organizationId: OrganizationId,
        id: WorkspaceId,) { yield* Effect.annotateCurrentSpan("repositoryId", id);
        const result = yield* tryDb(
          () =>
            db
              .query("DELETE FROM repository WHERE organization_id=? AND id=?")
              .run(organizationId, id),
          "WorkspaceRepo.deleteRepository",
        );
        return (yield* runChanges(result, "WorkspaceRepo.deleteRepository")) === 1; },
      );

      const getRepository = Effect.fn("WorkspaceRepo.getRepository")(
        function* (organizationId: OrganizationId,
        id: WorkspaceId,) { yield* Effect.annotateCurrentSpan("repositoryId", id);
        const row = yield* tryDb(
          () =>
            db
              .query<RepositoryRow, [string, string]>(
                "SELECT * FROM repository WHERE organization_id=? AND id=?",
              )
              .get(organizationId, id),
          "WorkspaceRepo.getRepository",
        );
        if (row === null) return Option.none();
        const decoded = yield* decodeRow(RepositoryRow, row, "RepositoryRecord");
        const record = yield* rowToRepositoryRecord(decoded);
        return Option.some(record); },
      );

      const listRepositories = Effect.fn("WorkspaceRepo.listRepositories")(
        function* (organizationId: OrganizationId,) { yield* Effect.annotateCurrentSpan("organizationId", organizationId);
        const rows = yield* tryDb(
          () =>
            db
              .query<RepositoryRow, [string]>(
                "SELECT * FROM repository WHERE organization_id=? ORDER BY created_at, id",
              )
              .all(organizationId),
          "WorkspaceRepo.listRepositories",
        );
        const decoded = yield* decodeRows(RepositoryRow, rows, "RepositoryRecord");
        return yield* Effect.forEach(decoded, rowToRepositoryRecord); },
      );

      const getDefaultRepository = Effect.fn("WorkspaceRepo.getDefaultRepository")(
        function* (organizationId: OrganizationId,) { yield* Effect.annotateCurrentSpan("organizationId", organizationId);
        const row = yield* tryDb(
          () =>
            db
              .query<RepositoryRow, [string]>(
                "SELECT * FROM repository WHERE organization_id=? AND is_default=1 LIMIT 1",
              )
              .get(organizationId),
          "WorkspaceRepo.getDefaultRepository",
        );
        if (row === null) return Option.none();
        const decoded = yield* decodeRow(RepositoryRow, row, "RepositoryRecord");
        const record = yield* rowToRepositoryRecord(decoded);
        return Option.some(record); },
      );

      return {
        setWorkspace,
        createRepository,
        updateRepository,
        deleteRepository,
        getRepository,
        listRepositories,
        getDefaultRepository,
      };
    }),
  }
) {}
