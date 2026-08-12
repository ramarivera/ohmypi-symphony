import { Database } from "bun:sqlite";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { Context, Effect, Exit, Layer, ParseResult, Schema } from "effect";
import { DatabaseError, RowDecodeError } from "../../domain/errors.js";

export interface SqliteClientShape {
  readonly db: Database;
}

export class SqliteClient extends Context.Tag("SqliteClient")<
  SqliteClient,
  SqliteClientShape
>() {}

export const tryDb = <A>(
  f: () => A,
  operation: string,
): Effect.Effect<A, DatabaseError> =>
  Effect.try({
    try: f,
    catch: (error) =>
      new DatabaseError({
        message: `${operation} failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
        cause: error instanceof Error ? error.stack : String(error),
      }),
  });

export const transact = <A, E, R>(
  db: Database,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | DatabaseError, R> =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const begin = () => db.exec("BEGIN IMMEDIATE");
      const commit = () => db.exec("COMMIT");
      const rollback = () => db.exec("ROLLBACK");

      // Keep the begin operation interruptible, then mask the hand-off to the
      // use/release section so an interrupt cannot land between BEGIN and the
      // finalizer that rolls it back.
      yield* restore(tryDb(begin, "BEGIN"));
      const result = yield* Effect.exit(restore(effect));

      return yield* Exit.matchEffect(result, {
        onSuccess: (value) =>
          tryDb(commit, "COMMIT").pipe(
            Effect.catchTag("@Gateway/DatabaseError", (error) =>
              Effect.gen(function* () {
                yield* Effect.orElse(
                  tryDb(rollback, "ROLLBACK"),
                  () => Effect.void,
                );
                return yield* Effect.fail(error);
              }),
            ),
            Effect.map(() => value),
          ),
        onFailure: (cause) =>
          tryDb(rollback, "ROLLBACK").pipe(
            Effect.orElse(() => Effect.void),
            Effect.zipRight(Effect.failCause(cause)),
          ),
      });
    }),
  );

export const decodeRow = <A, I, R>(
  schema: Schema.Schema<A, I, R>,
  row: unknown,
  entity: string,
): Effect.Effect<A, RowDecodeError, R> =>
  Schema.decodeUnknown(schema)(row).pipe(
    Effect.catchTag(
      "ParseError",
      (error) =>
        new RowDecodeError({
          message: ParseResult.TreeFormatter.formatErrorSync(error),
          entity,
          cause: String(error),
        }),
    ),
  );

export const decodeRows = <A, I, R>(
  schema: Schema.Schema<A, I, R>,
  rows: ReadonlyArray<unknown>,
  entity: string,
): Effect.Effect<ReadonlyArray<A>, RowDecodeError, R> =>
  Effect.forEach(rows, (row) => decodeRow(schema, row, entity));

const RunResult = Schema.Struct({ changes: Schema.Number });

export const runChanges = (
  result: unknown,
  operation: string,
): Effect.Effect<number, DatabaseError> =>
  Schema.decodeUnknown(RunResult)(result).pipe(
    Effect.map((r) => r.changes),
    Effect.catchTag(
      "ParseError",
      () =>
        new DatabaseError({
          message: `${operation}: run result did not contain changes`,
        }),
    ),
  );

const migrate = (db: Database): void => {
  db.exec(`
    CREATE TABLE IF NOT EXISTS installation (
      organization_id TEXT PRIMARY KEY,
      app_user_id TEXT NOT NULL,
      accessible_team_ids_json TEXT,
      can_access_all_public_teams INTEGER,
      access_token TEXT NOT NULL,
      refresh_token TEXT NOT NULL,
      expires_at INTEGER NOT NULL,
      scopes_json TEXT NOT NULL,
      revoked_at INTEGER
    );
    CREATE TABLE IF NOT EXISTS oauth_state (
      state_hash TEXT PRIMARY KEY,
      created_at INTEGER NOT NULL,
      expires_at INTEGER NOT NULL,
      consumed_at INTEGER
    );
    CREATE TABLE IF NOT EXISTS webhook_delivery (
      delivery_id TEXT PRIMARY KEY,
      organization_id TEXT NOT NULL,
      received_at INTEGER NOT NULL,
      payload_hash TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      error TEXT
    );
    CREATE TABLE IF NOT EXISTS agent_run (
      session_id TEXT PRIMARY KEY,
      organization_id TEXT NOT NULL,
      issue_id TEXT,
      repository_id TEXT,
      state TEXT NOT NULL,
      desired_state TEXT NOT NULL DEFAULT 'running',
      omp_session_id TEXT,
      omp_session_file TEXT,
      workspace_path TEXT,
      attempt INTEGER NOT NULL DEFAULT 0,
      lease_owner TEXT,
      lease_expires_at INTEGER,
      last_activity_at INTEGER,
      terminal_reason TEXT,
      team_id TEXT,
      project_id TEXT,
      next_attempt_at INTEGER,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS run_input (
      id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL REFERENCES agent_run(session_id) ON DELETE CASCADE,
      kind TEXT NOT NULL,
      body TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      processed_at INTEGER
    );
    CREATE INDEX IF NOT EXISTS run_input_pending ON run_input(session_id, processed_at, created_at);
    CREATE TABLE IF NOT EXISTS activity_projection (
      source_key TEXT PRIMARY KEY,
      session_id TEXT NOT NULL REFERENCES agent_run(session_id) ON DELETE CASCADE,
      activity_type TEXT NOT NULL,
      payload_hash TEXT NOT NULL,
      linear_activity_id TEXT,
      status TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS projection_outbox (
      source_key TEXT PRIMARY KEY REFERENCES activity_projection(source_key) ON DELETE CASCADE,
      session_id TEXT NOT NULL REFERENCES agent_run(session_id) ON DELETE CASCADE,
      activity_type TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      attempt INTEGER NOT NULL DEFAULT 0,
      next_attempt_at INTEGER NOT NULL DEFAULT 0,
      lease_owner TEXT,
      lease_expires_at INTEGER,
      last_error TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS projection_outbox_due
      ON projection_outbox(status, next_attempt_at, lease_expires_at);
    CREATE TABLE IF NOT EXISTS workspace (
      session_id TEXT PRIMARY KEY REFERENCES agent_run(session_id) ON DELETE CASCADE,
      canonical_path TEXT NOT NULL UNIQUE,
      repository_id TEXT NOT NULL,
      repository_url TEXT NOT NULL,
      repository_ref TEXT NOT NULL,
      state TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS repository (
      organization_id TEXT NOT NULL,
      id TEXT NOT NULL,
      url TEXT NOT NULL,
      ref TEXT NOT NULL,
      team_ids_json TEXT NOT NULL,
      project_ids_json TEXT NOT NULL,
      labels_json TEXT NOT NULL,
      nix_packages_json TEXT NOT NULL DEFAULT '[]',
      is_default INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY (organization_id, id)
    );
    CREATE TABLE IF NOT EXISTS mcp_server (
      organization_id TEXT NOT NULL,
      id TEXT NOT NULL,
      name TEXT NOT NULL,
      transport TEXT NOT NULL,
      command TEXT,
      args_json TEXT NOT NULL,
      url TEXT,
      env_json TEXT NOT NULL,
      headers_json TEXT NOT NULL DEFAULT '{}',
      repository_id TEXT,
      enabled INTEGER NOT NULL DEFAULT 1,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY (organization_id, id)
    );
    CREATE INDEX IF NOT EXISTS mcp_server_scope
      ON mcp_server(organization_id, repository_id, enabled);
    CREATE UNIQUE INDEX IF NOT EXISTS mcp_server_scope_name_unique
      ON mcp_server(organization_id, COALESCE(repository_id, ''), name);
    CREATE TABLE IF NOT EXISTS admin_session (
      token_hash TEXT PRIMARY KEY,
      organization_id TEXT NOT NULL,
      csrf_token_hash TEXT NOT NULL,
      expires_at INTEGER NOT NULL,
      created_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS prompt_template (
      organization_id TEXT NOT NULL,
      kind TEXT NOT NULL CHECK (kind IN ('created', 'prompted', 'contract')),
      body TEXT NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY (organization_id, kind)
    );
    CREATE INDEX IF NOT EXISTS admin_session_org ON admin_session(organization_id);
    CREATE INDEX IF NOT EXISTS agent_run_org_issue ON agent_run(organization_id, issue_id);
    CREATE UNIQUE INDEX IF NOT EXISTS repository_default
      ON repository(organization_id, is_default)
      WHERE is_default = 1;
    CREATE TABLE IF NOT EXISTS run_event (
      source_key TEXT PRIMARY KEY,
      session_id TEXT NOT NULL REFERENCES agent_run(session_id) ON DELETE CASCADE,
      kind TEXT NOT NULL,
      level TEXT NOT NULL,
      text TEXT,
      payload_json TEXT NOT NULL,
      status TEXT,
      error TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS run_event_session
      ON run_event(session_id, created_at, source_key);
  `);

  const repositoryColumns = db
    .query<{ name: string }, []>('PRAGMA table_info("repository")')
    .all()
    .map((column) => column.name);
  if (!repositoryColumns.includes("nix_packages_json")) {
    db.exec(
      "ALTER TABLE repository ADD COLUMN nix_packages_json TEXT NOT NULL DEFAULT '[]'",
    );
  }
  const mcpServerColumns = db
    .query<{ name: string }, []>('PRAGMA table_info("mcp_server")')
    .all()
    .map((column) => column.name);
  if (!mcpServerColumns.includes("headers_json")) {
    db.exec(
      "ALTER TABLE mcp_server ADD COLUMN headers_json TEXT NOT NULL DEFAULT '{}'",
    );
  }

  db.exec(`
    CREATE TABLE IF NOT EXISTS nix_cache (
      cache_key TEXT PRIMARY KEY,
      nixpkgs_flake_ref TEXT NOT NULL,
      packages_json TEXT NOT NULL,
      store_paths_json TEXT NOT NULL,
      path_entries_json TEXT NOT NULL,
      size_bytes INTEGER NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
  `);

  const indexes = db
    .query<{ name: string; unique: number; origin: string }, []>(
      'PRAGMA index_list("activity_projection")',
    )
    .all();
  if (indexes.some((index) => index.unique === 1 && index.origin === "u")) {
    db.exec(`
      PRAGMA foreign_keys=OFF;
      BEGIN IMMEDIATE;
      CREATE TABLE activity_projection_v2 (
        source_key TEXT PRIMARY KEY,
        session_id TEXT NOT NULL REFERENCES agent_run(session_id) ON DELETE CASCADE,
        activity_type TEXT NOT NULL,
        payload_hash TEXT NOT NULL,
        linear_activity_id TEXT,
        status TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      INSERT INTO activity_projection_v2
        (source_key, session_id, activity_type, payload_hash, linear_activity_id, status, created_at, updated_at)
        SELECT source_key, session_id, activity_type, payload_hash, linear_activity_id, status, created_at, updated_at
        FROM activity_projection;
      DROP TABLE activity_projection;
      ALTER TABLE activity_projection_v2 RENAME TO activity_projection;
      COMMIT;
      PRAGMA foreign_keys=ON;
    `);
  }
  db.exec("PRAGMA user_version=1");
};

export const SqliteClientLive = (
  path: string,
): Layer.Layer<SqliteClient, DatabaseError, never> =>
  Layer.scoped(
    SqliteClient,
    Effect.acquireRelease(
      Effect.gen(function* () {
        if (path !== ":memory:") {
          yield* Effect.tryPromise({
            try: () => mkdir(dirname(path), { recursive: true }),
            catch: (error) =>
              new DatabaseError({
                message: `mkdir failed: ${
                  error instanceof Error ? error.message : String(error)
                }`,
                cause: error instanceof Error ? error.stack : String(error),
              }),
          });
        }
        const db = yield* tryDb(
          () => new Database(path, { create: true, strict: true }),
          "open",
        );
        yield* tryDb(
          () =>
            db.exec(
              "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000;",
            ),
          "pragmas",
        );
        yield* tryDb(() => migrate(db), "migrate");
        return { db };
      }),
      ({ db }) => Effect.sync(() => db.close()),
    ),
  );
