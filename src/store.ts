import { Database } from "bun:sqlite";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import type {
  AgentRunRecord,
  DesiredRunState,
  InputKind,
  InstallationRecord,
  RunState,
} from "./domain";
import { TokenCipher } from "./token-crypto";

interface InstallationRow {
  organization_id: string;
  app_user_id: string;
  access_token: string;
  refresh_token: string;
  accessible_team_ids_json: string | null;
  can_access_all_public_teams: number | null;
  expires_at: number;
  scopes_json: string;
  revoked_at: number | null;
}

interface RunRow {
  session_id: string;
  organization_id: string;
  issue_id: string | null;
  repository_id: string | null;
  state: RunState;
  team_id: string | null;
  project_id: string | null;
  desired_state: DesiredRunState;
  omp_session_id: string | null;
  omp_session_file: string | null;
  workspace_path: string | null;
  attempt: number;
  lease_owner: string | null;
  lease_expires_at: number | null;
  last_activity_at: number | null;
  terminal_reason: string | null;
  next_attempt_at: number | null;
}

interface CountRow {
  count: number;
}
interface ValueRow {
  value: string;
}

interface ProjectionIdentityRow {
  source_key: string;
  payload_hash: string;
  status: string;
}

interface ProjectionOutboxRow {
  source_key: string;
  session_id: string;
  activity_type: string;
  payload_json: string;
  attempt: number;
}

export interface ProjectionJob {
  readonly sourceKey: string;
  readonly sessionId: string;
  readonly activityType: string;
  readonly payload: unknown;
  readonly attempt: number;
}

const TERMINAL_STATES: readonly RunState[] = [
  "succeeded",
  "failed",
  "canceled",
];

export class GatewayStore {
  readonly #db: Database;
  readonly #cipher: TokenCipher;

  private constructor(db: Database, cipher: TokenCipher) {
    this.#db = db;
    this.#cipher = cipher;
  }

  static async open(path: string, key: Uint8Array): Promise<GatewayStore> {
    if (path !== ":memory:") await mkdir(dirname(path), { recursive: true });
    const cipher = await TokenCipher.create(key);
    const db = new Database(path, { create: true, strict: true });
    db.exec(
      "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000;",
    );
    const store = new GatewayStore(db, cipher);
    store.migrate();
    return store;
  }

  close(): void {
    this.#db.close();
  }

  migrate(): void {
    this.#db.exec(`
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
    `);
    const indexes = this.#db
      .query<{ name: string; unique: number; origin: string }, []>(
        'PRAGMA index_list("activity_projection")',
      )
      .all();
    if (indexes.some((index) => index.unique === 1 && index.origin === "u")) {
      this.#db.exec(`
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
    this.#db.exec("PRAGMA user_version=1");
  }

  async putInstallation(record: InstallationRecord): Promise<void> {
    const [accessToken, refreshToken] = await Promise.all([
      this.#cipher.encrypt(record.accessToken),
      this.#cipher.encrypt(record.refreshToken),
    ]);
    this.#db
      .query(`
      INSERT INTO installation (
        organization_id, app_user_id, access_token, refresh_token, expires_at, scopes_json, revoked_at,
        accessible_team_ids_json, can_access_all_public_teams
      ) VALUES (
        $organizationId, $appUserId, $accessToken, $refreshToken, $expiresAt, $scopes, $revokedAt,
        $accessibleTeamIds, $canAccessAllPublicTeams
      )
      ON CONFLICT(organization_id) DO UPDATE SET
        app_user_id=excluded.app_user_id, access_token=excluded.access_token,
        refresh_token=excluded.refresh_token, expires_at=excluded.expires_at,
        scopes_json=excluded.scopes_json, revoked_at=excluded.revoked_at,
        accessible_team_ids_json=excluded.accessible_team_ids_json,
        can_access_all_public_teams=excluded.can_access_all_public_teams
    `)
      .run({
        organizationId: record.organizationId,
        appUserId: record.appUserId,
        accessToken,
        refreshToken,
        expiresAt: record.expiresAt,
        scopes: JSON.stringify(record.scopes),
        revokedAt: record.revokedAt,
        accessibleTeamIds:
          record.accessibleTeamIds === null
            ? null
            : JSON.stringify(record.accessibleTeamIds),
        canAccessAllPublicTeams:
          record.canAccessAllPublicTeams === null
            ? null
            : Number(record.canAccessAllPublicTeams),
      });
  }

  async getInstallation(
    organizationId: string,
  ): Promise<InstallationRecord | null> {
    const row = this.#db
      .query<InstallationRow, [string]>(
        "SELECT * FROM installation WHERE organization_id = ?",
      )
      .get(organizationId);
    if (!row) return null;
    const [accessToken, refreshToken] = await Promise.all([
      this.#cipher.decrypt(row.access_token),
      this.#cipher.decrypt(row.refresh_token),
    ]);
    return {
      organizationId: row.organization_id,
      appUserId: row.app_user_id,
      accessToken,
      refreshToken,
      expiresAt: row.expires_at,
      scopes: parseStringArray(row.scopes_json, "installation scopes"),
      revokedAt: row.revoked_at,
      accessibleTeamIds:
        row.accessible_team_ids_json === null
          ? null
          : parseStringArray(
              row.accessible_team_ids_json,
              "installation team access",
            ),
      canAccessAllPublicTeams:
        row.can_access_all_public_teams === null
          ? null
          : row.can_access_all_public_teams === 1,
    };
  }

  revokeInstallation(organizationId: string, at = Date.now()): void {
    this.#db
      .query("UPDATE installation SET revoked_at = ? WHERE organization_id = ?")
      .run(at, organizationId);
    this.#db
      .query(
        "UPDATE agent_run SET desired_state='canceled', updated_at=? WHERE organization_id=? AND state NOT IN ('succeeded','failed','canceled')",
      )
      .run(at, organizationId);
  }

  async applyPermissionChange(
    organizationId: string,
    appUserId: string,
    addedTeamIds: readonly string[],
    removedTeamIds: readonly string[],
    canAccessAllPublicTeams: boolean,
    at = Date.now(),
  ): Promise<void> {
    const installation = await this.getInstallation(organizationId);
    if (!installation) return;
    if (installation.appUserId !== appUserId) return;
    const teamIds = new Set(installation.accessibleTeamIds ?? []);
    for (const teamId of addedTeamIds) teamIds.add(teamId);
    for (const teamId of removedTeamIds) teamIds.delete(teamId);
    await this.putInstallation({
      ...installation,
      accessibleTeamIds: [...teamIds].sort(),
      canAccessAllPublicTeams,
    });
    for (const teamId of removedTeamIds) {
      this.#db
        .query(`
        UPDATE agent_run SET desired_state='canceled', updated_at=?
        WHERE organization_id=? AND team_id=? AND state NOT IN ('succeeded','failed','canceled')
      `)
        .run(at, organizationId, teamId);
    }
  }

  createOAuthState(hash: string, expiresAt: number, now = Date.now()): void {
    this.#db
      .query(
        "INSERT INTO oauth_state (state_hash, created_at, expires_at) VALUES (?, ?, ?)",
      )
      .run(hash, now, expiresAt);
  }

  consumeOAuthState(hash: string, now = Date.now()): boolean {
    return this.#db.transaction(() => {
      const result = this.#db
        .query(
          "UPDATE oauth_state SET consumed_at=? WHERE state_hash=? AND consumed_at IS NULL AND expires_at>=?",
        )
        .run(now, hash, now);
      return result.changes === 1;
    })();
  }

  acceptDelivery(input: {
    id: string;
    organizationId: string;
    payloadHash: string;
    payload: unknown;
    receivedAt?: number;
  }): boolean {
    const result = this.#db
      .query(`
      INSERT INTO webhook_delivery (delivery_id, organization_id, received_at, payload_hash, payload_json)
      VALUES (?, ?, ?, ?, ?) ON CONFLICT(delivery_id) DO NOTHING
    `)
      .run(
        input.id,
        input.organizationId,
        input.receivedAt ?? Date.now(),
        input.payloadHash,
        JSON.stringify(input.payload),
      );
    return result.changes === 1;
  }
  claimDelivery(input: {
    id: string;
    organizationId: string;
    payloadHash: string;
    payload: unknown;
    receivedAt?: number;
  }): "claimed" | "duplicate" | "conflict" {
    return this.#db.transaction(() => {
      if (this.acceptDelivery(input)) return "claimed";
      const existing = this.#db
        .query<{ payload_hash: string; status: string }, [string]>(
          "SELECT payload_hash, status FROM webhook_delivery WHERE delivery_id=?",
        )
        .get(input.id);
      if (!existing) throw new Error(`Delivery ${input.id} disappeared`);
      if (existing.payload_hash !== input.payloadHash) return "conflict";
      if (existing.status !== "failed") return "duplicate";
      const claimed = this.#db
        .query(
          "UPDATE webhook_delivery SET status='pending', error=NULL WHERE delivery_id=? AND status='failed'",
        )
        .run(input.id);
      return claimed.changes === 1 ? "claimed" : "duplicate";
    })();
  }

  markDelivery(
    id: string,
    status: "processed" | "failed",
    error?: string,
  ): void {
    this.#db
      .query(
        "UPDATE webhook_delivery SET status=?, error=? WHERE delivery_id=?",
      )
      .run(status, error ?? null, id);
  }

  createRun(input: {
    sessionId: string;
    organizationId: string;
    issueId: string | null;
    teamId?: string | null;
    projectId?: string | null;
    now?: number;
  }): AgentRunRecord {
    const now = input.now ?? Date.now();
    this.#db
      .query(`
      INSERT INTO agent_run (
        session_id, organization_id, issue_id, team_id, project_id, state, desired_state, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, 'queued', 'running', ?, ?)
      ON CONFLICT(session_id) DO NOTHING
    `)
      .run(
        input.sessionId,
        input.organizationId,
        input.issueId,
        input.teamId ?? null,
        input.projectId ?? null,
        now,
        now,
      );
    const run = this.getRun(input.sessionId);
    if (!run) throw new Error("Failed to create run");
    return run;
  }

  getRun(sessionId: string): AgentRunRecord | null {
    const row = this.#db
      .query<RunRow, [string]>("SELECT * FROM agent_run WHERE session_id=?")
      .get(sessionId);
    return row ? mapRun(row) : null;
  }

  enqueueInput(input: {
    id: string;
    sessionId: string;
    kind: InputKind;
    body: string;
    payload: unknown;
    createdAt?: number;
  }): boolean {
    return this.#db.transaction(() => {
      const run = this.getRun(input.sessionId);
      if (!run) throw new Error(`Unknown run ${input.sessionId}`);
      if (input.kind !== "stop" && run.desiredState === "canceled")
        return false;
      const result = this.#db
        .query(`
        INSERT INTO run_input (id, session_id, kind, body, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING
      `)
        .run(
          input.id,
          input.sessionId,
          input.kind,
          input.body,
          JSON.stringify(input.payload),
          input.createdAt ?? Date.now(),
        );
      if (result.changes === 1 && input.kind !== "stop") {
        this.#db
          .query(`
          UPDATE agent_run
          SET state='queued', desired_state='running', attempt=0,
            terminal_reason=NULL, next_attempt_at=NULL, updated_at=?
          WHERE session_id=? AND state IN ('succeeded','failed')
        `)
          .run(Date.now(), input.sessionId);
      }
      if (input.kind === "stop") {
        this.#db
          .query(`
          UPDATE agent_run SET desired_state='canceled',
            state=CASE WHEN state IN ('queued','waiting') THEN 'stopping' ELSE state END, updated_at=?
          WHERE session_id=?
        `)
          .run(Date.now(), input.sessionId);
      }
      return result.changes === 1;
    })();
  }

  pendingInputs(
    sessionId: string,
  ): Array<{ id: string; kind: InputKind; body: string; payload: unknown }> {
    const rows = this.#db
      .query<
        { id: string; kind: InputKind; body: string; payload_json: string },
        [string]
      >(
        "SELECT id, kind, body, payload_json FROM run_input WHERE session_id=? AND processed_at IS NULL ORDER BY created_at, id",
      )
      .all(sessionId);
    return rows.map((row) => ({
      id: row.id,
      kind: row.kind,
      body: row.body,
      payload: JSON.parse(row.payload_json) as unknown,
    }));
  }
  latestActionableInput(
    sessionId: string,
  ): { body: string; kind: InputKind } | null {
    return (
      this.#db
        .query<{ body: string; kind: InputKind }, [string]>(`
      SELECT body, kind FROM run_input
      WHERE session_id=? AND kind!='stop'
      ORDER BY created_at DESC, id DESC
      LIMIT 1
    `)
        .get(sessionId) ?? null
    );
  }

  listSessionsWithPendingInputs(): string[] {
    return this.#db
      .query<{ session_id: string }, []>(`
      SELECT DISTINCT r.session_id
      FROM agent_run r
      JOIN run_input i ON i.session_id=r.session_id AND i.processed_at IS NULL
      WHERE r.state NOT IN ('succeeded','failed','canceled')
      ORDER BY r.session_id
    `)
      .all()
      .map((row) => row.session_id);
  }

  markInputProcessed(id: string, at = Date.now()): void {
    this.#db
      .query("UPDATE run_input SET processed_at=? WHERE id=?")
      .run(at, id);
  }

  claimRun(
    sessionId: string,
    owner: string,
    leaseDurationMs: number,
    now = Date.now(),
  ): boolean {
    const result = this.#db
      .query(`
      UPDATE agent_run SET lease_owner=?, lease_expires_at=?, updated_at=?
      WHERE session_id=? AND desired_state='running' AND state NOT IN ('succeeded','failed','canceled')
        AND (lease_owner IS NULL OR lease_owner=? OR lease_expires_at<?)
    `)
      .run(owner, now + leaseDurationMs, now, sessionId, owner, now);
    return result.changes === 1;
  }

  renewLease(
    sessionId: string,
    owner: string,
    leaseDurationMs: number,
    now = Date.now(),
  ): boolean {
    return (
      this.#db
        .query(
          "UPDATE agent_run SET lease_expires_at=?, updated_at=? WHERE session_id=? AND lease_owner=? AND lease_expires_at>=?",
        )
        .run(now + leaseDurationMs, now, sessionId, owner, now).changes === 1
    );
  }

  releaseLease(sessionId: string, owner: string): void {
    this.#db
      .query(
        "UPDATE agent_run SET lease_owner=NULL, lease_expires_at=NULL, updated_at=? WHERE session_id=? AND lease_owner=?",
      )
      .run(Date.now(), sessionId, owner);
  }

  updateRun(
    sessionId: string,
    patch: {
      state?: RunState;
      repositoryId?: string | null;
      workspacePath?: string | null;
      ompSessionId?: string | null;
      ompSessionFile?: string | null;
      terminalReason?: string | null;
      lastActivityAt?: number | null;
      nextAttemptAt?: number | null;
      incrementAttempt?: boolean;
    },
  ): void {
    const current = this.getRun(sessionId);
    if (!current) throw new Error(`Unknown run ${sessionId}`);
    if (
      TERMINAL_STATES.includes(current.state) &&
      patch.state !== undefined &&
      patch.state !== current.state
    ) {
      throw new Error("Terminal run state is immutable");
    }
    this.#db
      .query(`
      UPDATE agent_run SET state=?, repository_id=?, workspace_path=?, omp_session_id=?, omp_session_file=?,
        terminal_reason=?, last_activity_at=?, next_attempt_at=?, attempt=attempt+?, updated_at=? WHERE session_id=?
    `)
      .run(
        patch.state ?? current.state,
        patch.repositoryId === undefined
          ? current.repositoryId
          : patch.repositoryId,
        patch.workspacePath === undefined
          ? current.workspacePath
          : patch.workspacePath,
        patch.ompSessionId === undefined
          ? current.ompSessionId
          : patch.ompSessionId,
        patch.ompSessionFile === undefined
          ? current.ompSessionFile
          : patch.ompSessionFile,
        patch.terminalReason === undefined
          ? current.terminalReason
          : patch.terminalReason,
        patch.lastActivityAt === undefined
          ? current.lastActivityAt
          : patch.lastActivityAt,
        patch.nextAttemptAt === undefined
          ? current.nextAttemptAt
          : patch.nextAttemptAt,
        patch.incrementAttempt ? 1 : 0,
        Date.now(),
        sessionId,
      );
  }

  listRunnable(now = Date.now()): AgentRunRecord[] {
    return this.#db
      .query<RunRow, [number, number]>(`
      SELECT * FROM agent_run WHERE desired_state='running' AND state NOT IN ('succeeded','failed','canceled')
        AND (next_attempt_at IS NULL OR next_attempt_at<=?) AND (lease_owner IS NULL OR lease_expires_at<?)
      ORDER BY created_at, session_id
    `)
      .all(now, now)
      .map(mapRun);
  }

  listCancellationPending(): AgentRunRecord[] {
    return this.#db
      .query<RunRow, []>(
        "SELECT * FROM agent_run WHERE desired_state='canceled' AND state NOT IN ('succeeded','failed','canceled')",
      )
      .all()
      .map(mapRun);
  }

  enqueueProjection(input: {
    sourceKey: string;
    sessionId: string;
    activityType: string;
    payloadHash: string;
    payload: unknown;
    now?: number;
    firstWriteWins?: boolean;
  }): boolean {
    const now = input.now ?? Date.now();
    return this.#db.transaction(() => {
      const reserved = this.#db
        .query(`
        INSERT INTO activity_projection (
          source_key, session_id, activity_type, payload_hash, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 'pending', ?, ?)
        ON CONFLICT DO NOTHING
      `)
        .run(
          input.sourceKey,
          input.sessionId,
          input.activityType,
          input.payloadHash,
          now,
          now,
        );
      if (reserved.changes !== 1) {
        const existingBySource = this.#db
          .query<ProjectionIdentityRow, [string]>(`
          SELECT source_key, payload_hash, status FROM activity_projection WHERE source_key=?
        `)
          .get(input.sourceKey);
        const existing =
          existingBySource ??
          this.#db
            .query<ProjectionIdentityRow, [string, string, string]>(`
            SELECT source_key, payload_hash, status FROM activity_projection
            WHERE session_id=? AND activity_type=? AND payload_hash=?
          `)
            .get(input.sessionId, input.activityType, input.payloadHash);
        if (!existing)
          throw new Error(
            `Projection ${input.sourceKey} reservation disappeared`,
          );
        if (existing.source_key !== input.sourceKey) return false;
        if (existing.payload_hash !== input.payloadHash) {
          if (input.firstWriteWins) return false;
          throw new Error(
            `Projection ${input.sourceKey} was reused with a different payload`,
          );
        }
        if (existing.status === "completed") return false;
        this.#db
          .query(`
          INSERT INTO projection_outbox (
            source_key, session_id, activity_type, payload_json, status,
            attempt, next_attempt_at, created_at, updated_at
          ) VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, ?)
          ON CONFLICT(source_key) DO NOTHING
        `)
          .run(
            input.sourceKey,
            input.sessionId,
            input.activityType,
            JSON.stringify(input.payload),
            now,
            now,
            now,
          );
        return false;
      }
      this.#db
        .query(`
        INSERT INTO projection_outbox (
          source_key, session_id, activity_type, payload_json, status,
          attempt, next_attempt_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, ?)
      `)
        .run(
          input.sourceKey,
          input.sessionId,
          input.activityType,
          JSON.stringify(input.payload),
          now,
          now,
          now,
        );
      return true;
    })();
  }

  claimProjection(
    sourceKey: string,
    owner: string,
    leaseDurationMs: number,
    now = Date.now(),
  ): ProjectionJob | null {
    return this.#db.transaction(() => {
      const claimed = this.#db
        .query(`
        UPDATE projection_outbox
        SET status='processing', attempt=attempt+1, lease_owner=?, lease_expires_at=?,
          last_error=NULL, updated_at=?
        WHERE source_key=? AND next_attempt_at<=?
          AND (
            status IN ('pending', 'failed')
            OR (status='processing' AND lease_expires_at<?)
          )
      `)
        .run(owner, now + leaseDurationMs, now, sourceKey, now, now);
      if (claimed.changes !== 1) return null;
      const row = this.#db
        .query<ProjectionOutboxRow, [string]>(`
        SELECT source_key, session_id, activity_type, payload_json, attempt
        FROM projection_outbox WHERE source_key=?
      `)
        .get(sourceKey);
      if (!row)
        throw new Error(`Projection ${sourceKey} disappeared after claim`);
      return {
        sourceKey: row.source_key,
        sessionId: row.session_id,
        activityType: row.activity_type,
        payload: JSON.parse(row.payload_json) as unknown,
        attempt: row.attempt,
      };
    })();
  }

  listDueProjectionKeys(now = Date.now(), limit = 50): string[] {
    return this.#db
      .query<{ source_key: string }, [number, number, number]>(`
      SELECT source_key FROM projection_outbox
      WHERE next_attempt_at<=?
        AND (
          status IN ('pending', 'failed')
          OR (status='processing' AND lease_expires_at<?)
        )
      ORDER BY next_attempt_at, created_at, source_key
      LIMIT ?
    `)
      .all(now, now, limit)
      .map((row) => row.source_key);
  }

  failProjection(
    sourceKey: string,
    owner: string,
    error: string,
    nextAttemptAt: number,
  ): void {
    this.#db.transaction(() => {
      this.#db
        .query(`
        UPDATE projection_outbox
        SET status='failed', next_attempt_at=?, lease_owner=NULL, lease_expires_at=NULL,
          last_error=?, updated_at=?
        WHERE source_key=? AND lease_owner=?
      `)
        .run(nextAttemptAt, error, Date.now(), sourceKey, owner);
      this.#db
        .query(`
        UPDATE activity_projection SET status='failed', updated_at=? WHERE source_key=?
      `)
        .run(Date.now(), sourceKey);
    })();
  }

  completeProjection(
    sourceKey: string,
    owner: string,
    activityId: string | null,
  ): void {
    this.#db.transaction(() => {
      const completed = this.#db
        .query(`
        UPDATE projection_outbox
        SET status='completed', lease_owner=NULL, lease_expires_at=NULL,
          last_error=NULL, updated_at=?
        WHERE source_key=? AND lease_owner=? AND status='processing'
      `)
        .run(Date.now(), sourceKey, owner);
      if (completed.changes !== 1) {
        throw new Error(
          `Projection ${sourceKey} lease was lost before completion`,
        );
      }
      this.#db
        .query(`
        UPDATE activity_projection
        SET status='completed', linear_activity_id=?, updated_at=? WHERE source_key=?
      `)
        .run(activityId, Date.now(), sourceKey);
    })();
  }

  projectionCount(sessionId: string, activityType?: string): number {
    const row = activityType
      ? this.#db
          .query<CountRow, [string, string]>(
            "SELECT COUNT(*) count FROM activity_projection WHERE session_id=? AND activity_type=?",
          )
          .get(sessionId, activityType)
      : this.#db
          .query<CountRow, [string]>(
            "SELECT COUNT(*) count FROM activity_projection WHERE session_id=?",
          )
          .get(sessionId);
    return row?.count ?? 0;
  }

  setWorkspace(input: {
    sessionId: string;
    path: string;
    repositoryId: string;
    url: string;
    ref: string;
    state: string;
  }): void {
    const now = Date.now();
    this.#db
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
      );
  }

  getRawEncryptedAccessToken(organizationId: string): string | null {
    return (
      this.#db
        .query<ValueRow, [string]>(
          "SELECT access_token value FROM installation WHERE organization_id=?",
        )
        .get(organizationId)?.value ?? null
    );
  }
}

function mapRun(row: RunRow): AgentRunRecord {
  return {
    sessionId: row.session_id,
    organizationId: row.organization_id,
    issueId: row.issue_id,
    repositoryId: row.repository_id,
    state: row.state,
    desiredState: row.desired_state,
    ompSessionId: row.omp_session_id,
    ompSessionFile: row.omp_session_file,
    workspacePath: row.workspace_path,
    attempt: row.attempt,
    teamId: row.team_id,
    projectId: row.project_id,
    leaseOwner: row.lease_owner,
    leaseExpiresAt: row.lease_expires_at,
    lastActivityAt: row.last_activity_at,
    terminalReason: row.terminal_reason,
    nextAttemptAt: row.next_attempt_at,
  };
}

function parseStringArray(json: string, label: string): string[] {
  const value: unknown = JSON.parse(json);
  if (
    !Array.isArray(value) ||
    !value.every((item) => typeof item === "string")
  ) {
    throw new Error(`Invalid ${label} in database`);
  }
  return value;
}
