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
        updated_at INTEGER NOT NULL,
        UNIQUE(session_id, activity_type, payload_hash)
      );
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

  reserveProjection(input: {
    sourceKey: string;
    sessionId: string;
    activityType: string;
    payloadHash: string;
    now?: number;
  }): boolean {
    const now = input.now ?? Date.now();
    const result = this.#db
      .query(`
      INSERT INTO activity_projection (source_key, session_id, activity_type, payload_hash, status, created_at, updated_at)
      VALUES (?, ?, ?, ?, 'pending', ?, ?) ON CONFLICT DO NOTHING
    `)
      .run(
        input.sourceKey,
        input.sessionId,
        input.activityType,
        input.payloadHash,
        now,
        now,
      );
    return result.changes === 1;
  }

  completeProjection(sourceKey: string, activityId: string): void {
    this.#db
      .query(
        "UPDATE activity_projection SET status='completed', linear_activity_id=?, updated_at=? WHERE source_key=?",
      )
      .run(activityId, Date.now(), sourceKey);
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
