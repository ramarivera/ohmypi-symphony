export type RunState =
  | "queued"
  | "starting"
  | "running"
  | "waiting"
  | "stopping"
  | "succeeded"
  | "failed"
  | "canceled"
  | "orphaned";

export type DesiredRunState = "running" | "canceled";
export type InputKind = "created" | "prompted" | "stop";
export type ActivityType =
  | "thought"
  | "action"
  | "elicitation"
  | "response"
  | "error";

export interface GatewayConfig {
  readonly linearClientId: string;
  readonly linearClientSecret: string;
  readonly linearWebhookSecret: string;
  readonly tokenEncryptionKey: Uint8Array;
  readonly publicUrl: URL;
  readonly databasePath: string;
  readonly workspaceRoot: string;
  readonly repositoryMapPath: string;
  readonly ompCliPath: string;
  readonly port: number;
  readonly leaseDurationMs: number;
  readonly webhookReplayWindowMs: number;
}

export interface RepositoryDefinition {
  readonly id: string;
  readonly url: string;
  readonly ref: string;
  readonly teamIds: readonly string[];
  readonly projectIds: readonly string[];
}

export interface RepositoryMap {
  readonly repositories: readonly RepositoryDefinition[];
}

export interface InstallationRecord {
  readonly organizationId: string;
  readonly appUserId: string;
  readonly accessToken: string;
  readonly refreshToken: string;
  readonly expiresAt: number;
  readonly scopes: readonly string[];
  readonly revokedAt: number | null;
  readonly accessibleTeamIds: readonly string[] | null;
  readonly canAccessAllPublicTeams: boolean | null;
}

export interface AgentRunRecord {
  readonly sessionId: string;
  readonly organizationId: string;
  readonly issueId: string | null;
  readonly repositoryId: string | null;
  readonly state: RunState;
  readonly desiredState: DesiredRunState;
  readonly ompSessionId: string | null;
  readonly ompSessionFile: string | null;
  readonly workspacePath: string | null;
  readonly teamId: string | null;
  readonly projectId: string | null;
  readonly attempt: number;
  readonly leaseOwner: string | null;
  readonly leaseExpiresAt: number | null;
  readonly lastActivityAt: number | null;
  readonly terminalReason: string | null;
  readonly nextAttemptAt: number | null;
}

export interface RunInput {
  readonly id: string;
  readonly sessionId: string;
  readonly kind: InputKind;
  readonly body: string;
  readonly payload: unknown;
  readonly createdAt: number;
}

export interface LinearActivityContent {
  readonly type: ActivityType;
  readonly body?: string;
  readonly action?: string;
  readonly parameter?: string;
  readonly result?: string;
}

export interface LinearGatewayPort {
  createActivity(input: {
    sessionId: string;
    content: LinearActivityContent;
    ephemeral?: boolean;
    signal?: "auth" | "continue" | "select" | "stop";
    signalMetadata?: Record<string, unknown>;
  }): Promise<string>;
  updateSession(input: {
    sessionId: string;
    plan?: Record<string, unknown>;
    externalUrls?: readonly { label: string; url: string }[];
  }): Promise<void>;
  refreshInstallation(organizationId: string): Promise<string>;
}

export interface RpcEvent {
  readonly type: string;
  readonly [key: string]: unknown;
}

export interface RpcWorker {
  readonly sessionId: string | null;
  readonly sessionFile: string | null;
  start(): Promise<void>;
  prompt(message: string): Promise<void>;
  steer(message: string): Promise<void>;
  followUp(message: string): Promise<void>;
  abort(): Promise<void>;
  getState(): Promise<Record<string, unknown>>;
  stop(): Promise<void>;
  onEvent(listener: (event: RpcEvent) => void): () => void;
}
