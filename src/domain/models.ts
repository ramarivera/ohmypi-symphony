import { Schema } from "effect";
import {
  ActivityId,
  AppUserId,
  DeliveryId,
  InputId,
  IssueId,
  OrganizationId,
  ProjectId,
  SessionId,
  SourceKey,
  TeamId,
  WorkspaceId,
} from "./ids.js";

export const RunState = Schema.Literal("queued", "starting", "running", "waiting", "stopping", "succeeded", "failed", "canceled", "orphaned");
export type RunState = Schema.Schema.Type<typeof RunState>;
export const DesiredRunState = Schema.Literal("running", "canceled");
export type DesiredRunState = Schema.Schema.Type<typeof DesiredRunState>;
export const InputKind = Schema.Literal("created", "prompted", "stop");
export type InputKind = Schema.Schema.Type<typeof InputKind>;
export const ActivityType = Schema.Literal("thought", "action", "elicitation", "response", "error");
export type ActivityType = Schema.Schema.Type<typeof ActivityType>;
export const LogLevel = Schema.Literal("trace", "debug", "info", "warn", "error", "fatal", "silent");
export type LogLevel = Schema.Schema.Type<typeof LogLevel>;

const NullableString = Schema.OptionFromNullOr(Schema.String);
const NullableNumber = Schema.OptionFromNullOr(Schema.Number);

export const RepositoryDefinition = Schema.Struct({
  id: WorkspaceId,
  url: Schema.String,
  ref: Schema.String,
  teamIds: Schema.Array(TeamId),
  projectIds: Schema.Array(ProjectId),
});
export type RepositoryDefinition = Schema.Schema.Type<typeof RepositoryDefinition>;

export const RepositoryRecord = Schema.extend(RepositoryDefinition, Schema.Struct({
  organizationId: OrganizationId,
  labels: Schema.Array(Schema.String),
  isDefault: Schema.Boolean,
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
}));
export type RepositoryRecord = Schema.Schema.Type<typeof RepositoryRecord>;

export const Installation = Schema.Struct({
  organizationId: OrganizationId,
  appUserId: AppUserId,
  accessToken: Schema.String,
  refreshToken: Schema.String,
  expiresAt: Schema.Number,
  scopes: Schema.Array(Schema.String),
  revokedAt: NullableNumber,
  accessibleTeamIds: Schema.OptionFromNullOr(Schema.Array(TeamId)),
  canAccessAllPublicTeams: Schema.OptionFromNullOr(Schema.Boolean),
});
export type Installation = Schema.Schema.Type<typeof Installation>;

export const AgentRun = Schema.Struct({
  sessionId: SessionId,
  organizationId: OrganizationId,
  issueId: Schema.OptionFromNullOr(IssueId),
  repositoryId: Schema.OptionFromNullOr(WorkspaceId),
  state: RunState,
  desiredState: DesiredRunState,
  ompSessionId: NullableString,
  ompSessionFile: NullableString,
  workspacePath: NullableString,
  teamId: Schema.OptionFromNullOr(TeamId),
  projectId: Schema.OptionFromNullOr(ProjectId),
  attempt: Schema.Number,
  leaseOwner: NullableString,
  leaseExpiresAt: NullableNumber,
  lastActivityAt: NullableNumber,
  terminalReason: NullableString,
  nextAttemptAt: NullableNumber,
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
});
export type AgentRun = Schema.Schema.Type<typeof AgentRun>;

export const WebhookDelivery = Schema.Struct({
  id: DeliveryId,
  organizationId: OrganizationId,
  payloadHash: Schema.String,
  receivedAt: Schema.Number,
  status: Schema.Literal("pending", "processed", "failed"),
  error: NullableString,
});
export type WebhookDelivery = Schema.Schema.Type<typeof WebhookDelivery>;

export const RunInput = Schema.Struct({
  id: InputId,
  sessionId: SessionId,
  kind: InputKind,
  body: Schema.String,
  payload: Schema.Unknown,
  createdAt: Schema.Number,
});
export type RunInput = Schema.Schema.Type<typeof RunInput>;

const RunEventStatus = Schema.Literal("observed", "pending", "completed", "failed");
export const RunEvent = Schema.Struct({
  sourceKey: SourceKey,
  sessionId: SessionId,
  kind: Schema.String,
  level: Schema.Literal("debug", "info", "warn", "result", "error"),
  status: Schema.OptionFromNullOr(RunEventStatus),
  text: NullableString,
  payload: Schema.Unknown,
  error: NullableString,
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
});

export const ProjectionJob = Schema.Struct({
  sourceKey: SourceKey,
  sessionId: SessionId,
  activityType: Schema.String,
  payload: Schema.Unknown,
  attempt: Schema.Number,
  payloadHash: Schema.String,
  nextAttemptAt: Schema.Number,
  createdAt: Schema.Number,
});
export type ProjectionJob = Schema.Schema.Type<typeof ProjectionJob>;

export const AgentSessionComment = Schema.Struct({ id: Schema.String, body: Schema.String });
export type AgentSessionComment = Schema.Schema.Type<typeof AgentSessionComment>;
export const AgentSessionIssue = Schema.Struct({
  id: Schema.String,
  title: Schema.String,
  description: Schema.NullOr(Schema.String),
  identifier: Schema.NullOr(Schema.String),
  url: Schema.NullOr(Schema.String),
  teamId: Schema.NullOr(Schema.String),
  projectId: Schema.NullOr(Schema.String),
});
export type AgentSessionIssue = Schema.Schema.Encoded<typeof AgentSessionIssue>;
export const AgentSessionActivity = Schema.Struct({
  id: Schema.String,
  agentSessionId: Schema.String,
  content: Schema.Unknown,
  signal: Schema.NullOr(Schema.String),
});
export type AgentSessionActivity = Schema.Schema.Encoded<typeof AgentSessionActivity>;

// Linear's embedded webhook AgentSession payload has no `type` field.
export const AgentSessionWebhookPayload = Schema.Struct({
  id: Schema.String,
  appUserId: Schema.String,
  organizationId: Schema.String,
  status: Schema.String,
  createdAt: Schema.String,
  updatedAt: Schema.String,
  issueId: Schema.NullOr(Schema.String),
  commentId: Schema.NullOr(Schema.String),
  sourceCommentId: Schema.NullOr(Schema.String),
  summary: Schema.NullOr(Schema.String),
  url: Schema.NullOr(Schema.String),
  archivedAt: Schema.NullOr(Schema.String),
  startedAt: Schema.NullOr(Schema.String),
  endedAt: Schema.NullOr(Schema.String),
  comment: Schema.NullOr(AgentSessionComment),
  issue: Schema.NullOr(AgentSessionIssue),
});
export type AgentSessionWebhookPayload = Schema.Schema.Encoded<typeof AgentSessionWebhookPayload>;

export const AgentSessionEvent = Schema.Struct({
  type: Schema.Literal("AgentSessionEvent"),
  action: Schema.String,
  organizationId: Schema.String,
  appUserId: Schema.String,
  oauthClientId: Schema.String,
  webhookId: Schema.String,
  webhookTimestamp: Schema.String,
  createdAt: Schema.String,
  promptContext: Schema.NullOr(Schema.String),
  guidance: Schema.NullOr(Schema.Array(Schema.Struct({ body: Schema.String }))),
  previousComments: Schema.NullOr(Schema.Array(AgentSessionComment)),
  agentActivity: Schema.NullOr(AgentSessionActivity),
  agentSession: AgentSessionWebhookPayload,
});
export type AgentSessionEvent = Schema.Schema.Encoded<typeof AgentSessionEvent>;
