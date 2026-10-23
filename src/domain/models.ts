import { Effect, Option, Schema } from "effect";
import {
  AppUserId,
  DeliveryId,
  InputId,
  IssueId,
  McpServerId,
  OrganizationId,
  ProjectId,
  SessionId,
  SourceKey,
  TeamId,
  WorkspaceId,
} from "./ids.js";

export const RunState = Schema.Literals([
  "queued",
  "starting",
  "running",
  "waiting",
  "stopping",
  "succeeded",
  "failed",
  "canceled",
  "orphaned",
]);
export type RunState = Schema.Schema.Type<typeof RunState>;

export const TERMINAL_RUN_STATES: ReadonlyArray<RunState> = [
  "succeeded",
  "failed",
  "canceled",
];
export const DesiredRunState = Schema.Literals(["running", "canceled"]);
export type DesiredRunState = Schema.Schema.Type<typeof DesiredRunState>;
export const InputKind = Schema.Literals(["created", "prompted", "stop"]);
export type InputKind = Schema.Schema.Type<typeof InputKind>;
export const APP_USER_NOTIFICATION_DEFERRED_STOP_ACTION = "issueStatusChanged";

export const isDeferredNotificationStopPayload = (
  payload: unknown,
): boolean => {
  if (typeof payload !== "object" || payload === null) return false;
  const record = payload as Record<string, unknown>;
  return (
    record.type === "AppUserNotification" &&
    record.action === APP_USER_NOTIFICATION_DEFERRED_STOP_ACTION
  );
};
export const ActivityType = Schema.Literals([
  "thought",
  "action",
  "elicitation",
  "response",
  "error",
]);
export type ActivityType = Schema.Schema.Type<typeof ActivityType>;
export const LogLevel = Schema.Literals([
  "trace",
  "debug",
  "info",
  "warn",
  "error",
  "fatal",
  "silent",
]);
export type LogLevel = Schema.Schema.Type<typeof LogLevel>;

const NullableString = Schema.OptionFromNullOr(Schema.String);
const NullableNumber = Schema.OptionFromNullOr(Schema.Number);

/**
 * A conservative Nix attribute path. Attribute selectors are passed to `nix`
 * as argv values, never interpolated into a shell command.
 */
export const NixPackageName = Schema.String.pipe(
  Schema.check(Schema.isPattern(/^[A-Za-z0-9_+-]+(?:\.[A-Za-z0-9_+-]+)*$/u)),
  Schema.brand("@Gateway/NixPackageName"),
);
export type NixPackageName = Schema.Schema.Type<typeof NixPackageName>;

export const NixCacheEntry = Schema.Struct({
  cacheKey: Schema.String.pipe(
    Schema.check(Schema.isPattern(/^[a-f0-9]{64}$/u)),
  ),
  nixpkgsFlakeRef: Schema.String,
  packages: Schema.Array(NixPackageName),
  storePaths: Schema.Array(Schema.String),
  pathEntries: Schema.Array(Schema.String),
  sizeBytes: Schema.Number.pipe(
    Schema.check(Schema.isInt()),
    Schema.check(Schema.isGreaterThanOrEqualTo(0)),
  ),
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
});
export type NixCacheEntry = Schema.Schema.Type<typeof NixCacheEntry>;

export const PromptTemplateKind = Schema.Literals([
  "created",
  "prompted",
  "contract",
]);
export type PromptTemplateKind = Schema.Schema.Type<typeof PromptTemplateKind>;

export const PromptTemplate = Schema.Struct({
  organizationId: OrganizationId,
  kind: PromptTemplateKind,
  body: Schema.String,
  updatedAt: Schema.Number,
});
export type PromptTemplate = Schema.Schema.Type<typeof PromptTemplate>;

export const normalizeNixPackages = (
  packages: ReadonlyArray<unknown>,
): ReadonlyArray<NixPackageName> => {
  const normalized = packages.map((value) =>
    Schema.decodeUnknownSync(NixPackageName)(value),
  );
  return [...new Set(normalized)].sort();
};

export const RepositoryDefinition = Schema.Struct({
  id: WorkspaceId,
  url: Schema.String,
  ref: Schema.String,
  teamIds: Schema.Array(TeamId),
  projectIds: Schema.Array(ProjectId),
  nixPackages: Schema.Array(NixPackageName),
});
export type RepositoryDefinition = Schema.Schema.Type<
  typeof RepositoryDefinition
>;

export const RepositoryRecord = Schema.Struct({
  ...RepositoryDefinition.fields,
  ...Schema.Struct({
    organizationId: OrganizationId,
    labels: Schema.Array(Schema.String),
    isDefault: Schema.Boolean,
    createdAt: Schema.Number,
    updatedAt: Schema.Number,
  }).fields,
});
export type RepositoryRecord = Schema.Schema.Type<typeof RepositoryRecord>;
export const McpServerTransport = Schema.Literals(["stdio", "http", "sse"]);
export type McpServerTransport = Schema.Schema.Type<typeof McpServerTransport>;

export const McpOAuthClientConfig = Schema.Struct({
  clientId: Schema.optional(Schema.String),
  clientSecret: Schema.optional(Schema.String),
  scope: Schema.optional(Schema.String),
  tokenEndpointAuthMethod: Schema.optional(
    Schema.Literals(["none", "client_secret_basic", "client_secret_post"]),
  ),
});
export type McpOAuthClientConfig = Schema.Schema.Type<
  typeof McpOAuthClientConfig
>;
export const McpServerRecord = Schema.Struct({
  id: McpServerId,
  organizationId: OrganizationId,
  name: Schema.String,
  transport: McpServerTransport,
  command: Schema.OptionFromNullOr(Schema.String),
  args: Schema.Array(Schema.String),
  url: Schema.OptionFromNullOr(Schema.String),
  env: Schema.Record(Schema.String, Schema.String),
  headers: Schema.Record(Schema.String, Schema.String).pipe(
    Schema.withDecodingDefaultType(Effect.sync(() => ({}))),
  ),
  oauthClient: Schema.optional(Schema.NullOr(McpOAuthClientConfig)),
  repositoryId: Schema.OptionFromNullOr(WorkspaceId),
  enabled: Schema.Boolean,
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
});
export type McpServerRecord = Schema.Schema.Type<typeof McpServerRecord>;

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
  status: Schema.Literals(["pending", "processed", "failed"]),
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

const RunEventStatus = Schema.Literals([
  "observed",
  "pending",
  "completed",
  "failed",
]);
export const RunEvent = Schema.Struct({
  sourceKey: SourceKey,
  sessionId: SessionId,
  kind: Schema.String,
  level: Schema.Literals(["debug", "info", "warn", "result", "error"]),
  status: Schema.OptionFromNullOr(RunEventStatus),
  text: NullableString,
  payload: Schema.Unknown,
  error: NullableString,
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
});
export type RunEvent = Schema.Schema.Type<typeof RunEvent>;

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

export const AgentSessionComment = Schema.StructWithRest(
  Schema.Struct({
    id: Schema.String.pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => "")),
    ),
    body: Schema.String,
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);
export type AgentSessionComment = Schema.Schema.Type<
  typeof AgentSessionComment
>;
export const AgentSessionIssue = Schema.StructWithRest(
  Schema.Struct({
    id: Schema.String,
    title: Schema.String,
    description: Schema.NullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => null)),
    ),
    identifier: Schema.NullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => null)),
    ),
    url: Schema.NullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => null)),
    ),
    teamId: Schema.NullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => null)),
    ),
    projectId: Schema.NullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => null)),
    ),
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);
export type AgentSessionIssue = Schema.Schema.Type<typeof AgentSessionIssue>;
export const AgentSessionActivity = Schema.StructWithRest(
  Schema.Struct({
    id: Schema.String,
    agentSessionId: Schema.String,
    content: Schema.Unknown,
    signal: Schema.OptionFromNullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
    ),
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);
export type AgentSessionActivity = Schema.Schema.Type<
  typeof AgentSessionActivity
>;

// Linear's embedded webhook AgentSession payload has no `type` field.
export const AgentSessionWebhookPayload = Schema.StructWithRest(
  Schema.Struct({
    id: Schema.String,
    appUserId: Schema.String,
    organizationId: Schema.String,
    status: Schema.String,
    createdAt: Schema.String,
    updatedAt: Schema.String,
    issueId: Schema.OptionFromNullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
    ),
    commentId: Schema.OptionFromNullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
    ),
    sourceCommentId: Schema.OptionFromNullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
    ),
    summary: Schema.OptionFromNullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
    ),
    url: Schema.OptionFromNullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
    ),
    archivedAt: Schema.OptionFromNullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
    ),
    startedAt: Schema.OptionFromNullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
    ),
    endedAt: Schema.OptionFromNullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
    ),
    comment: Schema.OptionFromNullOr(AgentSessionComment).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
    ),
    issue: Schema.OptionFromNullOr(AgentSessionIssue).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
    ),
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);
export type AgentSessionWebhookPayload = Schema.Schema.Type<
  typeof AgentSessionWebhookPayload
>;

export const AgentSessionEvent = Schema.Struct({
  type: Schema.Literal("AgentSessionEvent"),
  action: Schema.String,
  organizationId: Schema.String,
  appUserId: Schema.String,
  oauthClientId: Schema.String,
  webhookId: Schema.String,
  webhookTimestamp: Schema.Number,
  createdAt: Schema.String.pipe(
    Schema.withDecodingDefaultType(
      Effect.sync(() => "1970-01-01T00:00:00.000Z"),
    ),
  ),
  promptContext: Schema.OptionFromNullOr(Schema.String).pipe(
    Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
  ),
  guidance: Schema.OptionFromNullOr(
    Schema.Array(Schema.Struct({ body: Schema.String })),
  ).pipe(Schema.withDecodingDefaultType(Effect.sync(() => Option.none()))),
  previousComments: Schema.OptionFromNullOr(
    Schema.Array(AgentSessionComment),
  ).pipe(Schema.withDecodingDefaultType(Effect.sync(() => Option.none()))),
  agentActivity: Schema.OptionFromNullOr(AgentSessionActivity).pipe(
    Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
  ),
  agentSession: AgentSessionWebhookPayload,
});
export type AgentSessionEvent = Schema.Schema.Type<typeof AgentSessionEvent>;

/**
 * Linear SDK 89.0.0's AppUserNotification webhook envelope. The embedded
 * notification is a union of notification payloads; the shared fields below
 * are present on every generated variant, while issue details are optional for
 * generic notifications.
 */
export const AppUserNotificationIssue = Schema.StructWithRest(
  Schema.Struct({
    id: Schema.String,
    identifier: Schema.String,
    team: Schema.Struct({
      id: Schema.String,
      key: Schema.String,
      name: Schema.String,
    }),
    teamId: Schema.String,
    title: Schema.String,
    url: Schema.String,
    description: Schema.OptionFromNullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
    ),
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);
export type AppUserNotificationIssue = Schema.Schema.Type<
  typeof AppUserNotificationIssue
>;

export const AppUserNotificationDetails = Schema.StructWithRest(
  Schema.Struct({
    id: Schema.String,
    type: Schema.String,
    userId: Schema.String,
    createdAt: Schema.String,
    updatedAt: Schema.String,
    issueId: Schema.OptionFromNullOr(Schema.String).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
    ),
    issue: Schema.OptionFromNullOr(AppUserNotificationIssue).pipe(
      Schema.withDecodingDefaultType(Effect.sync(() => Option.none())),
    ),
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);
export type AppUserNotificationDetails = Schema.Schema.Type<
  typeof AppUserNotificationDetails
>;

export const AppUserNotification = Schema.Struct({
  type: Schema.Literal("AppUserNotification"),
  action: Schema.String,
  appUserId: Schema.String,
  createdAt: Schema.String,
  notification: AppUserNotificationDetails,
  oauthClientId: Schema.String,
  organizationId: Schema.String,
  webhookId: Schema.String,
  webhookTimestamp: Schema.Number,
});
export type AppUserNotification = Schema.Schema.Type<
  typeof AppUserNotification
>;
