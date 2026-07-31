import { Schema } from "effect";

export const SessionId = Schema.String.pipe(Schema.brand("@Gateway/SessionId"));
export type SessionId = Schema.Schema.Type<typeof SessionId>;

export const DeliveryId = Schema.String.pipe(
  Schema.brand("@Gateway/DeliveryId"),
);
export type DeliveryId = Schema.Schema.Type<typeof DeliveryId>;

export const OrganizationId = Schema.String.pipe(
  Schema.brand("@Gateway/OrganizationId"),
);
export type OrganizationId = Schema.Schema.Type<typeof OrganizationId>;

export const IssueId = Schema.String.pipe(Schema.brand("@Gateway/IssueId"));
export type IssueId = Schema.Schema.Type<typeof IssueId>;

export const TeamId = Schema.String.pipe(Schema.brand("@Gateway/TeamId"));
export type TeamId = Schema.Schema.Type<typeof TeamId>;

export const ProjectId = Schema.String.pipe(Schema.brand("@Gateway/ProjectId"));
export type ProjectId = Schema.Schema.Type<typeof ProjectId>;

export const InputId = Schema.String.pipe(Schema.brand("@Gateway/InputId"));
export type InputId = Schema.Schema.Type<typeof InputId>;

export const SourceKey = Schema.String.pipe(Schema.brand("@Gateway/SourceKey"));
export type SourceKey = Schema.Schema.Type<typeof SourceKey>;

export const ActivityId = Schema.String.pipe(
  Schema.brand("@Gateway/ActivityId"),
);
export type ActivityId = Schema.Schema.Type<typeof ActivityId>;

export const AppUserId = Schema.String.pipe(Schema.brand("@Gateway/AppUserId"));
export type AppUserId = Schema.Schema.Type<typeof AppUserId>;

export const WorkspaceId = Schema.String.pipe(
  Schema.brand("@Gateway/WorkspaceId"),
);
export type WorkspaceId = Schema.Schema.Type<typeof WorkspaceId>;
