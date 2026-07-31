import { Schema } from "effect";

const message = { message: Schema.String, cause: Schema.optional(Schema.String) };

export class DatabaseError extends Schema.TaggedError<DatabaseError>()("@Gateway/DatabaseError", message) {}
export class RowDecodeError extends Schema.TaggedError<RowDecodeError>()("@Gateway/RowDecodeError", { ...message, entity: Schema.String }) {}
export class TokenCipherError extends Schema.TaggedError<TokenCipherError>()(
  "@Gateway/TokenCipherError",
  {
    ...message,
    reason: Schema.Literal("key_import", "invalid_key", "encrypt", "decrypt", "version_mismatch", "truncate"),
  },
) {}
export class LinearApiError extends Schema.TaggedError<LinearApiError>()("@Gateway/LinearApiError", { ...message, operation: Schema.String, status: Schema.optional(Schema.Number) }) {}
export class LinearRateLimitError extends Schema.TaggedError<LinearRateLimitError>()("@Gateway/LinearRateLimitError", { ...message, retryAfterMs: Schema.optional(Schema.Number) }) {}
export class TokenRefreshError extends Schema.TaggedError<TokenRefreshError>()("@Gateway/TokenRefreshError", { ...message, organizationId: Schema.String }) {}
export class WebhookSignatureError extends Schema.TaggedError<WebhookSignatureError>()("@Gateway/WebhookSignatureError", message) {}
export class WebhookReplayError extends Schema.TaggedError<WebhookReplayError>()("@Gateway/WebhookReplayError", { ...message, timestamp: Schema.Number }) {}
export class WebhookPayloadError extends Schema.TaggedError<WebhookPayloadError>()("@Gateway/WebhookPayloadError", { ...message, status: Schema.Number }) {}
export class WebhookIdentityError extends Schema.TaggedError<WebhookIdentityError>()("@Gateway/WebhookIdentityError", message) {}
export class DeliveryConflictError extends Schema.TaggedError<DeliveryConflictError>()("@Gateway/DeliveryConflictError", { ...message, deliveryId: Schema.String }) {}
export class RunLeaseError extends Schema.TaggedError<RunLeaseError>()("@Gateway/RunLeaseError", { ...message, sessionId: Schema.String }) {}
export class RpcSpawnError extends Schema.TaggedError<RpcSpawnError>()("@Gateway/RpcSpawnError", message) {}
export class RpcProtocolError extends Schema.TaggedError<RpcProtocolError>()("@Gateway/RpcProtocolError", { ...message, method: Schema.String }) {}
export class RpcTimeoutError extends Schema.TaggedError<RpcTimeoutError>()("@Gateway/RpcTimeoutError", { ...message, method: Schema.String }) {}
export class WorkspaceError extends Schema.TaggedError<WorkspaceError>()("@Gateway/WorkspaceError", { ...message, sessionId: Schema.String, reason: Schema.Literal("root_not_directory", "root_is_symlink", "path_escapes_root", "target_is_symlink", "target_not_directory", "marker_mismatch", "git_failed") }) {}
export class OAuthStateError extends Schema.TaggedError<OAuthStateError>()("@Gateway/OAuthStateError", message) {}
export class InstallationRevokedError extends Schema.TaggedError<InstallationRevokedError>()("@Gateway/InstallationRevokedError", { ...message, organizationId: Schema.String }) {}
