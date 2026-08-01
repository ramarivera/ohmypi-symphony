import { it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Schema } from "effect";
import pino from "pino";
import { describe, expect } from "vitest";
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
} from "../src/domain/ids.js";
import {
  AgentRun,
  AgentSessionActivity,
  AgentSessionComment,
  AgentSessionEvent,
  AgentSessionIssue,
  AgentSessionWebhookPayload,
  Installation,
  ProjectionJob,
  RepositoryDefinition,
  RepositoryRecord,
  RunEvent,
  RunInput,
  WebhookDelivery,
} from "../src/domain/models.js";
import { GatewayConfig } from "../src/services/config.js";
import { redactionPaths } from "../src/services/logger.js";

const configValues = new Map<string, string>([
  ["LINEAR_CLIENT_ID", "client"],
  ["LINEAR_CLIENT_SECRET", "client-secret"],
  ["LINEAR_WEBHOOK_SECRET", "webhook-secret"],
  [
    "TOKEN_ENCRYPTION_KEY",
    Buffer.from(new Uint8Array(32).fill(7)).toString("base64"),
  ],
  ["PUBLIC_URL", "http://localhost:3000"],
]);

const configLayer = GatewayConfig.Default.pipe(
  Layer.provide(Layer.setConfigProvider(ConfigProvider.fromMap(configValues))),
);
const roundTrip = <A, I>(schema: Schema.Schema<A, I>, value: unknown) => {
  const decoded = Schema.decodeUnknownSync(schema)(value);
  const encoded = Schema.encodeUnknownSync(schema)(decoded);
  expect(Schema.decodeUnknownSync(schema)(encoded)).toEqual(decoded);
};

describe("Effect foundation", () => {
  it.layer(configLayer)("configuration", (it) => {
    it.effect("applies operational defaults", () =>
      Effect.gen(function* () {
        const config = yield* GatewayConfig;
        expect(config.port).toBe(3000);
        expect(config.leaseDurationMs).toBe(60_000);
        expect(config.webhookReplayWindowMs).toBe(60_000);
        expect(config.publicUrl.toString()).toBe("http://localhost:3000/");
      }),
    );
  });

  it.effect("rejects invalid public URLs", () =>
    Effect.gen(function* () {
      const layer = GatewayConfig.Default.pipe(
        Layer.provide(
          Layer.setConfigProvider(
            ConfigProvider.fromMap(
              new Map([...configValues, ["PUBLIC_URL", "http://example.com"]]),
            ),
          ),
        ),
      );
      const result = yield* Effect.either(
        Effect.gen(function* () {
          return yield* GatewayConfig;
        }).pipe(Effect.provide(layer)),
      );
      expect(result._tag).toBe("Left");
    }),
  );

  it.effect.prop(
    "branded identifier schemas decode arbitrary UUID strings",
    { value: Schema.UUID },
    ({ value }) =>
      Effect.gen(function* () {
        for (const id of [
          SessionId,
          DeliveryId,
          OrganizationId,
          IssueId,
          AppUserId,
          ActivityId,
          InputId,
          ProjectId,
          SourceKey,
          TeamId,
          WorkspaceId,
        ]) {
          expect(yield* Schema.decodeUnknown(id)(value)).toBe(value);
          roundTrip(id, value);
        }
      }),
    { timeout: 15_000, fastCheck: { numRuns: 20 } },
  );

  it.prop(
    "domain schemas preserve representative decode/encode round-trips",
    { nonce: Schema.String },
    ({ nonce }) => {
      const repository = {
        id: nonce,
        url: nonce,
        ref: nonce,
        teamIds: [nonce],
        projectIds: [nonce],
      };
      roundTrip(RepositoryDefinition, repository);
      roundTrip(RepositoryRecord, {
        ...repository,
        organizationId: nonce,
        labels: [nonce],
        isDefault: true,
        createdAt: 1,
        updatedAt: 2,
      });
      roundTrip(Installation, {
        organizationId: nonce,
        appUserId: nonce,
        accessToken: nonce,
        refreshToken: nonce,
        expiresAt: 1,
        scopes: [nonce],
        revokedAt: null,
        accessibleTeamIds: null,
        canAccessAllPublicTeams: null,
      });
      roundTrip(AgentRun, {
        sessionId: nonce,
        organizationId: nonce,
        issueId: null,
        repositoryId: null,
        state: "queued",
        desiredState: "running",
        ompSessionId: null,
        ompSessionFile: null,
        workspacePath: null,
        teamId: null,
        projectId: null,
        attempt: 0,
        leaseOwner: null,
        leaseExpiresAt: null,
        lastActivityAt: null,
        terminalReason: null,
        nextAttemptAt: null,
        createdAt: 1,
        updatedAt: 2,
      });
      roundTrip(WebhookDelivery, {
        id: nonce,
        organizationId: nonce,
        payloadHash: nonce,
        receivedAt: 1,
        status: "pending",
        error: null,
      });
      roundTrip(RunInput, {
        id: nonce,
        sessionId: nonce,
        kind: "prompted",
        body: nonce,
        payload: { nonce },
        createdAt: 1,
      });
      roundTrip(RunEvent, {
        sourceKey: nonce,
        sessionId: nonce,
        kind: nonce,
        level: "info",
        status: null,
        text: null,
        payload: { nonce },
        error: null,
        createdAt: 1,
        updatedAt: 2,
      });
      roundTrip(ProjectionJob, {
        sourceKey: nonce,
        sessionId: nonce,
        activityType: nonce,
        payload: { nonce },
        attempt: 0,
        payloadHash: nonce,
        nextAttemptAt: 1,
        createdAt: 1,
      });
      roundTrip(AgentSessionComment, { id: nonce, body: nonce });
      roundTrip(AgentSessionIssue, {
        id: nonce,
        title: nonce,
        description: nonce,
        identifier: nonce,
        url: nonce,
        teamId: nonce,
        projectId: nonce,
      });
      roundTrip(AgentSessionActivity, {
        id: nonce,
        agentSessionId: nonce,
        content: { nonce },
        signal: nonce,
      });
      const agentSession = {
        id: nonce,
        appUserId: nonce,
        organizationId: nonce,
        status: nonce,
        createdAt: nonce,
        updatedAt: nonce,
        issueId: nonce,
        commentId: nonce,
        sourceCommentId: nonce,
        summary: nonce,
        url: nonce,
        archivedAt: nonce,
        startedAt: nonce,
        endedAt: nonce,
        comment: { id: nonce, body: nonce },
        issue: {
          id: nonce,
          title: nonce,
          description: nonce,
          identifier: nonce,
          url: nonce,
          teamId: nonce,
          projectId: nonce,
        },
      };
      roundTrip(AgentSessionWebhookPayload, agentSession);
      roundTrip(AgentSessionEvent, {
        type: "AgentSessionEvent",
        action: nonce,
        organizationId: nonce,
        appUserId: nonce,
        oauthClientId: nonce,
        webhookId: nonce,
        webhookTimestamp: 1,
        createdAt: nonce,
        promptContext: nonce,
        guidance: [{ body: nonce }],
        previousComments: [{ id: nonce, body: nonce }],
        agentActivity: {
          id: nonce,
          agentSessionId: nonce,
          content: { nonce },
          signal: nonce,
        },
        agentSession,
      });
    },
  );

  it.prop(
    "logger redaction removes configured secret values",
    { secret: Schema.String.pipe(Schema.minLength(1)) },
    ({ secret }) => {
      let line = "";
      const logger = pino(
        { redact: { paths: redactionPaths, censor: "[REDACTED]" } },
        {
          write: (chunk) => {
            line += String(chunk);
          },
        },
      );
      logger.info(
        { nested: { token: secret }, clientSecret: secret },
        "redaction-check",
      );
      const fields = Schema.decodeUnknownSync(
        Schema.Struct({
          nested: Schema.Struct({ token: Schema.String }),
          clientSecret: Schema.String,
        }),
      )(JSON.parse(line));
      expect(fields.nested.token).toBe("[REDACTED]");
      expect(fields.clientSecret).toBe("[REDACTED]");
    },
  );
});
