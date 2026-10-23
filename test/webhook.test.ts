import { createHmac } from "node:crypto";
import { Writable } from "node:stream";
import { it } from "@effect/vitest";
import {
  Cause,
  Clock,
  ConfigProvider,
  Deferred,
  Effect,
  Fiber,
  Layer,
  Logger,
  Option,
  Redacted,
  References,
  Schema,
} from "effect";
import * as fc from "fast-check";
import pino from "pino";
import { describe, expect, vi } from "vitest";
import { recoverGateway } from "../src/conductor.js";
import { DatabaseError } from "../src/domain/errors.js";
import {
  AppUserId,
  DeliveryId,
  IssueId,
  OrganizationId,
  ProjectId,
  SessionId,
  TeamId,
} from "../src/domain/ids.js";
import { AgentSessionEvent, type Installation } from "../src/domain/models.js";
import {
  GatewayConfig,
  type GatewayConfigShape,
} from "../src/services/config.js";
import { makePinoEffectLogger } from "../src/services/logger.js";
import { deliveryReplayHash } from "../src/services/store/delivery-repo.js";
import {
  DeliveryRepo,
  InstallationRepo,
  PromptTemplateRepo,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
} from "../src/services/store/repositories.js";
import {
  SqliteClient,
  SqliteClientLive,
} from "../src/services/store/sqlite-client.js";
import { TokenCrypto } from "../src/services/token-crypto.js";
import {
  payloadHash,
  verifySignature,
  WebhookPipeline,
} from "../src/services/webhook.js";
import { suitePaths } from "./fixtures/paths.js";

const suitePath = suitePaths();

const secret = "webhook-secret";
const tokenEncryptionKey = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=";
const config: GatewayConfigShape = {
  githubAppId: undefined,
  githubAppPrivateKey: undefined,
  linearClientId: "client",
  linearClientSecret: Redacted.make("client-secret"),
  linearWebhookSecret: Redacted.make(secret),
  tokenEncryptionKey: Redacted.make(tokenEncryptionKey),
  publicUrl: new URL("https://gateway.example.com"),
  databasePath: ":memory:",
  nixBinaryPath: "nix",
  nixpkgsFlakeRef:
    "github:NixOS/nixpkgs/0123456789abcdef0123456789abcdef01234567",
  nixRootsDir: suitePath("nix-roots"),
  nixGcMaxBytes: 1_000_000,
  workspaceRoot: "/workspaces",
  ompCliPath: "omp",
  port: 3000,
  leaseDurationMs: 60_000,
  reconcilerIntervalMs: 1_000,
  reconcilerCatchupIntervalMs: 300_000,
  reconcilerCatchupMinAgeMs: 120_000,
  webhookReplayWindowMs: 60_000,
  repositorySuggestionConfidenceThreshold: 0.8,
  logLevel: "silent",
  logFile: Option.none(),
};
const configProviderFor = (gatewayConfig: GatewayConfigShape) =>
  ConfigProvider.fromUnknown(
    Object.fromEntries(
      new Map([
        ...(gatewayConfig.allowedOrganizationIds === undefined
          ? []
          : [
              [
                "LINEAR_ALLOWED_ORGANIZATION_IDS",
                [...gatewayConfig.allowedOrganizationIds].join(","),
              ] as const,
            ]),
        ["LINEAR_CLIENT_ID", gatewayConfig.linearClientId],
        ["LINEAR_CLIENT_SECRET", "client-secret"],
        ["LINEAR_WEBHOOK_SECRET", secret],
        ["TOKEN_ENCRYPTION_KEY", tokenEncryptionKey],
        ["PUBLIC_URL", gatewayConfig.publicUrl.toString()],
        ["DATABASE_PATH", gatewayConfig.databasePath],
        ["WORKSPACE_ROOT", gatewayConfig.workspaceRoot],
        ["OMP_CLI_PATH", gatewayConfig.ompCliPath],
        ["PORT", String(gatewayConfig.port)],
        ["LEASE_DURATION_MS", String(gatewayConfig.leaseDurationMs)],
        ["RECONCILER_INTERVAL_MS", String(gatewayConfig.reconcilerIntervalMs)],
        [
          "WEBHOOK_REPLAY_WINDOW_MS",
          String(gatewayConfig.webhookReplayWindowMs),
        ],
        ["LOG_LEVEL", gatewayConfig.logLevel],
        ["NIX_BINARY_PATH", gatewayConfig.nixBinaryPath],
        ["NIXPKGS_FLAKE_REF", gatewayConfig.nixpkgsFlakeRef],
        ["NIX_ROOTS_DIR", gatewayConfig.nixRootsDir],
        ["NIX_GC_MAX_BYTES", String(gatewayConfig.nixGcMaxBytes)],
      ]),
    ),
    { preserveEmptyStrings: true },
  );

const sessionId = (value: string) => Schema.decodeUnknownSync(SessionId)(value);
const organizationId = (value: string) =>
  Schema.decodeUnknownSync(OrganizationId)(value);
const appUserId = (value: string) => Schema.decodeUnknownSync(AppUserId)(value);
const issueId = (value: string) => Schema.decodeUnknownSync(IssueId)(value);
const teamId = (value: string) => Schema.decodeUnknownSync(TeamId)(value);
const projectId = (value: string) => Schema.decodeUnknownSync(ProjectId)(value);

const makeLayer = (gatewayConfig: GatewayConfigShape) =>
  Layer.mergeAll(
    GatewayConfig.layer,
    TokenCrypto.layer,
    InstallationRepo.layer,
    DeliveryRepo.layer,
    RunEventRepo.layer,
    RunRepo.layer,
    PromptTemplateRepo.layer,
    RunInputRepo.layer,
    WebhookPipeline.layer,
  ).pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        SqliteClientLive(gatewayConfig.databasePath),
        ConfigProvider.layer(configProviderFor(gatewayConfig)),
      ),
    ),
  );

const withWebhook = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    | WebhookPipeline
    | InstallationRepo
    | RunRepo
    | RunInputRepo
    | DeliveryRepo
    | PromptTemplateRepo
    | SqliteClient
  >,
  gatewayConfig = config,
) =>
  Effect.gen(function* () {
    yield* Effect.scope;
    return yield* effect.pipe(Effect.provide(makeLayer(gatewayConfig)));
  });

const sign = (body: string, signingSecret = secret): string =>
  createHmac("sha256", signingSecret).update(body).digest("hex");

let deliveryCounter = 0;

type RequestOptions = {
  readonly delivery?: string;
  readonly includeTimestampHeader?: boolean;
  readonly body?: string;
  readonly signature?: string;
  readonly method?: string;
  readonly url?: string;
};

const signedRequest = (
  payload: Record<string, unknown>,
  options: RequestOptions = {},
): Request => {
  const body = options.body ?? JSON.stringify(payload);
  const delivery =
    options.delivery ??
    `${typeof payload.webhookId === "string" ? payload.webhookId : "delivery"}-${++deliveryCounter}`;
  const headers = new Headers({
    "content-type": "application/json",
    "linear-signature": options.signature ?? sign(body),
    "linear-delivery": delivery,
  });
  if (options.includeTimestampHeader) {
    headers.set("linear-timestamp", String(payload.webhookTimestamp));
  }
  return new Request(
    options.url ?? "https://gateway.example.com/webhooks/linear",
    {
      method: options.method ?? "POST",
      headers,
      body,
    },
  );
};

type InstallationOverrides = {
  readonly organizationId?: OrganizationId;
  readonly appUserId?: AppUserId;
  readonly revokedAt?: Option.Option<number>;
  readonly accessibleTeamIds?: Option.Option<ReadonlyArray<TeamId>>;
  readonly canAccessAllPublicTeams?: Option.Option<boolean>;
};

const install = (overrides: InstallationOverrides = {}) =>
  Effect.gen(function* () {
    const installationRepo = yield* InstallationRepo;
    const installation: Installation = {
      organizationId: overrides.organizationId ?? organizationId("org"),
      appUserId: overrides.appUserId ?? appUserId("app-user"),
      accessToken: "access",
      refreshToken: "refresh",
      expiresAt: 1_060_000,
      scopes: ["read", "write", "app:assignable", "app:mentionable"],
      revokedAt: overrides.revokedAt ?? Option.none(),
      accessibleTeamIds:
        overrides.accessibleTeamIds ?? Option.some([teamId("team-a")]),
      canAccessAllPublicTeams:
        overrides.canAccessAllPublicTeams ?? Option.some(false),
    };
    yield* installationRepo.put(installation);
  });

const agentSessionBase = (
  overrides: Record<string, unknown> = {},
): Record<string, unknown> => ({
  id: "session-1",
  appUserId: "app-user",
  organizationId: "org",
  status: "pending",
  createdAt: "2024-01-01T00:00:00.000Z",
  updatedAt: "2024-01-01T00:00:00.000Z",
  ...overrides,
});

const createdPayload = (
  now: number,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> => ({
  type: "AgentSessionEvent",
  action: "created",
  organizationId: "org",
  appUserId: "app-user",
  oauthClientId: "client",
  webhookId: "webhook-config",
  webhookTimestamp: now,
  promptContext: "Implement the issue",
  guidance: [{ body: "Use TypeScript" }, { body: "Write tests" }],
  previousComments: [{ body: "Earlier comment" }],
  agentSession: agentSessionBase({
    issueId: "issue-1",
    issue: {
      id: "issue-1",
      title: "Fix the bug",
      description: "The bug description",
      identifier: "TEAM-123",
      url: "https://linear.app/issue/TEAM-123",
      teamId: "team-a",
      projectId: "project-1",
    },
    comment: { id: "comment-1", body: "A thread comment" },
  }),
  ...overrides,
});

const promptedPayload = (
  now: number,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> => ({
  type: "AgentSessionEvent",
  action: "prompted",
  organizationId: "org",
  appUserId: "app-user",
  oauthClientId: "client",
  webhookId: "webhook-config",
  webhookTimestamp: now,
  agentActivity: {
    id: "activity-prompt-1",
    agentSessionId: "session-1",
    content: {
      type: "prompt",
      body: "Please add more tests",
      title: "Follow-up prompt",
    },
  },
  agentSession: agentSessionBase({
    status: "active",
    updatedAt: "2024-01-01T00:00:01.000Z",
  }),
  ...overrides,
});

const stopPayload = (
  now: number,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> => ({
  type: "AgentSessionEvent",
  action: "stop",
  organizationId: "org",
  appUserId: "app-user",
  oauthClientId: "client",
  webhookId: "webhook-config",
  webhookTimestamp: now,
  agentActivity: {
    id: "activity-stop-1",
    agentSessionId: "session-1",
    content: { type: "prompt", body: "Stop" },
    signal: "stop",
  },
  agentSession: agentSessionBase({
    status: "active",
    updatedAt: "2024-01-01T00:00:02.000Z",
    issueId: "issue-1",
    issue: { id: "issue-1", title: "Fix the bug", teamId: "team-a" },
  }),
  ...overrides,
});

const permissionChangePayload = (
  now: number,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> => ({
  type: "PermissionChange",
  action: "teamAccessChanged",
  organizationId: "org",
  appUserId: "app-user",
  oauthClientId: "client",
  webhookId: "webhook-config",
  webhookTimestamp: now,
  addedTeamIds: [],
  removedTeamIds: ["team-b"],
  canAccessAllPublicTeams: false,
  ...overrides,
});

const appUserNotificationPayload = (
  now: number,
  action: string,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> => ({
  type: "AppUserNotification",
  action,
  appUserId: "app-user",
  createdAt: "2024-01-01T00:00:00.000Z",
  oauthClientId: "client",
  organizationId: "org",
  webhookId: "notification-webhook",
  webhookTimestamp: now,
  notification: {
    id: "notification-1",
    type: action,
    userId: "app-user",
    createdAt: "2024-01-01T00:00:00.000Z",
    updatedAt: "2024-01-01T00:00:00.000Z",
    issueId: "issue-1",
    issue: {
      id: "issue-1",
      identifier: "TEAM-123",
      team: { id: "team-a", key: "TEAM", name: "Team" },
      teamId: "team-a",
      title: "Fix the bug",
      url: "https://linear.app/issue/TEAM-123",
      description: null,
    },
  },
  ...overrides,
});

const oauthAppPayload = (
  now: number,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> => ({
  type: "OAuthApp",
  action: "revoked",
  organizationId: "org",
  oauthClientId: "client",
  webhookId: "webhook-config",
  webhookTimestamp: now,
  ...overrides,
});

const currentTime = Clock.currentTimeMillis;

const expectSome = <A>(value: Option.Option<A>): A | undefined => {
  expect(Option.isSome(value)).toBe(true);

  return Option.isSome(value) ? value.value : undefined;
};
describe("WebhookPipeline Pino lifecycle logs", () => {
  it.live(
    "emits correlated success and controlled-rejection logs through the gateway graph",
    () => {
      const chunks: string[] = [];
      const stream = new Writable({
        write(chunk, _encoding, callback) {
          chunks.push(chunk.toString());
          callback();
        },
      });
      const pinoLoggerLive = Logger.layer([
        makePinoEffectLogger(pino({ base: null, level: "info" }, stream)),
      ]);

      return withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          const accepted = yield* WebhookPipeline.use((service) =>
            service.handle(signedRequest(createdPayload(now))),
          );
          const rejected = yield* WebhookPipeline.use((service) =>
            service.handle(
              signedRequest(createdPayload(now), { method: "GET" }),
            ),
          );
          expect(accepted.status).toBe(200);
          expect(rejected.status).toBe(405);

          const lines = chunks.map(
            (chunk) => JSON.parse(chunk) as Record<string, unknown>,
          );
          const success = lines.find(
            (line) => line.msg === "webhook.processed",
          );
          const failure = lines.find(
            (line) =>
              line.msg === "webhook.rejected" &&
              line.reason === "method not allowed",
          );
          expect(success).toMatchObject({
            level: 30,
            span_name: "WebhookPipeline.handle",
          });
          expect(failure).toMatchObject({
            level: 40,
            span_name: "WebhookPipeline.handle",
            reason: "method not allowed",
          });
          expect(typeof success?.trace_id).toBe("string");
          expect(typeof failure?.trace_id).toBe("string");
        }),
      ).pipe(Effect.provide(pinoLoggerLive));
    },
  );
});

describe("Linear webhook input correctness", () => {
  it.live("created event is durably stored and acknowledged", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const response = yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(createdPayload(now))),
        );

        expect(response.status).toBe(200);
        expect(yield* Effect.promise(() => response.text())).toBe("OK");
        const run = expectSome(
          yield* RunRepo.use((service) => service.get(sessionId("session-1"))),
        );
        expect(run).toMatchObject({
          organizationId: organizationId("org"),
          issueId: Option.some(issueId("issue-1")),
          teamId: Option.some(teamId("team-a")),
          projectId: Option.some(projectId("project-1")),
          desiredState: "running",
        });
        const inputs = yield* RunInputRepo.use((service) =>
          service.pending(sessionId("session-1")),
        );
        expect(inputs).toHaveLength(1);
        expect(inputs[0]).toMatchObject({
          id: "session-1:created",
          kind: "created",
        });
        const body = inputs[0]?.body ?? "";
        for (const text of [
          "User request:",
          "Implement the issue",
          "Fix the bug",
          "TEAM-123",
          "The bug description",
          "A thread comment",
          "Earlier comment",
          "Guidance:",
          "Use TypeScript",
          "Write tests",
        ]) {
          expect(body).toContain(text);
        }
      }),
    ),
  );

  it.live(
    "created input uses structured context and guidance, not tracker text as policy",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          const response = yield* WebhookPipeline.use((service) =>
            service.handle(
              signedRequest(
                createdPayload(now, {
                  promptContext: "Please resolve this",
                  guidance: [{ body: "Use idiomatic TypeScript" }],
                }),
              ),
            ),
          );

          expect(response.status).toBe(200);
          const [input] = yield* RunInputRepo.use((service) =>
            service.pending(sessionId("session-1")),
          );
          const body = input?.body ?? "";
          expect(body.startsWith("User request:\nPlease resolve this")).toBe(
            true,
          );
          expect(body).toContain("Issue context:");
          expect(body).toContain("Guidance:");
          expect(body).toContain("Use idiomatic TypeScript");
          expect(body.indexOf("Guidance:")).toBeGreaterThan(
            body.indexOf("Issue context:"),
          );
        }),
      ),
  );

  it.live("created handles nullable fields and falls back to summary", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const response = yield* WebhookPipeline.use((service) =>
          service.handle(
            signedRequest(
              createdPayload(now, {
                promptContext: null,
                guidance: null,
                previousComments: null,
                agentSession: agentSessionBase({
                  summary: "Summary of direct chat",
                  issueId: null,
                  issue: null,
                  comment: null,
                }),
              }),
            ),
          ),
        );

        expect(response.status).toBe(200);
        const run = expectSome(
          yield* RunRepo.use((service) => service.get(sessionId("session-1"))),
        );
        expect(run).toMatchObject({
          issueId: Option.none(),
          teamId: Option.none(),
          projectId: Option.none(),
        });
        const [input] = yield* RunInputRepo.use((service) =>
          service.pending(sessionId("session-1")),
        );
        expect(input).toMatchObject({
          kind: "created",
          body: "User request:\nSummary of direct chat",
        });
      }),
    ),
  );

  it.live(
    "accepts real Linear AgentSessionEvent created payload with null guidance and no agentSession.type",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          const response = yield* WebhookPipeline.use((service) =>
            service.handle(
              signedRequest(
                createdPayload(now, {
                  webhookId: "webhook-uuid",
                  createdAt: "2024-01-01T00:00:00.000Z",
                  guidance: null,
                  previousComments: [
                    {
                      id: "prev-1",
                      body: "Earlier comment",
                      userId: "user-1",
                      issueId: "issue-1",
                    },
                  ],
                  agentSession: agentSessionBase({
                    commentId: "comment-1",
                    sourceCommentId: null,
                    issueId: "issue-1",
                    pullRequestId: null,
                    slugId: "slug-1",
                    archivedAt: null,
                    startedAt: null,
                    endedAt: null,
                    dismissedAt: null,
                    dismissedById: null,
                    externalLink: null,
                    summary: null,
                    url: "https://linear.app/issue/TEAM-123/session-1",
                    externalUrls: [],
                    context: [],
                    sourceMetadata: { source: "linear" },
                    plan: null,
                    workspaceDiff: null,
                    creatorId: "user-1",
                    creator: {
                      id: "user-1",
                      name: "Test User",
                      email: "test@example.com",
                      avatarUrl: "https://example.com/avatar.png",
                      url: "https://linear.app/user/user-1",
                    },
                    comment: {
                      id: "comment-1",
                      body: "A thread comment",
                      userId: "user-1",
                      issueId: "issue-1",
                    },
                    issue: {
                      id: "issue-1",
                      title: "Fix the bug",
                      description: "The bug description",
                      identifier: "TEAM-123",
                      url: "https://linear.app/issue/TEAM-123",
                      teamId: "team-a",
                      team: { id: "team-a" },
                    },
                  }),
                }),
              ),
            ),
          );

          expect(response.status).toBe(200);
          const run = expectSome(
            yield* RunRepo.use((service) =>
              service.get(sessionId("session-1")),
            ),
          );
          expect(run).toMatchObject({
            organizationId: organizationId("org"),
            issueId: Option.some(issueId("issue-1")),
            teamId: Option.some(teamId("team-a")),
          });
          const [input] = yield* RunInputRepo.use((service) =>
            service.pending(sessionId("session-1")),
          );
          expect(input?.kind).toBe("created");
          expect(input?.body).not.toContain("Guidance:");
        }),
      ),
  );

  it.live("prompted event appends agent activity body", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        expect(
          (yield* WebhookPipeline.use((service) =>
            service.handle(signedRequest(createdPayload(now))),
          )).status,
        ).toBe(200);
        const response = yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(promptedPayload(now))),
        );

        expect(response.status).toBe(200);
        const inputs = yield* RunInputRepo.use((service) =>
          service.pending(sessionId("session-1")),
        );
        expect(inputs).toHaveLength(2);
        expect(inputs.find((input) => input.kind === "prompted")).toMatchObject(
          {
            id: "session-1:prompted:activity-prompt-1",
            body: expect.stringContaining("# Follow-up prompt"),
          },
        );
        expect(
          inputs.find((input) => input.kind === "prompted")?.body,
        ).toContain("Please add more tests");
      }),
    ),
  );
  it.live("prompted inputs store the raw activity body", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        yield* PromptTemplateRepo.use((service) =>
          service.upsert({
            organizationId: organizationId("org"),
            kind: "prompted",
            body: "Follow-up:\n{{userRequest}}",
            updatedAt: now,
          }),
        );
        yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(createdPayload(now))),
        );
        const response = yield* WebhookPipeline.use((service) =>
          service.handle(
            signedRequest(
              promptedPayload(now, { webhookId: "prompt-template" }),
            ),
          ),
        );
        expect(response.status).toBe(200);
        const inputs = yield* RunInputRepo.use((service) =>
          service.pending(sessionId("session-1")),
        );
        // Raw body is stored even with a template configured: rendering
        // happens at prompt construction so repository selection and UI
        // answers see the user's literal text.
        expect(inputs.find((input) => input.kind === "prompted")?.body).toBe(
          "# Follow-up prompt\n\nPlease add more tests",
        );
      }),
    ),
  );

  it.live("prompted with stop signal cancels the run", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const payload = promptedPayload(now, {
          agentActivity: {
            id: "activity-prompt-stop",
            agentSessionId: "session-1",
            content: { type: "prompt", body: "Never mind" },
            signal: "stop",
          },
        });
        yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(createdPayload(now))),
        );
        const response = yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(payload)),
        );

        expect(response.status).toBe(200);
        const inputs = yield* RunInputRepo.use((service) =>
          service.pending(sessionId("session-1")),
        );
        expect(inputs.find((input) => input.kind === "stop")).toBeDefined();
        expect(
          expectSome(
            yield* RunRepo.use((service) =>
              service.get(sessionId("session-1")),
            ),
          )?.desiredState,
        ).toBe("canceled");
      }),
    ),
  );

  it.live("stop action cancels the run", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(createdPayload(now))),
        );
        const response = yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(stopPayload(now))),
        );

        expect(response.status).toBe(200);
        expect(
          expectSome(
            yield* RunRepo.use((service) =>
              service.get(sessionId("session-1")),
            ),
          )?.desiredState,
        ).toBe("canceled");
        const inputs = yield* RunInputRepo.use((service) =>
          service.pending(sessionId("session-1")),
        );
        expect(inputs).toHaveLength(2);
        expect(inputs[1]).toMatchObject({ kind: "stop", body: "Stop" });
      }),
    ),
  );

  it.live("prompted events after stop are accepted for resume", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(createdPayload(now))),
        );
        yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(stopPayload(now))),
        );
        const response = yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(promptedPayload(now))),
        );

        // The prompt is enqueued alongside the stop; the session authority
        // decides between honoring the cancellation and resuming the run.
        expect(response.status).toBe(200);
        expect(
          yield* RunInputRepo.use((service) =>
            service.pending(sessionId("session-1")),
          ),
        ).toHaveLength(3);
        expect(
          expectSome(
            yield* RunRepo.use((service) =>
              service.get(sessionId("session-1")),
            ),
          )?.desiredState,
        ).toBe("canceled");
      }),
    ),
  );

  it.live(
    "AppUserNotification unassignment stops every non-terminal issue run idempotently",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          for (const id of ["session-a", "session-b", "session-terminal"]) {
            yield* RunRepo.use((service) =>
              service.create({
                sessionId: sessionId(id),
                organizationId: organizationId("org"),
                issueId: Option.some(issueId("issue-1")),
                now,
              }),
            );
          }
          yield* RunRepo.use((service) =>
            service.update(sessionId("session-terminal"), {
              state: "succeeded",
            }),
          );
          const payload = appUserNotificationPayload(
            now,
            "issueUnassignedFromYou",
          );
          const first = yield* WebhookPipeline.use((service) =>
            service.handle(
              signedRequest(payload, { delivery: "notification-delivery-1" }),
            ),
          );
          const second = yield* WebhookPipeline.use((service) =>
            service.handle(
              signedRequest(payload, { delivery: "notification-delivery-2" }),
            ),
          );

          expect(first.status).toBe(200);
          expect(second.status).toBe(200);
          for (const id of ["session-a", "session-b"]) {
            expect(
              expectSome(
                yield* RunRepo.use((service) => service.get(sessionId(id))),
              )?.desiredState,
            ).toBe("canceled");
            const inputs = yield* RunInputRepo.use((service) =>
              service.pending(sessionId(id)),
            );
            expect(inputs).toHaveLength(1);
            expect(inputs[0]).toMatchObject({
              id: `${id}:stop:notification-1`,
              kind: "stop",
            });
          }
          expect(
            expectSome(
              yield* RunRepo.use((service) =>
                service.get(sessionId("session-terminal")),
              ),
            )?.desiredState,
          ).toBe("running");
          expect(
            yield* RunInputRepo.use((service) =>
              service.pending(sessionId("session-terminal")),
            ),
          ).toHaveLength(0);
        }),
      ),
  );

  it.live(
    "AppUserNotification status changes enqueue a deferred stop and ignore non-cancel actions",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          yield* RunRepo.use((service) =>
            service.create({
              sessionId: sessionId("session-a"),
              organizationId: organizationId("org"),
              issueId: Option.some(issueId("issue-1")),
              now,
            }),
          );
          const status = yield* WebhookPipeline.use((service) =>
            service.handle(
              signedRequest(
                appUserNotificationPayload(now, "issueStatusChanged"),
              ),
            ),
          );
          expect(status.status).toBe(200);
          expect(
            expectSome(
              yield* RunRepo.use((service) =>
                service.get(sessionId("session-a")),
              ),
            )?.desiredState,
          ).toBe("running");
          const inputs = yield* RunInputRepo.use((service) =>
            service.pending(sessionId("session-a")),
          );
          expect(inputs).toHaveLength(1);
          expect(inputs[0]?.payload).toMatchObject({
            type: "AppUserNotification",
            action: "issueStatusChanged",
            notification: { issueId: "issue-1" },
          });

          const ignored = yield* WebhookPipeline.use((service) =>
            service.handle(
              signedRequest(
                appUserNotificationPayload(now, "issueCommentMention", {
                  webhookId: "notification-webhook-ignored",
                }),
              ),
            ),
          );
          expect(ignored.status).toBe(200);
          expect(
            yield* RunInputRepo.use((service) =>
              service.pending(sessionId("session-a")),
            ),
          ).toHaveLength(1);
        }),
      ),
  );

  it.live("dedupes by Linear-Delivery id", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const payload = createdPayload(now);
        const response1 = yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(payload, { delivery: "d1" })),
        );
        const response2 = yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(payload, { delivery: "d1" })),
        );

        expect(response1.status).toBe(200);
        expect(response2.status).toBe(200);
        expect(yield* Effect.promise(() => response2.text())).toBe(
          "Duplicate delivery",
        );
        expect(
          yield* RunInputRepo.use((service) =>
            service.pending(sessionId("session-1")),
          ),
        ).toHaveLength(1);
      }),
    ),
  );

  it.live(
    "dedupes logical input on replay with a different Linear-Delivery id",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          const payload = createdPayload(now);
          const body = JSON.stringify(payload);
          const signature = sign(body);
          const response1 = yield* WebhookPipeline.use((service) =>
            service.handle(
              signedRequest(payload, { delivery: "d1", body, signature }),
            ),
          );
          const response2 = yield* WebhookPipeline.use((service) =>
            service.handle(
              signedRequest(payload, { delivery: "d2", body, signature }),
            ),
          );

          expect(response1.status).toBe(200);
          expect(response2.status).toBe(200);
          expect(
            yield* RunInputRepo.use((service) =>
              service.pending(sessionId("session-1")),
            ),
          ).toHaveLength(1);
        }),
      ),
  );

  it.live(
    "rejects stale and future timestamps with exact replay response",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          for (const timestamp of [now - 120_000, now + 120_000]) {
            const response = yield* WebhookPipeline.use((service) =>
              service.handle(
                signedRequest(createdPayload(timestamp), {
                  delivery: `outside-window-${timestamp}`,
                  includeTimestampHeader: true,
                }),
              ),
            );
            expect(response.status).toBe(401);
            expect(yield* Effect.promise(() => response.text())).toBe(
              "Webhook timestamp outside replay window",
            );
          }
          expect(
            yield* RunRepo.use((service) =>
              service.get(sessionId("session-1")),
            ),
          ).toEqual(Option.none());
        }),
      ),
  );

  it.live("rejects invalid and tampered HMACs without persistence", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const payload = createdPayload(now);
        const body = JSON.stringify(payload);
        for (const request of [
          signedRequest(payload, {
            delivery: "bad-signature",
            signature: "deadbeef",
          }),
          signedRequest(payload, {
            delivery: "tampered-body",
            body: body.replace("Fix the bug", "Fix the BUG"),
            signature: sign(body),
          }),
        ]) {
          const response = yield* WebhookPipeline.use((service) =>
            service.handle(request),
          );
          expect(response.status).toBe(401);
          expect(yield* Effect.promise(() => response.text())).toBe(
            "Invalid signature",
          );
        }
        expect(
          yield* RunRepo.use((service) => service.get(sessionId("session-1"))),
        ).toEqual(Option.none());
      }),
    ),
  );

  it.live("rejects missing signature and non-POST methods", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        const missing = signedRequest(createdPayload(now));
        missing.headers.delete("linear-signature");
        const missingResponse = yield* WebhookPipeline.use((service) =>
          service.handle(missing),
        );
        expect(missingResponse.status).toBe(400);
        expect(yield* Effect.promise(() => missingResponse.text())).toBe(
          "Missing signature",
        );

        const methodResponse = yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(createdPayload(now), { method: "GET" })),
        );
        expect(methodResponse.status).toBe(405);
        expect(yield* Effect.promise(() => methodResponse.text())).toBe(
          "Method not allowed",
        );
      }),
    ),
  );

  it.live("rejects malformed AgentSessionEvent payloads", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const response = yield* WebhookPipeline.use((service) =>
          service.handle(
            signedRequest(createdPayload(now, { agentSession: undefined })),
          ),
        );
        expect(response.status).toBe(400);
        expect(yield* Effect.promise(() => response.text())).toContain(
          "AgentSessionEvent payload is invalid",
        );
      }),
    ),
  );
  it.live(
    "keeps malformed session input out of webhook responses and logs",
    () =>
      withWebhook(
        Effect.gen(function* () {
          yield* install();
          const secret = "malformed-session-private-token";
          const logs: unknown[] = [];
          const logger = Logger.make((entry) => {
            logs.push({ message: entry.message, cause: entry.cause });
          });
          const now = yield* Clock.currentTimeMillis;
          const response = yield* WebhookPipeline.use((service) =>
            service.handle(
              signedRequest(createdPayload(now, { agentSession: secret })),
            ),
          ).pipe(Effect.provide(Logger.layer([logger])));
          expect(response.status).toBe(400);
          expect(yield* Effect.promise(() => response.text())).toBe(
            "AgentSessionEvent payload is invalid",
          );
          expect(JSON.stringify(logs)).not.toContain(secret);
        }),
      ),
  );
  it.live("does not expose malformed notification payload details", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const secretContent = "notification-content-secret";
        const payload = {
          ...appUserNotificationPayload(now, "issueStatusChanged"),
          notification: secretContent,
        };
        const response = yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(payload)),
        );
        const body = yield* Effect.promise(() => response.text());
        expect(response.status).toBe(400);
        expect(body).toBe("AppUserNotification payload is invalid");
        expect(body).not.toContain(secretContent);
      }),
    ),
  );

  it.live("rejects cross-tenant OAuth client identity", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const response = yield* WebhookPipeline.use((service) =>
          service.handle(
            signedRequest(
              createdPayload(now, { oauthClientId: "other-client" }),
            ),
          ),
        );
        expect(response.status).toBe(401);
        expect(yield* Effect.promise(() => response.text())).toBe(
          "OAuth client identity mismatch (received other-client)",
        );
      }),
    ),
  );

  it.live("rejects absent and revoked installations", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        const absent = yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(createdPayload(now))),
        );
        expect(absent.status).toBe(401);
        expect(yield* Effect.promise(() => absent.text())).toBe(
          "No installation for organization",
        );

        yield* install({ revokedAt: Option.some(now - 1_000) });
        const revoked = yield* WebhookPipeline.use((service) =>
          service.handle(
            signedRequest(
              createdPayload(now, { webhookId: "revoked-webhook" }),
            ),
          ),
        );
        expect(revoked.status).toBe(401);
        expect(yield* Effect.promise(() => revoked.text())).toBe(
          "Installation is revoked",
        );
      }),
    ),
  );

  it.live("OAuthApp revoked cancels runs and marks installation revoked", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        yield* RunRepo.use((service) =>
          service.create({
            sessionId: sessionId("session-1"),
            organizationId: organizationId("org"),
            issueId: Option.some(issueId("issue-1")),
            teamId: Option.some(teamId("team-a")),
            now,
          }),
        );

        const response = yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(oauthAppPayload(now))),
        );
        expect(response.status).toBe(200);
        expect(yield* Effect.promise(() => response.text())).toBe("OK");
        const installation = expectSome(
          yield* InstallationRepo.use((service) =>
            service.get(organizationId("org")),
          ),
        );
        expect(installation?.revokedAt).toEqual(Option.some(now));
        expect(
          expectSome(
            yield* RunRepo.use((service) =>
              service.get(sessionId("session-1")),
            ),
          )?.desiredState,
        ).toBe("canceled");
      }),
    ),
  );

  it.live("PermissionChange updates team access and cancels runs", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install({
          accessibleTeamIds: Option.some([teamId("team-a"), teamId("team-b")]),
        });
        yield* RunRepo.use((service) =>
          service.create({
            sessionId: sessionId("session-1"),
            organizationId: organizationId("org"),
            issueId: Option.some(issueId("issue-1")),
            teamId: Option.some(teamId("team-b")),
            now,
          }),
        );

        const response = yield* WebhookPipeline.use((service) =>
          service.handle(signedRequest(permissionChangePayload(now))),
        );
        expect(response.status).toBe(200);
        const installation = expectSome(
          yield* InstallationRepo.use((service) =>
            service.get(organizationId("org")),
          ),
        );
        expect(installation?.accessibleTeamIds).toEqual(
          Option.some([teamId("team-a")]),
        );
        expect(
          expectSome(
            yield* RunRepo.use((service) =>
              service.get(sessionId("session-1")),
            ),
          )?.desiredState,
        ).toBe("canceled");
      }),
    ),
  );

  it.live("PermissionChange rejects mismatched app-user identity", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const response = yield* WebhookPipeline.use((service) =>
          service.handle(
            signedRequest(
              permissionChangePayload(now, { appUserId: "other-user" }),
            ),
          ),
        );
        expect(response.status).toBe(401);
        expect(yield* Effect.promise(() => response.text())).toBe(
          "App user mismatch",
        );
        const installation = expectSome(
          yield* InstallationRepo.use((service) =>
            service.get(organizationId("org")),
          ),
        );
        expect(installation?.accessibleTeamIds).toEqual(
          Option.some([teamId("team-a")]),
        );
      }),
    ),
  );

  it.live(
    "PermissionChange accepts dashed and case variants of the client id",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          // Linear reports the OAuth client id in differing forms across
          // webhook categories (console hex vs dashed UUID); the comparison
          // is normalized, so both forms must pass.
          for (const oauthClientId of ["CLIENT", "c-l-i-e-n-t"]) {
            const response = yield* WebhookPipeline.use((service) =>
              service.handle(
                signedRequest(permissionChangePayload(now, { oauthClientId })),
              ),
            );
            expect(response.status, oauthClientId).toBe(200);
          }
        }),
      ),
  );

  it.live(
    "PermissionChange accepts the OAuth client UUID form Linear actually sends",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          // Live capture 2026-08-11: PermissionChange carries the OAuth
          // client's internal UUID (no shared hex with the console client_id).
          // HMAC is the auth boundary; org + appUserId pin tenant and app.
          const response = yield* WebhookPipeline.use((service) =>
            service.handle(
              signedRequest(
                permissionChangePayload(now, {
                  oauthClientId: "c2e52980-f2d4-4ecf-bc59-06c73dfb42b3",
                }),
              ),
            ),
          );
          expect(response.status).toBe(200);
          const installation = expectSome(
            yield* InstallationRepo.use((service) =>
              service.get(organizationId("org")),
            ),
          );
          expect(installation?.accessibleTeamIds).toEqual(
            Option.some([teamId("team-a")]),
          );
        }),
      ),
  );
  it.live("acknowledges unknown event types without side effects", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const response = yield* WebhookPipeline.use((service) =>
          service.handle(
            signedRequest(createdPayload(now, { type: "Unknown" })),
          ),
        );
        expect(response.status).toBe(200);
        expect(
          yield* RunRepo.use((service) => service.get(sessionId("session-1"))),
        ).toEqual(Option.none());
      }),
    ),
  );

  it.live("acknowledges unknown AgentSession actions without input", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const response = yield* WebhookPipeline.use((service) =>
          service.handle(
            signedRequest(createdPayload(now, { action: "updated" })),
          ),
        );
        expect(response.status).toBe(200);
        expect(
          yield* RunInputRepo.use((service) =>
            service.pending(sessionId("session-1")),
          ),
        ).toHaveLength(0);
      }),
    ),
  );

  it.live("resolves team and project from child objects", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const response = yield* WebhookPipeline.use((service) =>
          service.handle(
            signedRequest(
              createdPayload(now, {
                agentSession: agentSessionBase({
                  issueId: "issue-1",
                  issue: {
                    id: "issue-1",
                    title: "Fix the bug",
                    team: { id: "team-child" },
                    project: { id: "project-child" },
                  },
                }),
              }),
            ),
          ),
        );
        expect(response.status).toBe(200);
        expect(
          expectSome(
            yield* RunRepo.use((service) =>
              service.get(sessionId("session-1")),
            ),
          ),
        ).toMatchObject({
          teamId: Option.some(teamId("team-child")),
          projectId: Option.some(projectId("project-child")),
        });
      }),
    ),
  );
});

describe("webhook signature invariants", () => {
  it("HMAC-SHA256 verification accepts matching bytes and rejects tampered bytes or secrets", () =>
    fc.assert(
      fc.property(
        fc.record({
          body: fc
            .string()
            .filter(
              Schema.is(
                Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
              ),
            ),
          otherBody: fc
            .string()
            .filter(
              Schema.is(
                Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
              ),
            ),
          secret1: fc
            .string()
            .filter(
              Schema.is(
                Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
              ),
            ),
          secret2: fc
            .string()
            .filter(
              Schema.is(
                Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
              ),
            ),
        }),
        ({ body, otherBody, secret1, secret2 }) => {
          const bytes = new TextEncoder().encode(body);
          const signature = sign(body, secret1);
          expect(
            verifySignature(bytes, signature, Redacted.make(secret1)),
          ).toBe(true);
          expect(
            verifySignature(bytes, signature, Redacted.make(secret2)),
          ).toBe(secret1 === secret2);
          expect(
            verifySignature(
              new TextEncoder().encode(otherBody),
              signature,
              Redacted.make(secret1),
            ),
          ).toBe(otherBody === body);
        },
      ),
    ));

  it("AgentSessionEvent schemas preserve decoded nullable fields through encode/decode", () =>
    fc.assert(
      fc.property(
        fc.record({
          prompt: fc.string(),
          guidance: fc.string(),
          summary: fc.string(),
        }),
        ({ prompt, guidance, summary }) => {
          const raw = createdPayload(1_000, {
            promptContext: prompt,
            guidance: [{ body: guidance }],
            agentSession: agentSessionBase({ summary }),
          });
          const decoded = Schema.decodeUnknownSync(AgentSessionEvent)(raw);
          const encoded = Schema.encodeUnknownSync(AgentSessionEvent)(decoded);
          expect(Schema.decodeUnknownSync(AgentSessionEvent)(encoded)).toEqual(
            decoded,
          );
        },
      ),
    ));
});

describe("webhook replay, delivery, and identity invariants", () => {
  it.live(
    "replay-window boundaries accept exactly within the window and reject one millisecond outside",
    () =>
      Effect.gen(function* () {
        const cases = fc.record({
          window: fc
            .integer({ min: 1, max: 10_000 })
            .filter(
              Schema.is(
                Schema.Number.pipe(
                  Schema.check(Schema.isInt()),
                  Schema.check(
                    Schema.isBetween({ minimum: 1, maximum: 10_000 }),
                  ),
                ),
              ),
            ),
          boundary: fc.constantFrom(
            ...["inside", "lower", "upper", "too-old", "too-new"],
          ),
        });
        const check = ({
          window,
          boundary,
        }: typeof cases extends fc.Arbitrary<infer A> ? A : never) =>
          withWebhook(
            Effect.gen(function* () {
              const now = 1_000_000;
              const timestamp =
                boundary === "inside"
                  ? now
                  : boundary === "lower"
                    ? now - window
                    : boundary === "upper"
                      ? now + window
                      : boundary === "too-old"
                        ? now - window - 1
                        : now + window + 1;
              const liveClock = yield* Clock.Clock;
              const clock = {
                currentTimeNanosUnsafe:
                  liveClock.currentTimeNanosUnsafe.bind(liveClock),
                currentTimeNanos: liveClock.currentTimeNanos,
                monotonicTimeNanosUnsafe:
                  liveClock.monotonicTimeNanosUnsafe.bind(liveClock),
                monotonicTimeNanos: liveClock.monotonicTimeNanos,
                sleep: liveClock.sleep.bind(liveClock),
                currentTimeMillisUnsafe: () => now,
                currentTimeMillis: Effect.succeed(now),
              };
              const response = yield* WebhookPipeline.use((service) =>
                service.handle(
                  signedRequest(
                    oauthAppPayload(timestamp, { action: "created" }),
                  ),
                ),
              ).pipe(Effect.provideService(Clock.Clock, clock));
              const accepted =
                boundary === "inside" ||
                boundary === "lower" ||
                boundary === "upper";
              expect(response.status).toBe(accepted ? 200 : 401);
            }),
            { ...config, webhookReplayWindowMs: window },
          );
        const context =
          yield* Effect.context<Effect.Services<ReturnType<typeof check>>>();
        yield* Effect.promise(() =>
          fc.assert(
            fc.asyncProperty(cases, (sample) =>
              Effect.runPromiseWith(context)(check(sample)),
            ),
            { numRuns: 20 },
          ),
        );
      }),
    { timeout: 15_000 },
  );

  it.live(
    "a repeated delivery id is idempotent after durable processing",
    () =>
      Effect.gen(function* () {
        const cases = fc.record({
          delivery: fc
            .string()
            .filter(
              Schema.is(
                Schema.String.pipe(Schema.check(Schema.isMinLength(1))),
              ),
            ),
        });
        const check = ({
          delivery,
        }: typeof cases extends fc.Arbitrary<infer A> ? A : never) =>
          withWebhook(
            Effect.gen(function* () {
              const now = yield* currentTime;
              yield* install();
              const payload = createdPayload(now);
              const first = yield* WebhookPipeline.use((service) =>
                service.handle(signedRequest(payload, { delivery })),
              );
              const second = yield* WebhookPipeline.use((service) =>
                service.handle(signedRequest(payload, { delivery })),
              );
              expect(first.status).toBe(200);
              expect(second.status).toBe(200);
              expect(yield* Effect.promise(() => second.text())).toBe(
                "Duplicate delivery",
              );
              expect(
                yield* RunInputRepo.use((service) =>
                  service.pending(sessionId("session-1")),
                ),
              ).toHaveLength(1);
            }),
          );
        const context =
          yield* Effect.context<Effect.Services<ReturnType<typeof check>>>();
        yield* Effect.promise(() =>
          fc.assert(
            fc.asyncProperty(cases, (sample) =>
              Effect.runPromiseWith(context)(check(sample)),
            ),
            { numRuns: 20 },
          ),
        );
      }),
    { timeout: 15_000 },
  );

  it.live(
    "all generated AgentSession installation identity mismatch forms are rejected",
    () =>
      Effect.gen(function* () {
        const cases = fc.record({
          mismatch: fc.constantFrom(
            ...["organization", "embedded-app-user", "installation-app-user"],
          ),
        });
        const check = ({
          mismatch,
        }: typeof cases extends fc.Arbitrary<infer A> ? A : never) =>
          withWebhook(
            Effect.gen(function* () {
              const now = yield* currentTime;
              const payload =
                mismatch === "organization"
                  ? createdPayload(now, {
                      organizationId: "org-2",
                      agentSession: agentSessionBase({ organizationId: "org" }),
                    })
                  : mismatch === "embedded-app-user"
                    ? createdPayload(now, {
                        appUserId: "other-user",
                        agentSession: agentSessionBase({
                          appUserId: "app-user",
                        }),
                      })
                    : createdPayload(now, {
                        appUserId: "other-user",
                        agentSession: agentSessionBase({
                          appUserId: "other-user",
                        }),
                      });
              if (mismatch === "organization") {
                yield* install({ organizationId: organizationId("org-2") });
              } else {
                yield* install();
              }
              const response = yield* WebhookPipeline.use((service) =>
                service.handle(signedRequest(payload)),
              );
              expect(response.status).toBe(401);
              expect(
                yield* RunRepo.use((service) =>
                  service.get(sessionId("session-1")),
                ),
              ).toEqual(Option.none());
            }),
          );
        const context =
          yield* Effect.context<Effect.Services<ReturnType<typeof check>>>();
        yield* Effect.promise(() =>
          fc.assert(
            fc.asyncProperty(cases, (sample) =>
              Effect.runPromiseWith(context)(check(sample)),
            ),
            { numRuns: 20 },
          ),
        );
      }),
    { timeout: 15_000 },
  );
});

describe("webhook admission security", () => {
  it.live(
    "rejects a signed unadmitted org before delivery/run persistence",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          const req = signedRequest(createdPayload(now), {
            delivery: "denied-delivery",
          });
          const response = yield* WebhookPipeline.use((service) =>
            service.handle(req),
          );
          expect(response.status).toBe(403);
          expect(
            yield* RunRepo.use((service) =>
              service.get(sessionId("session-1")),
            ),
          ).toEqual(Option.none());
          expect(
            yield* RunInputRepo.use((service) =>
              service.pending(sessionId("session-1")),
            ),
          ).toHaveLength(0);
          expect(
            yield* DeliveryRepo.use((service) =>
              service.claim({
                id: "denied-delivery" as import("../src/domain/ids.js").DeliveryId,
                organizationId: "org",
                payloadHash: "different",
                payload: {},
              }),
            ),
          ).toBe("claimed");
        }),
        { ...config, allowedOrganizationIds: new Set(["other-org"]) },
      ),
  );
  it.live(
    "caps webhook bodies before HMAC verification even without Content-Length",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const response = yield* WebhookPipeline.use((service) =>
            service.handle(
              new Request("https://gateway.example/webhooks/linear", {
                method: "POST",
                body: "x".repeat(1024 * 1024 + 1),
              }),
            ),
          );
          expect(response.status).toBe(413);
        }),
      ),
  );
});

describe("Interrupted webhook delivery recovery", () => {
  it.live("reclaims a delivery when its processing fiber was interrupted", () =>
    withWebhook(
      Effect.gen(function* () {
        yield* install();
        const installationRepo = yield* InstallationRepo;
        const pipeline = yield* WebhookPipeline;
        const entered = yield* Deferred.make<void>();
        const spy = vi
          .spyOn(installationRepo, "get")
          .mockImplementation(() =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Effect.never),
            ),
          );
        const now = yield* Clock.currentTimeMillis;
        const payload = createdPayload(now);
        const fiber = yield* Effect.forkChild(
          pipeline.handle(
            signedRequest(payload, { delivery: "interrupted-webhook" }),
          ),
        );
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
        spy.mockRestore();
        const response = yield* pipeline.handle(
          signedRequest(payload, { delivery: "interrupted-webhook" }),
        );
        expect(response.status).toBe(200);
        expect(yield* Effect.promise(() => response.text())).toBe("OK");
        expect(
          Option.isSome(
            yield* RunRepo.use((service) =>
              service.get(sessionId("session-1")),
            ),
          ),
        ).toBe(true);
      }),
    ),
  );
});

describe("Webhook cleanup write failures", () => {
  it.live(
    "logs failed cleanup and retries it on redelivery without a restart",
    () =>
      withWebhook(
        Effect.gen(function* () {
          yield* install();
          const installations = yield* InstallationRepo;
          const deliveries = yield* DeliveryRepo;
          const pipeline = yield* WebhookPipeline;
          const entered = yield* Deferred.make<void>();
          const blocked = vi
            .spyOn(installations, "get")
            .mockImplementation(() =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Effect.never),
              ),
            );
          const mark = vi
            .spyOn(deliveries, "mark")
            .mockImplementation(() =>
              Effect.fail(new DatabaseError({ message: "disk unavailable" })),
            );
          const messages: string[] = [];
          const logger = Logger.make((options) => {
            messages.push(String(options.message));
          });
          const payload = createdPayload(yield* Clock.currentTimeMillis);
          const processing = yield* Effect.forkChild(
            pipeline
              .handle(
                signedRequest(payload, { delivery: "cleanup-write-failure" }),
              )
              .pipe(Effect.provide(Logger.layer([logger]))),
          );
          yield* Deferred.await(entered);
          yield* Fiber.interrupt(processing);
          expect(messages).toContain("webhook.delivery_cleanup_failed");
          blocked.mockRestore();
          // Even when repair also fails, return a retryable error instead of a false duplicate success.
          const unavailable = yield* pipeline.handle(
            signedRequest(payload, { delivery: "cleanup-write-failure" }),
          );
          expect(unavailable.status).toBe(500);
          mark.mockRestore();
          const retried = yield* pipeline.handle(
            signedRequest(payload, { delivery: "cleanup-write-failure" }),
          );
          expect(retried.status).toBe(200);
          expect(yield* Effect.promise(() => retried.text())).toBe("OK");
          expect(
            Option.isSome(
              yield* RunRepo.use((service) =>
                service.get(sessionId("session-1")),
              ),
            ),
          ).toBe(true);
        }),
      ),
  );
});

describe("Webhook claim-owned failure marking", () => {
  it.live.each(["identity", "payload"] as const)(
    "persists a safe reason for a post-claim %s failure",
    (kind) =>
      withWebhook(
        Effect.gen(function* () {
          yield* install();
          const deliveries = yield* DeliveryRepo;
          const mark = yield* Effect.acquireRelease(
            Effect.sync(() => vi.spyOn(deliveries, "mark")),
            (spy) => Effect.sync(() => spy.mockRestore()),
          );
          const original = createdPayload(yield* Clock.currentTimeMillis);
          const payload =
            kind === "identity"
              ? { ...original, oauthClientId: "untrusted-client-value" }
              : { ...original, agentSession: "untrusted-session-value" };
          const response = yield* WebhookPipeline.use((service) =>
            service.handle(signedRequest(payload)),
          );
          expect(response.status).toBe(kind === "identity" ? 401 : 400);
          expect(mark).toHaveBeenCalledTimes(1);
          expect(mark.mock.calls[0]?.slice(1)).toEqual([
            "failed",
            kind === "identity"
              ? "OAuth client identity mismatch"
              : "AgentSessionEvent payload is invalid",
            expect.any(String),
          ]);
        }).pipe(Effect.scoped),
      ),
  );

  it.live(
    "does not overwrite a reclaimed delivery after a tagged processing failure",
    () =>
      withWebhook(
        Effect.gen(function* () {
          yield* install();
          const installations = yield* InstallationRepo;
          const deliveries = yield* DeliveryRepo;
          const pipeline = yield* WebhookPipeline;
          const claim = vi.spyOn(deliveries, "claim");
          const originalClaim = deliveries.claim.bind(deliveries);
          const originalMark = deliveries.mark.bind(deliveries);
          const failed = vi
            .spyOn(installations, "get")
            .mockImplementationOnce(() =>
              Effect.fail(new DatabaseError({ message: "processing failed" })),
            );
          const marks = vi
            .spyOn(deliveries, "mark")
            .mockImplementation((id, status, error) =>
              originalMark(id, status, error).pipe(
                Effect.tap(() =>
                  Effect.gen(function* () {
                    if (status !== "failed") return;
                    const input = claim.mock.calls[0]?.[0];
                    if (input === undefined)
                      throw new Error("claim input missing");
                    expect(yield* originalClaim(input).pipe(Effect.orDie)).toBe(
                      "claimed",
                    );
                  }),
                ),
              ),
            );
          yield* Effect.gen(function* () {
            const payload = createdPayload(yield* Clock.currentTimeMillis);
            const response = yield* pipeline.handle(
              signedRequest(payload, { delivery: "reclaimed-after-failure" }),
            );
            expect(response.status).toBe(500);
            expect(marks).toHaveBeenCalledTimes(1);
            const input = claim.mock.calls[0]?.[0];
            if (input === undefined) throw new Error("claim input missing");
            expect(yield* originalClaim(input)).toBe("duplicate");
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                claim.mockRestore();
                marks.mockRestore();
                failed.mockRestore();
              }),
            ),
          );
        }),
      ),
  );

  it.live("does not mark a delivery when the claim itself fails", () =>
    withWebhook(
      Effect.gen(function* () {
        yield* install();
        const deliveries = yield* DeliveryRepo;
        const claim = vi
          .spyOn(deliveries, "claim")
          .mockImplementation(() =>
            Effect.fail(new DatabaseError({ message: "claim unavailable" })),
          );
        const mark = vi.spyOn(deliveries, "mark");
        yield* Effect.gen(function* () {
          const payload = createdPayload(yield* Clock.currentTimeMillis);
          const response = yield* WebhookPipeline.use((service) =>
            service.handle(signedRequest(payload)),
          );
          expect(response.status).toBe(500);
          expect(mark).not.toHaveBeenCalled();
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              claim.mockRestore();
              mark.mockRestore();
            }),
          ),
        );
      }),
    ),
  );
});

const crashDeliveryId = Schema.decodeUnknownSync(DeliveryId)("crash-delivery");
const replayStatus = (db: import("bun:sqlite").Database) => {
  const row = db
    .query<{ status: string; error: string | null }, []>(
      "SELECT status, error FROM webhook_delivery WHERE delivery_id='crash-delivery'",
    )
    .get();
  if (row === null) throw new Error("Missing crash delivery");
  return row;
};
const auditCounts = (db: import("bun:sqlite").Database) =>
  db
    .query<{ operation: string; count: number }, []>(
      "SELECT operation, COUNT(*) AS count FROM replay_audit GROUP BY operation ORDER BY operation",
    )
    .all();
const setupReplayAudit = (db: import("bun:sqlite").Database) =>
  db.exec(`
  CREATE TABLE replay_audit (operation TEXT NOT NULL);
  CREATE TRIGGER replay_installation AFTER UPDATE ON installation BEGIN
    INSERT INTO replay_audit VALUES ('installation');
  END;
  CREATE TRIGGER replay_input AFTER INSERT ON run_input BEGIN
    INSERT INTO replay_audit VALUES ('input');
  END;
`);
const crashWebhookProcess = (
  databasePath: string,
  checkpoint: string,
  payload: Record<string, unknown>,
) =>
  Effect.promise(async () => {
    const child = Bun.spawn(
      [
        process.execPath,
        "run",
        new URL("./fixtures/webhook-crash.ts", import.meta.url).pathname,
        databasePath,
        checkpoint,
        JSON.stringify(payload),
      ],
      { stdout: "pipe", stderr: "pipe" },
    );
    const [code, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    expect({ code, stdout, stderr }).toEqual({
      code: 86,
      stdout: "",
      stderr: "",
    });
  });

// Closing and reopening file-backed layers plus abrupt child process exits
// exercise WAL recovery, not request-fiber finalizers or in-memory mock state.
describe("Durable authenticated webhook replay", () => {
  it.live.each(["claim", "run", "enqueue", "processed"] as const)(
    "replays a process crash after %s exactly once before reconciliation",
    (checkpoint) =>
      Effect.gen(function* () {
        const databasePath = suitePath(`replay-${checkpoint}.db`);
        const fileConfig = { ...config, databasePath };
        yield* withWebhook(
          Effect.gen(function* () {
            yield* install();
            const { db } = yield* SqliteClient;
            setupReplayAudit(db);
          }),
          fileConfig,
        );
        const payload = createdPayload(yield* Clock.currentTimeMillis);
        yield* crashWebhookProcess(databasePath, checkpoint, payload);
        yield* withWebhook(
          Effect.gen(function* () {
            const { db } = yield* SqliteClient;
            expect(replayStatus(db).status).toBe("pending");
            const pipeline = yield* WebhookPipeline;
            const ingress = vi.spyOn(pipeline, "handle");
            yield* recoverGateway;
            yield* pipeline.recoverPendingDeliveries();
            expect(ingress).not.toHaveBeenCalled();
            ingress.mockRestore();
            const inputs = yield* RunInputRepo.use((repo) =>
              repo.pending(sessionId("session-1")),
            );
            expect(inputs).toHaveLength(1);
            expect(inputs[0]?.id).toBe("session-1:created");
            expect(inputs[0]?.body).toContain("Implement the issue");
            expect(replayStatus(db)).toEqual({
              status: "processed",
              error: null,
            });
            expect(auditCounts(db)).toEqual([{ operation: "input", count: 1 }]);
            // The stored timestamp is now older than ingress permits. Replay must
            // not synthesize a request or reapply the ingress freshness window.
          }),
          { ...fileConfig, webhookReplayWindowMs: 1 },
        );
      }),
    { timeout: 15_000 },
  );

  it.live.each(["enqueue", "processed"] as const)(
    "replays notification crash at %s without duplicated or cross-tenant stops",
    (checkpoint) =>
      Effect.gen(function* () {
        const databasePath = suitePath(`notification-${checkpoint}.db`);
        const fileConfig = { ...config, databasePath };
        yield* withWebhook(
          Effect.gen(function* () {
            yield* install();
            const runs = yield* RunRepo;
            for (const [sid, org] of [
              ["session-1", "org"],
              ["session-2", "org"],
              ["foreign-session", "other-org"],
            ]) {
              if (sid === undefined || org === undefined)
                throw new Error("Missing run fixture");
              yield* runs.create({
                sessionId: sessionId(sid),
                organizationId: organizationId(org),
                issueId: Option.some(issueId("issue-1")),
              });
            }
            setupReplayAudit((yield* SqliteClient).db);
          }),
          fileConfig,
        );
        yield* crashWebhookProcess(
          databasePath,
          checkpoint,
          appUserNotificationPayload(
            yield* Clock.currentTimeMillis,
            "issueUnassignedFromYou",
          ),
        );
        yield* withWebhook(
          Effect.gen(function* () {
            const pipeline = yield* WebhookPipeline;
            yield* pipeline.recoverPendingDeliveries();
            yield* pipeline.recoverPendingDeliveries();
            const inputs = yield* RunInputRepo;
            expect(yield* inputs.pending(sessionId("session-1"))).toHaveLength(
              1,
            );
            expect(yield* inputs.pending(sessionId("session-2"))).toHaveLength(
              1,
            );
            expect(
              yield* inputs.pending(sessionId("foreign-session")),
            ).toHaveLength(0);
            const { db } = yield* SqliteClient;
            expect(replayStatus(db).status).toBe("processed");
            expect(auditCounts(db)).toEqual([{ operation: "input", count: 2 }]);
          }),
          fileConfig,
        );
      }),
    { timeout: 15_000 },
  );

  it.live.each(["installation", "processed"] as const)(
    "replays permission crash at %s with one committed installation update",
    (checkpoint) =>
      Effect.gen(function* () {
        const databasePath = suitePath(`permission-${checkpoint}.db`);
        const fileConfig = { ...config, databasePath };
        yield* withWebhook(
          Effect.gen(function* () {
            yield* install();
            setupReplayAudit((yield* SqliteClient).db);
          }),
          fileConfig,
        );
        yield* crashWebhookProcess(databasePath, checkpoint, {
          type: "PermissionChange",
          action: "teamAccessChanged",
          organizationId: "org",
          appUserId: "app-user",
          oauthClientId: "client",
          webhookId: "permissions",
          webhookTimestamp: yield* Clock.currentTimeMillis,
          addedTeamIds: ["team-b"],
          removedTeamIds: ["team-a"],
          canAccessAllPublicTeams: false,
        });
        yield* withWebhook(
          Effect.gen(function* () {
            const pipeline = yield* WebhookPipeline;
            yield* pipeline.recoverPendingDeliveries();
            yield* pipeline.recoverPendingDeliveries();
            const installation = Option.getOrThrow(
              yield* InstallationRepo.use((repo) =>
                repo.get(organizationId("org")),
              ),
            );
            expect(installation.accessibleTeamIds).toEqual(
              Option.some([teamId("team-b")]),
            );
            const { db } = yield* SqliteClient;
            expect(replayStatus(db).status).toBe("processed");
            expect(auditCounts(db)).toEqual([
              { operation: "installation", count: 1 },
            ]);
          }),
          fileConfig,
        );
      }),
    { timeout: 15_000 },
  );
});

const storeReplay = (
  payload: Record<string, unknown>,
  token = "original-owner",
) =>
  DeliveryRepo.use((repo) =>
    repo.claim({
      id: crashDeliveryId,
      organizationId: "org",
      payload,
      payloadHash: payloadHash(
        new TextEncoder().encode(JSON.stringify(payload)),
      ),
      payloadVersion: 1,
      claimToken: token,
    }),
  );

describe("Webhook replay integrity and fencing", () => {
  it.live.each(["organization", "hash", "version", "json"] as const)(
    "rejects stored %s tampering and still replays the next delivery",
    (field) =>
      withWebhook(
        Effect.gen(function* () {
          yield* install();
          const payload = createdPayload(yield* Clock.currentTimeMillis);
          yield* storeReplay(payload);
          const deliveries = yield* DeliveryRepo;
          const next = createdPayload(yield* Clock.currentTimeMillis, {
            agentSession: agentSessionBase({ id: "next-session" }),
          });
          yield* deliveries.claim({
            id: Schema.decodeUnknownSync(DeliveryId)("next-delivery"),
            organizationId: "org",
            payload: next,
            payloadHash: payloadHash(
              new TextEncoder().encode(JSON.stringify(next)),
            ),
            payloadVersion: 1,
          });
          const { db } = yield* SqliteClient;
          switch (field) {
            case "organization": {
              const altered = JSON.stringify({
                ...payload,
                organizationId: "other-org",
              });
              // A valid checksum alone cannot authorize a tenant mismatch.
              const hash = payloadHash(
                new TextEncoder().encode(JSON.stringify(payload)),
              );
              db.query(
                "UPDATE webhook_delivery SET payload_json=?, replay_hash=? WHERE delivery_id=?",
              ).run(
                altered,
                deliveryReplayHash("org", hash, altered, 1),
                crashDeliveryId,
              );
              break;
            }
            case "hash":
              db.query(
                "UPDATE webhook_delivery SET payload_hash='tampered' WHERE delivery_id=?",
              ).run(crashDeliveryId);
              break;
            case "version":
              db.query(
                "UPDATE webhook_delivery SET payload_version=99 WHERE delivery_id=?",
              ).run(crashDeliveryId);
              break;
            case "json":
              db.query(
                "UPDATE webhook_delivery SET payload_json='not JSON', replay_hash=? WHERE delivery_id=?",
              ).run(
                deliveryReplayHash(
                  "org",
                  payloadHash(
                    new TextEncoder().encode(JSON.stringify(payload)),
                  ),
                  "not JSON",
                  1,
                ),
                crashDeliveryId,
              );
              break;
          }
          const messages: string[] = [];
          const logger = Logger.make((options) =>
            messages.push(
              JSON.stringify({
                message: options.message,
                cause:
                  options.cause === undefined
                    ? undefined
                    : Cause.pretty(options.cause),
                annotations: options.fiber.getRef(
                  References.CurrentLogAnnotations,
                ),
              }),
            ),
          );
          yield* WebhookPipeline.use((pipeline) =>
            pipeline.recoverPendingDeliveries(),
          ).pipe(Effect.provide(Logger.layer([logger])));
          expect(replayStatus(db)).toEqual({
            status: "failed",
            error: "Stored webhook replay failed",
          });
          expect(
            yield* RunInputRepo.use((repo) =>
              repo.pending(sessionId("session-1")),
            ),
          ).toHaveLength(0);
          expect(
            yield* RunInputRepo.use((repo) =>
              repo.pending(sessionId("next-session")),
            ),
          ).toHaveLength(1);
          expect(messages.join("\n")).toContain("webhook.replay_failed");
          expect(messages.join("\n")).not.toContain("Implement the issue");
          expect(messages.join("\n")).not.toContain(secret);
          expect(messages.join("\n")).not.toContain("client-secret");
          expect(messages.join("\n")).not.toContain("original-owner");
        }),
      ),
  );

  it.live.each([
    "unadmitted",
    "revoked",
    "app-user",
    "client",
    "nested-org",
    "existing-run",
  ] as const)("rechecks %s identity on replay", (failure) =>
    withWebhook(
      Effect.gen(function* () {
        yield* install({
          revokedAt: failure === "revoked" ? Option.some(1) : Option.none(),
        });
        const payload = createdPayload(yield* Clock.currentTimeMillis, {
          ...(failure === "app-user"
            ? {
                appUserId: "wrong-user",
                agentSession: agentSessionBase({ appUserId: "wrong-user" }),
              }
            : {}),
          ...(failure === "client" ? { oauthClientId: "wrong-client" } : {}),
          ...(failure === "nested-org"
            ? {
                agentSession: agentSessionBase({ organizationId: "other-org" }),
              }
            : {}),
        });
        if (failure === "existing-run") {
          yield* RunRepo.use((repo) =>
            repo.create({
              sessionId: sessionId("session-1"),
              organizationId: organizationId("other-org"),
              issueId: Option.none(),
            }),
          );
        }
        yield* storeReplay(payload);
        yield* recoverGateway;
        expect(replayStatus((yield* SqliteClient).db).status).toBe("failed");
        expect(
          yield* RunInputRepo.use((repo) =>
            repo.pending(sessionId("session-1")),
          ),
        ).toHaveLength(0);
      }),
      failure === "unadmitted"
        ? { ...config, allowedOrganizationIds: new Set(["other-org"]) }
        : config,
    ),
  );

  it.live(
    "fences both stale writes and stale failure cleanup after replay takes ownership",
    () =>
      withWebhook(
        Effect.gen(function* () {
          yield* install();
          yield* storeReplay(createdPayload(yield* Clock.currentTimeMillis));
          const deliveries = yield* DeliveryRepo;
          const takeover = yield* deliveries.claimReplay(
            crashDeliveryId,
            "recovery-owner",
          );
          expect(Option.isSome(takeover)).toBe(true);
          let staleEffectRan = false;
          expect(
            yield* deliveries.processClaim(
              crashDeliveryId,
              "original-owner",
              Effect.sync(() => {
                staleEffectRan = true;
              }),
            ),
          ).toBe(false);
          yield* deliveries.mark(
            crashDeliveryId,
            "failed",
            "stale cleanup",
            "original-owner",
          );
          expect(staleEffectRan).toBe(false);
          expect(replayStatus((yield* SqliteClient).db).status).toBe("pending");
          yield* recoverGateway;
          yield* deliveries.mark(
            crashDeliveryId,
            "failed",
            "stale cleanup",
            "recovery-owner",
          );
          expect(replayStatus((yield* SqliteClient).db)).toEqual({
            status: "processed",
            error: null,
          });
          expect(
            yield* RunInputRepo.use((repo) =>
              repo.pending(sessionId("session-1")),
            ),
          ).toHaveLength(1);
        }),
      ),
  );

  it.live.each(["run", "enqueued", "consumed"] as const)(
    "repairs a preexisting partial %s write without omitting or reactivating the input",
    (partial) =>
      withWebhook(
        Effect.gen(function* () {
          yield* install();
          const timestamp = yield* Clock.currentTimeMillis;
          yield* storeReplay(createdPayload(timestamp));
          yield* RunRepo.use((repo) =>
            repo.create({
              sessionId: sessionId("session-1"),
              organizationId: organizationId("org"),
              issueId: Option.none(),
            }),
          );
          const { db } = yield* SqliteClient;
          if (partial !== "run") {
            db.query(
              "INSERT INTO run_input (id, session_id, kind, body, payload_json, created_at, processed_at) VALUES (?, ?, 'created', 'original input', '{}', ?, ?)",
            ).run(
              "session-1:created",
              "session-1",
              timestamp,
              partial === "consumed" ? timestamp : null,
            );
          }
          yield* recoverGateway;
          yield* recoverGateway;
          expect(replayStatus(db).status).toBe("processed");
          expect(
            db
              .query<{ count: number }, []>(
                "SELECT COUNT(*) AS count FROM run_input WHERE session_id='session-1'",
              )
              .get()?.count,
          ).toBe(1);
          const inputs = yield* RunInputRepo.use((repo) =>
            repo.pending(sessionId("session-1")),
          );
          expect(inputs).toHaveLength(partial === "consumed" ? 0 : 1);
          if (partial === "enqueued")
            expect(inputs[0]?.body).toBe("original input");
        }),
      ),
  );
});

it.live.each([false, true])(
  "uses bounded replay backoff and signed retry recovery, exhausted=%s",
  (exhausted) =>
    withWebhook(
      Effect.gen(function* () {
        yield* install();
        const payload = createdPayload(yield* Clock.currentTimeMillis);
        yield* storeReplay(payload);
        const deliveries = yield* DeliveryRepo;
        const processClaim = deliveries.processClaim;
        const times: number[] = [];
        const spy = vi
          .spyOn(deliveries, "processClaim")
          .mockImplementation((id, token, effect) =>
            Effect.gen(function* () {
              times.push(yield* Clock.currentTimeMillis);
              if (exhausted || times.length <= 2)
                return yield* Effect.fail(
                  new DatabaseError({ message: "sensitive storage details" }),
                );
              return yield* processClaim(id, token, effect);
            }),
          );
        yield* WebhookPipeline.use((pipeline) =>
          pipeline.recoverPendingDeliveries(),
        );
        spy.mockRestore();
        expect(times).toHaveLength(3);
        const [first, second, third] = times;
        if (first === undefined || second === undefined || third === undefined)
          throw new Error("Missing retry attempts");
        expect(second - first).toBeGreaterThanOrEqual(25);
        expect(third - second).toBeGreaterThanOrEqual(50);
        const { db } = yield* SqliteClient;
        expect(replayStatus(db).status).toBe(
          exhausted ? "failed" : "processed",
        );
        if (exhausted) {
          const response = yield* WebhookPipeline.use((pipeline) =>
            pipeline.handle(
              signedRequest(payload, { delivery: "crash-delivery" }),
            ),
          );
          expect(response.status).toBe(200);
          expect(yield* Effect.promise(() => response.text())).toBe("OK");
        }
        expect(replayStatus(db)).toEqual({ status: "processed", error: null });
        expect(
          yield* RunInputRepo.use((repo) =>
            repo.pending(sessionId("session-1")),
          ),
        ).toHaveLength(1);
      }),
    ),
);

it.live("concurrent startup recoveries commit only one actionable input", () =>
  withWebhook(
    Effect.gen(function* () {
      yield* install();
      yield* storeReplay(createdPayload(yield* Clock.currentTimeMillis));
      const pipeline = yield* WebhookPipeline;
      yield* Effect.all(
        [
          pipeline.recoverPendingDeliveries(),
          pipeline.recoverPendingDeliveries(),
        ],
        { concurrency: 2 },
      );
      expect(replayStatus((yield* SqliteClient).db).status).toBe("processed");
      expect(
        yield* RunInputRepo.use((repo) => repo.pending(sessionId("session-1"))),
      ).toHaveLength(1);
    }),
  ),
);

it.live(
  "replays an OAuth revocation crash atomically with cancellation and admin-session invalidation",
  () =>
    Effect.gen(function* () {
      const databasePath = suitePath("oauth-replay.db");
      const fileConfig = { ...config, databasePath };
      yield* withWebhook(
        Effect.gen(function* () {
          yield* install();
          yield* RunRepo.use((repo) =>
            repo.create({
              sessionId: sessionId("session-1"),
              organizationId: organizationId("org"),
              issueId: Option.none(),
            }),
          );
          const { db } = yield* SqliteClient;
          db.exec(
            "INSERT INTO admin_session VALUES ('admin-token-hash', 'org', 'csrf-hash', 9999999999999, 1)",
          );
          setupReplayAudit(db);
        }),
        fileConfig,
      );
      yield* crashWebhookProcess(
        databasePath,
        "processed",
        oauthAppPayload(yield* Clock.currentTimeMillis),
      );
      yield* withWebhook(
        Effect.gen(function* () {
          const { db } = yield* SqliteClient;
          expect(
            db
              .query<{ count: number }, []>(
                "SELECT COUNT(*) AS count FROM admin_session",
              )
              .get()?.count,
          ).toBe(1);
          yield* recoverGateway;
          yield* recoverGateway;
          expect(replayStatus(db).status).toBe("processed");
          expect(
            db
              .query<{ count: number }, []>(
                "SELECT COUNT(*) AS count FROM admin_session",
              )
              .get()?.count,
          ).toBe(0);
          const run = Option.getOrThrow(
            yield* RunRepo.use((repo) => repo.get(sessionId("session-1"))),
          );
          expect(run.desiredState).toBe("canceled");
          const installation = Option.getOrThrow(
            yield* InstallationRepo.use((repo) =>
              repo.get(organizationId("org")),
            ),
          );
          expect(Option.isSome(installation.revokedAt)).toBe(true);
          expect(auditCounts(db)).toEqual([
            { operation: "installation", count: 1 },
          ]);
        }),
        fileConfig,
      );
    }),
  { timeout: 15_000 },
);

it.live(
  "replays normalized header-only timestamps while preserving raw-byte dedupe",
  () =>
    Effect.gen(function* () {
      const databasePath = suitePath("header-timestamp-replay.db");
      const fileConfig = { ...config, databasePath };
      yield* withWebhook(install(), fileConfig);
      const payload = createdPayload(yield* Clock.currentTimeMillis);
      delete payload.webhookTimestamp;
      yield* crashWebhookProcess(databasePath, "claim", payload);
      yield* withWebhook(
        Effect.gen(function* () {
          const { db } = yield* SqliteClient;
          const stored = db
            .query<{ payload_hash: string; payload_json: string }, []>(
              "SELECT payload_hash, payload_json FROM webhook_delivery",
            )
            .get();
          if (stored === null) throw new Error("Missing stored envelope");
          expect(stored.payload_hash).toBe(
            payloadHash(new TextEncoder().encode(JSON.stringify(payload))),
          );
          expect(stored.payload_json).toContain("webhookTimestamp");
          yield* recoverGateway;
          expect(replayStatus(db).status).toBe("processed");
          expect(
            yield* RunInputRepo.use((repo) =>
              repo.pending(sessionId("session-1")),
            ),
          ).toHaveLength(1);
          const request = signedRequest(payload, {
            delivery: "crash-delivery",
          });
          request.headers.set(
            "linear-timestamp",
            String(yield* Clock.currentTimeMillis),
          );
          const retry = yield* WebhookPipeline.use((pipeline) =>
            pipeline.handle(request),
          );
          expect(retry.status).toBe(200);
          expect(yield* Effect.promise(() => retry.text())).toBe(
            "Duplicate delivery",
          );
        }),
        fileConfig,
      );
    }),
  { timeout: 15_000 },
);

it.live("completion cannot be demoted even by its former claim owner", () =>
  withWebhook(
    Effect.gen(function* () {
      yield* install();
      yield* storeReplay(createdPayload(yield* Clock.currentTimeMillis));
      const deliveries = yield* DeliveryRepo;
      expect(
        yield* deliveries.processClaim(
          crashDeliveryId,
          "original-owner",
          Effect.void,
        ),
      ).toBe(true);
      yield* deliveries.mark(
        crashDeliveryId,
        "failed",
        "late cleanup",
        "original-owner",
      );
      expect(replayStatus((yield* SqliteClient).db)).toEqual({
        status: "processed",
        error: null,
      });
    }),
  ),
);

it.live(
  "returns a retryable response when recovery supersedes fresh ingress",
  () =>
    withWebhook(
      Effect.gen(function* () {
        yield* install();
        const deliveries = yield* DeliveryRepo;
        const processClaim = vi
          .spyOn(deliveries, "processClaim")
          .mockImplementation((id) =>
            deliveries.claimReplay(id, "takeover-owner").pipe(Effect.as(false)),
          );
        const payload = createdPayload(yield* Clock.currentTimeMillis);
        const response = yield* WebhookPipeline.use((pipeline) =>
          pipeline.handle(
            signedRequest(payload, { delivery: "crash-delivery" }),
          ),
        );
        processClaim.mockRestore();
        expect(response.status).toBe(503);
        expect(yield* Effect.promise(() => response.text())).toBe(
          "Delivery claim superseded",
        );
        expect(replayStatus((yield* SqliteClient).db).status).toBe("pending");
        yield* recoverGateway;
        expect(replayStatus((yield* SqliteClient).db).status).toBe("processed");
        expect(
          yield* RunInputRepo.use((repo) =>
            repo.pending(sessionId("session-1")),
          ),
        ).toHaveLength(1);
      }),
    ),
);
