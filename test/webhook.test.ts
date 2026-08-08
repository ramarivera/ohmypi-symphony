import { createHmac } from "node:crypto";
import { Writable } from "node:stream";
import { it } from "@effect/vitest";
import {
  Clock,
  ConfigProvider,
  Effect,
  Layer,
  Logger,
  Option,
  Redacted,
  Schema,
} from "effect";
import pino from "pino";
import { describe, expect } from "vitest";
import {
  AppUserId,
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
import {
  DeliveryRepo,
  InstallationRepo,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
} from "../src/services/store/repositories.js";
import { SqliteClientLive } from "../src/services/store/sqlite-client.js";
import { TokenCrypto } from "../src/services/token-crypto.js";
import { verifySignature, WebhookPipeline } from "../src/services/webhook.js";

const secret = "webhook-secret";
const tokenEncryptionKey = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=";
const config: GatewayConfigShape = {
  linearClientId: "client",
  linearClientSecret: Redacted.make("client-secret"),
  linearWebhookSecret: Redacted.make(secret),
  tokenEncryptionKey: Redacted.make(tokenEncryptionKey),
  publicUrl: new URL("https://gateway.example.com"),
  databasePath: ":memory:",
  nixBinaryPath: "nix",
  nixpkgsFlakeRef:
    "github:NixOS/nixpkgs/0123456789abcdef0123456789abcdef01234567",
  nixRootsDir: "/tmp/nix-roots",
  nixGcMaxBytes: 1_000_000,
  workspaceRoot: "/workspaces",
  ompCliPath: "omp",
  port: 3000,
  leaseDurationMs: 60_000,
  reconcilerIntervalMs: 1_000,
  webhookReplayWindowMs: 60_000,
  logLevel: "silent",
  logFile: Option.none(),
};
const configProviderFor = (gatewayConfig: GatewayConfigShape) =>
  ConfigProvider.fromMap(
    new Map([
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
      ["WEBHOOK_REPLAY_WINDOW_MS", String(gatewayConfig.webhookReplayWindowMs)],
      ["LOG_LEVEL", gatewayConfig.logLevel],
      ["NIX_BINARY_PATH", gatewayConfig.nixBinaryPath],
      ["NIXPKGS_FLAKE_REF", gatewayConfig.nixpkgsFlakeRef],
      ["NIX_ROOTS_DIR", gatewayConfig.nixRootsDir],
      ["NIX_GC_MAX_BYTES", String(gatewayConfig.nixGcMaxBytes)],
    ]),
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
    GatewayConfig.Default,
    TokenCrypto.Default,
    InstallationRepo.Default,
    DeliveryRepo.Default,
    RunEventRepo.Default,
    RunRepo.Default,
    RunInputRepo.Default,
    WebhookPipeline.Default,
  ).pipe(
    Layer.provide(
      Layer.mergeAll(
        SqliteClientLive(":memory:"),
        Layer.setConfigProvider(configProviderFor(gatewayConfig)),
      ),
    ),
  );

const withWebhook = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    WebhookPipeline | InstallationRepo | RunRepo | RunInputRepo | DeliveryRepo
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
  it.scopedLive(
    "emits correlated success and controlled-rejection logs through the gateway graph",
    () => {
      const chunks: string[] = [];
      const stream = new Writable({
        write(chunk, _encoding, callback) {
          chunks.push(chunk.toString());
          callback();
        },
      });
      const pinoLoggerLive = Logger.replace(
        Logger.defaultLogger,
        makePinoEffectLogger(pino({ base: null, level: "info" }, stream)),
      );

      return withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          const accepted = yield* WebhookPipeline.handle(
            signedRequest(createdPayload(now)),
          );
          const rejected = yield* WebhookPipeline.handle(
            signedRequest(createdPayload(now), { method: "GET" }),
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
  it.scopedLive("created event is durably stored and acknowledged", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const response = yield* WebhookPipeline.handle(
          signedRequest(createdPayload(now)),
        );

        expect(response.status).toBe(200);
        expect(yield* Effect.promise(() => response.text())).toBe("OK");
        const run = expectSome(yield* RunRepo.get(sessionId("session-1")));
        expect(run).toMatchObject({
          organizationId: organizationId("org"),
          issueId: Option.some(issueId("issue-1")),
          teamId: Option.some(teamId("team-a")),
          projectId: Option.some(projectId("project-1")),
          desiredState: "running",
        });
        const inputs = yield* RunInputRepo.pending(sessionId("session-1"));
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

  it.scopedLive(
    "created input uses structured context and guidance, not tracker text as policy",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          const response = yield* WebhookPipeline.handle(
            signedRequest(
              createdPayload(now, {
                promptContext: "Please resolve this",
                guidance: [{ body: "Use idiomatic TypeScript" }],
              }),
            ),
          );

          expect(response.status).toBe(200);
          const [input] = yield* RunInputRepo.pending(sessionId("session-1"));
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

  it.scopedLive(
    "created handles nullable fields and falls back to summary",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          const response = yield* WebhookPipeline.handle(
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
          );

          expect(response.status).toBe(200);
          const run = expectSome(yield* RunRepo.get(sessionId("session-1")));
          expect(run).toMatchObject({
            issueId: Option.none(),
            teamId: Option.none(),
            projectId: Option.none(),
          });
          const [input] = yield* RunInputRepo.pending(sessionId("session-1"));
          expect(input).toMatchObject({
            kind: "created",
            body: "User request:\nSummary of direct chat",
          });
        }),
      ),
  );

  it.scopedLive(
    "accepts real Linear AgentSessionEvent created payload with null guidance and no agentSession.type",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          const response = yield* WebhookPipeline.handle(
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
          );

          expect(response.status).toBe(200);
          const run = expectSome(yield* RunRepo.get(sessionId("session-1")));
          expect(run).toMatchObject({
            organizationId: organizationId("org"),
            issueId: Option.some(issueId("issue-1")),
            teamId: Option.some(teamId("team-a")),
          });
          const [input] = yield* RunInputRepo.pending(sessionId("session-1"));
          expect(input?.kind).toBe("created");
          expect(input?.body).not.toContain("Guidance:");
        }),
      ),
  );

  it.scopedLive("prompted event appends agent activity body", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        expect(
          (yield* WebhookPipeline.handle(signedRequest(createdPayload(now))))
            .status,
        ).toBe(200);
        const response = yield* WebhookPipeline.handle(
          signedRequest(promptedPayload(now)),
        );

        expect(response.status).toBe(200);
        const inputs = yield* RunInputRepo.pending(sessionId("session-1"));
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

  it.scopedLive("prompted with stop signal cancels the run", () =>
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
        yield* WebhookPipeline.handle(signedRequest(createdPayload(now)));
        const response = yield* WebhookPipeline.handle(signedRequest(payload));

        expect(response.status).toBe(200);
        const inputs = yield* RunInputRepo.pending(sessionId("session-1"));
        expect(inputs.find((input) => input.kind === "stop")).toBeDefined();
        expect(
          expectSome(yield* RunRepo.get(sessionId("session-1")))?.desiredState,
        ).toBe("canceled");
      }),
    ),
  );

  it.scopedLive("stop action cancels the run", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        yield* WebhookPipeline.handle(signedRequest(createdPayload(now)));
        const response = yield* WebhookPipeline.handle(
          signedRequest(stopPayload(now)),
        );

        expect(response.status).toBe(200);
        expect(
          expectSome(yield* RunRepo.get(sessionId("session-1")))?.desiredState,
        ).toBe("canceled");
        const inputs = yield* RunInputRepo.pending(sessionId("session-1"));
        expect(inputs).toHaveLength(2);
        expect(inputs[1]).toMatchObject({ kind: "stop", body: "Stop" });
      }),
    ),
  );

  it.scopedLive("prompted events after stop are accepted for resume", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        yield* WebhookPipeline.handle(signedRequest(createdPayload(now)));
        yield* WebhookPipeline.handle(signedRequest(stopPayload(now)));
        const response = yield* WebhookPipeline.handle(
          signedRequest(promptedPayload(now)),
        );

        // The prompt is enqueued alongside the stop; the session authority
        // decides between honoring the cancellation and resuming the run.
        expect(response.status).toBe(200);
        expect(
          yield* RunInputRepo.pending(sessionId("session-1")),
        ).toHaveLength(3);
        expect(
          expectSome(yield* RunRepo.get(sessionId("session-1")))?.desiredState,
        ).toBe("canceled");
      }),
    ),
  );

  it.scopedLive("dedupes by Linear-Delivery id", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const payload = createdPayload(now);
        const response1 = yield* WebhookPipeline.handle(
          signedRequest(payload, { delivery: "d1" }),
        );
        const response2 = yield* WebhookPipeline.handle(
          signedRequest(payload, { delivery: "d1" }),
        );

        expect(response1.status).toBe(200);
        expect(response2.status).toBe(200);
        expect(yield* Effect.promise(() => response2.text())).toBe(
          "Duplicate delivery",
        );
        expect(
          yield* RunInputRepo.pending(sessionId("session-1")),
        ).toHaveLength(1);
      }),
    ),
  );

  it.scopedLive(
    "dedupes logical input on replay with a different Linear-Delivery id",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          const payload = createdPayload(now);
          const body = JSON.stringify(payload);
          const signature = sign(body);
          const response1 = yield* WebhookPipeline.handle(
            signedRequest(payload, { delivery: "d1", body, signature }),
          );
          const response2 = yield* WebhookPipeline.handle(
            signedRequest(payload, { delivery: "d2", body, signature }),
          );

          expect(response1.status).toBe(200);
          expect(response2.status).toBe(200);
          expect(
            yield* RunInputRepo.pending(sessionId("session-1")),
          ).toHaveLength(1);
        }),
      ),
  );

  it.scopedLive(
    "rejects stale and future timestamps with exact replay response",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          for (const timestamp of [now - 120_000, now + 120_000]) {
            const response = yield* WebhookPipeline.handle(
              signedRequest(createdPayload(timestamp), {
                delivery: `outside-window-${timestamp}`,
                includeTimestampHeader: true,
              }),
            );
            expect(response.status).toBe(401);
            expect(yield* Effect.promise(() => response.text())).toBe(
              "Webhook timestamp outside replay window",
            );
          }
          expect(yield* RunRepo.get(sessionId("session-1"))).toEqual(
            Option.none(),
          );
        }),
      ),
  );

  it.scopedLive("rejects invalid and tampered HMACs without persistence", () =>
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
          const response = yield* WebhookPipeline.handle(request);
          expect(response.status).toBe(401);
          expect(yield* Effect.promise(() => response.text())).toBe(
            "Invalid signature",
          );
        }
        expect(yield* RunRepo.get(sessionId("session-1"))).toEqual(
          Option.none(),
        );
      }),
    ),
  );

  it.scopedLive("rejects missing signature and non-POST methods", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        const missing = signedRequest(createdPayload(now));
        missing.headers.delete("linear-signature");
        const missingResponse = yield* WebhookPipeline.handle(missing);
        expect(missingResponse.status).toBe(400);
        expect(yield* Effect.promise(() => missingResponse.text())).toBe(
          "Missing signature",
        );

        const methodResponse = yield* WebhookPipeline.handle(
          signedRequest(createdPayload(now), { method: "GET" }),
        );
        expect(methodResponse.status).toBe(405);
        expect(yield* Effect.promise(() => methodResponse.text())).toBe(
          "Method not allowed",
        );
      }),
    ),
  );

  it.scopedLive("rejects malformed AgentSessionEvent payloads", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const response = yield* WebhookPipeline.handle(
          signedRequest(createdPayload(now, { agentSession: undefined })),
        );
        expect(response.status).toBe(400);
        expect(yield* Effect.promise(() => response.text())).toContain(
          "AgentSessionEvent payload is invalid",
        );
      }),
    ),
  );

  it.scopedLive("rejects cross-tenant OAuth client identity", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const response = yield* WebhookPipeline.handle(
          signedRequest(createdPayload(now, { oauthClientId: "other-client" })),
        );
        expect(response.status).toBe(401);
        expect(yield* Effect.promise(() => response.text())).toBe(
          "OAuth client identity mismatch",
        );
      }),
    ),
  );

  it.scopedLive("rejects absent and revoked installations", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        const absent = yield* WebhookPipeline.handle(
          signedRequest(createdPayload(now)),
        );
        expect(absent.status).toBe(401);
        expect(yield* Effect.promise(() => absent.text())).toBe(
          "No installation for organization",
        );

        yield* install({ revokedAt: Option.some(now - 1_000) });
        const revoked = yield* WebhookPipeline.handle(
          signedRequest(createdPayload(now, { webhookId: "revoked-webhook" })),
        );
        expect(revoked.status).toBe(401);
        expect(yield* Effect.promise(() => revoked.text())).toBe(
          "Installation is revoked",
        );
      }),
    ),
  );

  it.scopedLive(
    "OAuthApp revoked cancels runs and marks installation revoked",
    () =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          yield* RunRepo.create({
            sessionId: sessionId("session-1"),
            organizationId: organizationId("org"),
            issueId: Option.some(issueId("issue-1")),
            teamId: Option.some(teamId("team-a")),
            now,
          });

          const response = yield* WebhookPipeline.handle(
            signedRequest(oauthAppPayload(now)),
          );
          expect(response.status).toBe(200);
          expect(yield* Effect.promise(() => response.text())).toBe("OK");
          const installation = expectSome(
            yield* InstallationRepo.get(organizationId("org")),
          );
          expect(installation?.revokedAt).toEqual(Option.some(now));
          expect(
            expectSome(yield* RunRepo.get(sessionId("session-1")))
              ?.desiredState,
          ).toBe("canceled");
        }),
      ),
  );

  it.scopedLive("PermissionChange updates team access and cancels runs", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install({
          accessibleTeamIds: Option.some([teamId("team-a"), teamId("team-b")]),
        });
        yield* RunRepo.create({
          sessionId: sessionId("session-1"),
          organizationId: organizationId("org"),
          issueId: Option.some(issueId("issue-1")),
          teamId: Option.some(teamId("team-b")),
          now,
        });

        const response = yield* WebhookPipeline.handle(
          signedRequest(permissionChangePayload(now)),
        );
        expect(response.status).toBe(200);
        const installation = expectSome(
          yield* InstallationRepo.get(organizationId("org")),
        );
        expect(installation?.accessibleTeamIds).toEqual(
          Option.some([teamId("team-a")]),
        );
        expect(
          expectSome(yield* RunRepo.get(sessionId("session-1")))?.desiredState,
        ).toBe("canceled");
      }),
    ),
  );

  it.scopedLive("PermissionChange rejects mismatched app-user identity", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const response = yield* WebhookPipeline.handle(
          signedRequest(
            permissionChangePayload(now, { appUserId: "other-user" }),
          ),
        );
        expect(response.status).toBe(401);
        expect(yield* Effect.promise(() => response.text())).toBe(
          "App user mismatch",
        );
        const installation = expectSome(
          yield* InstallationRepo.get(organizationId("org")),
        );
        expect(installation?.accessibleTeamIds).toEqual(
          Option.some([teamId("team-a")]),
        );
      }),
    ),
  );

  it.scopedLive("acknowledges unknown event types without side effects", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const response = yield* WebhookPipeline.handle(
          signedRequest(createdPayload(now, { type: "Unknown" })),
        );
        expect(response.status).toBe(200);
        expect(yield* RunRepo.get(sessionId("session-1"))).toEqual(
          Option.none(),
        );
      }),
    ),
  );

  it.scopedLive("acknowledges unknown AgentSession actions without input", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const response = yield* WebhookPipeline.handle(
          signedRequest(createdPayload(now, { action: "updated" })),
        );
        expect(response.status).toBe(200);
        expect(
          yield* RunInputRepo.pending(sessionId("session-1")),
        ).toHaveLength(0);
      }),
    ),
  );

  it.scopedLive("resolves team and project from child objects", () =>
    withWebhook(
      Effect.gen(function* () {
        const now = yield* currentTime;
        yield* install();
        const response = yield* WebhookPipeline.handle(
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
        );
        expect(response.status).toBe(200);
        expect(
          expectSome(yield* RunRepo.get(sessionId("session-1"))),
        ).toMatchObject({
          teamId: Option.some(teamId("team-child")),
          projectId: Option.some(projectId("project-child")),
        });
      }),
    ),
  );
});

describe("webhook signature invariants", () => {
  it.prop(
    "HMAC-SHA256 verification accepts matching bytes and rejects tampered bytes or secrets",
    {
      body: Schema.String.pipe(Schema.minLength(1)),
      otherBody: Schema.String.pipe(Schema.minLength(1)),
      secret1: Schema.String.pipe(Schema.minLength(1)),
      secret2: Schema.String.pipe(Schema.minLength(1)),
    },
    ({ body, otherBody, secret1, secret2 }) => {
      const bytes = new TextEncoder().encode(body);
      const signature = sign(body, secret1);
      expect(verifySignature(bytes, signature, Redacted.make(secret1))).toBe(
        true,
      );
      expect(verifySignature(bytes, signature, Redacted.make(secret2))).toBe(
        secret1 === secret2,
      );
      expect(
        verifySignature(
          new TextEncoder().encode(otherBody),
          signature,
          Redacted.make(secret1),
        ),
      ).toBe(otherBody === body);
    },
  );

  it.prop(
    "AgentSessionEvent schemas preserve decoded nullable fields through encode/decode",
    {
      prompt: Schema.String,
      guidance: Schema.String,
      summary: Schema.String,
    },
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
  );
});

describe("webhook replay, delivery, and identity invariants", () => {
  it.scopedLive.prop(
    "replay-window boundaries accept exactly within the window and reject one millisecond outside",
    {
      window: Schema.Number.pipe(Schema.int(), Schema.between(1, 10_000)),
      boundary: Schema.Literal(
        "inside",
        "lower",
        "upper",
        "too-old",
        "too-new",
      ),
    },
    ({ window, boundary }) =>
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
          const clock = Clock.make();
          clock.unsafeCurrentTimeMillis = () => now;
          const response = yield* WebhookPipeline.handle(
            signedRequest(oauthAppPayload(timestamp, { action: "created" })),
          ).pipe(Effect.withClock(clock));
          const accepted =
            boundary === "inside" ||
            boundary === "lower" ||
            boundary === "upper";
          expect(response.status).toBe(accepted ? 200 : 401);
        }),
        { ...config, webhookReplayWindowMs: window },
      ),
    { timeout: 15_000, fastCheck: { numRuns: 20 } },
  );

  it.scopedLive.prop(
    "a repeated delivery id is idempotent after durable processing",
    { delivery: Schema.String.pipe(Schema.minLength(1)) },
    ({ delivery }) =>
      withWebhook(
        Effect.gen(function* () {
          const now = yield* currentTime;
          yield* install();
          const payload = createdPayload(now);
          const first = yield* WebhookPipeline.handle(
            signedRequest(payload, { delivery }),
          );
          const second = yield* WebhookPipeline.handle(
            signedRequest(payload, { delivery }),
          );
          expect(first.status).toBe(200);
          expect(second.status).toBe(200);
          expect(yield* Effect.promise(() => second.text())).toBe(
            "Duplicate delivery",
          );
          expect(
            yield* RunInputRepo.pending(sessionId("session-1")),
          ).toHaveLength(1);
        }),
      ),
    { timeout: 15_000, fastCheck: { numRuns: 20 } },
  );

  it.scopedLive.prop(
    "all generated AgentSession installation identity mismatch forms are rejected",
    {
      mismatch: Schema.Literal(
        "organization",
        "embedded-app-user",
        "installation-app-user",
      ),
    },
    ({ mismatch }) =>
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
                    agentSession: agentSessionBase({ appUserId: "app-user" }),
                  })
                : createdPayload(now, {
                    appUserId: "other-user",
                    agentSession: agentSessionBase({ appUserId: "other-user" }),
                  });
          if (mismatch === "organization") {
            yield* install({ organizationId: organizationId("org-2") });
          } else {
            yield* install();
          }
          const response = yield* WebhookPipeline.handle(
            signedRequest(payload),
          );
          expect(response.status).toBe(401);
          expect(yield* RunRepo.get(sessionId("session-1"))).toEqual(
            Option.none(),
          );
        }),
      ),
    { timeout: 15_000, fastCheck: { numRuns: 20 } },
  );
});
