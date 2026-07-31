import { createHmac } from "node:crypto";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  test,
} from "@effect/vitest";
import { Effect, Schema } from "effect";
import type { GatewayConfig } from "../src/domain";
import { GatewayStore } from "../src/store";
import { handleWebhook } from "../src/webhook";

const secret = "webhook-secret";
const config: GatewayConfig = {
  linearClientId: "client",
  linearClientSecret: "client-secret",
  linearWebhookSecret: secret,
  tokenEncryptionKey: new Uint8Array(32).fill(4),
  publicUrl: new URL("https://gateway.example.com"),
  databasePath: ":memory:",
  workspaceRoot: "/workspaces",
  ompCliPath: "omp",
  port: 3000,
  leaseDurationMs: 60_000,
  webhookReplayWindowMs: 60_000,
  logLevel: "info",
};

let store: GatewayStore;
let now = 0;
let deliveryCounter = 0;

beforeEach(async () => {
  store = await GatewayStore.open(":memory:", config.tokenEncryptionKey);
  now = Date.now();
});

afterEach(() => store.close());

function sign(body: string): string {
  return createHmac("sha256", secret).update(body).digest("hex");
}

type RequestOptions = {
  readonly delivery?: string;
  readonly includeTimestampHeader?: boolean;
  readonly body?: string;
  readonly signature?: string;
  readonly method?: string;
  readonly url?: string;
};

function signedRequest(
  payload: Record<string, unknown>,
  options: RequestOptions = {},
): Request {
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
}

async function install(
  overrides: {
    readonly organizationId?: string;
    readonly appUserId?: string;
    readonly revokedAt?: number | null;
    readonly accessibleTeamIds?: readonly string[];
    readonly canAccessAllPublicTeams?: boolean;
  } = {},
): Promise<void> {
  await store.putInstallation({
    organizationId: overrides.organizationId ?? "org",
    appUserId: overrides.appUserId ?? "app-user",
    accessToken: "access",
    refreshToken: "refresh",
    expiresAt: now + 60_000,
    scopes: ["read", "write", "app:assignable", "app:mentionable"],
    revokedAt: overrides.revokedAt ?? null,
    accessibleTeamIds: overrides.accessibleTeamIds ?? ["team-a"],
    canAccessAllPublicTeams: overrides.canAccessAllPublicTeams ?? false,
  });
}

function agentSessionBase(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    id: "session-1",
    appUserId: "app-user",
    organizationId: "org",
    status: "pending",
    createdAt: "2024-01-01T00:00:00.000Z",
    updatedAt: "2024-01-01T00:00:00.000Z",
    ...overrides,
  };
}

function createdPayload(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
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
      comment: {
        id: "comment-1",
        body: "A thread comment",
      },
    }),
    ...overrides,
  };
}

function promptedPayload(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
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
  };
}

function stopPayload(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
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
      content: {
        type: "prompt",
        body: "Stop",
      },
      signal: "stop",
    },
    agentSession: agentSessionBase({
      status: "active",
      updatedAt: "2024-01-01T00:00:02.000Z",
      issueId: "issue-1",
      issue: {
        id: "issue-1",
        title: "Fix the bug",
        teamId: "team-a",
      },
    }),
    ...overrides,
  };
}

function permissionChangePayload(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
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
  };
}

function oauthAppPayload(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    type: "OAuthApp",
    action: "revoked",
    organizationId: "org",
    oauthClientId: "client",
    webhookId: "webhook-config",
    webhookTimestamp: now,
    ...overrides,
  };
}

describe("Linear webhook input correctness", () => {
  test("created event is durably stored and acknowledged", async () => {
    await install();
    const payload = createdPayload();

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(200);
    expect(store.getRun("session-1")).toMatchObject({
      organizationId: "org",
      issueId: "issue-1",
      teamId: "team-a",
      projectId: "project-1",
      desiredState: "running",
    });
    const inputs = store.pendingInputs("session-1");
    expect(inputs).toHaveLength(1);
    expect(inputs[0]?.id).toBe("session-1:created");
    expect(inputs[0]?.kind).toBe("created");
    expect(inputs[0]?.body).toContain("User request:");
    expect(inputs[0]?.body).toContain("Implement the issue");
    expect(inputs[0]?.body).toContain("Fix the bug");
    expect(inputs[0]?.body).toContain("TEAM-123");
    expect(inputs[0]?.body).toContain("The bug description");
    expect(inputs[0]?.body).toContain("A thread comment");
    expect(inputs[0]?.body).toContain("Earlier comment");
    expect(inputs[0]?.body).toContain("Guidance:");
    expect(inputs[0]?.body).toContain("Use TypeScript");
    expect(inputs[0]?.body).toContain("Write tests");
  });

  test("created input uses structured context and guidance, not tracker text as policy", async () => {
    await install();
    const payload = createdPayload({
      promptContext: "Please resolve this",
      guidance: [{ body: "Use idiomatic TypeScript" }],
    });

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(200);
    const [input] = store.pendingInputs("session-1");
    expect(input).toBeDefined();
    const body = input?.body ?? "";
    expect(body.startsWith("User request:\nPlease resolve this")).toBe(true);
    expect(body).toContain("Issue context:");
    expect(body).toContain("Guidance:");
    expect(body).toContain("Use idiomatic TypeScript");
    expect(body.indexOf("Guidance:")).toBeGreaterThan(
      body.indexOf("Issue context:"),
    );
  });

  test("created handles nullable fields and falls back to summary", async () => {
    await install();
    const payload = createdPayload({
      promptContext: null,
      guidance: null,
      previousComments: null,
      agentSession: agentSessionBase({
        summary: "Summary of direct chat",
        issueId: null,
        issue: null,
        comment: null,
      }),
    });

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(200);
    expect(store.getRun("session-1")).toMatchObject({
      issueId: null,
      teamId: null,
      projectId: null,
    });
    const [input] = store.pendingInputs("session-1");
    expect(input?.kind).toBe("created");
    expect(input?.body).toBe("User request:\nSummary of direct chat");
  });

  test("accepts real Linear AgentSessionEvent created payload with null guidance and no agentSession.type", async () => {
    await install();
    const payload = createdPayload({
      webhookId: "webhook-uuid",
      createdAt: "2024-01-01T00:00:00.000Z",
      promptContext: "Implement the issue",
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
    });

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(200);
    expect(store.getRun("session-1")).toMatchObject({
      organizationId: "org",
      issueId: "issue-1",
      teamId: "team-a",
    });
    const [input] = store.pendingInputs("session-1");
    expect(input).toBeDefined();
    expect(input?.kind).toBe("created");
    expect(input?.body).not.toContain("Guidance:");
  });

  test("prompted event appends agent activity body", async () => {
    await install();
    const response1 = await handleWebhook(
      signedRequest(createdPayload()),
      config,
      store,
    );
    expect(response1.status).toBe(200);

    const response2 = await handleWebhook(
      signedRequest(promptedPayload()),
      config,
      store,
    );
    expect(response2.status).toBe(200);

    const inputs = store.pendingInputs("session-1");
    expect(inputs).toHaveLength(2);
    const promptedInput = inputs.find((i) => i.kind === "prompted");
    expect(promptedInput).toBeDefined();
    expect(promptedInput?.id).toBe("session-1:prompted:activity-prompt-1");
    expect(promptedInput?.body).toContain("# Follow-up prompt");
    expect(promptedInput?.body).toContain("Please add more tests");
  });

  test("prompted with stop signal cancels the run", async () => {
    await install();
    const payload = promptedPayload({
      agentActivity: {
        id: "activity-prompt-stop",
        agentSessionId: "session-1",
        content: { type: "prompt", body: "Never mind" },
        signal: "stop",
      },
    });

    await handleWebhook(signedRequest(createdPayload()), config, store);
    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(200);
    const inputs = store.pendingInputs("session-1");
    expect(inputs.find((i) => i.kind === "stop")).toBeDefined();
    expect(store.getRun("session-1")?.desiredState).toBe("canceled");
  });

  test("stop action cancels the run", async () => {
    await install();
    await handleWebhook(signedRequest(createdPayload()), config, store);

    const response = await handleWebhook(
      signedRequest(stopPayload()),
      config,
      store,
    );

    expect(response.status).toBe(200);
    expect(store.getRun("session-1")?.desiredState).toBe("canceled");
    const inputs = store.pendingInputs("session-1");
    expect(inputs).toHaveLength(2);
    expect(inputs[1]?.kind).toBe("stop");
    expect(inputs[1]?.body).toBe("Stop");
  });

  test("stop dominates later prompted events", async () => {
    await install();
    await handleWebhook(signedRequest(createdPayload()), config, store);
    await handleWebhook(signedRequest(stopPayload()), config, store);

    const response = await handleWebhook(
      signedRequest(promptedPayload()),
      config,
      store,
    );

    expect(response.status).toBe(200);
    const inputs = store.pendingInputs("session-1");
    expect(inputs).toHaveLength(2);
    expect(store.getRun("session-1")?.desiredState).toBe("canceled");
  });

  test("dedupes by Linear-Delivery id", async () => {
    await install();
    const payload = createdPayload();

    const response1 = await handleWebhook(
      signedRequest(payload, { delivery: "d1" }),
      config,
      store,
    );
    const response2 = await handleWebhook(
      signedRequest(payload, { delivery: "d1" }),
      config,
      store,
    );

    expect(response1.status).toBe(200);
    expect(response2.status).toBe(200);
    expect(await response2.text()).toBe("Duplicate delivery");
    expect(store.pendingInputs("session-1")).toHaveLength(1);
  });

  test("dedupes logical input on replay with a different Linear-Delivery id", async () => {
    await install();
    const payload = createdPayload();
    const body = JSON.stringify(payload);
    const signature = sign(body);

    const response1 = await handleWebhook(
      signedRequest(payload, { delivery: "d1", body, signature }),
      config,
      store,
    );
    const response2 = await handleWebhook(
      signedRequest(payload, { delivery: "d2", body, signature }),
      config,
      store,
    );

    expect(response1.status).toBe(200);
    expect(response2.status).toBe(200);
    expect(store.pendingInputs("session-1")).toHaveLength(1);
  });

  test("accepts header timestamp and rejects stale body timestamps", async () => {
    await install();
    const payload = createdPayload({
      webhookTimestamp: now - 120_000,
    });

    const response = await handleWebhook(
      signedRequest(payload, { includeTimestampHeader: true }),
      config,
      store,
    );

    expect(response.status).toBe(401);
    expect(store.getRun("session-1")).toBeNull();
  });

  test("rejects future timestamps", async () => {
    await install();
    const payload = createdPayload({
      webhookTimestamp: now + 120_000,
    });

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(401);
  });

  test("rejects invalid signature", async () => {
    await install();
    const payload = createdPayload();

    const response = await handleWebhook(
      signedRequest(payload, { signature: "deadbeef" }),
      config,
      store,
    );

    expect(response.status).toBe(401);
    expect(store.getRun("session-1")).toBeNull();
  });

  test("rejects tampered raw body because HMAC covers raw bytes", async () => {
    await install();
    const payload = createdPayload();
    const body = JSON.stringify(payload);
    const signature = sign(body);
    const tampered = body.replace("Fix the bug", "Fix the BUG");

    const response = await handleWebhook(
      signedRequest(payload, { body: tampered, signature }),
      config,
      store,
    );

    expect(response.status).toBe(401);
    expect(store.getRun("session-1")).toBeNull();
  });

  test("rejects missing signature", async () => {
    await install();
    const request = signedRequest(createdPayload());
    request.headers.delete("linear-signature");

    const response = await handleWebhook(request, config, store);

    expect(response.status).toBe(400);
  });

  test("rejects non-POST method", async () => {
    const response = await handleWebhook(
      signedRequest(createdPayload(), { method: "GET" }),
      config,
      store,
    );

    expect(response.status).toBe(405);
  });

  test("rejects malformed payload missing agentSession", async () => {
    await install();
    const payload = createdPayload({
      agentSession: undefined,
    });

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(400);
  });

  test("rejects cross-tenant oauthClientId", async () => {
    await install();
    const payload = createdPayload({
      oauthClientId: "other-client",
    });

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(401);
  });

  test("rejects cross-tenant organization identity mismatch", async () => {
    await install({ organizationId: "org2" });
    const payload = createdPayload({
      organizationId: "org2",
      agentSession: agentSessionBase({
        organizationId: "org",
      }),
    });

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(401);
  });

  test("rejects cross-tenant appUser identity mismatch", async () => {
    await install();
    const payload = createdPayload({
      appUserId: "other-user",
      agentSession: agentSessionBase({
        appUserId: "app-user",
      }),
    });

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(401);
  });

  test("rejects appUser that does not match installation", async () => {
    await install();
    const payload = createdPayload({
      appUserId: "other-user",
      agentSession: agentSessionBase({
        appUserId: "other-user",
      }),
    });

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(401);
  });

  test("rejects when no installation exists", async () => {
    const payload = createdPayload();

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(401);
  });

  test("rejects when installation is revoked", async () => {
    await install({ revokedAt: now - 1_000 });
    const payload = createdPayload();

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(401);
  });

  test("OAuthApp revoked cancels runs and marks installation revoked", async () => {
    await install();
    store.createRun({
      sessionId: "session-1",
      organizationId: "org",
      issueId: "issue-1",
      teamId: "team-a",
    });

    const response = await handleWebhook(
      signedRequest(oauthAppPayload()),
      config,
      store,
    );

    expect(response.status).toBe(200);
    const installation = await store.getInstallation("org");
    expect(installation).not.toBeNull();
    expect(installation?.revokedAt).not.toBeNull();
    expect(store.getRun("session-1")?.desiredState).toBe("canceled");
  });

  test("PermissionChange updates team access and cancels runs", async () => {
    await install({ accessibleTeamIds: ["team-a", "team-b"] });
    store.createRun({
      sessionId: "session-1",
      organizationId: "org",
      issueId: "issue-1",
      teamId: "team-b",
    });

    const response = await handleWebhook(
      signedRequest(permissionChangePayload()),
      config,
      store,
    );

    expect(response.status).toBe(200);
    const installation = await store.getInstallation("org");
    expect(installation?.accessibleTeamIds).toEqual(["team-a"]);
    expect(store.getRun("session-1")?.desiredState).toBe("canceled");
  });

  test("PermissionChange rejects mismatched appUser", async () => {
    await install();

    const response = await handleWebhook(
      signedRequest(permissionChangePayload({ appUserId: "other-user" })),
      config,
      store,
    );

    expect(response.status).toBe(401);
    const installation = await store.getInstallation("org");
    expect(installation?.accessibleTeamIds).toEqual(["team-a"]);
  });

  test("acknowledges unknown event type without side effects", async () => {
    await install();
    const payload = createdPayload({ type: "Unknown" });

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(200);
    expect(store.getRun("session-1")).toBeNull();
  });

  test("acknowledges unknown AgentSession action without input", async () => {
    await install();
    const payload = createdPayload({ action: "updated" });

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(200);
    expect(store.pendingInputs("session-1")).toHaveLength(0);
  });

  test("resolves team and project from child objects", async () => {
    await install();
    const payload = createdPayload({
      agentSession: agentSessionBase({
        issueId: "issue-1",
        issue: {
          id: "issue-1",
          title: "Fix the bug",
          team: { id: "team-child" },
          project: { id: "project-child" },
        },
      }),
    });

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(200);
    expect(store.getRun("session-1")).toMatchObject({
      teamId: "team-child",
      projectId: "project-child",
    });
  });
});

describe("webhook signature invariants", () => {
  it.prop(
    "HMAC-SHA256 signature is deterministic and sensitive to body and secret",
    {
      body: Schema.String.pipe(Schema.minLength(1)),
      otherBody: Schema.String.pipe(Schema.minLength(1)),
      secret1: Schema.String.pipe(Schema.minLength(1)),
      secret2: Schema.String.pipe(Schema.minLength(1)),
    },
    ({ body, otherBody, secret1, secret2 }) => {
      const sign = (data: string, secret: string): string =>
        createHmac("sha256", secret).update(data).digest("hex");

      const a = sign(body, secret1);
      const b = sign(body, secret1);
      expect(b).toBe(a);

      const c = sign(body, secret2);
      const d = sign(otherBody, secret1);

      expect(c === a).toBe(secret1 === secret2);
      expect(d === a).toBe(body === otherBody);
    },
  );
});

describe("replay window invariants", () => {
  it.effect.prop(
    "acceptance strictly matches abs(now - timestamp) <= window",
    {
      now: Schema.Number.pipe(
        Schema.int(),
        Schema.between(1, 1_000_000_000_000),
      ),
      timestamp: Schema.Number.pipe(
        Schema.int(),
        Schema.between(1, 1_000_000_000_000),
      ),
      window: Schema.Number.pipe(
        Schema.int(),
        Schema.between(0, 1_000_000_000),
      ),
    },
    ({ now, timestamp, window }) =>
      Effect.gen(function* () {
        const originalNow = Date.now;
        Date.now = () => now;
        try {
          const payload = {
            type: "OAuthApp",
            action: "created",
            oauthClientId: config.linearClientId,
            organizationId: "org",
            webhookId: "webhook-config",
            webhookTimestamp: timestamp,
          };
          const request = signedRequest(payload);
          const response = yield* Effect.tryPromise({
            try: () =>
              handleWebhook(
                request,
                { ...config, webhookReplayWindowMs: window },
                store,
              ),
            catch: (error) => new Error(String(error)),
          });
          const accepted = response.status === 200;
          const expected = Math.abs(now - timestamp) <= window;
          expect(accepted).toBe(expected);
        } finally {
          Date.now = originalNow;
        }
      }),
  );
});
