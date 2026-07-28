import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { createHmac } from "node:crypto";
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
  repositoryMapPath: "/repositories.json",
  ompCliPath: "omp",
  port: 3000,
  leaseDurationMs: 60_000,
  webhookReplayWindowMs: 60_000,
};

let store: GatewayStore;

beforeEach(async () => {
  store = await GatewayStore.open(":memory:", config.tokenEncryptionKey);
});
afterEach(() => store.close());

function signedRequest(
  payload: Record<string, unknown>,
  includeTimestampHeader = false,
): Request {
  const body = JSON.stringify(payload);
  const headers = new Headers({
    "content-type": "application/json",
    "linear-signature": createHmac("sha256", secret).update(body).digest("hex"),
    "linear-delivery": String(payload.webhookId),
  });
  if (includeTimestampHeader)
    headers.set("linear-timestamp", String(payload.webhookTimestamp));
  return new Request("https://gateway.example.com/webhooks/linear", {
    method: "POST",
    headers,
    body,
  });
}

describe("Linear webhook edge", () => {
  test("accepts the signed payload timestamp and records issue routing context", async () => {
    const payload = {
      type: "AgentSessionEvent",
      action: "created",
      organizationId: "org",
      webhookId: "delivery-created",
      webhookTimestamp: Date.now(),
      promptContext: "Implement the issue",
      agentSession: {
        id: "session",
        organizationId: "org",
        issueId: "issue",
        issue: { teamId: "team", projectId: "project" },
      },
    };

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(200);
    expect(store.getRun("session")).toMatchObject({
      teamId: "team",
      projectId: "project",
    });
    expect(store.pendingInputs("session")).toHaveLength(1);
  });

  test("applies team permission changes and cancels runs that lost access", async () => {
    await store.putInstallation({
      organizationId: "org",
      appUserId: "app-user",
      accessToken: "access",
      refreshToken: "refresh",
      expiresAt: Date.now() + 60_000,
      scopes: ["read", "write"],
      revokedAt: null,
      accessibleTeamIds: ["team-a", "team-b"],
      canAccessAllPublicTeams: false,
    });
    store.createRun({
      sessionId: "session",
      organizationId: "org",
      issueId: "issue",
      teamId: "team-b",
    });
    const payload = {
      type: "PermissionChange",
      action: "teamAccessChanged",
      organizationId: "org",
      oauthClientId: "client",
      appUserId: "app-user",
      addedTeamIds: ["team-c"],
      removedTeamIds: ["team-b"],
      canAccessAllPublicTeams: false,
      webhookId: "delivery-permissions",
      webhookTimestamp: Date.now(),
    };

    const response = await handleWebhook(signedRequest(payload), config, store);

    expect(response.status).toBe(200);
    expect(await store.getInstallation("org")).toMatchObject({
      accessibleTeamIds: ["team-a", "team-c"],
    });
    expect(store.getRun("session")?.desiredState).toBe("canceled");
  });

  test("rejects stale payload timestamps even when the signature is valid", async () => {
    const payload = {
      type: "OAuthApp",
      action: "revoked",
      organizationId: "org",
      oauthClientId: "client",
      webhookId: "delivery-stale",
      webhookTimestamp: Date.now() - 120_000,
    };

    const response = await handleWebhook(
      signedRequest(payload, true),
      config,
      store,
    );

    expect(response.status).toBe(401);
  });
});
