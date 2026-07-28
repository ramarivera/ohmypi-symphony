import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import type { GatewayConfig } from "../src/domain";
import {
  buildInstallationRecord,
  parseTokenResponse,
} from "../src/linear-client";
import { startAuthorization } from "../src/oauth";
import { GatewayStore } from "../src/store";

const config: GatewayConfig = {
  linearClientId: "client-id",
  linearClientSecret: "client-secret",
  linearWebhookSecret: "webhook-secret",
  tokenEncryptionKey: new Uint8Array(32).fill(9),
  publicUrl: new URL("https://gateway.example/base/"),
  databasePath: ":memory:",
  workspaceRoot: "/workspaces",
  repositoryMapPath: "/repositories.json",
  ompCliPath: "omp",
  port: 3000,
  leaseDurationMs: 60_000,
  webhookReplayWindowMs: 60_000,
};

describe("Linear OAuth contract", () => {
  test("starts an app-actor authorization with the narrow agent scopes", async () => {
    const store = await GatewayStore.open(
      ":memory:",
      config.tokenEncryptionKey,
    );
    try {
      const authorization = await startAuthorization(config, store);

      expect(authorization.url.origin).toBe("https://linear.app");
      expect(authorization.url.pathname).toBe("/oauth/authorize");
      expect(authorization.url.searchParams.get("response_type")).toBe("code");
      expect(authorization.url.searchParams.get("client_id")).toBe("client-id");
      expect(authorization.url.searchParams.get("actor")).toBe("app");
      expect(authorization.url.searchParams.get("scope")?.split(",")).toEqual([
        "read",
        "write",
        "app:assignable",
        "app:mentionable",
      ]);
      expect(authorization.url.searchParams.get("redirect_uri")).toBe(
        "https://gateway.example/base/oauth/callback",
      );
      expect(authorization.url.searchParams.get("state")).toBe(
        authorization.state,
      );

      const stateHash = createHash("sha256")
        .update(authorization.state)
        .digest("base64url");
      expect(store.consumeOAuthState(stateHash)).toBeTrue();
      expect(store.consumeOAuthState(stateHash)).toBeFalse();
    } finally {
      store.close();
    }
  });

  test("parses rotating token responses into an expiring installation", () => {
    const token = parseTokenResponse({
      access_token: "access",
      refresh_token: "refresh",
      expires_in: 86_400,
      scope: "read,write app:assignable app:mentionable",
      token_type: "Bearer",
    });
    expect(token).toEqual({
      accessToken: "access",
      tokenType: "Bearer",
      refreshToken: "refresh",
      expiresIn: 86_400,
      scopes: ["read", "write", "app:assignable", "app:mentionable"],
    });
    expect(buildInstallationRecord(token, "org", "app-user", 1_000)).toEqual({
      organizationId: "org",
      appUserId: "app-user",
      accessToken: "access",
      refreshToken: "refresh",
      expiresAt: 86_401_000,
      scopes: ["read", "write", "app:assignable", "app:mentionable"],
      revokedAt: null,
      accessibleTeamIds: null,
      canAccessAllPublicTeams: null,
    });
  });
});
