import { describe, expect, it, test } from "@effect/vitest"
import { createHash } from "node:crypto";
import { Schema } from "effect";
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
  ompCliPath: "omp",
  port: 3000,
  leaseDurationMs: 60_000,
  webhookReplayWindowMs: 60_000,
  logLevel: "info",
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
      expect(store.consumeOAuthState(stateHash)).toBe(true);
      expect(store.consumeOAuthState(stateHash)).toBe(false);
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

describe("OAuth token invariants", () => {
  it.prop(
    "buildInstallationRecord computes expiresAt and preserves token fields",
    {
      accessToken: Schema.String.pipe(Schema.minLength(1)),
      refreshToken: Schema.String.pipe(Schema.minLength(1)),
      expiresIn: Schema.Number.pipe(Schema.int(), Schema.between(1, 100_000)),
      organizationId: Schema.String.pipe(Schema.minLength(1)),
      appUserId: Schema.String.pipe(Schema.minLength(1)),
      now: Schema.Number.pipe(Schema.int(), Schema.between(0, 2_000_000_000_000)),
    },
    ({ accessToken, refreshToken, expiresIn, organizationId, appUserId, now }) => {
      const token = parseTokenResponse({
        access_token: accessToken,
        refresh_token: refreshToken,
        expires_in: expiresIn,
        token_type: "Bearer",
        scope: "read,write app:assignable app:mentionable",
      });
      const installation = buildInstallationRecord(token, organizationId, appUserId, now);
      expect(installation.organizationId).toBe(organizationId);
      expect(installation.appUserId).toBe(appUserId);
      expect(installation.accessToken).toBe(accessToken);
      expect(installation.refreshToken).toBe(refreshToken);
      expect(installation.expiresAt).toBe(now + expiresIn * 1000);
      expect(installation.scopes).toEqual(["read", "write", "app:assignable", "app:mentionable"]);
      expect(installation.revokedAt).toBeNull();
      expect(installation.accessibleTeamIds).toBeNull();
      expect(installation.canAccessAllPublicTeams).toBeNull();
    },
  );
});
