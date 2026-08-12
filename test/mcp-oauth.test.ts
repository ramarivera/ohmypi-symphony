import { Database } from "bun:sqlite";
import { rm } from "node:fs/promises";
import { ConfigProvider, Effect, Either, Fiber, Layer, Option } from "effect";
import { describe, expect, it, vi } from "vitest";
import type { McpServerId, OrganizationId } from "../src/domain/ids.js";
import { GatewayConfig } from "../src/services/config.js";
import {
  buildMcpAuthorizeUrl,
  createPkceChallenge,
  createPkceVerifier,
  discoverMcpOAuthMetadata,
  MCP_OAUTH_ENCRYPTED_PREFIX,
  McpOAuth,
  materializeMcpAgentDb,
  mcpProviderForServerUrl,
} from "../src/services/mcp-oauth.js";
import { McpServerRepo } from "../src/services/store/repositories.js";
import {
  SqliteClient,
  SqliteClientLive,
} from "../src/services/store/sqlite-client.js";
import { TokenCrypto } from "../src/services/token-crypto.js";

describe("MCP OAuth primitives", () => {
  it("builds an RFC 7636 S256 authorize URL", () => {
    const verifier = "verifier-value";
    const url = buildMcpAuthorizeUrl({
      client: {
        clientId: "client-1",
        authorizationEndpoint: "https://auth.example/authorize",
        tokenEndpoint: "https://auth.example/token",
      },
      redirectUri: "https://gateway.example/oauth/mcp/callback",
      state: "state-value",
      codeVerifier: verifier,
      scope: "read",
      resource: "https://mcp.example/server",
    });

    expect(url.searchParams.get("client_id")).toBe("client-1");
    expect(url.searchParams.get("resource")).toBe("https://mcp.example/server");
    expect(url.searchParams.get("state")).toBe("state-value");
    expect(url.searchParams.get("code_challenge_method")).toBe("S256");
    expect(url.searchParams.get("code_challenge")).toBe(
      createPkceChallenge(verifier),
    );
    expect(createPkceVerifier()).toMatch(/^[A-Za-z0-9_-]{43}$/u);
  });

  it("discovers protected-resource and authorization-server metadata", async () => {
    const fetchMock = vi.fn<typeof fetch>();
    fetchMock
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ authorization_servers: ["https://issuer.example"] }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            authorization_endpoint: "https://issuer.example/authorize",
            token_endpoint: "https://issuer.example/token",
            registration_endpoint: "https://issuer.example/register",
          }),
          { status: 200 },
        ),
      );

    await expect(
      discoverMcpOAuthMetadata("https://mcp.example/server", fetchMock),
    ).resolves.toEqual({
      authorizationEndpoint: "https://issuer.example/authorize",
      tokenEndpoint: "https://issuer.example/token",
      registrationEndpoint: "https://issuer.example/register",
    });
    expect(fetchMock.mock.calls.map(([url]) => String(url))).toEqual([
      "https://mcp.example/.well-known/oauth-protected-resource/server",
      "https://issuer.example/.well-known/oauth-authorization-server",
    ]);
  });

  it("rejects an authorization server registration endpoint on another origin", async () => {
    const fetchMock = vi.fn<typeof fetch>();
    fetchMock
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ authorization_servers: ["https://issuer.example"] }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            authorization_endpoint: "https://issuer.example/authorize",
            token_endpoint: "https://issuer.example/token",
            registration_endpoint: "https://attacker.example/register",
          }),
          { status: 200 },
        ),
      );

    const result = discoverMcpOAuthMetadata(
      "https://mcp.example/server",
      fetchMock,
    );
    await expect(result).rejects.toThrow("untrusted origin");
  });

  it("materializes an isolated Pi auth database with access-only MCP credentials", async () => {
    const workspace = `/tmp/mcp-oauth-test-${crypto.randomUUID()}`;
    const agentDir = await materializeMcpAgentDb(workspace, [
      {
        serverUrl: "https://mcp.example/server",
        accessToken: "access-token",
        expiresAt: 1_700_000_000_000,
      },
    ]);
    await materializeMcpAgentDb(workspace, [
      {
        serverUrl: "https://mcp.example/server",
        accessToken: "access-token-2",
        expiresAt: 1_700_000_000_001,
      },
    ]);
    const db = new Database(`${agentDir}/agent.db`);
    const columns = db
      .query<{ name: string }, []>("PRAGMA table_info(auth_credentials)")
      .all()
      .map((column) => column.name);
    expect(columns).toEqual([
      "id",
      "provider",
      "credential_type",
      "data",
      "disabled_cause",
      "identity_key",
      "created_at",
      "updated_at",
    ]);
    const row = db
      .query<{ provider: string; credential_type: string; data: string }, []>(
        "SELECT provider, credential_type, data FROM auth_credentials",
      )
      .get();
    expect(row).toEqual({
      provider: mcpProviderForServerUrl("https://mcp.example/server"),
      credential_type: "oauth",
      data: JSON.stringify({
        access: "access-token-2",
        expires: 1_700_000_000_001,
      }),
    });
    expect(JSON.parse(row?.data ?? "{}")).not.toHaveProperty("type");
    db.close();
    await rm(workspace, { recursive: true, force: true });
  });

  it("does not resurrect a credential when disconnect wins a refresh race", async () => {
    const organizationId = "org-refresh-race" as OrganizationId;
    const serverId = "mcp-refresh-race" as McpServerId;
    let releaseRefresh!: () => void;
    let markRefreshStarted!: () => void;
    const refreshStarted = new Promise<void>((resolve) => {
      markRefreshStarted = resolve;
    });
    const refreshRelease = new Promise<void>((resolve) => {
      releaseRefresh = resolve;
    });
    const fetchMock = vi.fn(async () => {
      markRefreshStarted();
      await refreshRelease;
      return new Response(
        JSON.stringify({
          access_token: "new-access",
          refresh_token: "new-refresh",
          expires_in: 3600,
          token_type: "Bearer",
        }),
        { status: 200 },
      );
    });
    vi.stubGlobal("fetch", fetchMock);

    const configProvider = ConfigProvider.fromMap(
      new Map([
        ["LINEAR_CLIENT_ID", "test-client"],
        ["LINEAR_CLIENT_SECRET", "test-secret"],
        ["LINEAR_WEBHOOK_SECRET", "test-webhook-secret"],
        [
          "TOKEN_ENCRYPTION_KEY",
          "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=",
        ],
        ["PUBLIC_URL", "http://localhost:3000"],
        [
          "NIXPKGS_FLAKE_REF",
          "github:NixOS/nixpkgs/0123456789012345678901234567890123456789",
        ],
        ["WORKSPACE_ROOT", "/tmp/mcp-oauth-race"],
      ]),
    );
    const sqlite = SqliteClientLive(":memory:");
    const configLayer = Layer.setConfigProvider(configProvider);
    const token = TokenCrypto.Default.pipe(
      Layer.provide(Layer.mergeAll(sqlite, configLayer)),
    );
    const gateway = GatewayConfig.Default.pipe(Layer.provide(configLayer));
    const servers = McpServerRepo.Default.pipe(
      Layer.provide(Layer.mergeAll(sqlite, token)),
    );
    const oauth = McpOAuth.Default.pipe(
      Layer.provide(Layer.mergeAll(sqlite, token, gateway, servers)),
    );
    const dependencies = Layer.mergeAll(sqlite, token, gateway, servers, oauth);

    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const servers = yield* McpServerRepo;
            const crypto = yield* TokenCrypto;
            const oauth = yield* McpOAuth;
            const { db } = yield* SqliteClient;
            yield* servers.createMcpServer({
              organizationId,
              id: serverId,
              name: "refresh race",
              transport: "http",
              url: "https://mcp.example/server",
              now: 1,
            });
            const clientJson = yield* crypto.encrypt(
              JSON.stringify({
                clientId: "client-1",
                authorizationEndpoint: "https://mcp.example/authorize",
                tokenEndpoint: "https://mcp.example/token",
              }),
            );
            const accessToken = yield* crypto.encrypt("old-access");
            const refreshToken = yield* crypto.encrypt("old-refresh");
            db.query(
              "INSERT INTO mcp_oauth_credential (organization_id, server_id, server_url, client_json, access_token, refresh_token, expires_at, token_type, scope, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ).run(
              organizationId,
              serverId,
              "https://mcp.example/server",
              `${MCP_OAUTH_ENCRYPTED_PREFIX}${clientJson}`,
              `${MCP_OAUTH_ENCRYPTED_PREFIX}${accessToken}`,
              `${MCP_OAUTH_ENCRYPTED_PREFIX}${refreshToken}`,
              0,
              "Bearer",
              null,
              1,
              10,
            );
            expect(
              Option.isSome(
                yield* oauth.getCredential(organizationId, serverId),
              ),
            ).toBe(true);

            const refreshFiber = yield* Effect.fork(
              Effect.either(oauth.mintCredential(organizationId, serverId)),
            );
            yield* Effect.promise(() => refreshStarted);
            yield* oauth.disconnect(organizationId, serverId);
            releaseRefresh();
            const outcome = yield* Fiber.join(refreshFiber);
            expect(Either.isLeft(outcome)).toBe(true);
            expect(
              Option.isNone(
                yield* oauth.getCredential(organizationId, serverId),
              ),
            ).toBe(true);
          }).pipe(Effect.provide(dependencies)),
        ),
      );
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
