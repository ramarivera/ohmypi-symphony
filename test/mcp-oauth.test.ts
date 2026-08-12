import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
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
  isLoopbackHost,
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

const hash = (value: string) =>
  createHash("sha256").update(value).digest("base64url");

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
  it("only accepts parsed IPv4 loopback addresses", () => {
    expect(isLoopbackHost("127.0.0.1")).toBe(true);
    expect(isLoopbackHost("127.255.255.255")).toBe(true);
    expect(isLoopbackHost("127.attacker.example")).toBe(false);
    expect(isLoopbackHost("127.0.0.1.attacker.example")).toBe(false);
  });

  it("discovers protected-resource and authorization-server metadata", async () => {
    const fetchMock = vi.fn<typeof fetch>();
    fetchMock
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            resource: "https://mcp.example/server",
            authorization_servers: ["https://issuer.example"],
          }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            issuer: "https://issuer.example",
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
  it("rejects authorization metadata for a different issuer", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            resource: "https://mcp.example/server",
            authorization_servers: ["https://issuer.example"],
          }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            issuer: "https://other.example",
            authorization_endpoint: "https://issuer.example/authorize",
            token_endpoint: "https://issuer.example/token",
          }),
          { status: 200 },
        ),
      );
    await expect(
      discoverMcpOAuthMetadata("https://mcp.example/server", fetchMock),
    ).rejects.toThrow("metadata issuer");
  });
  it("returns a typed discovery reason for unavailable metadata", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValue(new Response("unavailable", { status: 503 }));
    await expect(
      discoverMcpOAuthMetadata("https://mcp.example/server", fetchMock),
    ).rejects.toThrow("protected-resource discovery failed (503)");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
  it("rejects an authorization server registration endpoint on another origin", async () => {
    const fetchMock = vi.fn<typeof fetch>();
    fetchMock
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            resource: "https://mcp.example/server",
            authorization_servers: ["https://issuer.example"],
          }),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            issuer: "https://issuer.example",
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
  it("rejects protected-resource metadata for a different resource", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          resource: "https://mcp.example/other",
          authorization_servers: ["https://issuer.example"],
        }),
        { status: 200 },
      ),
    );
    await expect(
      discoverMcpOAuthMetadata("https://mcp.example/server", fetchMock),
    ).rejects.toThrow("Protected-resource metadata describes");
  });

  it("rejects duplicate provider keys during credential materialization", async () => {
    const workspace = `/tmp/mcp-oauth-duplicate-${crypto.randomUUID()}`;
    await expect(
      materializeMcpAgentDb(workspace, [
        {
          serverUrl: "https://mcp.example/server",
          accessToken: "access-one",
          expiresAt: 1_700_000_000_000,
        },
        {
          serverUrl: "https://mcp.example/server",
          accessToken: "access-two",
          expiresAt: 1_700_000_000_001,
        },
      ]),
    ).rejects.toThrow("Duplicate MCP OAuth provider key");
    await rm(workspace, { recursive: true, force: true });
  });
  it("uses Basic authentication for confidential pre-registered clients", async () => {
    const organizationId = "org-basic-auth" as OrganizationId;
    const serverId = "mcp-basic-auth" as McpServerId;
    const state = "state-basic-auth";
    const verifier = "verifier-basic-auth";
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(
        JSON.stringify({
          access_token: "access-token",
          refresh_token: "refresh-token",
          token_type: "Bearer",
        }),
        { status: 200 },
      ),
    );
    const configProvider = ConfigProvider.fromMap(
      new Map([
        ["LINEAR_CLIENT_ID", "test-client"],
        ["LINEAR_CLIENT_SECRET", "test-secret"],
        ["LINEAR_WEBHOOK_SECRET", "test-webhook-secret"],
        [
          "TOKEN_ENCRYPTION_KEY",
          Buffer.from(new Uint8Array(32).fill(7)).toString("base64"),
        ],
        ["PUBLIC_URL", "http://localhost:3000"],
        [
          "NIXPKGS_FLAKE_REF",
          "github:NixOS/nixpkgs/0123456789012345678901234567890123456789",
        ],
        ["WORKSPACE_ROOT", "/tmp/mcp-oauth-basic"],
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
    vi.stubGlobal("fetch", fetchMock);
    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const serverRepo = yield* McpServerRepo;
            const crypto = yield* TokenCrypto;
            const { db } = yield* SqliteClient;
            yield* serverRepo.createMcpServer({
              organizationId,
              id: serverId,
              name: "basic auth",
              transport: "http",
              url: "https://mcp.example/server",
            });
            const client = yield* crypto.encrypt(
              JSON.stringify({
                clientId: "confidential: client",
                clientSecret: "confidential% secret",
                tokenEndpointAuthMethod: "client_secret_basic",
                authorizationEndpoint: "https://issuer.example/authorize",
                tokenEndpoint: "https://issuer.example/token",
              }),
            );
            const encryptedVerifier = yield* crypto.encrypt(verifier);
            const now = Date.now();
            db.query(
              "INSERT INTO mcp_oauth_state (state_hash, organization_id, server_id, server_url, admin_session_hash, code_verifier, redirect_uri, client_json, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ).run(
              hash(state),
              organizationId,
              serverId,
              "https://mcp.example/server",
              "admin-session-hash",
              `${MCP_OAUTH_ENCRYPTED_PREFIX}${encryptedVerifier}`,
              "http://localhost:3000/oauth/mcp/callback",
              `${MCP_OAUTH_ENCRYPTED_PREFIX}${client}`,
              now + 60_000,
            );
            const service = yield* McpOAuth;
            const wrongSession = yield* Effect.either(
              service.completeMcpAuthorization(
                new URL(
                  "http://localhost:3000/oauth/mcp/callback?code=auth-code&state=state-basic-auth",
                ),
                "other-admin-session",
              ),
            );
            expect(Either.isLeft(wrongSession)).toBe(true);
            yield* service.completeMcpAuthorization(
              new URL(
                "http://localhost:3000/oauth/mcp/callback?code=auth-code&state=state-basic-auth",
              ),
              "admin-session-hash",
            );
            const credential = yield* service.getCredentialDetails(
              organizationId,
              serverId,
            );
            const deniedState = "state-denied";
            db.query(
              "INSERT INTO mcp_oauth_state (state_hash, organization_id, server_id, server_url, admin_session_hash, code_verifier, redirect_uri, client_json, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ).run(
              hash(deniedState),
              organizationId,
              serverId,
              "https://mcp.example/server",
              "admin-session-hash",
              `${MCP_OAUTH_ENCRYPTED_PREFIX}${encryptedVerifier}`,
              "http://localhost:3000/oauth/mcp/callback",
              `${MCP_OAUTH_ENCRYPTED_PREFIX}${client}`,
              Date.now() + 60_000,
            );
            const denied = yield* Effect.either(
              service.completeMcpAuthorization(
                new URL(
                  "http://localhost:3000/oauth/mcp/callback?error=access_denied&error_description=User%20denied%20consent&state=state-denied",
                ),
                "admin-session-hash",
              ),
            );
            expect(Either.isLeft(denied)).toBe(true);
            if (Either.isLeft(denied)) {
              expect(denied.left.message).toContain("User denied consent");
            }
            expect(Option.getOrThrow(credential).token.expiresAt).toBe(
              Number.MAX_SAFE_INTEGER,
            );
          }).pipe(Effect.provide(dependencies)),
        ),
      );
    } finally {
      vi.unstubAllGlobals();
    }
    const [, init] = fetchMock.mock.calls[0] ?? [];
    expect(new Headers(init?.headers).get("authorization")).toBe(
      `Basic ${Buffer.from("confidential%3A+client:confidential%25+secret").toString("base64")}`,
    );
    expect(String(init?.body)).not.toContain("client_secret");
  });

  it("invalidates an earlier authorization before inserting a retry", async () => {
    const organizationId = "org-retry-state" as OrganizationId;
    const serverId = "mcp-retry-state" as McpServerId;
    const fetchMock = vi.fn<typeof fetch>();
    const responses = [
      new Response(
        JSON.stringify({
          resource: "https://mcp.example/server",
          authorization_servers: ["https://issuer.example"],
        }),
        { status: 200 },
      ),
      new Response(
        JSON.stringify({
          issuer: "https://issuer.example",
          authorization_endpoint: "https://issuer.example/authorize",
          token_endpoint: "https://issuer.example/token",
        }),
        { status: 200 },
      ),
      new Response(
        JSON.stringify({
          resource: "https://mcp.example/server",
          authorization_servers: ["https://issuer.example"],
        }),
        { status: 200 },
      ),
      new Response(
        JSON.stringify({
          issuer: "https://issuer.example",
          authorization_endpoint: "https://issuer.example/authorize",
          token_endpoint: "https://issuer.example/token",
        }),
        { status: 200 },
      ),
    ];
    fetchMock.mockImplementation(async () => {
      const response = responses.shift();
      return (
        response ??
        new Response("unexpected discovery request", { status: 500 })
      );
    });
    const configProvider = ConfigProvider.fromMap(
      new Map([
        ["LINEAR_CLIENT_ID", "test-client"],
        ["LINEAR_CLIENT_SECRET", "test-secret"],
        ["LINEAR_WEBHOOK_SECRET", "test-webhook-secret"],
        [
          "TOKEN_ENCRYPTION_KEY",
          Buffer.from(new Uint8Array(32).fill(7)).toString("base64"),
        ],
        ["PUBLIC_URL", "http://localhost:3000"],
        [
          "NIXPKGS_FLAKE_REF",
          "github:NixOS/nixpkgs/0123456789012345678901234567890123456789",
        ],
        ["WORKSPACE_ROOT", "/tmp/mcp-oauth-retry-state"],
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
    vi.stubGlobal("fetch", fetchMock);
    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const serverRepo = yield* McpServerRepo;
            const service = yield* McpOAuth;
            const { db } = yield* SqliteClient;
            yield* serverRepo.createMcpServer({
              organizationId,
              id: serverId,
              name: "retry state",
              transport: "http",
              url: "https://mcp.example/server",
            });
            const client = {
              clientId: "client-1",
              authorizationEndpoint: "https://issuer.example/authorize",
              tokenEndpoint: "https://issuer.example/token",
            };
            const first = yield* service.startMcpAuthorization(
              organizationId,
              serverId,
              client,
              undefined,
              "admin-session",
            );
            const second = yield* service.startMcpAuthorization(
              organizationId,
              serverId,
              client,
              undefined,
              "admin-session",
            );
            expect(second.state).not.toBe(first.state);
            const row = db
              .query<{ count: number }, [string, string]>(
                "SELECT COUNT(*) AS count FROM mcp_oauth_state WHERE organization_id = ? AND server_id = ? AND consumed_at IS NULL",
              )
              .get(organizationId, serverId);
            expect(row?.count).toBe(1);
            const active = db
              .query<{ state_hash: string }, [string, string]>(
                "SELECT state_hash FROM mcp_oauth_state WHERE organization_id = ? AND server_id = ? AND consumed_at IS NULL",
              )
              .get(organizationId, serverId);
            expect(active?.state_hash).toBe(hash(second.state));
            yield* service.disconnect(organizationId, serverId);
            const afterDisconnect = db
              .query<{ count: number }, [string, string]>(
                "SELECT COUNT(*) AS count FROM mcp_oauth_state WHERE organization_id = ? AND server_id = ? AND consumed_at IS NULL",
              )
              .get(organizationId, serverId);
            expect(afterDisconnect?.count).toBe(0);
          }).pipe(Effect.provide(dependencies)),
        ),
      );
    } finally {
      vi.unstubAllGlobals();
    }
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
  it("materializes refresh material for a live MCP connection", async () => {
    const workspace = `/tmp/mcp-oauth-refresh-${crypto.randomUUID()}`;
    const agentDir = await materializeMcpAgentDb(workspace, [
      {
        serverUrl: "https://mcp.example/server",
        accessToken: "access-token",
        refreshToken: "refresh-token",
        expiresAt: 1_700_000_000_000,
        tokenEndpoint: "https://issuer.example/token",
        clientId: "client-1",
      },
    ]);
    const db = new Database(`${agentDir}/agent.db`);
    const row = db
      .query<{ data: string }, []>("SELECT data FROM auth_credentials")
      .get();
    expect(JSON.parse(row?.data ?? "{}")).toMatchObject({
      access: "access-token",
      refresh: "refresh-token",
      tokenUrl: "https://issuer.example/token",
      clientId: "client-1",
    });
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
