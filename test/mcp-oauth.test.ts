import { Database } from "bun:sqlite";
import { describe, expect, it, vi } from "vitest";
import {
  buildMcpAuthorizeUrl,
  createPkceChallenge,
  createPkceVerifier,
  discoverMcpOAuthMetadata,
  materializeMcpAgentDb,
  mcpProviderForServerUrl,
} from "../src/services/mcp-oauth.js";

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
    });

    expect(url.searchParams.get("client_id")).toBe("client-1");
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
      "https://mcp.example/.well-known/oauth-protected-resource",
      "https://issuer.example/.well-known/oauth-authorization-server",
    ]);
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
        access: "access-token",
        expires: 1_700_000_000_000,
      }),
    });
    expect(JSON.parse(row?.data ?? "{}")).not.toHaveProperty("type");
    db.close();
  });
});
