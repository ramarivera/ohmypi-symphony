import { Database } from "bun:sqlite";
import { createHash, randomBytes } from "node:crypto";
import type { Stats } from "node:fs";
import { chmod, lstat, mkdir, rm, unlink, writeFile } from "node:fs/promises";
import { isAbsolute, join, resolve as resolvePath } from "node:path";
import { Clock, Deferred, Effect, Option, Ref, Schema } from "effect";
import {
  DatabaseError,
  McpOAuthError,
  OAuthStateError,
  type TokenCipherError,
  TokenRefreshError,
} from "../domain/errors.js";
import type { McpServerId, OrganizationId } from "../domain/ids.js";
import { GatewayConfig } from "./config.js";
import { McpServerRepo } from "./store/repositories.js";
import {
  decodeRow,
  decodeRows,
  runChanges,
  SqliteClient,
  transact,
  tryDb,
} from "./store/sqlite-client.js";
import { TokenCrypto } from "./token-crypto.js";

export const MCP_OAUTH_PROVIDER_PREFIX = "mcp_oauth:profile:default:";
export const MCP_OAUTH_ENCRYPTED_PREFIX = "mcpenc:v1:";
export const MCP_OAUTH_CALLBACK_PATH = "/oauth/mcp/callback";
export const MCP_OAUTH_STATE_TTL_MS = 10 * 60 * 1000;
const PKCE_BYTES = 32;
const FETCH_TIMEOUT_MS = 5_000;

type FetchLike = typeof fetch;

export interface McpOAuthMetadata {
  readonly authorizationEndpoint: string;
  readonly tokenEndpoint: string;
  readonly registrationEndpoint?: string | undefined;
}

export interface McpOAuthClientMetadata {
  readonly clientId: string;
  readonly clientSecret?: string | undefined;
  readonly authorizationEndpoint: string;
  readonly tokenEndpoint: string;
  readonly registrationEndpoint?: string | undefined;
}

export interface McpOAuthToken {
  readonly accessToken: string;
  readonly refreshToken?: string | undefined;
  readonly expiresAt: number;
  readonly tokenType?: string | undefined;
  readonly scope?: string | undefined;
}

export interface McpOAuthCredential {
  readonly serverUrl: string;
  readonly client: McpOAuthClientMetadata;
  readonly token: McpOAuthToken;
}

const McpOAuthMetadataSchema = Schema.Struct({
  authorizationEndpoint: Schema.String,
  tokenEndpoint: Schema.String,
  registrationEndpoint: Schema.optional(Schema.String),
});
const McpOAuthClientMetadataSchema = Schema.Struct({
  clientId: Schema.String,
  clientSecret: Schema.optional(Schema.String),
  authorizationEndpoint: Schema.String,
  tokenEndpoint: Schema.String,
  registrationEndpoint: Schema.optional(Schema.String),
});
const ProtectedResourceResponse = Schema.Struct({
  authorization_servers: Schema.optional(Schema.Array(Schema.String)),
  authorization_server: Schema.optional(Schema.String),
});
const AuthorizationServerResponse = Schema.Struct({
  authorization_endpoint: Schema.String,
  token_endpoint: Schema.String,
  registration_endpoint: Schema.optional(Schema.String),
});
const RegistrationResponse = Schema.Struct({
  client_id: Schema.String,
  client_secret: Schema.optional(Schema.String),
});
const TokenResponse = Schema.Struct({
  access_token: Schema.String,
  refresh_token: Schema.optional(Schema.String),
  expires_in: Schema.Number.pipe(Schema.nonNegative()),
  token_type: Schema.optional(Schema.String),
  scope: Schema.optional(Schema.String),
});

const StateRow = Schema.Struct({
  state_hash: Schema.String,
  organization_id: Schema.String,
  server_id: Schema.String,
  server_url: Schema.String,
  code_verifier: Schema.String,
  redirect_uri: Schema.String,
  client_json: Schema.String,
  expires_at: Schema.Number,
});
type StateRow = Schema.Schema.Type<typeof StateRow>;

const CredentialRow = Schema.Struct({
  server_url: Schema.String,
  client_json: Schema.String,
  access_token: Schema.String,
  refresh_token: Schema.NullOr(Schema.String),
  expires_at: Schema.Number,
  token_type: Schema.NullOr(Schema.String),
  scope: Schema.NullOr(Schema.String),
});
type CredentialRow = Schema.Schema.Type<typeof CredentialRow>;

const StatusRow = Schema.Struct({
  server_id: Schema.String,
  server_url: Schema.String,
  expires_at: Schema.Number,
});
type StatusRow = Schema.Schema.Type<typeof StatusRow>;

const hash = (value: string): string =>
  createHash("sha256").update(value).digest("base64url");

export const createPkceVerifier = (): string =>
  randomBytes(PKCE_BYTES).toString("base64url");

export const createPkceChallenge = (verifier: string): string => hash(verifier);

const callbackUri = (publicUrl: URL): string => {
  const base = publicUrl.toString().endsWith("/")
    ? publicUrl.toString()
    : `${publicUrl.toString()}/`;
  const value = new URL(base);
  value.pathname = `${value.pathname.replace(/\/$/u, "")}${MCP_OAUTH_CALLBACK_PATH}`;
  return value.toString();
};

const discoveryUrl = (base: URL, suffix: string): URL => {
  const value = new URL(base.toString());
  const path = value.pathname === "/" ? "" : value.pathname.replace(/\/$/u, "");
  value.pathname = `/.well-known/${suffix}${path}`;
  return value;
};

const validateEndpoint = (endpoint: string, base: URL): string => {
  const value = new URL(endpoint);
  if (value.protocol !== "https:")
    throw new Error("MCP OAuth endpoint must use HTTPS");
  if (value.username || value.password)
    throw new Error("MCP OAuth endpoint must not contain credentials");
  if (value.origin !== base.origin)
    throw new Error("MCP OAuth endpoint has an untrusted origin");
  return value.toString();
};

const fetchWithTimeout = async (
  fetchImpl: FetchLike,
  input: Parameters<FetchLike>[0],
  init?: Parameters<FetchLike>[1],
): Promise<Response> => {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    return await fetchImpl(input, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
};

const decodeJsonResponse = <A, I, R>(
  schema: Schema.Schema<A, I, R>,
  response: Response,
  operation: string,
): Effect.Effect<A, DatabaseError, R> =>
  Effect.tryPromise({
    try: () => response.json(),
    catch: (error) =>
      new DatabaseError({
        message: `${operation} response JSON failed: ${String(error)}`,
      }),
  }).pipe(
    Effect.flatMap((body) => Schema.decodeUnknown(schema)(body)),
    Effect.catchTag(
      "ParseError",
      (error) =>
        new DatabaseError({
          message: `${operation} response schema failed: ${String(error)}`,
        }),
    ),
  );

const decodeStoredJson = <A, I, R>(
  schema: Schema.Schema<A, I, R>,
  value: string,
  operation: string,
): Effect.Effect<A, DatabaseError, R> =>
  Effect.tryPromise({
    // Response.json performs JSON decoding without bypassing Effect's error channel.
    try: () => new Response(value).json(),
    catch: (error) =>
      new DatabaseError({
        message: `${operation} JSON failed: ${String(error)}`,
      }),
  }).pipe(
    Effect.flatMap((body) => Schema.decodeUnknown(schema)(body)),
    Effect.catchTag(
      "ParseError",
      (error) =>
        new DatabaseError({
          message: `${operation} schema failed: ${String(error)}`,
        }),
    ),
  );

const discoverMcpOAuthMetadataEffect = (
  serverUrl: string,
  fetchImpl: FetchLike,
): Effect.Effect<McpOAuthMetadata, DatabaseError | McpOAuthError> =>
  Effect.gen(function* () {
    const base = new URL(serverUrl);
    const protectedResource = new URL(
      discoveryUrl(base, "oauth-protected-resource").pathname,
      base,
    );
    const resourceResponse = yield* Effect.tryPromise({
      try: () => fetchWithTimeout(fetchImpl, protectedResource),
      catch: (error) => new DatabaseError({ message: String(error) }),
    });
    if (!resourceResponse.ok) {
      return yield* Effect.fail(
        new DatabaseError({
          message: `MCP OAuth protected-resource discovery failed (${resourceResponse.status})`,
        }),
      );
    }
    const resource = yield* decodeJsonResponse(
      ProtectedResourceResponse,
      resourceResponse,
      "MCP OAuth protected-resource discovery",
    );
    const issuer =
      resource.authorization_servers?.[0] ??
      resource.authorization_server ??
      base.origin;
    let authServerUrl: URL;
    try {
      authServerUrl = new URL(issuer);
      if (
        authServerUrl.protocol !== "https:" ||
        authServerUrl.username ||
        authServerUrl.password
      )
        throw new Error(
          "MCP OAuth issuer must be HTTPS and must not contain credentials",
        );
    } catch (error) {
      return yield* Effect.fail(
        new McpOAuthError({
          message: String(error),
          reason: "endpoint_validation",
        }),
      );
    }
    const metadataUrl = new URL(
      discoveryUrl(authServerUrl, "oauth-authorization-server").pathname,
      authServerUrl,
    );
    const authResponse = yield* Effect.tryPromise({
      try: () => fetchWithTimeout(fetchImpl, metadataUrl),
      catch: (error) => new DatabaseError({ message: String(error) }),
    });
    if (!authResponse.ok) {
      return yield* Effect.fail(
        new DatabaseError({
          message: `MCP OAuth authorization-server discovery failed (${authResponse.status})`,
        }),
      );
    }
    const metadata = yield* decodeJsonResponse(
      AuthorizationServerResponse,
      authResponse,
      "MCP OAuth authorization-server discovery",
    );
    let authorizationEndpoint: string;
    let tokenEndpoint: string;
    try {
      authorizationEndpoint = validateEndpoint(
        metadata.authorization_endpoint,
        authServerUrl,
      );
      tokenEndpoint = validateEndpoint(metadata.token_endpoint, authServerUrl);
    } catch (error) {
      return yield* Effect.fail(
        new McpOAuthError({
          message: String(error),
          reason: "endpoint_validation",
        }),
      );
    }
    return {
      authorizationEndpoint,
      tokenEndpoint,
      ...(metadata.registration_endpoint !== undefined
        ? {
            registrationEndpoint: validateEndpoint(
              metadata.registration_endpoint,
              authServerUrl,
            ),
          }
        : {}),
    };
  });

export const discoverMcpOAuthMetadata = async (
  serverUrl: string,
  fetchImpl: FetchLike = fetch,
): Promise<McpOAuthMetadata> =>
  Effect.runPromise(discoverMcpOAuthMetadataEffect(serverUrl, fetchImpl));

export const buildMcpAuthorizeUrl = (input: {
  readonly client: McpOAuthClientMetadata;
  readonly redirectUri: string;
  readonly state: string;
  readonly codeVerifier: string;
  readonly scope?: string;
  readonly resource?: string;
}): URL => {
  const url = new URL(input.client.authorizationEndpoint);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("client_id", input.client.clientId);
  url.searchParams.set("redirect_uri", input.redirectUri);
  url.searchParams.set("state", input.state);
  url.searchParams.set(
    "code_challenge",
    createPkceChallenge(input.codeVerifier),
  );
  url.searchParams.set("code_challenge_method", "S256");
  if (input.scope) url.searchParams.set("scope", input.scope);
  if (input.resource) url.searchParams.set("resource", input.resource);
  return url;
};

const registerClient = (
  metadata: McpOAuthMetadata,
  redirectUri: string,
  fetchImpl: FetchLike,
): Effect.Effect<McpOAuthClientMetadata, DatabaseError> =>
  Effect.gen(function* () {
    if (!metadata.registrationEndpoint) {
      return yield* Effect.fail(
        new DatabaseError({
          message: "MCP OAuth server requires a pre-registered client",
        }),
      );
    }
    const response = yield* Effect.tryPromise({
      try: () =>
        fetchWithTimeout(fetchImpl, metadata.registrationEndpoint!, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            accept: "application/json",
          },
          body: JSON.stringify({
            redirect_uris: [redirectUri],
            token_endpoint_auth_method: "none",
            grant_types: ["authorization_code", "refresh_token"],
            response_types: ["code"],
            client_name: "Oh My Pi Linear Gateway",
          }),
        }),
      catch: (error) => new DatabaseError({ message: String(error) }),
    });
    if (!response.ok) {
      return yield* Effect.fail(
        new DatabaseError({
          message: `MCP OAuth client registration failed (${response.status})`,
        }),
      );
    }
    const body = yield* decodeJsonResponse(
      RegistrationResponse,
      response,
      "MCP OAuth client registration",
    );
    return {
      clientId: body.client_id,
      ...(body.client_secret !== undefined
        ? { clientSecret: body.client_secret }
        : {}),
      authorizationEndpoint: metadata.authorizationEndpoint,
      tokenEndpoint: metadata.tokenEndpoint,
      ...(metadata.registrationEndpoint !== undefined
        ? { registrationEndpoint: metadata.registrationEndpoint }
        : {}),
    };
  });

const redeemCode = (
  client: McpOAuthClientMetadata,
  code: string,
  redirectUri: string,
  verifier: string,
  fetchImpl: FetchLike,
  now: number,
  resource: string,
): Effect.Effect<McpOAuthToken, DatabaseError> =>
  Effect.gen(function* () {
    const params = new URLSearchParams({
      grant_type: "authorization_code",
      code,
      redirect_uri: redirectUri,
      client_id: client.clientId,
      code_verifier: verifier,
      resource,
    });
    if (client.clientSecret) params.set("client_secret", client.clientSecret);
    const response = yield* Effect.tryPromise({
      try: () =>
        fetchWithTimeout(fetchImpl, client.tokenEndpoint, {
          method: "POST",
          headers: {
            "content-type": "application/x-www-form-urlencoded",
            accept: "application/json",
          },
          body: params.toString(),
        }),
      catch: (error) => new DatabaseError({ message: String(error) }),
    });
    if (!response.ok) {
      return yield* Effect.fail(
        new DatabaseError({
          message: `MCP OAuth token redemption failed (${response.status})`,
        }),
      );
    }
    const body = yield* decodeJsonResponse(
      TokenResponse,
      response,
      "MCP OAuth token redemption",
    );
    return {
      accessToken: body.access_token,
      ...(body.refresh_token !== undefined
        ? { refreshToken: body.refresh_token }
        : {}),
      expiresAt: now + body.expires_in * 1000,
      ...(body.token_type !== undefined ? { tokenType: body.token_type } : {}),
      ...(body.scope !== undefined ? { scope: body.scope } : {}),
    };
  });

const refreshAccessToken = (
  client: McpOAuthClientMetadata,
  refreshToken: string,
  fetchImpl: FetchLike,
  now: number,
  resource: string,
): Effect.Effect<McpOAuthToken, DatabaseError> =>
  Effect.gen(function* () {
    const params = new URLSearchParams({
      grant_type: "refresh_token",
      refresh_token: refreshToken,
      client_id: client.clientId,
      resource,
    });
    if (client.clientSecret) params.set("client_secret", client.clientSecret);
    const response = yield* Effect.tryPromise({
      try: () =>
        fetchWithTimeout(fetchImpl, client.tokenEndpoint, {
          method: "POST",
          headers: {
            "content-type": "application/x-www-form-urlencoded",
            accept: "application/json",
          },
          body: params.toString(),
        }),
      catch: (error) => new DatabaseError({ message: String(error) }),
    });
    if (!response.ok) {
      return yield* Effect.fail(
        new DatabaseError({
          message: `MCP OAuth token refresh failed (${response.status})`,
        }),
      );
    }
    const body = yield* decodeJsonResponse(
      TokenResponse,
      response,
      "MCP OAuth token refresh",
    );
    return {
      accessToken: body.access_token,
      refreshToken: body.refresh_token ?? refreshToken,
      expiresAt: now + body.expires_in * 1000,
      ...(body.token_type !== undefined ? { tokenType: body.token_type } : {}),
      ...(body.scope !== undefined ? { scope: body.scope } : {}),
    };
  });

const encrypted = (value: string): string =>
  `${MCP_OAUTH_ENCRYPTED_PREFIX}${value}`;
const decryptValue = (
  crypto: TokenCrypto,
  value: string,
): Effect.Effect<string, TokenCipherError> =>
  value.startsWith(MCP_OAUTH_ENCRYPTED_PREFIX)
    ? crypto.decrypt(value.slice(MCP_OAUTH_ENCRYPTED_PREFIX.length))
    : Effect.succeed(value);

export class McpOAuth extends Effect.Service<McpOAuth>()("McpOAuth", {
  accessors: true,
  dependencies: [
    GatewayConfig.Default,
    McpServerRepo.Default,
    TokenCrypto.Default,
  ],
  effect: Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const mcpServerRepo = yield* McpServerRepo;
    const tokenCrypto = yield* TokenCrypto;
    const sqlite = yield* SqliteClient;
    const { db } = sqlite;
    const refreshState = yield* Ref.make(
      new Map<
        string,
        Deferred.Deferred<McpOAuthToken, DatabaseError | TokenCipherError>
      >(),
    );

    const startMcpAuthorization = Effect.fn("McpOAuth.startMcpAuthorization")(
      function* (
        organizationId: OrganizationId,
        serverId: McpServerId,
        preRegisteredClient?: {
          readonly clientId: string;
          readonly clientSecret?: string;
          readonly scope?: string;
        },
      ) {
        const server = yield* mcpServerRepo.getMcpServer(
          organizationId,
          serverId,
        );
        if (
          Option.isNone(server) ||
          Option.isNone(server.value.url) ||
          server.value.transport === "stdio"
        ) {
          return yield* Effect.fail(
            new DatabaseError({
              message: "MCP server is not an OAuth-capable HTTP/SSE server",
            }),
          );
        }
        const serverUrl = server.value.url.value;
        const redirectUri = callbackUri(config.publicUrl);
        const discovered = yield* discoverMcpOAuthMetadataEffect(
          serverUrl,
          fetch,
        );
        const client = preRegisteredClient
          ? {
              ...preRegisteredClient,
              authorizationEndpoint: discovered.authorizationEndpoint,
              tokenEndpoint: discovered.tokenEndpoint,
              ...(discovered.registrationEndpoint
                ? { registrationEndpoint: discovered.registrationEndpoint }
                : {}),
            }
          : yield* registerClient(discovered, redirectUri, fetch);
        const state = randomBytes(32).toString("base64url");
        const verifier = createPkceVerifier();
        const now = yield* Clock.currentTimeMillis;
        const encryptedClient = yield* tokenCrypto.encrypt(
          JSON.stringify(client),
        );
        const encryptedVerifier = yield* tokenCrypto.encrypt(verifier);
        yield* tryDb(
          () =>
            db
              .query(
                "DELETE FROM mcp_oauth_state WHERE consumed_at IS NOT NULL OR expires_at < ?",
              )
              .run(now),
          "McpOAuth.startMcpAuthorization.prune",
        );
        yield* tryDb(
          () =>
            db
              .query(
                "INSERT INTO mcp_oauth_state (state_hash, organization_id, server_id, server_url, code_verifier, redirect_uri, client_json, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
              )
              .run(
                hash(state),
                organizationId,
                serverId,
                serverUrl,
                encrypted(encryptedVerifier),
                redirectUri,
                encrypted(encryptedClient),
                now + MCP_OAUTH_STATE_TTL_MS,
              ),
          "McpOAuth.startMcpAuthorization",
        );
        return {
          state,
          url: buildMcpAuthorizeUrl({
            client,
            redirectUri,
            state,
            codeVerifier: verifier,
            ...(preRegisteredClient?.scope
              ? { scope: preRegisteredClient.scope }
              : {}),
            resource: serverUrl,
          }),
        };
      },
    );

    const completeMcpAuthorization = Effect.fn(
      "McpOAuth.completeMcpAuthorization",
    )(function* (callbackUrl: URL) {
      const code = callbackUrl.searchParams.get("code");
      const state = callbackUrl.searchParams.get("state");
      if (!code || !state) {
        return yield* Effect.fail(
          new OAuthStateError({ message: "Missing MCP OAuth code or state" }),
        );
      }
      const now = yield* Clock.currentTimeMillis;
      const rawRow = yield* tryDb(
        () =>
          db
            .query<StateRow, [string, number]>(
              "SELECT state_hash, organization_id, server_id, server_url, code_verifier, redirect_uri, client_json, expires_at FROM mcp_oauth_state WHERE state_hash = ? AND consumed_at IS NULL AND expires_at >= ?",
            )
            .get(hash(state), now),
        "McpOAuth.completeMcpAuthorization.state",
      );
      if (rawRow === null) {
        return yield* Effect.fail(
          new OAuthStateError({
            message: "Invalid or expired MCP OAuth state",
          }),
        );
      }
      const row = yield* decodeRow(StateRow, rawRow, "McpOAuthState");
      const client = yield* decodeStoredJson(
        McpOAuthClientMetadataSchema,
        yield* decryptValue(tokenCrypto, row.client_json),
        "McpOAuth.completeMcpAuthorization.client",
      );
      const verifier = yield* decryptValue(tokenCrypto, row.code_verifier);
      yield* transact(
        db,
        Effect.gen(function* () {
          const consumed = yield* tryDb(
            () =>
              db
                .query(
                  "UPDATE mcp_oauth_state SET consumed_at = ? WHERE state_hash = ? AND consumed_at IS NULL AND expires_at >= ?",
                )
                .run(now, hash(state), now),
            "McpOAuth.completeMcpAuthorization.consume",
          );
          if (
            (yield* runChanges(
              consumed,
              "McpOAuth.completeMcpAuthorization.consume",
            )) !== 1
          )
            return yield* Effect.fail(
              new OAuthStateError({
                message: "Invalid or expired MCP OAuth state",
              }),
            );
          yield* tryDb(
            () =>
              db
                .query(
                  "DELETE FROM mcp_oauth_state WHERE consumed_at IS NOT NULL OR expires_at < ?",
                )
                .run(now),
            "McpOAuth.completeMcpAuthorization.prune",
          );
        }),
      );
      const token = yield* redeemCode(
        client,
        code,
        row.redirect_uri,
        verifier,
        fetch,
        now,
        row.server_url,
      );
      const accessToken = yield* tokenCrypto.encrypt(token.accessToken);
      const refreshToken = token.refreshToken
        ? yield* tokenCrypto.encrypt(token.refreshToken)
        : null;

      const result = yield* transact(
        db,
        Effect.gen(function* () {
          yield* tryDb(
            () =>
              db
                .query(
                  "INSERT INTO mcp_oauth_credential (organization_id, server_id, server_url, client_json, access_token, refresh_token, expires_at, token_type, scope, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(organization_id, server_id) DO UPDATE SET server_url=excluded.server_url, client_json=excluded.client_json, access_token=excluded.access_token, refresh_token=excluded.refresh_token, expires_at=excluded.expires_at, token_type=excluded.token_type, scope=excluded.scope, updated_at=excluded.updated_at",
                )
                .run(
                  row.organization_id,
                  row.server_id,
                  row.server_url,
                  row.client_json,
                  encrypted(accessToken),
                  refreshToken ? encrypted(refreshToken) : null,
                  token.expiresAt,
                  token.tokenType ?? null,
                  token.scope ?? null,
                  now,
                  now,
                ),
            "McpOAuth.completeMcpAuthorization.credential",
          );
          return {
            organizationId: row.organization_id as OrganizationId,
            serverId: row.server_id as McpServerId,
            expiresAt: token.expiresAt,
          };
        }),
      );
      return result;
    });

    const disconnect = Effect.fn("McpOAuth.disconnect")(
      (organizationId: OrganizationId, serverId: McpServerId) =>
        tryDb(
          () =>
            db
              .query(
                "DELETE FROM mcp_oauth_credential WHERE organization_id = ? AND server_id = ?",
              )
              .run(organizationId, serverId),
          "McpOAuth.disconnect",
        ).pipe(Effect.asVoid),
    );

    const getCredentialDetails = Effect.fn("McpOAuth.getCredentialDetails")(
      function* (organizationId: OrganizationId, serverId: McpServerId) {
        const server = yield* mcpServerRepo.getMcpServer(
          organizationId,
          serverId,
        );
        if (Option.isNone(server) || Option.isNone(server.value.url))
          return Option.none<McpOAuthCredential>();
        const serverUrl = server.value.url.value;
        const row = yield* tryDb(
          () =>
            db
              .query<CredentialRow, [string, string, string]>(
                "SELECT server_url, client_json, access_token, refresh_token, expires_at, token_type, scope FROM mcp_oauth_credential WHERE organization_id = ? AND server_id = ? AND server_url = ?",
              )
              .get(organizationId, serverId, serverUrl),
          "McpOAuth.getCredentialDetails",
        );
        if (row === null) return Option.none<McpOAuthCredential>();
        const decoded = yield* decodeRow(
          CredentialRow,
          row,
          "McpOAuthCredential",
        );
        const accessToken = yield* decryptValue(
          tokenCrypto,
          decoded.access_token,
        );
        const refreshToken = decoded.refresh_token
          ? yield* decryptValue(tokenCrypto, decoded.refresh_token)
          : undefined;
        const token: McpOAuthToken = {
          accessToken,
          expiresAt: decoded.expires_at,
          ...(refreshToken ? { refreshToken } : {}),
          ...(decoded.token_type ? { tokenType: decoded.token_type } : {}),
          ...(decoded.scope ? { scope: decoded.scope } : {}),
        };
        const client = yield* decodeStoredJson(
          McpOAuthClientMetadataSchema,
          yield* decryptValue(tokenCrypto, decoded.client_json),
          "McpOAuth.getCredentialDetails.client",
        );
        return Option.some({ serverUrl: decoded.server_url, client, token });
      },
    );

    const getCredential = Effect.fn("McpOAuth.getCredential")(function* (
      organizationId: OrganizationId,
      serverId: McpServerId,
    ) {
      const details = yield* getCredentialDetails(organizationId, serverId);
      return Option.map(details, (value) => value.token);
    });

    const persistRefreshedToken = (
      organizationId: OrganizationId,
      serverId: McpServerId,
      token: McpOAuthToken,
      now: number,
    ): Effect.Effect<void, DatabaseError | TokenCipherError> =>
      Effect.gen(function* () {
        const accessToken = yield* tokenCrypto.encrypt(token.accessToken);
        const refreshToken = token.refreshToken
          ? yield* tokenCrypto.encrypt(token.refreshToken)
          : null;
        yield* tryDb(
          () =>
            db
              .query(
                "UPDATE mcp_oauth_credential SET access_token = ?, refresh_token = ?, expires_at = ?, token_type = ?, scope = ?, updated_at = ? WHERE organization_id = ? AND server_id = ?",
              )
              .run(
                encrypted(accessToken),
                refreshToken ? encrypted(refreshToken) : null,
                token.expiresAt,
                token.tokenType ?? null,
                token.scope ?? null,
                now,
                organizationId,
                serverId,
              ),
          "McpOAuth.persistRefreshedToken",
        );
      });

    const refreshTokens = Effect.fn("McpOAuth.refreshTokens")(function* (
      organizationId: OrganizationId,
      serverId: McpServerId,
      current: McpOAuthCredential,
      now: number,
    ): Effect.fn.Return<McpOAuthToken, DatabaseError | TokenCipherError> {
      const key = `${organizationId}:${serverId}`;
      const mine = yield* Deferred.make<
        McpOAuthToken,
        DatabaseError | TokenCipherError
      >();
      const claim = yield* Ref.modify(refreshState, (flights) => {
        const existing = flights.get(key);
        if (existing) return [Option.some(existing), flights] as const;
        const next = new Map(flights);
        next.set(key, mine);
        return [
          Option.none<
            Deferred.Deferred<McpOAuthToken, DatabaseError | TokenCipherError>
          >(),
          next,
        ] as const;
      });
      if (Option.isSome(claim)) return yield* Deferred.await(claim.value);
      const refreshed = yield* refreshAccessToken(
        current.client,
        current.token.refreshToken!,
        fetch,
        now,
        current.serverUrl,
      ).pipe(
        Effect.tap((token) =>
          persistRefreshedToken(organizationId, serverId, token, now),
        ),
        Effect.tap((token) => Deferred.succeed(mine, token)),
        Effect.tapError((error) => Deferred.fail(mine, error)),
        Effect.onExit((exit) => Deferred.done(mine, exit)),
        Effect.ensuring(
          Ref.update(refreshState, (flights) => {
            const next = new Map(flights);
            next.delete(key);
            return next;
          }),
        ),
      );
      return refreshed;
    });

    const mintCredential = Effect.fn("McpOAuth.mintCredential")(function* (
      organizationId: OrganizationId,
      serverId: McpServerId,
    ) {
      const details = yield* getCredentialDetails(organizationId, serverId);
      if (Option.isNone(details)) return Option.none<McpOAuthToken>();
      const now = yield* Clock.currentTimeMillis;
      const current = details.value;
      if (current.token.expiresAt > now + 60_000)
        return Option.some(current.token);
      if (current.token.refreshToken === undefined) {
        return yield* Effect.fail(
          new TokenRefreshError({
            organizationId,
            message:
              "MCP OAuth access token expired and no refresh token is available",
          }),
        );
      }
      const token = yield* refreshTokens(
        organizationId,
        serverId,
        current,
        now,
      );
      return Option.some(token);
    });

    const listStatuses = Effect.fn("McpOAuth.listStatuses")(function* (
      organizationId: OrganizationId,
    ) {
      const now = yield* Clock.currentTimeMillis;
      const rows = yield* tryDb(
        () =>
          db
            .query<StatusRow, [string]>(
              "SELECT server_id, server_url, expires_at FROM mcp_oauth_credential WHERE organization_id = ?",
            )
            .all(organizationId),
        "McpOAuth.listStatuses",
      );
      const decoded = yield* decodeRows(StatusRow, rows, "McpOAuthStatus");
      return new Map(
        decoded.map((row) => [
          row.server_id,
          {
            connected: true,
            serverUrl: row.server_url,
            expired: row.expires_at <= now,
            expiresAt: row.expires_at,
          },
        ]),
      );
    });

    return {
      startMcpAuthorization,
      completeMcpAuthorization,
      disconnect,
      getCredential,
      getCredentialDetails,
      mintCredential,
      listStatuses,
    };
  }),
}) {}
const lstatIfExists = async (path: string): Promise<Stats | undefined> => {
  try {
    return await lstat(path);
  } catch (error) {
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      error.code === "ENOENT"
    ) {
      return undefined;
    }
    throw error;
  }
};

export const materializeMcpAgentDb = async (
  workspace: string,
  credentials: ReadonlyArray<{
    readonly serverUrl: string;
    readonly accessToken: string;
    readonly expiresAt: number;
  }>,
): Promise<string> => {
  const agentDir = join(workspace, ".omp-gateway");
  const path = join(agentDir, "agent.db");
  const existingDir = await lstatIfExists(agentDir);
  if (existingDir !== undefined && !existingDir.isDirectory()) {
    throw new Error("MCP OAuth agent directory must be a real directory");
  }
  await mkdir(agentDir, { recursive: true, mode: 0o700 });
  await chmod(agentDir, 0o700);
  const existingPath = await lstatIfExists(path);
  if (existingPath?.isSymbolicLink()) {
    throw new Error("MCP OAuth agent database must not be a symlink");
  }
  if (existingPath !== undefined && !existingPath.isFile()) {
    throw new Error("MCP OAuth agent database must be a regular file");
  }
  await rm(path, { force: true });
  let db: Database | undefined;
  try {
    db = new Database(path);
    await Effect.runPromise(
      tryDb(() => {
        db!.exec(
          `CREATE TABLE IF NOT EXISTS auth_credentials (id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, credential_type TEXT NOT NULL, data TEXT NOT NULL, disabled_cause TEXT DEFAULT NULL, identity_key TEXT DEFAULT NULL, created_at INTEGER NOT NULL DEFAULT (unixepoch()), updated_at INTEGER NOT NULL DEFAULT (unixepoch())); CREATE INDEX IF NOT EXISTS idx_auth_provider ON auth_credentials(provider); CREATE INDEX IF NOT EXISTS idx_auth_provider_identity ON auth_credentials(provider, identity_key) WHERE identity_key IS NOT NULL;`,
        );
        const insert = db!.query(
          "INSERT INTO auth_credentials (provider, credential_type, data, identity_key) VALUES (?, 'oauth', ?, NULL)",
        );
        for (const credential of credentials) {
          insert.run(
            `${MCP_OAUTH_PROVIDER_PREFIX}${credential.serverUrl}`,
            JSON.stringify({
              access: credential.accessToken,
              expires: credential.expiresAt,
            }),
          );
        }
      }, "McpOAuth.materializeMcpAgentDb"),
    );
    await chmod(path, 0o600);
    const gitPath = join(workspace, ".git");
    let gitDir = gitPath;
    if (await Bun.file(gitPath).exists()) {
      const gitEntry = await Bun.file(gitPath).text();
      if (gitEntry.startsWith("gitdir:")) {
        const worktreeGitDir = gitEntry.slice("gitdir:".length).trim();
        gitDir = isAbsolute(worktreeGitDir)
          ? worktreeGitDir
          : resolvePath(workspace, worktreeGitDir);
        const commonDirFile = join(gitDir, "commondir");
        if (await Bun.file(commonDirFile).exists()) {
          const commonDir = (await Bun.file(commonDirFile).text()).trim();
          gitDir = isAbsolute(commonDir)
            ? commonDir
            : resolvePath(gitDir, commonDir);
        }
      }
    }
    if (await Bun.file(join(gitDir, "HEAD")).exists()) {
      const infoDir = join(gitDir, "info");
      await mkdir(infoDir, { recursive: true });
      const excludePath = join(infoDir, "exclude");
      const existing = (await Bun.file(excludePath).exists())
        ? await Bun.file(excludePath).text()
        : "";
      if (
        !existing.split("\n").some((line) => line.trim() === ".omp-gateway/")
      ) {
        const separator =
          existing.length > 0 && !existing.endsWith("\n") ? "\n" : "";
        await writeFile(
          excludePath,
          `${existing}${separator}.omp-gateway/\n`,
          "utf8",
        );
      }
    }
    return agentDir;
  } catch (error) {
    try {
      await unlink(path);
    } catch {
      // Partial materialization cleanup is intentionally best effort.
    }
    throw error;
  } finally {
    db?.close();
  }
};

export const removeMcpAgentDb = (
  workspace: string,
): Effect.Effect<void, never, never> =>
  Effect.promise(async () => {
    try {
      await unlink(join(workspace, ".omp-gateway", "agent.db"));
    } catch {}
  });

export const mcpProviderForServerUrl = (serverUrl: string): string =>
  `${MCP_OAUTH_PROVIDER_PREFIX}${serverUrl}`;
