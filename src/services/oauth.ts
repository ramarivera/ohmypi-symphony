import { createHash, randomBytes } from "node:crypto";
import { Effect, Redacted, Schema } from "effect";
import { LinearApiError, OAuthStateError } from "../domain/errors.js";
import {
  Installation,
  type Installation as InstallationType,
} from "../domain/models.js";
import { GatewayConfig } from "./config.js";
import {
  buildInstallationRecord,
  discoverAppInstallation,
  parseTokenResponse,
  type TokenResponse,
} from "./linear-oauth.js";
import { InstallationRepo } from "./store/repositories.js";

const DEFAULT_AUTHORIZE_REDIRECT_PATH = "oauth/callback";
const DEFAULT_STATE_TTL_MS = 10 * 60 * 1000;
const AUTHORIZE_URL = "https://linear.app/oauth/authorize";
const DEFAULT_SCOPES: readonly string[] = [
  "read",
  "write",
  "app:assignable",
  "app:mentionable",
];

const LINEAR_TOKEN_URL = "https://api.linear.app/oauth/token";

const hashState = (rawState: string): string =>
  createHash("sha256").update(rawState).digest("base64url");

const normalizeRedirectPath = (path: string): string =>
  path.replace(/^\/+/u, "");

const buildRedirectUri = (config: GatewayConfig, path: string): string => {
  const base = new URL(config.publicUrl.toString().replace(/\/?$/u, "/"));
  const normalized = normalizeRedirectPath(path);
  return new URL(normalized, base).toString();
};

const exchangeAuthorizationCode = (
  config: GatewayConfig,
  code: string,
  redirectUri: string,
): Effect.Effect<TokenResponse, LinearApiError> =>
  Effect.tryPromise({
    try: async () => {
      const body = new URLSearchParams();
      body.set("client_id", config.linearClientId);
      body.set("client_secret", Redacted.value(config.linearClientSecret));
      body.set("grant_type", "authorization_code");
      body.set("code", code);
      body.set("redirect_uri", redirectUri);

      const response = await fetch(LINEAR_TOKEN_URL, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: body.toString(),
      });
      if (!response.ok) {
        const text = await response.text();
        throw new LinearApiError({
          message: `Linear token exchange failed: ${text}`,
          operation: "exchangeAuthorizationCode",
          status: response.status,
        });
      }
      const raw: unknown = await response.json();
      return parseTokenResponse(raw);
    },
    catch: (error) =>
      error instanceof LinearApiError
        ? error
        : new LinearApiError({
            message: String(error),
            operation: "exchangeAuthorizationCode",
          }),
  });

const discoverInstallation = (
  accessToken: string,
): Effect.Effect<
  { readonly organizationId: string; readonly appUserId: string },
  LinearApiError
> =>
  Effect.tryPromise({
    try: () => discoverAppInstallation(accessToken),
    catch: (error) =>
      new LinearApiError({
        message: String(error),
        operation: "discoverAppInstallation",
      }),
  });

const buildInstallation = (
  token: TokenResponse,
  organizationId: string,
  appUserId: string,
  now: number,
): Effect.Effect<InstallationType, LinearApiError> => {
  const record = buildInstallationRecord(token, organizationId, appUserId, now);
  return Schema.decodeUnknown(Installation)(record).pipe(
    Effect.mapError(
      (error) =>
        new LinearApiError({
          message: `Invalid installation record: ${error}`,
          operation: "buildInstallation",
        }),
    ),
  );
};

export class OAuth extends Effect.Service<OAuth>()("OAuth", {
  accessors: true,
  dependencies: [GatewayConfig.Default, InstallationRepo.Default],
  effect: Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const installationRepo = yield* InstallationRepo;

    const startAuthorization = Effect.fn("OAuth.startAuthorization")(
      function* () {
        const state = yield* Effect.sync(() =>
          randomBytes(32).toString("base64url"),
        );
        const stateHash = hashState(state);
        const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
        const expiresAt = now + DEFAULT_STATE_TTL_MS;

        yield* Effect.logInfo("oauth.startAuthorization", {
          stateHash,
          expiresAt,
        });

        yield* installationRepo.createOAuthState(stateHash, expiresAt, now);

        const redirectUri = buildRedirectUri(
          config,
          DEFAULT_AUTHORIZE_REDIRECT_PATH,
        );
        const url = new URL(AUTHORIZE_URL);
        url.searchParams.set("response_type", "code");
        url.searchParams.set("client_id", config.linearClientId);
        url.searchParams.set("redirect_uri", redirectUri);
        url.searchParams.set("state", state);
        url.searchParams.set("scope", DEFAULT_SCOPES.join(","));
        url.searchParams.set("actor", "app");
        url.searchParams.set("prompt", "consent");

        return { state, url };
      },
    );

    const completeAuthorization = Effect.fn("OAuth.completeAuthorization")(
      function* (callbackUrl: URL) {
        const code = callbackUrl.searchParams.get("code");
        const state = callbackUrl.searchParams.get("state");
        if (!code || !state) {
          return yield* Effect.fail(
            new OAuthStateError({
              message: "Missing OAuth code or state",
            }),
          );
        }

        const stateHash = hashState(state);
        const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);

        const consumed = yield* installationRepo
          .consumeOAuthState(stateHash, now)
          .pipe(
            Effect.mapError(
              () =>
                new OAuthStateError({
                  message: "Invalid or expired OAuth state",
                }),
            ),
          );
        if (!consumed) {
          return yield* Effect.fail(
            new OAuthStateError({
              message: "Invalid or expired OAuth state",
            }),
          );
        }

        const redirectUri = buildRedirectUri(
          config,
          DEFAULT_AUTHORIZE_REDIRECT_PATH,
        );

        const token = yield* exchangeAuthorizationCode(
          config,
          code,
          redirectUri,
        );
        const { organizationId, appUserId } = yield* discoverInstallation(
          token.accessToken,
        );

        const installation = yield* buildInstallation(
          token,
          organizationId,
          appUserId,
          now,
        );

        yield* Effect.logInfo("oauth.completeAuthorization", {
          organizationId: installation.organizationId,
          appUserId: installation.appUserId,
          scopes: installation.scopes,
        });

        yield* installationRepo.put(installation);

        return installation;
      },
    );

    return { startAuthorization, completeAuthorization };
  }),
}) {}
