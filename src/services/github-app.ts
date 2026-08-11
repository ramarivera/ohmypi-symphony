import { createSign } from "node:crypto";
import { Deferred, Effect, FiberId, Redacted, Schema } from "effect";
import { GatewayConfig } from "./config.js";

const GITHUB_API = "https://api.github.com";
const TOKEN_LIFETIME_MS = 60 * 60_000;
const TOKEN_REFRESH_MARGIN_MS = 10 * 60_000;
const TOKEN_CACHE_MAX_ENTRIES = 100;
const JWT_LIFETIME_SECONDS = 10 * 60;

export type GitHubFetch = (
  url: string | URL,
  init?: RequestInit,
) => Promise<Response>;

export class GitHubAppError extends Schema.TaggedError<GitHubAppError>()(
  "@Gateway/GitHubAppError",
  {
    message: Schema.String,
    operation: Schema.String,
    reason: Schema.Literal(
      "disabled",
      "invalid_repository",
      "jwt",
      "installation_lookup",
      "token_request",
      "invalid_response",
    ),
    status: Schema.optional(Schema.Number),
  },
) {}

export interface GitHubAppTokenService {
  readonly getInstallationToken: (
    owner: string,
    repository: string,
  ) => Effect.Effect<string, GitHubAppError>;
}

export const buildGitHubAppJwt = (
  appId: string,
  privateKey: string,
  now = Date.now(),
): string => {
  const issuedAt = Math.floor(now / 1_000) - 60;
  const expiresAt = issuedAt + JWT_LIFETIME_SECONDS;
  const encode = (value: unknown) =>
    Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
  const header = encode({ alg: "RS256", typ: "JWT" });
  const payload = encode({ iat: issuedAt, exp: expiresAt, iss: appId });
  const signer = createSign("RSA-SHA256");
  signer.update(`${header}.${payload}`);
  signer.end();

  const signature = signer.sign(privateKey).toString("base64url");
  return `${header}.${payload}.${signature}`;
};

export const buildGitHubExtraHeader = (token: string): string =>
  `AUTHORIZATION: basic ${Buffer.from(
    `x-access-token:${token}`,
    "utf8",
  ).toString("base64")}`;

const responseError = (
  operation: string,
  reason: GitHubAppError["reason"],
  response: Response,
) =>
  new GitHubAppError({
    message: `GitHub API ${operation} failed with status ${response.status}`,
    operation,
    reason,
    status: response.status,
  });

const responseJson = (
  fetchImpl: GitHubFetch,
  operation: string,
  reason: GitHubAppError["reason"],
  url: string,
  init: RequestInit,
) =>
  Effect.tryPromise({
    try: async () => {
      const response = await fetchImpl(url, init);
      if (!response.ok) throw responseError(operation, reason, response);
      return (await response.json()) as unknown;
    },
    catch: (error) =>
      error instanceof GitHubAppError
        ? error
        : new GitHubAppError({
            message: `GitHub API ${operation} request failed`,
            operation,
            reason,
          }),
  });

export const makeGitHubApp = (input: {
  readonly appId: string | undefined;
  readonly privateKey: string | undefined;
  readonly fetch?: GitHubFetch;
  readonly now?: () => number;
}): GitHubAppTokenService => {
  const fetchImpl =
    input.fetch ??
    ((url: string | URL, init?: RequestInit) => fetch(url, init));
  const now = input.now ?? Date.now;
  const cache = new Map<
    string,
    { readonly token: string; readonly expiresAt: number }
  >();
  const inFlight = new Map<string, Deferred.Deferred<string, GitHubAppError>>();

  const getInstallationToken = Effect.fn("GitHubApp.getInstallationToken")(
    function* (
      owner: string,
      repository: string,
    ): Effect.fn.Return<string, GitHubAppError> {
      if (input.appId === undefined || input.privateKey === undefined) {
        return yield* Effect.fail(
          new GitHubAppError({
            message: "GitHub App credentials are not configured",
            operation: "getInstallationToken",
            reason: "disabled",
          }),
        );
      }
      if (
        !/^[A-Za-z0-9_.-]+$/u.test(owner) ||
        !/^[A-Za-z0-9_.-]+$/u.test(repository)
      ) {
        return yield* Effect.fail(
          new GitHubAppError({
            message: "GitHub repository name is invalid",
            operation: "getInstallationToken",
            reason: "invalid_repository",
          }),
        );
      }

      const key = `${owner}/${repository}`;
      const timestamp = now();
      for (const [cachedKey, entry] of cache) {
        if (timestamp >= entry.expiresAt) cache.delete(cachedKey);
      }
      const cached = cache.get(key);
      if (
        cached !== undefined &&
        timestamp < cached.expiresAt - TOKEN_REFRESH_MARGIN_MS
      ) {
        return cached.token;
      }

      const existing = inFlight.get(key);
      if (existing !== undefined) return yield* Deferred.await(existing);

      const deferred = Deferred.unsafeMake<string, GitHubAppError>(
        FiberId.none,
      );
      inFlight.set(key, deferred);
      const mint = Effect.gen(function* () {
        const jwt = yield* Effect.try({
          try: () =>
            buildGitHubAppJwt(
              input.appId as string,
              input.privateKey as string,
              timestamp,
            ),
          catch: () =>
            new GitHubAppError({
              message: "Could not sign GitHub App JWT",
              operation: "getInstallationToken",
              reason: "jwt",
            }),
        });
        const headers = {
          Accept: "application/vnd.github+json",
          Authorization: `Bearer ${jwt}`,
          "X-GitHub-Api-Version": "2022-11-28",
        };
        const encodedOwner = encodeURIComponent(owner);
        const encodedRepository = encodeURIComponent(repository);
        const installation = yield* responseJson(
          fetchImpl,
          "installation_lookup",
          "installation_lookup",
          `${GITHUB_API}/repos/${encodedOwner}/${encodedRepository}/installation`,
          { headers },
        );
        if (
          typeof installation !== "object" ||
          installation === null ||
          typeof (installation as { id?: unknown }).id !== "number"
        ) {
          return yield* Effect.fail(
            new GitHubAppError({
              message: "GitHub installation response was invalid",
              operation: "installation_lookup",
              reason: "invalid_response",
            }),
          );
        }
        const tokenResponse = yield* responseJson(
          fetchImpl,
          "token_request",
          "token_request",
          `${GITHUB_API}/app/installations/${(installation as { id: number }).id}/access_tokens`,
          {
            method: "POST",
            headers: { ...headers, "Content-Type": "application/json" },
            body: JSON.stringify({ repositories: [repository] }),
          },
        );
        if (
          typeof tokenResponse !== "object" ||
          tokenResponse === null ||
          typeof (tokenResponse as { token?: unknown }).token !== "string"
        ) {
          return yield* Effect.fail(
            new GitHubAppError({
              message: "GitHub access token response was invalid",
              operation: "token_request",
              reason: "invalid_response",
            }),
          );
        }
        const expiresAtValue = (tokenResponse as { expires_at?: unknown })
          .expires_at;
        const expiresAt =
          typeof expiresAtValue === "string" &&
          Number.isFinite(Date.parse(expiresAtValue))
            ? Math.min(
                Date.parse(expiresAtValue),
                timestamp + TOKEN_LIFETIME_MS,
              )
            : timestamp + TOKEN_LIFETIME_MS;
        const token = (tokenResponse as { token: string }).token;
        cache.delete(key);
        if (cache.size >= TOKEN_CACHE_MAX_ENTRIES) {
          let oldestKey: string | undefined;
          let oldestExpiresAt = Number.POSITIVE_INFINITY;
          for (const [cachedKey, entry] of cache) {
            if (entry.expiresAt < oldestExpiresAt) {
              oldestKey = cachedKey;
              oldestExpiresAt = entry.expiresAt;
            }
          }
          if (oldestKey !== undefined) cache.delete(oldestKey);
        }
        cache.set(key, { token, expiresAt });
        return token;
      });
      return yield* mint.pipe(
        Effect.tap((token) => Deferred.succeed(deferred, token)),
        Effect.tapError((error) => Deferred.fail(deferred, error)),
        Effect.ensuring(
          Effect.sync(() => {
            if (inFlight.get(key) === deferred) inFlight.delete(key);
          }),
        ),
      );
    },
  );

  return { getInstallationToken };
};

export class GitHubApp extends Effect.Service<GitHubApp>()("GitHubApp", {
  accessors: true,
  dependencies: [GatewayConfig.Default],
  effect: Effect.gen(function* () {
    const config = yield* GatewayConfig;
    return makeGitHubApp({
      appId: config.githubAppId,
      privateKey:
        config.githubAppPrivateKey === undefined
          ? undefined
          : Redacted.value(config.githubAppPrivateKey),
    });
  }),
}) {}
