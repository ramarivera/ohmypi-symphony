import { createHash } from "node:crypto";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Either, Layer, Option, Redacted, Schema } from "effect";
import { afterEach, beforeEach, vi } from "vitest";
import { DatabaseError } from "../src/domain/errors.js";
import type { Installation } from "../src/domain/models.js";
import type { GatewayConfigShape } from "../src/services/config.js";
import { GatewayConfig } from "../src/services/config.js";
import { OAuth } from "../src/services/oauth.js";
import { InstallationRepo } from "../src/services/store/repositories.js";
import { TokenCrypto } from "../src/services/token-crypto.js";

const linearTestDouble = vi.hoisted(() => ({
  viewer: {
    id: "app-user",
    app: {} as object | null,
    organization: Promise.resolve({ id: "org" }),
  },
}));

vi.mock("@linear/sdk", () => ({
  AgentActivitySignal: {
    Auth: "auth",
    Continue: "continue",
    Select: "select",
    Stop: "stop",
  },
  LinearClient: class {
    readonly viewer = linearTestDouble.viewer;
  },
}));

vi.mock("../src/services/store/repositories.js", async () => {
  // Vitest mock factories are hoisted before static imports resolve this module.
  const { Effect } = await import("effect");

  class InstallationRepo extends Effect.Service<InstallationRepo>()(
    "InstallationRepo",
    {
      accessors: true,
      effect: Effect.succeed({}),
    },
  ) {}

  return { InstallationRepo };
});

const testConfig: GatewayConfigShape = {
  linearClientId: "client-id",
  linearClientSecret: Redacted.make("client-secret"),
  linearWebhookSecret: Redacted.make("webhook-secret"),
  tokenEncryptionKey: Redacted.make("test-token-key"),
  publicUrl: new URL("https://gateway.example/base/"),
  databasePath: ":memory:",
  nixBinaryPath: "nix",
  nixpkgsFlakeRef:
    "github:NixOS/nixpkgs/0123456789abcdef0123456789abcdef01234567",
  nixRootsDir: "/tmp/nix-roots",
  nixGcMaxBytes: 1_000_000,
  workspaceRoot: "/workspaces",
  ompCliPath: "omp",
  port: 3000,
  leaseDurationMs: 60_000,
  reconcilerIntervalMs: 1_000,
  webhookReplayWindowMs: 60_000,
  logLevel: "info",
  logFile: Option.none(),
};

interface StoredOAuthState {
  readonly expiresAt: number;
  consumed: boolean;
}

const testState = {
  failCreateOAuthState: false,
  installations: [] as Array<Installation>,
  oauthStates: new Map<string, StoredOAuthState>(),
};

const resetTestState = (): void => {
  testState.failCreateOAuthState = false;
  testState.installations.length = 0;
  testState.oauthStates.clear();
  linearTestDouble.viewer = {
    id: "app-user",
    app: {},
    organization: Promise.resolve({ id: "org" }),
  };
};

const installationRepo = InstallationRepo.make({
  put: (installation) =>
    Effect.sync(() => {
      testState.installations.push(installation);
    }),
  get: () => Effect.succeed(Option.none()),
  revoke: () => Effect.void,
  applyPermissionChange: () => Effect.succeed(false),
  getRawEncryptedAccessToken: () => Effect.succeed(Option.none()),
  createOAuthState: (hash, expiresAt, _now) => {
    if (testState.failCreateOAuthState) {
      return Effect.fail(
        new DatabaseError({ message: "OAuth state storage unavailable" }),
      );
    }
    return Effect.sync(() => {
      testState.oauthStates.set(hash, { expiresAt, consumed: false });
    });
  },
  consumeOAuthState: (hash, now) =>
    Effect.sync(() => {
      const state = testState.oauthStates.get(hash);
      if (state === undefined || state.consumed || state.expiresAt < now) {
        return false;
      }
      state.consumed = true;
      return true;
    }),
});

const tokenCrypto = TokenCrypto.make({
  encrypt: (plaintext) => Effect.succeed(plaintext),
  decrypt: (ciphertext) => Effect.succeed(ciphertext),
});

const oauthDependencies = Layer.mergeAll(
  Layer.succeed(GatewayConfig, GatewayConfig.make(testConfig)),
  Layer.succeed(InstallationRepo, installationRepo),
  Layer.succeed(TokenCrypto, tokenCrypto),
);

const OAuthTestLayer = Layer.mergeAll(
  OAuth.DefaultWithoutDependencies.pipe(Layer.provide(oauthDependencies)),
  oauthDependencies,
);

const startAuthorization = () =>
  Effect.gen(function* () {
    const oauth = yield* OAuth;
    return yield* oauth.startAuthorization();
  }).pipe(Effect.provide(OAuthTestLayer));

const completeAuthorization = (callbackUrl: URL) =>
  Effect.gen(function* () {
    const oauth = yield* OAuth;
    return yield* oauth.completeAuthorization(callbackUrl);
  }).pipe(Effect.provide(OAuthTestLayer));

const createAndConsumeState = (hash: string, expiresAt: number, now: number) =>
  Effect.gen(function* () {
    const repo = yield* InstallationRepo;
    yield* repo.createOAuthState(hash, expiresAt, now);
    return yield* repo.consumeOAuthState(hash, now);
  }).pipe(Effect.provide(oauthDependencies));

const consumeState = (hash: string, now: number) =>
  Effect.gen(function* () {
    const repo = yield* InstallationRepo;
    return yield* repo.consumeOAuthState(hash, now);
  }).pipe(Effect.provide(oauthDependencies));

const stateHash = (state: string): string =>
  createHash("sha256").update(state).digest("base64url");

const expectTaggedFailure = <A, E>(
  result: Either.Either<A, E>,
  expected: Record<string, unknown>,
): void => {
  if (Either.isLeft(result)) {
    expect(result.left).toMatchObject(expected);
    return;
  }
  expect.fail("Expected Effect failure");
};

const callbackUrl = (state: string): URL =>
  new URL(
    `https://gateway.example/base/oauth/callback?code=authorization-code&state=${encodeURIComponent(state)}`,
  );

const tokenResponse = (
  overrides: Record<string, unknown> = {},
): Record<string, unknown> => ({
  access_token: "access",
  refresh_token: "refresh",
  expires_in: 86_400,
  scope: "read,write app:assignable app:mentionable",
  token_type: "Bearer",
  ...overrides,
});

const fetchMock = vi.fn();

beforeEach(() => {
  resetTestState();
  fetchMock.mockReset();
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("OAuth", () => {
  it("starts an app-actor authorization with narrow scopes and a hashed ten-minute state", async () => {
    const before = Date.now();
    const authorization = await Effect.runPromise(startAuthorization());
    const after = Date.now();

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

    const stored = testState.oauthStates.get(stateHash(authorization.state));
    expect(stored).toBeDefined();
    expect(stored?.expiresAt).toBeGreaterThanOrEqual(before + 600_000);
    expect(stored?.expiresAt).toBeLessThanOrEqual(after + 600_000);
  });

  it("keeps state-storage infrastructure failures distinct from invalid callback state", async () => {
    testState.failCreateOAuthState = true;

    const storageFailure = await Effect.runPromise(
      Effect.either(startAuthorization()),
    );
    expectTaggedFailure(storageFailure, {
      _tag: "@Gateway/DatabaseError",
      message: "OAuth state storage unavailable",
    });

    const invalidState = await Effect.runPromise(
      Effect.either(
        completeAuthorization(
          new URL(
            "https://gateway.example/base/oauth/callback?code=code&state=unknown",
          ),
        ),
      ),
    );
    expectTaggedFailure(invalidState, {
      _tag: "@Gateway/OAuthStateError",
      message: "Invalid or expired OAuth state",
    });
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("rejects missing, expired, and reused callback states before Linear calls", async () => {
    const missingCode = await Effect.runPromise(
      Effect.either(
        completeAuthorization(
          new URL(
            "https://gateway.example/base/oauth/callback?state=missing-code",
          ),
        ),
      ),
    );
    expectTaggedFailure(missingCode, {
      _tag: "@Gateway/OAuthStateError",
      message: "Missing OAuth code or state",
    });

    const expiredState = "expired-state";
    const now = Date.now();
    testState.oauthStates.set(stateHash(expiredState), {
      expiresAt: now - 1,
      consumed: false,
    });
    const expired = await Effect.runPromise(
      Effect.either(completeAuthorization(callbackUrl(expiredState))),
    );
    expectTaggedFailure(expired, {
      _tag: "@Gateway/OAuthStateError",
      message: "Invalid or expired OAuth state",
    });

    fetchMock.mockResolvedValue(
      new Response(JSON.stringify(tokenResponse()), { status: 200 }),
    );
    const authorization = await Effect.runPromise(startAuthorization());
    await expect(
      Effect.runPromise(
        completeAuthorization(callbackUrl(authorization.state)),
      ),
    ).resolves.toMatchObject({
      organizationId: "org",
      appUserId: "app-user",
    });
    const reused = await Effect.runPromise(
      Effect.either(completeAuthorization(callbackUrl(authorization.state))),
    );
    expectTaggedFailure(reused, {
      _tag: "@Gateway/OAuthStateError",
      message: "Invalid or expired OAuth state",
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("validates token responses and reports non-app identities as Linear failures", async () => {
    fetchMock.mockResolvedValueOnce(
      new Response(
        JSON.stringify(tokenResponse({ refresh_token: undefined })),
        {
          status: 200,
        },
      ),
    );
    const invalidTokenAuthorization = await Effect.runPromise(
      startAuthorization(),
    );
    const invalidToken = await Effect.runPromise(
      Effect.either(
        completeAuthorization(callbackUrl(invalidTokenAuthorization.state)),
      ),
    );
    expectTaggedFailure(invalidToken, {
      _tag: "@Gateway/LinearApiError",
      operation: "exchangeAuthorizationCode",
    });
    expect(testState.installations).toHaveLength(0);

    linearTestDouble.viewer = {
      id: "app-user",
      app: null,
      organization: Promise.resolve({ id: "org" }),
    };
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify(tokenResponse()), { status: 200 }),
    );
    const nonAppAuthorization = await Effect.runPromise(startAuthorization());
    const nonAppIdentity = await Effect.runPromise(
      Effect.either(
        completeAuthorization(callbackUrl(nonAppAuthorization.state)),
      ),
    );
    expectTaggedFailure(nonAppIdentity, {
      _tag: "@Gateway/LinearApiError",
      operation: "discoverAppInstallation",
    });
    expect(testState.installations).toHaveLength(0);
  });

  it.effect.prop(
    "OAuth state is single-use and expires only after its inclusive boundary",
    {
      rawState: Schema.String.pipe(Schema.minLength(1)),
      now: Schema.Number.pipe(
        Schema.int(),
        Schema.between(1, 2_000_000_000_000),
      ),
    },
    ({ rawState, now }) =>
      Effect.gen(function* () {
        resetTestState();
        const activeHash = stateHash(rawState);
        expect(yield* createAndConsumeState(activeHash, now, now)).toBe(true);
        expect(yield* consumeState(activeHash, now)).toBe(false);

        const expiredHash = stateHash(`${rawState}:expired`);
        expect(yield* createAndConsumeState(expiredHash, now - 1, now)).toBe(
          false,
        );
      }),
    { fastCheck: { numRuns: 50 } },
  );

  it.effect.prop(
    "generated bearer responses preserve refresh tokens and compute millisecond expiry",
    {
      accessToken: Schema.String.pipe(Schema.minLength(1)),
      refreshToken: Schema.String.pipe(Schema.minLength(1)),
      expiresIn: Schema.Number.pipe(Schema.int(), Schema.between(1, 100_000)),
    },
    ({ accessToken, refreshToken, expiresIn }) =>
      Effect.gen(function* () {
        resetTestState();
        fetchMock.mockReset();
        fetchMock.mockResolvedValue(
          new Response(
            JSON.stringify(
              tokenResponse({
                access_token: accessToken,
                refresh_token: refreshToken,
                expires_in: expiresIn,
              }),
            ),
            { status: 200 },
          ),
        );

        const authorization = yield* startAuthorization();
        const before = yield* Effect.clockWith(
          (clock) => clock.currentTimeMillis,
        );
        const installation = yield* completeAuthorization(
          callbackUrl(authorization.state),
        );
        const after = yield* Effect.clockWith(
          (clock) => clock.currentTimeMillis,
        );

        expect(installation.accessToken).toBe(accessToken);
        expect(installation.refreshToken).toBe(refreshToken);
        expect(installation.scopes).toEqual([
          "read",
          "write",
          "app:assignable",
          "app:mentionable",
        ]);
        expect(installation.revokedAt).toEqual(Option.none());
        expect(installation.accessibleTeamIds).toEqual(Option.none());
        expect(installation.canAccessAllPublicTeams).toEqual(Option.none());
        expect(installation.expiresAt).toBeGreaterThanOrEqual(
          before + expiresIn * 1_000,
        );
        expect(installation.expiresAt).toBeLessThanOrEqual(
          after + expiresIn * 1_000,
        );
        expect(testState.installations).toEqual([installation]);
      }),
    { fastCheck: { numRuns: 50 } },
  );
});
