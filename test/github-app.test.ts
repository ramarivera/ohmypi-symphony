import { generateKeyPairSync } from "node:crypto";
import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Result } from "effect";
import {
  buildGitHubAppJwt,
  buildGitHubExtraHeader,
  type GitHubFetch,
  makeGitHubApp,
} from "../src/services/github-app.js";

const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
const privateKeyPem = privateKey
  .export({ type: "pkcs8", format: "pem" })
  .toString();
const decodeJwtPart = (value: string) =>
  JSON.parse(Buffer.from(value, "base64url").toString("utf8")) as Record<
    string,
    unknown
  >;

describe("GitHubApp", () => {
  it("builds a short-lived RS256 JWT with the app claims", () => {
    const jwt = buildGitHubAppJwt("12345", privateKeyPem, 1_700_000_000_000);
    const [encodedHeader, encodedPayload, encodedSignature] = jwt.split(".");
    expect(decodeJwtPart(encodedHeader ?? "").alg).toBe("RS256");

    expect(decodeJwtPart(encodedHeader ?? "").typ).toBe("JWT");
    expect(decodeJwtPart(encodedPayload ?? "")).toMatchObject({
      iss: "12345",
      iat: 1_699_999_940,
      exp: 1_700_000_540,
    });
    expect((encodedSignature ?? "").length).toBeGreaterThan(100);
  });
  it("encodes the x-access-token credential in the Git extraheader", () => {
    expect(buildGitHubExtraHeader("ghs_test_token")).toBe(
      `AUTHORIZATION: basic ${Buffer.from(
        "x-access-token:ghs_test_token",
      ).toString("base64")}`,
    );
  });

  it("looks up an installation, mints a repository-scoped token, and caches it", async () => {
    let now = 1_700_000_000_000;
    const requests: Array<{ url: string; init: RequestInit | undefined }> = [];
    const fetchMock: GitHubFetch = async (url, init) => {
      requests.push({ url: String(url), init });
      if (requests.length % 2 === 1) {
        return new Response(JSON.stringify({ id: 42 }), { status: 200 });
      }
      return new Response(
        JSON.stringify({
          token: "ghs_test_token",
          expires_at: new Date(now + 60 * 60_000).toISOString(),
        }),
        { status: 201 },
      );
    };
    const service = makeGitHubApp({
      allowedOwners: { "org-test": ["octo-org"] },
      appId: "12345",
      privateKey: privateKeyPem,
      fetch: fetchMock,
      now: () => now,
    });

    const first = await Effect.runPromise(
      service.getInstallationToken("octo-org", "private-repo", "org-test"),
    );
    const second = await Effect.runPromise(
      service.getInstallationToken("octo-org", "private-repo", "org-test"),
    );
    expect(first).toBe("ghs_test_token");
    expect(second).toBe(first);
    expect(requests).toHaveLength(2);
    expect(requests[0]?.url).toBe(
      "https://api.github.com/repos/octo-org/private-repo/installation",
    );
    expect(requests[1]?.url).toBe(
      "https://api.github.com/app/installations/42/access_tokens",
    );
    expect(JSON.parse(String(requests[1]?.init?.body))).toEqual({
      repositories: ["private-repo"],
    });
    expect(String(requests[0]?.init?.headers)).not.toContain("ghs_test_token");

    now += 51 * 60_000;
    await Effect.runPromise(
      service.getInstallationToken("octo-org", "private-repo", "org-test"),
    );
    expect(requests).toHaveLength(4);
  });

  it("bounds the token cache and prunes expired entries", async () => {
    let now = 1_700_000_000_000;
    let requests = 0;
    const fetchMock: GitHubFetch = async (url) => {
      requests += 1;
      const path = String(url);
      if (path.includes("/repos/")) {
        return new Response(JSON.stringify({ id: requests }), { status: 200 });
      }
      return new Response(
        JSON.stringify({
          token: `token-${requests}`,
          expires_at: new Date(now + 1_000).toISOString(),
        }),
        { status: 201 },
      );
    };
    const service = makeGitHubApp({
      allowedOwners: { "org-test": ["octo-org"] },
      appId: "12345",
      privateKey: privateKeyPem,
      fetch: fetchMock,
      now: () => now,
    });

    for (let index = 0; index < 100; index += 1) {
      await Effect.runPromise(
        service.getInstallationToken("octo-org", `repo-${index}`, "org-test"),
      );
    }
    await Effect.runPromise(
      service.getInstallationToken("octo-org", "repo-100", "org-test"),
    );
    const afterInitialMints = requests;

    // repo-0 had the oldest expiry and is evicted when repo-100 is inserted.
    await Effect.runPromise(
      service.getInstallationToken("octo-org", "repo-0", "org-test"),
    );
    expect(requests).toBe(afterInitialMints + 2);

    now += 2_000;
    await Effect.runPromise(
      service.getInstallationToken("octo-org", "repo-100", "org-test"),
    );
    expect(requests).toBe(afterInitialMints + 4);
  });

  it("single-flights concurrent token mints for one repository", async () => {
    let releaseFirstRequest: (() => void) | undefined;
    let resolveStarted: (() => void) | undefined;
    const started = new Promise<void>((resolve) => {
      resolveStarted = resolve;
    });
    const gate = new Promise<void>((resolve) => {
      releaseFirstRequest = resolve;
    });
    let requests = 0;
    const fetchMock: GitHubFetch = async (url) => {
      requests += 1;
      if (requests === 1) {
        resolveStarted?.();
        await gate;
      }
      if (String(url).includes("/repos/")) {
        return new Response(JSON.stringify({ id: 42 }), { status: 200 });
      }
      return new Response(JSON.stringify({ token: "single-flight-token" }), {
        status: 201,
      });
    };
    const service = makeGitHubApp({
      allowedOwners: { "org-test": ["octo-org"] },
      appId: "12345",
      privateKey: privateKeyPem,
      fetch: fetchMock,
    });

    const first = Effect.runPromise(
      service.getInstallationToken("octo-org", "single-flight", "org-test"),
    );
    await started;
    const second = Effect.runPromise(
      service.getInstallationToken("octo-org", "single-flight", "org-test"),
    );
    releaseFirstRequest?.();
    await expect(Promise.all([first, second])).resolves.toEqual([
      "single-flight-token",
      "single-flight-token",
    ]);
    expect(requests).toBe(2);
  });

  it("mints a fresh token when a waiting caller retries immediately after failure", async () => {
    const started = Deferred.makeUnsafe<void>();
    const firstResponse = Promise.withResolvers<Response>();
    let lookups = 0;
    let mints = 0;
    let retries = 0;
    const service = makeGitHubApp({
      allowedOwners: { "org-test": ["octo-org"] },
      appId: "12345",
      privateKey: privateKeyPem,
      fetch: async (url) => {
        if (String(url).includes("/repos/")) {
          lookups += 1;
          return Response.json({ id: 42 });
        }
        mints += 1;
        if (mints === 1) {
          Deferred.doneUnsafe(started, Effect.void);
          return firstResponse.promise;
        }
        return Response.json({ token: "retried-token" }, { status: 201 });
      },
    });
    const token = () =>
      service.getInstallationToken("octo-org", "repo", "org-test");
    const program = Effect.gen(function* () {
      const winner = yield* Effect.forkChild(Effect.result(token()), {
        startImmediately: true,
      });
      yield* Deferred.await(started);
      const waiter = yield* Effect.forkChild(
        token().pipe(
          Effect.catchTag("@Gateway/GitHubAppError", (error) => {
            expect(error.status).toBe(503);
            retries += 1;
            return token();
          }),
        ),
        { startImmediately: true },
      );
      expect(waiter.pollUnsafe()).toBeUndefined();
      expect(lookups).toBe(1);
      expect(mints).toBe(1);
      firstResponse.resolve(Response.json({}, { status: 503 }));
      expect(yield* Fiber.join(waiter)).toBe("retried-token");
      const failed = yield* Fiber.join(winner);
      expect(Result.isFailure(failed)).toBe(true);
      expect(retries).toBe(1);
      expect(lookups).toBe(2);
      expect(mints).toBe(2);
    });
    await Effect.runPromise(Effect.timeout(program, "5 seconds"));
  });

  it("returns a typed failure without exposing disabled credentials", async () => {
    const result = await Effect.runPromise(
      Effect.result(
        makeGitHubApp({
          allowedOwners: { "org-test": ["octo-org"] },
          appId: undefined,
          privateKey: undefined,
        }).getInstallationToken("owner", "repo", "org-test"),
      ),
    );
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result)) {
      expect(result.failure.reason).toBe("disabled");
      expect(result.failure.message).not.toContain("private");
    }
  });

  it("reports not_installed when the App is absent from the repository", async () => {
    const fetchMock: GitHubFetch = async () =>
      new Response(JSON.stringify({ message: "Not Found" }), {
        status: 404,
      });
    const service = makeGitHubApp({
      allowedOwners: { "org-test": ["octo-org"] },
      appId: "12345",
      privateKey: privateKeyPem,
      fetch: fetchMock,
    });
    const result = await Effect.runPromise(
      Effect.result(
        service.getInstallationToken("octo-org", "other-repo", "org-test"),
      ),
    );
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result)) {
      expect(result.failure.reason).toBe("not_installed");
    }
  });

  it("releases single-flight waiters when the winner is interrupted", async () => {
    const started = Deferred.makeUnsafe<void>();
    const service = makeGitHubApp({
      allowedOwners: { "org-test": ["octo-org"] },
      appId: "12345",
      privateKey: privateKeyPem,
      fetch: async () => {
        Deferred.doneUnsafe(started, Effect.void);
        return new Promise<Response>(() => {}); // never resolves
      },
    });
    const program = Effect.gen(function* () {
      const winner = yield* Effect.forkChild(
        service.getInstallationToken("octo-org", "repo", "org-test"),
      );
      yield* Deferred.await(started);
      const waiter = yield* Effect.forkChild(
        service.getInstallationToken("octo-org", "repo", "org-test"),
      );
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(winner);
      // The waiter must be released (interrupted exit), not hang forever.
      const waiterExit = yield* Fiber.await(waiter);
      expect(Exit.hasInterrupts(waiterExit)).toBe(true);
    });
    await Effect.runPromise(Effect.timeout(program, "5 seconds"));
  });
});

describe("GitHub App bot identity", () => {
  it.live(
    "looks up the bot user id, not the app id, and caches concurrent requests",
    () =>
      Effect.gen(function* () {
        const requests: Array<{ url: string; init: RequestInit | undefined }> =
          [];
        const app = makeGitHubApp({
          appId: "12345",
          privateKey: privateKeyPem,
          fetch: async (url, init) => {
            requests.push({ url: String(url), init });
            return String(url).endsWith("/app")
              ? Response.json({ slug: "test-app", id: 12345 })
              : Response.json({ id: 98765 });
          },
        });
        const identity = {
          name: "test-app[bot]",
          email: "98765+test-app[bot]@users.noreply.github.com",
        };
        expect(
          yield* Effect.all([app.getBotIdentity(), app.getBotIdentity()], {
            concurrency: "unbounded",
          }),
        ).toEqual([identity, identity]);
        expect(yield* app.getBotIdentity()).toEqual(identity);
        expect(requests.map((r) => r.url)).toEqual([
          "https://api.github.com/app",
          "https://api.github.com/users/test-app%5Bbot%5D",
        ]);
        expect(
          new Headers(requests[0]?.init?.headers).get("Authorization"),
        ).toMatch(/^Bearer /u);
        expect(
          new Headers(requests[1]?.init?.headers).get("Authorization"),
        ).toBeNull();
      }),
  );
  for (const failure of ["status", "invalid", "network"] as const) {
    it.live(
      `falls back to the slug email on ${failure} bot lookup failure`,
      () =>
        Effect.gen(function* () {
          const app = makeGitHubApp({
            appId: "12345",
            privateKey: privateKeyPem,
            fetch: async (url) => {
              if (String(url).endsWith("/app"))
                return Response.json({ slug: "test-app" });
              if (failure === "network") throw new Error("offline");
              return failure === "status"
                ? new Response(null, { status: 503 })
                : Response.json({ id: "not-a-user-id" });
            },
          });
          expect(yield* app.getBotIdentity()).toEqual({
            name: "test-app[bot]",
            email: "test-app[bot]@users.noreply.github.com",
          });
        }),
    );
  }
});
