import { generateKeyPairSync } from "node:crypto";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Either } from "effect";
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
      appId: "12345",
      privateKey: privateKeyPem,
      fetch: fetchMock,
      now: () => now,
    });

    const first = await Effect.runPromise(
      service.getInstallationToken("octo-org", "private-repo"),
    );
    const second = await Effect.runPromise(
      service.getInstallationToken("octo-org", "private-repo"),
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
      service.getInstallationToken("octo-org", "private-repo"),
    );
    expect(requests).toHaveLength(4);
  });

  it("returns a typed failure without exposing disabled credentials", async () => {
    const result = await Effect.runPromise(
      Effect.either(
        makeGitHubApp({
          appId: undefined,
          privateKey: undefined,
        }).getInstallationToken("owner", "repo"),
      ),
    );
    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect(result.left.reason).toBe("disabled");
      expect(result.left.message).not.toContain("private");
    }
  });
});
