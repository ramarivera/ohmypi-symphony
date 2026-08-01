import { Effect, Redacted } from "effect";
import {
  GitHubAppApiError,
  GitHubAppConfigurationError,
  GitHubAppRemoteError,
} from "../domain/errors.js";
import type { GatewayConfigShape, GitHubAppConfigShape } from "./config.js";
import { GatewayConfig } from "./config.js";

const GITHUB_API_BASE = "https://api.github.com";
const GITHUB_API_VERSION = "2022-11-28";
const JWT_LIFETIME_SECONDS = 9 * 60;
const JWT_IAT_SKEW_SECONDS = 60;

export interface GitHubRepository {
  readonly owner: string;
  readonly repository: string;
}

export interface CreatePullRequestInput {
  readonly repositoryUrl: string;
  readonly base: string;
  readonly head: string;
  readonly title: string;
  readonly body: string;
}

export interface CreatedPullRequest {
  readonly url: string;
  readonly number: number;
}

export interface PublishPullRequestInput {
  readonly repositoryUrl: string;
  readonly base: string;
  readonly baseCommit: string;
  readonly branch: string;
  readonly workspacePath: string;
  readonly title: string;
  readonly body: string;
}

const isRepositoryPart = (value: string): boolean =>
  value.length > 0 &&
  value !== "." &&
  value !== ".." &&
  /^[A-Za-z0-9._-]+$/.test(value);

const repositoryFromParts = (
  owner: string,
  repository: string,
): GitHubRepository => {
  const normalizedRepository = repository.endsWith(".git")
    ? repository.slice(0, -4)
    : repository;
  if (!isRepositoryPart(owner) || !isRepositoryPart(normalizedRepository)) {
    throw new GitHubAppRemoteError({
      message: "Repository URL must identify a canonical github.com repository",
    });
  }
  return { owner, repository: normalizedRepository };
};

/** Parse only canonical github.com HTTPS and SSH remotes. */
export const parseGitHubRemote = (remote: string): GitHubRepository => {
  const value = remote.trim();
  const scp = /^git@github\.com:([^/\s]+)\/([^/\s]+)\/?$/.exec(value);
  if (scp && scp[1] !== undefined && scp[2] !== undefined) {
    return repositoryFromParts(scp[1], scp[2]);
  }

  const ssh = /^ssh:\/\/git@github\.com\/([^/\s]+)\/([^/\s]+)\/?$/.exec(value);
  if (ssh && ssh[1] !== undefined && ssh[2] !== undefined) {
    return repositoryFromParts(ssh[1], ssh[2]);
  }

  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new GitHubAppRemoteError({
      message:
        "Repository URL must be a canonical github.com HTTPS or SSH remote",
    });
  }
  if (
    url.protocol !== "https:" ||
    url.hostname !== "github.com" ||
    url.username !== "" ||
    url.password !== "" ||
    url.port !== "" ||
    url.search !== "" ||
    url.hash !== ""
  ) {
    throw new GitHubAppRemoteError({
      message:
        "Repository URL must be a canonical github.com HTTPS or SSH remote",
    });
  }

  const path = url.pathname.replace(/\/$/, "");
  const parts = path.split("/");
  if (
    parts.length !== 3 ||
    parts[0] !== "" ||
    parts[1] === undefined ||
    parts[2] === undefined
  ) {
    throw new GitHubAppRemoteError({
      message: "Repository URL must identify a canonical github.com repository",
    });
  }
  return repositoryFromParts(parts[1], parts[2]);
};
const isGitBranchRef = (value: string): boolean =>
  value.length > 0 &&
  !value.startsWith("refs/") &&
  !value.startsWith("/") &&
  !value.endsWith("/") &&
  !value.endsWith(".") &&
  !value.includes("..") &&
  !value.includes("@{") &&
  !/[ ~^:?*\[\\]/u.test(value) &&
  !value.includes("//");

const base64Url = (bytes: Uint8Array): string => {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
};

const encodeJson = (value: unknown): Uint8Array =>
  new TextEncoder().encode(JSON.stringify(value));

const derLength = (length: number): Uint8Array => {
  if (length < 0x80) return new Uint8Array([length]);
  const bytes: number[] = [];
  let remaining = length;
  while (remaining > 0) {
    bytes.unshift(remaining & 0xff);
    remaining >>>= 8;
  }
  return new Uint8Array([0x80 | bytes.length, ...bytes]);
};

const derElement = (tag: number, value: Uint8Array): Uint8Array => {
  const length = derLength(value.length);
  return new Uint8Array([tag, ...length, ...value]);
};

const derConcat = (...parts: Uint8Array[]): Uint8Array => {
  const result = new Uint8Array(
    parts.reduce((total, part) => total + part.length, 0),
  );
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }
  return result;
};

const pkcs1ToPkcs8 = (pkcs1: Uint8Array): Uint8Array => {
  const algorithm = derElement(
    0x30,
    derConcat(
      derElement(
        0x06,
        new Uint8Array([0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01]),
      ),
      derElement(0x05, new Uint8Array()),
    ),
  );
  return derElement(
    0x30,
    derConcat(
      derElement(0x02, new Uint8Array([0x00])),
      algorithm,
      derElement(0x04, pkcs1),
    ),
  );
};

const pemToDer = (pem: string): Uint8Array => {
  const normalized = pem.trim().replace(/\r/g, "");
  const isPkcs1 = normalized.includes("BEGIN RSA PRIVATE KEY");
  const encoded = normalized
    .replace(/-----BEGIN [^-]+-----/g, "")
    .replace(/-----END [^-]+-----/g, "")
    .replace(/\s/g, "");
  let bytes: Uint8Array;
  try {
    const binary = atob(encoded);
    bytes = Uint8Array.from(binary, (character) => character.charCodeAt(0));
  } catch {
    throw new Error("invalid private key");
  }
  return isPkcs1 ? pkcs1ToPkcs8(bytes) : bytes;
};
const toArrayBuffer = (bytes: Uint8Array): ArrayBuffer => {
  const buffer = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(buffer).set(bytes);
  return buffer;
};

const createAppJwt = (
  appId: string,
  privateKey: Redacted.Redacted<string>,
): Effect.Effect<string, GitHubAppApiError> =>
  Effect.gen(function* () {
    const now = Math.floor(Date.now() / 1000);
    const header = base64Url(encodeJson({ alg: "RS256", typ: "JWT" }));
    const payload = base64Url(
      encodeJson({
        iat: now - JWT_IAT_SKEW_SECONDS,
        exp: now + JWT_LIFETIME_SECONDS,
        iss: Number(appId),
      }),
    );
    const unsigned = `${header}.${payload}`;
    const key = yield* Effect.tryPromise({
      try: () =>
        crypto.subtle.importKey(
          "pkcs8",
          toArrayBuffer(pemToDer(Redacted.value(privateKey))),
          { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
          false,
          ["sign"],
        ),
      catch: () =>
        new GitHubAppApiError({
          message: "GitHub App private key could not be imported",
          operation: "jwt",
        }),
    });
    const signature = yield* Effect.tryPromise({
      try: () =>
        crypto.subtle.sign(
          { name: "RSASSA-PKCS1-v1_5" },
          key,
          toArrayBuffer(new TextEncoder().encode(unsigned)),
        ),
      catch: () =>
        new GitHubAppApiError({
          message: "GitHub App JWT could not be signed",
          operation: "jwt",
        }),
    });
    return `${unsigned}.${base64Url(new Uint8Array(signature))}`;
  }).pipe(
    Effect.catchAll((error) =>
      error instanceof GitHubAppApiError
        ? Effect.fail(error)
        : Effect.fail(
            new GitHubAppApiError({
              message: "GitHub App JWT could not be created",
              operation: "jwt",
            }),
          ),
    ),
  );

const apiError = (operation: string, status?: number): GitHubAppApiError =>
  new GitHubAppApiError({
    message: `GitHub ${operation} request failed`,
    operation,
    ...(status === undefined ? {} : { status }),
  });

const installationLookupError = (
  repository: GitHubRepository,
  status?: number,
): GitHubAppApiError =>
  new GitHubAppApiError({
    message: `GitHub installation lookup failed for ${repository.owner}/${repository.repository}`,
    operation: "installation lookup",
    ...(status === undefined ? {} : { status }),
  });

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const getConfig = (config: GatewayConfigShape): GitHubAppConfigShape => {
  const githubApp = config.githubApp;
  if (githubApp === undefined) {
    throw new GitHubAppConfigurationError({
      message: "GitHub App configuration is incomplete or invalid",
      missing: ["GITHUB_APP_ID", "GITHUB_APP_PRIVATE_KEY"],
    });
  }

  const missing: string[] = [];
  if (githubApp.appId.trim() === "") missing.push("GITHUB_APP_ID");
  if (Redacted.value(githubApp.privateKey).trim() === "") {
    missing.push("GITHUB_APP_PRIVATE_KEY");
  }
  if (missing.length > 0) {
    throw new GitHubAppConfigurationError({
      message: "GitHub App configuration is incomplete or invalid",
      missing,
    });
  }
  const validAppId =
    /^\d+$/.test(githubApp.appId) &&
    Number.isSafeInteger(Number(githubApp.appId)) &&
    Number(githubApp.appId) > 0;
  if (!validAppId) {
    throw new GitHubAppConfigurationError({
      message: "GitHub App configuration is incomplete or invalid",
      missing: ["GITHUB_APP_ID"],
    });
  }
  return githubApp;
};

const githubHeaders = (jwtOrToken: string): HeadersInit => ({
  Accept: "application/vnd.github+json",
  "X-GitHub-Api-Version": GITHUB_API_VERSION,
  Authorization: `Bearer ${jwtOrToken}`,
});

const getInstallationId = (
  githubApp: GitHubAppConfigShape,
  repository: GitHubRepository,
): Effect.Effect<number, GitHubAppApiError> =>
  Effect.gen(function* () {
    const jwt = yield* createAppJwt(githubApp.appId, githubApp.privateKey);
    const response = yield* Effect.tryPromise({
      try: () =>
        fetch(
          `${GITHUB_API_BASE}/repos/${encodeURIComponent(repository.owner)}/${encodeURIComponent(repository.repository)}/installation`,
          { headers: githubHeaders(jwt) },
        ),
      catch: () => installationLookupError(repository),
    });
    if (!response.ok) {
      return yield* Effect.fail(
        installationLookupError(repository, response.status),
      );
    }
    const body = yield* Effect.tryPromise({
      try: () => response.json() as Promise<unknown>,
      catch: () => installationLookupError(repository),
    });
    if (
      !isRecord(body) ||
      typeof body.id !== "number" ||
      !Number.isSafeInteger(body.id) ||
      body.id <= 0
    ) {
      return yield* Effect.fail(installationLookupError(repository));
    }
    return body.id;
  });

const getInstallationToken = (
  githubApp: GitHubAppConfigShape,
  repository: GitHubRepository,
): Effect.Effect<string, GitHubAppApiError> =>
  Effect.gen(function* () {
    const installationId = yield* getInstallationId(githubApp, repository);
    const jwt = yield* createAppJwt(githubApp.appId, githubApp.privateKey);
    const response = yield* Effect.tryPromise({
      try: () =>
        fetch(
          `${GITHUB_API_BASE}/app/installations/${installationId}/access_tokens`,
          {
            method: "POST",
            headers: githubHeaders(jwt),
          },
        ),
      catch: () => apiError("installation token"),
    });
    if (!response.ok) {
      return yield* Effect.fail(
        apiError("installation token", response.status),
      );
    }
    const body = yield* Effect.tryPromise({
      try: () => response.json() as Promise<unknown>,
      catch: () => apiError("installation token"),
    });
    if (
      !isRecord(body) ||
      typeof body.token !== "string" ||
      body.token.trim() === ""
    ) {
      return yield* Effect.fail(apiError("installation token"));
    }
    return body.token;
  });

interface GitResult {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
}

const BASE_GIT_ENV: Record<string, string> = {
  PATH: "/usr/bin:/bin:/usr/sbin:/sbin",
  HOME: "/nonexistent",
  GIT_CONFIG_NOSYSTEM: "1",
  GIT_CONFIG_GLOBAL: "/dev/null",
  GIT_TERMINAL_PROMPT: "0",
};

const runGit = (
  args: ReadonlyArray<string>,
  cwd: string,
  env?: Record<string, string>,
): Effect.Effect<GitResult, GitHubAppApiError> =>
  Effect.tryPromise({
    try: async () => {
      const child = Bun.spawn(["git", ...args], {
        cwd,
        env: { ...BASE_GIT_ENV, ...env },
        stdout: "pipe",
        stderr: "pipe",
      });
      const [exitCode, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      return { exitCode, stdout, stderr };
    },
    catch: () => apiError("workspace publication"),
  });
const publishWorkspace = (
  githubApp: GitHubAppConfigShape,
  input: PublishPullRequestInput,
): Effect.Effect<
  CreatedPullRequest | undefined,
  GitHubAppRemoteError | GitHubAppApiError
> =>
  Effect.gen(function* () {
    const repository = yield* Effect.try({
      try: () => parseGitHubRemote(input.repositoryUrl),
      catch: (error) =>
        error instanceof GitHubAppRemoteError
          ? error
          : new GitHubAppRemoteError({
              message:
                "Repository URL must be a canonical github.com repository",
            }),
    });
    if (
      !/^gateway\/[a-f0-9]{32}$/u.test(input.branch) ||
      !isGitBranchRef(input.base) ||
      !/^[0-9a-f]{40}$/u.test(input.baseCommit)
    ) {
      return yield* Effect.fail(
        new GitHubAppRemoteError({
          message: "GitHub pull request base must be a branch and workspace snapshot must be a commit",
        }),
      );
    }

    const status = yield* runGit(
      ["status", "--porcelain"],
      input.workspacePath,
    );
    yield* requireGitSuccess(status);
    let ahead = yield* runGit(
      ["rev-list", "--count", `${input.baseCommit}..HEAD`],
      input.workspacePath,
    );
    yield* requireGitSuccess(ahead);
    let commitCount = Number.parseInt(ahead.stdout.trim(), 10);
    if (!Number.isSafeInteger(commitCount) || commitCount < 0) {
      return yield* Effect.fail(apiError("workspace publication"));
    }

    if (status.stdout.trim().length > 0) {
      yield* requireGitSuccess(
        yield* runGit(["add", "--all"], input.workspacePath),
      );
      const staged = yield* runGit(
        ["diff", "--cached", "--quiet"],
        input.workspacePath,
      );
      if (staged.exitCode === 1) {
        yield* requireGitSuccess(
          yield* runGit(
            [
              "-c",
              "user.name=OhMyPi Gateway",
              "-c",
              "user.email=ohmypi-gateway@localhost",
              "commit",
              "-m",
              "Gateway workspace changes",
            ],
            input.workspacePath,
          ),
        );
      } else {
        yield* requireGitSuccess(staged);
      }
      ahead = yield* runGit(
        ["rev-list", "--count", `${input.baseCommit}..HEAD`],
        input.workspacePath,
      );
      yield* requireGitSuccess(ahead);
      commitCount = Number.parseInt(ahead.stdout.trim(), 10);
      if (!Number.isSafeInteger(commitCount) || commitCount < 0) {
        return yield* Effect.fail(apiError("workspace publication"));
      }
    }

    if (commitCount === 0) return undefined;

    const installationToken = yield* getInstallationToken(
      githubApp,
      repository,
    );
    const credentialHelper =
      "!f() { printf 'username=x-access-token\\npassword=%s\\n' \"$GITHUB_APP_INSTALLATION_TOKEN\"; }; f";
    const push = yield* runGit(
      [
        "-c",
        "credential.helper=",
        "-c",
        `credential.helper=${credentialHelper}`,
        "push",
        `https://github.com/${repository.owner}/${repository.repository}.git`,
        `HEAD:refs/heads/${input.branch}`,
      ],
      input.workspacePath,
      {
        GITHUB_APP_INSTALLATION_TOKEN: installationToken,
        GIT_TERMINAL_PROMPT: "0",
      },
    );
    yield* requireGitSuccess(push);

    const headers = {
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": GITHUB_API_VERSION,
      Authorization: `Bearer ${installationToken}`,
    };
    const query = new URLSearchParams({
      state: "open",
      head: `${repository.owner}:${input.branch}`,
      base: input.base,
    });
    const existingResponse = yield* Effect.tryPromise({
      try: () =>
        fetch(
          `${GITHUB_API_BASE}/repos/${encodeURIComponent(repository.owner)}/${encodeURIComponent(repository.repository)}/pulls?${query}`,
          { headers },
        ),
      catch: () => apiError("pull request lookup"),
    });
    if (!existingResponse.ok) {
      return yield* Effect.fail(
        apiError("pull request lookup", existingResponse.status),
      );
    }
    const existingBody = yield* Effect.tryPromise({
      try: () => existingResponse.json() as Promise<unknown>,
      catch: () => apiError("pull request lookup"),
    });
    if (Array.isArray(existingBody)) {
      for (const item of existingBody) {
        if (
          isRecord(item) &&
          typeof item.html_url === "string" &&
          item.html_url.trim() !== "" &&
          typeof item.number === "number" &&
          Number.isSafeInteger(item.number)
        ) {
          return Object.freeze({ url: item.html_url, number: item.number });
        }
      }
    }

    const pullResponse = yield* Effect.tryPromise({
      try: () =>
        fetch(
          `${GITHUB_API_BASE}/repos/${encodeURIComponent(repository.owner)}/${encodeURIComponent(repository.repository)}/pulls`,
          {
            method: "POST",
            headers: { ...headers, "Content-Type": "application/json" },
            body: JSON.stringify({
              title: input.title,
              body: input.body,
              head: input.branch,
              base: input.base,
            }),
          },
        ),
      catch: () => apiError("pull request"),
    });
    if (!pullResponse.ok) {
      return yield* Effect.fail(apiError("pull request", pullResponse.status));
    }
    const pullBody = yield* Effect.tryPromise({
      try: () => pullResponse.json() as Promise<unknown>,
      catch: () => apiError("pull request"),
    });
    if (
      !isRecord(pullBody) ||
      typeof pullBody.html_url !== "string" ||
      pullBody.html_url.trim() === "" ||
      typeof pullBody.number !== "number" ||
      !Number.isSafeInteger(pullBody.number)
    ) {
      return yield* Effect.fail(apiError("pull request"));
    }
    return Object.freeze({ url: pullBody.html_url, number: pullBody.number });
  });

const requireGitSuccess = (
  result: GitResult,
): Effect.Effect<void, GitHubAppApiError> =>
  result.exitCode === 0
    ? Effect.succeed(undefined)
    : Effect.fail(
        new GitHubAppApiError({
          message: "GitHub workspace publication git command failed",
          operation: "workspace publication",
          cause: result.stderr.trim() || `exit code ${result.exitCode}`,
        }),
      );

export class GitHubApp extends Effect.Service<GitHubApp>()("GitHubApp", {
  accessors: true,
  dependencies: [GatewayConfig.Default],
  effect: Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const enabled = config.githubApp !== undefined;

    const createPullRequest = (
      input: CreatePullRequestInput,
    ): Effect.Effect<
      CreatedPullRequest,
      GitHubAppConfigurationError | GitHubAppRemoteError | GitHubAppApiError
    > =>
      Effect.gen(function* () {
        const githubApp = yield* Effect.try({
          try: () => getConfig(config),
          catch: (error) =>
            error instanceof GitHubAppConfigurationError
              ? error
              : new GitHubAppConfigurationError({
                  message: "GitHub App configuration is incomplete or invalid",
                  missing: ["GITHUB_APP_ID", "GITHUB_APP_PRIVATE_KEY"],
                }),
        });
        const repository = yield* Effect.try({
          try: () => parseGitHubRemote(input.repositoryUrl),
          catch: (error) =>
            error instanceof GitHubAppRemoteError
              ? error
              : new GitHubAppRemoteError({
                  message:
                    "Repository URL must be a canonical github.com repository",
                }),
        });
        const installationToken = yield* getInstallationToken(
          githubApp,
          repository,
        );

        const pullResponse = yield* Effect.tryPromise({
          try: () =>
            fetch(
              `${GITHUB_API_BASE}/repos/${encodeURIComponent(repository.owner)}/${encodeURIComponent(repository.repository)}/pulls`,
              {
                method: "POST",
                headers: {
                  Accept: "application/vnd.github+json",
                  "X-GitHub-Api-Version": GITHUB_API_VERSION,
                  Authorization: `Bearer ${installationToken}`,
                  "Content-Type": "application/json",
                },
                body: JSON.stringify({
                  title: input.title,
                  body: input.body,
                  head: input.head,
                  base: input.base,
                }),
              },
            ),
          catch: () => apiError("pull request"),
        });
        if (!pullResponse.ok) {
          return yield* Effect.fail(
            apiError("pull request", pullResponse.status),
          );
        }
        const pullBody = yield* Effect.tryPromise({
          try: () => pullResponse.json() as Promise<unknown>,
          catch: () => apiError("pull request"),
        });
        if (
          !isRecord(pullBody) ||
          typeof pullBody.html_url !== "string" ||
          pullBody.html_url.trim() === "" ||
          typeof pullBody.number !== "number" ||
          !Number.isSafeInteger(pullBody.number)
        ) {
          return yield* Effect.fail(apiError("pull request"));
        }
        return Object.freeze({
          url: pullBody.html_url,
          number: pullBody.number,
        });
      });

    const publishPullRequest = (
      input: PublishPullRequestInput,
    ): Effect.Effect<
      CreatedPullRequest | undefined,
      GitHubAppConfigurationError | GitHubAppRemoteError | GitHubAppApiError
    > =>
      !enabled
        ? Effect.succeed(undefined)
        : Effect.gen(function* () {
            const githubApp = yield* Effect.try({
              try: () => getConfig(config),
              catch: (error) =>
                error instanceof GitHubAppConfigurationError
                  ? error
                  : new GitHubAppConfigurationError({
                      message:
                        "GitHub App configuration is incomplete or invalid",
                      missing: ["GITHUB_APP_ID", "GITHUB_APP_PRIVATE_KEY"],
                    }),
            });
            return yield* publishWorkspace(githubApp, input);
          });

    return {
      enabled,
      isEnabled: () => enabled,
      createPullRequest,
      publishPullRequest,
    };
  }),
}) {}
