import { Effect, Option, Redacted, Schema } from "effect";
import { describe, expect, it } from "vitest";
import { LinearRateLimitError, WorkspaceError } from "../src/domain/errors.js";
import {
  IssueId,
  OrganizationId,
  ProjectId,
  SessionId,
  TeamId,
  WorkspaceId,
} from "../src/domain/ids.js";
import {
  type AgentRun,
  RepositoryRecord,
  type RepositoryRecord as RepositoryRecordType,
} from "../src/domain/models.js";
import {
  type AdminDeps,
  createAdminHandle,
  csrfHash,
  deriveCsrfToken,
} from "../src/services/admin.js";
import type { GatewayConfigShape } from "../src/services/config.js";
import { NixEnvironment } from "../src/services/nix-environment.js";
import {
  AdminSessionRepo,
  InstallationRepo,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
  WorkspaceRepo,
} from "../src/services/store/repositories.js";

const unreachable = (..._args: ReadonlyArray<unknown>) => Effect.never;
const token = "admin-token";
const organizationId = Schema.decodeUnknownSync(OrganizationId)(
  "11111111-1111-4111-8111-111111111111",
);
const issueId = Schema.decodeUnknownSync(IssueId)(
  "44444444-4444-4444-8444-444444444444",
);
const repositoryId = Schema.decodeUnknownSync(WorkspaceId)("repo-main");
const teamId = Schema.decodeUnknownSync(TeamId)("team-main");
const projectId = Schema.decodeUnknownSync(ProjectId)("project-main");
const config: GatewayConfigShape = {
  linearClientId: "client",
  linearClientSecret: Redacted.make("secret"),
  linearWebhookSecret: Redacted.make("webhook"),
  tokenEncryptionKey: Redacted.make("key"),
  publicUrl: new URL("http://localhost:3000"),
  logLevel: "silent",
  logFile: Option.none(),
  databasePath: ":memory:",
  workspaceRoot: "/tmp/workspaces",
  ompCliPath: "omp",
  port: 3000,
  leaseDurationMs: 60_000,
  reconcilerIntervalMs: 1_000,
  nixBinaryPath: "nix",
  nixpkgsFlakeRef:
    "github:NixOS/nixpkgs/0123456789abcdef0123456789abcdef01234567",
  nixRootsDir: "/tmp/nix-roots",
  nixGcMaxBytes: 1_000_000,
  reconcilerCatchupIntervalMs: 300_000,
  reconcilerCatchupMinAgeMs: 120_000,
  webhookReplayWindowMs: 60_000,
  repositorySuggestionConfidenceThreshold: 0.8,
};

const deps: AdminDeps = {
  config,
  adminSessionRepo: AdminSessionRepo.make({
    get: () =>
      Effect.succeed(
        Option.some({
          organizationId,
          csrfTokenHash: csrfHash(deriveCsrfToken(token)),
        }),
      ),
    create: unreachable,
    deleteAdminSession: unreachable,
  }),
  installationRepo: InstallationRepo.make({
    put: unreachable,
    get: unreachable,
    revoke: unreachable,
    applyPermissionChange: unreachable,
    getRawEncryptedAccessToken: unreachable,
    createOAuthState: unreachable,
    consumeOAuthState: unreachable,
  }),
  runRepo: RunRepo.make({
    get: unreachable,
    create: unreachable,
    createIfNoActiveForIssue: unreachable,
    update: unreachable,
    reopen: unreachable,
    hasActiveForIssue: unreachable,
    listNonTerminalByIssue: unreachable,
    listRunnable: unreachable,
    listCancellationPending: unreachable,
    claimLease: unreachable,
    renewLease: unreachable,
    releaseLease: unreachable,
    recoverInterruptedRuns: unreachable,
  }),
  runEventRepo: RunEventRepo.make({
    upsert: unreachable,
    list: unreachable,
  }),
  runInputRepo: RunInputRepo.make({
    enqueue: unreachable,
    applyStop: unreachable,
    pending: unreachable,
    latestActionableInput: unreachable,
    listSessionsWithPendingInputs: unreachable,
    markProcessed: unreachable,
  }),
  linearGateway: {
    createSessionOnIssue: unreachable,
  },
  workspaceRepo: WorkspaceRepo.make({
    setWorkspace: unreachable,
    createRepository: unreachable,
    getRepository: unreachable,
    listRepositories: unreachable,
    updateRepository: unreachable,
    deleteRepository: unreachable,
    getDefaultRepository: unreachable,
  }),
  workspace: {
    resolve: () => Effect.succeed({ kind: "none" }),
    materialize: (_sessionId: string, _repository: RepositoryRecordType) =>
      Effect.fail(
        new WorkspaceError({
          message: "unused",
          sessionId: "unused",
          reason: "git_failed",
        }),
      ),
  },
  reconciler: {
    status: () =>
      Effect.succeed({
        running: false,
        lastStartedAt: Option.none(),
        lastCompletedAt: Option.none(),
        lastError: Option.none(),
      }),
  },
  nixEnvironment: NixEnvironment.make({
    prepare: unreachable,
    list: () => Effect.succeed([]),
    prune: () => Effect.succeed(false),
  }),
};

const handle = createAdminHandle(deps);
const request = (path: string, body: unknown): Request =>
  new Request(new URL(path, config.publicUrl).toString(), {
    method: "POST",
    headers: {
      Cookie: `omp_gateway_admin=${token}`,
      Origin: config.publicUrl.toString(),
      "X-CSRF-Token": deriveCsrfToken(token),
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

const run = async (requestValue: Request): Promise<Response> => {
  const result = await Effect.runPromise(handle(requestValue));
  return Option.match(result, {
    onNone: () => {
      throw new Error("expected an admin response");
    },
    onSome: (response) => response,
  });
};

describe("service Admin request validation", () => {
  it("returns 400 for malformed optional repository arrays", async () => {
    for (const field of ["teamIds", "projectIds", "labels"]) {
      const response = await run(
        request("/api/admin/repositories", {
          id: `repo-${field}`,
          url: "https://example.com/repo.git",
          ref: "main",
          [field]: "not-an-array",
        }),
      );
      expect(response.status).toBe(400);
    }
  });

  it("normalizes valid packages and rejects invalid Nix package names", async () => {
    const valid = {
      id: "repo-nix",
      url: "https://example.com/repo.git",
      ref: "main",
      nixPackages: ["nodejs_22", "git", "nodejs_22"],
    };
    const invalid = await run(
      request("/api/admin/repositories", {
        ...valid,
        nixPackages: ["git;rm"],
      }),
    );
    expect(invalid.status).toBe(400);
    expect(await invalid.text()).toBe("Invalid Nix package name");
  });

  it("updates repository packages as normalized values", async () => {
    let updatedPackages: ReadonlyArray<string> = [];
    const repository = Schema.decodeUnknownSync(RepositoryRecord)({
      id: "repo-nix",
      organizationId,
      url: "https://example.com/repo.git",
      ref: "main",
      teamIds: [],
      projectIds: [],
      labels: [],
      nixPackages: ["git", "nodejs_22"],
      isDefault: false,
      createdAt: 1,
      updatedAt: 2,
    });
    const updateHandle = createAdminHandle({
      ...deps,
      workspaceRepo: WorkspaceRepo.make({
        setWorkspace: unreachable,
        createRepository: unreachable,
        getRepository: unreachable,
        listRepositories: () => Effect.succeed([]),
        updateRepository: (_organizationId, _id, update) => {
          updatedPackages = (update.nixPackages ?? []).map(String);
          return Effect.succeed(repository);
        },
        deleteRepository: unreachable,
        getDefaultRepository: unreachable,
      }),
    });
    const result = await Effect.runPromise(
      updateHandle(
        new Request(
          new URL("/api/admin/repositories/repo-nix", config.publicUrl),
          {
            method: "PUT",
            headers: {
              Cookie: `omp_gateway_admin=${token}`,
              Origin: config.publicUrl.toString(),
              "X-CSRF-Token": deriveCsrfToken(token),
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              id: "repo-nix",
              url: "https://example.com/repo.git",
              ref: "main",
              nixPackages: ["nodejs_22", "git", "nodejs_22"],
            }),
          },
        ),
      ),
    );
    expect(Option.getOrElse(result, () => null)?.status).toBe(200);
    expect(updatedPackages).toEqual(["git", "nodejs_22"]);
  });

  it("requires an admin session for the cache list", async () => {
    const response = await run(
      new Request(new URL("/api/admin/nix-cache", config.publicUrl), {
        method: "GET",
      }),
    );
    expect(response.status).toBe(401);
  });

  it("rejects cache pruning without CSRF and invalid cache keys", async () => {
    const withoutCsrf = new Request(
      new URL(`/api/admin/nix-cache/${"a".repeat(64)}/prune`, config.publicUrl),
      {
        method: "POST",
        headers: {
          Cookie: `omp_gateway_admin=${token}`,
          Origin: config.publicUrl.toString(),
        },
      },
    );
    expect((await run(withoutCsrf)).status).toBe(403);

    const invalid = await run(
      request("/api/admin/nix-cache/not-a-key/prune", {}),
    );
    expect(invalid.status).toBe(400);
    expect(await invalid.text()).toBe("Invalid Nix cache key");
  });

  it("lists and prunes Nix cache entries with an authenticated CSRF mutation", async () => {
    const cacheKey = "a".repeat(64);
    const pruned: Array<string> = [];
    const cacheHandle = createAdminHandle({
      ...deps,
      nixEnvironment: NixEnvironment.make({
        prepare: unreachable,
        list: () =>
          Effect.succeed([
            {
              cacheKey,
              nixpkgsFlakeRef:
                "github:NixOS/nixpkgs/0123456789abcdef0123456789abcdef01234567",
              packages: [],
              storePaths: [],
              pathEntries: [],
              sizeBytes: 4096,
              createdAt: 1,
              updatedAt: 2,
            },
          ]),
        prune: (key) => {
          pruned.push(key);
          return Effect.succeed(true);
        },
      }),
    });
    const cacheRun = async (requestValue: Request): Promise<Response> => {
      const result = await Effect.runPromise(cacheHandle(requestValue));
      return Option.match(result, {
        onNone: () => {
          throw new Error("expected an admin response");
        },
        onSome: (response) => response,
      });
    };

    const listed = await cacheRun(
      new Request(new URL("/api/admin/nix-cache", config.publicUrl), {
        headers: { Cookie: `omp_gateway_admin=${token}` },
      }),
    );
    expect(listed.status).toBe(200);
    expect((await listed.json()).entries[0]).toMatchObject({
      cacheKey,
      status: "ready",
      sizeBytes: 4096,
      lastUsedAt: 2,
    });

    const prunedResponse = await cacheRun(
      request(`/api/admin/nix-cache/${cacheKey}/prune`, {}),
    );
    expect(prunedResponse.status).toBe(200);
    expect(pruned).toEqual([cacheKey]);
  });

  it("returns 400 for malformed optional preview arrays", async () => {
    for (const field of ["issueLabels", "projectLabels"]) {
      const response = await run(
        request("/api/admin/preview", { [field]: "not-an-array" }),
      );
      expect(response.status).toBe(400);
    }
  });
});

describe("POST /api/admin/runs/:id/rerun", () => {
  const terminalRun: AgentRun = {
    sessionId: Schema.decodeUnknownSync(SessionId)(
      "22222222-2222-4222-8222-222222222222",
    ),
    organizationId,
    issueId: Option.some(issueId),
    repositoryId: Option.some(repositoryId),
    state: "succeeded",
    desiredState: "running",
    ompSessionId: Option.none(),
    ompSessionFile: Option.none(),
    workspacePath: Option.none(),
    teamId: Option.some(teamId),
    projectId: Option.some(projectId),
    attempt: 0,
    leaseOwner: Option.none(),
    leaseExpiresAt: Option.none(),
    lastActivityAt: Option.none(),
    terminalReason: Option.none(),
    nextAttemptAt: Option.none(),
    createdAt: 0,
    updatedAt: 0,
  };

  it("rejects unauthenticated and un-CSRF requests", async () => {
    const noCookie = new Request(
      new URL(
        "/api/admin/runs/22222222-2222-4222-8222-222222222222/rerun",
        config.publicUrl,
      ),
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      },
    );
    expect((await run(noCookie)).status).toBe(401);

    const noCsrf = new Request(
      new URL(
        "/api/admin/runs/22222222-2222-4222-8222-222222222222/rerun",
        config.publicUrl,
      ),
      {
        method: "POST",
        headers: {
          Cookie: `omp_gateway_admin=${token}`,
          Origin: config.publicUrl.toString(),
          "Content-Type": "application/json",
        },
        body: "{}",
      },
    );
    expect((await run(noCsrf)).status).toBe(403);
  });

  it("rejects reruns for runs without an issue or with an active run", async () => {
    const noIssue: AgentRun = {
      ...terminalRun,
      sessionId: Schema.decodeUnknownSync(SessionId)(
        "33333333-3333-4333-8333-333333333333",
      ),
      issueId: Option.none(),
    };
    const noIssueHandle = createAdminHandle({
      ...deps,
      runRepo: RunRepo.make({
        ...deps.runRepo,
        get: () => Effect.succeed(Option.some(noIssue)),
      }),
    });
    const noIssueResponse = await Effect.runPromise(
      noIssueHandle(
        request(
          "/api/admin/runs/33333333-3333-4333-8333-333333333333/rerun",
          {},
        ),
      ),
    );
    const noIssueResponseValue = Option.getOrElse(noIssueResponse, () => null);
    expect(noIssueResponseValue?.status).toBe(409);
    expect(await noIssueResponseValue?.text()).toBe(
      "Run is not linked to an issue",
    );
    const activeHandle = createAdminHandle({
      ...deps,
      runRepo: RunRepo.make({
        ...deps.runRepo,
        get: () => Effect.succeed(Option.some(terminalRun)),
        createIfNoActiveForIssue: () => Effect.succeed("active"),
      }),
      linearGateway: {
        createSessionOnIssue: () =>
          Effect.succeed("55555555-5555-4555-8555-555555555555"),
      },
    });
    const activeResponse = await Effect.runPromise(
      activeHandle(
        request(
          "/api/admin/runs/22222222-2222-4222-8222-222222222222/rerun",
          {},
        ),
      ),
    );
    const activeResponseValue = Option.getOrElse(activeResponse, () => null);
    expect(activeResponseValue?.status).toBe(409);
    expect(await activeResponseValue?.text()).toBe(
      "A run for this issue is already active",
    );
  });
  it("creates a new session, run, and dedupe-safe created input", async () => {
    const created: {
      sessionId?: string;
      createdRun?: boolean;
      enqueued?: boolean;
      createdRunInput?: unknown;
      payload?: unknown;
    } = {};
    const newSessionId = "55555555-5555-4555-8555-555555555555";
    const rerunHandle = createAdminHandle({
      ...deps,
      runRepo: RunRepo.make({
        ...deps.runRepo,
        get: () => Effect.succeed(Option.some(terminalRun)),
        createIfNoActiveForIssue: (input) => {
          created.createdRunInput = input;
          created.sessionId = input.sessionId;
          created.createdRun = true;
          return Effect.succeed("created");
        },
      }),
      runInputRepo: RunInputRepo.make({
        ...deps.runInputRepo,
        enqueue: (input) => {
          created.payload = input.payload;
          created.enqueued = true;
          return Effect.succeed(true);
        },
      }),
      linearGateway: {
        createSessionOnIssue: () => Effect.succeed(newSessionId),
      },
    });
    const response = await Effect.runPromise(
      rerunHandle(
        request(
          "/api/admin/runs/22222222-2222-4222-8222-222222222222/rerun",
          {},
        ),
      ),
    );
    const res = Option.getOrElse(response, () => null);
    expect(res?.status).toBe(200);
    expect(await res?.json()).toEqual({ sessionId: newSessionId });
    expect(created.sessionId).toBe(newSessionId);
    expect(created.createdRun).toBe(true);
    expect(created.enqueued).toBe(true);
    const createdInput = created.createdRunInput as {
      organizationId: unknown;
      issueId: unknown;
      repositoryId: unknown;
      teamId: unknown;
      projectId: unknown;
    };
    expect(createdInput.organizationId).toBe(organizationId);
    expect(createdInput.issueId).toEqual(Option.some(issueId));
    expect(createdInput.repositoryId).toEqual(Option.some(repositoryId));
    expect(createdInput.teamId).toEqual(Option.some(teamId));
    expect(createdInput.projectId).toEqual(Option.some(projectId));
    const payload = created.payload as Record<string, unknown>;
    expect(payload.repositoryId).toBe(repositoryId);
    expect(payload.automationDelegated).toBe(false);
  });
  it("maps Linear rate limits to 429 with retry delay", async () => {
    const rateLimitHandle = createAdminHandle({
      ...deps,
      runRepo: RunRepo.make({
        ...deps.runRepo,
        get: () => Effect.succeed(Option.some(terminalRun)),
      }),
      linearGateway: {
        createSessionOnIssue: () =>
          Effect.fail(
            new LinearRateLimitError({
              message: "Linear rate limit exceeded",
              retryAfterMs: 12_345,
            }),
          ),
      },
    });
    const response = await Effect.runPromise(
      rateLimitHandle(
        request(
          "/api/admin/runs/22222222-2222-4222-8222-222222222222/rerun",
          {},
        ),
      ),
    );
    const responseValue = Option.getOrElse(response, () => null);
    expect(responseValue?.status).toBe(429);
    expect(await responseValue?.text()).toContain("12345ms");
  });
});
