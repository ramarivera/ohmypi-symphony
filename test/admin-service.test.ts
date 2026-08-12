import { Effect, Option, Redacted, Schema } from "effect";
import { describe, expect, it } from "vitest";
import {
  DatabaseError,
  LinearRateLimitError,
  WorkspaceError,
} from "../src/domain/errors.js";
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
  McpServerRecord,
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
  McpServerRepo,
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
  githubAppId: undefined,
  githubAppPrivateKey: undefined,
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
    listCatchupCandidates: unreachable,
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
  mcpServerRepo: {
    listMcpServers: unreachable,
    createMcpServer: unreachable,
    updateMcpServer: unreachable,
    deleteMcpServer: unreachable,
  } as unknown as AdminDeps["mcpServerRepo"],
  mcpOAuth: {
    listStatuses: () => Effect.succeed(new Map()),
    startMcpAuthorization: unreachable,
    completeMcpAuthorization: unreachable,
    disconnect: unreachable,
  } as unknown as AdminDeps["mcpOAuth"],
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
    trigger: () => Effect.void,
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
    let createSessionCalls = 0;
    const activeHandle = createAdminHandle({
      ...deps,
      runRepo: RunRepo.make({
        ...deps.runRepo,
        get: () => Effect.succeed(Option.some(terminalRun)),
        hasActiveForIssue: () => Effect.succeed(true),
        createIfNoActiveForIssue: () => Effect.succeed("active"),
      }),
      linearGateway: {
        createSessionOnIssue: () => {
          createSessionCalls += 1;
          return Effect.succeed("55555555-5555-4555-8555-555555555555");
        },
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
    expect(createSessionCalls).toBe(0);
    const activeResponseValue = Option.getOrElse(activeResponse, () => null);
    expect(activeResponseValue?.status).toBe(409);
    expect(await activeResponseValue?.text()).toBe(
      "A run for this issue is already active",
    );
  });
  it("rejects nonterminal reruns before creating a Linear session", async () => {
    let createSessionCalls = 0;
    const nonterminalRun: AgentRun = {
      ...terminalRun,
      state: "running",
    };
    const nonterminalHandle = createAdminHandle({
      ...deps,
      runRepo: RunRepo.make({
        ...deps.runRepo,
        get: () => Effect.succeed(Option.some(nonterminalRun)),
      }),
      linearGateway: {
        createSessionOnIssue: () => {
          createSessionCalls += 1;
          return Effect.succeed("55555555-5555-4555-8555-555555555555");
        },
      },
    });
    const response = await Effect.runPromise(
      nonterminalHandle(
        request(
          "/api/admin/runs/22222222-2222-4222-8222-222222222222/rerun",
          {},
        ),
      ),
    );
    const responseValue = Option.getOrElse(response, () => null);
    expect(responseValue?.status).toBe(409);
    expect(await responseValue?.text()).toBe("Run is not terminal");
    expect(createSessionCalls).toBe(0);
  });
  it("returns 404 for runs belonging to another organization", async () => {
    const foreignRun: AgentRun = {
      ...terminalRun,
      organizationId: Schema.decodeUnknownSync(OrganizationId)(
        "99999999-9999-4999-8999-999999999999",
      ),
    };
    const foreignHandle = createAdminHandle({
      ...deps,
      runRepo: RunRepo.make({
        ...deps.runRepo,
        get: () => Effect.succeed(Option.some(foreignRun)),
      }),
      linearGateway: {
        createSessionOnIssue: () =>
          Effect.die(new Error("must not be called cross-org")),
      },
    });
    const response = await Effect.runPromise(
      foreignHandle(
        request(
          "/api/admin/runs/22222222-2222-4222-8222-222222222222/rerun",
          {},
        ),
      ),
    );
    expect(Option.getOrElse(response, () => null)?.status).toBe(404);
  });

  it("creates a new session, run, and dedupe-safe created input", async () => {
    const created: {
      sessionId?: string;
      createdRun?: boolean;
      enqueued?: boolean;
      createdRunInput?: unknown;
      createdInputId?: unknown;
      payload?: unknown;
    } = {};
    let reconcilerTriggers = 0;
    const newSessionId = "55555555-5555-4555-8555-555555555555";
    const rerunHandle = createAdminHandle({
      ...deps,
      runRepo: RunRepo.make({
        ...deps.runRepo,
        get: () => Effect.succeed(Option.some(terminalRun)),
        hasActiveForIssue: () => Effect.succeed(false),
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
          created.createdInputId = input.id;
          created.payload = input.payload;
          created.enqueued = true;
          return Effect.succeed(true);
        },
      }),
      linearGateway: {
        createSessionOnIssue: () => Effect.succeed(newSessionId),
      },
      reconciler: {
        ...deps.reconciler,
        trigger: () => {
          reconcilerTriggers += 1;
          return Effect.void;
        },
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
    expect(reconcilerTriggers).toBe(1);
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
    expect(created.createdInputId).toBe(`${newSessionId}:created`);
    const payload = created.payload as Record<string, unknown>;
    expect(payload.repositoryId).toBe(repositoryId);
    expect(payload.automationDelegated).toBe(false);
  });
  it("serializes overlapping reruns for the same issue before creating a session", async () => {
    let active = false;
    let createSessionCalls = 0;
    let releaseFirst = () => {};
    let signalFirstStarted = () => {};
    const firstStarted = new Promise<void>((resolve) => {
      signalFirstStarted = resolve;
    });
    const firstReleased = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const newSessionId = "55555555-5555-4555-8555-555555555555";
    const rerunHandle = createAdminHandle({
      ...deps,
      runRepo: RunRepo.make({
        ...deps.runRepo,
        get: () => Effect.succeed(Option.some(terminalRun)),
        hasActiveForIssue: () => Effect.succeed(active),
        createIfNoActiveForIssue: () => {
          if (active) return Effect.succeed("active");
          active = true;
          return Effect.succeed("created");
        },
      }),
      runInputRepo: RunInputRepo.make({
        ...deps.runInputRepo,
        enqueue: () => Effect.succeed(true),
      }),
      linearGateway: {
        createSessionOnIssue: () => {
          createSessionCalls += 1;
          signalFirstStarted();
          return createSessionCalls === 1
            ? Effect.promise(() => firstReleased.then(() => newSessionId))
            : Effect.succeed(newSessionId);
        },
      },
    });

    const firstResponsePromise = Effect.runPromise(
      rerunHandle(
        request(
          "/api/admin/runs/22222222-2222-4222-8222-222222222222/rerun",
          {},
        ),
      ),
    );
    await firstStarted;
    const secondResponsePromise = Effect.runPromise(
      rerunHandle(
        request(
          "/api/admin/runs/22222222-2222-4222-8222-222222222222/rerun",
          {},
        ),
      ),
    );
    releaseFirst();
    const [firstResponse, secondResponse] = await Promise.all([
      firstResponsePromise,
      secondResponsePromise,
    ]);
    const first = Option.getOrElse(firstResponse, () => null);
    const second = Option.getOrElse(secondResponse, () => null);
    expect(first?.status).toBe(200);
    expect(second?.status).toBe(409);
    expect(createSessionCalls).toBe(1);
  });
  it("maps Linear rate limits to 429 with retry delay", async () => {
    const rateLimitHandle = createAdminHandle({
      ...deps,
      runRepo: RunRepo.make({
        ...deps.runRepo,
        get: () => Effect.succeed(Option.some(terminalRun)),
        hasActiveForIssue: () => Effect.succeed(false),
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
describe("MCP admin endpoints", () => {
  it("requires CSRF and masks secret environment values", async () => {
    const server = Schema.decodeUnknownSync(McpServerRecord)({
      id: "mcp-admin",
      organizationId,
      name: "github",
      transport: "stdio",
      command: "node",
      args: ["server.js"],
      url: null,
      env: { API_TOKEN: "super-secret" },
      headers: { Authorization: "header-secret" },
      repositoryId: null,
      enabled: true,
      createdAt: 1,
      updatedAt: 1,
    });
    const mcpServerRepo = McpServerRepo.make({
      createMcpServer: unreachable,
      getMcpServer: () => Effect.succeed(Option.some(server)),
      listMcpServers: () => Effect.succeed([server]),
      updateMcpServer: unreachable,
      deleteMcpServer: unreachable,
    });
    const mcpHandle = createAdminHandle({
      ...deps,
      installationRepo: InstallationRepo.make({
        ...deps.installationRepo,
        get: () => Effect.succeed(Option.none()),
      }),
      workspaceRepo: WorkspaceRepo.make({
        ...deps.workspaceRepo,
        listRepositories: () => Effect.succeed([]),
      }),
      mcpServerRepo,
    });
    const noCsrf = await Effect.runPromise(
      mcpHandle(
        new Request(new URL("/api/admin/mcp-servers", config.publicUrl), {
          method: "POST",
          headers: {
            Cookie: `omp_gateway_admin=${token}`,
            Origin: config.publicUrl.toString(),
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            id: "mcp-new",
            name: "new",
            transport: "stdio",
            command: "node",
            args: [],
            env: {},
          }),
        }),
      ),
    );
    expect(Option.getOrElse(noCsrf, () => null)?.status).toBe(403);

    const bootstrap = Option.getOrThrow(
      await Effect.runPromise(
        mcpHandle(
          new Request(new URL("/api/admin/bootstrap", config.publicUrl), {
            headers: { Cookie: `omp_gateway_admin=${token}` },
          }),
        ),
      ),
    );
    const bootstrapBody = await bootstrap.json();
    expect(bootstrapBody.mcpServers[0].env).toEqual({ API_TOKEN: "•••" });
    expect(bootstrapBody.mcpServers[0].headers).toEqual({
      Authorization: "•••",
    });
    expect(JSON.stringify(bootstrapBody)).not.toContain("super-secret");

    const detail = Option.getOrThrow(
      await Effect.runPromise(
        mcpHandle(
          new Request(
            new URL("/api/admin/mcp-servers/mcp-admin", config.publicUrl),
            {
              headers: { Cookie: `omp_gateway_admin=${token}` },
            },
          ),
        ),
      ),
    );
    const detailBody = await detail.json();
    expect(detailBody.mcpServer.env).toEqual({ API_TOKEN: "•••" });
    expect(detailBody.mcpServer.headers).toEqual({
      Authorization: "•••",
    });
    expect(JSON.stringify(detailBody)).not.toContain("super-secret");
  });
  it("preserves masked headers on update and clears them with an explicit empty map", async () => {
    const server = Schema.decodeUnknownSync(McpServerRecord)({
      id: "mcp-headers",
      organizationId,
      name: "remote",
      transport: "http",
      command: null,
      args: [],
      url: "https://mcp.example.test",
      env: { API_TOKEN: "super-secret" },
      headers: { Authorization: "header-secret" },
      repositoryId: null,
      enabled: true,
      createdAt: 1,
      updatedAt: 1,
    });
    let received: Record<string, unknown> | undefined;
    const mcpServerRepo = McpServerRepo.make({
      listMcpServers: () => Effect.succeed([server]),
      createMcpServer: unreachable,
      getMcpServer: () => Effect.succeed(Option.some(server)),
      updateMcpServer: (_org, _id, input) => {
        received = input as Record<string, unknown>;
        return Effect.succeed(server);
      },
      deleteMcpServer: unreachable,
    });
    const handle = createAdminHandle({ ...deps, mcpServerRepo });
    const put = (headers: Record<string, string>) =>
      Effect.runPromise(
        handle(
          new Request(
            new URL("/api/admin/mcp-servers/mcp-headers", config.publicUrl),
            {
              method: "PUT",
              headers: {
                Cookie: `omp_gateway_admin=${token}`,
                Origin: config.publicUrl.toString(),
                "Content-Type": "application/json",
                "X-CSRF-Token": deriveCsrfToken(token),
              },
              body: JSON.stringify({
                id: "mcp-headers",
                name: "remote",
                transport: "http",
                command: null,
                args: [],
                url: "https://mcp.example.test",
                env: { API_TOKEN: "•••" },
                headers,
                repositoryId: null,
                enabled: true,
              }),
            },
          ),
        ),
      );
    await put({ Authorization: "•••", "X-Trace": "trace-secret" });
    expect(received?.headers).toEqual({
      Authorization: "header-secret",
      "X-Trace": "trace-secret",
    });
    await put({});
    expect(received?.headers).toEqual({});
  });
  it("rejects non-string repository ids before persistence", async () => {
    let called = false;
    const mcpServerRepo = McpServerRepo.make({
      ...deps.mcpServerRepo,
      createMcpServer: (..._args: ReadonlyArray<unknown>) => {
        called = true;
        return Effect.never;
      },
    });
    const handle = createAdminHandle({ ...deps, mcpServerRepo });
    const response = Option.getOrThrow(
      await Effect.runPromise(
        handle(
          new Request(new URL("/api/admin/mcp-servers", config.publicUrl), {
            method: "POST",
            headers: {
              Cookie: `omp_gateway_admin=${token}`,
              Origin: config.publicUrl.toString(),
              "Content-Type": "application/json",
              "X-CSRF-Token": deriveCsrfToken(token),
            },
            body: JSON.stringify({
              id: "numeric-repository",
              name: "numeric-repository",
              transport: "stdio",
              command: "node",
              repositoryId: 42,
            }),
          }),
        ),
      ),
    );
    expect(response.status).toBe(400);
    expect(called).toBe(false);
  });
  it("accepts OAuth scope without a client id for dynamic registration", async () => {
    let called = false;
    const mcpServerRepo = {
      ...deps.mcpServerRepo,
      createMcpServer: () => {
        called = true;
        return Effect.fail(new DatabaseError({ message: "sentinel" }));
      },
    } as AdminDeps["mcpServerRepo"];
    const handle = createAdminHandle({ ...deps, mcpServerRepo });
    const response = Option.getOrThrow(
      await Effect.runPromise(
        handle(
          new Request(new URL("/api/admin/mcp-servers", config.publicUrl), {
            method: "POST",
            headers: {
              Cookie: `omp_gateway_admin=${token}`,
              Origin: config.publicUrl.toString(),
              "Content-Type": "application/json",
              "X-CSRF-Token": deriveCsrfToken(token),
            },
            body: JSON.stringify({
              id: "scope-without-client",
              name: "scope-without-client",
              transport: "stdio",
              args: [],
              command: "node",
              oauthScope: "read",
            }),
          }),
        ),
      ),
    );
    expect(response.status).toBe(500);
    expect(called).toBe(true);
  });
  it("drops the previous OAuth secret when replacing the client id", async () => {
    const server = Schema.decodeUnknownSync(McpServerRecord)({
      id: "mcp-oauth-replace",
      organizationId,
      name: "oauth replace",
      transport: "http",
      command: null,
      args: [],
      url: "https://mcp.example.test",
      env: {},
      headers: {},
      oauthClient: {
        clientId: "old-client",
        clientSecret: "old-secret",
        scope: "read",
        tokenEndpointAuthMethod: "client_secret_basic",
      },
      repositoryId: null,
      enabled: true,
      createdAt: 1,
      updatedAt: 1,
    });
    let received: Record<string, unknown> | undefined;
    let disconnected = false;
    const mcpServerRepo = McpServerRepo.make({
      ...deps.mcpServerRepo,
      getMcpServer: () => Effect.succeed(Option.some(server)),
      updateMcpServer: (_org, _id, input) => {
        received = input as Record<string, unknown>;
        return Effect.succeed(server);
      },
    });
    const handle = createAdminHandle({
      ...deps,
      mcpServerRepo,
      mcpOAuth: {
        ...deps.mcpOAuth,
        disconnect: () => {
          disconnected = true;
          return Effect.void;
        },
      } as AdminDeps["mcpOAuth"],
    });
    const response = Option.getOrThrow(
      await Effect.runPromise(
        handle(
          new Request(
            new URL(
              "/api/admin/mcp-servers/mcp-oauth-replace",
              config.publicUrl,
            ),
            {
              method: "PUT",
              headers: {
                Cookie: `omp_gateway_admin=${token}`,
                Origin: config.publicUrl.toString(),
                "Content-Type": "application/json",
                "X-CSRF-Token": deriveCsrfToken(token),
              },
              body: JSON.stringify({
                id: "mcp-oauth-replace",
                name: "oauth replace",
                transport: "http",
                command: null,
                args: [],
                url: "https://mcp.example.test",
                env: {},
                headers: {},
                repositoryId: null,
                enabled: true,
                oauthClientId: "new-client",
                oauthTokenEndpointAuthMethod: "client_secret_post",
              }),
            },
          ),
        ),
      ),
    );
    expect(response.status).toBe(200);
    expect(received?.oauthClient).toEqual({
      clientId: "new-client",
      tokenEndpointAuthMethod: "client_secret_post",
    });
    expect(disconnected).toBe(true);
  });
  it("requires enabled to be a boolean and defaults omitted to true", async () => {
    const server = Schema.decodeUnknownSync(McpServerRecord)({
      id: "mcp-enabled",
      organizationId,
      name: "enabled",
      transport: "stdio",
      command: "node",
      args: [],
      url: null,
      env: {},
      headers: {},
      repositoryId: null,
      enabled: true,
      createdAt: 1,
      updatedAt: 1,
    });
    let receivedEnabled: boolean | undefined;
    let receivedArgs: ReadonlyArray<string> | undefined;
    const mcpServerRepo = {
      ...deps.mcpServerRepo,
      createMcpServer: (
        input: Parameters<AdminDeps["mcpServerRepo"]["createMcpServer"]>[0],
      ) => {
        receivedEnabled = input.enabled;
        receivedArgs = input.args;
        return Effect.succeed(server);
      },
    } as AdminDeps["mcpServerRepo"];
    const handle = createAdminHandle({ ...deps, mcpServerRepo });
    const runLocal = async (requestValue: Request) =>
      Option.getOrThrow(await Effect.runPromise(handle(requestValue)));
    const post = (
      enabled: unknown,
      includeEnabled = true,
      args: ReadonlyArray<string> = [],
    ) =>
      runLocal(
        request("/api/admin/mcp-servers", {
          id: "mcp-enabled",
          name: "enabled",
          transport: "stdio",
          command: "node",
          args,
          ...(includeEnabled ? { enabled } : {}),
        }),
      );
    expect((await post("false")).status).toBe(400);
    const commaArgs = ["--filter=a,b", '{"query":"x,y"}'];
    expect((await post(true, true, commaArgs)).status).toBe(201);
    expect(receivedArgs).toEqual(commaArgs);
    expect(receivedEnabled).toBe(true);
    expect((await post(false)).status).toBe(201);
    expect(receivedEnabled).toBe(false);
    expect((await post(undefined, false)).status).toBe(201);
    expect(receivedEnabled).toBe(true);
  });

  it("rejects invalid MCP header names and accepts RFC token names", async () => {
    const server = Schema.decodeUnknownSync(McpServerRecord)({
      id: "mcp-header-validation",
      organizationId,
      name: "header-validation",
      transport: "http",
      command: null,
      args: [],
      url: "https://mcp.example.test",
      env: {},
      headers: {},
      repositoryId: null,
      enabled: true,
      createdAt: 1,
      updatedAt: 1,
    });
    const mcpServerRepo = {
      ...deps.mcpServerRepo,
      createMcpServer: () => Effect.succeed(server),
    } as AdminDeps["mcpServerRepo"];
    const handle = createAdminHandle({ ...deps, mcpServerRepo });
    const runLocal = async (requestValue: Request) =>
      Option.getOrThrow(await Effect.runPromise(handle(requestValue)));
    const post = (headers: Record<string, string>) =>
      runLocal(
        request("/api/admin/mcp-servers", {
          id: "mcp-header-validation",
          name: "header-validation",
          transport: "http",
          command: null,
          args: [],
          url: "https://mcp.example.test",
          headers,
        }),
      );
    for (const name of ["Bad:Name", "Bad,Name", "Bad Name"]) {
      const response = await post({ [name]: "secret" });
      expect(response.status).toBe(400);
      expect(await response.text()).toContain(name);
    }
    expect(
      (await post({ Authorization: "secret", "X-Trace_2": "ok" })).status,
    ).toBe(201);
  });
});
