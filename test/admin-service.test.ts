import { Effect, Option, Redacted, Schema } from "effect";
import { describe, expect, it } from "vitest";
import { WorkspaceError } from "../src/domain/errors.js";
import { OrganizationId } from "../src/domain/ids.js";
import {
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
  RunRepo,
  WorkspaceRepo,
} from "../src/services/store/repositories.js";

const unreachable = (..._args: ReadonlyArray<unknown>) => Effect.never;
const token = "admin-token";
const organizationId = Schema.decodeUnknownSync(OrganizationId)(
  "11111111-1111-4111-8111-111111111111",
);
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
  webhookReplayWindowMs: 60_000,
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
    update: unreachable,
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
