import { Effect, Option, Redacted, Schema } from "effect";
import { describe, expect, it } from "vitest";
import { WorkspaceError } from "../src/domain/errors.js";
import { OrganizationId } from "../src/domain/ids.js";
import type { RepositoryRecord } from "../src/domain/models.js";
import {
  type AdminDeps,
  createAdminHandle,
  csrfHash,
  deriveCsrfToken,
} from "../src/services/admin.js";
import type { GatewayConfigShape } from "../src/services/config.js";
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
    materialize: (_sessionId: string, _repository: RepositoryRecord) =>
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

  it("returns 400 for malformed optional preview arrays", async () => {
    for (const field of ["issueLabels", "projectLabels"]) {
      const response = await run(
        request("/api/admin/preview", { [field]: "not-an-array" }),
      );
      expect(response.status).toBe(400);
    }
  });
});
