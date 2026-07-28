import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createHash, createHmac, randomBytes } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayStore } from "../src/store";
import { WorkspaceManager } from "../src/workspace";

process.env.PUBLIC_URL = "http://localhost:3000";
process.env.LINEAR_CLIENT_ID = "client";
process.env.LINEAR_CLIENT_SECRET = "secret";
process.env.LINEAR_WEBHOOK_SECRET = "webhook";
process.env.TOKEN_ENCRYPTION_KEY = Buffer.from(
  new Uint8Array(32).fill(9),
).toString("base64");
process.env.DATABASE_PATH = ":memory:";
process.env.WORKSPACE_ROOT = join(tmpdir(), "gateway-admin-workspaces");
process.env.PORT = "3000";
process.env.OMP_CLI_PATH = "omp";
process.env.LEASE_DURATION_MS = "60000";
process.env.WEBHOOK_REPLAY_WINDOW_MS = "60000";

const CSRF_SALT = "omp-gateway-admin-csrf";
const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;

let fetchHandler: (request: Request) => Promise<Response>;
let config: { publicUrl: URL };
let store: GatewayStore;
let reconciler: { stop: () => Promise<void> };
let tempRoot: string;

function rawToken(): string {
  return randomBytes(32).toString("base64url");
}

function tokenHash(rawToken: string): string {
  return createHash("sha256").update(rawToken).digest("base64url");
}

function deriveCsrfToken(rawToken: string): string {
  return createHmac("sha256", rawToken).update(CSRF_SALT).digest("base64url");
}

function csrfHash(rawCsrf: string): string {
  return createHash("sha256").update(rawCsrf).digest("base64url");
}

function createAdminSession(
  organizationId: string,
  now: number,
): { token: string; csrf: string } {
  const token = rawToken();
  const csrf = deriveCsrfToken(token);
  store.createAdminSession({
    organizationId,
    tokenHash: tokenHash(token),
    csrfTokenHash: csrfHash(csrf),
    expiresAt: now + SEVEN_DAYS_MS,
    now,
  });
  return { token, csrf };
}

function adminRequest(
  path: string,
  options: {
    method?: string;
    token?: string;
    csrf?: string;
    origin?: string;
    body?: unknown;
  } = {},
): Request {
  const headers = new Headers();
  if (options.token) {
    headers.set("Cookie", `omp_gateway_admin=${options.token}`);
  }
  if (options.csrf) {
    headers.set("X-CSRF-Token", options.csrf);
  }
  if (options.origin) {
    headers.set("Origin", options.origin);
  }
  const init: RequestInit = { method: options.method ?? "GET", headers };
  if (options.body !== undefined) {
    headers.set("Content-Type", "application/json");
    init.body = JSON.stringify(options.body);
  }
  return new Request(new URL(path, config.publicUrl).toString(), init);
}

beforeAll(async () => {
  // src/index.ts calls loadConfig() at the top of the module, so we must
  // set environment variables before any static import. Dynamic import is
  // the test boundary that lets us configure the environment first.
  const mod = await import("../src/index");
  fetchHandler = mod.buildFetch();
  config = mod.config;
  store = mod.store;
  reconciler = mod.reconciler;
  tempRoot = await mkdtemp(join(tmpdir(), "admin-workspace-"));
});

afterAll(async () => {
  await reconciler.stop();
  store.close();
  await rm(tempRoot, { recursive: true, force: true });
});

describe("admin control plane", () => {
  describe("configuration", () => {
    test("removes repository map path from config", () => {
      expect(config).not.toHaveProperty("repositoryMapPath");
    });
  });

  describe("database CRUD and organization isolation", () => {
    test("creates and isolates repositories by organization", () => {
      const a = store.createRepository({
        organizationId: "org-a",
        id: "repo",
        url: "https://example.com/a.git",
        ref: "main",
        teamIds: [],
        projectIds: [],
        labels: [],
        isDefault: false,
      });
      const b = store.createRepository({
        organizationId: "org-b",
        id: "repo",
        url: "https://example.com/b.git",
        ref: "main",
        teamIds: [],
        projectIds: [],
        labels: [],
        isDefault: false,
      });
      expect(a.organizationId).toBe("org-a");
      expect(b.organizationId).toBe("org-b");
      expect(store.listRepositories("org-a").map((r) => r.url)).toEqual([
        "https://example.com/a.git",
      ]);
      expect(store.listRepositories("org-b").map((r) => r.url)).toEqual([
        "https://example.com/b.git",
      ]);
    });

    test("enforces only one default repository per organization", () => {
      const first = store.createRepository({
        organizationId: "org-default",
        id: "first",
        url: "https://example.com/first.git",
        ref: "main",
        teamIds: [],
        projectIds: [],
        labels: [],
        isDefault: true,
      });
      expect(first.isDefault).toBe(true);
      const second = store.createRepository({
        organizationId: "org-default",
        id: "second",
        url: "https://example.com/second.git",
        ref: "main",
        teamIds: [],
        projectIds: [],
        labels: [],
        isDefault: true,
      });
      expect(second.isDefault).toBe(true);
      expect(store.getDefaultRepository("org-default")?.id).toBe("second");
      const updated = store.updateRepository("org-default", "first", {
        isDefault: true,
      });
      expect(updated.isDefault).toBe(true);
      expect(store.getDefaultRepository("org-default")?.id).toBe("first");
    });

    test("updates and deletes repositories", () => {
      store.createRepository({
        organizationId: "org-crud",
        id: "repo",
        url: "https://example.com/repo.git",
        ref: "main",
        teamIds: ["team"],
        projectIds: [],
        labels: [],
        isDefault: false,
      });
      const updated = store.updateRepository("org-crud", "repo", {
        ref: "develop",
        labels: ["frontend"],
      });
      expect(updated.ref).toBe("develop");
      expect(updated.labels).toEqual(["frontend"]);
      expect(store.getRepository("org-crud", "repo")?.ref).toBe("develop");
      expect(store.deleteRepository("org-crud", "repo")).toBe(true);
      expect(store.getRepository("org-crud", "repo")).toBeNull();
    });
  });

  describe("routing precedence and ambiguity", () => {
    function routingStore(now: number) {
      store.createRepository({
        organizationId: "routing",
        id: "default-repo",
        url: "https://example.com/default.git",
        ref: "main",
        teamIds: [],
        projectIds: [],
        labels: [],
        isDefault: true,
        now,
      });
      store.createRepository({
        organizationId: "routing",
        id: "issue-labels",
        url: "https://example.com/issue-labels.git",
        ref: "main",
        teamIds: [],
        projectIds: [],
        labels: ["ui"],
        isDefault: false,
        now,
      });
      store.createRepository({
        organizationId: "routing",
        id: "project-labels",
        url: "https://example.com/project-labels.git",
        ref: "main",
        teamIds: [],
        projectIds: [],
        labels: ["platform"],
        isDefault: false,
        now,
      });
      store.createRepository({
        organizationId: "routing",
        id: "project-id",
        url: "https://example.com/project-id.git",
        ref: "main",
        teamIds: [],
        projectIds: ["project-x"],
        labels: [],
        isDefault: false,
        now,
      });
      store.createRepository({
        organizationId: "routing",
        id: "team-id",
        url: "https://example.com/team-id.git",
        ref: "main",
        teamIds: ["ops"],
        projectIds: [],
        labels: [],
        isDefault: false,
        now,
      });
      store.createRepository({
        organizationId: "routing",
        id: "ambiguous-team",
        url: "https://example.com/ambiguous.git",
        ref: "main",
        teamIds: ["ops"],
        projectIds: [],
        labels: [],
        isDefault: false,
        now,
      });
      return store;
    }

    test("resolves with correct precedence", () => {
      const now = Date.now();
      routingStore(now);
      const workspaces = new WorkspaceManager(tempRoot, store);

      expect(
        workspaces.resolve({
          organizationId: "routing",
          repositoryId: "project-id",
          issueLabels: [],
          projectLabels: [],
          projectId: null,
          teamId: null,
        }),
      ).toMatchObject({ kind: "match", repository: { id: "project-id" } });

      expect(
        workspaces.resolve({
          organizationId: "routing",
          repositoryId: null,
          issueLabels: ["ui"],
          projectLabels: [],
          projectId: null,
          teamId: null,
        }),
      ).toMatchObject({ kind: "match", repository: { id: "issue-labels" } });

      expect(
        workspaces.resolve({
          organizationId: "routing",
          repositoryId: null,
          issueLabels: [],
          projectLabels: ["platform"],
          projectId: "project-x",
          teamId: null,
        }),
      ).toMatchObject({
        kind: "match",
        repository: { id: "project-labels" },
      });

      expect(
        workspaces.resolve({
          organizationId: "routing",
          repositoryId: null,
          issueLabels: [],
          projectLabels: [],
          projectId: "project-x",
          teamId: null,
        }),
      ).toMatchObject({ kind: "match", repository: { id: "project-id" } });

      expect(
        workspaces.resolve({
          organizationId: "routing",
          repositoryId: null,
          issueLabels: [],
          projectLabels: [],
          projectId: null,
          teamId: "ops",
        }),
      ).toMatchObject({ kind: "ambiguous" });

      expect(
        workspaces.resolve({
          organizationId: "routing",
          repositoryId: null,
          issueLabels: [],
          projectLabels: [],
          projectId: null,
          teamId: null,
        }),
      ).toMatchObject({ kind: "match", repository: { id: "default-repo" } });
    });
  });

  describe("OAuth session, cookie, and redirects", () => {
    test("creates an opaque admin session and cookie", async () => {
      const now = Date.now();
      const { token } = createAdminSession("org-oauth", now);
      const session = store.getAdminSession(tokenHash(token), now);
      expect(session).not.toBeNull();
      expect(session?.organizationId).toBe("org-oauth");

      const response = await fetchHandler(adminRequest("/", { token }));
      expect(response.status).toBe(302);
      expect(response.headers.get("location")).toBe("/admin");

      const admin = await fetchHandler(adminRequest("/admin", { token }));
      expect(admin.status).toBe(200);
    });

    test("landing page is public and admin requires authentication", async () => {
      const landing = await fetchHandler(adminRequest("/"));
      expect(landing.status).toBe(200);

      const dashboard = await fetchHandler(adminRequest("/admin"));
      expect(dashboard.status).toBe(302);
      expect(dashboard.headers.get("location")).toBe("/");
    });

    test("logout revokes the session and clears the cookie", async () => {
      const now = Date.now();
      const { token, csrf } = createAdminSession("org-logout", now);
      const logout = await fetchHandler(
        adminRequest("/api/admin/logout", {
          method: "POST",
          token,
          csrf,
          origin: config.publicUrl.toString(),
        }),
      );
      expect(logout.status).toBe(302);
      expect(logout.headers.get("location")).toBe("/");
      const clearCookie = logout.headers.get("set-cookie") ?? "";
      expect(clearCookie).toInclude("omp_gateway_admin=");
      expect(clearCookie).toInclude("Max-Age=0");
      expect(store.getAdminSession(tokenHash(token), now)).toBeNull();
    });
  });

  describe("admin API auth, CSRF, and CRUD", () => {
    async function bootstrap(
      organizationId: string,
      now: number,
    ): Promise<{ token: string; csrf: string }> {
      const { token, csrf } = createAdminSession(organizationId, now);
      const response = await fetchHandler(
        adminRequest("/api/admin/bootstrap", {
          token,
          origin: config.publicUrl.toString(),
        }),
      );
      expect(response.status).toBe(200);
      const body = (await response.json()) as { csrfToken: string };
      expect(body.csrfToken).toBe(csrf);
      return { token, csrf };
    }

    test("rejects unauthenticated and invalid requests", async () => {
      const boot = await fetchHandler(adminRequest("/api/admin/bootstrap"));
      expect(boot.status).toBe(401);

      const create = await fetchHandler(
        adminRequest("/api/admin/repositories", {
          method: "POST",
          body: { id: "x", url: "https://example.com/x.git", ref: "main" },
        }),
      );
      expect(create.status).toBe(401);
    });

    test("bootstrap returns a full non-secret installation", async () => {
      const now = Date.now();
      const { token } = createAdminSession("org-install", now);
      await store.putInstallation({
        organizationId: "org-install",
        appUserId: "app-user",
        accessToken: "secret-access",
        refreshToken: "secret-refresh",
        expiresAt: now + 60_000,
        scopes: ["read"],
        revokedAt: null,
        accessibleTeamIds: ["team-1"],
        canAccessAllPublicTeams: false,
      });
      const response = await fetchHandler(
        adminRequest("/api/admin/bootstrap", {
          token,
          origin: config.publicUrl.toString(),
        }),
      );
      expect(response.status).toBe(200);
      const body = (await response.json()) as {
        installation: {
          organizationId: string;
          appUserId: string;
          scopes: string[];
          revokedAt: null;
          accessibleTeamIds: string[] | null;
          canAccessAllPublicTeams: boolean | null;
          accessToken?: string;
          refreshToken?: string;
        };
      };
      expect(body.installation).toMatchObject({
        organizationId: "org-install",
        appUserId: "app-user",
        scopes: ["read"],
        revokedAt: null,
        accessibleTeamIds: ["team-1"],
        canAccessAllPublicTeams: false,
      });
      expect(body.installation).not.toHaveProperty("accessToken");
      expect(body.installation).not.toHaveProperty("refreshToken");
    });

    test("requires origin and csrf for mutations", async () => {
      const now = Date.now();
      const { token, csrf } = await bootstrap("org-csrf", now);

      const noOrigin = await fetchHandler(
        adminRequest("/api/admin/repositories", {
          method: "POST",
          token,
          csrf,
          body: { id: "x", url: "https://example.com/x.git", ref: "main" },
        }),
      );
      expect(noOrigin.status).toBe(403);

      const noCsrf = await fetchHandler(
        adminRequest("/api/admin/repositories", {
          method: "POST",
          token,
          origin: config.publicUrl.toString(),
          body: { id: "x", url: "https://example.com/x.git", ref: "main" },
        }),
      );
      expect(noCsrf.status).toBe(403);
    });

    test("creates, lists, updates, and deletes repositories", async () => {
      const now = Date.now();
      const { token, csrf } = await bootstrap("org-api", now);

      const create = await fetchHandler(
        adminRequest("/api/admin/repositories", {
          method: "POST",
          token,
          csrf,
          origin: config.publicUrl.toString(),
          body: {
            id: "api-repo",
            url: "https://example.com/api.git",
            ref: "main",
            teamIds: ["team-a"],
          },
        }),
      );
      expect(create.status).toBe(201);
      const created = (await create.json()) as { repository: { id: string } };
      expect(created.repository.id).toBe("api-repo");

      const list = await fetchHandler(
        adminRequest("/api/admin/repositories", {
          token,
          origin: config.publicUrl.toString(),
        }),
      );
      expect(list.status).toBe(200);
      const listed = (await list.json()) as {
        repositories: Array<{ id: string; teamIds: string[] }>;
      };
      expect(listed.repositories.map((r) => r.id)).toEqual(["api-repo"]);
      expect(listed.repositories[0]?.teamIds).toEqual(["team-a"]);

      const update = await fetchHandler(
        adminRequest("/api/admin/repositories/api-repo", {
          method: "PUT",
          token,
          csrf,
          origin: config.publicUrl.toString(),
          body: {
            id: "api-repo",
            url: "https://example.com/api.git",
            ref: "develop",
            labels: ["frontend"],
          },
        }),
      );
      expect(update.status).toBe(200);
      const updated = (await update.json()) as {
        repository: { ref: string; labels: string[] };
      };
      expect(updated.repository.ref).toBe("develop");
      expect(updated.repository.labels).toEqual(["frontend"]);

      const deleted = await fetchHandler(
        adminRequest("/api/admin/repositories/api-repo", {
          method: "DELETE",
          token,
          csrf,
          origin: config.publicUrl.toString(),
        }),
      );
      expect(deleted.status).toBe(204);

      const missing = await fetchHandler(
        adminRequest("/api/admin/repositories/api-repo", {
          method: "DELETE",
          token,
          csrf,
          origin: config.publicUrl.toString(),
        }),
      );
      expect(missing.status).toBe(404);
    });

    test("preview resolves and reports ambiguity", async () => {
      const now = Date.now();
      const { token, csrf } = await bootstrap("org-preview", now);

      store.createRepository({
        organizationId: "org-preview",
        id: "default",
        url: "https://example.com/default.git",
        ref: "main",
        teamIds: [],
        projectIds: [],
        labels: [],
        isDefault: true,
        now,
      });
      store.createRepository({
        organizationId: "org-preview",
        id: "issue",
        url: "https://example.com/issue.git",
        ref: "main",
        teamIds: [],
        projectIds: [],
        labels: ["ui"],
        isDefault: false,
        now,
      });
      store.createRepository({
        organizationId: "org-preview",
        id: "team-1",
        url: "https://example.com/team-1.git",
        ref: "main",
        teamIds: ["ops"],
        projectIds: [],
        labels: [],
        isDefault: false,
        now,
      });
      store.createRepository({
        organizationId: "org-preview",
        id: "team-2",
        url: "https://example.com/team-2.git",
        ref: "main",
        teamIds: ["ops"],
        projectIds: [],
        labels: [],
        isDefault: false,
        now,
      });

      const match = await fetchHandler(
        adminRequest("/api/admin/preview", {
          method: "POST",
          token,
          csrf,
          origin: config.publicUrl.toString(),
          body: { issueLabels: ["ui"] },
        }),
      );
      expect(match.status).toBe(200);
      const matchBody = (await match.json()) as {
        kind: string;
        repository: { id: string } | null;
      };
      expect(matchBody.kind).toBe("match");
      expect(matchBody.repository?.id).toBe("issue");

      const defaultPreview = await fetchHandler(
        adminRequest("/api/admin/preview", {
          method: "POST",
          token,
          csrf,
          origin: config.publicUrl.toString(),
          body: {},
        }),
      );
      expect(defaultPreview.status).toBe(200);
      const defaultBody = (await defaultPreview.json()) as {
        kind: string;
        repository: { id: string } | null;
      };
      expect(defaultBody.kind).toBe("match");
      expect(defaultBody.repository?.id).toBe("default");

      const ambiguous = await fetchHandler(
        adminRequest("/api/admin/preview", {
          method: "POST",
          token,
          csrf,
          origin: config.publicUrl.toString(),
          body: { teamId: "ops" },
        }),
      );
      expect(ambiguous.status).toBe(200);
      const ambiguousBody = (await ambiguous.json()) as {
        kind: string;
        repositories: Array<{ id: string }>;
      };
      expect(ambiguousBody.kind).toBe("ambiguous");
      expect(ambiguousBody.repositories.map((r) => r.id).sort()).toEqual([
        "team-1",
        "team-2",
      ]);
    });
  });
});
