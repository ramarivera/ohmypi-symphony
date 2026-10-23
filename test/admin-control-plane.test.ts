import { it } from "@effect/vitest";
import {
  ConfigProvider,
  Effect,
  Layer,
  Option,
  Schema,
  type Scope,
} from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/http";
import * as fc from "fast-check";
import { describe, expect } from "vitest";
import type {
  AppUserId,
  IssueId,
  McpServerId,
  OrganizationId,
  ProjectId,
  SessionId,
  SourceKey,
  TeamId,
  WorkspaceId,
} from "../src/domain/ids.js";
import { httpApp, routes } from "../src/http/router.js";
import {
  Admin,
  createAdminSession,
  csrfHash,
  deriveCsrfToken,
  tokenHash,
} from "../src/services/admin.js";
import { GatewayConfig } from "../src/services/config.js";
import { McpOAuth } from "../src/services/mcp-oauth.js";
import { NixEnvironment } from "../src/services/nix-environment.js";
import { OAuth } from "../src/services/oauth.js";
import { Reconciler } from "../src/services/reconciler.js";
import {
  AdminSessionRepo,
  DeliveryRepo,
  InstallationRepo,
  McpServerRepo,
  NixCacheRepo,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
  WorkspaceRepo,
} from "../src/services/store/repositories.js";
import { SqliteClientLive } from "../src/services/store/sqlite-client.js";
import { TokenCrypto } from "../src/services/token-crypto.js";
import { WebhookPipeline } from "../src/services/webhook.js";
import { Workspace } from "../src/services/workspace.js";
import { suitePaths } from "./fixtures/paths.js";

const suitePath = suitePaths();

const PUBLIC_URL = "http://localhost:3000";
const TEST_KEY = Buffer.from(new Uint8Array(32).fill(9)).toString("base64");

const configProvider = ConfigProvider.fromUnknown(
  Object.fromEntries(
    new Map<string, string>([
      ["PUBLIC_URL", PUBLIC_URL],
      ["LINEAR_CLIENT_ID", "client"],
      ["LINEAR_CLIENT_SECRET", "secret"],
      ["LINEAR_WEBHOOK_SECRET", "webhook"],
      ["TOKEN_ENCRYPTION_KEY", TEST_KEY],
      ["DATABASE_PATH", ":memory:"],
      ["WORKSPACE_ROOT", suitePath("gateway-admin-workspaces")],
      ["PORT", "3000"],
      ["OMP_CLI_PATH", "omp"],
      ["LEASE_DURATION_MS", "60000"],
      ["WEBHOOK_REPLAY_WINDOW_MS", "60000"],
      ["LOG_LEVEL", "silent"],
      ["NIX_BINARY_PATH", "nix"],
      [
        "NIXPKGS_FLAKE_REF",
        "github:NixOS/nixpkgs/0123456789abcdef0123456789abcdef01234567",
      ],
      ["NIX_ROOTS_DIR", suitePath("gateway-admin-nix-roots")],
      ["NIX_GC_MAX_BYTES", "1000000"],
    ]),
  ),
  { preserveEmptyStrings: true },
);

const reconcilerLayer = Layer.succeed(Reconciler, {
  status: () =>
    Effect.succeed({
      running: false,
      lastStartedAt: Option.none(),
      lastCompletedAt: Option.none(),
      lastError: Option.none(),
    }),
  tick: () => Effect.void,
  catchup: () => Effect.void,
  trigger: () => Effect.void,
  awaitTrigger: () => Effect.void,
});

const withApp = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    | Admin
    | AdminSessionRepo
    | DeliveryRepo
    | GatewayConfig
    | InstallationRepo
    | NixCacheRepo
    | NixEnvironment
    | OAuth
    | McpOAuth
    | McpServerRepo
    | RunEventRepo
    | RunInputRepo
    | RunRepo
    | WebhookPipeline
    | Reconciler
    | Workspace
    | WorkspaceRepo
    | Scope.Scope
  >,
) =>
  Effect.gen(function* () {
    yield* Effect.scope;
    const services = Layer.mergeAll(
      GatewayConfig.layer,
      TokenCrypto.layer,
      AdminSessionRepo.layer,
      DeliveryRepo.layer,
      InstallationRepo.layer,
      OAuth.layer,
      McpOAuth.layer,
      RunEventRepo.layer,
      RunInputRepo.layer,
      McpServerRepo.layer,
      RunRepo.layer,
      WebhookPipeline.layer,
      WorkspaceRepo.layer,
      Workspace.layer,
      NixCacheRepo.layer,
      NixEnvironment.layer,
      Admin.layer,
      reconcilerLayer,
    ).pipe(
      Layer.provide(
        Layer.mergeAll(
          SqliteClientLive(":memory:"),
          ConfigProvider.layer(configProvider),
        ),
      ),
    );
    return yield* effect.pipe(Effect.provide(services));
  });

const organizationId = (value: string): OrganizationId =>
  value as OrganizationId;
const appUserId = (value: string): AppUserId => value as AppUserId;
const workspaceId = (value: string): WorkspaceId => value as WorkspaceId;
const teamId = (value: string): TeamId => value as TeamId;
const projectId = (value: string): ProjectId => value as ProjectId;
const sessionId = (value: string): SessionId => value as SessionId;
const issueId = (value: string): IssueId => value as IssueId;
const sourceKey = (value: string): SourceKey => value as SourceKey;

const adminRequest = (
  path: string,
  options: {
    readonly method?: string;
    readonly token?: string;
    readonly csrf?: string;
    readonly origin?: string;
    readonly body?: unknown;
  } = {},
): Request => {
  const headers = new Headers();
  if (options.token)
    headers.set("Cookie", `omp_gateway_admin=${options.token}`);
  if (options.csrf) headers.set("X-CSRF-Token", options.csrf);
  if (options.origin) headers.set("Origin", options.origin);
  const init: RequestInit = { method: options.method ?? "GET", headers };
  if (options.body !== undefined) {
    headers.set("Content-Type", "application/json");
    init.body = JSON.stringify(options.body);
  }
  return new Request(new URL(path, PUBLIC_URL).toString(), init);
};

const fetchApp = (request: Request) =>
  Effect.gen(function* () {
    const app = yield* httpApp;
    const response = yield* app.pipe(
      Effect.provideService(
        HttpServerRequest.HttpServerRequest,
        HttpServerRequest.fromWeb(request),
      ),
    );
    return HttpServerResponse.toWeb(response);
  });

const createSession = (organization: string, now: number) =>
  Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const adminSessionRepo = yield* AdminSessionRepo;
    return yield* createAdminSession(
      { config, adminSessionRepo },
      organizationId(organization),
      now,
    );
  });

const requireSome = <A>(value: Option.Option<A>): A => {
  expect(Option.isSome(value)).toBe(true);
  if (Option.isSome(value)) return value.value;
  throw new Error("Expected Option.some");
};

describe("admin control plane", () => {
  it.live("removes repository map path from config", () =>
    withApp(
      Effect.gen(function* () {
        const config = yield* GatewayConfig;
        expect(config).not.toHaveProperty("repositoryMapPath");
      }),
    ),
  );

  it.live("creates and isolates repositories by organization", () =>
    withApp(
      Effect.gen(function* () {
        const repo = yield* WorkspaceRepo;
        const a = yield* repo.createRepository({
          organizationId: organizationId("org-a"),
          id: workspaceId("repo"),
          url: "https://github.com/test/a.git",
          ref: "main",
          teamIds: [],
          projectIds: [],
          labels: [],
          isDefault: false,
        });
        const b = yield* repo.createRepository({
          organizationId: organizationId("org-b"),
          id: workspaceId("repo"),
          url: "https://github.com/test/b.git",
          ref: "main",
          teamIds: [],
          projectIds: [],
          labels: [],
          isDefault: false,
        });
        expect(a.organizationId).toBe(organizationId("org-a"));
        expect(b.organizationId).toBe(organizationId("org-b"));
        expect(
          (yield* repo.listRepositories(organizationId("org-a"))).map(
            (r) => r.url,
          ),
        ).toEqual(["https://github.com/test/a.git"]);
        expect(
          (yield* repo.listRepositories(organizationId("org-b"))).map(
            (r) => r.url,
          ),
        ).toEqual(["https://github.com/test/b.git"]);
      }),
    ),
  );

  it.live("enforces only one default repository per organization", () =>
    withApp(
      Effect.gen(function* () {
        const repo = yield* WorkspaceRepo;
        const organization = organizationId("org-default");
        const first = yield* repo.createRepository({
          organizationId: organization,
          id: workspaceId("first"),
          url: "https://github.com/test/first.git",
          ref: "main",
          teamIds: [],
          projectIds: [],
          labels: [],
          isDefault: true,
        });
        expect(first.isDefault).toBe(true);
        const second = yield* repo.createRepository({
          organizationId: organization,
          id: workspaceId("second"),
          url: "https://github.com/test/second.git",
          ref: "main",
          teamIds: [],
          projectIds: [],
          labels: [],
          isDefault: true,
        });
        expect(second.isDefault).toBe(true);
        expect(
          requireSome(yield* repo.getDefaultRepository(organization)).id,
        ).toBe(workspaceId("second"));
        const updated = yield* repo.updateRepository(
          organization,
          workspaceId("first"),
          { isDefault: true },
        );
        expect(updated.isDefault).toBe(true);
        expect(
          requireSome(yield* repo.getDefaultRepository(organization)).id,
        ).toBe(workspaceId("first"));
      }),
    ),
  );

  it.live("updates and deletes repositories", () =>
    withApp(
      Effect.gen(function* () {
        const repo = yield* WorkspaceRepo;
        const organization = organizationId("org-crud");
        const id = workspaceId("repo");
        yield* repo.createRepository({
          organizationId: organization,
          id,
          url: "https://github.com/test/repo.git",
          ref: "main",
          teamIds: [teamId("team")],
          projectIds: [],
          labels: [],
          isDefault: false,
        });
        const mcp = yield* McpServerRepo;
        yield* mcp.createMcpServer({
          organizationId: organization,
          id: "mcp-scoped" as McpServerId,
          name: "repo-only",
          transport: "stdio",
          command: "node",
          args: ["server.js"],
          repositoryId: Option.some(id),
          now: 1,
        });
        const updated = yield* repo.updateRepository(organization, id, {
          ref: "develop",
          labels: ["frontend"],
        });
        expect(updated.ref).toBe("develop");
        expect(updated.labels).toEqual(["frontend"]);
        expect(
          requireSome(yield* repo.getRepository(organization, id)).ref,
        ).toBe("develop");
        expect(yield* repo.deleteRepository(organization, id)).toBe(true);
        expect(Option.isNone(yield* repo.getRepository(organization, id))).toBe(
          true,
        );
        expect(yield* mcp.listMcpServers(organization)).toHaveLength(0);
      }),
    ),
  );

  it.live("resolves repositories with correct precedence and ambiguity", () =>
    withApp(
      Effect.gen(function* () {
        const repo = yield* WorkspaceRepo;
        const workspace = yield* Workspace;
        const organization = organizationId("routing");
        const now = Date.now();
        const createRepository = (
          id: string,
          options: {
            readonly labels?: ReadonlyArray<string>;
            readonly projectIds?: ReadonlyArray<ProjectId>;
            readonly teamIds?: ReadonlyArray<TeamId>;
            readonly isDefault?: boolean;
          } = {},
        ) =>
          repo.createRepository({
            organizationId: organization,
            id: workspaceId(id),
            url: `https://github.com/test/${id}.git`,
            ref: "main",
            teamIds: options.teamIds ?? [],
            projectIds: options.projectIds ?? [],
            labels: options.labels ?? [],
            isDefault: options.isDefault ?? false,
            now,
          });
        yield* createRepository("default-repo", { isDefault: true });
        yield* createRepository("issue-labels", { labels: ["ui"] });
        yield* createRepository("project-labels", { labels: ["platform"] });
        yield* createRepository("project-id", {
          projectIds: [projectId("project-x")],
        });
        yield* createRepository("team-id", { teamIds: [teamId("ops")] });
        yield* createRepository("ambiguous-team", {
          teamIds: [teamId("ops")],
        });

        expect(
          yield* workspace.resolve({
            organizationId: organization,
            repositoryId: workspaceId("project-id"),
            issueLabels: [],
            projectLabels: [],
            projectId: null,
            teamId: null,
          }),
        ).toMatchObject({
          kind: "match",
          repository: { id: workspaceId("project-id") },
        });
        expect(
          yield* workspace.resolve({
            organizationId: organization,
            repositoryId: null,
            issueLabels: ["ui"],
            projectLabels: [],
            projectId: null,
            teamId: null,
          }),
        ).toMatchObject({
          kind: "match",
          repository: { id: workspaceId("issue-labels") },
        });
        expect(
          yield* workspace.resolve({
            organizationId: organization,
            repositoryId: null,
            issueLabels: [],
            projectLabels: ["platform"],
            projectId: projectId("project-x"),
            teamId: null,
          }),
        ).toMatchObject({
          kind: "match",
          repository: { id: workspaceId("project-labels") },
        });
        expect(
          yield* workspace.resolve({
            organizationId: organization,
            repositoryId: null,
            issueLabels: [],
            projectLabels: [],
            projectId: projectId("project-x"),
            teamId: null,
          }),
        ).toMatchObject({
          kind: "match",
          repository: { id: workspaceId("project-id") },
        });
        expect(
          yield* workspace.resolve({
            organizationId: organization,
            repositoryId: null,
            issueLabels: [],
            projectLabels: [],
            projectId: null,
            teamId: teamId("ops"),
          }),
        ).toMatchObject({ kind: "ambiguous" });
        expect(
          yield* workspace.resolve({
            organizationId: organization,
            repositoryId: null,
            issueLabels: [],
            projectLabels: [],
            projectId: null,
            teamId: null,
          }),
        ).toMatchObject({
          kind: "match",
          repository: { id: workspaceId("default-repo") },
        });
      }),
    ),
  );

  it.live(
    "creates opaque sessions, serves public routes, and protects admin routes",
    () =>
      withApp(
        Effect.gen(function* () {
          const now = Date.now();
          const sessions = yield* AdminSessionRepo;
          const { token } = yield* createSession("org-oauth", now);
          const session = yield* sessions.get(tokenHash(token), now);
          expect(requireSome(session).organizationId).toBe(
            organizationId("org-oauth"),
          );

          const root = yield* fetchApp(adminRequest("/", { token }));
          expect(root.status).toBe(302);
          expect(root.headers.get("location")).toBe("/admin");
          expect(
            (yield* fetchApp(adminRequest("/admin", { token }))).status,
          ).toBe(200);

          expect((yield* fetchApp(adminRequest("/"))).status).toBe(200);
          const dashboard = yield* fetchApp(adminRequest("/admin"));
          expect(dashboard.status).toBe(302);
          expect(dashboard.headers.get("location")).toContain(
            "/oauth/start?returnTo=",
          );
        }),
      ),
  );

  it.live("revokes sessions and clears the admin cookie on logout", () =>
    withApp(
      Effect.gen(function* () {
        const now = Date.now();
        const sessions = yield* AdminSessionRepo;
        const { token, csrf } = yield* createSession("org-logout", now);
        const logout = yield* fetchApp(
          adminRequest("/api/admin/logout", {
            method: "POST",
            token,
            csrf,
            origin: PUBLIC_URL,
          }),
        );
        expect(logout.status).toBe(302);
        expect(logout.headers.get("location")).toBe("/");
        const clearCookie = logout.headers.get("set-cookie") ?? "";
        expect(clearCookie).toContain("omp_gateway_admin=");
        expect(clearCookie).toContain("Max-Age=0");
        expect(Option.isNone(yield* sessions.get(tokenHash(token), now))).toBe(
          true,
        );
      }),
    ),
  );

  it.live(
    "enforces admin authentication, CSRF, origin, and repository CRUD",
    () =>
      withApp(
        Effect.gen(function* () {
          const bootstrap = (organization: string, now: number) =>
            Effect.gen(function* () {
              const session = yield* createSession(organization, now);
              const response = yield* fetchApp(
                adminRequest("/api/admin/bootstrap", {
                  token: session.token,
                  origin: PUBLIC_URL,
                }),
              );
              expect(response.status).toBe(200);
              const body = (yield* Effect.promise(() => response.json())) as {
                readonly csrfToken: string;
              };
              expect(body.csrfToken).toBe(session.csrf);
              return session;
            });

          expect(
            (yield* fetchApp(adminRequest("/api/admin/bootstrap"))).status,
          ).toBe(401);
          expect(
            (yield* fetchApp(
              adminRequest("/api/admin/repositories", {
                method: "POST",
                body: {
                  id: "x",
                  url: "https://github.com/test/x.git",
                  ref: "main",
                },
              }),
            )).status,
          ).toBe(401);

          const now = Date.now();
          const { token, csrf } = yield* bootstrap("org-api", now);
          expect(
            (yield* fetchApp(
              adminRequest("/api/admin/repositories", {
                method: "POST",
                token,
                csrf,
                body: {
                  id: "x",
                  url: "https://github.com/test/x.git",
                  ref: "main",
                },
              }),
            )).status,
          ).toBe(403);
          expect(
            (yield* fetchApp(
              adminRequest("/api/admin/repositories", {
                method: "POST",
                token,
                origin: PUBLIC_URL,
                body: {
                  id: "x",
                  url: "https://github.com/test/x.git",
                  ref: "main",
                },
              }),
            )).status,
          ).toBe(403);

          const create = yield* fetchApp(
            adminRequest("/api/admin/repositories", {
              method: "POST",
              token,
              csrf,
              origin: PUBLIC_URL,
              body: {
                id: "api-repo",
                url: "https://github.com/test/api.git",
                ref: "main",
                teamIds: ["team-a"],
              },
            }),
          );
          expect(create.status).toBe(201);
          expect(yield* Effect.promise(() => create.json())).toMatchObject({
            repository: { id: "api-repo" },
          });

          const list = yield* fetchApp(
            adminRequest("/api/admin/repositories", {
              token,
              origin: PUBLIC_URL,
            }),
          );
          expect(list.status).toBe(200);
          expect(yield* Effect.promise(() => list.json())).toEqual({
            repositories: [
              expect.objectContaining({ id: "api-repo", teamIds: ["team-a"] }),
            ],
          });

          const update = yield* fetchApp(
            adminRequest("/api/admin/repositories/api-repo", {
              method: "PUT",
              token,
              csrf,
              origin: PUBLIC_URL,
              body: {
                id: "api-repo",
                url: "https://github.com/test/api.git",
                ref: "develop",
                labels: ["frontend"],
              },
            }),
          );
          expect(update.status).toBe(200);
          expect(yield* Effect.promise(() => update.json())).toMatchObject({
            repository: { ref: "develop", labels: ["frontend"] },
          });

          const deleted = yield* fetchApp(
            adminRequest("/api/admin/repositories/api-repo", {
              method: "DELETE",
              token,
              csrf,
              origin: PUBLIC_URL,
            }),
          );
          expect(deleted.status).toBe(204);
          expect(
            (yield* fetchApp(
              adminRequest("/api/admin/repositories/api-repo", {
                method: "DELETE",
                token,
                csrf,
                origin: PUBLIC_URL,
              }),
            )).status,
          ).toBe(404);
        }),
      ),
  );

  it.live(
    "authenticates run details, scopes them to their org, and redacts HTML and JSON",
    () =>
      withApp(
        Effect.gen(function* () {
          const now = Date.UTC(2026, 6, 31, 12, 0, 0);
          const runs = yield* RunRepo;
          const events = yield* RunEventRepo;
          const id = sessionId("session/private");
          yield* runs.create({
            sessionId: id,
            organizationId: organizationId("org-runs"),
            issueId: Option.some(issueId("issue-runs")),
            now,
          });
          yield* runs.update(id, {
            state: "succeeded",
            terminalReason: Option.some(
              "Authorization: Bearer terminal-secret https://example.test/?token=query-secret",
            ),
            lastActivityAt: Option.some(now + 1_000),
          });
          yield* events.upsert({
            sourceKey: sourceKey("input:run-created"),
            sessionId: id,
            kind: "input:created",
            level: "info",
            text: "Authorization: Bearer payload-secret https://example.test/?signature=payload-signature",
            payload: {
              accessToken: "payload-access-token",
              agentSession: {
                issue: {
                  identifier: "TEAM-123",
                  title: "Fix the run page",
                  url: "https://linear.app/issue/TEAM-123",
                },
              },
            },
            status: "completed",
            now,
          });
          const path = `/runs/${encodeURIComponent(id)}`;
          const { token } = yield* createSession("org-runs", Date.now());
          const { token: otherToken } = yield* createSession(
            "org-other",
            Date.now(),
          );
          const unauthenticated = yield* fetchApp(adminRequest(path));
          expect(unauthenticated.status).toBe(302);
          expect(unauthenticated.headers.get("location")).toContain(
            "returnTo=",
          );
          expect((yield* fetchApp(adminRequest(`${path}.json`))).status).toBe(
            401,
          );
          expect(
            (yield* fetchApp(adminRequest(path, { token: otherToken }))).status,
          ).toBe(404);
          expect(
            (yield* fetchApp(
              adminRequest(`${path}.json`, { token: otherToken }),
            )).status,
          ).toBe(404);

          const html = yield* fetchApp(adminRequest(path, { token }));
          expect(html.status).toBe(200);
          expect(html.headers.get("content-type")).toContain("text/html");
          const page = yield* Effect.promise(() => html.text());
          expect(page).toContain("Run details");
          expect(page).toContain("TEAM-123");
          expect(page).toContain('data-run-level-toggle="debug"');
          expect(page).toContain('data-run-level-toggle="error"');
          expect(page).toContain("ohmypi-admin-appearance");
          expect(page).toContain("redacted");
          expect(page).not.toContain("terminal-secret");
          expect(page).not.toContain("payload-secret");
          expect(page).not.toContain("payload-signature");
          expect(page).not.toContain("payload-access-token");

          const json = yield* fetchApp(
            new Request(new URL(path, PUBLIC_URL).toString(), {
              headers: {
                Accept: "application/json",
                Cookie: `omp_gateway_admin=${token}`,
              },
            }),
          );
          expect(json.status).toBe(200);
          expect(json.headers.get("content-type")).toContain(
            "application/json",
          );
          const detail = (yield* Effect.promise(() => json.json())) as {
            readonly sessionId: string;
            readonly state: string;
            readonly attempt: number;
            readonly run: { readonly terminalReason: string | null };
            readonly events: ReadonlyArray<{
              readonly sourceKey: string;
              readonly text: string | null;
            }>;
          };
          expect(detail).toMatchObject({
            sessionId: id,
            state: "succeeded",
            attempt: 0,
          });
          expect(detail.run.terminalReason).toContain(
            "Authorization: Bearer redacted",
          );
          expect(
            detail.events.find(
              (event) => event.sourceKey === "input:run-created",
            ),
          ).toMatchObject({
            sourceKey: "input:run-created",
            text: "Authorization: Bearer redacted https://example.test/?signature=redacted",
          });
          expect(JSON.stringify(detail)).not.toContain("query-secret");

          const suffix = yield* fetchApp(
            adminRequest(`${path}.json`, { token }),
          );
          expect(suffix.status).toBe(200);
          expect(suffix.headers.get("content-type")).toContain(
            "application/json",
          );
        }),
      ),
  );

  it.live("redacts installation secrets from bootstrap", () =>
    withApp(
      Effect.gen(function* () {
        const now = Date.now();
        const installations = yield* InstallationRepo;
        const { token } = yield* createSession("org-install", now);
        yield* installations.put({
          organizationId: organizationId("org-install"),
          appUserId: appUserId("app-user"),
          accessToken: "secret-access",
          refreshToken: "secret-refresh",
          expiresAt: now + 60_000,
          scopes: ["read"],
          revokedAt: Option.none(),
          accessibleTeamIds: Option.some([teamId("team-1")]),
          canAccessAllPublicTeams: Option.some(false),
        });
        const response = yield* fetchApp(
          adminRequest("/api/admin/bootstrap", { token, origin: PUBLIC_URL }),
        );
        expect(response.status).toBe(200);
        const body = (yield* Effect.promise(() => response.json())) as {
          readonly installation: Record<string, unknown>;
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
      }),
    ),
  );

  it.live("previews matching, default, and ambiguous repositories", () =>
    withApp(
      Effect.gen(function* () {
        const now = Date.now();
        const repo = yield* WorkspaceRepo;
        const organization = organizationId("org-preview");
        const { token, csrf } = yield* createSession("org-preview", now);
        const createRepository = (
          id: string,
          options: {
            readonly labels?: ReadonlyArray<string>;
            readonly teamIds?: ReadonlyArray<TeamId>;
            readonly isDefault?: boolean;
          } = {},
        ) =>
          repo.createRepository({
            organizationId: organization,
            id: workspaceId(id),
            url: `https://github.com/test/${id}.git`,
            ref: "main",
            teamIds: options.teamIds ?? [],
            projectIds: [],
            labels: options.labels ?? [],
            isDefault: options.isDefault ?? false,
            now,
          });
        yield* createRepository("default", { isDefault: true });
        yield* createRepository("issue", { labels: ["ui"] });
        yield* createRepository("team-1", { teamIds: [teamId("ops")] });
        yield* createRepository("team-2", { teamIds: [teamId("ops")] });

        const preview = (body: unknown) =>
          fetchApp(
            adminRequest("/api/admin/preview", {
              method: "POST",
              token,
              csrf,
              origin: PUBLIC_URL,
              body,
            }),
          );
        const match = yield* preview({ issueLabels: ["ui"] });
        expect(match.status).toBe(200);
        expect(yield* Effect.promise(() => match.json())).toMatchObject({
          kind: "match",
          repository: { id: "issue" },
        });
        const defaultPreview = yield* preview({});
        expect(defaultPreview.status).toBe(200);
        expect(
          yield* Effect.promise(() => defaultPreview.json()),
        ).toMatchObject({
          kind: "match",
          repository: { id: "default" },
        });
        const ambiguous = yield* preview({ teamId: "ops" });
        expect(ambiguous.status).toBe(200);
        const ambiguousBody = (yield* Effect.promise(() =>
          ambiguous.json(),
        )) as {
          readonly kind: string;
          readonly repositories: ReadonlyArray<{ readonly id: string }>;
        };
        expect(ambiguousBody.kind).toBe("ambiguous");
        expect(
          ambiguousBody.repositories.map((repository) => repository.id).sort(),
        ).toEqual(["team-1", "team-2"]);
      }),
    ),
  );
});

describe("admin run-event pagination", () => {
  const seed = (count: number) =>
    Effect.gen(function* () {
      const id = sessionId("pagination/run");
      const runs = yield* RunRepo;
      const events = yield* RunEventRepo;
      yield* runs.create({
        sessionId: id,
        organizationId: organizationId("org-pagination"),
        issueId: Option.none(),
        now: 1000,
      });
      const keys = Array.from(
        { length: count },
        (_, index) => `event:${String(index).padStart(4, "0")}:雪`,
      );
      // Insert in reverse order and share timestamps to exercise the tie-breaker.
      for (let index = keys.length - 1; index >= 0; index -= 1) {
        const key = keys[index];
        if (key === undefined) throw new Error("Missing event fixture");
        yield* events.upsert({
          sessionId: id,
          sourceKey: sourceKey(key),
          kind: "action",
          level: "info",
          now: 1000 + Math.floor(index / 5),
        });
      }
      const { token } = yield* createSession("org-pagination", Date.now());
      return { path: `/runs/${encodeURIComponent(id)}`, token, keys };
    });

  const detailSchema = Schema.Struct({
    sessionId: Schema.String,
    run: Schema.Struct({ sessionId: Schema.String }),
    events: Schema.Array(Schema.Struct({ sourceKey: Schema.String })),
  });
  const readDetail = (response: Response) =>
    Effect.promise(() => response.json()).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(detailSchema)),
    );
  const nextPath = (response: Response): string | null => {
    const link = response.headers.get("link");
    if (link === null) return null;
    const match = /^<([^>]+)>; rel="next"$/u.exec(link);
    if (match?.[1] === undefined) throw new Error("Invalid continuation link");
    return match[1];
  };

  it.live("defaults to 100 events and preserves the JSON body shape", () =>
    withApp(
      Effect.gen(function* () {
        const { path, token, keys } = yield* seed(105);
        const response = yield* fetchApp(
          adminRequest(`${path}.json`, { token }),
        );
        expect(response.status).toBe(200);
        const body: unknown = yield* Effect.promise(() => response.json());
        expect(Object.keys(body as object).sort()).toEqual([
          "attempt",
          "events",
          "issue",
          "lastActivityAt",
          "run",
          "sessionId",
          "state",
        ]);
        const detail = yield* Schema.decodeUnknownEffect(detailSchema)(body);
        expect(detail.events.map((event) => event.sourceKey)).toEqual(
          keys.slice(0, 100),
        );
        const next = nextPath(response);
        expect(next).not.toBeNull();
        if (next === null) throw new Error("Missing default continuation");
        expect(new URL(next, PUBLIC_URL).searchParams.get("limit")).toBe("100");
        const tail = yield* fetchApp(adminRequest(next, { token }));
        expect(
          (yield* readDetail(tail)).events.map((event) => event.sourceKey),
        ).toEqual(keys.slice(100));
        expect(nextPath(tail)).toBeNull();
      }),
    ),
  );

  it.live(
    "accepts 1000 and rejects malformed or out-of-range limits with 400",
    () =>
      withApp(
        Effect.gen(function* () {
          const { path, token, keys } = yield* seed(1001);
          const response = yield* fetchApp(
            adminRequest(`${path}.json?limit=1000`, { token }),
          );
          expect(response.status).toBe(200);
          expect(
            (yield* readDetail(response)).events.map(
              (event) => event.sourceKey,
            ),
          ).toEqual(keys.slice(0, 1000));
          const next = nextPath(response);
          if (next === null)
            throw new Error("Missing maximum-limit continuation");
          const tail = yield* fetchApp(adminRequest(next, { token }));
          expect(
            (yield* readDetail(tail)).events.map((event) => event.sourceKey),
          ).toEqual(keys.slice(1000));
          expect(nextPath(tail)).toBeNull();
          for (const limit of [
            "1001",
            "0",
            "-1",
            "1.5",
            "NaN",
            "Infinity",
            "abc",
            "",
          ]) {
            const invalid = yield* fetchApp(
              adminRequest(`${path}.json?limit=${limit}`, { token }),
            );
            expect(invalid.status).toBe(400);
          }
        }),
      ),
  );

  it.live(
    "continues through every event exactly once in repository order",
    () =>
      withApp(
        Effect.gen(function* () {
          const { path, token, keys } = yield* seed(21);
          let next: string | null = `${path}.json?limit=7`;
          const seen: string[] = [];
          let pages = 0;
          while (next !== null) {
            if (++pages > 3) throw new Error("Pagination did not terminate");
            const response = yield* fetchApp(adminRequest(next, { token }));
            expect(response.status).toBe(200);
            const detail = yield* readDetail(response);
            expect(detail.events).toHaveLength(7);
            seen.push(...detail.events.map((event) => event.sourceKey));
            next = nextPath(response);
          }
          expect(pages).toBe(3);
          expect(seen).toEqual(keys);
          expect(new Set(seen).size).toBe(keys.length);
        }),
      ),
  );

  it.live("renders a usable HTML continuation and supports Accept JSON", () =>
    withApp(
      Effect.gen(function* () {
        const { path, token, keys } = yield* seed(2);
        const response = yield* fetchApp(
          adminRequest(`${path}?limit=1`, { token }),
        );
        expect(response.status).toBe(200);
        const next = nextPath(response);
        if (next === null) throw new Error("Missing HTML continuation");
        const markup = yield* Effect.promise(() => response.text());
        expect(markup).toContain(
          `<a rel="next" href="${next.replaceAll("&", "&amp;")}">Next events</a>`,
        );
        expect(markup).toContain(
          'window.location.pathname + ".json" + window.location.search',
        );
        const request = adminRequest(next, { token });
        request.headers.set("Accept", "application/json");
        const tail = yield* fetchApp(request);
        expect(tail.headers.get("content-type")).toContain("application/json");
        expect(
          (yield* readDetail(tail)).events.map((event) => event.sourceKey),
        ).toEqual(keys.slice(1));
        expect(nextPath(tail)).toBeNull();
        const finalHtml = yield* fetchApp(adminRequest(next, { token }));
        expect(yield* Effect.promise(() => finalHtml.text())).not.toContain(
          'rel="next"',
        );
      }),
    ),
  );

  it.live("keeps linked issue metadata on continuation pages", () =>
    withApp(
      Effect.gen(function* () {
        const { path, token } = yield* seed(3);
        const events = yield* RunEventRepo;
        yield* events.upsert({
          sessionId: sessionId("pagination/run"),
          sourceKey: sourceKey("event:0000:created"),
          kind: "created",
          level: "info",
          now: 999,
          payload: {
            agentSession: {
              issue: {
                identifier: "SYM-23",
                title: "Paginate run events",
                url: "https://linear.app/acme/issue/SYM-23",
              },
            },
          },
        });
        const issueSchema = Schema.Struct({
          issue: Schema.Struct({
            identifier: Schema.NullOr(Schema.String),
            title: Schema.NullOr(Schema.String),
            url: Schema.NullOr(Schema.String),
          }),
        });
        const expected = {
          identifier: "SYM-23",
          title: "Paginate run events",
          url: "https://linear.app/acme/issue/SYM-23",
        };
        let next: string | null = `${path}.json?limit=1`;
        let pages = 0;
        while (next !== null) {
          if (++pages > 4) throw new Error("Pagination did not terminate");
          const response = yield* fetchApp(adminRequest(next, { token }));
          const body: unknown = yield* Effect.promise(() => response.json());
          const detail = yield* Schema.decodeUnknownEffect(issueSchema)(body);
          expect(detail.issue).toEqual(expected);
          next = nextPath(response);
        }
        expect(pages).toBe(4);
      }),
    ),
  );

  it.live(
    "rejects malformed cursors with 400 and keeps authentication first",
    () =>
      withApp(
        Effect.gen(function* () {
          const { path, token } = yield* seed(0);
          const encode = (text: string) =>
            Buffer.from(text).toString("base64url");
          const cursors = [
            "",
            "%%%",
            "a",
            encode("not json"),
            encode("null"),
            encode("[]"),
            encode("{}"),
            encode('{"createdAt":"1000","sourceKey":"event"}'),
            encode('{"createdAt":1e400,"sourceKey":"event"}'),
            encode('{"createdAt":1000,"sourceKey":7}'),
          ];
          for (const cursor of cursors) {
            for (const suffix of ["", ".json"]) {
              const response = yield* fetchApp(
                adminRequest(
                  `${path}${suffix}?cursor=${encodeURIComponent(cursor)}`,
                  { token },
                ),
              );
              expect(response.status).toBe(400);
            }
          }
          expect(
            (yield* fetchApp(adminRequest(`${path}.json?cursor=%%%`))).status,
          ).toBe(401);
          expect(
            (yield* fetchApp(adminRequest(`${path}?limit=0`))).status,
          ).toBe(302);
          const empty = yield* fetchApp(
            adminRequest(`${path}.json`, { token }),
          );
          expect(empty.status).toBe(200);
          expect((yield* readDetail(empty)).events).toEqual([]);
          expect(nextPath(empty)).toBeNull();
        }),
      ),
  );
});

describe("admin session and routing invariants", () => {
  it("token and CSRF derivations are deterministic and separate input domains", () =>
    fc.assert(
      fc.property(
        fc.record({
          rawA: fc.string(),
          rawB: fc.string(),
          csrfA: fc.string(),
          csrfB: fc.string(),
        }),
        ({ rawA, rawB, csrfA, csrfB }) => {
          expect(tokenHash(rawA)).toBe(tokenHash(rawA));
          expect(deriveCsrfToken(rawA)).toBe(deriveCsrfToken(rawA));
          expect(csrfHash(csrfA)).toBe(csrfHash(csrfA));
          if (rawA !== rawB)
            expect(deriveCsrfToken(rawA)).not.toBe(deriveCsrfToken(rawB));
          if (csrfA !== csrfB)
            expect(csrfHash(csrfA)).not.toBe(csrfHash(csrfB));
        },
      ),
    ));

  it.effect(
    "repository resolution never returns a repository owned by another organization",
    () =>
      Effect.gen(function* () {
        const cases = fc.record({
          owner: fc
            .string()
            .filter(
              Schema.is(
                Schema.String.pipe(
                  Schema.check(
                    Schema.makeFilter((value) => value.trim().length > 0),
                  ),
                ),
              ),
            ),
          label: fc.string(),
        });
        const check = ({
          owner,
          label,
        }: typeof cases extends fc.Arbitrary<infer A> ? A : never) =>
          Effect.scoped(
            withApp(
              Effect.gen(function* () {
                const repo = yield* WorkspaceRepo;
                const workspace = yield* Workspace;
                yield* repo.createRepository({
                  organizationId: organizationId(owner),
                  id: workspaceId("private-repository"),
                  url: "https://github.com/test/private.git",
                  ref: "main",
                  labels: [label],
                  isDefault: true,
                });
                expect(
                  yield* workspace.resolve({
                    organizationId: organizationId(`${owner}:other`),
                    repositoryId: null,
                    issueLabels: [label],
                    projectLabels: [],
                    projectId: null,
                    teamId: null,
                  }),
                ).toEqual({ kind: "none" });
              }),
            ),
          );
        const context =
          yield* Effect.context<Effect.Services<ReturnType<typeof check>>>();
        yield* Effect.promise(() =>
          fc.assert(
            fc.asyncProperty(cases, (sample) =>
              Effect.runPromiseWith(context)(check(sample)),
            ),
            { numRuns: 20 },
          ),
        );
      }),
    { timeout: 15_000 },
  );
});

describe("route authentication inventory", () => {
  it.live(
    "requires login for every registered route outside the explicit protocol exemptions",
    () =>
      withApp(
        Effect.gen(function* () {
          const publicRoutes = new Set([
            "GET /",
            "GET /health",
            "GET /oauth/start",
            "GET /oauth/callback",
            "POST /webhooks/linear",
          ]);
          const encountered = new Set<string>();
          for (const route of routes) {
            const methods =
              route.method === "*"
                ? ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
                : [route.method];
            const path = route.path
              .replace(":id.json", "example.json")
              .replace(":id", "example")
              .replace("*", "example");
            for (const method of methods) {
              const key = `${method} ${path}`;
              if (publicRoutes.has(key)) {
                encountered.add(key);
                continue;
              }
              const response = yield* fetchApp(adminRequest(path, { method }));
              expect([401, 302], key).toContain(response.status);
              if (response.status === 302)
                expect(response.headers.get("location"), key).toContain(
                  "/oauth/start?returnTo=",
                );
            }
          }
          expect([...encountered].sort()).toEqual([...publicRoutes].sort());
        }),
      ),
  );
  it.live("deletes all admin sessions when their installation is revoked", () =>
    withApp(
      Effect.gen(function* () {
        const { token } = yield* createSession("org-revoked", Date.now());
        expect(
          (yield* fetchApp(adminRequest("/admin", { token }))).status,
        ).toBe(200);
        yield* InstallationRepo.use((service) =>
          service.revoke(organizationId("org-revoked"), Date.now()),
        );
        expect(
          (yield* fetchApp(adminRequest("/api/admin/bootstrap", { token })))
            .status,
        ).toBe(401);
        expect(
          yield* AdminSessionRepo.use((service) =>
            service.get(tokenHash(token)),
          ),
        ).toEqual(Option.none());
      }),
    ),
  );
});
