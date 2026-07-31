import { HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { it } from "@effect/vitest";
import {
  ConfigProvider,
  Effect,
  Layer,
  Option,
  Schema,
  type Scope,
} from "effect";
import { describe, expect } from "vitest";
import type {
  AppUserId,
  IssueId,
  OrganizationId,
  ProjectId,
  SessionId,
  SourceKey,
  TeamId,
  WorkspaceId,
} from "../src/domain/ids.js";
import { httpApp } from "../src/http/router.js";
import {
  createAdminSession,
  csrfHash,
  deriveCsrfToken,
  tokenHash,
} from "../src/services/admin.js";
import { GatewayConfig } from "../src/services/config.js";
import {
  Admin,
  AdminSessionRepo,
  DeliveryRepo,
  InstallationRepo,
  OAuth,
  Reconciler,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
  WebhookPipeline,
  Workspace,
  WorkspaceRepo,
} from "../src/services/contracts.js";
import { SqliteClientLive } from "../src/services/store/sqlite-client.js";
import { TokenCrypto } from "../src/services/token-crypto.js";

const PUBLIC_URL = "http://localhost:3000";
const TEST_KEY = Buffer.from(new Uint8Array(32).fill(9)).toString("base64");

const configProvider = ConfigProvider.fromMap(
  new Map<string, string>([
    ["PUBLIC_URL", PUBLIC_URL],
    ["LINEAR_CLIENT_ID", "client"],
    ["LINEAR_CLIENT_SECRET", "secret"],
    ["LINEAR_WEBHOOK_SECRET", "webhook"],
    ["TOKEN_ENCRYPTION_KEY", TEST_KEY],
    ["DATABASE_PATH", ":memory:"],
    ["WORKSPACE_ROOT", "/tmp/gateway-admin-workspaces"],
    ["PORT", "3000"],
    ["OMP_CLI_PATH", "omp"],
    ["LEASE_DURATION_MS", "60000"],
    ["WEBHOOK_REPLAY_WINDOW_MS", "60000"],
    ["LOG_LEVEL", "silent"],
  ]),
);

const reconcilerLayer = Layer.succeed(Reconciler, {
  _tag: "Reconciler",
  status: () =>
    Effect.succeed({
      running: false,
      lastStartedAt: null,
      lastCompletedAt: null,
      lastError: null,
    }),
  tick: () => Effect.void,
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
    | OAuth
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
      GatewayConfig.Default,
      TokenCrypto.Default,
      AdminSessionRepo.Default,
      DeliveryRepo.Default,
      InstallationRepo.Default,
      OAuth.Default,
      RunEventRepo.Default,
      RunInputRepo.Default,
      RunRepo.Default,
      WebhookPipeline.Default,
      WorkspaceRepo.Default,
      Workspace.Default,
      Admin.Default,
      reconcilerLayer,
    ).pipe(
      Layer.provide(
        Layer.mergeAll(
          SqliteClientLive(":memory:"),
          Layer.setConfigProvider(configProvider),
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
  it.scopedLive("removes repository map path from config", () =>
    withApp(
      Effect.gen(function* () {
        const config = yield* GatewayConfig;
        expect(config).not.toHaveProperty("repositoryMapPath");
      }),
    ),
  );

  it.scopedLive("creates and isolates repositories by organization", () =>
    withApp(
      Effect.gen(function* () {
        const repo = yield* WorkspaceRepo;
        const a = yield* repo.createRepository({
          organizationId: organizationId("org-a"),
          id: workspaceId("repo"),
          url: "https://example.com/a.git",
          ref: "main",
          teamIds: [],
          projectIds: [],
          labels: [],
          isDefault: false,
        });
        const b = yield* repo.createRepository({
          organizationId: organizationId("org-b"),
          id: workspaceId("repo"),
          url: "https://example.com/b.git",
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
        ).toEqual(["https://example.com/a.git"]);
        expect(
          (yield* repo.listRepositories(organizationId("org-b"))).map(
            (r) => r.url,
          ),
        ).toEqual(["https://example.com/b.git"]);
      }),
    ),
  );

  it.scopedLive("enforces only one default repository per organization", () =>
    withApp(
      Effect.gen(function* () {
        const repo = yield* WorkspaceRepo;
        const organization = organizationId("org-default");
        const first = yield* repo.createRepository({
          organizationId: organization,
          id: workspaceId("first"),
          url: "https://example.com/first.git",
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
          url: "https://example.com/second.git",
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

  it.scopedLive("updates and deletes repositories", () =>
    withApp(
      Effect.gen(function* () {
        const repo = yield* WorkspaceRepo;
        const organization = organizationId("org-crud");
        const id = workspaceId("repo");
        yield* repo.createRepository({
          organizationId: organization,
          id,
          url: "https://example.com/repo.git",
          ref: "main",
          teamIds: [teamId("team")],
          projectIds: [],
          labels: [],
          isDefault: false,
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
      }),
    ),
  );

  it.scopedLive(
    "resolves repositories with correct precedence and ambiguity",
    () =>
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
              url: `https://example.com/${id}.git`,
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

  it.scopedLive(
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
          expect(dashboard.headers.get("location")).toBe("/");
        }),
      ),
  );

  it.scopedLive("revokes sessions and clears the admin cookie on logout", () =>
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

  it.scopedLive(
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
                  url: "https://example.com/x.git",
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
                  url: "https://example.com/x.git",
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
                  url: "https://example.com/x.git",
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
                url: "https://example.com/api.git",
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
                url: "https://example.com/api.git",
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

  it.scopedLive(
    "redacts public run details and preserves their JSON representation",
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

          const html = yield* fetchApp(adminRequest(path));
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
              headers: { Accept: "application/json" },
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

          const suffix = yield* fetchApp(adminRequest(`${path}.json`));
          expect(suffix.status).toBe(200);
          expect(suffix.headers.get("content-type")).toContain(
            "application/json",
          );
        }),
      ),
  );

  it.scopedLive("redacts installation secrets from bootstrap", () =>
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

  it.scopedLive("previews matching, default, and ambiguous repositories", () =>
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
            url: `https://example.com/${id}.git`,
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

describe("admin session and routing invariants", () => {
  it.prop(
    "token and CSRF derivations are deterministic and separate input domains",
    {
      rawA: Schema.String,
      rawB: Schema.String,
      csrfA: Schema.String,
      csrfB: Schema.String,
    },
    ({ rawA, rawB, csrfA, csrfB }) => {
      expect(tokenHash(rawA)).toBe(tokenHash(rawA));
      expect(deriveCsrfToken(rawA)).toBe(deriveCsrfToken(rawA));
      expect(csrfHash(csrfA)).toBe(csrfHash(csrfA));
      if (rawA !== rawB)
        expect(deriveCsrfToken(rawA)).not.toBe(deriveCsrfToken(rawB));
      if (csrfA !== csrfB) expect(csrfHash(csrfA)).not.toBe(csrfHash(csrfB));
    },
  );

  it.effect.prop(
    "repository resolution never returns a repository owned by another organization",
    {
      owner: Schema.String.pipe(Schema.minLength(1)),
      label: Schema.String,
    },
    ({ owner, label }) =>
      Effect.scoped(
        withApp(
          Effect.gen(function* () {
            const repo = yield* WorkspaceRepo;
            const workspace = yield* Workspace;
            yield* repo.createRepository({
              organizationId: organizationId(owner),
              id: workspaceId("private-repository"),
              url: "https://example.com/private.git",
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
      ),
  );
});
