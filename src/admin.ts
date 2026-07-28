import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { renderAdminPage, renderLandingPage } from "./admin-ui";
import type {
  GatewayConfig,
  InstallationRecord,
  RepositoryRecord,
} from "./domain";
import type { Logger } from "./logger";
import {
  completeAuthorization,
  createAdminSession,
  startAuthorization,
} from "./oauth";
import type { Reconciler } from "./reconciler";
import type { WorkspacePort } from "./session-authority";
import type { GatewayStore } from "./store";

const ADMIN_COOKIE = "omp_gateway_admin";
const CSRF_SALT = "omp-gateway-admin-csrf";

const SECURITY_HEADERS: Record<string, string> = {
  "X-Content-Type-Options": "nosniff",
  "X-Frame-Options": "DENY",
  "Referrer-Policy": "strict-origin-when-cross-origin",
  "Cache-Control": "no-store",
};

const HTML_SECURITY_HEADERS: Record<string, string> = {
  ...SECURITY_HEADERS,
  "Content-Security-Policy":
    "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'",
};

function isSecure(config: GatewayConfig): boolean {
  return config.publicUrl.protocol === "https:";
}

function adminCookieAttributes(
  config: GatewayConfig,
  expiresAt?: number,
): string {
  const base = "HttpOnly; SameSite=Strict; Path=/";
  const secure = isSecure(config) ? "; Secure" : "";
  const expiry =
    expiresAt === undefined
      ? "; Max-Age=0"
      : `; Max-Age=${Math.max(0, Math.floor((expiresAt - Date.now()) / 1000))}`;
  return `${base}${secure}${expiry}`;
}

function setAdminCookie(
  config: GatewayConfig,
  token: string,
  expiresAt: number,
): string {
  return `${ADMIN_COOKIE}=${encodeURIComponent(token)}; ${adminCookieAttributes(config, expiresAt)}`;
}

function clearAdminCookie(config: GatewayConfig): string {
  return `${ADMIN_COOKIE}=; ${adminCookieAttributes(config)}`;
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

function timingSafeEquals(a: string, b: string): boolean {
  const aBuf = Buffer.from(a, "utf8");
  const bBuf = Buffer.from(b, "utf8");
  if (aBuf.length !== bBuf.length) return false;
  return timingSafeEqual(aBuf, bBuf);
}

function findCookieValue(request: Request, name: string): string | null {
  const header = request.headers.get("cookie");
  if (!header) return null;
  const pattern = new RegExp(`(?:^|;\\s*)${name}=([^;]*)`, "u");
  const match = pattern.exec(header);
  if (!match) return null;
  return match[1] ? decodeURIComponent(match[1]) : null;
}

function adminSession(
  store: GatewayStore,
  request: Request,
  now = Date.now(),
): { organizationId: string; rawToken: string; csrfTokenHash: string } | null {
  const rawToken = findCookieValue(request, ADMIN_COOKIE);
  if (!rawToken) return null;
  const session = store.getAdminSession(tokenHash(rawToken), now);
  if (!session) return null;
  return {
    organizationId: session.organizationId,
    rawToken,
    csrfTokenHash: session.csrfTokenHash,
  };
}

function validateOrigin(config: GatewayConfig, request: Request): boolean {
  const origin = request.headers.get("origin");
  if (!origin) return false;
  try {
    return new URL(origin).origin === config.publicUrl.origin;
  } catch {
    return false;
  }
}

function validateCsrf(request: Request, csrfTokenHash: string): boolean {
  const header = request.headers.get("x-csrf-token");
  if (!header) return false;
  return timingSafeEquals(csrfHash(header), csrfTokenHash);
}

async function parseJsonBody(request: Request): Promise<unknown> {
  if (
    request.headers.get("content-type")?.startsWith("application/json") !== true
  ) {
    throw new Error("Request body must be JSON");
  }
  const body = await request.text();
  if (body.length === 0) return {};
  try {
    return JSON.parse(body) as unknown;
  } catch {
    throw new Error("Invalid JSON");
  }
}

function requireRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error("Request body must be a JSON object");
  return value as Record<string, unknown>;
}

function optionalString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function stringArray(value: unknown, field: string): string[] {
  if (!Array.isArray(value)) throw new Error(`${field} must be an array`);
  if (value.some((item) => typeof item !== "string"))
    throw new Error(`${field} must contain only strings`);
  return value.map((item) => String(item).trim().toLowerCase()).filter(Boolean);
}

function optionalStringArray(value: unknown, field: string): string[] {
  if (value === undefined || value === null) return [];
  return stringArray(value, field);
}

function optionalBoolean(value: unknown): boolean | undefined {
  if (typeof value === "boolean") return value;
  return undefined;
}

function repositoryPayload(body: Record<string, unknown>): {
  id: string;
  url: string;
  ref: string;
  teamIds: string[];
  projectIds: string[];
  labels: string[];
  isDefault: boolean | undefined;
} {
  const id = optionalString(body.id);
  const url = optionalString(body.url);
  const ref = optionalString(body.ref);
  if (!id) throw new Error("id is required");
  if (!url) throw new Error("url is required");
  if (!ref) throw new Error("ref is required");
  return {
    id,
    url,
    ref,
    teamIds: optionalStringArray(body.teamIds, "teamIds"),
    projectIds: optionalStringArray(body.projectIds, "projectIds"),
    labels: optionalStringArray(body.labels, "labels"),
    isDefault: optionalBoolean(body.isDefault),
  };
}

export function toApiRepository(repository: RepositoryRecord) {
  return {
    id: repository.id,
    organizationId: repository.organizationId,
    url: repository.url,
    ref: repository.ref,
    teamIds: [...repository.teamIds],
    projectIds: [...repository.projectIds],
    labels: [...repository.labels],
    isDefault: repository.isDefault,
    createdAt: repository.createdAt,
    updatedAt: repository.updatedAt,
  };
}

function toAdminInstallation(installation: InstallationRecord) {
  return {
    organizationId: installation.organizationId,
    appUserId: installation.appUserId,
    scopes: [...installation.scopes],
    revokedAt: installation.revokedAt,
    accessibleTeamIds:
      installation.accessibleTeamIds === null
        ? null
        : [...installation.accessibleTeamIds],
    canAccessAllPublicTeams: installation.canAccessAllPublicTeams,
  };
}

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: {
      "content-type": "application/json",
      ...SECURITY_HEADERS,
    },
  });
}

function html(body: string, status = 200): Response {
  return new Response(body, {
    status,
    headers: {
      "content-type": "text/html; charset=utf-8",
      ...HTML_SECURITY_HEADERS,
    },
  });
}

function text(message: string, status: number): Response {
  return new Response(message, {
    status,
    headers: {
      "content-type": "text/plain; charset=utf-8",
      ...SECURITY_HEADERS,
    },
  });
}

function redirect(
  location: string,
  status = 302,
  extraHeaders: Record<string, string> = {},
): Response {
  return new Response(null, {
    status,
    headers: { location, ...SECURITY_HEADERS, ...extraHeaders },
  });
}

export interface AdminRouter {
  handle(request: Request, url: URL, now: number): Promise<Response | null>;
}

export function createAdminRouter(deps: {
  config: GatewayConfig;
  store: GatewayStore;
  workspaces: WorkspacePort;
  reconciler: Reconciler;
  logger: Logger;
}): AdminRouter {
  const { config, store, workspaces, reconciler, logger } = deps;

  return {
    async handle(request, url, now) {
      if (url.pathname === "/" && request.method === "GET") {
        const session = adminSession(store, request, now);
        if (session) return redirect("/admin");
        return html(renderLandingPage());
      }

      if (url.pathname === "/admin" && request.method === "GET") {
        const session = adminSession(store, request, now);
        if (!session) return redirect("/");
        return html(renderAdminPage());
      }

      if (url.pathname === "/oauth/start" && request.method === "GET") {
        logger.info({ event: "oauth.started", path: url.pathname });
        const authorization = await startAuthorization(config, store);
        return redirect(authorization.url.toString(), 302);
      }

      if (url.pathname === "/oauth/callback" && request.method === "GET") {
        try {
          const installation = await completeAuthorization(config, store, url);
          logger.info({
            event: "oauth.completed",
            organizationId: installation.organizationId,
          });
          const { token, expiresAt } = createAdminSession(
            store,
            installation.organizationId,
            now,
          );
          return redirect("/admin", 302, {
            "set-cookie": setAdminCookie(config, token, expiresAt),
          });
        } catch (error) {
          const message =
            error instanceof Error ? error.message : String(error);
          logger.error({ event: "oauth.failed", error: message });
          throw error;
        }
      }

      if (url.pathname === "/api/admin/bootstrap" && request.method === "GET") {
        const session = adminSession(store, request, now);
        if (!session) return text("Unauthorized", 401);
        const installation = await store.getInstallation(
          session.organizationId,
        );
        const adminInstallation =
          installation === null ? null : toAdminInstallation(installation);
        const repositories = store.listRepositories(session.organizationId);
        return json({
          health: {
            installation:
              adminInstallation !== null &&
              adminInstallation.revokedAt === null,
            reconciler: reconciler.status,
          },
          installation: adminInstallation,
          repositories: repositories.map(toApiRepository),
          csrfToken: deriveCsrfToken(session.rawToken),
        });
      }

      if (
        url.pathname === "/api/admin/repositories" &&
        request.method === "GET"
      ) {
        const session = adminSession(store, request, now);
        if (!session) return text("Unauthorized", 401);
        const repositories = store.listRepositories(session.organizationId);
        return json({ repositories: repositories.map(toApiRepository) });
      }

      if (
        url.pathname === "/api/admin/repositories" &&
        request.method === "POST"
      ) {
        const session = adminSession(store, request, now);
        if (!session) return text("Unauthorized", 401);
        if (!validateOrigin(config, request)) return text("Forbidden", 403);
        if (!validateCsrf(request, session.csrfTokenHash))
          return text("Forbidden", 403);
        const payload = repositoryPayload(
          requireRecord(await parseJsonBody(request)),
        );
        const repository = store.createRepository({
          organizationId: session.organizationId,
          id: payload.id,
          url: payload.url,
          ref: payload.ref,
          teamIds: payload.teamIds,
          projectIds: payload.projectIds,
          labels: payload.labels,
          isDefault: payload.isDefault ?? false,
        });
        return json({ repository: toApiRepository(repository) }, 201);
      }

      if (url.pathname.startsWith("/api/admin/repositories/")) {
        const id = decodeURIComponent(
          url.pathname.slice("/api/admin/repositories/".length),
        );

        if (request.method === "PUT") {
          const session = adminSession(store, request, now);
          if (!session) return text("Unauthorized", 401);
          if (!validateOrigin(config, request)) return text("Forbidden", 403);
          if (!validateCsrf(request, session.csrfTokenHash))
            return text("Forbidden", 403);
          const body = requireRecord(await parseJsonBody(request));
          if (body.id !== undefined && body.id !== id)
            throw new Error("Repository id in body does not match path");
          const payload = repositoryPayload(body);
          const update: {
            url: string;
            ref: string;
            teamIds: readonly string[];
            projectIds: readonly string[];
            labels: readonly string[];
            isDefault?: boolean;
          } = {
            url: payload.url,
            ref: payload.ref,
            teamIds: payload.teamIds,
            projectIds: payload.projectIds,
            labels: payload.labels,
          };
          if (payload.isDefault !== undefined)
            update.isDefault = payload.isDefault;
          const repository = store.updateRepository(
            session.organizationId,
            id,
            update,
          );
          return json({ repository: toApiRepository(repository) });
        }

        if (request.method === "DELETE") {
          const session = adminSession(store, request, now);
          if (!session) return text("Unauthorized", 401);
          if (!validateOrigin(config, request)) return text("Forbidden", 403);
          if (!validateCsrf(request, session.csrfTokenHash))
            return text("Forbidden", 403);
          const deleted = store.deleteRepository(session.organizationId, id);
          if (!deleted) return text("Not found", 404);
          return new Response(null, {
            status: 204,
            headers: SECURITY_HEADERS,
          });
        }
      }

      if (url.pathname === "/api/admin/preview" && request.method === "POST") {
        const session = adminSession(store, request, now);
        if (!session) return text("Unauthorized", 401);
        if (!validateOrigin(config, request)) return text("Forbidden", 403);
        if (!validateCsrf(request, session.csrfTokenHash))
          return text("Forbidden", 403);
        const body = requireRecord(await parseJsonBody(request));
        const resolution = workspaces.resolve({
          organizationId: session.organizationId,
          teamId: optionalString(body.teamId),
          projectId: optionalString(body.projectId),
          repositoryId: optionalString(body.repositoryId),
          issueLabels: optionalStringArray(body.issueLabels, "issueLabels"),
          projectLabels: optionalStringArray(
            body.projectLabels,
            "projectLabels",
          ),
        });
        return json({
          kind: resolution.kind,
          repository:
            resolution.kind === "match"
              ? toApiRepository(resolution.repository)
              : null,
          repositories:
            resolution.kind === "ambiguous"
              ? resolution.repositories.map(toApiRepository)
              : null,
        });
      }

      if (url.pathname === "/api/admin/logout" && request.method === "POST") {
        const session = adminSession(store, request, now);
        if (!session) return text("Unauthorized", 401);
        if (!validateOrigin(config, request)) return text("Forbidden", 403);
        if (!validateCsrf(request, session.csrfTokenHash))
          return text("Forbidden", 403);
        store.deleteAdminSession(tokenHash(session.rawToken));
        return redirect("/", 302, {
          "set-cookie": clearAdminCookie(config),
        });
      }

      return null;
    },
  };
}
