import {
  createHash,
  createHmac,
  randomBytes,
  timingSafeEqual,
} from "node:crypto";
import { Cause, Clock, Effect, Either, Option, Schema } from "effect";
import { redact } from "../admin-ui/run-detail.js";
import {
  type RunDetailModel,
  renderAdminPage,
  renderLandingPage,
  renderRunDetailPage,
} from "../admin-ui.js";
import {
  DatabaseError,
  type NixEnvironmentError,
  RowDecodeError,
  type TokenCipherError,
  type WorkspaceError,
} from "../domain/errors.js";
import {
  InputId,
  McpServerId,
  type OrganizationId,
  ProjectId,
  SessionId,
  TeamId,
  WorkspaceId,
} from "../domain/ids.js";
import type {
  AgentRun,
  Installation,
  McpServerRecord,
  NixCacheEntry,
  NixPackageName,
  RepositoryRecord,
  RunEvent,
} from "../domain/models.js";
import { normalizeNixPackages } from "../domain/models.js";
import { GatewayConfig, type GatewayConfigShape } from "./config.js";
import { LinearGateway } from "./linear-gateway.js";
import { NixEnvironment } from "./nix-environment.js";
import { Reconciler, type ReconcilerStatus } from "./reconciler.js";
import {
  AdminSessionRepo,
  InstallationRepo,
  McpServerRepo,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
  WorkspaceRepo,
} from "./store/repositories.js";
import { type RepositoryResolution, Workspace } from "./workspace.js";

const ADMIN_COOKIE = "omp_gateway_admin";
const CSRF_SALT = "omp-gateway-admin-csrf";
const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;

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

export interface WorkspaceShape {
  readonly resolve: (
    context: unknown,
  ) => Effect.Effect<RepositoryResolution, DatabaseError | RowDecodeError>;
  readonly materialize: (
    sessionId: string,
    repository: RepositoryRecord,
  ) => Effect.Effect<string, WorkspaceError>;
}

export interface ReconcilerShape {
  readonly status: () => Effect.Effect<ReconcilerStatus, never>;
}

export interface AdminDeps {
  readonly config: GatewayConfigShape;
  readonly adminSessionRepo: AdminSessionRepo;
  readonly installationRepo: InstallationRepo;
  readonly runRepo: RunRepo;
  readonly runEventRepo: RunEventRepo;
  readonly runInputRepo: RunInputRepo;
  readonly linearGateway: {
    readonly createSessionOnIssue: LinearGateway["createSessionOnIssue"];
  };
  readonly workspaceRepo: WorkspaceRepo;
  readonly mcpServerRepo?: McpServerRepo;
  readonly workspace: WorkspaceShape;
  readonly reconciler: ReconcilerShape;
  readonly nixEnvironment: NixEnvironment;
}

interface RepositoryPayload {
  readonly id: string;
  readonly url: string;
  readonly ref: string;
  readonly teamIds: ReadonlyArray<string>;
  readonly projectIds: ReadonlyArray<string>;
  readonly labels: ReadonlyArray<string>;
  readonly isDefault: boolean | undefined;
  readonly nixPackages: ReadonlyArray<NixPackageName>;
}

class AdminError extends Schema.TaggedError<AdminError>()(
  "@Gateway/AdminError",
  {
    message: Schema.String,
    status: Schema.Literal(400, 401, 403, 404, 409, 429, 500),
  },
) {}

function isSecure(config: { publicUrl: URL }): boolean {
  return config.publicUrl.protocol === "https:";
}

function adminCookieAttributes(
  config: { publicUrl: URL },
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

export function setAdminCookie(
  config: GatewayConfigShape,
  token: string,
  expiresAt: number,
): string {
  return `${ADMIN_COOKIE}=${encodeURIComponent(token)}; ${adminCookieAttributes(config, expiresAt)}`;
}

export function clearAdminCookie(config: GatewayConfigShape): string {
  return `${ADMIN_COOKIE}=; ${adminCookieAttributes(config)}`;
}

export function tokenHash(rawToken: string): string {
  return createHash("sha256").update(rawToken).digest("base64url");
}

export function deriveCsrfToken(rawToken: string): string {
  return createHmac("sha256", rawToken).update(CSRF_SALT).digest("base64url");
}

export function csrfHash(rawCsrf: string): string {
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

function validateOrigin(config: GatewayConfigShape, request: Request): boolean {
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

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function optionalString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function stringArray(
  value: unknown,
  field: string,
): Either.Either<ReadonlyArray<string>, string> {
  if (!Array.isArray(value)) return Either.left(`${field} must be an array`);
  if (value.some((item) => typeof item !== "string"))
    return Either.left(`${field} must contain only strings`);
  return Either.right(
    value.map((item) => String(item).trim().toLowerCase()).filter(Boolean),
  );
}

function optionalStringArray(
  value: unknown,
  field: string,
): Either.Either<ReadonlyArray<string>, string> {
  if (value === undefined || value === null) return Either.right([]);
  return stringArray(value, field);
}

function optionalNixPackageArray(
  value: unknown,
): Either.Either<ReadonlyArray<NixPackageName>, string> {
  if (value === undefined || value === null) return Either.right([]);
  if (!Array.isArray(value)) return Either.left("nixPackages must be an array");
  if (value.some((item) => typeof item !== "string")) {
    return Either.left("nixPackages must contain only strings");
  }
  const values = value.map((item) => item.trim()).filter(Boolean);
  if (
    values.some(
      (item) => !/^[A-Za-z0-9_+-]+(?:\.[A-Za-z0-9_+-]+)*$/u.test(item),
    )
  ) {
    return Either.left("Invalid Nix package name");
  }
  return Either.right(normalizeNixPackages(values));
}

function optionalBoolean(value: unknown): boolean | undefined {
  if (typeof value === "boolean") return value;
  return undefined;
}

function repositoryPayload(
  body: Record<string, unknown>,
): Either.Either<RepositoryPayload, string> {
  const id = optionalString(body.id);
  const url = optionalString(body.url);
  const ref = optionalString(body.ref);
  if (id === null) return Either.left("id is required");
  if (url === null) return Either.left("url is required");
  if (ref === null) return Either.left("ref is required");
  const teamIds = optionalStringArray(body.teamIds, "teamIds");
  if (Either.isLeft(teamIds)) return Either.left(teamIds.left);
  const projectIds = optionalStringArray(body.projectIds, "projectIds");
  if (Either.isLeft(projectIds)) return Either.left(projectIds.left);
  const labels = optionalStringArray(body.labels, "labels");
  if (Either.isLeft(labels)) return Either.left(labels.left);
  const nixPackages = optionalNixPackageArray(body.nixPackages);
  if (Either.isLeft(nixPackages)) return Either.left(nixPackages.left);
  return Either.right({
    id,
    url,
    ref,
    teamIds: teamIds.right,
    projectIds: projectIds.right,
    labels: labels.right,
    nixPackages: nixPackages.right,
    isDefault: optionalBoolean(body.isDefault),
  });
}

export function toApiRepository(repository: RepositoryRecord) {
  return {
    id: repository.id,
    organizationId: repository.organizationId,
    nixPackages: [...repository.nixPackages],
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
interface McpServerPayload {
  readonly id: string;
  readonly name: string;
  readonly transport: "stdio" | "http" | "sse";
  readonly command: string | null;
  readonly args: ReadonlyArray<string>;
  readonly url: string | null;
  readonly env: Readonly<Record<string, string>>;
  readonly repositoryId: string | null;
  readonly enabled: boolean | undefined;
}

function mcpServerPayload(
  body: Record<string, unknown>,
): Either.Either<McpServerPayload, string> {
  const id = optionalString(body.id);
  const name = optionalString(body.name);
  if (id === null) return Either.left("id is required");
  if (name === null) return Either.left("name is required");
  const rawTransport = body.transport;
  if (
    rawTransport !== "stdio" &&
    rawTransport !== "http" &&
    rawTransport !== "sse"
  ) {
    return Either.left("transport must be stdio, http, or sse");
  }
  const command = body.command === null ? null : optionalString(body.command);
  const url = body.url === null ? null : optionalString(body.url);
  const args = optionalStringArray(body.args, "args");
  if (Either.isLeft(args)) return Either.left(args.left);
  const repositoryId =
    body.repositoryId === null ? null : optionalString(body.repositoryId);
  const envValue = body.env;
  const env: Record<string, string> = {};
  if (envValue !== undefined && envValue !== null) {
    if (!record(envValue)) return Either.left("env must be an object");
    for (const [key, value] of Object.entries(envValue)) {
      if (typeof value !== "string")
        return Either.left(`env.${key} must be a string`);
      env[key] = value;
    }
  }
  return Either.right({
    id,
    name,
    transport: rawTransport,
    command,
    args: args.right,
    url,
    env,
    repositoryId,
    enabled: optionalBoolean(body.enabled),
  });
}

export function toApiMcpServer(server: McpServerRecord) {
  const env: Record<string, string> = {};
  for (const name of Object.keys(server.env)) env[name] = "•••";
  return {
    id: server.id,
    organizationId: server.organizationId,
    name: server.name,
    transport: server.transport,
    command: Option.getOrElse(server.command, () => null),
    args: [...server.args],
    url: Option.getOrElse(server.url, () => null),
    env,
    repositoryId: Option.getOrElse(server.repositoryId, () => null),
    enabled: server.enabled,
    createdAt: server.createdAt,
    updatedAt: server.updatedAt,
  };
}

function toAdminInstallation(installation: Installation) {
  return {
    organizationId: installation.organizationId,

    appUserId: installation.appUserId,
    scopes: [...installation.scopes],
    revokedAt: Option.getOrElse(installation.revokedAt, () => null),
    accessibleTeamIds: Option.match(installation.accessibleTeamIds, {
      onNone: () => null,
      onSome: (ids) => [...ids],
    }),
    canAccessAllPublicTeams: Option.getOrElse(
      installation.canAccessAllPublicTeams,
      () => null,
    ),
  };
}

function toApiNixCache(entry: NixCacheEntry) {
  return {
    cacheKey: entry.cacheKey,
    status: "ready",
    sizeBytes: entry.sizeBytes,
    lastUsedAt: entry.updatedAt,
  };
}

function stringField(
  value: Record<string, unknown>,
  key: string,
): string | null {
  const field = value[key];
  return typeof field === "string" && field.length > 0 ? field : null;
}

function safeUrl(value: string | null): string | null {
  if (value === null) return null;
  try {
    const url = new URL(value);
    return url.protocol === "https:" || url.protocol === "http:"
      ? redact(value)
      : null;
  } catch {
    return null;
  }
}

function redactedStringField(
  value: Record<string, unknown>,
  key: string,
): string | null {
  const field = stringField(value, key);
  return field === null ? null : redact(field);
}

function issueForRun(
  run: AgentRun,
  events: ReadonlyArray<RunEvent>,
): RunDetailModel["issue"] {
  for (const event of events) {
    if (!record(event.payload)) continue;
    const agentSession = event.payload.agentSession;
    if (!record(agentSession)) continue;
    const issue = agentSession.issue;
    if (!record(issue)) continue;
    return {
      identifier: redactedStringField(issue, "identifier"),
      title: redactedStringField(issue, "title"),
      url: safeUrl(stringField(issue, "url")),
    };
  }
  return Option.match(run.issueId, {
    onNone: () => null,
    onSome: (id) => ({
      identifier: redact(id),
      title: null,
      url: null,
    }),
  });
}

function redactOption(value: Option.Option<string>): string | null {
  return Option.getOrElse(Option.map(value, redact), () => null);
}

function runDetailModel(
  run: AgentRun,
  events: ReadonlyArray<RunEvent>,
): RunDetailModel {
  return {
    run: {
      sessionId: run.sessionId,
      organizationId: redact(run.organizationId),
      issueId: redactOption(Option.map(run.issueId, (x) => String(x))),
      repositoryId: redactOption(
        Option.map(run.repositoryId, (x) => String(x)),
      ),
      state: run.state,
      desiredState: run.desiredState,
      ompSessionId: redactOption(run.ompSessionId),
      ompSessionFile: redactOption(run.ompSessionFile),
      workspacePath: redactOption(run.workspacePath),
      teamId: redactOption(Option.map(run.teamId, (x) => String(x))),
      projectId: redactOption(Option.map(run.projectId, (x) => String(x))),
      attempt: run.attempt,
      leaseOwner: redactOption(run.leaseOwner),
      leaseExpiresAt: Option.getOrElse(run.leaseExpiresAt, () => null),
      lastActivityAt: Option.getOrElse(run.lastActivityAt, () => null),
      terminalReason: redactOption(run.terminalReason),
      nextAttemptAt: Option.getOrElse(run.nextAttemptAt, () => null),
      createdAt: run.createdAt,
      updatedAt: run.updatedAt,
    },
    issue: issueForRun(run, events),
    events: events.map((event) => ({
      sourceKey: redact(String(event.sourceKey)),
      kind: redact(event.kind),
      level: event.level,
      text: redactOption(event.text),
      payload: redact(JSON.stringify(event.payload, null, 2) ?? "null"),
      status: redactOption(event.status),
      error: redactOption(event.error),
      createdAt: event.createdAt,
      updatedAt: event.updatedAt,
    })),
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

function emptyResponse(
  status: number,
  extraHeaders: Record<string, string> = {},
): Response {
  return new Response(null, {
    status,
    headers: { ...SECURITY_HEADERS, ...extraHeaders },
  });
}

function parseJsonBody(request: Request) {
  return Effect.gen(function* () {
    const contentType = request.headers.get("content-type") ?? "";
    if (!contentType.startsWith("application/json")) {
      return yield* Effect.fail(
        new AdminError({ message: "Request body must be JSON", status: 400 }),
      );
    }

    const body = yield* Effect.tryPromise({
      try: () => request.text(),
      catch: (error) =>
        new DatabaseError({
          message: `body read failed: ${error}`,
        }),
    });

    if (body.length === 0) return {};

    const parsed = yield* Effect.try({
      try: () => JSON.parse(body),
      catch: () => new AdminError({ message: "Invalid JSON", status: 400 }),
    });

    if (Array.isArray(parsed) || !record(parsed)) {
      return yield* Effect.fail(
        new AdminError({
          message: "Request body must be a JSON object",
          status: 400,
        }),
      );
    }

    return parsed;
  });
}

function mapCauseToResponse(
  cause: Cause.Cause<unknown>,
): Option.Option<Response> {
  const typed = Array.from(Cause.failures(cause));
  const defects = Array.from(Cause.defects(cause));
  const all = [...typed, ...defects];

  for (const error of all) {
    if (error instanceof AdminError) {
      return Option.some(text(error.message, error.status));
    }
    if (error instanceof RowDecodeError) {
      return Option.some(text(error.message, 400));
    }
  }

  const message = all
    .map((error) => (error instanceof Error ? error.message : String(error)))
    .join("; ");

  if (/already exists|unique constraint/i.test(message)) {
    return Option.some(text(message, 409));
  }

  if (/not found|does not exist/i.test(message)) {
    return Option.some(text(message, 404));
  }

  return Option.some(text("Internal server error", 500));
}

export const createAdminHandle = (deps: AdminDeps) =>
  Effect.fn("Admin.handle")(
    function* (
      request: Request,
    ): Effect.fn.Return<
      Option.Option<Response>,
      | AdminError
      | DatabaseError
      | NixEnvironmentError
      | RowDecodeError
      | TokenCipherError
    > {
      const now = yield* Clock.currentTimeMillis;
      const url = new URL(request.url);

      const requireSession = (req: Request) =>
        Effect.gen(function* () {
          const rawToken = findCookieValue(req, ADMIN_COOKIE);
          if (rawToken === null) {
            return yield* Effect.fail(
              new AdminError({ message: "Unauthorized", status: 401 }),
            );
          }
          const session = yield* deps.adminSessionRepo.get(
            tokenHash(rawToken),
            now,
          );
          if (Option.isNone(session)) {
            return yield* Effect.fail(
              new AdminError({ message: "Unauthorized", status: 401 }),
            );
          }
          return {
            organizationId: session.value.organizationId,
            rawToken,
            csrfTokenHash: session.value.csrfTokenHash,
          };
        });

      const requireMutation = (req: Request) =>
        Effect.gen(function* () {
          const session = yield* requireSession(req);
          if (!validateOrigin(deps.config, req)) {
            return yield* Effect.fail(
              new AdminError({ message: "Forbidden", status: 403 }),
            );
          }
          if (!validateCsrf(req, session.csrfTokenHash)) {
            return yield* Effect.fail(
              new AdminError({ message: "Forbidden", status: 403 }),
            );
          }
          return session;
        });

      if (url.pathname === "/" && request.method === "GET") {
        const rawToken = findCookieValue(request, ADMIN_COOKIE);
        if (rawToken !== null) {
          const session = yield* deps.adminSessionRepo.get(
            tokenHash(rawToken),
            now,
          );
          if (Option.isSome(session)) {
            return Option.some(redirect("/admin"));
          }
        }
        return Option.some(html(renderLandingPage()));
      }

      if (url.pathname === "/admin" && request.method === "GET") {
        const rawToken = findCookieValue(request, ADMIN_COOKIE);
        if (rawToken === null) {
          return Option.some(redirect("/"));
        }
        const session = yield* deps.adminSessionRepo.get(
          tokenHash(rawToken),
          now,
        );
        if (Option.isNone(session)) {
          return Option.some(redirect("/"));
        }
        return Option.some(html(renderAdminPage()));
      }

      if (url.pathname.startsWith("/runs/") && request.method === "GET") {
        const encodedRunId = url.pathname.slice("/runs/".length);
        const isJsonPath = encodedRunId.endsWith(".json");
        const rawSessionId = decodeURIComponent(
          isJsonPath ? encodedRunId.slice(0, -".json".length) : encodedRunId,
        );
        const sessionId = yield* Schema.decodeUnknown(SessionId)(
          rawSessionId,
        ).pipe(
          Effect.catchTags({
            ParseError: () =>
              Effect.fail(
                new AdminError({ message: "Invalid run id", status: 400 }),
              ),
          }),
        );
        const runOption = yield* deps.runRepo.get(sessionId);
        if (Option.isNone(runOption)) {
          return Option.some(text("Not found", 404));
        }
        const run = runOption.value;
        const events = yield* deps.runEventRepo.list(sessionId);
        const model = runDetailModel(run, events);

        const rawToken = findCookieValue(request, ADMIN_COOKIE);
        const adminSession =
          rawToken === null
            ? Option.none()
            : yield* deps.adminSessionRepo.get(tokenHash(rawToken), now).pipe(
                Effect.catchAll((error) =>
                  Effect.logWarning("admin.session_lookup_failed").pipe(
                    Effect.annotateLogs({
                      sessionId,
                      error:
                        error instanceof Error ? error.message : String(error),
                    }),
                    Effect.as(Option.none()),
                  ),
                ),
              );
        const csrfToken = Option.match(adminSession, {
          onNone: () => null,
          onSome: () => deriveCsrfToken(rawToken ?? ""),
        });

        const acceptsJson =
          request.headers.get("accept")?.includes("application/json") ?? false;
        const detail = {
          sessionId: model.run.sessionId,
          state: model.run.state,
          attempt: model.run.attempt,
          lastActivityAt: model.run.lastActivityAt,
          ...model,
        };
        return Option.some(
          isJsonPath || acceptsJson
            ? json(detail)
            : html(renderRunDetailPage(model, csrfToken)),
        );
      }

      const rerunMatch = /^\/api\/admin\/runs\/([^/]+)\/rerun$/u.exec(
        url.pathname,
      );
      if (rerunMatch !== null && request.method === "POST") {
        const rawSessionId = decodeURIComponent(rerunMatch[1] ?? "");
        const sessionId = yield* Schema.decodeUnknown(SessionId)(
          rawSessionId,
        ).pipe(
          Effect.catchTags({
            ParseError: () =>
              Effect.fail(
                new AdminError({ message: "Invalid run id", status: 400 }),
              ),
          }),
        );
        const adminSession = yield* requireMutation(request);
        const runOption = yield* deps.runRepo.get(sessionId);
        if (Option.isNone(runOption)) {
          return Option.some(text("Not found", 404));
        }
        const run = runOption.value;
        // Scope to the authenticated admin's organization: a valid admin
        // session for org A must not drive runs (or spend tokens) of org B.
        // 404 rather than 403 so cross-org run ids are not confirmable.
        if (run.organizationId !== adminSession.organizationId) {
          return Option.some(text("Not found", 404));
        }
        if (Option.isNone(run.issueId)) {
          return Option.some(text("Run is not linked to an issue", 409));
        }
        const issueId = run.issueId.value;
        const newSessionId = yield* deps.linearGateway
          .createSessionOnIssue({
            organizationId: run.organizationId,
            issueId,
          })
          .pipe(
            Effect.mapError((error) => {
              if (error._tag === "@Gateway/LinearRateLimitError") {
                const retry =
                  error.retryAfterMs === undefined
                    ? ""
                    : ` Retry after ${error.retryAfterMs}ms.`;
                return new AdminError({
                  message: `Could not create Linear agent session: ${error.message}.${retry}`,
                  status: 429,
                });
              }
              return new AdminError({
                message: `Could not create Linear agent session: ${error.message}`,
                status: 500,
              });
            }),
          );
        const newRunSessionId = yield* Schema.decodeUnknown(SessionId)(
          newSessionId,
        ).pipe(
          Effect.catchTags({
            ParseError: () =>
              Effect.fail(
                new AdminError({
                  message: "Invalid session id from Linear",
                  status: 500,
                }),
              ),
          }),
        );
        const rerunStatus = yield* deps.runRepo.createIfNoActiveForIssue({
          sessionId: newRunSessionId,
          organizationId: run.organizationId,
          issueId: Option.some(issueId),
          repositoryId: run.repositoryId,
          teamId: run.teamId,
          projectId: run.projectId,
        });
        if (rerunStatus === "active") {
          return Option.some(
            text("A run for this issue is already active", 409),
          );
        }
        const rerunCreatedAt = yield* Clock.currentTimeMillis;
        const inputId = yield* Schema.decodeUnknown(InputId)(
          `${newSessionId}:created`,
        ).pipe(
          Effect.catchTags({
            ParseError: () =>
              Effect.fail(
                new AdminError({ message: "Invalid input id", status: 500 }),
              ),
          }),
        );
        const body = `User request:\nWork on the issue below.\n\nIssue context:\nIssue: ${issueId}`;
        yield* deps.runInputRepo.enqueue({
          id: inputId,
          sessionId: newRunSessionId,
          kind: "created",
          body,
          payload: {
            type: "AgentSessionEvent",
            action: "created",
            organizationId: run.organizationId,
            ...(Option.isSome(run.repositoryId)
              ? { repositoryId: run.repositoryId.value }
              : {}),
            automationDelegated: false,
            appUserId: "synthetic",
            oauthClientId: "synthetic",
            webhookId: "synthetic",
            webhookTimestamp: rerunCreatedAt,
            agentSession: {
              id: newSessionId,
              appUserId: "synthetic",
              organizationId: run.organizationId,
              status: "pending",
              createdAt: rerunCreatedAt,
              updatedAt: rerunCreatedAt,
              issueId,
            },
          },
          createdAt: rerunCreatedAt,
        });
        return Option.some(json({ sessionId: newSessionId }));
      }
      if (url.pathname === "/api/admin/bootstrap" && request.method === "GET") {
        const session = yield* requireSession(request);
        const installation = yield* deps.installationRepo.get(
          session.organizationId,
        );
        const adminInstallation = Option.match(installation, {
          onNone: () => null,
          onSome: toAdminInstallation,
        });
        const repositories = yield* deps.workspaceRepo.listRepositories(
          session.organizationId,
        );
        const mcpServers = deps.mcpServerRepo
          ? yield* deps.mcpServerRepo.listMcpServers(session.organizationId)
          : [];
        const reconcilerStatus = yield* deps.reconciler.status();
        return Option.some(
          json({
            health: {
              installation:
                adminInstallation !== null &&
                adminInstallation.revokedAt === null,
              reconciler: reconcilerStatus,
            },
            installation: adminInstallation,
            repositories: repositories.map(toApiRepository),
            mcpServers: mcpServers.map(toApiMcpServer),
            csrfToken: deriveCsrfToken(session.rawToken),
          }),
        );
      }

      if (
        url.pathname === "/api/admin/repositories" &&
        request.method === "GET"
      ) {
        const session = yield* requireSession(request);
        const repositories = yield* deps.workspaceRepo.listRepositories(
          session.organizationId,
        );
        return Option.some(
          json({ repositories: repositories.map(toApiRepository) }),
        );
      }
      if (
        url.pathname === "/api/admin/mcp-servers" &&
        request.method === "GET"
      ) {
        const session = yield* requireSession(request);
        const mcpServers = deps.mcpServerRepo
          ? yield* deps.mcpServerRepo.listMcpServers(session.organizationId)
          : [];
        return Option.some(
          json({ mcpServers: mcpServers.map(toApiMcpServer) }),
        );
      }

      if (
        url.pathname === "/api/admin/mcp-servers" &&
        request.method === "POST"
      ) {
        const session = yield* requireMutation(request);
        if (!deps.mcpServerRepo)
          return yield* Effect.fail(
            new AdminError({
              message: "MCP server repository unavailable",
              status: 500,
            }),
          );
        const body = yield* parseJsonBody(request);
        const payloadEither = mcpServerPayload(body);
        if (Either.isLeft(payloadEither))
          return Option.some(text(payloadEither.left, 400));
        const payload = payloadEither.right;
        const repositoryId =
          payload.repositoryId === null
            ? Option.none<WorkspaceId>()
            : yield* Schema.decodeUnknown(WorkspaceId)(payload.repositoryId)
                .pipe(
                  Effect.catchTags({
                    ParseError: () =>
                      Effect.fail(
                        new AdminError({
                          message: "Invalid repository id",
                          status: 400,
                        }),
                      ),
                  }),
                )
                .pipe(Effect.map(Option.some));
        const id = yield* Schema.decodeUnknown(McpServerId)(payload.id).pipe(
          Effect.catchTags({
            ParseError: () =>
              Effect.fail(
                new AdminError({
                  message: "Invalid MCP server id",
                  status: 400,
                }),
              ),
          }),
        );
        const server = yield* deps.mcpServerRepo.createMcpServer({
          organizationId: session.organizationId,
          id,
          name: payload.name,
          transport: payload.transport,
          command: payload.command,
          args: payload.args,
          url: payload.url,
          env: payload.env,
          repositoryId,
          enabled: payload.enabled ?? true,
          now,
        });
        return Option.some(json({ mcpServer: toApiMcpServer(server) }, 201));
      }

      if (url.pathname.startsWith("/api/admin/mcp-servers/")) {
        const rawId = decodeURIComponent(
          url.pathname.slice("/api/admin/mcp-servers/".length),
        );
        const id = yield* Schema.decodeUnknown(McpServerId)(rawId).pipe(
          Effect.catchTags({
            ParseError: () =>
              Effect.fail(
                new AdminError({
                  message: "Invalid MCP server id",
                  status: 400,
                }),
              ),
          }),
        );
        if (request.method === "GET") {
          const session = yield* requireSession(request);
          if (!deps.mcpServerRepo) return Option.some(text("Not found", 404));
          const server = yield* deps.mcpServerRepo.getMcpServer(
            session.organizationId,
            id,
          );
          return Option.isNone(server)
            ? Option.some(text("Not found", 404))
            : Option.some(json({ mcpServer: toApiMcpServer(server.value) }));
        }
        if (request.method === "PUT") {
          const session = yield* requireMutation(request);
          if (!deps.mcpServerRepo)
            return yield* Effect.fail(
              new AdminError({
                message: "MCP server repository unavailable",
                status: 500,
              }),
            );
          const body = yield* parseJsonBody(request);
          const payloadEither = mcpServerPayload(body);
          if (Either.isLeft(payloadEither))
            return Option.some(text(payloadEither.left, 400));
          const payload = payloadEither.right;
          if (payload.id !== id)
            return Option.some(
              text("MCP server id in body does not match path", 400),
            );
          const current = yield* deps.mcpServerRepo.getMcpServer(
            session.organizationId,
            id,
          );
          if (Option.isNone(current))
            return Option.some(text("Not found", 404));
          const repositoryId =
            payload.repositoryId === null
              ? Option.none<WorkspaceId>()
              : yield* Schema.decodeUnknown(WorkspaceId)(payload.repositoryId)
                  .pipe(
                    Effect.catchTags({
                      ParseError: () =>
                        Effect.fail(
                          new AdminError({
                            message: "Invalid repository id",
                            status: 400,
                          }),
                        ),
                    }),
                  )
                  .pipe(Effect.map(Option.some));
          const env: Record<string, string> =
            body.env === undefined ? { ...current.value.env } : {};
          if (body.env !== undefined) {
            for (const [key, value] of Object.entries(payload.env)) {
              const preserved = current.value.env[key];
              env[key] =
                value === "•••" && preserved !== undefined ? preserved : value;
            }
          }
          const server = yield* deps.mcpServerRepo.updateMcpServer(
            session.organizationId,
            id,
            {
              name: payload.name,
              transport: payload.transport,
              command: payload.command,
              args: payload.args,
              url: payload.url,
              env,
              repositoryId,
              enabled: payload.enabled,
              now,
            },
          );
          return Option.some(json({ mcpServer: toApiMcpServer(server) }));
        }
        if (request.method === "DELETE") {
          const session = yield* requireMutation(request);
          if (!deps.mcpServerRepo) return Option.some(text("Not found", 404));
          const deleted = yield* deps.mcpServerRepo.deleteMcpServer(
            session.organizationId,
            id,
          );
          return deleted
            ? Option.some(emptyResponse(204))
            : Option.some(text("Not found", 404));
        }
      }

      if (url.pathname === "/api/admin/nix-cache" && request.method === "GET") {
        yield* requireSession(request);
        const entries = yield* deps.nixEnvironment.list();
        return Option.some(json({ entries: entries.map(toApiNixCache) }));
      }

      const nixCachePrune =
        /^\/api\/admin\/nix-cache\/([a-f0-9]{64})\/prune$/u.exec(url.pathname);
      if (nixCachePrune !== null && request.method === "POST") {
        const cacheKey = nixCachePrune[1];
        if (cacheKey === undefined) {
          return Option.some(text("Invalid Nix cache key", 400));
        }
        yield* requireMutation(request);
        const pruned = yield* deps.nixEnvironment.prune(cacheKey);
        return Option.some(json({ pruned }));
      }

      if (
        url.pathname.startsWith("/api/admin/nix-cache/") &&
        request.method === "POST"
      ) {
        yield* requireMutation(request);
        return Option.some(text("Invalid Nix cache key", 400));
      }

      if (
        url.pathname === "/api/admin/repositories" &&
        request.method === "POST"
      ) {
        const session = yield* requireMutation(request);
        const body = yield* parseJsonBody(request);
        const payloadEither = repositoryPayload(body);
        if (Either.isLeft(payloadEither)) {
          return Option.some(text(payloadEither.left, 400));
        }
        const payload = payloadEither.right;
        const repositoryId = yield* Schema.decodeUnknown(WorkspaceId)(
          payload.id,
        ).pipe(
          Effect.catchTags({
            ParseError: () =>
              Effect.fail(
                new AdminError({
                  message: "Invalid repository id",
                  status: 400,
                }),
              ),
          }),
        );
        const teamIds = yield* Schema.decodeUnknown(Schema.Array(TeamId))(
          payload.teamIds,
        ).pipe(
          Effect.catchTags({
            ParseError: () =>
              Effect.fail(
                new AdminError({ message: "Invalid team ids", status: 400 }),
              ),
          }),
        );
        const projectIds = yield* Schema.decodeUnknown(Schema.Array(ProjectId))(
          payload.projectIds,
        ).pipe(
          Effect.catchTags({
            ParseError: () =>
              Effect.fail(
                new AdminError({ message: "Invalid project ids", status: 400 }),
              ),
          }),
        );
        const repository = yield* deps.workspaceRepo.createRepository({
          organizationId: session.organizationId,
          id: repositoryId,
          url: payload.url,
          ref: payload.ref,
          teamIds,
          projectIds,
          labels: payload.labels,
          nixPackages: payload.nixPackages,
          isDefault: payload.isDefault ?? false,
          now,
        });
        return Option.some(
          json({ repository: toApiRepository(repository) }, 201),
        );
      }

      if (url.pathname.startsWith("/api/admin/repositories/")) {
        const rawId = decodeURIComponent(
          url.pathname.slice("/api/admin/repositories/".length),
        );
        const id = yield* Schema.decodeUnknown(WorkspaceId)(rawId).pipe(
          Effect.catchTags({
            ParseError: () =>
              Effect.fail(
                new AdminError({
                  message: "Invalid repository id",
                  status: 400,
                }),
              ),
          }),
        );

        if (request.method === "PUT") {
          const session = yield* requireMutation(request);
          const body = yield* parseJsonBody(request);
          const payloadEither = repositoryPayload(body);
          if (Either.isLeft(payloadEither)) {
            return Option.some(text(payloadEither.left, 400));
          }
          const payload = payloadEither.right;
          if (payload.id !== id) {
            return Option.some(
              text("Repository id in body does not match path", 400),
            );
          }
          const teamIds = yield* Schema.decodeUnknown(Schema.Array(TeamId))(
            payload.teamIds,
          ).pipe(
            Effect.catchTags({
              ParseError: () =>
                Effect.fail(
                  new AdminError({ message: "Invalid team ids", status: 400 }),
                ),
            }),
          );
          const projectIds = yield* Schema.decodeUnknown(
            Schema.Array(ProjectId),
          )(payload.projectIds).pipe(
            Effect.catchTags({
              ParseError: () =>
                Effect.fail(
                  new AdminError({
                    message: "Invalid project ids",
                    status: 400,
                  }),
                ),
            }),
          );
          const repository = yield* deps.workspaceRepo.updateRepository(
            session.organizationId,
            id,
            {
              url: payload.url,
              ref: payload.ref,
              teamIds,
              projectIds,
              labels: payload.labels,
              nixPackages: payload.nixPackages,
              ...(payload.isDefault !== undefined
                ? { isDefault: payload.isDefault }
                : {}),
              now,
            },
          );
          return Option.some(json({ repository: toApiRepository(repository) }));
        }

        if (request.method === "DELETE") {
          const session = yield* requireMutation(request);
          const deleted = yield* deps.workspaceRepo.deleteRepository(
            session.organizationId,
            id,
          );
          if (!deleted) {
            return Option.some(text("Not found", 404));
          }
          return Option.some(emptyResponse(204));
        }
      }

      if (url.pathname === "/api/admin/preview" && request.method === "POST") {
        const session = yield* requireMutation(request);
        const body = yield* parseJsonBody(request);
        const issueLabels = optionalStringArray(
          body.issueLabels,
          "issueLabels",
        );
        if (Either.isLeft(issueLabels)) {
          return Option.some(text(issueLabels.left, 400));
        }
        const projectLabels = optionalStringArray(
          body.projectLabels,
          "projectLabels",
        );
        if (Either.isLeft(projectLabels)) {
          return Option.some(text(projectLabels.left, 400));
        }
        const resolution = yield* deps.workspace.resolve({
          organizationId: session.organizationId,
          teamId: optionalString(body.teamId),
          projectId: optionalString(body.projectId),
          repositoryId: optionalString(body.repositoryId),
          issueLabels: issueLabels.right,
          projectLabels: projectLabels.right,
        });
        return Option.some(
          json({
            kind: resolution.kind,
            repository:
              resolution.kind === "match"
                ? toApiRepository(resolution.repository)
                : null,
            repositories:
              resolution.kind === "ambiguous"
                ? resolution.repositories.map(toApiRepository)
                : null,
          }),
        );
      }

      if (url.pathname === "/api/admin/logout" && request.method === "POST") {
        const session = yield* requireMutation(request);
        yield* deps.adminSessionRepo.deleteAdminSession(
          tokenHash(session.rawToken),
        );
        return Option.some(
          redirect("/", 302, {
            "set-cookie": clearAdminCookie(deps.config),
          }),
        );
      }

      return Option.none();
    },
    (effect) =>
      effect.pipe(
        Effect.catchAllCause((cause) =>
          Effect.succeed(mapCauseToResponse(cause)),
        ),
      ),
  );

export const createAdminSession = (
  deps: Pick<AdminDeps, "config" | "adminSessionRepo">,
  organizationId: OrganizationId,
  now = Date.now(),
): Effect.Effect<
  { readonly token: string; readonly csrf: string; readonly expiresAt: number },
  DatabaseError
> =>
  Effect.gen(function* () {
    const token = randomBytes(32).toString("base64url");
    const csrf = deriveCsrfToken(token);
    const expiresAt = now + SEVEN_DAYS_MS;
    yield* deps.adminSessionRepo.create({
      organizationId,
      tokenHash: tokenHash(token),
      csrfTokenHash: csrfHash(csrf),
      expiresAt,
      now,
    });
    return { token, csrf, expiresAt };
  });

export class Admin extends Effect.Service<Admin>()("Admin", {
  accessors: true,
  dependencies: [
    GatewayConfig.Default,
    AdminSessionRepo.Default,
    InstallationRepo.Default,
    RunRepo.Default,
    RunEventRepo.Default,
    RunInputRepo.Default,
    LinearGateway.Default,
    WorkspaceRepo.Default,
    McpServerRepo.Default,
    Workspace.Default,
    Reconciler.Default,
    NixEnvironment.Default,
  ],
  effect: Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const adminSessionRepo = yield* AdminSessionRepo;
    const installationRepo = yield* InstallationRepo;
    const runRepo = yield* RunRepo;
    const runEventRepo = yield* RunEventRepo;
    const workspaceRepo = yield* WorkspaceRepo;
    const mcpServerRepo = yield* McpServerRepo;
    const workspace = yield* Workspace;
    const reconciler = yield* Reconciler;
    const nixEnvironment = yield* NixEnvironment;
    const runInputRepo = yield* RunInputRepo;
    const linearGateway = yield* LinearGateway;
    const handle = createAdminHandle({
      config,
      adminSessionRepo,
      installationRepo,
      runRepo,
      runEventRepo,
      runInputRepo,
      linearGateway,
      workspaceRepo,
      mcpServerRepo,
      workspace,
      reconciler,
      nixEnvironment,
    });
    return { handle };
  }),
}) {}
