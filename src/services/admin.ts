import {
  createHash,
  createHmac,
  randomBytes,
  timingSafeEqual,
} from "node:crypto";
import {
  Cause,
  Clock,
  Context,
  Effect,
  Layer,
  Option,
  Result,
  Schema,
  Semaphore,
} from "effect";
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
  type OAuthStateError,
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
import { normalizeNixPackages, TERMINAL_RUN_STATES } from "../domain/models.js";
import {
  GatewayConfig,
  type GatewayConfigShape,
  organizationAllowed,
} from "./config.js";
import {
  Executor,
  type ExecutorRequestError,
  executorUrlForPath,
} from "./executor.js";
import { LinearGateway } from "./linear-gateway.js";
import { McpOAuth, type McpOAuthTokenEndpointAuthMethod } from "./mcp-oauth.js";
import { NixEnvironment } from "./nix-environment.js";
import {
  promptTemplateWarnings,
  substitutePromptTemplate,
} from "./prompt-templates.js";
import { Reconciler, type ReconcilerStatus } from "./reconciler.js";
import {
  AdminSessionRepo,
  ExecutorInstanceRepo,
  InstallationRepo,
  McpServerRepo,
  PromptTemplateRepo,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
  WorkspaceRepo,
} from "./store/repositories.js";
import { runEventCursor } from "./store/run-event-repo.js";
import {
  isHttpsGitHubRepositoryUrl,
  type RepositoryResolution,
  Workspace,
} from "./workspace.js";

const ADMIN_COOKIE = "omp_gateway_admin";
const CSRF_SALT = "omp-gateway-admin-csrf";
const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;
const HTTP_TOKEN_RE = /^[A-Za-z0-9!#$%&'*+.^_`|~-]+$/u;
// Linear exposes no public session-delete API, so preventing duplicate reruns
// before creating a session is the only way to avoid orphaned sessions.
const rerunIssueGates = new Map<
  string,
  { semaphore: Semaphore.Semaphore; users: number }
>();

export const withRerunIssueGate = <A, E, R>(
  organizationId: OrganizationId,
  issueId: string,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      const key = `${organizationId}:${issueId}`;
      const entry = rerunIssueGates.get(key) ?? {
        semaphore: Semaphore.makeUnsafe(1),
        users: 0,
      };
      entry.users += 1;
      rerunIssueGates.set(key, entry);
      return { key, entry };
    }),
    ({ entry }) => entry.semaphore.withPermits(1)(effect),
    ({ key, entry }) =>
      Effect.sync(() => {
        entry.users -= 1;
        if (entry.users === 0 && rerunIssueGates.get(key) === entry)
          rerunIssueGates.delete(key);
      }),
  );
export const rerunIssueGateCount = (): number => rerunIssueGates.size;

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
  readonly trigger: () => Effect.Effect<void, never>;
}
export interface AdminDeps {
  readonly config: GatewayConfigShape;
  readonly adminSessionRepo: AdminSessionRepo["Service"];
  readonly executorInstanceRepo: ExecutorInstanceRepo["Service"];
  readonly executor: {
    readonly listToolkits: Executor["Service"]["listToolkits"];
  };
  readonly installationRepo: InstallationRepo["Service"];
  readonly runRepo: RunRepo["Service"];
  readonly runEventRepo: RunEventRepo["Service"];
  readonly runInputRepo: RunInputRepo["Service"];
  readonly linearGateway: {
    readonly createSessionOnIssue: LinearGateway["Service"]["createSessionOnIssue"];
  };
  readonly workspaceRepo: WorkspaceRepo["Service"];
  readonly promptTemplateRepo: PromptTemplateRepo["Service"];
  readonly mcpServerRepo: McpServerRepo["Service"];
  readonly mcpOAuth: McpOAuth["Service"];
  readonly workspace: WorkspaceShape;
  readonly reconciler: ReconcilerShape;
  readonly nixEnvironment: NixEnvironment["Service"];
}

interface RepositoryPayload {
  readonly id: string;
  readonly url: string;
  readonly ref: string;
  readonly teamIds: ReadonlyArray<string>;
  readonly projectIds: ReadonlyArray<string>;
  readonly labels: ReadonlyArray<string>;
  readonly isDefault: boolean | undefined;
  readonly defaultDriver: "omp" | "t3" | undefined;
  readonly nixPackages: ReadonlyArray<NixPackageName>;
}

class AdminError extends Schema.TaggedError<AdminError>()(
  "@Gateway/AdminError",
  {
    message: Schema.String,
    status: Schema.Literals([400, 401, 403, 404, 409, 429, 500, 502]),
  },
) {}

const RunEventCursorParam = Schema.StringFromBase64Url.pipe(
  Schema.decodeTo(
    Schema.fromJsonString(
      Schema.Struct({
        createdAt: Schema.Finite,
        sourceKey: Schema.String,
      }),
    ),
  ),
);
const RunEventQuery = Schema.Struct({
  limit: Schema.NumberFromString.check(
    Schema.isInt(),
    Schema.isBetween({ minimum: 1, maximum: 1000 }),
  ),
  cursor: Schema.optionalKey(RunEventCursorParam),
});

function isSecure(config: { publicUrl: URL }): boolean {
  return config.publicUrl.protocol === "https:";
}

function adminCookieAttributes(
  config: { publicUrl: URL },
  expiresAt?: number,
): string {
  const base = "HttpOnly; SameSite=Lax; Path=/";
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
  try {
    return match[1] ? decodeURIComponent(match[1]) : null;
  } catch {
    return null;
  }
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
): Result.Result<ReadonlyArray<string>, string> {
  if (!Array.isArray(value)) return Result.fail(`${field} must be an array`);
  if (value.some((item) => typeof item !== "string"))
    return Result.fail(`${field} must contain only strings`);
  return Result.succeed(
    value.map((item) => String(item).trim().toLowerCase()).filter(Boolean),
  );
}
function preservingStringArray(
  value: unknown,
  field: string,
): Result.Result<ReadonlyArray<string>, string> {
  if (!Array.isArray(value)) return Result.fail(`${field} must be an array`);
  if (value.some((item) => typeof item !== "string")) {
    return Result.fail(`${field} must contain only strings`);
  }
  return Result.succeed(
    value.map((item) => String(item).trim()).filter(Boolean),
  );
}

function optionalStringArray(
  value: unknown,
  field: string,
): Result.Result<ReadonlyArray<string>, string> {
  if (value === undefined || value === null) return Result.succeed([]);
  return stringArray(value, field);
}

function optionalNixPackageArray(
  value: unknown,
): Result.Result<ReadonlyArray<NixPackageName>, string> {
  if (value === undefined || value === null) return Result.succeed([]);
  if (!Array.isArray(value)) return Result.fail("nixPackages must be an array");
  if (value.some((item) => typeof item !== "string")) {
    return Result.fail("nixPackages must contain only strings");
  }
  const values = value.map((item) => item.trim()).filter(Boolean);
  if (
    values.some(
      (item) => !/^[A-Za-z0-9_+-]+(?:\.[A-Za-z0-9_+-]+)*$/u.test(item),
    )
  ) {
    return Result.fail("Invalid Nix package name");
  }
  return Result.succeed(normalizeNixPackages(values));
}

function optionalBoolean(value: unknown): boolean | undefined {
  if (typeof value === "boolean") return value;
  return undefined;
}

function repositoryPayload(
  body: Record<string, unknown>,
): Result.Result<RepositoryPayload, string> {
  const id = optionalString(body.id);
  const url = optionalString(body.url);
  const ref = optionalString(body.ref);
  if (id === null) return Result.fail("id is required");
  if (url === null) return Result.fail("url is required");
  if (!isHttpsGitHubRepositoryUrl(url))
    return Result.fail("url must be https://github.com/<owner>/<repo>");
  if (ref === null) return Result.fail("ref is required");
  if (ref.startsWith("-") || /[\r\n\0]/u.test(ref))
    return Result.fail("Invalid repository ref");
  const teamIds = optionalStringArray(body.teamIds, "teamIds");
  if (Result.isFailure(teamIds)) return Result.fail(teamIds.failure);
  const projectIds = optionalStringArray(body.projectIds, "projectIds");
  if (Result.isFailure(projectIds)) return Result.fail(projectIds.failure);
  const labels = optionalStringArray(body.labels, "labels");
  if (Result.isFailure(labels)) return Result.fail(labels.failure);
  const nixPackages = optionalNixPackageArray(body.nixPackages);
  if (Result.isFailure(nixPackages)) return Result.fail(nixPackages.failure);
  if (
    body.defaultDriver !== undefined &&
    body.defaultDriver !== "omp" &&
    body.defaultDriver !== "t3"
  ) {
    return Result.fail("defaultDriver must be omp or t3");
  }
  return Result.succeed({
    id,
    url,
    ref,
    defaultDriver: body.defaultDriver,
    teamIds: teamIds.success,
    projectIds: projectIds.success,
    labels: labels.success,
    nixPackages: nixPackages.success,
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
    defaultDriver: repository.defaultDriver ?? "omp",
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
  readonly headers: Readonly<Record<string, string>>;
  readonly oauthClientId: string | null;
  readonly oauthClientSecret: string | null;
  readonly oauthScope: string | null;
  readonly oauthTokenEndpointAuthMethod: McpOAuthTokenEndpointAuthMethod | null;
  readonly repositoryId: string | null;
  readonly enabled: boolean | undefined;
}

function mcpServerPayload(
  body: Record<string, unknown>,
): Result.Result<McpServerPayload, string> {
  const id = optionalString(body.id);
  const name = optionalString(body.name);
  if (id === null) return Result.fail("id is required");
  if (name === null) return Result.fail("name is required");
  const rawTransport = body.transport;
  if (
    rawTransport !== "stdio" &&
    rawTransport !== "http" &&
    rawTransport !== "sse"
  ) {
    return Result.fail("transport must be stdio, http, or sse");
  }
  const command = body.command === null ? null : optionalString(body.command);
  const url = body.url === null ? null : optionalString(body.url);
  if (rawTransport === "stdio" && command === null) {
    return Result.fail("stdio transport requires a command");
  }
  if (rawTransport !== "stdio") {
    if (url === null) {
      return Result.fail(`${rawTransport} transport requires a valid URL`);
    }
    try {
      const parsed = new URL(url);
      if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
        return Result.fail(
          `${rawTransport} transport requires a valid http(s) URL`,
        );
      }
    } catch {
      return Result.fail(
        `${rawTransport} transport requires a valid http(s) URL`,
      );
    }
  }
  const argsResult = preservingStringArray(body.args, "args");
  if (Result.isFailure(argsResult)) return Result.fail(argsResult.failure);
  const args = argsResult.success;
  let repositoryId: string | null;
  if (body.repositoryId === null || body.repositoryId === undefined) {
    repositoryId = null;
  } else if (typeof body.repositoryId !== "string") {
    return Result.fail("repositoryId must be a string or null");
  } else {
    repositoryId = optionalString(body.repositoryId);
  }
  const envValue = body.env;
  const env: Record<string, string> = {};
  if (envValue !== undefined && envValue !== null) {
    if (!record(envValue)) return Result.fail("env must be an object");
    for (const [key, value] of Object.entries(envValue)) {
      if (typeof value !== "string")
        return Result.fail(`env.${key} must be a string`);
      env[key] = value;
    }
  }
  const headersValue = body.headers;
  const headers: Record<string, string> = {};
  if (headersValue !== undefined && headersValue !== null) {
    if (!record(headersValue)) return Result.fail("headers must be an object");
    const invalidHeaderNames = Object.keys(headersValue).filter(
      (key) => !HTTP_TOKEN_RE.test(key),
    );
    if (invalidHeaderNames.length > 0) {
      return Result.fail(
        `Invalid MCP header name: ${invalidHeaderNames.join(", ")}`,
      );
    }
    for (const [key, value] of Object.entries(headersValue)) {
      if (typeof value !== "string")
        return Result.fail(`headers.${key} must be a string`);
      headers[key] = value;
    }
  }
  const oauthClientId = optionalString(body.oauthClientId);
  const oauthClientSecret = optionalString(body.oauthClientSecret);
  const oauthScope = optionalString(body.oauthScope);
  const rawAuthMethod = body.oauthTokenEndpointAuthMethod;
  const oauthTokenEndpointAuthMethod =
    rawAuthMethod === undefined ||
    rawAuthMethod === null ||
    rawAuthMethod === ""
      ? null
      : rawAuthMethod === "none" ||
          rawAuthMethod === "client_secret_basic" ||
          rawAuthMethod === "client_secret_post"
        ? rawAuthMethod
        : null;
  if (
    rawAuthMethod !== undefined &&
    rawAuthMethod !== null &&
    rawAuthMethod !== "" &&
    oauthTokenEndpointAuthMethod === null
  ) {
    return Result.fail(
      "oauthTokenEndpointAuthMethod must be none, client_secret_basic, or client_secret_post",
    );
  }
  if (oauthClientSecret !== null && oauthClientId === null) {
    return Result.fail(
      "oauthClientId is required when oauthClientSecret is set",
    );
  }
  if (body.enabled !== undefined && typeof body.enabled !== "boolean") {
    return Result.fail("enabled must be a boolean");
  }
  return Result.succeed({
    id,
    name,
    transport: rawTransport,
    command,
    args,
    url,
    env,
    headers,
    oauthClientId,
    oauthClientSecret,
    oauthScope,
    oauthTokenEndpointAuthMethod,
    repositoryId,
    enabled: optionalBoolean(body.enabled),
  });
}
export function toApiMcpServer(
  server: McpServerRecord,
  oauthStatus?: {
    readonly connected: boolean;
    readonly serverUrl?: string;
    readonly expired: boolean;
    readonly expiresAt: number | null;
  },
) {
  const env: Record<string, string> = {};
  for (const name of Object.keys(server.env)) env[name] = "•••";
  const headers: Record<string, string> = {};
  for (const name of Object.keys(server.headers ?? {})) {
    headers[name] = "•••";
  }
  return {
    oauthClient:
      server.oauthClient === undefined || server.oauthClient === null
        ? null
        : {
            clientId: server.oauthClient.clientId ?? null,
            clientSecret:
              server.oauthClient.clientSecret === undefined ? null : "•••",
            scope: server.oauthClient.scope ?? null,
            tokenEndpointAuthMethod:
              server.oauthClient.tokenEndpointAuthMethod ?? null,
          },
    id: server.id,
    organizationId: server.organizationId,
    name: server.name,
    transport: server.transport,
    command: Option.getOrElse(server.command, () => null),
    args: [...server.args],
    url: Option.getOrElse(server.url, () => null),
    env,
    headers,
    repositoryId: Option.getOrElse(server.repositoryId, () => null),
    enabled: server.enabled,
    oauth: oauthStatus ?? {
      connected: false,
      expired: false,
      expiresAt: null,
    },
    createdAt: server.createdAt,
    updatedAt: server.updatedAt,
  };
}
const EXECUTOR_ATTACHMENT_PREFIX = "executor:";

const executorAttachmentSlug = (server: McpServerRecord): string | null => {
  if (!server.name.startsWith(EXECUTOR_ATTACHMENT_PREFIX)) return null;
  const marker = server.name
    .slice(EXECUTOR_ATTACHMENT_PREFIX.length)
    .split(":", 1)[0];
  if (!marker) return null;
  try {
    return decodeURIComponent(marker);
  } catch {
    return null;
  }
};

const synchronizeExecutorAttachments = (
  deps: AdminDeps,
  organizationId: OrganizationId,
  endpoint: string | null,
  token: string | null,
  now: number,
): Effect.Effect<void, DatabaseError | RowDecodeError | TokenCipherError> =>
  Effect.gen(function* () {
    const servers = yield* deps.mcpServerRepo.listMcpServers(organizationId);
    yield* Effect.forEach(servers, (server) => {
      if (executorAttachmentSlug(server) === null) return Effect.void;
      if (endpoint === null || token === null) {
        return deps.mcpServerRepo
          .updateMcpServer(organizationId, server.id, {
            enabled: false,
            now,
          })
          .pipe(Effect.asVoid);
      }
      const slug = executorAttachmentSlug(server);
      if (slug === null) return Effect.void;
      return deps.mcpServerRepo
        .updateMcpServer(organizationId, server.id, {
          url: executorUrlForPath(
            endpoint,
            `mcp/toolkits/${encodeURIComponent(slug)}`,
          ),
          headers: { Authorization: `Bearer ${token}` },
          now,
        })
        .pipe(Effect.asVoid);
    });
  });

function toApiExecutorInstance(instance: {
  readonly endpoint: string;
  readonly updatedAt: number;
}) {
  return {
    endpoint: instance.endpoint,
    token: "•••",
    updatedAt: instance.updatedAt,
  };
}

function executorError(error: ExecutorRequestError): AdminError {
  const status =
    error.reason === "not_configured"
      ? 404
      : error.reason === "invalid_endpoint"
        ? 400
        : 502;
  return new AdminError({ message: error.message, status });
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

function hasIssueEvent(events: ReadonlyArray<RunEvent>): boolean {
  return events.some(
    (event) =>
      record(event.payload) &&
      record(event.payload.agentSession) &&
      record(event.payload.agentSession.issue),
  );
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
  issueEvents: ReadonlyArray<RunEvent> = events,
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
    issue: issueForRun(run, issueEvents),
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
  const typed = cause.reasons
    .filter(Cause.isFailReason)
    .map((reason) => reason.error);
  const defects = cause.reasons
    .filter(Cause.isDieReason)
    .map((reason) => reason.defect);
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
      | OAuthStateError
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
          if (
            Option.isNone(session) ||
            !organizationAllowed(deps.config, session.value.organizationId)
          ) {
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

      if (url.pathname !== "/") {
        const authenticated = yield* requireSession(request).pipe(
          Effect.result,
        );
        if (Result.isFailure(authenticated)) {
          if (
            request.method === "GET" &&
            !url.pathname.endsWith(".json") &&
            !url.pathname.startsWith("/api/") &&
            !request.headers.get("accept")?.includes("application/json")
          ) {
            return Option.some(
              redirect(
                `/oauth/start?returnTo=${encodeURIComponent(url.pathname + url.search)}`,
              ),
            );
          }
          return Option.some(text("Unauthorized", 401));
        }
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
        return Option.some(
          html(
            renderAdminPage(
              deps.config.gatewayAdminOrganizationIds?.has(
                session.value.organizationId,
              ) === true,
            ),
          ),
        );
      }

      if (url.pathname.startsWith("/runs/") && request.method === "GET") {
        const encodedRunId = url.pathname.slice("/runs/".length);
        const isJsonPath = encodedRunId.endsWith(".json");
        const rawSessionId = decodeURIComponent(
          isJsonPath ? encodedRunId.slice(0, -".json".length) : encodedRunId,
        );
        const sessionId = yield* Schema.decodeUnknownEffect(SessionId)(
          rawSessionId,
        ).pipe(
          Effect.catchTags({
            SchemaError: () =>
              Effect.fail(
                new AdminError({ message: "Invalid run id", status: 400 }),
              ),
          }),
        );
        const runOption = yield* deps.runRepo.get(sessionId);
        if (Option.isNone(runOption)) {
          return Option.some(text("Not found", 404));
        }
        const session = yield* requireSession(request);
        const run = runOption.value;
        if (run.organizationId !== session.organizationId)
          return Option.some(text("Not found", 404));
        const pageOptions = yield* Schema.decodeUnknownEffect(RunEventQuery)({
          limit: url.searchParams.get("limit") ?? "100",
          ...(url.searchParams.has("cursor")
            ? { cursor: url.searchParams.get("cursor") }
            : {}),
        }).pipe(
          Effect.catchTags({
            SchemaError: () =>
              Effect.fail(
                new AdminError({
                  message: "Invalid event pagination",
                  status: 400,
                }),
              ),
          }),
        );
        const events = yield* deps.runEventRepo.list(sessionId, pageOptions);
        const lastEvent = events.at(-1);
        let nextEventsUrl: string | null = null;
        if (events.length === pageOptions.limit && lastEvent !== undefined) {
          const cursor = runEventCursor(lastEvent);
          const remaining = yield* deps.runEventRepo.list(sessionId, {
            limit: 1,
            cursor,
          });
          if (remaining.length > 0) {
            const encodedCursor = yield* Schema.encodeEffect(
              RunEventCursorParam,
            )(cursor).pipe(
              Effect.mapError(
                () =>
                  new AdminError({
                    message: "Could not encode event cursor",
                    status: 500,
                  }),
              ),
            );
            const query = new URLSearchParams({
              limit: String(pageOptions.limit),
              cursor: encodedCursor,
            });
            nextEventsUrl = `${url.pathname}?${query}`;
          }
        }
        // Issue metadata arrives with the first webhook events, so later pages
        // read it from the head of the timeline instead of the current page.
        const issueEvents =
          pageOptions.cursor === undefined || hasIssueEvent(events)
            ? events
            : yield* deps.runEventRepo.list(sessionId, { limit: 100 });
        const model = runDetailModel(run, events, issueEvents);

        const rawToken = findCookieValue(request, ADMIN_COOKIE);
        const adminSession =
          rawToken === null
            ? Option.none()
            : yield* deps.adminSessionRepo.get(tokenHash(rawToken), now).pipe(
                Effect.catch((error) =>
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
        const response =
          isJsonPath || acceptsJson
            ? json(detail)
            : html(renderRunDetailPage(model, csrfToken, nextEventsUrl));
        if (nextEventsUrl !== null)
          response.headers.set("Link", `<${nextEventsUrl}>; rel="next"`);
        return Option.some(response);
      }

      const rerunMatch = /^\/api\/admin\/runs\/([^/]+)\/rerun$/u.exec(
        url.pathname,
      );
      if (rerunMatch !== null && request.method === "POST") {
        const rawSessionId = decodeURIComponent(rerunMatch[1] ?? "");
        const sessionId = yield* Schema.decodeUnknownEffect(SessionId)(
          rawSessionId,
        ).pipe(
          Effect.catchTags({
            SchemaError: () =>
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
        if (!TERMINAL_RUN_STATES.includes(run.state)) {
          return Option.some(text("Run is not terminal", 409));
        }
        const issueId = run.issueId.value;
        const rerunResponse = yield* withRerunIssueGate(
          run.organizationId,
          issueId,
          Effect.gen(function* () {
            const hasActiveRun = yield* deps.runRepo.hasActiveForIssue({
              organizationId: run.organizationId,
              issueId,
            });
            if (hasActiveRun) {
              return Option.some(
                text("A run for this issue is already active", 409),
              );
            }
            const rerunTemplate = yield* deps.promptTemplateRepo
              .get(run.organizationId, "created")
              .pipe(
                Effect.catchTag(
                  "@Gateway/DatabaseError",
                  (error) =>
                    new AdminError({
                      message: error.message,
                      status: 500,
                    }),
                ),
              );
            // Admin reruns honor a configured created template; the
            // substitution mirrors the webhook's created body builder with
            // the synthetic context.
            const body = Option.isSome(rerunTemplate)
              ? substitutePromptTemplate(rerunTemplate.value.body, {
                  userRequest: "User request:\nWork on the issue below.",
                  issueContext: `Issue context:\nIssue: ${issueId}`,
                  threadComment: "",
                  previousComments: "",
                  guidance: "",
                })
              : `User request:\nWork on the issue below.\n\nIssue context:\nIssue: ${issueId}`;
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
            const newRunSessionId = yield* Schema.decodeUnknownEffect(
              SessionId,
            )(newSessionId).pipe(
              Effect.catchTags({
                SchemaError: () =>
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
            const inputId = yield* Schema.decodeUnknownEffect(InputId)(
              `${newSessionId}:created`,
            ).pipe(
              Effect.catchTags({
                SchemaError: () =>
                  Effect.fail(
                    new AdminError({
                      message: "Invalid input id",
                      status: 500,
                    }),
                  ),
              }),
            );
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
            yield* deps.reconciler.trigger();
            return Option.some(json({ sessionId: newSessionId }));
          }),
        );
        return rerunResponse;
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
        const mcpServers = yield* deps.mcpServerRepo.listMcpServers(
          session.organizationId,
        );
        const oauthStatuses = yield* deps.mcpOAuth.listStatuses(
          session.organizationId,
        );
        const executorInstance = yield* deps.executorInstanceRepo.get(
          session.organizationId,
        );
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
            executorInstance: Option.match(executorInstance, {
              onNone: () => null,
              onSome: toApiExecutorInstance,
            }),
            csrfToken: deriveCsrfToken(session.rawToken),
            mcpServers: mcpServers.map((server) =>
              toApiMcpServer(
                server,
                (() => {
                  const status = oauthStatuses.get(server.id);
                  const url = Option.getOrElse(server.url, () => null);
                  return status !== undefined &&
                    (status.serverUrl === undefined || status.serverUrl === url)
                    ? status
                    : undefined;
                })(),
              ),
            ),
          }),
        );
      }

      if (
        url.pathname === "/api/admin/prompt-templates" &&
        request.method === "GET"
      ) {
        const session = yield* requireSession(request);
        if (deps.promptTemplateRepo === undefined) {
          return Option.some(text("Prompt templates unavailable", 500));
        }
        const templates = yield* deps.promptTemplateRepo.list(
          session.organizationId,
        );
        return Option.some(json({ templates }));
      }
      if (
        url.pathname === "/api/admin/prompt-templates" &&
        request.method === "PUT"
      ) {
        const session = yield* requireMutation(request);
        if (deps.promptTemplateRepo === undefined) {
          return Option.some(text("Prompt templates unavailable", 500));
        }
        const payload = yield* parseJsonBody(request);
        const kind = yield* Schema.decodeUnknownEffect(
          Schema.Literals(["created", "prompted", "contract"]),
        )(payload.kind).pipe(
          Effect.catchTag(
            "SchemaError",
            () =>
              new AdminError({
                message: "Invalid prompt template kind",
                status: 400,
              }),
          ),
        );
        if (typeof payload.body !== "string") {
          return Option.some(
            text("Prompt template body must be a string", 400),
          );
        }
        const body = payload.body.trim().length === 0 ? "" : payload.body;
        if (new TextEncoder().encode(body).byteLength > 16_384) {
          return Option.some(
            text("Prompt template body exceeds the 16 KiB limit", 400),
          );
        }
        const updatedAt = yield* Clock.currentTimeMillis;
        if (body.length === 0) {
          yield* deps.promptTemplateRepo.remove(session.organizationId, kind);
          return Option.some(
            json({
              template: null,
              warnings: [],
            }),
          );
        }
        yield* deps.promptTemplateRepo.upsert({
          organizationId: session.organizationId,
          kind,
          body,
          updatedAt,
        });
        return Option.some(
          json({
            template: {
              organizationId: session.organizationId,
              kind,
              body,
              updatedAt,
            },
            warnings: promptTemplateWarnings(kind, body),
          }),
        );
      }
      if (
        url.pathname === "/api/admin/executor-instance" &&
        request.method === "GET"
      ) {
        const session = yield* requireSession(request);
        const instance = yield* deps.executorInstanceRepo.get(
          session.organizationId,
        );
        return Option.some(
          json({
            executorInstance: Option.match(instance, {
              onNone: () => null,
              onSome: toApiExecutorInstance,
            }),
          }),
        );
      }
      if (
        url.pathname === "/api/admin/executor-instance" &&
        request.method === "PUT"
      ) {
        const session = yield* requireMutation(request);
        const body = yield* parseJsonBody(request);
        const endpoint = optionalString(body.endpoint);
        if (endpoint === null)
          return Option.some(text("endpoint is required", 400));
        let parsedEndpoint: URL;
        try {
          parsedEndpoint = new URL(endpoint);
        } catch {
          return Option.some(text("endpoint must be a URL", 400));
        }
        if (parsedEndpoint.protocol !== "https:")
          return Option.some(text("endpoint must use https", 400));
        if (
          parsedEndpoint.username ||
          parsedEndpoint.password ||
          parsedEndpoint.search ||
          parsedEndpoint.hash
        )
          return Option.some(
            text(
              "endpoint must not contain credentials, query, or fragment",
              400,
            ),
          );
        const current = yield* deps.executorInstanceRepo.get(
          session.organizationId,
        );
        const rawToken = optionalString(body.token);
        const token =
          rawToken === null || rawToken === "•••"
            ? Option.match(current, {
                onNone: () => null,
                onSome: (entry) => entry.token,
              })
            : rawToken;
        if (token === null) return Option.some(text("token is required", 400));
        const instance = yield* deps.executorInstanceRepo.put({
          organizationId: session.organizationId,
          endpoint: parsedEndpoint.toString().replace(/\/+$/u, ""),
          token,
          updatedAt: now,
        });
        yield* synchronizeExecutorAttachments(
          deps,
          session.organizationId,
          instance.endpoint,
          instance.token,
          now,
        );
        return Option.some(
          json({ executorInstance: toApiExecutorInstance(instance) }),
        );
      }
      if (
        url.pathname === "/api/admin/executor-instance" &&
        request.method === "DELETE"
      ) {
        const session = yield* requireMutation(request);
        yield* deps.executorInstanceRepo.remove(session.organizationId);
        yield* synchronizeExecutorAttachments(
          deps,
          session.organizationId,
          null,
          null,
          now,
        );
        return Option.some(emptyResponse(204));
      }
      if (
        url.pathname === "/api/admin/executor/toolkits" &&
        request.method === "GET"
      ) {
        const session = yield* requireSession(request);
        const toolkits = yield* deps.executor
          .listToolkits(session.organizationId)
          .pipe(Effect.mapError(executorError));
        return Option.some(json({ toolkits }));
      }
      if (
        url.pathname === "/api/admin/executor/attach" &&
        request.method === "POST"
      ) {
        const session = yield* requireMutation(request);
        const body = yield* parseJsonBody(request);
        const slug = optionalString(body.slug);
        if (slug === null) return Option.some(text("slug is required", 400));
        const instance = yield* deps.executorInstanceRepo.get(
          session.organizationId,
        );
        if (Option.isNone(instance))
          return Option.some(text("Executor is not configured", 404));
        const name = `${EXECUTOR_ATTACHMENT_PREFIX}${encodeURIComponent(slug)}:${optionalString(body.name) ?? slug}`;
        const rawId = optionalString(body.id) ?? `executor-${slug}`;
        const id = yield* Schema.decodeUnknownEffect(McpServerId)(rawId).pipe(
          Effect.catchTags({
            SchemaError: () =>
              Effect.fail(
                new AdminError({
                  message: "Invalid MCP server id",
                  status: 400,
                }),
              ),
          }),
        );
        let repositoryId = Option.none<WorkspaceId>();
        if (body.repositoryId !== undefined && body.repositoryId !== null) {
          const value = optionalString(body.repositoryId);
          if (value === null)
            return Option.some(
              text("repositoryId must be a string or null", 400),
            );
          repositoryId = Option.some(
            yield* Schema.decodeUnknownEffect(WorkspaceId)(value).pipe(
              Effect.catchTags({
                SchemaError: () =>
                  Effect.fail(
                    new AdminError({
                      message: "Invalid repository id",
                      status: 400,
                    }),
                  ),
              }),
            ),
          );
        }
        const server = yield* deps.mcpServerRepo.createMcpServer({
          organizationId: session.organizationId,
          id,
          name,
          transport: "http",
          url: executorUrlForPath(
            instance.value.endpoint,
            `mcp/toolkits/${encodeURIComponent(slug)}`,
          ),
          headers: { Authorization: `Bearer ${instance.value.token}` },
          repositoryId,
          enabled: true,
          now,
        });
        return Option.some(json({ mcpServer: toApiMcpServer(server) }, 201));
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
        const oauthStatuses = yield* deps.mcpOAuth.listStatuses(
          session.organizationId,
        );
        const mcpServers = yield* deps.mcpServerRepo.listMcpServers(
          session.organizationId,
        );
        return Option.some(
          json({
            mcpServers: mcpServers.map((server) =>
              toApiMcpServer(
                server,
                (() => {
                  const status = oauthStatuses.get(server.id);
                  const url = Option.getOrElse(server.url, () => null);
                  return status !== undefined &&
                    (status.serverUrl === undefined || status.serverUrl === url)
                    ? status
                    : undefined;
                })(),
              ),
            ),
          }),
        );
      }

      if (
        url.pathname === "/api/admin/mcp-servers" &&
        request.method === "POST"
      ) {
        const session = yield* requireMutation(request);
        const body = yield* parseJsonBody(request);
        const payloadEither = mcpServerPayload(body);
        if (Result.isFailure(payloadEither))
          return Option.some(text(payloadEither.failure, 400));
        const payload = payloadEither.success;
        const repositoryId =
          payload.repositoryId === null
            ? Option.none<WorkspaceId>()
            : yield* Schema.decodeUnknownEffect(WorkspaceId)(
                payload.repositoryId,
              )
                .pipe(
                  Effect.catchTags({
                    SchemaError: () =>
                      Effect.fail(
                        new AdminError({
                          message: "Invalid repository id",
                          status: 400,
                        }),
                      ),
                  }),
                )
                .pipe(Effect.map(Option.some));
        const id = yield* Schema.decodeUnknownEffect(McpServerId)(
          payload.id,
        ).pipe(
          Effect.catchTags({
            SchemaError: () =>
              Effect.fail(
                new AdminError({
                  message: "Invalid MCP server id",
                  status: 400,
                }),
              ),
          }),
        );
        const oauthClient =
          payload.oauthClientId === null && payload.oauthScope === null
            ? null
            : {
                ...(payload.oauthClientId !== null
                  ? { clientId: payload.oauthClientId }
                  : {}),
                ...(payload.oauthClientSecret !== null
                  ? { clientSecret: payload.oauthClientSecret }
                  : {}),
                ...(payload.oauthScope !== null
                  ? { scope: payload.oauthScope }
                  : {}),
                ...(payload.oauthTokenEndpointAuthMethod !== null
                  ? {
                      tokenEndpointAuthMethod:
                        payload.oauthTokenEndpointAuthMethod,
                    }
                  : {}),
              };
        const server = yield* deps.mcpServerRepo.createMcpServer({
          organizationId: session.organizationId,
          id,
          name: payload.name,
          transport: payload.transport,
          command: payload.command,
          args: payload.args,
          url: payload.url,
          env: payload.env,
          headers: payload.headers,
          oauthClient,
          repositoryId,
          enabled: payload.enabled ?? true,
          now,
        });
        return Option.some(json({ mcpServer: toApiMcpServer(server) }, 201));
      }

      const mcpOauthConnect =
        /^\/api\/admin\/mcp-servers\/([^/]+)\/oauth\/connect$/u.exec(
          url.pathname,
        );
      if (mcpOauthConnect !== null && request.method === "POST") {
        const session = yield* requireMutation(request);
        const rawId = mcpOauthConnect[1];
        if (rawId === undefined) return Option.some(text("Not found", 404));
        const id = yield* Schema.decodeUnknownEffect(McpServerId)(
          decodeURIComponent(rawId),
        ).pipe(
          Effect.catchTags({
            SchemaError: () =>
              Effect.fail(
                new AdminError({
                  message: "Invalid MCP server id",
                  status: 400,
                }),
              ),
          }),
        );
        const server = yield* deps.mcpServerRepo.getMcpServer(
          session.organizationId,
          id,
        );
        if (Option.isNone(server)) return Option.some(text("Not found", 404));
        const oauth = Option.isSome(server)
          ? server.value.oauthClient
          : undefined;
        const result = yield* deps.mcpOAuth
          .startMcpAuthorization(
            session.organizationId,
            id,
            oauth?.clientId !== undefined
              ? {
                  clientId: oauth.clientId,
                  ...(oauth.clientSecret !== undefined
                    ? { clientSecret: oauth.clientSecret }
                    : {}),
                  ...(oauth.scope !== undefined ? { scope: oauth.scope } : {}),
                  ...(oauth.tokenEndpointAuthMethod !== undefined
                    ? {
                        tokenEndpointAuthMethod: oauth.tokenEndpointAuthMethod,
                      }
                    : {}),
                }
              : undefined,
            oauth?.scope,
            tokenHash(session.rawToken),
          )
          .pipe(
            Effect.catchTag("@Gateway/McpOAuthError", (error) =>
              Effect.fail(
                new AdminError({
                  message: `MCP OAuth setup failed (${error.reason}): ${error.message}`,
                  status: 400,
                }),
              ),
            ),
          );
        return Option.some(json({ authorizationUrl: result.url.toString() }));
      }
      const mcpOauthDisconnect =
        /^\/api\/admin\/mcp-servers\/([^/]+)\/oauth\/disconnect$/u.exec(
          url.pathname,
        );
      if (mcpOauthDisconnect !== null && request.method === "POST") {
        const session = yield* requireMutation(request);
        const rawId = mcpOauthDisconnect[1];
        if (rawId === undefined) return Option.some(text("Not found", 404));
        const id = yield* Schema.decodeUnknownEffect(McpServerId)(
          decodeURIComponent(rawId),
        ).pipe(
          Effect.catchTags({
            SchemaError: () =>
              Effect.fail(
                new AdminError({
                  message: "Invalid MCP server id",
                  status: 400,
                }),
              ),
          }),
        );
        yield* deps.mcpOAuth.disconnect(session.organizationId, id);
        return Option.some(json({ disconnected: true }));
      }
      if (url.pathname.startsWith("/api/admin/mcp-servers/")) {
        const rawId = decodeURIComponent(
          url.pathname.slice("/api/admin/mcp-servers/".length),
        );
        const id = yield* Schema.decodeUnknownEffect(McpServerId)(rawId).pipe(
          Effect.catchTags({
            SchemaError: () =>
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
          const server = yield* deps.mcpServerRepo.getMcpServer(
            session.organizationId,
            id,
          );
          if (Option.isNone(server)) return Option.some(text("Not found", 404));
          const statuses = yield* deps.mcpOAuth.listStatuses(
            session.organizationId,
          );
          return Option.some(
            json({
              mcpServer: toApiMcpServer(
                server.value,
                (() => {
                  const status = statuses.get(id);
                  const url = Option.getOrElse(server.value.url, () => null);
                  return status !== undefined && status.serverUrl === url
                    ? status
                    : undefined;
                })(),
              ),
            }),
          );
        }
        if (request.method === "PUT") {
          const session = yield* requireMutation(request);
          const body = yield* parseJsonBody(request);
          const payloadEither = mcpServerPayload(body);
          if (Result.isFailure(payloadEither))
            return Option.some(text(payloadEither.failure, 400));
          const payload = payloadEither.success;
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
              : yield* Schema.decodeUnknownEffect(WorkspaceId)(
                  payload.repositoryId,
                )
                  .pipe(
                    Effect.catchTags({
                      SchemaError: () =>
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
          const headers: Record<string, string> =
            body.headers === undefined ? { ...current.value.headers } : {};
          if (body.headers !== undefined) {
            for (const [key, value] of Object.entries(payload.headers)) {
              const preserved = current.value.headers[key];
              headers[key] =
                value === "•••" && preserved !== undefined ? preserved : value;
            }
          }
          const oauthClient =
            body.oauthClientId === undefined
              ? (current.value.oauthClient ?? null)
              : payload.oauthClientId === null && payload.oauthScope === null
                ? null
                : {
                    ...(payload.oauthClientId !== null
                      ? { clientId: payload.oauthClientId }
                      : {}),
                    ...(payload.oauthClientSecret !== null &&
                    payload.oauthClientSecret !== "•••"
                      ? { clientSecret: payload.oauthClientSecret }
                      : current.value.oauthClient?.clientId ===
                            payload.oauthClientId &&
                          current.value.oauthClient?.clientSecret !== undefined
                        ? {
                            clientSecret:
                              current.value.oauthClient.clientSecret,
                          }
                        : {}),
                    ...(payload.oauthScope !== null
                      ? { scope: payload.oauthScope }
                      : body.oauthScope === undefined &&
                          current.value.oauthClient?.clientId ===
                            payload.oauthClientId &&
                          current.value.oauthClient?.scope !== undefined
                        ? { scope: current.value.oauthClient.scope }
                        : {}),
                    ...(payload.oauthTokenEndpointAuthMethod !== null
                      ? {
                          tokenEndpointAuthMethod:
                            payload.oauthTokenEndpointAuthMethod,
                        }
                      : body.oauthTokenEndpointAuthMethod === undefined &&
                          current.value.oauthClient?.clientId ===
                            payload.oauthClientId &&
                          current.value.oauthClient?.tokenEndpointAuthMethod !==
                            undefined
                        ? {
                            tokenEndpointAuthMethod:
                              current.value.oauthClient.tokenEndpointAuthMethod,
                          }
                        : {}),
                  };
          const previousOAuthClient = current.value.oauthClient ?? null;
          const oauthClientChanged =
            previousOAuthClient?.clientId !== oauthClient?.clientId ||
            previousOAuthClient?.clientSecret !== oauthClient?.clientSecret ||
            previousOAuthClient?.scope !== oauthClient?.scope ||
            previousOAuthClient?.tokenEndpointAuthMethod !==
              oauthClient?.tokenEndpointAuthMethod;
          const previousUrl = Option.getOrNull(current.value.url);
          const oauthCredentialInvalidated =
            oauthClientChanged || previousUrl !== payload.url;
          if (oauthCredentialInvalidated) {
            yield* deps.mcpOAuth.disconnect(session.organizationId, id);
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
              headers,
              oauthClient,
              enabled: payload.enabled,
              repositoryId,
              now,
            },
          );

          return Option.some(json({ mcpServer: toApiMcpServer(server) }));
        }
        if (request.method === "DELETE") {
          const session = yield* requireMutation(request);
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
        const session = yield* requireSession(request);
        if (
          !deps.config.gatewayAdminOrganizationIds?.has(session.organizationId)
        )
          return Option.some(text("Forbidden", 403));
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
        const session = yield* requireMutation(request);
        if (
          !deps.config.gatewayAdminOrganizationIds?.has(session.organizationId)
        )
          return Option.some(text("Forbidden", 403));
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
        if (Result.isFailure(payloadEither)) {
          return Option.some(text(payloadEither.failure, 400));
        }
        const payload = payloadEither.success;
        const repositoryId = yield* Schema.decodeUnknownEffect(WorkspaceId)(
          payload.id,
        ).pipe(
          Effect.catchTags({
            SchemaError: () =>
              Effect.fail(
                new AdminError({
                  message: "Invalid repository id",
                  status: 400,
                }),
              ),
          }),
        );
        const teamIds = yield* Schema.decodeUnknownEffect(Schema.Array(TeamId))(
          payload.teamIds,
        ).pipe(
          Effect.catchTags({
            SchemaError: () =>
              Effect.fail(
                new AdminError({ message: "Invalid team ids", status: 400 }),
              ),
          }),
        );
        const projectIds = yield* Schema.decodeUnknownEffect(
          Schema.Array(ProjectId),
        )(payload.projectIds).pipe(
          Effect.catchTags({
            SchemaError: () =>
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
          defaultDriver: payload.defaultDriver ?? "omp",
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
        const id = yield* Schema.decodeUnknownEffect(WorkspaceId)(rawId).pipe(
          Effect.catchTags({
            SchemaError: () =>
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
          if (Result.isFailure(payloadEither)) {
            return Option.some(text(payloadEither.failure, 400));
          }
          const payload = payloadEither.success;
          if (payload.id !== id) {
            return Option.some(
              text("Repository id in body does not match path", 400),
            );
          }
          const teamIds = yield* Schema.decodeUnknownEffect(
            Schema.Array(TeamId),
          )(payload.teamIds).pipe(
            Effect.catchTags({
              SchemaError: () =>
                Effect.fail(
                  new AdminError({ message: "Invalid team ids", status: 400 }),
                ),
            }),
          );
          const projectIds = yield* Schema.decodeUnknownEffect(
            Schema.Array(ProjectId),
          )(payload.projectIds).pipe(
            Effect.catchTags({
              SchemaError: () =>
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
              ...(payload.defaultDriver === undefined
                ? {}
                : { defaultDriver: payload.defaultDriver }),
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
        if (Result.isFailure(issueLabels)) {
          return Option.some(text(issueLabels.failure, 400));
        }
        const projectLabels = optionalStringArray(
          body.projectLabels,
          "projectLabels",
        );
        if (Result.isFailure(projectLabels)) {
          return Option.some(text(projectLabels.failure, 400));
        }
        const resolution = yield* deps.workspace.resolve({
          organizationId: session.organizationId,
          teamId: optionalString(body.teamId),
          projectId: optionalString(body.projectId),
          repositoryId: optionalString(body.repositoryId),
          issueLabels: issueLabels.success,
          projectLabels: projectLabels.success,
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
        Effect.catchCause((cause) => Effect.succeed(mapCauseToResponse(cause))),
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

export class Admin extends Context.Service<Admin>()("Admin", {
  make: Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const adminSessionRepo = yield* AdminSessionRepo;
    const executorInstanceRepo = yield* ExecutorInstanceRepo;
    const executor = yield* Executor;
    const installationRepo = yield* InstallationRepo;
    const runRepo = yield* RunRepo;
    const runEventRepo = yield* RunEventRepo;
    const workspaceRepo = yield* WorkspaceRepo;
    const mcpServerRepo = yield* McpServerRepo;
    const mcpOAuth = yield* McpOAuth;
    const promptTemplateRepo = yield* PromptTemplateRepo;
    const workspace = yield* Workspace;
    const reconciler = yield* Reconciler;
    const nixEnvironment = yield* NixEnvironment;
    const runInputRepo = yield* RunInputRepo;
    const linearGateway = yield* LinearGateway;
    const handle = createAdminHandle({
      config,
      adminSessionRepo,
      executorInstanceRepo,
      executor,
      installationRepo,
      runRepo,
      runEventRepo,
      runInputRepo,
      linearGateway,
      workspaceRepo,
      promptTemplateRepo,
      mcpServerRepo,
      workspace,
      mcpOAuth,
      reconciler,
      nixEnvironment,
    });
    return { handle };
  }),
}) {
  static readonly layerWithoutDependencies = Layer.effect(this, this.make);
  static readonly layer = this.layerWithoutDependencies.pipe(
    Layer.provide([
      GatewayConfig.layer,
      AdminSessionRepo.layer,
      InstallationRepo.layer,
      ExecutorInstanceRepo.layer,
      Executor.layer,
      RunRepo.layer,
      RunEventRepo.layer,
      RunInputRepo.layer,
      LinearGateway.layer,
      WorkspaceRepo.layer,
      PromptTemplateRepo.layer,
      McpServerRepo.layer,
      McpOAuth.layer,
      Workspace.layer,
      Reconciler.layer,
      NixEnvironment.layer,
    ]),
  );
}
