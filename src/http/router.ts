import { BunHttpServerRequest } from "@effect/platform-bun";
import { Clock, Effect, Option } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { OAuthStateError } from "../domain/errors.js";
import {
  Admin,
  createAdminSession,
  setAdminCookie,
  tokenHash,
} from "../services/admin.js";
import { GatewayConfig, organizationAllowed } from "../services/config.js";
import { McpOAuth } from "../services/mcp-oauth.js";
import { OAuth } from "../services/oauth.js";
import { Reconciler } from "../services/reconciler.js";
import { AdminSessionRepo } from "../services/store/repositories.js";
import { WebhookPipeline } from "../services/webhook.js";

const SECURITY_HEADERS: Record<string, string> = {
  "X-Content-Type-Options": "nosniff",
  "X-Frame-Options": "DENY",
  "Referrer-Policy": "strict-origin-when-cross-origin",
  "Cache-Control": "no-store",
};

export const health = Effect.gen(function* () {
  const status = yield* Reconciler.use((service) => service.status());
  const now = yield* Clock.currentTimeMillis;
  const config = yield* Effect.serviceOption(GatewayConfig);
  const maxAgeMs = Math.max(
    30_000,
    Option.isSome(config) ? config.value.reconcilerIntervalMs * 3 : 30_000,
  );
  const lastAliveAt = Math.max(
    Option.getOrElse(status.lastProgressAt ?? Option.none(), () => 0),
    Option.getOrElse(status.lastCompletedAt, () => 0),
  );
  const degraded =
    !status.running ||
    (Option.isNone(status.lastCompletedAt) &&
      Option.isNone(status.lastProgressAt ?? Option.none())) ||
    now - lastAliveAt > maxAgeMs;
  const body = {
    status: degraded ? "degraded" : "ok",
    reconciler: {
      running: status.running,
      lastStartedAt: Option.match(status.lastStartedAt, {
        onNone: () => null,
        onSome: (value) => value,
      }),
      lastCompletedAt: Option.match(status.lastCompletedAt, {
        onNone: () => null,
        onSome: (value) => value,
      }),
    },
  };
  return yield* HttpServerResponse.json(body, { status: degraded ? 503 : 200 });
});

export const webhook = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const response = yield* WebhookPipeline.use((service) =>
    service.handle(BunHttpServerRequest.toBunServerRequest(request)),
  );
  if (response.ok) {
    yield* Reconciler.use((service) => service.trigger());
  }
  return HttpServerResponse.fromWeb(response);
});

export const oauthStart = Effect.gen(function* () {
  yield* Effect.logInfo("oauth.started").pipe(
    Effect.annotateLogs({
      event: "oauth.started",
      path: "/oauth/start",
    }),
  );
  const request = yield* HttpServerRequest.HttpServerRequest;
  const returnTo = safeReturnTo(
    new URL(request.url, "http://localhost").searchParams.get("returnTo"),
  );
  const config = yield* GatewayConfig;
  const { url } = yield* OAuth.use((service) => service.startAuthorization());
  return HttpServerResponse.redirect(url, {
    status: 302,
    headers: {
      ...SECURITY_HEADERS,
      "set-cookie": `omp_gateway_return=${encodeURIComponent(returnTo)}; HttpOnly; SameSite=Lax; Path=/oauth; Max-Age=600${config.publicUrl.protocol === "https:" ? "; Secure" : ""}`,
    },
  });
});

export const oauthCallback = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const callback = Effect.gen(function* () {
    const installation = yield* OAuth.use((service) =>
      service.completeAuthorization(new URL(request.url, "http://localhost")),
    );
    yield* Effect.logInfo("oauth.completed").pipe(
      Effect.annotateLogs({
        event: "oauth.completed",
        organizationId: installation.organizationId,
      }),
    );
    const config = yield* GatewayConfig;
    const adminSessionRepo = yield* AdminSessionRepo;
    const now = yield* Clock.currentTimeMillis;
    const { token, expiresAt } = yield* createAdminSession(
      { config, adminSessionRepo },
      installation.organizationId,
      now,
    );
    const returnCookie = BunHttpServerRequest.toBunServerRequest(request)
      .headers.get("cookie")
      ?.match(/(?:^|;\s*)omp_gateway_return=([^;]*)/u)?.[1];
    const returnTo = safeReturnTo(
      returnCookie === undefined ? null : safeDecode(returnCookie),
    );
    return HttpServerResponse.redirect(returnTo, {
      status: 302,
      headers: {
        ...SECURITY_HEADERS,
        "set-cookie": setAdminCookie(config, token, expiresAt),
      },
    });
  });
  return yield* callback.pipe(
    Effect.tapError((error) =>
      Effect.logError("oauth.failed").pipe(
        Effect.annotateLogs({
          event: "oauth.failed",
          error: String(error),
        }),
      ),
    ),
  );
});
export const mcpOauthCallback = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const callback = new URL(request.url, "http://localhost");
  const config = yield* GatewayConfig;
  const adminSessionRepo = yield* AdminSessionRepo;
  const adminUrl = new URL(config.publicUrl.toString());
  adminUrl.pathname = `${adminUrl.pathname.replace(/\/$/u, "")}/admin`;
  adminUrl.search = "";
  const cookie =
    BunHttpServerRequest.toBunServerRequest(request).headers.get("cookie");
  const rawToken = cookie?.match(/(?:^|;\s*)omp_gateway_admin=([^;]*)/u)?.[1];
  if (rawToken === undefined) {
    return yield* Effect.fail(
      new OAuthStateError({ message: "Unauthorized MCP OAuth callback" }),
    );
  }
  const adminSessionHash = tokenHash(decodeURIComponent(rawToken));
  const now = yield* Clock.currentTimeMillis;
  const session = yield* adminSessionRepo.get(adminSessionHash, now);
  if (Option.isNone(session)) {
    return yield* Effect.fail(
      new OAuthStateError({ message: "Unauthorized MCP OAuth callback" }),
    );
  }
  return yield* McpOAuth.use((service) =>
    service.completeMcpAuthorization(callback, adminSessionHash),
  ).pipe(
    Effect.as(
      HttpServerResponse.redirect(`${adminUrl.pathname}?mcp=connected`, {
        status: 302,
        headers: SECURITY_HEADERS,
      }),
    ),
    Effect.catch((error) =>
      Effect.logError("mcp.oauth_callback_failed").pipe(
        Effect.annotateLogs({ error: String(error) }),
        Effect.as(
          HttpServerResponse.redirect(
            `${adminUrl.pathname}?mcp=error&message=OAuth%20connection%20failed`,
            { status: 302, headers: SECURITY_HEADERS },
          ),
        ),
      ),
    ),
  );
});

const admin = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const response = yield* Admin.use((service) =>
    service.handle(BunHttpServerRequest.toBunServerRequest(request)),
  );
  return response._tag === "Some"
    ? HttpServerResponse.fromWeb(response.value)
    : HttpServerResponse.text("Not found", { status: 404 });
});

export const safeDecode = (value: string): string => {
  try {
    return decodeURIComponent(value);
  } catch {
    return "";
  }
};
export const safeReturnTo = (value: string | null): string =>
  value !== null &&
  /^\/(?!\/)/u.test(value) &&
  !value.includes("\\") &&
  [...value].every(
    (character) =>
      character.charCodeAt(0) > 32 && character.charCodeAt(0) !== 127,
  )
    ? value
    : "/admin";
export const publicRoute = (method: string, path: string): boolean =>
  (method === "GET" &&
    ["/", "/health", "/oauth/start", "/oauth/callback"].includes(path)) ||
  (method === "POST" && path === "/webhooks/linear");

export const authenticateRoutes = <E, R>(
  app: Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>,
) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const path = new URL(request.url, "http://localhost").pathname;
    // v4 also matches the parent of a wildcard route. v3 registered only
    // GET /admin, and never registered the bare /api/admin path.
    const parentPath = path.replace(/\/$/u, "");
    if (
      parentPath === "/api/admin" ||
      (parentPath === "/admin" &&
        request.method !== "GET" &&
        request.method !== "HEAD")
    ) {
      return HttpServerResponse.empty({ status: 404 });
    }
    if (publicRoute(request.method, path)) return yield* app;
    const raw = BunHttpServerRequest.toBunServerRequest(request)
      .headers.get("cookie")
      ?.match(/(?:^|;\s*)omp_gateway_admin=([^;]*)/u)?.[1];
    const repo = yield* AdminSessionRepo;
    const config = yield* GatewayConfig;
    const now = yield* Clock.currentTimeMillis;
    const session =
      raw === undefined
        ? Option.none()
        : yield* repo.get(tokenHash(safeDecode(raw)), now);
    if (
      Option.isNone(session) ||
      !organizationAllowed(config, session.value.organizationId)
    ) {
      return request.method === "GET" &&
        !path.endsWith(".json") &&
        !path.startsWith("/api/") &&
        !request.headers.accept?.includes("application/json")
        ? HttpServerResponse.redirect(
            `/oauth/start?returnTo=${encodeURIComponent(new URL(request.url, "http://localhost").pathname + new URL(request.url, "http://localhost").search)}`,
            { status: 302, headers: SECURITY_HEADERS },
          )
        : HttpServerResponse.text("Unauthorized", {
            status: 401,
            headers: SECURITY_HEADERS,
          });
    }
    return yield* app;
  });

export const routes = [
  HttpRouter.route("GET", "/health", authenticateRoutes(health)),
  HttpRouter.route("*", "/webhooks/linear", authenticateRoutes(webhook)),
  HttpRouter.route("GET", "/oauth/start", authenticateRoutes(oauthStart)),
  HttpRouter.route("GET", "/oauth/callback", authenticateRoutes(oauthCallback)),
  HttpRouter.route(
    "GET",
    "/oauth/mcp/callback",
    authenticateRoutes(mcpOauthCallback),
  ),
  HttpRouter.route("GET", "/", authenticateRoutes(admin)),
  HttpRouter.route("GET", "/admin", authenticateRoutes(admin)),
  HttpRouter.route("GET", "/runs/:id", authenticateRoutes(admin)),
  HttpRouter.route("GET", "/runs/:id.json", authenticateRoutes(admin)),
  HttpRouter.route("*", "/admin/*", authenticateRoutes(admin)),
  HttpRouter.route("*", "/api/admin/*", authenticateRoutes(admin)),
] as const;

// v4 expands /admin/* to /admin too. Both use the admin handler; retain
// the original inventory above and enforce its method boundary in middleware.
export const RouterLive = HttpRouter.addAll(
  routes.filter((route) => route.path !== "/admin"),
);
export const httpApp = HttpRouter.toHttpEffect(RouterLive);
