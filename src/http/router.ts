import { HttpRouter, HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { BunHttpServerRequest } from "@effect/platform-bun";
import { Clock, Effect } from "effect";
import { setAdminCookie, createAdminSession } from "../services/admin.js";
import { GatewayConfig } from "../services/config.js";
import { Admin, AdminSessionRepo, OAuth, Reconciler, WebhookPipeline } from "../services/contracts.js";

const health = Effect.gen(function* () {
  const status = yield* Reconciler.status();
  const degraded = status.lastError !== null;
  const body = {
    status: degraded ? "degraded" : "ok",
    reconciler: {
      running: status.running,
      lastStartedAt: status.lastStartedAt,
      lastCompletedAt: status.lastCompletedAt,
    },
  };
  return yield* HttpServerResponse.json(body, { status: degraded ? 503 : 200 });
});

const webhook = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const response = yield* WebhookPipeline.handle(BunHttpServerRequest.toRequest(request));
  return HttpServerResponse.fromWeb(response);
});

const oauthStart = Effect.gen(function* () {
  const { url } = yield* OAuth.startAuthorization();
  return HttpServerResponse.redirect(url.toString());
});

const oauthCallback = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const installation = yield* OAuth.completeAuthorization(new URL(request.url, "http://localhost"));
  const config = yield* GatewayConfig;
  const adminSessionRepo = yield* AdminSessionRepo;
  const now = yield* Clock.currentTimeMillis;
  const { token, expiresAt } = yield* createAdminSession(
    { config, adminSessionRepo },
    installation.organizationId,
    now,
  );
  return HttpServerResponse.redirect("/admin", {
    status: 302,
    headers: { "set-cookie": setAdminCookie(config, token, expiresAt) },
  });
});

const admin = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const response = yield* Admin.handle(BunHttpServerRequest.toRequest(request));
  return response._tag === "Some"
    ? HttpServerResponse.fromWeb(response.value)
    : HttpServerResponse.text("Not found", { status: 404 });
});

export const router = HttpRouter.empty.pipe(
  HttpRouter.get("/health", health),
  HttpRouter.post("/webhooks/linear", webhook),
  HttpRouter.get("/oauth/linear", oauthStart),
  HttpRouter.get("/oauth/callback", oauthCallback),
  HttpRouter.get("/", admin),
  HttpRouter.get("/admin", admin),
  HttpRouter.get("/runs/:id", admin),
  HttpRouter.get("/runs/:id.json", admin),
  HttpRouter.all("/admin/*", admin),
  HttpRouter.all("/api/admin/*", admin),
);

export const httpApp = HttpRouter.toHttpApp(router);
