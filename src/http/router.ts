import { HttpRouter, HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { BunHttpServerRequest } from "@effect/platform-bun";
import { Effect } from "effect";
import { Admin, OAuth, Reconciler, WebhookPipeline } from "../services/contracts.js";

const health = Effect.gen(function* () {
  yield* Reconciler.tick();
  return yield* HttpServerResponse.json({ status: "ok" });
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
  return yield* HttpServerResponse.json({ organizationId: installation.organizationId });
});

const admin = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const response = yield* Admin.handle(BunHttpServerRequest.toRequest(request));
  return response._tag === "Some" ? HttpServerResponse.fromWeb(response.value) : HttpServerResponse.text("Not found", { status: 404 });
});

export const router = HttpRouter.empty.pipe(
  HttpRouter.get("/health", health),
  HttpRouter.post("/webhooks/linear", webhook),
  HttpRouter.get("/oauth/linear", oauthStart),
  HttpRouter.get("/oauth/callback", oauthCallback),
  HttpRouter.get("/runs/:id", admin),
  HttpRouter.get("/runs/:id.json", admin),
  HttpRouter.all("/admin/*", admin),
);

export const httpApp = HttpRouter.toHttpApp(router);
