import { HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Logger, Option, Redacted, Schema } from "effect";
import { OAuthStateError } from "../src/domain/errors.js";
import { AppUserId, OrganizationId, type TeamId } from "../src/domain/ids.js";
import {
  oauthCallback,
  oauthStart,
  router,
  webhook,
} from "../src/http/router.js";
import { GatewayConfig } from "../src/services/config.js";
import { OAuth } from "../src/services/oauth.js";
import { Reconciler } from "../src/services/reconciler.js";
import { AdminSessionRepo } from "../src/services/store/repositories.js";
import { WebhookPipeline } from "../src/services/webhook.js";

const routes = [...router.routes];

function route(method: string, path: string) {
  return routes.find(
    (candidate) => candidate.method === method && candidate.path === path,
  );
}

function request(
  method: string,
  path: string,
): HttpServerRequest.HttpServerRequest {
  return HttpServerRequest.fromWeb(
    new Request(`https://gateway.example${path}`, { method }),
  );
}

describe("HTTP router parity", () => {
  it("registers the legacy OAuth start path and all webhook methods", () => {
    expect(route("GET", "/oauth/start")).toBeDefined();
    expect(route("GET", "/oauth/linear")).toBeUndefined();
    expect(route("*", "/webhooks/linear")).toBeDefined();
  });

  it("delegates non-POST webhook methods so the pipeline owns 405 behavior", async () => {
    const methods: string[] = [];
    const pipeline: WebhookPipeline = {
      _tag: "WebhookPipeline",
      handle: (incoming: Request) =>
        Effect.sync(() => {
          methods.push(incoming.method);
          return new Response("Method not allowed", { status: 405 });
        }),
    };
    const reconciler: Reconciler = {
      _tag: "Reconciler",
      tick: () => Effect.succeed(undefined),
      trigger: () => Effect.void,
      awaitTrigger: () => Effect.never,
      status: () =>
        Effect.succeed({
          running: false,
          lastStartedAt: null,
          lastCompletedAt: null,
          lastError: null,
        }),
    };
    const response = await Effect.runPromise(
      webhook.pipe(
        Effect.provideService(
          HttpServerRequest.HttpServerRequest,
          request("PUT", "/webhooks/linear"),
        ),
        Effect.provideService(WebhookPipeline, pipeline),
        Effect.provideService(Reconciler, reconciler),
      ),
    );

    expect(response.status).toBe(405);
    expect(methods).toEqual(["PUT"]);
  });

  it("enqueues reconciliation without delaying a successful webhook response", async () => {
    let triggerCalls = 0;
    const pipeline: WebhookPipeline = {
      _tag: "WebhookPipeline",
      handle: (_incoming: Request) =>
        Effect.succeed(new Response(null, { status: 204 })),
    };
    const reconciler: Reconciler = {
      _tag: "Reconciler",
      tick: () => Effect.void,
      trigger: () =>
        Effect.sync(() => {
          triggerCalls += 1;
        }),
      awaitTrigger: () => Effect.never,
      status: () =>
        Effect.succeed({
          running: false,
          lastStartedAt: null,
          lastCompletedAt: null,
          lastError: null,
        }),
    };
    const response = await Effect.runPromise(
      webhook.pipe(
        Effect.provideService(
          HttpServerRequest.HttpServerRequest,
          request("POST", "/webhooks/linear"),
        ),
        Effect.provideService(WebhookPipeline, pipeline),
        Effect.provideService(Reconciler, reconciler),
      ),
    );

    expect(response.status).toBe(204);
    expect(triggerCalls).toBe(1);
  });

  it.effect.prop(
    "triggers reconciliation exactly for generated successful webhook statuses",
    { status: Schema.Number.pipe(Schema.int(), Schema.between(200, 599)) },
    ({ status }) => {
      let triggerCalls = 0;
      const pipeline: WebhookPipeline = {
        _tag: "WebhookPipeline",
        handle: () => Effect.succeed(new Response(null, { status })),
      };
      const reconciler: Reconciler = {
        _tag: "Reconciler",
        tick: () => Effect.void,
        trigger: () =>
          Effect.sync(() => {
            triggerCalls += 1;
          }),
        awaitTrigger: () => Effect.never,
        status: () =>
          Effect.succeed({
            running: false,
            lastStartedAt: null,
            lastCompletedAt: null,
            lastError: null,
          }),
      };
      return Effect.gen(function* () {
        const response = yield* webhook;
        expect(response.status).toBe(status);
        expect(triggerCalls).toBe(status >= 200 && status < 300 ? 1 : 0);
      }).pipe(
        Effect.provideService(
          HttpServerRequest.HttpServerRequest,
          request("POST", "/webhooks/linear"),
        ),
        Effect.provideService(WebhookPipeline, pipeline),
        Effect.provideService(Reconciler, reconciler),
      );
    },
    { fastCheck: { numRuns: 20 } },
  );

  it("adds legacy security headers and structured lifecycle logging to OAuth start", async () => {
    const logs: string[] = [];
    const logger = Logger.make(({ message }) => {
      logs.push(
        Array.isArray(message)
          ? message.map(String).join(" ")
          : String(message),
      );
    });
    const oauth: OAuth = {
      _tag: "OAuth",
      startAuthorization: () =>
        Effect.succeed({
          state: "test-state",
          url: new URL("https://linear.example/authorize"),
        }),
      completeAuthorization: (_url: URL) =>
        Effect.fail(new OAuthStateError({ message: "unused" })),
    };
    const response = await Effect.runPromise(
      oauthStart.pipe(
        Effect.provideService(OAuth, oauth),
        Effect.provide(Logger.replace(Logger.defaultLogger, logger)),
      ),
    );

    const webResponse = HttpServerResponse.toWeb(response);

    expect(webResponse.status).toBe(302);
    expect(webResponse.headers.get("location")).toBe(
      "https://linear.example/authorize",
    );
    expect(webResponse.headers.get("x-content-type-options")).toBe("nosniff");
    expect(webResponse.headers.get("x-frame-options")).toBe("DENY");
    expect(webResponse.headers.get("referrer-policy")).toBe(
      "strict-origin-when-cross-origin",
    );
    expect(webResponse.headers.get("cache-control")).toBe("no-store");
    expect(logs.some((message) => message.startsWith("oauth.started"))).toBe(
      true,
    );
  });

  it("adds security headers and logs OAuth callback completion", async () => {
    const logs: string[] = [];
    const logger = Logger.make(({ message }) => {
      logs.push(
        Array.isArray(message)
          ? message.map(String).join(" ")
          : String(message),
      );
    });
    const oauth: OAuth = {
      _tag: "OAuth",
      startAuthorization: () =>
        Effect.succeed({
          state: "test-state",
          url: new URL("https://linear.example/authorize"),
        }),
      completeAuthorization: (_url: URL) =>
        Effect.succeed({
          organizationId: Schema.decodeUnknownSync(OrganizationId)(
            "11111111-1111-4111-8111-111111111111",
          ),
          appUserId: Schema.decodeUnknownSync(AppUserId)(
            "22222222-2222-4222-8222-222222222222",
          ),
          accessToken: "access-token",
          refreshToken: "refresh-token",
          expiresAt: 1_000,
          scopes: [],
          revokedAt: Option.none(),
          accessibleTeamIds:
            Option.none<ReadonlyArray<Schema.Schema.Type<typeof TeamId>>>(),
          canAccessAllPublicTeams: Option.none(),
        }),
    };
    const config: GatewayConfig = {
      _tag: "GatewayConfig",
      linearClientId: "client",
      linearClientSecret: Redacted.make("secret"),
      linearWebhookSecret: Redacted.make("webhook"),
      tokenEncryptionKey: Redacted.make("key"),
      publicUrl: new URL("https://gateway.example"),
      logLevel: "silent",
      databasePath: ":memory:",
      workspaceRoot: "/Volumes/ExtSSD/SCRATCHPADS_FOR_AGENTS/router-test",
      ompCliPath: "omp",
      port: 3000,
      leaseDurationMs: 60_000,
      reconcilerIntervalMs: 1_000,
      webhookReplayWindowMs: 60_000,
      allowedOrganizationIds: new Set(["org"]),
      githubApp: undefined,
    };
    const adminSessionRepo: AdminSessionRepo = {
      _tag: "AdminSessionRepo",
      create: (_input) => Effect.void,
      get: (_tokenHash, _now) => Effect.succeed(Option.none()),
      deleteAdminSession: (_tokenHash) => Effect.succeed(false),
    };
    const response = await Effect.runPromise(
      oauthCallback.pipe(
        Effect.provideService(
          HttpServerRequest.HttpServerRequest,
          request("GET", "/oauth/callback?code=code&state=state"),
        ),
        Effect.provideService(OAuth, oauth),
        Effect.provideService(GatewayConfig, config),
        Effect.provideService(AdminSessionRepo, adminSessionRepo),
        Effect.provide(Logger.replace(Logger.defaultLogger, logger)),
      ),
    );
    const webResponse = HttpServerResponse.toWeb(response);

    expect(webResponse.status).toBe(302);
    expect(webResponse.headers.get("location")).toBe("/admin");
    expect(webResponse.headers.get("x-content-type-options")).toBe("nosniff");
    expect(webResponse.headers.get("x-frame-options")).toBe("DENY");
    expect(webResponse.headers.get("referrer-policy")).toBe(
      "strict-origin-when-cross-origin",
    );
    expect(webResponse.headers.get("cache-control")).toBe("no-store");
    expect(webResponse.headers.get("set-cookie")).toContain(
      "omp_gateway_admin=",
    );
    expect(logs.some((message) => message.startsWith("oauth.completed"))).toBe(
      true,
    );
  });
  it("logs OAuth callback failures without erasing the failure", async () => {
    const logs: string[] = [];
    const logger = Logger.make(({ message }) => {
      logs.push(
        Array.isArray(message)
          ? message.map(String).join(" ")
          : String(message),
      );
    });
    const oauth: OAuth = {
      _tag: "OAuth",
      startAuthorization: () =>
        Effect.succeed({
          state: "test-state",
          url: new URL("https://linear.example/authorize"),
        }),
      completeAuthorization: (_url: URL) =>
        Effect.fail(new OAuthStateError({ message: "invalid callback" })),
    };
    const result = await Effect.runPromiseExit(
      oauthCallback.pipe(
        Effect.provideService(
          HttpServerRequest.HttpServerRequest,
          request("GET", "/oauth/callback"),
        ),
        Effect.provideService(OAuth, oauth),
        Effect.provideService(GatewayConfig, {
          _tag: "GatewayConfig",
          linearClientId: "client",
          linearClientSecret: Redacted.make("secret"),
          linearWebhookSecret: Redacted.make("webhook"),
          tokenEncryptionKey: Redacted.make("key"),
          githubApp: undefined,
          allowedOrganizationIds: new Set(["org"]),
          publicUrl: new URL("https://gateway.example"),
          logLevel: "silent",
          databasePath: ":memory:",
          workspaceRoot: "/Volumes/ExtSSD/SCRATCHPADS_FOR_AGENTS/router-test",
          ompCliPath: "omp",
          port: 3000,
          leaseDurationMs: 60_000,
          reconcilerIntervalMs: 1_000,
          webhookReplayWindowMs: 60_000,
        }),
        Effect.provideService(AdminSessionRepo, {
          _tag: "AdminSessionRepo",
          create: (_input) => Effect.void,
          get: (_tokenHash, _now) => Effect.succeed(Option.none()),
          deleteAdminSession: (_tokenHash) => Effect.succeed(false),
        }),
        Effect.provide(Logger.replace(Logger.defaultLogger, logger)),
      ),
    );

    expect(Exit.isFailure(result)).toBe(true);
    expect(logs.some((message) => message.startsWith("oauth.failed"))).toBe(
      true,
    );
  });
});
