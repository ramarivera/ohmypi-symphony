import { describe, expect, it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Schema } from "effect";
import { HttpRouter } from "effect/http";
import { GatewayServicesLive } from "../src/conductor.js";
import { SessionId } from "../src/domain/ids.js";
import * as Models from "../src/domain/models.js";
import { RouterLive } from "../src/http/router.js";
import { Reconciler } from "../src/services/reconciler.js";
import { v3HttpFixtures } from "./fixtures/effect-v3-http.js";
import { v3ModelFixtures } from "./fixtures/effect-v3-models.js";

describe("v3 wire compatibility", () => {
  it("covers every exported model schema", () => {
    const names = Object.entries(Models)
      .filter(([, model]) => Schema.isSchema(model))
      .map(([name]) => name)
      .sort();
    expect(v3ModelFixtures.map(({ name }) => name).sort()).toEqual(names);
  });

  for (const { name, input, expected } of v3ModelFixtures) {
    it(`preserves ${name} decode and encode`, () => {
      const schemas = { ...Models };
      const schema = schemas[name];
      const decoded = Schema.decodeUnknownSync(schema)(input);
      const encoded = Schema.encodeUnknownSync(schema)(decoded);
      expect(encoded).toEqual(expected);
      expect(Schema.decodeUnknownSync(schema)(encoded)).toEqual(decoded);
    });
  }

  it("retains permissive GUID validation for non-RFC version bits", () => {
    const value = "12345678-1234-f234-0234-123456789abc";
    expect(Schema.decodeUnknownSync(SessionId)(value)).toBe(value);
  });

  it.effect("keeps decoding defaults on missing and undefined properties", () =>
    Effect.gen(function* () {
      const input = { id: "activity", agentSessionId: "session", content: {} };
      for (const value of [input, { ...input, signal: undefined }]) {
        const decoded = yield* Schema.decodeUnknownEffect(
          Models.AgentSessionActivity,
        )(value);
        expect(Schema.encodeSync(Models.AgentSessionActivity)(decoded)).toEqual(
          { ...input, signal: null },
        );
      }
    }),
  );
});

const httpServices = GatewayServicesLive.pipe(
  Layer.provide(
    ConfigProvider.layer(
      ConfigProvider.fromUnknown({
        LINEAR_CLIENT_ID: "dummy-client",
        LINEAR_CLIENT_SECRET: "dummy-secret",
        LINEAR_WEBHOOK_SECRET: "dummy-webhook",
        TOKEN_ENCRYPTION_KEY: Buffer.alloc(32).toString("base64"),
        PUBLIC_URL: "http://localhost:3000",
        DATABASE_PATH: ":memory:",
        LOG_LEVEL: "silent",
      }),
    ),
  ),
);

it.live(
  "preserves the v3 HTTP method, redirect, and security-header matrix",
  () =>
    Effect.gen(function* () {
      const context = yield* Layer.build(httpServices);
      yield* Reconciler.use((service) => service.tick()).pipe(
        Effect.provideContext(context),
      );
      const { handler } = yield* Effect.acquireRelease(
        Effect.sync(() =>
          HttpRouter.toWebHandler(RouterLive, { disableLogger: true }),
        ),
        ({ dispose }) => Effect.promise(dispose),
      );
      for (const fixture of v3HttpFixtures) {
        const response = yield* Effect.promise(() =>
          handler(
            new Request(`http://localhost:3000${fixture.path}`, {
              method: fixture.method,
            }),
            context,
          ),
        );
        expect(response.status, `${fixture.method} ${fixture.path}`).toBe(
          fixture.status,
        );
        if (fixture.location === "<oauth-start>") {
          const target = new URL(response.headers.get("location") ?? "");
          expect(target.origin + target.pathname).toBe(
            "https://linear.app/oauth/authorize",
          );
          expect(target.searchParams.get("state")).toBeTruthy();
        } else {
          expect(response.headers.get("location")).toBe(fixture.location);
        }
        expect(response.headers.get("x-content-type-options")).toBe(
          fixture.secured ? "nosniff" : null,
        );
        expect(response.headers.get("x-frame-options")).toBe(
          fixture.secured ? "DENY" : null,
        );
        expect(response.headers.get("referrer-policy")).toBe(
          fixture.secured ? "strict-origin-when-cross-origin" : null,
        );
        expect(response.headers.get("cache-control")).toBe(
          fixture.secured ? "no-store" : null,
        );
      }
    }),
);
