// Real process termination deliberately skips Effect finalizers. SQLite must
// recover the durable claim and roll back an in-flight processing transaction.
import { createHmac } from "node:crypto";
import { ConfigProvider, Effect, Layer, Logger } from "effect";
import { GatewayServicesLive } from "../../src/conductor.js";
import {
  DeliveryRepo,
  InstallationRepo,
  RunInputRepo,
  RunRepo,
} from "../../src/services/store/repositories.js";
import { WebhookPipeline } from "../../src/services/webhook.js";

const [databasePath, checkpoint, body] = process.argv.slice(2);
if (
  databasePath === undefined ||
  checkpoint === undefined ||
  body === undefined
) {
  throw new Error("Missing crash fixture arguments");
}
const crash = () => process.exit(86);
const program = Effect.gen(function* () {
  const deliveries = yield* DeliveryRepo;
  const runs = yield* RunRepo;
  const inputs = yield* RunInputRepo;
  const installations = yield* InstallationRepo;
  if (checkpoint === "claim") {
    const claim = deliveries.claim;
    deliveries.claim = (input) =>
      claim(input).pipe(Effect.tap(() => Effect.sync(crash)));
  }
  if (checkpoint === "run") {
    const create = runs.create;
    runs.create = (input) =>
      create(input).pipe(Effect.tap(() => Effect.sync(crash)));
  }
  if (checkpoint === "enqueue") {
    const enqueue = inputs.enqueue;
    inputs.enqueue = (input) =>
      enqueue(input).pipe(Effect.tap(() => Effect.sync(crash)));
  }
  if (checkpoint === "installation") {
    const apply = installations.applyPermissionChange;
    installations.applyPermissionChange = (...args) =>
      apply(...args).pipe(Effect.tap(() => Effect.sync(crash)));
  }
  if (checkpoint === "processed") {
    const processClaim = deliveries.processClaim;
    deliveries.processClaim = (id, token, effect) =>
      processClaim(id, token, effect.pipe(Effect.andThen(Effect.sync(crash))));
  }
  const pipeline = yield* WebhookPipeline;
  const response = yield* pipeline.handle(
    new Request("http://localhost/webhooks/linear", {
      method: "POST",
      headers: {
        "linear-signature": createHmac("sha256", "webhook-secret")
          .update(body)
          .digest("hex"),
        "linear-delivery": "crash-delivery",
        "linear-timestamp": String(Date.now()),
      },
      body,
    }),
  );
  throw new Error(`Crash checkpoint wasn't reached, HTTP ${response.status}`);
});
await Effect.runPromise(
  program.pipe(
    Effect.provide([
      Logger.layer([]),
      GatewayServicesLive.pipe(
        Layer.provide(
          ConfigProvider.layer(
            ConfigProvider.fromUnknown({
              LINEAR_CLIENT_ID: "client",
              LINEAR_CLIENT_SECRET: "client-secret",
              LINEAR_WEBHOOK_SECRET: "webhook-secret",
              TOKEN_ENCRYPTION_KEY:
                "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=",
              PUBLIC_URL: "http://localhost:3000",
              DATABASE_PATH: databasePath,
              LOG_LEVEL: "silent",
            }),
          ),
        ),
      ),
    ]),
  ),
);
