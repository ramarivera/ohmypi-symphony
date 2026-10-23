import { BunHttpServer } from "@effect/platform-bun";
import { Effect, Layer, Schedule } from "effect";
import { HttpRouter } from "effect/http";
import { RouterLive } from "./http/router.js";
import { Admin } from "./services/admin.js";
import { GatewayConfig } from "./services/config.js";
import { Executor } from "./services/executor.js";
import { GitHubApp } from "./services/github-app.js";
import { LinearGateway } from "./services/linear-gateway.js";
import { GatewayLogger, PinoLoggerLive } from "./services/logger.js";
import { McpOAuth } from "./services/mcp-oauth.js";
import { NixEnvironment } from "./services/nix-environment.js";
import { OAuth } from "./services/oauth.js";
import { ActivityProjector } from "./services/projector.js";
import { Reconciler } from "./services/reconciler.js";
import { Retention } from "./services/retention.js";
import { RpcWorker } from "./services/rpc-worker.js";
import { SessionAuthority } from "./services/session-authority.js";
import {
  AdminSessionRepo,
  DeliveryRepo,
  ExecutorInstanceRepo,
  InstallationRepo,
  McpServerRepo,
  ProjectionRepo,
  PromptTemplateRepo,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
  WorkspaceRepo,
} from "./services/store/repositories.js";
import { SqliteClientLive } from "./services/store/sqlite-client.js";
import { TokenCrypto } from "./services/token-crypto.js";
import { WebhookPipeline } from "./services/webhook.js";
import { Workspace } from "./services/workspace.js";

const sqliteClientLayer = Layer.unwrap(
  Effect.gen(function* () {
    const { databasePath } = yield* GatewayConfig;
    return SqliteClientLive(databasePath);
  }),
);

export const GatewayServicesLive = Layer.mergeAll(
  TokenCrypto.layer,
  AdminSessionRepo.layer,
  InstallationRepo.layer,
  DeliveryRepo.layer,
  RunRepo.layer,
  RunInputRepo.layer,
  RunEventRepo.layer,
  ProjectionRepo.layer,
  WorkspaceRepo.layer,
  PromptTemplateRepo.layer,
  ExecutorInstanceRepo.layer,
  Executor.layer,
  McpServerRepo.layer,
  LinearGateway.layer,
  WebhookPipeline.layer,
  ActivityProjector.layer,
  SessionAuthority.layer,
  NixEnvironment.layer,
  RpcWorker.layer,
  GitHubApp.layer,
  OAuth.layer,
  McpOAuth.layer,
  Reconciler.layer,
  Retention.layer,
  Workspace.layer,
  Admin.layer,
).pipe(
  Layer.provideMerge(sqliteClientLayer),
  Layer.provideMerge(GatewayConfig.layer),
);

// Recovery must finish before accepting deliveries or starting reconciliation.
export const recoverGateway = Effect.gen(function* () {
  yield* RunRepo.use((service) => service.recoverInterruptedRuns());
  yield* DeliveryRepo.use((service) => service.recoverPendingDeliveries());
});

const recoveredServices = Layer.provideMerge(
  Layer.effectDiscard(recoverGateway),
  GatewayServicesLive,
);

const gatewayLoggerLive = PinoLoggerLive.pipe(
  Layer.provideMerge(GatewayLogger.layer),
  Layer.provideMerge(GatewayConfig.layer),
);

const scheduledReconciler = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const periodic = Effect.repeat(
      Reconciler.use((service) => service.tick()),
      Schedule.spaced(config.reconcilerIntervalMs),
    );
    const triggered = Effect.forever(
      Reconciler.use((service) => service.awaitTrigger()).pipe(
        Effect.andThen(Reconciler.use((service) => service.tick())),
      ),
    );
    return Layer.effectDiscard(
      Effect.all([periodic, triggered], {
        concurrency: "unbounded",
        discard: true,
      }).pipe(Effect.forkScoped),
    );
  }),
);

const serverLayer = Layer.unwrap(
  Effect.map(GatewayConfig, ({ port }) => BunHttpServer.layer({ port })),
);

const servicesWithServer = Layer.provideMerge(serverLayer, recoveredServices);
const servicesWithHttp = Layer.provideMerge(
  HttpRouter.serve(RouterLive, { disableLogger: true, disableListenLog: true }),
  servicesWithServer,
);
const servicesWithScheduler = Layer.provideMerge(
  Layer.merge(
    scheduledReconciler,
    Layer.effectDiscard(
      Effect.flatMap(Retention, (retention) =>
        Effect.forkScoped(retention.run),
      ),
    ),
  ),
  servicesWithHttp,
);

export const GatewayLive = servicesWithScheduler;

export const main = Layer.launch(GatewayLive).pipe(
  Effect.provide(gatewayLoggerLive),
);
