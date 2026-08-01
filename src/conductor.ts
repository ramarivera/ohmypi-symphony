import { HttpServer } from "@effect/platform";
import { BunHttpServer } from "@effect/platform-bun";
import { Effect, Layer, Schedule } from "effect";
import { router } from "./http/router.js";
import { Admin } from "./services/admin.js";
import { GatewayConfig } from "./services/config.js";
import { GitHubApp } from "./services/github-app.js";
import { LinearGateway } from "./services/linear-gateway.js";
import { GatewayLogger, PinoLoggerLive } from "./services/logger.js";
import { OAuth } from "./services/oauth.js";
import { ActivityProjector } from "./services/projector.js";
import { Reconciler } from "./services/reconciler.js";
import { RpcWorker } from "./services/rpc-worker.js";
import { SessionAuthority } from "./services/session-authority.js";
import {
  AdminSessionRepo,
  DeliveryRepo,
  InstallationRepo,
  ProjectionRepo,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
  WorkspaceRepo,
} from "./services/store/repositories.js";
import { SqliteClientLive } from "./services/store/sqlite-client.js";
import { TokenCrypto } from "./services/token-crypto.js";
import { WebhookPipeline } from "./services/webhook.js";
import { Workspace } from "./services/workspace.js";

const sqliteClientLayer = Layer.unwrapEffect(
  Effect.gen(function* () {
    const { databasePath } = yield* GatewayConfig;
    return SqliteClientLive(databasePath);
  }),
);

export const GatewayServicesLive = GatewayConfig.Default.pipe(
  Layer.merge(GatewayLogger.Default),
  Layer.merge(TokenCrypto.Default),
  Layer.merge(AdminSessionRepo.Default),
  Layer.merge(InstallationRepo.Default),
  Layer.merge(DeliveryRepo.Default),
  Layer.merge(RunRepo.Default),
  Layer.merge(RunInputRepo.Default),
  Layer.merge(RunEventRepo.Default),
  Layer.merge(ProjectionRepo.Default),
  Layer.merge(WorkspaceRepo.Default),
  Layer.merge(LinearGateway.Default),
  Layer.merge(GitHubApp.Default),
  Layer.merge(WebhookPipeline.Default),
  Layer.merge(ActivityProjector.Default),
  Layer.merge(SessionAuthority.Default),
  Layer.merge(RpcWorker.Default),
  Layer.merge(Reconciler.Default),
  Layer.merge(Workspace.Default),
  Layer.merge(OAuth.Default),
  Layer.merge(Admin.Default),
).pipe(Layer.provide(sqliteClientLayer), Layer.provide(GatewayConfig.Default));

const scheduledReconciler = Layer.unwrapEffect(
  Effect.gen(function* () {
    const config = yield* GatewayConfig;
    const periodic = Effect.repeat(
      Reconciler.tick(),
      Schedule.spaced(config.reconcilerIntervalMs),
    );
    const triggered = Effect.forever(
      Reconciler.awaitTrigger().pipe(Effect.zipRight(Reconciler.tick())),
    );
    return Layer.scopedDiscard(
      Effect.all([periodic, triggered], {
        concurrency: "unbounded",
        discard: true,
      }).pipe(Effect.forkScoped),
    );
  }),
);

const serverLayer = Layer.unwrapEffect(
  Effect.map(GatewayConfig, ({ port }) => BunHttpServer.layer({ port })),
);

const servicesWithServer = Layer.provideMerge(serverLayer, GatewayServicesLive);
const servicesWithHttp = Layer.provideMerge(
  HttpServer.serve(router),
  servicesWithServer,
);
const servicesWithScheduler = Layer.provideMerge(
  scheduledReconciler,
  servicesWithHttp,
);

export const GatewayLive = Layer.provideMerge(
  PinoLoggerLive,
  servicesWithScheduler,
);

export const main = Layer.launch(GatewayLive);
