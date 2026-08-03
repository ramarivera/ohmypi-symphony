import { HttpServer } from "@effect/platform";
import { BunHttpServer } from "@effect/platform-bun";
import { Effect, Layer, Schedule } from "effect";
import { router } from "./http/router.js";
import { Admin } from "./services/admin.js";
import { GatewayConfig } from "./services/config.js";
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

export const GatewayServicesLive = Layer.mergeAll(
  TokenCrypto.Default,
  AdminSessionRepo.Default,
  InstallationRepo.Default,
  DeliveryRepo.Default,
  RunRepo.Default,
  RunInputRepo.Default,
  RunEventRepo.Default,
  ProjectionRepo.Default,
  WorkspaceRepo.Default,
  LinearGateway.Default,
  WebhookPipeline.Default,
  ActivityProjector.Default,
  SessionAuthority.Default,
  RpcWorker.Default,
  Reconciler.Default,
  Workspace.Default,
  OAuth.Default,
  Admin.Default,
).pipe(
  Layer.provideMerge(sqliteClientLayer),
  Layer.provideMerge(GatewayConfig.Default),
);

const gatewayLoggerLive = PinoLoggerLive.pipe(
  Layer.provideMerge(GatewayLogger.Default),
  Layer.provideMerge(GatewayConfig.Default),
);

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

export const GatewayLive = servicesWithScheduler;

export const main = Layer.launch(GatewayLive).pipe(
  Effect.provide(gatewayLoggerLive),
);
