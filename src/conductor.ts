import { HttpServer } from "@effect/platform";
import { BunHttpServer } from "@effect/platform-bun";
import { Effect, Layer, Schedule } from "effect";
import { router } from "./http/router.js";
import {
  ActivityProjector,
  AdminSessionRepo,
  Admin,
  DeliveryRepo,
  InstallationRepo,
  LinearGateway,
  OAuth,
  ProjectionRepo,
  Reconciler,
  RpcWorker,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
  SessionAuthority,
  WebhookPipeline,
  Workspace,
  WorkspaceRepo,
} from "./services/contracts.js";
import { GatewayConfig } from "./services/config.js";
import { GatewayLogger, PinoLoggerLive } from "./services/logger.js";
import { TokenCrypto } from "./services/token-crypto.js";
import { SqliteClientLive } from "./services/store/sqlite-client.js";

const sqliteClientLayer = Layer.unwrapEffect(
  Effect.gen(function* () {
    const { databasePath } = yield* GatewayConfig;
    return SqliteClientLive(databasePath);
  }),
);

const serviceLayers = GatewayConfig.Default.pipe(
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
  Layer.merge(WebhookPipeline.Default),
  Layer.merge(ActivityProjector.Default),
  Layer.merge(SessionAuthority.Default),
  Layer.merge(RpcWorker.Default),
  Layer.merge(Reconciler.Default),
  Layer.merge(Workspace.Default),
  Layer.merge(OAuth.Default),
  Layer.merge(Admin.Default),
).pipe(
  Layer.provide(sqliteClientLayer),
  Layer.provide(GatewayConfig.Default),
);

const scheduledReconciler = Layer.unwrapEffect(
  Effect.gen(function* () {
    const config = yield* GatewayConfig;
    return Layer.scopedDiscard(
      Effect.repeat(Reconciler.tick(), Schedule.spaced(config.reconcilerIntervalMs)).pipe(Effect.forkScoped),
    );
  }),
);

const serverLayer = Layer.unwrapEffect(
  Effect.map(GatewayConfig, ({ port }) => BunHttpServer.layer({ port })),
);

const servicesWithServer = Layer.provideMerge(serverLayer, serviceLayers);
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
