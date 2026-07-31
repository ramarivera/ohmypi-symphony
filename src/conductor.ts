import { HttpServer } from "@effect/platform";
import { BunHttpServer } from "@effect/platform-bun";
import { Effect, Layer, Schedule } from "effect";
import { router } from "./http/router.js";
import {
  ActivityProjector,
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

const serviceLayers = Layer.mergeAll(
  GatewayConfig.Default,
  GatewayLogger.Default,
  TokenCrypto.Default,
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
);

const scheduledReconciler = Layer.unwrapEffect(
  Effect.gen(function* () {
    const config = yield* GatewayConfig;
    return Layer.scopedDiscard(
      Effect.repeat(Reconciler.tick(), Schedule.spaced(config.reconcilerIntervalMs)).pipe(Effect.forkScoped),
    );
  }),
).pipe(Layer.provide(serviceLayers));

const serverLayer = Layer.unwrapEffect(
  Effect.map(GatewayConfig, ({ port }) => BunHttpServer.layer({ port })),
).pipe(Layer.provide(serviceLayers));

export const GatewayLive = Layer.mergeAll(
  HttpServer.serve(router).pipe(
    Layer.provide(serviceLayers),
    Layer.provide(serverLayer),
  ),
  scheduledReconciler.pipe(Layer.provide(serviceLayers)),
  PinoLoggerLive.pipe(Layer.provide(serviceLayers)),
);

export const main = Layer.launch(GatewayLive);
