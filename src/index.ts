import { hostname } from "node:os";
import type { Server } from "bun";
import { createAdminRouter } from "./admin";
import { loadConfig } from "./config";
import { createLinearGateway } from "./linear-client";
import { createLogger } from "./logger";
import { ActivityProjector } from "./projector";
import { Reconciler } from "./reconciler";
import { OhMyPiRpcWorker } from "./rpc-worker";
import { SessionAuthority } from "./session-authority";
import { GatewayStore } from "./store";
import { handleWebhook } from "./webhook";
import { WorkspaceManager } from "./workspace";

const config = loadConfig();
const logger = createLogger({ level: config.logLevel, name: "gateway" });
logger.info({ event: "config.ready" });
logger.info({ event: "boot", publicUrl: config.publicUrl.toString() });

const store = await GatewayStore.open(
  config.databasePath,
  config.tokenEncryptionKey,
);
store.recoverPendingDeliveries();
store.recoverInterruptedRuns();
const linear = createLinearGateway(
  config,
  store,
  logger.child({ component: "linear" }),
);
const workspaces = new WorkspaceManager(
  config.workspaceRoot,
  store,
  logger.child({ component: "workspace" }),
);
const authority = new SessionAuthority({
  store,
  projector: new ActivityProjector(store, linear),
  workspaces,
  workerFactory: ({ cwd, run }) =>
    new OhMyPiRpcWorker({
      command: [
        config.ompCliPath,
        ...(run.ompSessionFile ? ["--session", run.ompSessionFile] : []),
      ],
      cwd,
      env: Bun.env,
      logger: logger.child({ component: "omp-rpc" }),
    }),
  owner: `${hostname()}:${process.pid}`,
  leaseDurationMs: config.leaseDurationMs,
  runUrlForSession: (sessionId) =>
    new URL(
      `/runs/${encodeURIComponent(sessionId)}`,
      config.publicUrl,
    ).toString(),
  logger: logger.child({ component: "session-authority" }),
});
const reconciler = new Reconciler(
  authority,
  1_000,
  logger.child({ component: "reconciler" }),
);
reconciler.start();

const adminRouter = createAdminRouter({
  config,
  store,
  workspaces,
  reconciler,
  logger: logger.child({ component: "admin" }),
});

function text(message: string, status: number): Response {
  return new Response(message, { status });
}

function buildFetch(): (request: Request) => Promise<Response> {
  return async (request) => {
    const url = new URL(request.url);
    const now = Date.now();
    try {
      const adminResponse = await adminRouter.handle(request, url, now);
      if (adminResponse !== null) return adminResponse;

      if (url.pathname === "/health" && request.method === "GET") {
        const status = reconciler.status;
        return Response.json(
          {
            status: status.lastError === null ? "ok" : "degraded",
            reconciler: {
              running: status.running,
              lastStartedAt: status.lastStartedAt,
              lastCompletedAt: status.lastCompletedAt,
            },
          },
          { status: status.lastError === null ? 200 : 503 },
        );
      }

      if (url.pathname === "/webhooks/linear") {
        const response = await handleWebhook(
          request,
          config,
          store,
          logger.child({ component: "webhook" }),
        );
        if (response.ok) void reconciler.tick();
        return response;
      }

      return new Response("Not found", { status: 404 });
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Internal server error";
      if (
        message.includes("already exists") ||
        message.includes("already exists for")
      ) {
        return text(message, 409);
      }
      if (
        message.includes("is required") ||
        message.includes("must be") ||
        message.includes("must not be empty") ||
        message.includes("must be a non-empty") ||
        message.includes("does not match path") ||
        message.includes("Invalid JSON") ||
        message.includes("Request body must be")
      ) {
        return text(message, 400);
      }
      logger.error({
        event: "request.failed",
        method: request.method,
        path: url.pathname,
        error: message,
      });
      return new Response("Internal server error", { status: 500 });
    }
  };
}

export function createServer(): Server<unknown> {
  return Bun.serve({
    port: config.port,
    fetch: buildFetch(),
  });
}

export { buildFetch, config, reconciler, store };

function start(): void {
  const server = createServer();
  logger.info({
    event: "listening",
    port: config.port,
    url: server.url.toString(),
  });

  let stopping = false;
  async function shutdown(): Promise<void> {
    if (stopping) return;
    stopping = true;
    logger.info({ event: "shutdown" });
    await server.stop(false);
    await reconciler.stop();
    store.close();
  }

  process.on("SIGINT", () => {
    void shutdown().finally(() => process.exit(0));
  });
  process.on("SIGTERM", () => {
    void shutdown().finally(() => process.exit(0));
  });
}

if (import.meta.main) {
  start();
}
