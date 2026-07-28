import { hostname } from "node:os";
import { loadConfig, loadRepositoryMap } from "./config";
import { createLinearGateway } from "./linear-client";
import { completeAuthorization, startAuthorization } from "./oauth";
import { ActivityProjector } from "./projector";
import { Reconciler } from "./reconciler";
import { OhMyPiRpcWorker } from "./rpc-worker";
import { SessionAuthority } from "./session-authority";
import { GatewayStore } from "./store";
import { handleWebhook } from "./webhook";
import { WorkspaceManager } from "./workspace";

const config = loadConfig();
const [store, repositoryMap] = await Promise.all([
  GatewayStore.open(config.databasePath, config.tokenEncryptionKey),
  loadRepositoryMap(config.repositoryMapPath),
]);
const linear = createLinearGateway(config, store);
const workspaces = new WorkspaceManager(config.workspaceRoot, repositoryMap);
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
    }),
  owner: `${hostname()}:${process.pid}`,
  leaseDurationMs: config.leaseDurationMs,
  runUrlForSession: (sessionId) =>
    new URL(
      `/runs/${encodeURIComponent(sessionId)}`,
      config.publicUrl,
    ).toString(),
});
const reconciler = new Reconciler(authority);
reconciler.start();

const server = Bun.serve({
  port: config.port,
  async fetch(request) {
    const url = new URL(request.url);
    try {
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
      if (url.pathname === "/oauth/start" && request.method === "GET") {
        const authorization = await startAuthorization(config, store);
        return Response.redirect(authorization.url, 302);
      }
      if (url.pathname === "/oauth/callback" && request.method === "GET") {
        await completeAuthorization(config, store, url);
        return new Response(
          "Linear installation connected. You can close this window.",
        );
      }
      if (url.pathname.startsWith("/runs/") && request.method === "GET") {
        const sessionId = decodeURIComponent(
          url.pathname.slice("/runs/".length),
        );
        const run = store.getRun(sessionId);
        if (!run) return new Response("Not found", { status: 404 });
        return Response.json({
          sessionId: run.sessionId,
          state: run.state,
          attempt: run.attempt,
          lastActivityAt: run.lastActivityAt,
        });
      }
      if (url.pathname === "/webhooks/linear") {
        const response = await handleWebhook(request, config, store);
        if (response.ok) void reconciler.tick();
        return response;
      }
      return new Response("Not found", { status: 404 });
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Internal server error";
      console.error(message);
      return new Response("Internal server error", { status: 500 });
    }
  },
});

console.log(`OhMyPi Linear gateway listening on ${server.url}`);

let stopping = false;
async function shutdown(): Promise<void> {
  if (stopping) return;
  stopping = true;
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
