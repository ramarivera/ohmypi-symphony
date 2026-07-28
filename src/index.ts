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
  workerFactory: ({ cwd }) =>
    new OhMyPiRpcWorker({
      command: [config.ompCliPath],
      cwd,
      env: Bun.env,
    }),
  owner: `${hostname()}:${process.pid}`,
  leaseDurationMs: config.leaseDurationMs,
});
const reconciler = new Reconciler(authority);
reconciler.start();

const server = Bun.serve({
  port: config.port,
  async fetch(request) {
    const url = new URL(request.url);
    try {
      if (url.pathname === "/health" && request.method === "GET") {
        return Response.json({ status: "ok", reconciler: reconciler.status });
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
      if (url.pathname === "/webhooks/linear") {
        return handleWebhook(request, config, store);
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
