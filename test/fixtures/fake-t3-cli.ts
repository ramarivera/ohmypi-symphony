/**
 * Minimal `t3` CLI for launcher tests: `auth session issue --json` records a
 * bearer in the base dir; `serve` accepts only bearers from its own base dir,
 * starts a long-lived child like a provider would, and prints a pairing code.
 * The first argument selects the behavior: `ok`, `crash`, or
 * `proxy <port> <token>`, which hands out an in-process FakeT3Server's bearer
 * and forwards HTTP and WebSocket traffic to it.
 */
import { appendFileSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const [behavior, ...rest] = process.argv.slice(2);
const upstream = behavior === "proxy" ? rest.splice(0, 2) : [];
const args = rest;
const flag = (name: string): string => {
  const index = args.indexOf(name);
  const value = index === -1 ? undefined : args[index + 1];
  if (value === undefined) throw new Error(`missing ${name}`);
  return value;
};
const baseDir = flag("--base-dir");
const envKeys = Object.keys(process.env).sort();

if (args[0] === "auth" && args[1] === "session" && args[2] === "issue") {
  const token = upstream[1] ?? `bearer-${crypto.randomUUID()}`;
  appendFileSync(join(baseDir, "tokens.txt"), `${token}\n`);
  writeFileSync(join(baseDir, "issue-env.json"), JSON.stringify(envKeys));
  process.stdout.write(
    JSON.stringify({ token, sessionId: crypto.randomUUID(), scopes: [] }),
  );
  process.exit(0);
}

if (args[0] !== "serve") throw new Error(`unexpected command ${args[0]}`);
writeFileSync(
  join(baseDir, "serve-env.json"),
  JSON.stringify({
    keys: envKeys,
    home: process.env.HOME,
    t3Home: process.env.T3CODE_HOME,
    cwd: process.cwd(),
    workspace: args.at(-1),
  }),
);
console.log("Token: PAIRING1234X");
if (behavior === "crash") {
  console.error("fatal: provider setup failed");
  process.exit(3);
}
const provider = Bun.spawn(["sleep", "300"], {
  stdio: ["ignore", "ignore", "ignore"],
});
writeFileSync(join(baseDir, "provider.pid"), String(provider.pid));
if (behavior === "proxy") {
  const target = `127.0.0.1:${upstream[0]}`;
  Bun.serve<{ readonly upstream: WebSocket; readonly queue: string[] }>({
    hostname: flag("--host"),
    port: Number(flag("--port")),
    fetch(request, server) {
      const url = new URL(request.url);
      if (url.pathname === "/ws") {
        const socket = new WebSocket(
          `ws://${target}${url.pathname}${url.search}`,
        );
        const queue: string[] = [];
        return server.upgrade(request, { data: { upstream: socket, queue } })
          ? undefined
          : new Response("upgrade failed", { status: 400 });
      }
      return fetch(`http://${target}${url.pathname}${url.search}`, {
        method: request.method,
        headers: request.headers,
        body: request.method === "GET" ? null : request.body,
      });
    },
    websocket: {
      open(ws) {
        const { upstream: socket, queue } = ws.data;
        socket.onmessage = (event) => ws.send(String(event.data));
        socket.onclose = () => ws.close();
        socket.onopen = () => {
          for (const message of queue.splice(0)) socket.send(message);
        };
      },
      message(ws, message) {
        const text = typeof message === "string" ? message : message.toString();
        if (ws.data.upstream.readyState === WebSocket.OPEN)
          ws.data.upstream.send(text);
        else ws.data.queue.push(text);
      },
      close(ws) {
        ws.data.upstream.close();
      },
    },
  });
  process.on("SIGTERM", () => process.exit(0));
} else {
  const tokens = () =>
    new Set(readFileSync(join(baseDir, "tokens.txt"), "utf8").split("\n"));
  Bun.serve({
    hostname: flag("--host"),
    port: Number(flag("--port")),
    fetch(request) {
      const url = new URL(request.url);
      if (url.pathname === "/.well-known/t3/environment")
        return Response.json({ serverVersion: "fake" });
      if (url.pathname === "/api/auth/websocket-ticket") {
        const bearer = request.headers
          .get("authorization")
          ?.replace(/^Bearer /u, "");
        return bearer !== undefined && tokens().has(bearer)
          ? Response.json({ ticket: "ticket" })
          : new Response("unauthorized", { status: 401 });
      }
      return new Response("not found", { status: 404 });
    },
  });
  process.on("SIGTERM", () => process.exit(0));
}
