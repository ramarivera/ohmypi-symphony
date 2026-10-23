import { randomBytes, timingSafeEqual } from "node:crypto";
import { Option } from "effect";
import type { McpServerRecord } from "../domain/models.js";
import { readBoundedBody } from "./bounded-body.js";
import { MCP_MAX_RESPONSE_BYTES, safeMcpFetch } from "./safe-http.js";

export interface WorkerMcpProxy {
  readonly servers: ReadonlyArray<McpServerRecord>;
  readonly stop: () => void;
}

interface ProxyOptions {
  readonly servers: ReadonlyArray<McpServerRecord>;
  readonly allowLoopbackHttp: boolean;
  readonly accessToken: (
    server: McpServerRecord,
    rejectedAccessToken?: string,
  ) => Promise<string>;
  readonly refreshFailed: (server: McpServerRecord) => Promise<void>;
}

// Only these protocol headers cross the boundary. In particular, a worker can't
// override upstream authorization, cookies, Host, or proxy credentials.
const requestHeaders = [
  "accept",
  "content-type",
  "mcp-session-id",
  "mcp-protocol-version",
  "last-event-id",
];
const responseHeaders = [
  "content-type",
  "mcp-session-id",
  "mcp-protocol-version",
];

/** One capability and listener per worker lifetime, bound only to IPv4 loopback. */
export const startWorkerMcpProxy = (options: ProxyOptions): WorkerMcpProxy => {
  const bearer = randomBytes(32).toString("base64url");
  const expected = Buffer.from(`Bearer ${bearer}`);
  const lifetime = new AbortController();
  const routes = new Map<string, { server: McpServerRecord; upstream: URL }>();
  const endpoints = new Map<string, () => void>();
  const inputs = options.servers.map((server, index) => {
    const path = `/mcp/${index}`;
    routes.set(path, {
      server,
      upstream: new URL(Option.getOrThrow(server.url)),
    });
    return { server, path };
  });
  const listener = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    idleTimeout: 0,
    maxRequestBodySize: MCP_MAX_RESPONSE_BYTES,
    async fetch(request) {
      const supplied = Buffer.from(request.headers.get("authorization") ?? "");
      if (
        supplied.length !== expected.length ||
        !timingSafeEqual(supplied, expected)
      )
        return new Response("Unauthorized", { status: 401 });
      const url = new URL(request.url);
      const route = routes.get(url.pathname);
      if (!route || url.search)
        return new Response("Not found", { status: 404 });
      if (!["GET", "POST", "DELETE"].includes(request.method))
        return new Response("Method not allowed", { status: 405 });
      const { server, upstream } = route;
      const token = async (rejected?: string) => {
        try {
          return await options.accessToken(server, rejected);
        } catch {
          // Never include provider errors or credential material in a run event.
          await options.refreshFailed(server);
          throw new Error("MCP credential renewal failed");
        }
      };
      try {
        const body =
          request.method === "POST"
            ? new TextDecoder().decode(
                await readBoundedBody(request, MCP_MAX_RESPONSE_BYTES),
              )
            : undefined;
        const headers = new Headers(server.headers);
        for (const name of requestHeaders) {
          const value = request.headers.get(name);
          if (value !== null) headers.set(name, value);
        }
        const send = (access: string) => {
          headers.set("authorization", `Bearer ${access}`);
          return safeMcpFetch(
            upstream,
            {
              method: request.method,
              headers,
              ...(body === undefined ? {} : { body }),
              signal: AbortSignal.any([lifetime.signal, request.signal]),
            },
            { allowLoopbackHttp: options.allowLoopbackHttp },
          );
        };
        const access = await token();
        let response = await send(access);
        // A rejected token may have been revoked before its advertised expiry.
        // Retry once; mintCredential coalesces refreshes across worker sessions.
        if (response.status === 401) {
          await response.body?.cancel();
          response = await send(await token(access));
          if (response.status === 401) {
            await response.body?.cancel();
            await options.refreshFailed(server);
            throw new Error("MCP credential renewal failed");
          }
        }
        if (!response.ok) {
          await response.body?.cancel();
          return new Response("MCP upstream request failed", {
            status: response.status,
          });
        }
        const outgoing = new Headers({ "cache-control": "no-store" });
        for (const name of responseHeaders) {
          const value = response.headers.get(name);
          if (value !== null) outgoing.set(name, value);
        }
        let stream: ReadableStream<Uint8Array> | null = response.body;
        if (stream && server.transport === "sse" && request.method === "GET") {
          // Legacy SSE advertises a POST URL in an endpoint event. Replace it
          // with an opaque local route; never accept a worker-supplied target.
          let pending = "";
          const decoder = new TextDecoder();
          const streamRoutes = new Map<string, string>();
          stream = stream
            .pipeThrough(
              new TransformStream<Uint8Array, string>({
                transform(chunk, controller) {
                  controller.enqueue(decoder.decode(chunk, { stream: true }));
                },
                flush(controller) {
                  controller.enqueue(decoder.decode());
                },
              }),
            )
            .pipeThrough(
              new TransformStream<string, Uint8Array>({
                transform(chunk, controller) {
                  pending += chunk;
                  for (;;) {
                    const boundary = /\r?\n\r?\n/u.exec(pending);
                    if (!boundary) break;
                    let event = pending.slice(0, boundary.index);
                    pending = pending.slice(
                      boundary.index + boundary[0].length,
                    );
                    if (event.length > MCP_MAX_RESPONSE_BYTES)
                      throw new Error("MCP event too large");
                    if (/^event:\s*endpoint\s*$/mu.test(event)) {
                      const data = event
                        .split(/\r?\n/u)
                        .filter((line) => line.startsWith("data:"))
                        .map((line) => line.slice(5).trimStart())
                        .join("\n");
                      const target = new URL(data, upstream);
                      if (
                        target.origin !== upstream.origin ||
                        target.username ||
                        target.password ||
                        target.hash
                      )
                        throw new Error("Untrusted MCP SSE endpoint");
                      let path = streamRoutes.get(target.href);
                      if (path === undefined) {
                        if (endpoints.size >= 256)
                          endpoints.values().next().value?.();
                        const newPath = `/messages/${randomBytes(16).toString("hex")}`;
                        path = newPath;
                        streamRoutes.set(target.href, newPath);
                        endpoints.set(newPath, () => {
                          endpoints.delete(newPath);
                          routes.delete(newPath);
                          streamRoutes.delete(target.href);
                        });
                        routes.set(path, { server, upstream: target });
                      }
                      event = `event: endpoint\ndata: ${path}`;
                    }
                    controller.enqueue(
                      new TextEncoder().encode(`${event}\n\n`),
                    );
                  }
                  if (pending.length > MCP_MAX_RESPONSE_BYTES)
                    throw new Error("MCP event too large");
                },
              }),
            );
          const reader = stream.getReader();
          const release = () => {
            for (const path of streamRoutes.values()) endpoints.get(path)?.();
            request.signal.removeEventListener("abort", release);
          };
          request.signal.addEventListener("abort", release, { once: true });
          if (request.signal.aborted) release();
          stream = new ReadableStream<Uint8Array>({
            async pull(controller) {
              try {
                const next = await reader.read();
                if (next.done) {
                  release();
                  controller.close();
                } else controller.enqueue(next.value);
              } catch (error) {
                release();
                controller.error(error);
              }
            },
            cancel(reason) {
              release();
              return reader.cancel(reason);
            },
          });
        }
        return new Response(stream, {
          status: response.status,
          headers: outgoing,
        });
      } catch {
        return new Response("MCP gateway request failed; check run events", {
          status: 502,
        });
      }
    },
    error: () => new Response("MCP gateway request failed", { status: 502 }),
  });
  return {
    servers: inputs.map(({ server, path }) => ({
      ...server,
      url: Option.some(`http://127.0.0.1:${listener.port}${path}`),
      headers: { Authorization: `Bearer ${bearer}` },
      env: {},
    })),
    stop: () => {
      lifetime.abort();
      listener.stop(true);
      routes.clear();
      endpoints.clear();
    },
  };
};
