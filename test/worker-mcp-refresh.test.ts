import { Database } from "bun:sqlite";
import { readFile, writeFile } from "node:fs/promises";
import {
  createServer,
  request as httpRequest,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { join } from "node:path";
import { Readable } from "node:stream";
import {
  Clock,
  ConfigProvider,
  Effect,
  Layer,
  Option,
  Runtime,
  Schema,
} from "effect";
import { describe, expect, it, vi } from "vitest";
import { McpServerId, OrganizationId } from "../src/domain/ids.js";
import { McpServerRecord } from "../src/domain/models.js";
import { GatewayConfig } from "../src/services/config.js";
import { writeOmpMcpConfig } from "../src/services/mcp-config.js";
import {
  MCP_OAUTH_ENCRYPTED_PREFIX,
  McpOAuth,
  materializeMcpAgentDb,
} from "../src/services/mcp-oauth.js";
import { safeMcpFetch } from "../src/services/safe-http.js";
import { McpServerRepo } from "../src/services/store/repositories.js";
import {
  SqliteClient,
  SqliteClientLive,
} from "../src/services/store/sqlite-client.js";
import { TokenCrypto } from "../src/services/token-crypto.js";
import { tokenContext } from "../src/services/token-migration.js";
import {
  startWorkerMcpProxy,
  type WorkerMcpProxy,
} from "../src/services/worker-mcp-proxy.js";
import { testPaths } from "./fixtures/paths.js";

const testPath = testPaths();
const org = Schema.decodeUnknownSync(OrganizationId)("refresh-org");
const id = Schema.decodeUnknownSync(McpServerId)("refresh-server");
const refreshSecret = "gateway-only-refresh-secret";
const clientSecret = "gateway-only-client-secret";

const withGateway = async (
  run: (context: {
    readonly createProxy: () => WorkerMcpProxy;
    readonly advance: () => void;
    readonly failRefresh: () => void;
    readonly rejectAccess: () => void;
    readonly rejectAllAccess: () => void;
    readonly refreshes: () => number;
    readonly failures: string[];
    readonly seen: string[];
  }) => Promise<void>,
) => {
  let now = Date.now();
  let refreshed = 0;
  let fail = false;
  let reject = false;
  let rejectAll = false;
  const seen: string[] = [];
  const failures: string[] = [];
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      if (new URL(request.url).pathname === "/token") {
        refreshed++;
        const form = new URLSearchParams(await request.text());
        expect(form.get("refresh_token")).toBe(
          refreshed === 1 ? refreshSecret : `rotated-${refreshed - 1}`,
        );
        expect(form.get("client_secret")).toBe(clientSecret);
        // Force competing sessions to overlap while the real refresh is in flight.
        await Bun.sleep(20);
        return fail
          ? Response.json({ error: refreshSecret }, { status: 400 })
          : Response.json({
              access_token: `fresh-${refreshed}`,
              refresh_token: `rotated-${refreshed}`,
              expires_in: 3600,
            });
      }
      const auth = request.headers.get("authorization") ?? "";
      seen.push(auth);
      expect(request.headers.get("cookie")).toBeNull();
      if (rejectAll || (reject && auth === "Bearer initial-access"))
        return new Response(null, { status: 401 });
      if (request.method === "GET") return new Response(null, { status: 405 });
      return Response.json(
        { ok: true, session: request.headers.get("mcp-session-id") },
        {
          headers: {
            "mcp-session-id": "upstream-session",
            "set-cookie": "private=secret",
          },
        },
      );
    },
  });
  const url = `http://127.0.0.1:${upstream.port}/mcp`;
  const provider = ConfigProvider.fromMap(
    new Map([
      ["LINEAR_CLIENT_ID", "client"],
      ["LINEAR_CLIENT_SECRET", "secret"],
      ["LINEAR_WEBHOOK_SECRET", "webhook"],
      ["PUBLIC_URL", "http://localhost:3000"],
      ["TOKEN_ENCRYPTION_KEY", Buffer.alloc(32, 7).toString("base64")],
      ["MCP_OAUTH_ALLOW_LOOPBACK_HTTP", "true"],
    ]),
  );
  const config = GatewayConfig.Default.pipe(
    Layer.provide(Layer.setConfigProvider(provider)),
  );
  const sqlite = SqliteClientLive(":memory:");
  const crypto = TokenCrypto.Default.pipe(Layer.provide(config));
  const servers = McpServerRepo.Default.pipe(
    Layer.provide(Layer.mergeAll(sqlite, crypto)),
  );
  const oauth = McpOAuth.Default.pipe(
    Layer.provide(Layer.mergeAll(config, sqlite, crypto, servers)),
  );
  const clock = Object.assign(Clock.make(), {
    currentTimeMillis: Effect.sync(() => now),
    unsafeCurrentTimeMillis: () => now,
  });
  try {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const repo = yield* McpServerRepo;
          const cipher = yield* TokenCrypto;
          const { db } = yield* SqliteClient;
          const service = yield* McpOAuth;
          yield* repo.createMcpServer({
            organizationId: org,
            id,
            name: "upstream",
            transport: "http",
            url,
            now,
          });
          const encrypted = (field: string, value: string) =>
            cipher.encrypt(
              value,
              tokenContext("mcp_oauth_credential", org, id, field),
            );
          const client = yield* encrypted(
            "client_json",
            JSON.stringify({
              clientId: "client",
              clientSecret,
              tokenEndpointAuthMethod: "client_secret_post",
              authorizationEndpoint: `http://127.0.0.1:${upstream.port}/authorize`,
              tokenEndpoint: `http://127.0.0.1:${upstream.port}/token`,
            }),
          );
          const access = yield* encrypted("access_token", "initial-access");
          const refresh = yield* encrypted("refresh_token", refreshSecret);
          db.query(
            "INSERT INTO mcp_oauth_credential (organization_id, server_id, server_url, client_json, access_token, refresh_token, expires_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
          ).run(
            org,
            id,
            url,
            `${MCP_OAUTH_ENCRYPTED_PREFIX}${client}`,
            `${MCP_OAUTH_ENCRYPTED_PREFIX}${access}`,
            `${MCP_OAUTH_ENCRYPTED_PREFIX}${refresh}`,
            now + 120_000,
            now,
            now,
          );
          const server = Option.getOrThrow(yield* repo.getMcpServer(org, id));
          const runPromise = Runtime.runPromise(yield* Effect.runtime<never>());
          const proxies: WorkerMcpProxy[] = [];
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              for (const proxy of proxies) proxy.stop();
            }),
          );
          yield* Effect.promise(() =>
            run({
              createProxy: () => {
                const proxy = startWorkerMcpProxy({
                  servers: [server],
                  allowLoopbackHttp: true,
                  accessToken: (entry, rejectedAccessToken) =>
                    runPromise(
                      service
                        .mintCredential(org, entry.id, {
                          serverUrl: url,
                          ...(rejectedAccessToken === undefined
                            ? {}
                            : { rejectedAccessToken }),
                        })
                        .pipe(
                          Effect.map(
                            (token) => Option.getOrThrow(token).accessToken,
                          ),
                        ),
                    ),
                  refreshFailed: async (entry) => {
                    failures.push(entry.id);
                  },
                });
                proxies.push(proxy);
                return proxy;
              },
              advance: () => {
                now += 3_600_001;
              },
              failRefresh: () => {
                fail = true;
              },
              rejectAccess: () => {
                reject = true;
              },
              rejectAllAccess: () => {
                rejectAll = true;
              },
              refreshes: () => refreshed,
              failures,
              seen,
            }),
          );
        }).pipe(
          Effect.provide(Layer.mergeAll(sqlite, crypto, servers, oauth)),
          Effect.withClock(clock),
        ),
      ),
    );
  } finally {
    upstream.stop(true);
  }
};

const request = (
  proxy: WorkerMcpProxy,
  headers: Record<string, string> = {},
) => {
  const server = proxy.servers[0];
  if (server === undefined) throw new Error("Expected proxy server");
  return fetch(Option.getOrThrow(server.url), {
    method: "POST",
    headers: { ...server.headers, ...headers },
    body: "{}",
  });
};

const withSseProxy = async (
  run: (fixture: {
    readonly open: () => Promise<{
      readonly reader: ReadableStreamDefaultReader<Uint8Array>;
      readonly path: string;
      readonly upstream: ServerResponse;
      readonly advertise: (endpoint: string) => Promise<string>;
    }>;
    readonly post: (path: string) => Promise<Response>;
  }) => Promise<void>,
  sharedEndpoint = false,
) => {
  const streams: ServerResponse[] = [];
  const upstream = createServer((request, response) => {
    if (request.method === "POST") {
      response.writeHead(202);
      response.end();
      return;
    }
    streams.push(response);
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.write(
      `event: endpoint\ndata: /messages?session=${sharedEndpoint ? 0 : streams.length}\n\n`,
    );
  });
  await new Promise<void>((resolve) =>
    upstream.listen(0, "127.0.0.1", resolve),
  );
  const address = upstream.address();
  if (address === null || typeof address === "string")
    throw new Error("Expected upstream address");
  const server = Schema.decodeUnknownSync(McpServerRecord)({
    id,
    organizationId: org,
    name: "legacy-lifetime",
    transport: "sse",
    command: null,
    args: [],
    url: `http://127.0.0.1:${address.port}/sse`,
    env: {},
    repositoryId: null,
    enabled: true,
    createdAt: 1,
    updatedAt: 1,
  });
  const proxy = startWorkerMcpProxy({
    servers: [server],
    allowLoopbackHttp: true,
    accessToken: async () => "access",
    refreshFailed: async () => {
      throw new Error("Unexpected refresh failure");
    },
  });
  try {
    const config = proxy.servers[0];
    if (config === undefined) throw new Error("Expected proxy server");
    const url = Option.getOrThrow(config.url);
    const readEndpoint = async (
      reader: ReadableStreamDefaultReader<Uint8Array>,
    ) => {
      const chunk = await reader.read();
      const event = new TextDecoder().decode(chunk.value);
      expect(event).toMatch(
        /^event: endpoint\ndata: \/messages\/[a-f0-9]+\n\n$/u,
      );
      const path = event.split("\n")[1]?.slice("data: ".length);
      if (path === undefined) throw new Error("Expected endpoint");
      return path;
    };
    await run({
      async open() {
        const response = await new Promise<IncomingMessage>(
          (resolve, reject) => {
            const request = httpRequest(
              url,
              { headers: config.headers, agent: false },
              resolve,
            );
            request.on("error", reject);
            request.end();
          },
        );
        const reader = (
          Readable.toWeb(response) as unknown as ReadableStream<Uint8Array>
        ).getReader();
        const path = await readEndpoint(reader);
        const stream = streams.at(-1);
        if (path === undefined || stream === undefined)
          throw new Error("Expected endpoint and upstream stream");
        return {
          reader,
          path,
          upstream: stream,
          advertise: async (endpoint) => {
            stream.write(`event: endpoint\ndata: ${endpoint}\n\n`);
            return readEndpoint(reader);
          },
        };
      },
      post: (path) =>
        fetch(new URL(path, url), {
          method: "POST",
          headers: config.headers,
          body: "{}",
        }),
    });
  } finally {
    proxy.stop();
    await new Promise<void>((resolve, reject) => {
      upstream.close((error) => (error ? reject(error) : resolve()));
      upstream.closeAllConnections();
    });
  }
};

describe("gateway-mediated worker MCP refresh", () => {
  it("keeps the same worker configuration working after expiry without worker OAuth material", async () => {
    await withGateway(async (gateway) => {
      const proxy = gateway.createProxy();
      const workspace = testPath("worker");
      await Effect.runPromise(writeOmpMcpConfig(workspace, proxy.servers));
      const agentDir = await materializeMcpAgentDb(workspace, []);
      const config = await readFile(join(workspace, "mcp.json"), "utf8");
      const db = new Database(join(agentDir, "agent.db"));
      expect(db.query("SELECT * FROM auth_credentials").all()).toEqual([]);
      db.close();
      const payload = JSON.stringify(proxy.servers);
      for (const secret of [
        refreshSecret,
        clientSecret,
        "initial-access",
        "/token",
      ]) {
        expect(config).not.toContain(secret);
        expect(payload).not.toContain(secret);
        expect(
          (await readFile(join(agentDir, "agent.db"))).includes(
            Buffer.from(secret),
          ),
        ).toBe(false);
      }
      expect((await request(proxy)).status).toBe(200);
      gateway.advance();
      expect((await request(proxy)).status).toBe(200);
      gateway.advance();
      expect((await request(proxy)).status).toBe(200);
      expect(gateway.seen).toEqual([
        "Bearer initial-access",
        "Bearer fresh-1",
        "Bearer fresh-2",
      ]);
      expect(await readFile(join(workspace, "mcp.json"), "utf8")).toBe(config);
      expect(gateway.refreshes()).toBe(2);
    });
  });

  it("single-flights one installation credential across concurrent worker sessions", async () => {
    await withGateway(async (gateway) => {
      const first = gateway.createProxy();
      const second = gateway.createProxy();
      gateway.advance();
      const responses = await Promise.all(
        Array.from({ length: 12 }, (_, i) => request(i % 2 ? first : second)),
      );
      expect(responses.map((response) => response.status)).toEqual(
        Array(12).fill(200),
      );
      expect(gateway.refreshes()).toBe(1);
      expect(new Set(gateway.seen)).toEqual(new Set(["Bearer fresh-1"]));
    });
  });

  it("reports a sanitized renewal failure for each affected worker and never sends the expired token", async () => {
    await withGateway(async (gateway) => {
      const first = gateway.createProxy();
      const second = gateway.createProxy();
      gateway.advance();
      gateway.failRefresh();
      const responses = await Promise.all([request(first), request(second)]);
      expect(gateway.refreshes()).toBe(1);
      expect(gateway.failures).toEqual([id, id]);
      expect(gateway.seen).toEqual([]);
      for (const response of responses) {
        expect(response.status).toBe(502);
        expect(await response.text()).not.toContain(refreshSecret);
      }
    });
  });

  it("refreshes once on an early upstream 401 and strips worker-supplied credentials", async () => {
    await withGateway(async (gateway) => {
      const proxy = gateway.createProxy();
      gateway.rejectAccess();
      const response = await request(proxy, {
        cookie: "worker-cookie",
        "mcp-session-id": "session",
      });
      expect(response.status).toBe(200);
      expect(response.headers.get("set-cookie")).toBeNull();
      expect(response.headers.get("mcp-session-id")).toBe("upstream-session");
      expect(await response.json()).toEqual({ ok: true, session: "session" });
      expect(gateway.seen).toEqual(["Bearer initial-access", "Bearer fresh-1"]);
      expect(gateway.refreshes()).toBe(1);
    });
  });

  it("rejects absent and other-session bearers and revokes access on stop", async () => {
    await withGateway(async (gateway) => {
      const first = gateway.createProxy();
      const second = gateway.createProxy();
      expect((await request(first, { Authorization: "" })).status).toBe(401);
      expect((await request(first, second.servers[0]?.headers)).status).toBe(
        401,
      );
      const server = first.servers[0];
      if (server === undefined) throw new Error("Expected proxy server");
      expect(
        (
          await fetch(`${Option.getOrThrow(server.url)}?target=http://evil`, {
            headers: server.headers,
          })
        ).status,
      ).toBe(404);
      expect(gateway.seen).toEqual([]);
      first.stop();
      await expect(request(first)).rejects.toThrow();
    });
  });

  it("reports a sanitized failure when the refreshed credential is also rejected", async () => {
    await withGateway(async (gateway) => {
      const proxy = gateway.createProxy();
      gateway.rejectAllAccess();
      const response = await request(proxy);
      expect(response.status).toBe(502);
      expect(response.headers.get("www-authenticate")).toBeNull();
      expect(await response.text()).toBe(
        "MCP gateway request failed; check run events",
      );
      expect(gateway.seen).toEqual(["Bearer initial-access", "Bearer fresh-1"]);
      expect(gateway.refreshes()).toBe(1);
      expect(gateway.failures).toEqual([id]);
    });
  });
});

describe("worker MCP proxy transport boundary", () => {
  it.each(["close", "cancel", "error"] as const)(
    "releases legacy SSE routes on %s and supports repeated reconnects",
    async (termination) => {
      await withSseProxy(async ({ open, post }) => {
        const reconnects = termination === "error" ? 1 : 260;
        for (let index = 0; index < reconnects; index++) {
          const stream = await open();
          expect((await post(stream.path)).status).toBe(202);
          if (termination === "cancel") await stream.reader.cancel();
          else if (termination === "close") {
            stream.upstream.end();
            expect((await stream.reader.read()).done).toBe(true);
          } else {
            stream.upstream.destroy();
            // Bun may translate a reset after headers to EOF at its server
            // boundary. Either result must terminate the stream and its routes.
            await stream.reader.read().then(
              (next) => expect(next.done).toBe(true),
              (error: unknown) => expect(error).toBeInstanceOf(Error),
            );
          }
          await vi.waitFor(async () => {
            expect((await post(stream.path)).status).toBe(404);
          });
        }
      });
    },
    30_000,
  );

  it("evicts the oldest active SSE route at the cap and still accepts new endpoints", async () => {
    await withSseProxy(async ({ open, post }) => {
      const oldest = await open();
      let recent = oldest.path;
      for (let index = 1; index < 256; index++)
        recent = await oldest.advertise(`/messages?session=extra-${index}`);
      expect((await post(oldest.path)).status).toBe(202);
      const newest = await open();
      expect((await post(oldest.path)).status).toBe(404);
      expect((await post(newest.path)).status).toBe(202);
      await oldest.reader.cancel();
      expect((await post(newest.path)).status).toBe(202);
      await vi.waitFor(async () => {
        expect((await post(recent)).status).toBe(404);
      });
    });
  });

  it("keeps overlapping streams with the same upstream endpoint independently usable", async () => {
    await withSseProxy(async ({ open, post }) => {
      const first = await open();
      const second = await open();
      await first.reader.cancel();
      await vi.waitFor(async () => {
        expect((await post(first.path)).status).toBe(404);
      });
      expect((await post(second.path)).status).toBe(202);
      await second.reader.cancel();
      await vi.waitFor(async () => {
        expect((await post(second.path)).status).toBe(404);
      });
    }, true);
  });

  it("allows a real MCP tool to return headers after the OAuth five-second deadline", async () => {
    await withGateway(async (gateway) => {
      // Exercise the actual proxy and pinned HTTP socket, not a mocked fetch.
      const upstream = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        async fetch() {
          await Bun.sleep(5500);
          return Response.json({ result: "slow tool completed" });
        },
      });
      const original = gateway.createProxy().servers[0];
      if (original === undefined) throw new Error("Expected proxy server");
      const proxy = startWorkerMcpProxy({
        servers: [
          {
            ...original,
            url: Option.some(`http://127.0.0.1:${upstream.port}/mcp`),
          },
        ],
        allowLoopbackHttp: true,
        accessToken: async () => "access",
        refreshFailed: async () => {
          throw new Error("Unexpected refresh failure");
        },
      });
      try {
        const response = await request(proxy);
        expect(response.status).toBe(200);
        expect(await response.json()).toEqual({
          result: "slow tool completed",
        });
      } finally {
        proxy.stop();
        upstream.stop(true);
      }
    });
  }, 10_000);

  it("bounds MCP response headers independently and leaves established SSE streams alive", async () => {
    const upstream = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      async fetch(request) {
        if (new URL(request.url).pathname === "/slow") {
          await Bun.sleep(200);
          return Response.json({ ok: true });
        }
        return new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(
                new TextEncoder().encode("data: connected\n\n"),
              );
            },
          }),
          { headers: { "content-type": "text/event-stream" } },
        );
      },
    });
    const lifetime = new AbortController();
    try {
      const base = `http://127.0.0.1:${upstream.port}`;
      await expect(
        safeMcpFetch(`${base}/slow`, {}, { allowLoopbackHttp: true }, 50),
      ).rejects.toThrow();
      const response = await safeMcpFetch(
        `${base}/sse`,
        { signal: lifetime.signal },
        { allowLoopbackHttp: true },
        100,
      );
      const reader = response.body?.getReader();
      if (reader === undefined) throw new Error("Expected SSE stream");
      expect((await reader.read()).done).toBe(false);
      await Bun.sleep(200);
      const pending = reader.read();
      const outcome = pending.then(
        () => "ended",
        () => "ended",
      );
      expect(
        await Promise.race([outcome, Bun.sleep(50).then(() => "open")]),
      ).toBe("open");
      lifetime.abort();
      await expect(pending).rejects.toThrow();
    } finally {
      lifetime.abort();
      upstream.stop(true);
    }
  });

  it("streams legacy SSE, rewrites its POST endpoint and uses fresh authorization", async () => {
    const seen: string[] = [];
    let access = "first";
    const upstream = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch(request) {
        seen.push(request.headers.get("authorization") ?? "");
        if (request.method === "GET")
          return new Response(
            new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(
                  new TextEncoder().encode(
                    "event: endpoint\r\ndata: /messages?session=private\r\n\r\n",
                  ),
                );
              },
            }),
            { headers: { "content-type": "text/event-stream" } },
          );
        expect(new URL(request.url).pathname).toBe("/messages");
        expect(new URL(request.url).searchParams.get("session")).toBe(
          "private",
        );
        return new Response(null, { status: 202 });
      },
    });
    const server = Schema.decodeUnknownSync(McpServerRecord)({
      id,
      organizationId: org,
      name: "legacy",
      transport: "sse",
      command: null,
      args: [],
      url: `http://127.0.0.1:${upstream.port}/sse`,
      env: {},
      repositoryId: null,
      enabled: true,
      createdAt: 1,
      updatedAt: 1,
    });
    const proxy = startWorkerMcpProxy({
      servers: [server],
      allowLoopbackHttp: true,
      accessToken: async () => access,
      refreshFailed: async () => {
        throw new Error("Unexpected refresh failure");
      },
    });
    try {
      const config = proxy.servers[0];
      if (config === undefined) throw new Error("Expected proxy server");
      const response = await fetch(Option.getOrThrow(config.url), {
        headers: config.headers,
      });
      const reader = response.body?.getReader();
      if (reader === undefined) throw new Error("Expected SSE stream");
      const chunk = await reader.read();
      const event = new TextDecoder().decode(chunk.value);
      expect(event).toMatch(
        /^event: endpoint\ndata: \/messages\/[a-f0-9]+\n\n$/u,
      );
      expect(event).not.toContain("private");
      const path = event.split("\n")[1]?.slice("data: ".length);
      if (path === undefined) throw new Error("Expected advertised endpoint");
      access = "second";
      const posted = await fetch(new URL(path, Option.getOrThrow(config.url)), {
        method: "POST",
        headers: config.headers,
        body: "{}",
      });
      expect(posted.status).toBe(202);
      expect(seen).toEqual(["Bearer first", "Bearer second"]);
      await reader.cancel();
    } finally {
      proxy.stop();
      upstream.stop(true);
    }
  });

  it.each([true, false])(
    "rejects upstream redirects and enforces loopback opt-in with allowLoopbackHttp=%s",
    async (allowLoopbackHttp) => {
      let requests = 0;
      const upstream = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        fetch(request) {
          requests++;
          expect(new URL(request.url).pathname).toBe("/redirect");
          return new Response(null, {
            status: 302,
            headers: { location: "/secret-target" },
          });
        },
      });
      const server = Schema.decodeUnknownSync(McpServerRecord)({
        id,
        organizationId: org,
        name: "redirect",
        transport: "http",
        command: null,
        args: [],
        url: `http://127.0.0.1:${upstream.port}/redirect`,
        env: {},
        repositoryId: null,
        enabled: true,
        createdAt: 1,
        updatedAt: 1,
      });
      const proxy = startWorkerMcpProxy({
        servers: [server],
        allowLoopbackHttp,
        accessToken: async () => "access",
        refreshFailed: async () => {
          throw new Error("Unexpected refresh failure");
        },
      });
      try {
        const response = await request(proxy);
        expect(response.status).toBe(502);
        expect(response.headers.get("location")).toBeNull();
        expect(requests).toBe(allowLoopbackHttp ? 1 : 0);
      } finally {
        proxy.stop();
        upstream.stop(true);
      }
    },
  );

  it("refuses to skip a foreign configuration when a gateway proxy is required", async () => {
    const workspace = testPath("foreign-config");
    await writeFile(join(workspace, "mcp.json"), '{"foreign":true}');
    await expect(
      Effect.runPromise(writeOmpMcpConfig(workspace, [], "session", true)),
    ).rejects.toThrow("cannot replace foreign mcp.json");
    expect(await readFile(join(workspace, "mcp.json"), "utf8")).toBe(
      '{"foreign":true}',
    );
  });
});
