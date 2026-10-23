import { Database } from "bun:sqlite";
import { createCipheriv, randomBytes } from "node:crypto";
import {
  link,
  mkdir,
  mkdtemp,
  realpath,
  rm,
  stat,
  symlink,
  writeFile,
} from "node:fs/promises";
import { createServer } from "node:net";
import { homedir } from "node:os";
import { join } from "node:path";
import { ConfigProvider, Effect, Either, Fiber, Layer, Schema } from "effect";
import { afterEach, describe, expect, it, vi } from "vitest";
import { redact } from "../src/admin-ui/run-detail.js";
import { OrganizationId } from "../src/domain/ids.js";
import {
  rerunIssueGateCount,
  withRerunIssueGate,
} from "../src/services/admin.js";
import {
  BodyTooLargeError,
  readBoundedBody,
} from "../src/services/bounded-body.js";
import { GatewayConfig } from "../src/services/config.js";
import { makeGitHubApp } from "../src/services/github-app.js";
import { materializeMcpAgentDb } from "../src/services/mcp-oauth.js";
import {
  isPublicAddress,
  MCP_MAX_RESPONSE_BYTES,
  safeOAuthFetch,
} from "../src/services/safe-http.js";
import {
  SqliteClient,
  SqliteClientLive,
} from "../src/services/store/sqlite-client.js";
import { TokenCrypto } from "../src/services/token-crypto.js";
import {
  migrateTokenRows,
  tokenContext,
} from "../src/services/token-migration.js";
import {
  prepareWorkerWorkspace,
  workerEnvironment,
  workerIdentityCommand,
  workerMcpCredential,
} from "../src/services/worker-security.js";
import {
  isHttpsGitHubRepositoryUrl,
  persistGitHubExtraHeader,
} from "../src/services/workspace.js";
import {
  gitEnvironment,
  workspaceProcess,
} from "../src/services/workspace-process.js";

const dnsLookup = vi.hoisted(() =>
  vi.fn<
    (
      hostname: string,
      options: { all: true; verbatim: true },
    ) => Promise<Array<{ address: string; family: number }>>
  >(async () => [{ address: "93.184.216.34", family: 4 }]),
);
vi.mock("node:dns/promises", () => ({ lookup: dnsLookup }));
const dirs: string[] = [];
// Scratch storage is mandated by the global AGENTS.md; /tmp can be shared tmpfs.
// Override the base for CI while keeping fixture paths canonical.
const temporary = async () => {
  const parent =
    process.env.SECURITY_TEST_TMPDIR ??
    join(homedir(), "dev/agentic-scratchpads/ohmypi-symphony/security/tests");
  await mkdir(parent, { recursive: true });
  const dir = await realpath(await mkdtemp(join(parent, "case-")));
  dirs.push(dir);
  return dir;
};
afterEach(async () => {
  await Promise.all(
    dirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })),
  );
  vi.clearAllMocks();
});
const key = Buffer.alloc(32, 0x42).toString("base64");
const values = new Map([
  ["LINEAR_CLIENT_ID", "test"],
  ["LINEAR_CLIENT_SECRET", "test"],
  ["LINEAR_WEBHOOK_SECRET", "test"],
  ["TOKEN_ENCRYPTION_KEY", key],
  ["PUBLIC_URL", "http://localhost:3000"],
]);
const configLayer = (
  overrides: ReadonlyArray<readonly [string, string]> = [],
) =>
  Layer.setConfigProvider(
    ConfigProvider.fromMap(new Map([...values, ...overrides])),
  );
const cryptoLayer = (
  overrides: ReadonlyArray<readonly [string, string]> = [],
) => TokenCrypto.Default.pipe(Layer.provide(configLayer(overrides)));
const legacyToken = (plaintext: string): string => {
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", Buffer.from(key, "base64"), iv);
  const body = Buffer.concat([
    cipher.update(plaintext, "utf8"),
    cipher.final(),
  ]);
  return Buffer.concat([
    Buffer.from([1]),
    iv,
    body,
    cipher.getAuthTag(),
  ]).toString("base64url");
};

describe("security boundaries", () => {
  it("excludes unlisted secrets, secret files, database paths, and runtime injection variables from worker env", () => {
    const result = workerEnvironment({
      PATH: "/bin",
      HOME: "/worker",
      LANG: "C",
      TERM: "xterm",
      NIX_SSL_CERT_FILE: "/cert.pem",
      OPENAI_API_KEY: "worker-model-key",
      PI_CODING_AGENT_DIR: "/workspace/.omp",
      GITHUB_APP_PRIVATE_KEY: "secret",
      GITHUB_APP_PRIVATE_KEY_FILE: "/run/secrets/key",
      TOKEN_ENCRYPTION_KEY_FILE: "/keys/crypto",
      LINEAR_CLIENT_SECRET: "secret",
      DATABASE_PATH: "/app/data/gateway.sqlite",
      FUTURE_GATEWAY_SECRET: "secret",
      NODE_OPTIONS: "--import=evil",
      LD_PRELOAD: "/evil",
      NIX_SECRET_FILE: "/keys/nix",
    });
    expect(Object.keys(result).sort()).toEqual(
      [
        "HOME",
        "LANG",
        "NIX_SSL_CERT_FILE",
        "OPENAI_API_KEY",
        "PATH",
        "PI_CODING_AGENT_DIR",
        "TERM",
      ].sort(),
    );
    expect(workerIdentityCommand(["omp", "--mode", "rpc"], "1001")).toEqual(
      expect.arrayContaining([
        "--reuid",
        "1001",
        "--no-new-privs",
        "--bounding-set=-all",
      ]),
    );
    expect(() => workerIdentityCommand(["omp"], "0")).toThrow();
  });

  it("doesn't follow workspace symlinks when assigning worker ownership", async () => {
    const dir = await temporary();
    const workspace = join(dir, "workspace");
    await mkdir(workspace);
    const outside = join(dir, "outside");
    await writeFile(outside, "gateway");
    await symlink(outside, join(workspace, "key"));
    const before = await stat(outside);
    await prepareWorkerWorkspace(workspace, String(process.getuid?.()));
    expect((await stat(outside)).uid).toBe(before.uid);
    expect((await stat(outside)).mode).toBe(before.mode);
  });

  it("rejects hardlinks before assigning worker ownership", async () => {
    const dir = await temporary();
    const workspace = join(dir, "workspace");
    await mkdir(workspace);
    const outside = join(dir, "outside");
    await writeFile(outside, "gateway", { mode: 0o400 });
    await link(outside, join(workspace, "linked"));
    const before = await stat(outside);
    await expect(
      prepareWorkerWorkspace(workspace, String(process.getuid?.())),
    ).rejects.toThrow("Multiply-linked");
    expect((await stat(outside)).uid).toBe(before.uid);
    expect(await Bun.file(outside).text()).toBe("gateway");
  });

  it("runs the materializer entry point and excludes writable tool state", async () => {
    const dir = await temporary();
    const initialized = Bun.spawn(["git", "init", dir], {
      stdout: "ignore",
      stderr: "pipe",
    });
    expect(await initialized.exited).toBe(0);
    await workspaceProcess(dir, "tool-directories", null);
    await writeFile(join(dir, ".omp-home", "private"), "secret");
    await writeFile(join(dir, ".omp-tmp", "temporary"), "temp");
    const status = Bun.spawn(["git", "status", "--porcelain"], {
      cwd: dir,
      stdout: "pipe",
    });
    expect(await new Response(status.stdout).text()).toBe("");
    expect(await status.exited).toBe(0);
    const agentDir = await workspaceProcess(dir, "agent-db", []);
    expect(agentDir).toBe(join(dir, ".omp-gateway"));
    await workspaceProcess(dir, "remove-agent-db", null);
    expect(await Bun.file(join(agentDir, "agent.db")).exists()).toBe(false);
  });

  it("refuses to append credentials when the previous config cannot be parsed", async () => {
    const dir = await temporary();
    await mkdir(join(dir, ".git"));
    const path = join(dir, ".git", "config");
    const malformed = "[unterminated\nextraheader = old\n";
    await writeFile(path, malformed);
    const result = await Effect.runPromise(
      Effect.either(
        persistGitHubExtraHeader(
          dir,
          "test",
          `AUTHORIZATION: basic ${Buffer.from("x-access-token:secret").toString("base64")}`,
        ),
      ),
    );
    expect(Either.isLeft(result)).toBe(true);
    expect(await Bun.file(path).text()).toBe(malformed);
  });

  it("retains a useful credential persistence failure cause without exposing tokens", async () => {
    const dir = await temporary();
    const outside = join(dir, "outside");
    await mkdir(outside);
    const workspace = join(dir, "workspace");
    await mkdir(workspace);
    await symlink(outside, join(workspace, ".git"));
    const result = await Effect.runPromise(
      Effect.either(
        persistGitHubExtraHeader(
          workspace,
          "test",
          `AUTHORIZATION: basic ${Buffer.from("x-access-token:secret").toString("base64")}`,
        ),
      ),
    );
    if (Either.isRight(result)) throw new Error("Expected persistence to fail");
    expect(result.left.message).toBe("Could not persist GitHub credential");
    expect(result.left.cause).toContain(".git");
    expect(result.left.cause).not.toContain("secret");
  });

  it("doesn't expose gateway or model secrets to Git or its helpers", () => {
    const environment = gitEnvironment({
      PATH: "/bin",
      HOME: "/home/git",
      GIT_CONFIG_GLOBAL: "/gitconfig",
      LINEAR_CLIENT_SECRET: "secret",
      TOKEN_ENCRYPTION_KEY_FILE: "/key",
      OPENAI_API_KEY: "model-secret",
      GITHUB_APP_PRIVATE_KEY: "private",
      GIT_CONFIG_COUNT: "1",
      GIT_CONFIG_VALUE_0: "stale",
      NODE_OPTIONS: "--import=evil",
    });
    expect(environment).toEqual({
      PATH: "/bin",
      HOME: "/home/git",
      GIT_CONFIG_GLOBAL: "/gitconfig",
      GIT_CONFIG_NOSYSTEM: "1",
      GIT_TERMINAL_PROMPT: "0",
    });
  });

  it.each([
    "--upload-pack=touch /app/data/owned",
    "-repo",
    "file:///app/data",
    "/app/data",
    "git@github.com:org/repo",
    "https://github.com@evil.test/org/repo",
    "https://github.com/org/repo?x=y",
    "https://github.com/org/repo#x",
    "https://github.com/org/%2Frepo",
    "https://github.com/org/../repo",
    "https://github.com:443/org/repo",
    "https://github.com/org/repo/extra",
    "https://github.com/org/..",
  ])("rejects unsafe repository URL %s", (url) =>
    expect(isHttpsGitHubRepositoryUrl(url)).toBe(false),
  );
  it("accepts canonical HTTPS GitHub repository URLs", () => {
    expect(isHttpsGitHubRepositoryUrl("https://github.com/org/repo.git")).toBe(
      true,
    );
    expect(
      isHttpsGitHubRepositoryUrl("https://github.com/org/repo_name-1"),
    ).toBe(true);
  });

  it("denies cross-tenant GitHub token minting before making a network request", async () => {
    const fetchMock = vi.fn();
    const app = makeGitHubApp({
      appId: "1",
      privateKey: "unused",
      allowedOwners: { "org-a": ["alice"] },
      fetch: fetchMock,
    });
    const denied = await Effect.runPromise(
      Effect.either(app.getInstallationToken("alice", "repo", "org-b")),
    );
    expect(Either.isLeft(denied) && denied.left.reason).toBe(
      "tenant_forbidden",
    );
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("redacts GitHub, Basic, bearer and multiline PEM credentials", () => {
    const secrets = [
      "ghs_12345",
      "ghp_12345",
      "gho_12345",
      "github_pat_12345",
      "Basic dXNlcjpwYXNz",
      "bEaReR secret-value",
      "-----BEGIN PRIVATE KEY-----\nsecret-material\n-----END PRIVATE KEY-----",
    ];
    const output = redact(`AUTHORIZATION: ${secrets.join("\n")}`);
    for (const secret of secrets) expect(output).not.toContain(secret);
    expect(output).not.toContain("secret-material");
  });

  it("enforces body caps for declared and streamed bodies, cancelling on overflow", async () => {
    await expect(
      readBoundedBody(
        new Request("http://example.test", {
          method: "POST",
          headers: { "content-length": "100" },
          body: "x",
        }),
        4,
      ),
    ).rejects.toBeInstanceOf(BodyTooLargeError);
    let cancelled = false;
    const stream = new ReadableStream<Uint8Array>({
      pull(controller) {
        controller.enqueue(new Uint8Array(5));
      },
      cancel() {
        cancelled = true;
      },
    });
    await expect(
      readBoundedBody(new Response(stream), 4),
    ).rejects.toBeInstanceOf(BodyTooLargeError);
    expect(cancelled).toBe(true);
  });

  it.each([
    "127.0.0.1",
    "10.0.0.1",
    "100.100.100.200",
    "169.254.169.254",
    "172.16.0.1",
    "192.168.1.1",
    "0.0.0.0",
    "224.0.0.1",
    "::1",
    "::ffff:127.0.0.1",
    "fd00:ec2::254",
    "fe80::1",
    "64:ff9b::a00:1",
  ])("blocks private or metadata address %s", (address) =>
    expect(isPublicAddress(address)).toBe(false),
  );
  it("blocks DNS with a private answer, cross-origin redirects, POST redirects, and oversized metadata", async () => {
    const lookupMock = dnsLookup;
    lookupMock.mockResolvedValueOnce([{ address: "10.0.0.1", family: 4 }]);
    const transport = vi.fn<typeof fetch>();
    await expect(
      safeOAuthFetch("https://private.example", {}, {}, transport),
    ).rejects.toThrow("forbidden address");
    expect(transport).not.toHaveBeenCalled();
    transport.mockResolvedValueOnce(
      new Response(null, {
        status: 302,
        headers: { location: "http://169.254.169.254/latest" },
      }),
    );
    await expect(
      safeOAuthFetch("https://public.example", {}, {}, transport),
    ).rejects.toThrow("redirect");
    transport.mockResolvedValueOnce(
      new Response(null, { status: 307, headers: { location: "/other" } }),
    );
    await expect(
      safeOAuthFetch(
        "https://public.example",
        { method: "POST", body: "client_secret=secret" },
        {},
        transport,
      ),
    ).rejects.toThrow("redirect");
    transport.mockResolvedValueOnce(
      new Response("x".repeat(MCP_MAX_RESPONSE_BYTES + 1)),
    );
    await expect(
      safeOAuthFetch("https://public.example", {}, {}, transport),
    ).rejects.toBeInstanceOf(BodyTooLargeError);
    expect(
      transport.mock.calls.every(([, init]) => init?.redirect === "manual"),
    ).toBe(true);
  });
  it("revalidates DNS on same-origin redirects and gates loopback HTTP explicitly", async () => {
    const transport = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response(null, { status: 302, headers: { location: "/next" } }),
      );
    dnsLookup
      .mockResolvedValueOnce([{ address: "93.184.216.34", family: 4 }])
      .mockResolvedValueOnce([{ address: "169.254.169.254", family: 4 }]);
    await expect(
      safeOAuthFetch("https://rebinding.example", {}, {}, transport),
    ).rejects.toThrow("forbidden address");
    expect(transport).toHaveBeenCalledTimes(1);
    await expect(
      safeOAuthFetch("http://127.0.0.1", {}, {}, transport),
    ).rejects.toThrow("HTTPS");
    transport.mockResolvedValueOnce(new Response("ok"));
    expect(
      (
        await safeOAuthFetch(
          "http://127.0.0.1",
          {},
          { allowLoopbackHttp: true },
          transport,
        )
      ).status,
    ).toBe(200);
  });

  it("uses the pinned HTTP transport for explicitly enabled local development", async () => {
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: () => new Response("pinned-connection"),
    });
    try {
      const response = await safeOAuthFetch(
        `http://127.0.0.1:${server.port}/metadata`,
        {},
        { allowLoopbackHttp: true },
      );
      expect(await response.text()).toBe("pinned-connection");
    } finally {
      await server.stop(true);
    }
  });

  it("rejects invalid upstream statuses without an uncaught callback exception", async () => {
    const server = createServer((socket) => {
      socket.once("data", () =>
        socket.end(
          "HTTP/1.1 600 Invalid\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        ),
      );
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const address = server.address();
    if (!address || typeof address === "string")
      throw new Error("Missing fixture address");
    try {
      await expect(
        safeOAuthFetch(
          `http://127.0.0.1:${address.port}`,
          {},
          { allowLoopbackHttp: true },
        ),
      ).rejects.toThrow("Invalid OAuth HTTP response status");
    } finally {
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    }
  });

  it("fails closed in production and reads organization/owner policy", async () => {
    const load = (overrides: ReadonlyArray<readonly [string, string]>) =>
      Effect.runPromise(
        Effect.either(
          Effect.gen(function* () {
            return yield* GatewayConfig;
          }).pipe(
            Effect.provide(
              GatewayConfig.Default.pipe(Layer.provide(configLayer(overrides))),
            ),
          ),
        ),
      );
    expect(Either.isLeft(await load([["NODE_ENV", "production"]]))).toBe(true);
    const admitted = await load([
      ["NODE_ENV", "production"],
      ["LINEAR_ALLOWED_ORGANIZATION_IDS", "org-a,org-b"],
      ["GITHUB_APP_ALLOWED_OWNERS_JSON", '{"org-a":["Alice"]}'],
    ]);
    expect(
      Either.isRight(admitted) &&
        admitted.right.allowedOrganizationIds?.has("org-b"),
    ).toBe(true);
    expect(
      Either.isRight(admitted) && admitted.right.githubAppAllowedOwners,
    ).toEqual({ "org-a": ["alice"] });
  });

  it("binds v2 tokens to table, org, row and field; decrypts v1; rotates keys", async () => {
    const context = tokenContext(
      "installation",
      "org-a",
      "org-a",
      "access_token",
    );
    const token = await Effect.runPromise(
      Effect.gen(function* () {
        const cipher = yield* TokenCrypto;
        expect(yield* cipher.decrypt(legacyToken("old"), context)).toBe("old");
        const encrypted = yield* cipher.encrypt("secret", context);
        expect([...Buffer.from(encrypted, "base64url").subarray(0, 2)]).toEqual(
          [2, 1],
        );
        for (const wrong of [
          tokenContext("other", "org-a", "org-a", "access_token"),
          tokenContext("installation", "org-b", "org-a", "access_token"),
          tokenContext("installation", "org-a", "other", "access_token"),
          tokenContext("installation", "org-a", "org-a", "refresh_token"),
        ])
          expect(
            Either.isLeft(
              yield* Effect.either(cipher.decrypt(encrypted, wrong)),
            ),
          ).toBe(true);
        return encrypted;
      }).pipe(Effect.provide(cryptoLayer())),
    );
    const next = Buffer.alloc(32, 0x43).toString("base64");
    await Effect.runPromise(
      Effect.gen(function* () {
        const cipher = yield* TokenCrypto;
        expect(yield* cipher.decrypt(token, context)).toBe("secret");
        const encrypted = yield* cipher.encrypt("new", context);
        expect(Buffer.from(encrypted, "base64url")[1]).toBe(2);
      }).pipe(
        Effect.provide(
          cryptoLayer([
            ["TOKEN_ENCRYPTION_KEY", next],
            ["TOKEN_ENCRYPTION_KEY_ID", "2"],
            [
              "TOKEN_ENCRYPTION_PREVIOUS_KEYS_JSON",
              JSON.stringify({ "1": key }),
            ],
          ]),
        ),
      ),
    );
  });

  it("migrates stored v1 rows atomically and loads TOKEN_ENCRYPTION_KEY_FILE", async () => {
    const dir = await temporary();
    const keyFile = join(dir, "key");
    await writeFile(keyFile, key, { mode: 0o600 });
    const fileValues = new Map(values);
    fileValues.delete("TOKEN_ENCRYPTION_KEY");
    fileValues.set("TOKEN_ENCRYPTION_KEY_FILE", keyFile);
    const sqlite = SqliteClientLive(":memory:");
    const cipher = TokenCrypto.Default.pipe(
      Layer.provide(
        Layer.setConfigProvider(ConfigProvider.fromMap(fileValues)),
      ),
    );
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const { db } = yield* SqliteClient;
          const crypto = yield* TokenCrypto;
          db.query(
            "INSERT INTO executor_instance (organization_id, endpoint, token, updated_at) VALUES (?, ?, ?, ?)",
          ).run(
            "org-a",
            "https://executor.example",
            `mcpenc:v1:${legacyToken("legacy")}`,
            1,
          );
          yield* migrateTokenRows(db, {
            ...crypto,
            needsReencrypt: (value) => Buffer.from(value, "base64url")[0] === 1,
          });
          const row = db
            .query<{ token: string }, []>("SELECT token FROM executor_instance")
            .get();
          if (!row) throw new Error("Missing migrated token");
          const payload = row.token.slice("mcpenc:v1:".length);
          expect(Buffer.from(payload, "base64url")[0]).toBe(2);
          expect(
            yield* crypto.decrypt(
              payload,
              tokenContext("executor_instance", "org-a", "org-a", "token"),
            ),
          ).toBe("legacy");
        }).pipe(Effect.provide(Layer.mergeAll(sqlite, cipher))),
      ),
    );
  });

  it("rejects expired and long-lived worker access tokens", () => {
    for (const expiresAt of [
      0,
      Number.MAX_SAFE_INTEGER,
      Number.POSITIVE_INFINITY,
      Date.now() + 2 * 60 * 60_000,
    ])
      expect(() =>
        workerMcpCredential({ accessToken: "access", expiresAt }),
      ).toThrow();
    const token = {
      accessToken: "access",
      expiresAt: Date.now() + 600_000,
      refreshToken: "private",
      clientSecret: "private",
    };
    expect(workerMcpCredential(token)).toEqual({
      accessToken: "access",
      expiresAt: token.expiresAt,
    });
  });

  it("writes no refresh token or client secret to the worker agent database", async () => {
    const dir = await temporary();
    const agent = await materializeMcpAgentDb(dir, [
      {
        serverUrl: "https://mcp.example",
        accessToken: "access",
        expiresAt: Date.now() + 600_000,
        refreshToken: "refresh-secret",
        clientSecret: "client-secret",
        clientId: "client",
      },
    ]);
    const db = new Database(join(agent, "agent.db"));
    try {
      const row = db
        .query<{ data: string }, []>("SELECT data FROM auth_credentials")
        .get();
      expect(JSON.parse(row?.data ?? "null")).toEqual({
        access: "access",
        expires: expect.any(Number),
      });
    } finally {
      db.close();
    }
  });

  it("evicts rerun gates after success, failure, and interruption", async () => {
    const org = Schema.decodeUnknownSync(OrganizationId)("org");
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* withRerunIssueGate(org, "success", Effect.void);
        yield* Effect.either(
          withRerunIssueGate(org, "failure", Effect.fail("failed")),
        );
        const holder = yield* Effect.fork(
          withRerunIssueGate(org, "interrupt", Effect.never),
        );
        yield* Effect.yieldNow();
        const waiter = yield* Effect.fork(
          withRerunIssueGate(org, "interrupt", Effect.never),
        );
        yield* Effect.yieldNow();
        expect(rerunIssueGateCount()).toBe(1);
        yield* Fiber.interrupt(waiter);
        expect(rerunIssueGateCount()).toBe(1);
        yield* Fiber.interrupt(holder);
        expect(rerunIssueGateCount()).toBe(0);
      }),
    );
  });
});

it("upgrades v1 rows automatically when TokenCrypto starts with the gateway database", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const { db } = yield* SqliteClient;
        db.query(
          "INSERT INTO executor_instance (organization_id, endpoint, token, updated_at) VALUES (?, ?, ?, ?)",
        ).run(
          "org",
          "https://executor.example",
          `mcpenc:v1:${legacyToken("legacy")}`,
          1,
        );
        const layer = TokenCrypto.Default.pipe(
          Layer.provide(
            Layer.mergeAll(Layer.succeed(SqliteClient, { db }), configLayer()),
          ),
        );
        yield* Effect.gen(function* () {
          yield* TokenCrypto;
        }).pipe(Effect.provide(layer));
        const row = db
          .query<{ token: string }, []>("SELECT token FROM executor_instance")
          .get();
        expect(
          Buffer.from(
            row?.token.slice("mcpenc:v1:".length) ?? "",
            "base64url",
          )[0],
        ).toBe(2);
      }).pipe(Effect.provide(SqliteClientLive(":memory:"))),
    ),
  );
});
