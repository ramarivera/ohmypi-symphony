import { Database } from "bun:sqlite";
import { createCipheriv, randomBytes } from "node:crypto";
import * as fs from "node:fs";
import {
  type FileHandle,
  link,
  mkdir,
  mkdtemp,
  open,
  readdir,
  realpath,
  rm,
  stat,
  symlink,
  writeFile,
} from "node:fs/promises";
import { createServer } from "node:net";
import { homedir } from "node:os";
import { join } from "node:path";
import { brotliCompressSync, deflateSync, gzipSync } from "node:zlib";
import {
  ConfigProvider,
  Effect,
  Either,
  Fiber,
  Layer,
  Logger,
  Schema,
} from "effect";
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
import { removeMcpConfig } from "../src/services/mcp-config.js";
import {
  materializeMcpAgentDb,
  removeMcpAgentDb,
} from "../src/services/mcp-oauth.js";
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
  prepareWorkerWorkspaceSync,
  workerEnvironment,
  workerIdentityCommand,
  workerMcpCredential,
} from "../src/services/worker-security.js";
import {
  isHttpsGitHubRepositoryUrl,
  persistGitHubExtraHeader,
  unsetGitHubExtraHeader,
} from "../src/services/workspace.js";
import {
  cleanupWorkspaceProcess,
  gitEnvironment,
  removeWorkerWorkspaceSync,
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
vi.mock("node:fs", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs")>();
  return { ...actual, fchownSync: vi.fn(actual.fchownSync) };
});
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
const withWorkspaceIpc = async (
  kind: "FIFO" | "socket",
  use: (workspace: string, path: string) => Promise<void>,
) => {
  const workspace = await temporary();
  const path = join(workspace, "ipc");
  const server = createServer();
  if (kind === "FIFO") {
    const result = Bun.spawnSync(["mkfifo", path]);
    expect(result.exitCode).toBe(0);
  } else {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(path, resolve);
    });
  }
  try {
    await use(workspace, path);
  } finally {
    if (server.listening)
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
  }
};
afterEach(async () => {
  await Promise.all(
    dirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })),
  );
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
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
  it("worker retention cleanup preserves its parent and external link targets", async () => {
    const dir = await temporary();
    const external = await temporary();
    const sentinel = join(external, "sentinel");
    await writeFile(sentinel, "keep");
    await symlink(external, join(dir, "directory-link"));
    await symlink(sentinel, join(dir, "file-link"));
    await link(sentinel, join(dir, "hardlink"));
    await workspaceProcess(dir, "remove-workspace-contents", null);
    expect((await stat(dir)).isDirectory()).toBe(true);
    expect(await readdir(dir)).toEqual([]);
    await expect(Bun.file(sentinel).text()).resolves.toBe("keep");
  });

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
    // A native unprivileged test can't chown the parent directory to a foreign
    // uid. Stub that syscall while retaining the real inode ownership/nlink.
    const handle = await open(workspace, "r");
    const prototype = Object.getPrototypeOf(handle) as FileHandle;
    await handle.close();
    const chown = vi.spyOn(prototype, "chown").mockResolvedValue(undefined);
    const foreignUid = (process.getuid?.() ?? 1000) + 1;
    await expect(
      prepareWorkerWorkspace(workspace, String(foreignUid)),
    ).rejects.toThrow("Multiply-linked");
    expect(chown).toHaveBeenCalledExactlyOnceWith(foreignUid, foreignUid);
    expect((await stat(outside)).uid).toBe(before.uid);
    expect(await Bun.file(outside).text()).toBe("gateway");
  });

  it("rejects hardlinks in the synchronous retention walk too", async () => {
    const dir = await temporary();
    const workspace = join(dir, "workspace");
    await mkdir(workspace);
    const outside = join(dir, "outside");
    await writeFile(outside, "gateway", { mode: 0o400 });
    await link(outside, join(workspace, "linked"));
    const before = await stat(outside);
    const chown = vi.spyOn(fs, "fchownSync").mockReturnValue(undefined);
    const foreignUid = (process.getuid?.() ?? 1000) + 1;
    expect(() =>
      prepareWorkerWorkspaceSync(workspace, String(foreignUid)),
    ).toThrow("Multiply-linked");
    expect(chown).toHaveBeenCalledExactlyOnceWith(
      expect.any(Number),
      foreignUid,
      foreignUid,
    );
    expect((await stat(outside)).uid).toBe(before.uid);
  });

  it("resumes with already-owned hardlinked tool state without changing ownership", async () => {
    const dir = await temporary();
    const workspace = join(dir, "workspace");
    await mkdir(join(workspace, ".omp-home"), { recursive: true });
    await writeFile(join(workspace, ".omp-home", "cache"), "tool-state");
    await link(
      join(workspace, ".omp-home", "cache"),
      join(workspace, "dependency"),
    );
    const handle = await open(workspace, "r");
    const prototype = Object.getPrototypeOf(handle) as FileHandle;
    await handle.close();
    vi.spyOn(prototype, "chown").mockRejectedValue(
      new Error("Unexpected ownership change"),
    );
    for (let attempt = 0; attempt < 2; attempt++)
      await prepareWorkerWorkspace(workspace, String(process.getuid?.()));
    expect(await Bun.file(join(workspace, "dependency")).text()).toBe(
      "tool-state",
    );
    expect((await stat(join(workspace, "dependency"))).nlink).toBe(2);
  });

  it.each(["FIFO", "socket"] as const)(
    "accepts a worker-owned %s in both ownership walks without chown",
    async (kind) => {
      await withWorkspaceIpc(kind, async (workspace, path) => {
        const handle = await open(workspace, "r");
        const prototype = Object.getPrototypeOf(handle) as FileHandle;
        await handle.close();
        const chown = vi.spyOn(prototype, "chown");
        const chownSync = vi.spyOn(fs, "fchownSync");
        const uid = String(process.getuid?.());
        await prepareWorkerWorkspace(workspace, uid);
        prepareWorkerWorkspaceSync(workspace, uid);
        expect(chown).not.toHaveBeenCalled();
        expect(chownSync).not.toHaveBeenCalled();
        const node = await stat(path);
        expect(kind === "FIFO" ? node.isFIFO() : node.isSocket()).toBe(true);
        expect(node.uid).toBe(process.getuid?.());
      });
    },
  );

  it.each(["FIFO", "socket"] as const)(
    "rejects a foreign-owned %s in both ownership walks without chowning it",
    async (kind) => {
      await withWorkspaceIpc(kind, async (workspace, path) => {
        const handle = await open(workspace, "r");
        const prototype = Object.getPrototypeOf(handle) as FileHandle;
        await handle.close();
        // Keep the real foreign inode metadata; only fake migrating the parent.
        const chown = vi.spyOn(prototype, "chown").mockResolvedValue(undefined);
        const chownSync = vi.spyOn(fs, "fchownSync").mockReturnValue(undefined);
        const uid = (process.getuid?.() ?? 1000) + 1;
        await expect(
          prepareWorkerWorkspace(workspace, String(uid)),
        ).rejects.toThrow("Foreign-owned workspace FIFO or socket");
        expect(() =>
          prepareWorkerWorkspaceSync(workspace, String(uid)),
        ).toThrow("Foreign-owned workspace FIFO or socket");
        expect(chown).toHaveBeenCalledExactlyOnceWith(uid, uid);
        expect(chownSync).toHaveBeenCalledExactlyOnceWith(
          expect.any(Number),
          uid,
          uid,
        );
        expect((await stat(path)).uid).toBe(process.getuid?.());
      });
    },
  );

  it("rejects retention paths outside the root and symlinked workspace paths before migration", async () => {
    const root = await temporary();
    const outside = await temporary();
    const alias = join(root, "linked");
    await symlink(outside, alias);
    vi.stubEnv("WORKER_UID", String((process.getuid?.() ?? 1000) + 1));
    const chown = vi.spyOn(fs, "fchownSync");
    const spawn = vi.spyOn(Bun, "spawnSync");
    for (const path of [root, outside, alias])
      expect(() => removeWorkerWorkspaceSync(path, root)).toThrow(
        "inside the workspace root",
      );
    expect(chown).not.toHaveBeenCalled();
    expect(spawn).not.toHaveBeenCalled();
  });

  it("runs the materializer entry point and excludes writable tool state", async () => {
    const dir = await temporary();
    const initialized = Bun.spawn(["git", "init", dir], {
      stdout: "ignore",
      stderr: "pipe",
    });
    expect(await initialized.exited).toBe(0);
    const exclude = join(dir, ".git", "info", "exclude");
    const original = "# user exclusions\n/custom/\n/.omp-home/";
    await writeFile(exclude, original);
    await workspaceProcess(dir, "tool-directories", null);
    const first = await Bun.file(exclude).text();
    expect(first).toBe(`${original}\n/.omp-tmp/\n`);
    for (let attempt = 0; attempt < 3; attempt++)
      await workspaceProcess(dir, "tool-directories", null);
    expect(await Bun.file(exclude).text()).toBe(first);
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
      GIT_ASKPASS: "",
      GIT_TERMINAL_PROMPT: "0",
    });
  });

  it("fails Git authentication without launching a configured askpass helper", async () => {
    const dir = await temporary();
    const marker = join(dir, "askpass-invoked");
    const helper = join(dir, "askpass");
    const config = join(dir, "gitconfig");
    await writeFile(
      helper,
      `#!/bin/sh\necho invoked >> '${marker}'\necho credential\n`,
      { mode: 0o700 },
    );
    await writeFile(config, `[core]\naskPass = ${helper}\n`);
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: () =>
        new Response(null, {
          status: 401,
          headers: { "WWW-Authenticate": 'Basic realm="test"' },
        }),
    });
    try {
      const child = Bun.spawn(
        [
          "git",
          "-c",
          "credential.helper=",
          "ls-remote",
          `http://127.0.0.1:${server.port}/repo`,
        ],
        {
          env: gitEnvironment({
            ...process.env,
            HOME: dir,
            GIT_CONFIG_GLOBAL: config,
            GIT_ASKPASS: helper,
          }),
          stdout: "ignore",
          stderr: "pipe",
        },
      );
      const stderr = await new Response(child.stderr).text();
      expect(await child.exited).not.toBe(0);
      expect(stderr).toContain("terminal prompts disabled");
      expect(await Bun.file(marker).exists()).toBe(false);
    } finally {
      await server.stop(true);
    }
  });

  it("warns with context when delegated credential cleanup fails, while preserving best effort", async () => {
    const dir = await temporary();
    // A missing cwd fails the real delegated spawn regardless of runner uid.
    const missing = join(dir, "missing");
    vi.stubEnv("WORKER_UID", String((process.getuid?.() ?? 1000) + 1));
    const logs: Array<{
      message: unknown;
      annotations: Record<string, unknown>;
    }> = [];
    const logger = Logger.make(({ message, annotations }) => {
      logs.push({ message, annotations: Object.fromEntries(annotations) });
    });
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* unsetGitHubExtraHeader(missing, "cleanup-session");
        yield* removeMcpConfig(missing);
        yield* removeMcpAgentDb(missing);
      }).pipe(Effect.provide(Logger.replace(Logger.defaultLogger, logger))),
    );
    expect(logs.map((entry) => entry.message)).toEqual([
      ["workspace.github_cleanup_failed"],
      ["mcp.config.cleanup_failed"],
      ["mcp.agent_db.cleanup_failed"],
    ]);
    expect(logs[0]?.annotations.sessionId).toBe("cleanup-session");
    expect(logs.map((entry) => entry.annotations.action)).toEqual([
      "clear-git",
      "remove-mcp-config",
      "remove-agent-db",
    ]);
    for (const entry of logs) {
      expect(entry.annotations.cwd).toBe(missing);
      expect(entry.annotations.error).toEqual(expect.stringMatching(/.+/u));
    }
  });

  it("reports worker-side credential deletion failures over IPC and tolerates missing files", async () => {
    const dir = await temporary();
    vi.stubEnv("WORKER_UID", "");
    await mkdir(join(dir, ".git"));
    await writeFile(join(dir, ".git", "config"), "[unterminated\n");
    await mkdir(join(dir, "mcp.json"));
    await mkdir(join(dir, ".omp-gateway", "agent.db"), { recursive: true });
    const logs: Array<{
      message: unknown;
      annotations: Record<string, unknown>;
    }> = [];
    const logger = Logger.make(({ message, annotations }) => {
      logs.push({ message, annotations: Object.fromEntries(annotations) });
    });
    for (const action of [
      "clear-git",
      "remove-mcp-config",
      "remove-agent-db",
    ]) {
      await expect(workspaceProcess(dir, action, null)).rejects.toThrow(
        `Workspace ${action} failed`,
      );
      await Effect.runPromise(
        cleanupWorkspaceProcess(dir, action, "cleanup.failed").pipe(
          Effect.provide(Logger.replace(Logger.defaultLogger, logger)),
        ),
      );
    }
    expect(logs).toHaveLength(3);
    expect(logs[0]?.annotations.error).toContain("bad config line");
    expect(logs[1]?.annotations.error).toContain("mcp.json");
    expect(logs[2]?.annotations.error).toContain("agent.db");
    await rm(join(dir, "mcp.json"), { recursive: true });
    await rm(join(dir, ".omp-gateway"), { recursive: true });
    await expect(
      workspaceProcess(dir, "remove-mcp-config", null),
    ).resolves.toBe("");
    await expect(workspaceProcess(dir, "remove-agent-db", null)).resolves.toBe(
      "",
    );
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

  it.each([
    ["gzip", gzipSync],
    ["x-gzip", gzipSync],
    ["deflate", deflateSync],
    ["br", brotliCompressSync],
  ] as const)("decodes %s OAuth JSON responses", async (encoding, compress) => {
    const metadata = { token_endpoint: "https://issuer.example/token" };
    const compressed = compress(JSON.stringify(metadata));
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: (request) => {
        if (request.headers.get("x-check-encoding") === "true")
          expect(request.headers.get("accept-encoding")).toBe(
            "gzip, deflate, br",
          );
        return new Response(compressed, {
          headers: {
            "content-type": "application/json",
            "content-encoding": encoding,
            "content-length": String(compressed.byteLength),
          },
        });
      },
    });
    try {
      const url = `http://127.0.0.1:${server.port}/metadata`;
      if (encoding === "gzip")
        expect(await (await fetch(url)).json()).toEqual(metadata);
      const response = await safeOAuthFetch(
        url,
        {
          headers: {
            "accept-encoding": "unsupported",
            "x-check-encoding": "true",
          },
        },
        { allowLoopbackHttp: true },
      );
      expect(await response.json()).toEqual(metadata);
      expect(response.headers.get("content-type")).toBe("application/json");
      expect(response.headers.has("content-encoding")).toBe(false);
      expect(response.headers.has("content-length")).toBe(false);
    } finally {
      await server.stop(true);
    }
  });

  it("rejects a decompression bomb and cancels the unfinished upstream body", async () => {
    const compressed = gzipSync("x".repeat(MCP_MAX_RESPONSE_BYTES + 1));
    expect(compressed.byteLength).toBeLessThan(MCP_MAX_RESPONSE_BYTES);
    let cancelled = false;
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: () =>
        new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(compressed);
              // Leave the response open to prove overflow stops upstream input.
            },
            cancel() {
              cancelled = true;
            },
          }),
          { headers: { "content-encoding": "gzip" } },
        ),
    });
    try {
      await expect(
        safeOAuthFetch(
          `http://127.0.0.1:${server.port}/metadata`,
          {},
          { allowLoopbackHttp: true },
        ),
      ).rejects.toBeInstanceOf(BodyTooLargeError);
      await vi.waitFor(() => expect(cancelled).toBe(true));
    } finally {
      await server.stop(true);
    }
  });

  it.each(["zstd", "gzip, br", "identity, gzip"])(
    "rejects unsupported or stacked OAuth content encoding %s",
    async (encoding) => {
      const server = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        fetch: () =>
          new Response('{"ok":true}', {
            headers: { "content-encoding": encoding },
          }),
      });
      try {
        await expect(
          safeOAuthFetch(
            `http://127.0.0.1:${server.port}/metadata`,
            {},
            { allowLoopbackHttp: true },
          ),
        ).rejects.toThrow("Unsupported OAuth content encoding");
      } finally {
        await server.stop(true);
      }
    },
  );

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

  it("strips all renewal material before serializing the agent-db worker payload", async () => {
    const dir = await temporary();
    vi.stubEnv("WORKER_UID", String((process.getuid?.() ?? 1000) + 1));
    const spawn = vi.spyOn(Bun, "spawn").mockImplementation(() => {
      throw new Error("Worker payload intercepted");
    });
    const credential = {
      serverUrl: "https://mcp.example",
      accessToken: "access",
      expiresAt: Date.now() + 600_000,
      refreshToken: "refresh-secret",
      tokenEndpoint: "https://issuer.example/token",
      clientId: "client",
      clientSecret: "client-secret",
    };
    await expect(materializeMcpAgentDb(dir, [credential])).rejects.toThrow(
      "Worker payload intercepted",
    );
    expect(spawn).toHaveBeenCalledOnce();
    const options = spawn.mock.calls[0]?.[1];
    if (!(options?.stdin instanceof Response))
      throw new Error("Missing serialized worker input");
    expect(await options.stdin.json()).toEqual([
      {
        serverUrl: credential.serverUrl,
        accessToken: credential.accessToken,
        expiresAt: credential.expiresAt,
      },
    ]);
  });

  it("defensively filters credentials received by the real workspace worker", async () => {
    const dir = await temporary();
    vi.stubEnv("WORKER_UID", "");
    const credential = {
      serverUrl: "https://mcp.example",
      accessToken: "access",
      expiresAt: Date.now() + 600_000,
      refreshToken: "refresh-secret",
      tokenEndpoint: "https://issuer.example/token",
      clientId: "client",
      clientSecret: "client-secret",
    };
    const agent = await workspaceProcess(dir, "agent-db", [credential]);
    const db = new Database(join(agent, "agent.db"));
    try {
      const rows = db
        .query<{ data: string }, []>("SELECT data FROM auth_credentials")
        .all();
      expect(rows.map((row) => JSON.parse(row.data))).toEqual([
        { access: credential.accessToken, expires: credential.expiresAt },
      ]);
    } finally {
      db.close();
    }
    // Bypass the gateway and prove the worker enforces workerMcpCredential too.
    await expect(
      workspaceProcess(dir, "agent-db", [{ ...credential, expiresAt: 0 }]),
    ).rejects.toThrow("MCP worker access token must expire within one hour");
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
