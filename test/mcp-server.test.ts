import { mkdir, mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { it } from "@effect/vitest";
import {
  ConfigProvider,
  Effect,
  Either,
  HashMap,
  Layer,
  Logger,
  Option,
} from "effect";
import { describe, expect } from "vitest";
import type {
  McpServerId,
  OrganizationId,
  WorkspaceId,
} from "../src/domain/ids.js";
import { McpServerRecord } from "../src/domain/models.js";
import {
  removeMcpConfig,
  resolveEffectiveMcpServers,
  toOmpMcpConfig,
  writeOmpMcpConfig,
} from "../src/services/mcp-config.js";
import { McpServerRepo } from "../src/services/store/repositories.js";
import {
  SqliteClient,
  SqliteClientLive,
} from "../src/services/store/sqlite-client.js";
import { TokenCrypto } from "../src/services/token-crypto.js";

const org = "org-mcp" as OrganizationId;
const repo = "repo-a" as WorkspaceId;
const serverId = (id: string) => id as McpServerId;

const withRepo = <A, E>(
  effect: Effect.Effect<A, E, McpServerRepo | SqliteClient>,
) =>
  Effect.gen(function* () {
    const sqlite = SqliteClientLive(":memory:");
    const token = TokenCrypto.Default.pipe(
      Layer.provide(
        Layer.setConfigProvider(
          ConfigProvider.fromMap(
            new Map([
              [
                "TOKEN_ENCRYPTION_KEY",
                Buffer.from(new Uint8Array(32).fill(0x42)).toString("base64"),
              ],
            ]),
          ),
        ),
      ),
    );
    const repoLayer = McpServerRepo.Default.pipe(
      Layer.provide(Layer.mergeAll(sqlite, token)),
    );
    const layer = Layer.mergeAll(sqlite, token, repoLayer);
    return yield* effect.pipe(Effect.provide(layer));
  });

describe("MCP server storage and worker config", () => {
  it.scopedLive("round-trips CRUD and scopes servers", () =>
    withRepo(
      Effect.gen(function* () {
        const servers = yield* McpServerRepo;
        const wide = yield* servers.createMcpServer({
          organizationId: org,
          id: serverId("wide"),
          name: "shared",
          transport: "stdio",
          command: "node",
          args: ["server.js", "--filter=a,b", '{"query":"x,y"}'],
          env: { TOKEN: "secret" },
          headers: { Authorization: "header-secret" },
          repositoryId: Option.none(),
          now: 1,
        });
        const { db } = yield* SqliteClient;
        const stored = db
          .query<{ readonly env_json: string }, [string, string]>(
            "SELECT env_json FROM mcp_server WHERE organization_id=? AND id=?",
          )
          .get(org, serverId("wide"));
        expect(stored?.env_json).toMatch(/"TOKEN":"mcpenc:v1:[A-Za-z0-9_-]+"/u);
        const storedArgs = db
          .query<{ readonly args_json: string }, [string, string]>(
            "SELECT args_json FROM mcp_server WHERE organization_id=? AND id=?",
          )
          .get(org, serverId("wide"));
        expect(storedArgs?.args_json).toBe(
          JSON.stringify(["server.js", "--filter=a,b", '{"query":"x,y"}']),
        );
        expect(stored?.env_json).not.toContain("secret");
        const storedHeaders = db
          .query<{ readonly headers_json: string }, [string, string]>(
            "SELECT headers_json FROM mcp_server WHERE organization_id=? AND id=?",
          )
          .get(org, serverId("wide"));
        expect(storedHeaders?.headers_json).toMatch(
          /"Authorization":"mcpenc:v1:[A-Za-z0-9_-]+"/u,
        );
        expect(storedHeaders?.headers_json).not.toContain("header-secret");
        const scoped = yield* servers.createMcpServer({
          organizationId: org,
          id: serverId("scoped"),
          name: "repo-only",
          transport: "http",
          url: "https://mcp.example.test",
          repositoryId: Option.some(repo),
          headers: { "X-Scoped": "scoped-secret" },
          now: 2,
        });
        yield* servers.createMcpServer({
          organizationId: org,
          id: serverId("off"),
          name: "disabled",
          transport: "sse",
          url: "https://disabled.example.test",
          enabled: false,
          now: 3,
        });
        expect(wide.env.TOKEN).toBe("secret");
        expect(Option.isSome(scoped.repositoryId)).toBe(true);
        expect(scoped.headers["X-Scoped"]).toBe("scoped-secret");
        const listed = yield* servers.listMcpServers(org);
        expect(listed).toHaveLength(3);
        const updated = yield* servers.updateMcpServer(org, serverId("wide"), {
          env: { TOKEN: "rotated" },
          enabled: false,
        });
        expect(updated.enabled).toBe(false);
        const oauthServer = yield* servers.createMcpServer({
          organizationId: org,
          id: serverId("oauth"),
          name: "oauth",
          transport: "http",
          url: "https://oauth.example.test",
          oauthClient: {
            clientId: "client",
            clientSecret: "secret",
          },
          now: 4,
        });
        expect(oauthServer.oauthClient?.clientId).toBe("client");
        yield* servers.updateMcpServer(org, serverId("oauth"), {
          oauthClient: null,
        });
        const cleared = yield* servers.getMcpServer(org, serverId("oauth"));
        expect(Option.isSome(cleared) && cleared.value.oauthClient).toBeNull();
        expect(yield* servers.deleteMcpServer(org, serverId("off"))).toBe(true);
      }),
    ),
  );
  it.scopedLive(
    "preserves legacy plaintext and rejects tampered envelopes",
    () =>
      withRepo(
        Effect.gen(function* () {
          const servers = yield* McpServerRepo;
          const { db } = yield* SqliteClient;
          db.query(
            `INSERT INTO mcp_server
            (organization_id, id, name, transport, command, args_json, url, env_json, repository_id, enabled, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          ).run(
            org,
            serverId("legacy"),
            "legacy",
            "stdio",
            "node",
            "[]",
            null,
            JSON.stringify({ TOKEN: "AQlegacy" }),
            null,
            1,
            1,
            1,
          );
          const legacy = yield* servers.getMcpServer(org, serverId("legacy"));
          expect(Option.isSome(legacy)).toBe(true);
          expect(Option.isSome(legacy) ? legacy.value.env.TOKEN : null).toBe(
            "AQlegacy",
          );

          yield* servers.createMcpServer({
            organizationId: org,
            id: serverId("tampered"),
            name: "tampered",
            transport: "stdio",
            command: "node",
            env: { TOKEN: "secret" },
            repositoryId: Option.none(),
          });
          const stored = db
            .query<{ readonly env_json: string }, [string, string]>(
              "SELECT env_json FROM mcp_server WHERE organization_id=? AND id=?",
            )
            .get(org, serverId("tampered"));
          if (stored === null) throw new Error("tampered row missing");
          const env = JSON.parse(stored.env_json) as Record<string, string>;
          const ciphertext = env.TOKEN;
          if (ciphertext === undefined) throw new Error("TOKEN env missing");
          const payload = new Uint8Array(
            Buffer.from(ciphertext.slice("mcpenc:v1:".length), "base64url"),
          );
          const last = payload.length - 1;
          const byte = payload[last];
          if (byte === undefined) throw new Error("ciphertext is empty");
          payload[last] = byte ^ 0xff;
          env.TOKEN = `mcpenc:v1:${Buffer.from(payload).toString("base64url")}`;
          db.query(
            "UPDATE mcp_server SET env_json=? WHERE organization_id=? AND id=?",
          ).run(JSON.stringify(env), org, serverId("tampered"));
          const tampered = yield* Effect.either(servers.listMcpServers(org));
          expect(Either.isLeft(tampered)).toBe(true);
        }),
      ),
  );
  it.scopedLive("rejects duplicate MCP names within one scope", () =>
    withRepo(
      Effect.gen(function* () {
        const servers = yield* McpServerRepo;
        yield* servers.createMcpServer({
          organizationId: org,
          id: serverId("wide"),
          name: "duplicate",
          transport: "stdio",
          command: "node",
          repositoryId: Option.none(),
        });
        const duplicate = yield* Effect.either(
          servers.createMcpServer({
            organizationId: org,
            id: serverId("wide-2"),
            name: "duplicate",
            transport: "stdio",
            command: "node",
            repositoryId: Option.none(),
          }),
        );
        expect(Either.isLeft(duplicate)).toBe(true);
        const concurrent = yield* Effect.either(
          Effect.all(
            [
              servers.createMcpServer({
                organizationId: org,
                id: serverId("race-1"),
                name: "race",
                transport: "stdio",
                command: "node",
                repositoryId: Option.some(repo),
              }),
              servers.createMcpServer({
                organizationId: org,
                id: serverId("race-2"),
                name: "race",
                transport: "stdio",
                command: "node",
                repositoryId: Option.some(repo),
              }),
            ],
            { concurrency: 2 },
          ),
        );
        expect(Either.isLeft(concurrent)).toBe(true);

        yield* servers.createMcpServer({
          organizationId: org,
          id: serverId("repo-1"),
          name: "repo-name",
          transport: "stdio",
          command: "node",
          repositoryId: Option.some(repo),
        });
        const collisionOnUpdate = yield* Effect.either(
          servers.updateMcpServer(org, serverId("repo-1"), {
            name: "duplicate",
            repositoryId: Option.none(),
          }),
        );
        expect(Either.isLeft(collisionOnUpdate)).toBe(true);
      }),
    ),
  );

  it("filters effective servers and emits omp mcp.json shape", () => {
    const wide = {
      id: serverId("wide"),
      organizationId: org,
      name: "shared",
      transport: "stdio" as const,
      command: Option.some("node"),
      args: ["wide.js"],
      url: Option.none<string>(),
      env: { TOKEN: "secret" },
      headers: { "X-Stdio": "stdio-secret" },
      repositoryId: Option.none<WorkspaceId>(),
      enabled: true,
      createdAt: 1,
      updatedAt: 1,
    };
    const override = {
      ...wide,
      id: serverId("override"),
      command: Option.some("python"),
      args: ["repo.py"],
      repositoryId: Option.some(repo),
    };
    const http = {
      ...wide,
      id: serverId("http"),
      name: "remote",
      transport: "http" as const,
      command: Option.none<string>(),
      args: [],
      url: Option.some("https://mcp.example.test"),
      headers: { Authorization: "remote-secret" },
      repositoryId: Option.none<WorkspaceId>(),
      env: {},
    };
    const sse = {
      ...http,
      id: serverId("sse"),
      name: "events",
      transport: "sse" as const,
      url: Option.some("https://sse.example.test"),
      headers: { "X-SSE": "sse-secret" },
    };
    const disabled = {
      ...wide,
      id: serverId("disabled"),
      name: "disabled",
      enabled: false,
    };
    const effective = resolveEffectiveMcpServers(
      [wide, override, http, sse, disabled],
      repo,
    );
    expect(effective.map((server) => server.name)).toEqual([
      "shared",
      "remote",
      "events",
    ]);
    expect(toOmpMcpConfig(effective)).toEqual({
      mcpServers: {
        shared: {
          type: "stdio",
          command: "python",
          args: ["repo.py"],
          env: { TOKEN: "secret" },
        },
        remote: {
          type: "http",
          url: "https://mcp.example.test",
          headers: { Authorization: "remote-secret" },
        },
        events: {
          type: "sse",
          url: "https://sse.example.test",
          headers: { "X-SSE": "sse-secret" },
        },
      },
    });
    const disabledOverride = {
      ...override,
      id: serverId("disabled-override"),
      enabled: false,
    };
    expect(resolveEffectiveMcpServers([wide, disabledOverride], repo)).toEqual(
      [],
    );
    expect(
      resolveEffectiveMcpServers(
        [wide, { ...override, enabled: true }],
        repo,
      )[0]?.id,
    ).toBe(override.id);
    expect(McpServerRecord).toBeDefined();
  });

  it("writes mcp.json at 0600 and git-excludes it from commits", async () => {
    const root = await mkdtemp(join("/tmp", "mcp-writer-"));
    await mkdir(join(root, ".git"), { recursive: true });
    await writeFile(join(root, ".git", "HEAD"), "ref: refs/heads/main\n");

    const record = {
      ...McpServerRecord.make({
        id: serverId("wide"),
        organizationId: org,
        name: "shared",
        transport: "stdio" as const,
        command: Option.some("python"),
        args: ["server.py"],
        url: Option.none<string>(),
        env: { TOKEN: "secret" },
        headers: {},
        repositoryId: Option.none<WorkspaceId>(),
        enabled: true,
        createdAt: 0,
        updatedAt: 0,
      }),
    };

    await Effect.runPromise(writeOmpMcpConfig(root, [record]));

    const contents = JSON.parse(await readFile(join(root, "mcp.json"), "utf8"));
    expect(contents.mcpServers.shared.command).toBe("python");
    expect((await stat(join(root, "mcp.json"))).mode & 0o777).toBe(0o600);
    const exclude = await readFile(
      join(root, ".git", "info", "exclude"),
      "utf8",
    );
    expect(exclude.split("\n")).toContain("mcp.json");
    // Rewrites stay idempotent (no duplicate exclude lines).
    await Effect.runPromise(writeOmpMcpConfig(root, [record]));
    const rewritten = await readFile(
      join(root, ".git", "info", "exclude"),
      "utf8",
    );
    expect(
      rewritten.split("\n").filter((line) => line === "mcp.json"),
    ).toHaveLength(1);

    const bare = await mkdtemp(join("/tmp", "mcp-writer-bare-"));
    await Effect.runPromise(writeOmpMcpConfig(bare, []));
    expect(JSON.parse(await readFile(join(bare, "mcp.json"), "utf8"))).toEqual({
      mcpServers: {},
    });
  });
  it("appends mcp.json on a new line in an existing git exclude", async () => {
    const root = await mkdtemp(join("/tmp", "mcp-writer-exclude-"));
    await mkdir(join(root, ".git", "info"), { recursive: true });
    await writeFile(join(root, ".git", "HEAD"), "ref: refs/heads/main\n");
    await writeFile(join(root, ".git", "info", "exclude"), "foo");

    await Effect.runPromise(writeOmpMcpConfig(root, []));

    expect(await readFile(join(root, ".git", "info", "exclude"), "utf8")).toBe(
      "foo\nmcp.json\n",
    );
  });

  it("skips tracked mcp.json and logs the session warning", async () => {
    const root = await mkdtemp(join("/tmp", "mcp-writer-tracked-"));
    const original = '{"keep":"repo-content"}\n';
    await writeFile(join(root, "mcp.json"), original);
    const init = Bun.spawn(["git", "init", "-q"], {
      cwd: root,
      stdout: "ignore",
      stderr: "ignore",
    });
    expect(await init.exited).toBe(0);
    const add = Bun.spawn(["git", "add", "mcp.json"], {
      cwd: root,
      stdout: "ignore",
      stderr: "ignore",
    });
    expect(await add.exited).toBe(0);
    const excludePath = join(root, ".git", "info", "exclude");
    const excludeBefore = await readFile(excludePath, "utf8");
    const logs: Array<{
      readonly message: string;
      readonly event: unknown;
      readonly sessionId: unknown;
    }> = [];
    const logger = Logger.make(({ message, annotations }) => {
      logs.push({
        message: String(message),
        event: Option.getOrNull(HashMap.get(annotations, "event")),
        sessionId: Option.getOrNull(HashMap.get(annotations, "sessionId")),
      });
    });

    await Effect.runPromise(
      writeOmpMcpConfig(root, [], "tracked-session").pipe(
        Effect.provide(Logger.replace(Logger.defaultLogger, logger)),
      ),
    );

    expect(
      logs.some(
        (entry) =>
          entry.message === "mcp.config.tracked_skip" &&
          entry.event === "mcp.config.tracked_skip" &&
          entry.sessionId === "tracked-session",
      ),
    ).toBe(true);
    expect(await readFile(join(root, "mcp.json"), "utf8")).toBe(original);
    expect(await readFile(excludePath, "utf8")).toBe(excludeBefore);
    await Effect.runPromise(removeMcpConfig(root));
    expect(await readFile(join(root, "mcp.json"), "utf8")).toBe(original);

    const untracked = await mkdtemp(join("/tmp", "mcp-writer-cleanup-"));
    await writeFile(join(untracked, "mcp.json"), "generated");
    await Effect.runPromise(removeMcpConfig(untracked));
    await expect(stat(join(untracked, "mcp.json"))).rejects.toThrow();
  });
  it("preserves foreign untracked configs and overwrites gateway configs", async () => {
    const foreignRoot = await mkdtemp(join("/tmp", "mcp-writer-foreign-"));
    const foreign = '{"other":"keep"}\n';
    await writeFile(join(foreignRoot, "mcp.json"), foreign);
    const logs: Array<{ readonly message: string; readonly event: unknown }> =
      [];
    const logger = Logger.make(({ message, annotations }) => {
      logs.push({
        message: String(message),
        event: Option.getOrNull(HashMap.get(annotations, "event")),
      });
    });
    await Effect.runPromise(
      writeOmpMcpConfig(foreignRoot, [], "foreign-session").pipe(
        Effect.provide(Logger.replace(Logger.defaultLogger, logger)),
      ),
    );
    expect(await readFile(join(foreignRoot, "mcp.json"), "utf8")).toBe(foreign);
    expect(
      logs.some(
        (entry) =>
          entry.message === "mcp.config.foreign_skip" &&
          entry.event === "mcp.config.foreign_skip",
      ),
    ).toBe(true);

    const gatewayRoot = await mkdtemp(join("/tmp", "mcp-writer-gateway-"));
    await writeFile(
      join(gatewayRoot, "mcp.json"),
      '{"mcpServers":{"old":{"type":"stdio"}}}\n',
    );
    await Effect.runPromise(writeOmpMcpConfig(gatewayRoot, []));
    expect(
      JSON.parse(await readFile(join(gatewayRoot, "mcp.json"), "utf8")),
    ).toEqual({ mcpServers: {} });
  });
});
