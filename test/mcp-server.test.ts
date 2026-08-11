import { mkdir, mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Option } from "effect";
import { describe, expect } from "vitest";
import type {
  McpServerId,
  OrganizationId,
  WorkspaceId,
} from "../src/domain/ids.js";
import { McpServerRecord } from "../src/domain/models.js";
import {
  resolveEffectiveMcpServers,
  toOmpMcpConfig,
  writeOmpMcpConfig,
} from "../src/services/mcp-config.js";
import { McpServerRepo } from "../src/services/store/repositories.js";
import { SqliteClientLive } from "../src/services/store/sqlite-client.js";

const org = "org-mcp" as OrganizationId;
const repo = "repo-a" as WorkspaceId;
const serverId = (id: string) => id as McpServerId;

const withRepo = <A, E>(effect: Effect.Effect<A, E, McpServerRepo>) =>
  Effect.gen(function* () {
    const sqlite = SqliteClientLive(":memory:");
    const layer = McpServerRepo.Default.pipe(
      Layer.provide(
        Layer.mergeAll(
          sqlite,
          Layer.setConfigProvider(ConfigProvider.fromMap(new Map())),
        ),
      ),
    );
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
          args: ["server.js"],
          env: { TOKEN: "secret" },
          repositoryId: Option.none(),
          now: 1,
        });
        const scoped = yield* servers.createMcpServer({
          organizationId: org,
          id: serverId("scoped"),
          name: "repo-only",
          transport: "http",
          url: "https://mcp.example.test",
          repositoryId: Option.some(repo),
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
        const listed = yield* servers.listMcpServers(org);
        expect(listed).toHaveLength(3);
        const updated = yield* servers.updateMcpServer(org, serverId("wide"), {
          env: { TOKEN: "rotated" },
          enabled: false,
        });
        expect(updated.enabled).toBe(false);
        expect(yield* servers.deleteMcpServer(org, serverId("off"))).toBe(true);
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
      repositoryId: Option.none<WorkspaceId>(),
      env: {},
    };
    const disabled = {
      ...wide,
      id: serverId("disabled"),
      name: "disabled",
      enabled: false,
    };
    const effective = resolveEffectiveMcpServers(
      [wide, override, http, disabled],
      repo,
    );
    expect(effective.map((server) => server.name)).toEqual([
      "shared",
      "remote",
    ]);
    expect(toOmpMcpConfig(effective)).toEqual({
      mcpServers: {
        shared: {
          type: "stdio",
          command: "python",
          args: ["repo.py"],
          env: { TOKEN: "secret" },
        },
        remote: { type: "http", url: "https://mcp.example.test" },
      },
    });
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
});
