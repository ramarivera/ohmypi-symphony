import { chmod, mkdir, unlink, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { Effect, Option } from "effect";
import type { McpServerRecord } from "../domain/models.js";

const isTrackedMcpConfig = async (cwd: string): Promise<boolean> => {
  try {
    const process = Bun.spawn(
      ["git", "ls-files", "--error-unmatch", "mcp.json"],
      {
        cwd,
        stdout: "ignore",
        stderr: "ignore",
      },
    );
    return (await process.exited) === 0;
  } catch {
    return false;
  }
};

const isGatewayMcpConfig = (value: unknown): boolean => {
  if (
    typeof value !== "object" ||
    value === null ||
    Array.isArray(value) ||
    !("mcpServers" in value)
  ) {
    return false;
  }
  const mcpServers = value.mcpServers;
  return (
    typeof mcpServers === "object" &&
    mcpServers !== null &&
    !Array.isArray(mcpServers)
  );
};

/**
 * Best-effort removal of a generated mcp.json. Repository-tracked files are
 * never removed: only the generated, untracked workspace copy is eligible.
 */
export const removeMcpConfig = (
  cwd: string,
): Effect.Effect<void, never, never> =>
  Effect.promise(async () => {
    if (await isTrackedMcpConfig(cwd)) return;
    try {
      await unlink(join(cwd, "mcp.json"));
    } catch {
      // Cleanup is deliberately best effort: a missing or locked config must
      // not mask the terminal state of the run.
    }
  });

/**
 * Resolve the servers visible to one worker. Repository-scoped entries have
 * precedence over installation-wide entries with the same MCP name; disabled
 * entries never reach the generated config.
 */
export const resolveEffectiveMcpServers = (
  servers: ReadonlyArray<McpServerRecord>,
  repositoryId: string | null,
): ReadonlyArray<McpServerRecord> => {
  const effective: McpServerRecord[] = [];
  const positions = new Map<string, number>();
  for (const server of servers) {
    const scope = Option.match(server.repositoryId, {
      onNone: () => null,
      onSome: (id) => id,
    });
    if (scope !== null && scope !== repositoryId) continue;
    const position = positions.get(server.name);
    if (position === undefined) {
      positions.set(server.name, effective.length);
      effective.push(server);
    } else {
      const existing = effective[position];
      if (
        scope !== null &&
        existing !== undefined &&
        Option.isNone(existing.repositoryId)
      ) {
        effective[position] = server;
      }
    }
  }
  return effective.filter((server) => server.enabled);
};

export const toOmpMcpConfig = (servers: ReadonlyArray<McpServerRecord>) => ({
  mcpServers: Object.fromEntries(
    servers.map((server) => {
      const headers = server.headers ?? {};
      const config: Record<string, unknown> = {
        type: server.transport,
      };
      if (server.transport === "stdio") {
        config.command = Option.getOrThrow(server.command);
        config.args = [...server.args];
      } else {
        config.url = Option.getOrThrow(server.url);
        if (Object.keys(headers).length > 0) {
          config.headers = { ...headers };
        }
      }
      if (Object.keys(server.env).length > 0) config.env = { ...server.env };
      return [server.name, config];
    }),
  ),
});
export const writeOmpMcpConfig = (
  cwd: string,
  servers: ReadonlyArray<McpServerRecord>,
  sessionId?: string,
): Effect.Effect<void, Error> => {
  const contents = `${JSON.stringify(toOmpMcpConfig(servers), null, 2)}\n`;
  return Effect.gen(function* () {
    if (yield* Effect.promise(() => isTrackedMcpConfig(cwd))) {
      yield* Effect.logWarning("mcp.config.tracked_skip").pipe(
        Effect.annotateLogs({
          event: "mcp.config.tracked_skip",
          ...(sessionId === undefined ? {} : { sessionId }),
        }),
      );
      return;
    }
    const path = join(cwd, "mcp.json");
    if (yield* Effect.promise(() => Bun.file(path).exists())) {
      const gatewayShape = yield* Effect.promise(async () => {
        try {
          return isGatewayMcpConfig(await Bun.file(path).json());
        } catch {
          return false;
        }
      });
      if (!gatewayShape) {
        yield* Effect.logWarning("mcp.config.foreign_skip").pipe(
          Effect.annotateLogs({
            event: "mcp.config.foreign_skip",
            ...(sessionId === undefined ? {} : { sessionId }),
          }),
        );
        return;
      }
    }
    yield* Effect.tryPromise({
      try: async () => {
        await mkdir(cwd, { recursive: true });
        await writeFile(path, contents, { encoding: "utf8", mode: 0o600 });
        await chmod(path, 0o600);
        // mcp.json carries plaintext env secrets inside a git clone: exclude it
        // locally so an agent's `git add -A` can never commit it to a branch it
        // will push. .git/info/exclude is workspace-local and never committed.
        const gitDir = join(cwd, ".git");
        if (await Bun.file(join(gitDir, "HEAD")).exists()) {
          const infoDir = join(gitDir, "info");
          await mkdir(infoDir, { recursive: true });
          const excludePath = join(infoDir, "exclude");
          const existing = (await Bun.file(excludePath).exists())
            ? await Bun.file(excludePath).text()
            : "";
          if (
            !existing.split("\n").some((line) => line.trim() === "mcp.json")
          ) {
            const separator =
              existing.length > 0 && !existing.endsWith("\n") ? "\n" : "";
            await writeFile(
              excludePath,
              `${existing}${separator}mcp.json\n`,
              "utf8",
            );
          }
        }
      },
      catch: (error) =>
        error instanceof Error ? error : new Error(String(error)),
    });
  });
};
