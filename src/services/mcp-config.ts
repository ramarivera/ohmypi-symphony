import { chmod, mkdir, writeFile } from "node:fs/promises";
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
    if (!server.enabled) continue;
    const scope = Option.match(server.repositoryId, {
      onNone: () => null,
      onSome: (id) => id,
    });
    if (scope !== null && scope !== repositoryId) continue;
    const position = positions.get(server.name);
    if (position === undefined) {
      positions.set(server.name, effective.length);
      effective.push(server);
    } else if (scope !== null) {
      effective[position] = server;
    }
  }
  return effective;
};

export const toOmpMcpConfig = (servers: ReadonlyArray<McpServerRecord>) => ({
  mcpServers: Object.fromEntries(
    servers.map((server) => {
      const config: Record<string, unknown> = {
        type: server.transport,
      };
      if (server.transport === "stdio") {
        config.command = Option.getOrThrow(server.command);
        config.args = [...server.args];
      } else {
        config.url = Option.getOrThrow(server.url);
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
    yield* Effect.tryPromise({
      try: async () => {
        const path = join(cwd, "mcp.json");
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
