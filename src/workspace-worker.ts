import { appendFile, mkdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { Effect } from "effect";
import {
  removeMcpConfigFile,
  writeOmpMcpConfig,
} from "./services/mcp-config.js";
import {
  materializeMcpAgentDb,
  removeMcpAgentDbFile,
} from "./services/mcp-oauth.js";
import {
  persistGitHubExtraHeader,
  removeGitHubExtraHeaders,
} from "./services/workspace.js";

const cwd = process.cwd();
const action = process.argv[2];
const input: unknown = await Bun.stdin.json();
switch (action) {
  case "agent-db": {
    const credentials = input as Parameters<typeof materializeMcpAgentDb>[1];
    process.stdout.write(await materializeMcpAgentDb(cwd, credentials));
    break;
  }
  case "mcp-config":
    await Effect.runPromise(
      writeOmpMcpConfig(cwd, input as Parameters<typeof writeOmpMcpConfig>[1]),
    );
    break;
  case "remove-agent-db":
    await removeMcpAgentDbFile(cwd);
    break;
  case "remove-mcp-config":
    await removeMcpConfigFile(cwd);
    break;
  case "persist-git":
    if (typeof input !== "string") throw new Error("Expected Git credential");
    await Effect.runPromise(persistGitHubExtraHeader(cwd, "worker", input));
    break;
  case "clear-git":
    await Effect.runPromise(removeGitHubExtraHeaders(cwd, "worker"));
    break;
  case "tool-directories": {
    for (const name of [".omp-home", ".omp-tmp"])
      await mkdir(join(cwd, name), { recursive: true, mode: 0o700 });
    await mkdir(join(cwd, ".git", "info"), { recursive: true });
    const excludePath = join(cwd, ".git", "info", "exclude");
    const existing = await readFile(excludePath, "utf8").catch(
      (error: unknown) => {
        if (
          error instanceof Error &&
          "code" in error &&
          error.code === "ENOENT"
        )
          return "";
        throw error;
      },
    );
    const lines = existing.split(/\r?\n/u);
    const missing = ["/.omp-home/", "/.omp-tmp/"].filter(
      (entry) => !lines.includes(entry),
    );
    if (missing.length > 0)
      await appendFile(excludePath, `\n${missing.join("\n")}\n`);
    break;
  }
  default:
    throw new Error("Unknown workspace operation");
}
