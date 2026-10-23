import { appendFile, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { Effect } from "effect";
import { removeMcpConfig, writeOmpMcpConfig } from "./services/mcp-config.js";
import {
  materializeMcpAgentDb,
  removeMcpAgentDb,
} from "./services/mcp-oauth.js";
import {
  persistGitHubExtraHeader,
  unsetGitHubExtraHeader,
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
    await Effect.runPromise(removeMcpAgentDb(cwd));
    break;
  case "remove-mcp-config":
    await Effect.runPromise(removeMcpConfig(cwd));
    break;
  case "persist-git":
    if (typeof input !== "string") throw new Error("Expected Git credential");
    await Effect.runPromise(persistGitHubExtraHeader(cwd, "worker", input));
    break;
  case "clear-git":
    await Effect.runPromise(unsetGitHubExtraHeader(cwd, "worker"));
    break;
  case "tool-directories":
    for (const name of [".omp-home", ".omp-tmp"])
      await mkdir(join(cwd, name), { recursive: true, mode: 0o700 });
    await mkdir(join(cwd, ".git", "info"), { recursive: true });
    await appendFile(
      join(cwd, ".git", "info", "exclude"),
      "\n/.omp-home/\n/.omp-tmp/\n",
    );
    break;
  default:
    throw new Error("Unknown workspace operation");
}
