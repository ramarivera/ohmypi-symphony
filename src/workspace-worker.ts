import { readdirSync, rmSync } from "node:fs";
import { appendFile, mkdir, readFile } from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import { Effect } from "effect";
import {
  removeMcpConfigFile,
  writeOmpMcpConfigFile,
} from "./services/mcp-config.js";
import {
  materializeMcpAgentDb,
  removeMcpAgentDbFile,
} from "./services/mcp-oauth.js";
import { workerMcpCredential } from "./services/worker-security.js";
import {
  persistGitHubExtraHeader,
  removeGitHubExtraHeaders,
} from "./services/workspace.js";

const cwd = process.argv[3];
if (cwd === undefined || !isAbsolute(cwd))
  throw new Error("Expected absolute workspace path");
const action = process.argv[2];
const input: unknown = await Bun.stdin.json();
switch (action) {
  case "agent-db": {
    const credentials = input as Parameters<typeof materializeMcpAgentDb>[1];
    process.stdout.write(
      await materializeMcpAgentDb(
        cwd,
        credentials.map((credential) => ({
          serverUrl: credential.serverUrl,
          ...workerMcpCredential(credential),
        })),
      ),
    );
    break;
  }
  case "mcp-config":
    await Effect.runPromise(
      writeOmpMcpConfigFile(
        cwd,
        input as Parameters<typeof writeOmpMcpConfigFile>[1],
      ),
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
  case "remove-workspace-contents":
    // rm unlinks symlinks; traversal stays under the explicit workspace path
    // and runs as the worker, without access to gateway-owned files.
    for (const name of readdirSync(cwd))
      rmSync(join(cwd, name), { recursive: true, force: true });
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
