import { readdirSync, rmSync } from "node:fs";
import {
  appendFile,
  mkdir,
  readFile,
  rename,
  writeFile,
} from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import { Effect } from "effect";
import type { T3LaunchSettings } from "./drivers/t3/launcher.js";
import { t3SessionPaths } from "./drivers/t3/paths.js";
import {
  removeMcpConfigFile,
  writeOmpMcpConfigFile,
} from "./services/mcp-config.js";
import {
  materializeMcpAgentDb,
  removeMcpAgentDbFile,
} from "./services/mcp-oauth.js";
import {
  seedWorkerProfileFiles,
  type WorkerProfileManifest,
} from "./services/worker-profile.js";
import { workerMcpCredential } from "./services/worker-security.js";
import { cleanupWorkerSharedDirectoriesSync } from "./services/worker-shared-cleanup.js";
import {
  assertT3GitCredentials,
  persistGitHubExtraHeader,
  removeGitHubExtraHeaders,
} from "./services/workspace.js";

const cwd = process.argv[3];
if (cwd === undefined || !isAbsolute(cwd))
  throw new Error("Expected absolute workspace path");
const action = process.argv[2];
const input: unknown = await Bun.stdin.json();
switch (action) {
  case "cleanup-shared":
    cleanupWorkerSharedDirectoriesSync();
    break;
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
  case "seed-profile": {
    // The gateway validated this manifest; the copy re-checks every file
    // without following links and can only write this worker's own tree.
    const { source, manifest } = input as {
      readonly source: string;
      readonly manifest: WorkerProfileManifest;
    };
    process.stdout.write(await seedWorkerProfileFiles(cwd, source, manifest));
    break;
  }
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
  case "check-t3-git":
    await Effect.runPromise(
      assertT3GitCredentials(cwd, "worker", 120_000, input === true),
    );
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
  case "t3-config": {
    // Paths come from the workspace, never the payload. Rewritten every launch.
    const { settings, codexConfig } = input as T3LaunchSettings;
    if (typeof codexConfig !== "string" || typeof settings !== "object")
      throw new Error("Expected T3 settings");
    const paths = t3SessionPaths(cwd);
    const userdata = join(paths.t3Home, "userdata");
    for (const dir of [userdata, paths.codexHome, paths.claudeHome])
      await mkdir(dir, { recursive: true, mode: 0o700 });
    const replace = async (path: string, content: string) => {
      await writeFile(`${path}.tmp`, content, { mode: 0o600 });
      await rename(`${path}.tmp`, path);
    };
    await replace(
      join(userdata, "settings.json"),
      `${JSON.stringify(settings, null, 2)}\n`,
    );
    await replace(join(paths.codexHome, "config.toml"), codexConfig);
    break;
  }
  default:
    throw new Error("Unknown workspace operation");
}
