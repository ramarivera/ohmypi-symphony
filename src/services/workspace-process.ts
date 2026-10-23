import { rmdirSync, rmSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { Effect } from "effect";
import { workerIdentityCommand } from "./worker-security.js";

/** Git is allowed transport settings, never gateway or model credentials. */
export const gitEnvironment = (
  input: Record<string, string | undefined>,
): Record<string, string> => {
  const names = [
    "PATH",
    "HOME",
    "LANG",
    "LC_ALL",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "GIT_SSL_CAINFO",
    "GIT_CONFIG_GLOBAL",
  ];
  return {
    ...Object.fromEntries(
      names.flatMap((name) =>
        input[name] === undefined ? [] : [[name, input[name]]],
      ),
    ),
    GIT_ASKPASS: "",
    GIT_TERMINAL_PROMPT: "0",
    GIT_CONFIG_NOSYSTEM: "1",
  };
};

export const needsWorkspaceIdentity = (): boolean =>
  !!process.env.WORKER_UID &&
  process.getuid?.() !== Number(process.env.WORKER_UID);

const workspaceWorkerScript = fileURLToPath(
  new URL(
    import.meta.url.endsWith(".ts")
      ? "../workspace-worker.ts"
      : "./workspace-worker.js",
    import.meta.url,
  ),
);

/** All writes into worker-controlled trees run without gateway privileges. */
export const workspaceProcess = async (
  cwd: string,
  action: string,
  payload: unknown,
): Promise<string> => {
  const child = Bun.spawn(
    workerIdentityCommand(
      [process.execPath, workspaceWorkerScript, action],
      process.env.WORKER_UID,
    ),
    {
      cwd,
      env: gitEnvironment(process.env),
      stdin: new Response(JSON.stringify(payload)),
      stdout: "pipe",
      stderr: "pipe",
    },
  );
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  if (code !== 0)
    throw new Error(`Workspace ${action} failed: ${stderr.trim()}`);
  return stdout;
};

/** Keep retention's SQLite reopen fence synchronous while dropping tree writes to the worker. */
export const removeWorkerWorkspaceSync = (cwd: string): void => {
  if (!needsWorkspaceIdentity()) {
    rmSync(cwd, { recursive: true, force: true });
    return;
  }
  const child = Bun.spawnSync(
    workerIdentityCommand(
      [process.execPath, workspaceWorkerScript, "remove-workspace-contents"],
      process.env.WORKER_UID,
    ),
    {
      cwd,
      env: gitEnvironment(process.env),
      stdin: Buffer.from("null"),
      stdout: "pipe",
      stderr: "pipe",
    },
  );
  if (child.exitCode !== 0)
    throw new Error(
      `Workspace removal failed: ${child.stderr.toString().trim()}`,
    );
  // The workspace parent belongs to the gateway, so only it can unlink this
  // directory. Never recurse as root, even if a worker adds files after cleanup.
  rmdirSync(cwd);
};

/** Cleanup stays best effort, but failed delegation must leave an operator signal. */
export const cleanupWorkspaceProcess = (
  cwd: string,
  action: string,
  event: string,
  annotations: Record<string, string> = {},
): Effect.Effect<void> =>
  Effect.tryPromise({
    try: () => workspaceProcess(cwd, action, null),
    catch: (error) => (error instanceof Error ? error.message : String(error)),
  }).pipe(
    Effect.asVoid,
    Effect.catchAll((error) =>
      Effect.logWarning(event).pipe(
        Effect.annotateLogs({ ...annotations, cwd, action, error }),
      ),
    ),
  );
