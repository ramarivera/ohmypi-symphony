import { lstatSync, realpathSync, rmdirSync, rmSync } from "node:fs";
import { lstat, realpath, rm, rmdir } from "node:fs/promises";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { Effect } from "effect";
import { isWithin } from "../util/paths.js";
import {
  prepareWorkerWorkspace,
  prepareWorkerWorkspaceSync,
  workerIdentityCommand,
  workerUidRange,
} from "./worker-security.js";

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

export const needsWorkspaceIdentity = (): boolean => {
  const range = workerUidRange(process.env);
  return (
    range !== undefined &&
    (process.env.WORKER_UID_BASE !== undefined ||
      process.getuid?.() !== range.base)
  );
};

/** The root inode is gateway-published; workers cannot chown it to another uid. */
export const workspaceWorkerUid = (cwd: string): string | undefined => {
  const range = workerUidRange(process.env);
  if (range === undefined) return undefined;
  const uid = lstatSync(cwd).uid;
  if (uid >= range.base && uid < range.base + range.count) return String(uid);
  // One-slot deployments also support legacy ownership migration.
  if (!process.env.WORKER_UID_BASE && process.env.WORKER_UID)
    return String(range.base);
  throw new Error(
    "Workspace has no allocated worker uid; prepare it before worker operations",
  );
};

const workspaceWorkerScript = fileURLToPath(
  new URL(
    import.meta.url.endsWith(".ts")
      ? "../workspace-worker.ts"
      : "./workspace-worker.js",
    import.meta.url,
  ),
);

// Only gateway-installed code and dependencies may initialize the helper. The
// workspace is data, never Bun's cwd/config/module-resolution root.
const workspaceWorkerCommand = (
  cwd: string,
  action: string,
  uid = workspaceWorkerUid(cwd),
): string[] =>
  workerIdentityCommand(
    [
      process.execPath,
      "--config=/dev/null",
      "--no-env-file",
      "--no-install",
      workspaceWorkerScript,
      action,
      cwd,
    ],
    uid,
  );

/** All writes into worker-controlled trees run without gateway privileges. */
export const workspaceProcess = async (
  cwd: string,
  action: string,
  payload: unknown,
): Promise<string> => {
  const child = Bun.spawn(workspaceWorkerCommand(cwd, action), {
    cwd: dirname(workspaceWorkerScript),
    // This allowlist excludes BUN_*, NODE_OPTIONS and loader variables.
    env: gitEnvironment(process.env),
    stdin: new Response(JSON.stringify(payload)),
    stdout: "pipe",
    stderr: "pipe",
  });
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  if (code !== 0)
    throw new Error(`Workspace ${action} failed: ${stderr.trim()}`);
  return stdout;
};

/** Run shared-directory cleanup with exactly the retired worker's privileges. */
export const cleanupWorkerSharedFilesSync = (uid: number): void => {
  if (process.getuid?.() !== 0) return;
  const child = Bun.spawnSync(
    workspaceWorkerCommand("/", "cleanup-shared", String(uid)),
    {
      cwd: dirname(workspaceWorkerScript),
      env: gitEnvironment(process.env),
      stdin: Buffer.from("null"),
      stdout: "pipe",
      stderr: "pipe",
      timeout: 5000,
    },
  );
  if (child.exitCode !== 0)
    throw new Error("Retired worker shared-directory cleanup failed");
};

/** Staging cleanup has the same uid boundary as terminal workspace retention. */
export const removeWorkerWorkspace = async (
  cwd: string,
  workspaceRoot: string,
  workerUid?: string,
): Promise<void> => {
  const root = await realpath(workspaceRoot);
  const node = await lstat(cwd).catch((error: unknown) => {
    if (error instanceof Error && "code" in error && error.code === "ENOENT")
      return undefined;
    throw error;
  });
  if (node === undefined) return;
  if (
    cwd === root ||
    !isWithin(root, cwd) ||
    node.isSymbolicLink() ||
    !node.isDirectory() ||
    (await realpath(cwd)) !== cwd
  )
    throw new Error("Staging workspace must be inside the workspace root");
  if (!needsWorkspaceIdentity()) {
    await rm(cwd, { recursive: true, force: true });
    return;
  }
  await prepareWorkerWorkspace(cwd, workerUid ?? workspaceWorkerUid(cwd));
  await workspaceProcess(cwd, "remove-workspace-contents", null);
  await rmdir(cwd);
};

/** Keep retention's SQLite reopen fence synchronous while dropping tree writes to the worker. */
export const removeWorkerWorkspaceSync = (
  cwd: string,
  workspaceRoot: string,
  workerUid?: string,
): void => {
  const root = realpathSync(workspaceRoot);
  if (cwd === root || !isWithin(root, cwd) || realpathSync(cwd) !== cwd)
    throw new Error("Retention workspace must be inside the workspace root");
  if (!needsWorkspaceIdentity()) {
    rmSync(cwd, { recursive: true, force: true });
    return;
  }
  // Terminal workspaces from older images never pass through resume preparation.
  // Migrate them before delegation, without yielding retention's reopen fence.
  const uid = workerUid ?? workspaceWorkerUid(cwd);
  prepareWorkerWorkspaceSync(cwd, uid);
  const child = Bun.spawnSync(
    workspaceWorkerCommand(cwd, "remove-workspace-contents", uid),
    {
      cwd: dirname(workspaceWorkerScript),
      // This allowlist excludes BUN_*, NODE_OPTIONS and loader variables.
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
