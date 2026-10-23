import {
  closeSync,
  constants,
  fchmodSync,
  fchownSync,
  fstatSync,
  lstatSync,
  openSync,
  readdirSync,
  readFileSync,
  realpathSync,
  type Stats,
} from "node:fs";
import {
  type FileHandle,
  lstat,
  open,
  readdir,
  realpath,
} from "node:fs/promises";
import { join } from "node:path";

// No prefixes such as AWS_*, GITHUB_*, PI_*, or *_FILE. New gateway secrets
// are excluded by default. Per-run values use the same explicit contract.
// No model-provider keys or endpoints: the only model credential a worker gets
// is the CLIProxyAPI client key, injected per run by workerModelEnvironment.
const WORKER_ENV = new Set([
  "PATH",
  "HOME",
  "LANG",
  "LANGUAGE",
  "LC_ALL",
  "LC_CTYPE",
  "TERM",
  "COLORTERM",
  "TMPDIR",
  "XDG_CACHE_HOME",
  "XDG_CONFIG_HOME",
  "XDG_DATA_HOME",
  "BUN_INSTALL_CACHE_DIR",
  "BUN_RUNTIME_TRANSPILER_CACHE_PATH",
  "NIX_SSL_CERT_FILE",
  "PI_CODING_AGENT_DIR",
  "PI_PACKAGE_DIR",
  "OMP_SKIP_UPDATE_CHECK",
  "NO_COLOR",
]);
export const workerEnvironment = (
  input: Record<string, string | undefined>,
): Record<string, string> =>
  // The gateway realizes and roots packages before passing their bin paths.
  // Workers use those files directly; they don't need Nix store configuration.
  Object.fromEntries(
    Object.entries(input).filter(
      (entry): entry is [string, string] =>
        WORKER_ENV.has(entry[0]) && entry[1] !== undefined,
    ),
  );

/** Legacy WORKER_UID alone is a one-slot range. Native launches may opt out. */
export const workerUidRange = (
  env: Record<string, string | undefined>,
): { readonly base: number; readonly count: number } | undefined => {
  const base = env.WORKER_UID_BASE || env.WORKER_UID;
  const count = env.WORKER_UID_COUNT || (env.WORKER_UID_BASE ? "64" : "1");
  if (!base) {
    if (env.WORKER_UID_COUNT)
      throw new Error("WORKER_UID_COUNT requires WORKER_UID_BASE");
    return undefined;
  }
  if (
    !/^[1-9][0-9]*$/u.test(base) ||
    !/^[1-9][0-9]*$/u.test(count) ||
    !Number.isSafeInteger(Number(base)) ||
    !Number.isSafeInteger(Number(count)) ||
    Number(base) + Number(count) > 4294967295
  )
    throw new Error(
      "Worker uid range must contain non-root Linux uid/gid values",
    );
  return { base: Number(base), count: Number(count) };
};

export const sessionWorkerEnvironment = (
  cwd: string,
  input: Record<string, string | undefined>,
): Record<string, string> => ({
  ...workerEnvironment(input),
  BUN_INSTALL_CACHE_DIR: join(cwd, ".omp-home", ".cache", "bun", "install"),
  BUN_RUNTIME_TRANSPILER_CACHE_PATH: join(
    cwd,
    ".omp-home",
    ".cache",
    "bun",
    "transpiler",
  ),
  HOME: join(cwd, ".omp-home"),
  TMPDIR: join(cwd, ".omp-tmp"),
  XDG_CACHE_HOME: join(cwd, ".omp-home", ".cache"),
  XDG_CONFIG_HOME: join(cwd, ".omp-home", ".config"),
  XDG_DATA_HOME: join(cwd, ".omp-home", ".local", "share"),
});

export const workerIdentityCommand = (
  command: ReadonlyArray<string>,
  uid: string | undefined,
): string[] => {
  if (uid === undefined || uid === "") return [...command];
  if (!/^[1-9][0-9]*$/u.test(uid))
    throw new Error("Worker uid must be a non-root numeric uid");
  return [
    "/usr/bin/setpriv",
    "--reuid",
    uid,
    "--regid",
    uid,
    "--clear-groups",
    "--no-new-privs",
    "--bounding-set=-all",
    "--inh-caps=-all",
    "--ambient-caps=-all",
    "--",
    ...command,
  ];
};

const NO_FOLLOW_FLAGS =
  constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK;

const validWorkerUid = (uid: string | undefined): number | undefined => {
  if (uid === undefined || uid === "") return undefined;
  if (!/^[1-9][0-9]*$/u.test(uid)) throw new Error("Invalid worker uid");
  return Number(uid);
};

const isSymlinkLoop = (error: unknown): boolean =>
  error instanceof Error && "code" in error && error.code === "ELOOP";

const isNoDevice = (error: unknown): boolean =>
  error instanceof Error && "code" in error && error.code === "ENXIO";

const isWorkspaceIpc = (stat: Stats): boolean =>
  stat.isFIFO() || stat.isSocket();

/** Whether this node must be chowned; throws for files the worker must never own. */
const needsChown = (stat: Stats, uid: number): boolean => {
  // IPC nodes need no group migration. Never transfer a foreign IPC endpoint,
  // even for retention: all callers keep the same fail-closed ownership policy.
  if (isWorkspaceIpc(stat)) {
    if (stat.uid !== uid)
      throw new Error("Foreign-owned workspace FIFO or socket");
    return false;
  }
  if (!stat.isDirectory() && !stat.isFile())
    throw new Error("Unsupported workspace file type");
  const owned = stat.uid === uid && stat.gid === uid;
  if (stat.isFile() && stat.nlink !== 1 && !owned)
    throw new Error("Multiply-linked workspace files are forbidden");
  return !owned;
};

/** Give only the worker workspace to its uid. Never follow repository symlinks. */
export const prepareWorkerWorkspace = async (
  cwd: string,
  workerUid: string | undefined,
): Promise<void> => {
  const uid = validWorkerUid(workerUid);
  if (uid === undefined) return;
  const root = await realpath(cwd);
  if (root !== cwd || (await lstat(cwd)).isSymbolicLink())
    throw new Error("Worker workspace must be canonical");
  const visit = async (path: string): Promise<void> => {
    let handle: FileHandle;
    try {
      handle = await open(path, NO_FOLLOW_FLAGS);
    } catch (error) {
      if (isSymlinkLoop(error)) return;
      if (isNoDevice(error)) {
        const stat = await lstat(path);
        if (isWorkspaceIpc(stat)) {
          needsChown(stat, uid);
          return;
        }
      }
      throw error;
    }
    try {
      const stat = await handle.stat();
      if (path === root) {
        // CHOWN is available but FOWNER isn't. Temporarily own the root inode
        // before chmod, then restore its session identity while it's pinned.
        if (process.getuid?.() === 0 && stat.uid !== 0)
          await handle.chown(0, 0);
        await handle.chmod(0o700);
        if (process.getuid?.() === 0 && stat.uid === uid)
          await handle.chown(uid, uid);
      }
      if (needsChown(stat, uid)) await handle.chown(uid, uid);
      if (stat.isDirectory()) {
        const fdPath = `/proc/self/fd/${handle.fd}`;
        for (const name of await readdir(fdPath))
          await visit(join(fdPath, name));
      }
    } finally {
      await handle.close();
    }
  };
  await visit(root);
};

/**
 * Synchronous twin for retention, whose SQLite reopen fence must not yield.
 * Resume and clone paths use the async walk so large trees don't block the loop.
 */
export const prepareWorkerWorkspaceSync = (
  cwd: string,
  workerUid: string | undefined,
): void => {
  const uid = validWorkerUid(workerUid);
  if (uid === undefined) return;
  const root = realpathSync(cwd);
  if (root !== cwd || lstatSync(cwd).isSymbolicLink())
    throw new Error("Worker workspace must be canonical");
  const visit = (path: string): void => {
    let fd: number;
    try {
      fd = openSync(path, NO_FOLLOW_FLAGS);
    } catch (error) {
      if (isSymlinkLoop(error)) return;
      if (isNoDevice(error)) {
        const stat = lstatSync(path);
        if (isWorkspaceIpc(stat)) {
          needsChown(stat, uid);
          return;
        }
      }
      throw error;
    }
    try {
      const stat = fstatSync(fd);
      if (path === root) {
        if (process.getuid?.() === 0 && stat.uid !== 0) fchownSync(fd, 0, 0);
        fchmodSync(fd, 0o700);
        if (process.getuid?.() === 0 && stat.uid === uid)
          fchownSync(fd, uid, uid);
      }
      if (needsChown(stat, uid)) fchownSync(fd, uid, uid);
      if (stat.isDirectory()) {
        const fdPath = `/proc/self/fd/${fd}`;
        for (const name of readdirSync(fdPath)) visit(join(fdPath, name));
      }
    } finally {
      closeSync(fd);
    }
  };
  visit(root);
};

/** Workers never receive renewal material or access tokens lasting over an hour. */
export const workerMcpCredential = (
  token: { readonly accessToken: string; readonly expiresAt: number },
  now = Date.now(),
): { readonly accessToken: string; readonly expiresAt: number } => {
  if (
    !Number.isFinite(token.expiresAt) ||
    token.expiresAt <= now ||
    token.expiresAt > now + 60 * 60_000
  )
    throw new Error("MCP worker access token must expire within one hour");
  return { accessToken: token.accessToken, expiresAt: token.expiresAt };
};

/** Revocation before recycling a uid. Retained files remain behind a root-only gate. */
export const sealWorkerWorkspaceSync = (cwd: string): void => {
  if (realpathSync(cwd) !== cwd)
    throw new Error("Worker workspace must be canonical");
  const fd = openSync(cwd, NO_FOLLOW_FLAGS | constants.O_DIRECTORY);
  try {
    fchownSync(fd, 0, 0);
    fchmodSync(fd, 0o700);
  } finally {
    closeSync(fd);
  }
};

/** Seal abandoned staging directories as well as the published workspace. */
export const sealWorkerUidWorkspacesSync = (
  workspaceRoot: string,
  uid: number,
): void => {
  let root: string;
  try {
    root = realpathSync(workspaceRoot);
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT")
      return;
    throw error;
  }
  for (const name of readdirSync(root)) {
    const path = join(root, name);
    const stat = lstatSync(path);
    if (stat.isDirectory() && stat.uid === uid) sealWorkerWorkspaceSync(path);
  }
};

/** Kill escaped descendants before recycling their uid, using existing KILL. */
export const stopWorkerUidProcessesSync = (uid: number): void => {
  if (process.getuid?.() !== 0) return;
  if (uid === 0) throw new Error("Cannot retire gateway uid");
  // Repeat after signaling to catch children forked during the first scan. A
  // zombie can't execute; any process that won't exit keeps the uid reserved.
  const deadline = Date.now() + 500;
  const pause = new Int32Array(new SharedArrayBuffer(4));
  for (;;) {
    let live = false;
    for (const name of readdirSync("/proc")) {
      if (!/^[1-9][0-9]*$/u.test(name)) continue;
      try {
        const status = readFileSync(`/proc/${name}/status`, "utf8");
        const identities = /^Uid:\s+(.*)$/mu
          .exec(status)?.[1]
          ?.trim()
          .split(/\s+/u)
          .map(Number);
        if (!identities?.includes(uid) || /^State:\s+Z/mu.test(status))
          continue;
        live = true;
        process.kill(Number(name), "SIGKILL");
      } catch (error) {
        if (
          error instanceof Error &&
          "code" in error &&
          (error.code === "ENOENT" || error.code === "ESRCH")
        )
          continue;
        throw error;
      }
    }
    if (!live) return;
    if (Date.now() >= deadline) break;
    // Let the kernel finish SIGKILL before scanning again.
    Atomics.wait(pause, 0, 0, 10);
  }
  throw new Error(
    "Worker processes didn't exit; keeping uid reserved for a later retry",
  );
};
