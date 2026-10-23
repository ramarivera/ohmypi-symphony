import {
  closeSync,
  constants,
  fchownSync,
  fstatSync,
  lstatSync,
  openSync,
  readdirSync,
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
  "NIX_SSL_CERT_FILE",
  "NIX_PATH",
  "NIX_PROFILES",
  "NIX_REMOTE",
  "NIX_CONFIG",
  "PI_CODING_AGENT_DIR",
  "PI_PACKAGE_DIR",
  "OMP_SKIP_UPDATE_CHECK",
  "NO_COLOR",
  "ANTHROPIC_API_KEY",
  "OPENAI_API_KEY",
  "OPENAI_BASE_URL",
  "ANTHROPIC_BASE_URL",
  "GOOGLE_API_KEY",
  "GEMINI_API_KEY",
  "OPENROUTER_API_KEY",
]);
export const workerEnvironment = (
  input: Record<string, string | undefined>,
  workerUid?: string,
): Record<string, string> => ({
  ...Object.fromEntries(
    Object.entries(input).filter(
      (entry): entry is [string, string] =>
        WORKER_ENV.has(entry[0]) && entry[1] !== undefined,
    ),
  ),
  // The single-user store is prepared by the gateway. Workers must not take
  // its write lock, mutate its database, or connect to a privileged daemon.
  ...(workerUid ? { NIX_REMOTE: "local?read-only=true" } : {}),
});

export const workerIdentityCommand = (
  command: ReadonlyArray<string>,
  uid: string | undefined,
): string[] => {
  if (uid === undefined || uid === "") return [...command];
  if (!/^[1-9][0-9]*$/u.test(uid))
    throw new Error("WORKER_UID must be a non-root numeric uid");
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
