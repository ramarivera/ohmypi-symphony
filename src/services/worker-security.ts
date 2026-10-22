import { constants } from "node:fs";
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
): Record<string, string> =>
  Object.fromEntries(
    Object.entries(input).filter(
      (entry): entry is [string, string] =>
        WORKER_ENV.has(entry[0]) && entry[1] !== undefined,
    ),
  );

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

/** Give only the worker workspace to its uid. Never follow repository symlinks. */
export const prepareWorkerWorkspace = async (
  cwd: string,
  uid: string | undefined,
): Promise<void> => {
  if (uid === undefined || uid === "") return;
  if (!/^[1-9][0-9]*$/u.test(uid)) throw new Error("Invalid worker uid");
  const root = await realpath(cwd);
  if (root !== cwd || (await lstat(cwd)).isSymbolicLink())
    throw new Error("Worker workspace must be canonical");
  const visit = async (path: string): Promise<void> => {
    let handle: FileHandle;
    try {
      handle = await open(
        path,
        constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
      );
    } catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ELOOP")
        return;
      throw error;
    }
    try {
      const stat = await handle.stat();
      if (!stat.isDirectory() && !stat.isFile())
        throw new Error("Unsupported workspace file type");
      await handle.chown(Number(uid), Number(uid));
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
