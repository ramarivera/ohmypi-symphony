import { fileURLToPath } from "node:url";
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
    GIT_TERMINAL_PROMPT: "0",
    GIT_CONFIG_NOSYSTEM: "1",
  };
};

export const needsWorkspaceIdentity = (): boolean =>
  !!process.env.WORKER_UID &&
  process.getuid?.() !== Number(process.env.WORKER_UID);

/** All writes into worker-controlled trees run without gateway privileges. */
export const workspaceProcess = async (
  cwd: string,
  action: string,
  payload: unknown,
): Promise<string> => {
  const script = fileURLToPath(
    new URL(
      import.meta.url.endsWith(".ts")
        ? "../workspace-worker.ts"
        : "./workspace-worker.js",
      import.meta.url,
    ),
  );
  const child = Bun.spawn(
    workerIdentityCommand(
      [process.execPath, script, action],
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
