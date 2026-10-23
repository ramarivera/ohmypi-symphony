import { constants } from "node:fs";
import {
  chmod,
  type FileHandle,
  lstat,
  mkdir,
  open,
  readdir,
  realpath,
  rm,
  writeFile,
} from "node:fs/promises";
import { isAbsolute, join } from "node:path";

/**
 * Curated, credential-free omp agent profile copied into each worker's private
 * PI_CODING_AGENT_DIR. Contract: docs/worker-profile.md.
 */
export const PROFILE_DIRECTORIES: ReadonlySet<string> = new Set([
  "agents",
  "extensions",
  "rules",
  "skills",
  "themes",
]);
const PROFILE_FILES: ReadonlySet<string> = new Set([
  "config.yml",
  "models.yml",
]);
// AGENTS.md, RULES.md, SYSTEM.md, APPEND_SYSTEM.md and similar context files.
const PROFILE_CONTEXT_FILE = /^[A-Z][A-Z0-9_]*\.md$/u;

/** The only model credential a worker receives, referenced by name from models.yml. */
export const CLIPROXYAPI_KEY_ENV = "CLIPROXYAPI_API_KEY";
/** Provider registered by pi-claude-cli; it has no endpoint or key of its own. */
export const PI_CLAUDE_CLI_PROVIDER = "pi-claude-cli";
// OpenAI-compatible wire formats. omp's direct anthropic-messages client is
// forbidden: Claude goes through pi-claude-cli and the Claude Code CLI.
const ALLOWED_PROVIDER_APIS: ReadonlySet<string> = new Set([
  "openai-completions",
  "openai-responses",
]);
const ALLOWED_PROVIDER_KEYS: ReadonlySet<string> = new Set([
  "name",
  "baseUrl",
  "apiKey",
  "api",
  "compat",
  "authHeader",
  "models",
  "modelOverrides",
  "disableStrictTools",
]);
const FORBIDDEN_MODEL_KEYS: ReadonlySet<string> = new Set([
  "headers",
  "auth",
  "transport",
]);
// Key names that carry credentials anywhere in config.yml or models.yml. Only
// a provider apiKey naming CLIPROXYAPI_KEY_ENV is accepted.
const SECRET_KEY =
  /^(api[-_]?key|access[-_]?token|refresh[-_]?token|token|secret|client[-_]?secret|password|authorization|bearer|cookie|headers)$/iu;

const CREDENTIAL_NAMES: ReadonlySet<string> = new Set([
  "auth.json",
  "credentials.json",
  "mcp.json",
  ".env",
  ".netrc",
  ".npmrc",
  ".pypirc",
  "id_rsa",
  "id_ecdsa",
  "id_ed25519",
  "sessions",
]);
const CREDENTIAL_SUFFIX =
  /\.(db|db-wal|db-shm|db-journal|sqlite|sqlite3|pem|key|p12|pfx|token)$/iu;
const isCredentialName = (name: string): boolean =>
  CREDENTIAL_NAMES.has(name) ||
  CREDENTIAL_SUFFIX.test(name) ||
  (name.startsWith(".env.") && name !== ".env.example");

export interface WorkerProfilePolicy {
  /** Every provider in models.yml must point below this CLIProxyAPI origin. */
  readonly cliproxyApiBaseUrl: string | undefined;
  /** Validated against models.yml so a typo fails before a worker starts. */
  readonly defaultModel: string | undefined;
  readonly maxBytes: number;
  readonly maxEntries: number;
  /** Owners allowed for profile inodes; defaults to root and the gateway uid. */
  readonly trustedUids?: ReadonlyArray<number>;
  /** Worker-uid copies need other-readable files and searchable directories. */
  readonly requireWorldReadable: boolean;
}

export interface WorkerProfileManifest {
  readonly directories: ReadonlyArray<string>;
  readonly files: ReadonlyArray<{
    readonly path: string;
    readonly size: number;
    readonly executable: boolean;
  }>;
}

function fail(message: string): never {
  throw new Error(`Worker profile ${message}`);
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const parseYaml = (path: string, text: string): unknown => {
  try {
    return Bun.YAML.parse(text);
  } catch {
    return fail(`${path} is not valid YAML`);
  }
};

/** Reject credential-bearing keys and omp `!command` values anywhere in a tree. */
const scanForSecrets = (
  file: string,
  value: unknown,
  path: ReadonlyArray<string>,
  allowed: (path: ReadonlyArray<string>, value: unknown) => boolean,
): void => {
  if (typeof value === "string") {
    if (value.trimStart().startsWith("!"))
      fail(`${file} must not run commands (${path.join(".")})`);
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item, index) => {
      scanForSecrets(file, item, [...path, String(index)], allowed);
    });
    return;
  }
  if (!isRecord(value)) return;
  for (const [key, child] of Object.entries(value)) {
    const childPath = [...path, key];
    if (SECRET_KEY.test(key) && !allowed(childPath, child))
      fail(`${file} must not contain credentials (${childPath.join(".")})`);
    scanForSecrets(file, child, childPath, allowed);
  }
};

const isProviderApiKey = (path: ReadonlyArray<string>, value: unknown) =>
  path.length === 3 &&
  path[0] === "providers" &&
  path[2] === "apiKey" &&
  value === CLIPROXYAPI_KEY_ENV;

const withinBaseUrl = (value: unknown, base: string): boolean => {
  if (typeof value !== "string" || !URL.canParse(base)) return false;
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return false;
  }
  const root = new URL(base);
  const prefix = root.pathname.endsWith("/")
    ? root.pathname
    : `${root.pathname}/`;
  return (
    url.origin === root.origin &&
    url.username === "" &&
    url.password === "" &&
    (url.pathname === root.pathname || url.pathname.startsWith(prefix))
  );
};

const validateModelEntry = (
  where: string,
  model: unknown,
  base: string | undefined,
): void => {
  if (!isRecord(model)) fail(`models.yml ${where} must be a mapping`);
  for (const key of FORBIDDEN_MODEL_KEYS)
    if (key in model) fail(`models.yml ${where} must not set ${key}`);
  if ("baseUrl" in model && !withinBaseUrl(model.baseUrl, base ?? ""))
    fail(`models.yml ${where} baseUrl must use CLIPROXYAPI_BASE_URL`);
  if ("api" in model && !ALLOWED_PROVIDER_APIS.has(String(model.api)))
    fail(
      `models.yml ${where} api must be openai-responses or openai-completions`,
    );
};

/** Returns the provider ids declared by a validated models.yml. */
export const validateModelsYaml = (
  text: string,
  cliproxyApiBaseUrl: string | undefined,
): ReadonlySet<string> => {
  const parsed = parseYaml("models.yml", text);
  if (parsed === null || parsed === undefined) return new Set();
  if (!isRecord(parsed)) return fail("models.yml must be a mapping");
  for (const key of Object.keys(parsed))
    if (key !== "providers") fail(`models.yml must not set ${key}`);
  scanForSecrets("models.yml", parsed, [], isProviderApiKey);
  const providers = parsed.providers ?? {};
  if (!isRecord(providers))
    return fail("models.yml providers must be a mapping");
  for (const [id, provider] of Object.entries(providers)) {
    if (!isRecord(provider))
      return fail(`models.yml provider ${id} must be a mapping`);
    if (id === PI_CLAUDE_CLI_PROVIDER)
      fail(`models.yml must not override ${PI_CLAUDE_CLI_PROVIDER}`);
    for (const key of Object.keys(provider))
      if (!ALLOWED_PROVIDER_KEYS.has(key))
        fail(`models.yml provider ${id} must not set ${key}`);
    if (cliproxyApiBaseUrl === undefined)
      fail(`models.yml provider ${id} requires CLIPROXYAPI_BASE_URL`);
    if (!withinBaseUrl(provider.baseUrl, cliproxyApiBaseUrl ?? ""))
      fail(`models.yml provider ${id} baseUrl must use CLIPROXYAPI_BASE_URL`);
    if (!ALLOWED_PROVIDER_APIS.has(String(provider.api)))
      fail(
        `models.yml provider ${id} api must be openai-responses or openai-completions`,
      );
    const models = provider.models ?? [];
    if (!Array.isArray(models))
      return fail(`models.yml provider ${id} models must be a list`);
    models.forEach((model, index) => {
      validateModelEntry(
        `provider ${id} model ${index}`,
        model,
        cliproxyApiBaseUrl,
      );
    });
    const overrides = provider.modelOverrides ?? {};
    if (!isRecord(overrides))
      return fail(`models.yml provider ${id} modelOverrides must be a mapping`);
    for (const [model, override] of Object.entries(overrides))
      validateModelEntry(
        `provider ${id} override ${model}`,
        override,
        cliproxyApiBaseUrl,
      );
  }
  return new Set(Object.keys(providers));
};

export const validateConfigYaml = (text: string): void => {
  const parsed = parseYaml("config.yml", text);
  if (parsed !== null && parsed !== undefined && !isRecord(parsed))
    fail("config.yml must be a mapping");
  scanForSecrets("config.yml", parsed, [], () => false);
};

/** `provider/model` with an optional `:thinking` suffix. */
export const modelProvider = (model: string): string | undefined =>
  /^([A-Za-z0-9][A-Za-z0-9._-]*)\/[^\s/:][^\s:]*(?::[a-z]+)?$/u.exec(
    model,
  )?.[1];

const NO_FOLLOW =
  constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK;

const readProfileFile = async (
  handle: FileHandle,
  limit: number,
): Promise<string> => {
  const stat = await handle.stat();
  if (stat.size > limit) fail("file exceeds WORKER_PROFILE_MAX_BYTES");
  return (await handle.readFile()).toString("utf8");
};

/**
 * Gateway-side validation of the read-only profile source. Runs before every
 * seed so a changed mount can't bypass it; workers copy only manifest entries.
 */
export const validateWorkerProfile = async (
  source: string,
  policy: WorkerProfilePolicy,
): Promise<WorkerProfileManifest> => {
  if (!isAbsolute(source)) fail("source must be an absolute path");
  const root = await realpath(source).catch(() =>
    fail(`source ${source} does not exist`),
  );
  if (root !== source) fail("source must be a canonical path without symlinks");
  const trusted = new Set(policy.trustedUids ?? [0, process.getuid?.() ?? 0]);
  const directories: string[] = [];
  const files: Array<WorkerProfileManifest["files"][number]> = [];
  let bytes = 0;
  let providers: ReadonlySet<string> = new Set();

  const visit = async (relative: string, depth: number): Promise<void> => {
    const path = relative === "" ? root : join(root, relative);
    const name = relative.split("/").at(-1) ?? "";
    if (depth === 1) {
      const allowed =
        PROFILE_DIRECTORIES.has(name) ||
        PROFILE_FILES.has(name) ||
        PROFILE_CONTEXT_FILE.test(name);
      if (!allowed)
        fail(`entry ${relative} is not part of the profile contract`);
    }
    if (depth > 0 && isCredentialName(name))
      fail(`must not contain credential files (${relative})`);
    if (directories.length + files.length > policy.maxEntries)
      fail("exceeds WORKER_PROFILE_MAX_ENTRIES");
    let handle: FileHandle;
    try {
      handle = await open(path, NO_FOLLOW);
    } catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ELOOP")
        return fail(`must not contain symlinks (${relative || "."})`);
      throw error;
    }
    try {
      const stat = await handle.stat();
      if (!trusted.has(stat.uid))
        fail(`entry ${relative || "."} has a foreign owner (uid ${stat.uid})`);
      if ((stat.mode & 0o022) !== 0)
        fail(`entry ${relative || "."} is group- or world-writable`);
      if (stat.isDirectory()) {
        if (depth === 1 && !PROFILE_DIRECTORIES.has(name))
          fail(`entry ${relative} must be a regular file`);
        if (policy.requireWorldReadable && (stat.mode & 0o005) !== 0o005)
          fail(`directory ${relative || "."} must be world-readable`);
        if (depth > 0) directories.push(relative);
        const names = (await readdir(`/proc/self/fd/${handle.fd}`)).sort();
        for (const child of names)
          await visit(
            relative === "" ? child : `${relative}/${child}`,
            depth + 1,
          );
        return;
      }
      if (!stat.isFile())
        fail(`entry ${relative} must be a regular file or directory`);
      if (depth === 0) fail("source must be a directory");
      if (depth === 1 && PROFILE_DIRECTORIES.has(name))
        fail(`entry ${relative} must be a directory`);
      if (policy.requireWorldReadable && (stat.mode & 0o004) === 0)
        fail(`file ${relative} must be world-readable`);
      bytes += stat.size;
      if (bytes > policy.maxBytes) fail("exceeds WORKER_PROFILE_MAX_BYTES");
      if (relative === "models.yml")
        providers = validateModelsYaml(
          await readProfileFile(handle, policy.maxBytes),
          policy.cliproxyApiBaseUrl,
        );
      if (relative === "config.yml")
        validateConfigYaml(await readProfileFile(handle, policy.maxBytes));
      files.push({
        path: relative,
        size: stat.size,
        executable: (stat.mode & 0o100) !== 0,
      });
    } finally {
      await handle.close();
    }
  };
  await visit("", 0);

  if (policy.defaultModel !== undefined) {
    const provider = modelProvider(policy.defaultModel);
    if (
      provider === undefined ||
      (provider !== PI_CLAUDE_CLI_PROVIDER && !providers.has(provider))
    )
      fail(
        `models.yml doesn't declare the provider of WORKER_DEFAULT_MODEL ${policy.defaultModel}`,
      );
  }
  return { directories, files };
};

const managedEntry = (name: string): boolean =>
  PROFILE_DIRECTORIES.has(name) ||
  PROFILE_FILES.has(name) ||
  PROFILE_CONTEXT_FILE.test(name);

const isSafeRelative = (path: string): boolean =>
  path.length > 0 &&
  !isAbsolute(path) &&
  path.split("/").every((part) => part !== "" && part !== "." && part !== "..");

/**
 * Worker-side copy into `<workspace>/.omp-gateway`. Runs as the worker uid in
 * Docker, so it can only read the world-readable profile and write its own tree.
 * Profile-managed entries are replaced; agent.db and sessions are untouched.
 */
export const seedWorkerProfileFiles = async (
  workspace: string,
  source: string,
  manifest: WorkerProfileManifest,
): Promise<string> => {
  const agentDir = join(workspace, ".omp-gateway");
  const existing = await lstat(agentDir).catch((error: unknown) => {
    if (error instanceof Error && "code" in error && error.code === "ENOENT")
      return undefined;
    throw error;
  });
  if (existing !== undefined && !existing.isDirectory())
    fail("agent directory must be a real directory");
  await mkdir(agentDir, { recursive: true, mode: 0o700 });
  await chmod(agentDir, 0o700);
  // rm doesn't follow a symlinked entry; it unlinks it.
  for (const name of await readdir(agentDir))
    if (managedEntry(name))
      await rm(join(agentDir, name), { recursive: true, force: true });
  for (const directory of manifest.directories) {
    if (
      !isSafeRelative(directory) ||
      !managedEntry(directory.split("/")[0] ?? "")
    )
      fail(`manifest directory ${directory} is invalid`);
    await mkdir(join(agentDir, directory), { mode: 0o700 });
  }
  for (const file of manifest.files) {
    if (
      !isSafeRelative(file.path) ||
      !managedEntry(file.path.split("/")[0] ?? "")
    )
      fail(`manifest file ${file.path} is invalid`);
    const handle = await open(join(source, file.path), NO_FOLLOW);
    try {
      const stat = await handle.stat();
      if (!stat.isFile() || stat.size !== file.size)
        fail(`file ${file.path} changed after validation`);
      // `wx` refuses to write through anything planted since the cleanup above.
      await writeFile(join(agentDir, file.path), await handle.readFile(), {
        flag: "wx",
        mode: file.executable ? 0o700 : 0o600,
      });
    } finally {
      await handle.close();
    }
  }
  return agentDir;
};

/** Resumed omp sessions keep the model they were using. */
export const workerModelArguments = (
  defaultModel: string | undefined,
  resuming: boolean,
): ReadonlyArray<string> =>
  defaultModel === undefined || resuming ? [] : ["--model", defaultModel];

/** Values for worker env only; omp itself never sees an Anthropic endpoint. */
export const workerModelEnvironment = (routing: {
  readonly cliproxyApiBaseUrl: string | undefined;
  readonly cliproxyApiKey: string | undefined;
  readonly claudeExecutable: string;
}): Record<string, string> =>
  routing.cliproxyApiBaseUrl === undefined ||
  routing.cliproxyApiKey === undefined
    ? {}
    : {
        CLIPROXYAPI_BASE_URL: routing.cliproxyApiBaseUrl,
        [CLIPROXYAPI_KEY_ENV]: routing.cliproxyApiKey,
        // pi-claude-cli rejects inherited ANTHROPIC_* in login mode, so this
        // also proves omp's environment carries no Anthropic credential. The
        // wrapper maps the CLIProxyAPI values for the Claude Code child only.
        PI_CLAUDE_DRIVER: "cli",
        PI_CLAUDE_AUTH: "claude-login",
        PI_CLAUDE_EXECUTABLE: routing.claudeExecutable,
      };
