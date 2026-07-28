import { resolve } from "node:path";
import type {
  GatewayConfig,
  RepositoryDefinition,
  RepositoryMap,
} from "./domain";

function required(
  env: Record<string, string | undefined>,
  name: string,
): string {
  const value = env[name]?.trim();
  if (!value) throw new Error(`Missing required environment variable ${name}`);
  return value;
}

function positiveInteger(
  value: string | undefined,
  fallback: number,
  name: string,
): number {
  const parsed = value === undefined ? fallback : Number.parseInt(value, 10);
  if (!Number.isSafeInteger(parsed) || parsed <= 0)
    throw new Error(`${name} must be a positive integer`);
  return parsed;
}

function decodeKey(value: string): Uint8Array {
  const key = Uint8Array.from(Buffer.from(value, "base64"));
  if (key.byteLength !== 32)
    throw new Error("TOKEN_ENCRYPTION_KEY must be 32 bytes encoded as Base64");
  return key;
}

export function loadConfig(
  env: Record<string, string | undefined> = Bun.env,
): GatewayConfig {
  const publicUrl = new URL(required(env, "PUBLIC_URL"));
  if (publicUrl.protocol !== "https:" && publicUrl.hostname !== "localhost") {
    throw new Error("PUBLIC_URL must use HTTPS except on localhost");
  }
  return {
    linearClientId: required(env, "LINEAR_CLIENT_ID"),
    linearClientSecret: required(env, "LINEAR_CLIENT_SECRET"),
    linearWebhookSecret: required(env, "LINEAR_WEBHOOK_SECRET"),
    tokenEncryptionKey: decodeKey(required(env, "TOKEN_ENCRYPTION_KEY")),
    publicUrl,
    databasePath: resolve(env.DATABASE_PATH ?? "./data/gateway.sqlite"),
    workspaceRoot: resolve(env.WORKSPACE_ROOT ?? "./data/workspaces"),
    repositoryMapPath: resolve(
      env.REPOSITORY_MAP_PATH ?? "./repositories.json",
    ),
    ompCliPath: env.OMP_CLI_PATH?.trim() || "omp",
    port: positiveInteger(env.PORT, 3000, "PORT"),
    leaseDurationMs: positiveInteger(
      env.LEASE_DURATION_MS,
      60_000,
      "LEASE_DURATION_MS",
    ),
    webhookReplayWindowMs: positiveInteger(
      env.WEBHOOK_REPLAY_WINDOW_MS,
      60_000,
      "WEBHOOK_REPLAY_WINDOW_MS",
    ),
  };
}

function stringArray(value: unknown, field: string): readonly string[] {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    throw new Error(`${field} must be an array of strings`);
  }
  return value;
}

function parseRepository(value: unknown, index: number): RepositoryDefinition {
  if (typeof value !== "object" || value === null)
    throw new Error(`repositories[${index}] must be an object`);
  const candidate = value as Record<string, unknown>;
  for (const field of ["id", "url", "ref"] as const) {
    if (typeof candidate[field] !== "string" || candidate[field].length === 0) {
      throw new Error(
        `repositories[${index}].${field} must be a non-empty string`,
      );
    }
  }
  return {
    id: candidate.id as string,
    url: candidate.url as string,
    ref: candidate.ref as string,
    teamIds: stringArray(
      candidate.teamIds ?? [],
      `repositories[${index}].teamIds`,
    ),
    projectIds: stringArray(
      candidate.projectIds ?? [],
      `repositories[${index}].projectIds`,
    ),
  };
}

export async function loadRepositoryMap(path: string): Promise<RepositoryMap> {
  const value: unknown = await Bun.file(path).json();
  if (
    typeof value !== "object" ||
    value === null ||
    !Array.isArray((value as { repositories?: unknown }).repositories)
  ) {
    throw new Error("Repository map must contain a repositories array");
  }
  const repositories = (value as { repositories: unknown[] }).repositories.map(
    parseRepository,
  );
  const ids = new Set<string>();
  for (const repository of repositories) {
    if (ids.has(repository.id))
      throw new Error(`Duplicate repository id ${repository.id}`);
    ids.add(repository.id);
  }
  return { repositories };
}
