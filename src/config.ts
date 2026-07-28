import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { GatewayConfig } from "./domain";
import { validLogLevel } from "./logger";

function required(
  env: Record<string, string | undefined>,
  name: string,
): string {
  const value = env[name]?.trim();
  if (value) return value;

  const file = env[`${name}_FILE`]?.trim();
  if (file) {
    const fileValue = readFileSync(file, "utf8").trim();
    if (fileValue) return fileValue;
  }

  throw new Error(
    `Missing required environment variable ${name} or ${name}_FILE`,
  );
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
    logLevel: validLogLevel(env.LOG_LEVEL),
    databasePath:
      env.DATABASE_PATH === ":memory:"
        ? ":memory:"
        : resolve(env.DATABASE_PATH ?? "./data/gateway.sqlite"),
    workspaceRoot: resolve(env.WORKSPACE_ROOT ?? "./data/workspaces"),
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
