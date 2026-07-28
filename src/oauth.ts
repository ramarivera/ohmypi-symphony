import { createHash, randomBytes } from "node:crypto";
import type { GatewayConfig, InstallationRecord } from "./domain";
import {
  buildInstallationRecord,
  discoverAppInstallation,
  exchangeAuthorizationCode,
} from "./linear-client";
import type { GatewayStore } from "./store";

const DEFAULT_AUTHORIZE_REDIRECT_PATH = "oauth/callback";
const DEFAULT_STATE_TTL_MS = 10 * 60 * 1000;
const AUTHORIZE_URL = "https://linear.app/oauth/authorize";
const DEFAULT_SCOPES: readonly string[] = [
  "read",
  "write",
  "app:assignable",
  "app:mentionable",
];

function hashState(rawState: string): string {
  return createHash("sha256").update(rawState).digest("base64url");
}

function generateState(): string {
  return randomBytes(32).toString("base64url");
}

function normalizeRedirectPath(path: string): string {
  return path.replace(/^\/+/u, "");
}

function buildRedirectUri(config: GatewayConfig, path: string): string {
  const base = new URL(config.publicUrl.toString().replace(/\/?$/u, "/"));
  const normalized = normalizeRedirectPath(path);
  return new URL(normalized, base).toString();
}

export async function startAuthorization(
  config: GatewayConfig,
  store: GatewayStore,
  redirectPath = DEFAULT_AUTHORIZE_REDIRECT_PATH,
  stateTtlMs = DEFAULT_STATE_TTL_MS,
): Promise<{ state: string; url: URL }> {
  const state = generateState();
  const stateHash = hashState(state);
  const now = Date.now();
  store.createOAuthState(stateHash, now + stateTtlMs, now);

  const redirectUri = buildRedirectUri(config, redirectPath);
  const url = new URL(AUTHORIZE_URL);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("client_id", config.linearClientId);
  url.searchParams.set("redirect_uri", redirectUri);
  url.searchParams.set("state", state);
  url.searchParams.set("scope", DEFAULT_SCOPES.join(","));
  url.searchParams.set("actor", "app");
  url.searchParams.set("prompt", "consent");

  return { state, url };
}

export async function completeAuthorization(
  config: GatewayConfig,
  store: GatewayStore,
  callbackUrl: URL,
): Promise<InstallationRecord> {
  const code = callbackUrl.searchParams.get("code");
  const state = callbackUrl.searchParams.get("state");
  if (!code || !state) throw new Error("Missing OAuth code or state");

  const stateHash = hashState(state);
  const consumed = store.consumeOAuthState(stateHash);
  if (!consumed) throw new Error("Invalid or expired OAuth state");

  const redirectUri = `${callbackUrl.origin}${callbackUrl.pathname}`;
  const token = await exchangeAuthorizationCode(config, code, redirectUri);
  const { organizationId, appUserId } = await discoverAppInstallation(
    token.accessToken,
  );
  const record = buildInstallationRecord(token, organizationId, appUserId);
  await store.putInstallation(record);
  return record;
}
