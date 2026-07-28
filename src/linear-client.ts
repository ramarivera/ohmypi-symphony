import { AgentActivitySignal, LinearClient } from "@linear/sdk";
import type {
  GatewayConfig,
  InstallationRecord,
  LinearActivityContent,
  LinearGatewayPort,
} from "./domain";
import type { Logger } from "./logger";
import { createLogger } from "./logger";
import type { GatewayStore } from "./store";

const LINEAR_TOKEN_URL = "https://api.linear.app/oauth/token";
const TOKEN_REFRESH_BUFFER_MS = 5 * 60 * 1000;

export type TokenResponse = {
  readonly accessToken: string;
  readonly tokenType: string;
  readonly expiresIn: number;
  readonly refreshToken: string;
  readonly scopes: readonly string[];
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function isNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function requireString(value: Record<string, unknown>, key: string): string {
  const field = value[key];
  if (!isString(field))
    throw new Error(`Linear token response missing or invalid ${key}`);
  return field;
}

function parseScopes(raw: unknown): readonly string[] {
  if (isString(raw)) {
    return raw.split(/[,\s]+/).filter(Boolean);
  }
  if (Array.isArray(raw)) {
    return raw.map((item, index) => {
      if (!isString(item))
        throw new Error(
          `Linear token response scope[${index}] is not a string`,
        );
      return item;
    });
  }
  throw new Error("Linear token response scope is not a string or array");
}

function parseExpiresIn(raw: unknown): number {
  if (isNumber(raw) && raw > 0) return raw;
  if (isString(raw)) {
    const parsed = Number.parseInt(raw, 10);
    if (Number.isSafeInteger(parsed) && parsed > 0) return parsed;
  }
  throw new Error("Linear token response missing or invalid expires_in");
}

export function parseTokenResponse(value: unknown): TokenResponse {
  if (!isRecord(value))
    throw new Error("Linear token response is not an object");
  const accessToken = requireString(value, "access_token");
  const tokenType = requireString(value, "token_type");
  const expiresIn = parseExpiresIn(value.expires_in);
  const refreshToken = requireString(value, "refresh_token");
  const scopes = parseScopes(value.scope);
  if (tokenType.toLowerCase() !== "bearer")
    throw new Error(`Unexpected token type ${tokenType}`);
  return { accessToken, tokenType, expiresIn, refreshToken, scopes };
}

async function exchangeToken(
  config: Pick<GatewayConfig, "linearClientId" | "linearClientSecret">,
  input: {
    grantType: string;
    code?: string;
    refreshToken?: string;
    redirectUri?: string;
  },
): Promise<TokenResponse> {
  const body = new URLSearchParams();
  body.set("client_id", config.linearClientId);
  body.set("client_secret", config.linearClientSecret);
  body.set("grant_type", input.grantType);
  if (input.code) body.set("code", input.code);
  if (input.refreshToken) body.set("refresh_token", input.refreshToken);
  if (input.redirectUri) body.set("redirect_uri", input.redirectUri);

  const response = await fetch(LINEAR_TOKEN_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Linear token exchange failed (${response.status}): ${text}`,
    );
  }
  const raw: unknown = await response.json();
  return parseTokenResponse(raw);
}

export function exchangeAuthorizationCode(
  config: GatewayConfig,
  code: string,
  redirectUri: string,
): Promise<TokenResponse> {
  return exchangeToken(config, {
    grantType: "authorization_code",
    code,
    redirectUri,
  });
}

export async function discoverAppInstallation(
  accessToken: string,
): Promise<{ organizationId: string; appUserId: string }> {
  const client = new LinearClient({ accessToken });
  const viewer = await client.viewer;
  if (!viewer.app) throw new Error("Linear viewer is not an app user");
  const organization = await viewer.organization;
  return { organizationId: organization.id, appUserId: viewer.id };
}

export function buildInstallationRecord(
  token: TokenResponse,
  organizationId: string,
  appUserId: string,
  now = Date.now(),
): InstallationRecord {
  return {
    organizationId,
    appUserId,
    accessToken: token.accessToken,
    refreshToken: token.refreshToken,
    expiresAt: now + token.expiresIn * 1000,
    scopes: token.scopes,
    revokedAt: null,
    accessibleTeamIds: null,
    canAccessAllPublicTeams: null,
  };
}

function mapActivitySignal(
  signal: "auth" | "continue" | "select" | "stop",
): AgentActivitySignal {
  switch (signal) {
    case "auth":
      return AgentActivitySignal.Auth;
    case "continue":
      return AgentActivitySignal.Continue;
    case "select":
      return AgentActivitySignal.Select;
    case "stop":
      return AgentActivitySignal.Stop;
  }
}

function mapActivityContent(
  content: LinearActivityContent,
): Record<string, unknown> {
  switch (content.type) {
    case "thought":
      return { type: "thought", body: content.body ?? "" };
    case "action": {
      const action = content.action ?? "";
      const parameter = content.parameter ?? "";
      if (!action) throw new Error("Linear action activity requires an action");
      if (!parameter)
        throw new Error("Linear action activity requires a parameter");
      const payload: Record<string, unknown> = {
        type: "action",
        action,
        parameter,
      };
      if (content.result !== undefined) payload.result = content.result;
      return payload;
    }
    case "elicitation":
      return { type: "elicitation", body: content.body ?? "" };
    case "response":
      return { type: "response", body: content.body ?? "" };
    case "error":
      return { type: "error", body: content.body ?? "" };
  }
}

function rateLimitDelay(error: unknown): number | null {
  if (!isRecord(error)) return null;
  const type = error.type;
  const status = error.status ?? error.statusCode;
  if (type !== "Ratelimited" && status !== 429) return null;
  const retryAfter = error.retryAfter;
  return typeof retryAfter === "number" && Number.isFinite(retryAfter)
    ? Math.min(60_000, Math.max(0, retryAfter * 1_000))
    : 1_000;
}

class LinearGateway implements LinearGatewayPort {
  readonly #config: GatewayConfig;
  readonly #store: GatewayStore;
  readonly #refreshing: Map<string, Promise<InstallationRecord>> = new Map();
  readonly #apiQueues = new Map<string, Promise<void>>();
  #logger: Logger;

  constructor(config: GatewayConfig, store: GatewayStore, logger?: Logger) {
    this.#config = config;
    this.#store = store;
    this.#logger =
      logger ?? createLogger({ name: "linear" }).child({ component: "linear" });
  }

  async #performRefresh(
    record: InstallationRecord,
    now: number,
  ): Promise<InstallationRecord> {
    try {
      const response = await exchangeToken(this.#config, {
        grantType: "refresh_token",
        refreshToken: record.refreshToken,
      });
      const updated: InstallationRecord = {
        organizationId: record.organizationId,
        appUserId: record.appUserId,
        accessToken: response.accessToken,
        refreshToken: response.refreshToken,
        expiresAt: now + response.expiresIn * 1000,
        scopes: response.scopes,
        revokedAt: null,
        accessibleTeamIds: record.accessibleTeamIds,
        canAccessAllPublicTeams: record.canAccessAllPublicTeams,
      };
      await this.#store.putInstallation(updated);
      this.#logger.debug({
        event: "linear.token.refresh",
        organizationId: record.organizationId,
        expiresAt: updated.expiresAt,
      });
      return updated;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.#logger.error({
        event: "linear.token.refresh.failed",
        organizationId: record.organizationId,
        error: message,
      });
      throw error;
    }
  }

  async #refreshTokens(
    record: InstallationRecord,
    now: number,
  ): Promise<InstallationRecord> {
    const inFlight = this.#refreshing.get(record.organizationId);
    if (inFlight) return inFlight;
    const promise = this.#performRefresh(record, now).finally(() => {
      this.#refreshing.delete(record.organizationId);
    });
    this.#refreshing.set(record.organizationId, promise);
    return promise;
  }

  async #ensureToken(
    organizationId: string,
    now = Date.now(),
  ): Promise<InstallationRecord> {
    const record = await this.#store.getInstallation(organizationId);
    if (!record)
      throw new Error(`No Linear installation for ${organizationId}`);
    if (record.revokedAt !== null)
      throw new Error(`Linear installation for ${organizationId} is revoked`);
    if (record.expiresAt - TOKEN_REFRESH_BUFFER_MS > now) return record;
    return this.#refreshTokens(record, now);
  }

  async #clientFor(
    organizationId: string,
    now = Date.now(),
  ): Promise<LinearClient> {
    const record = await this.#ensureToken(organizationId, now);
    return new LinearClient({ accessToken: record.accessToken });
  }

  async #schedule<T>(
    organizationId: string,
    operation: () => Promise<T>,
  ): Promise<T> {
    const previous = this.#apiQueues.get(organizationId) ?? Promise.resolve();
    const run = previous
      .catch(() => undefined)
      .then(async () => {
        try {
          return await operation();
        } catch (error) {
          const delay = rateLimitDelay(error);
          if (delay === null) {
            const message =
              error instanceof Error ? error.message : String(error);
            this.#logger.error({
              event: "linear.api.activity.failed",
              organizationId,
              error: message,
            });
            throw error;
          }
          this.#logger.debug({
            event: "linear.api.rateLimited",
            organizationId,
            delay,
          });
          await Bun.sleep(delay);
          return operation();
        }
      });
    const tail = run.then(
      () => undefined,
      () => undefined,
    );
    this.#apiQueues.set(organizationId, tail);
    try {
      return await run;
    } finally {
      if (this.#apiQueues.get(organizationId) === tail) {
        this.#apiQueues.delete(organizationId);
      }
    }
  }

  async createActivity(input: {
    sessionId: string;
    content: LinearActivityContent;
    ephemeral?: boolean;
    signal?: "auth" | "continue" | "select" | "stop";
    signalMetadata?: Record<string, unknown>;
  }): Promise<string> {
    const run = this.#store.getRun(input.sessionId);
    if (!run) throw new Error(`Unknown run ${input.sessionId}`);
    return this.#schedule(run.organizationId, async () => {
      const client = await this.#clientFor(run.organizationId);
      const activityInput: Parameters<LinearClient["createAgentActivity"]>[0] =
        {
          agentSessionId: input.sessionId,
          content: mapActivityContent(input.content),
        };
      if (input.ephemeral !== undefined)
        activityInput.ephemeral = input.ephemeral;
      if (input.signal !== undefined)
        activityInput.signal = mapActivitySignal(input.signal);
      if (input.signalMetadata !== undefined)
        activityInput.signalMetadata = input.signalMetadata;
      const payload = await client.createAgentActivity(activityInput);
      if (!payload.success)
        throw new Error("Linear failed to create agent activity");
      const id = payload.agentActivity
        ? (await payload.agentActivity).id
        : undefined;
      if (!id) throw new Error("Linear did not return an agent activity id");
      return id;
    });
  }

  async updateSession(input: {
    sessionId: string;
    plan?: Record<string, unknown>;
    externalUrls?: readonly { label: string; url: string }[];
  }): Promise<void> {
    const run = this.#store.getRun(input.sessionId);
    if (!run) throw new Error(`Unknown run ${input.sessionId}`);
    await this.#schedule(run.organizationId, async () => {
      const client = await this.#clientFor(run.organizationId);
      const updateInput: Parameters<LinearClient["updateAgentSession"]>[1] = {};
      if (input.plan !== undefined) updateInput.plan = input.plan;
      if (input.externalUrls !== undefined) {
        updateInput.externalUrls = input.externalUrls.map((url) => ({
          label: url.label,
          url: url.url,
        }));
      }
      const payload = await client.updateAgentSession(
        input.sessionId,
        updateInput,
      );
      if (!payload.success)
        throw new Error("Linear failed to update agent session");
    });
  }

  async refreshInstallation(organizationId: string): Promise<string> {
    const record = await this.#ensureToken(organizationId, Date.now());
    return record.accessToken;
  }
}

export function createLinearGateway(
  config: GatewayConfig,
  store: GatewayStore,
  logger?: Logger,
): LinearGatewayPort {
  return new LinearGateway(config, store, logger);
}
