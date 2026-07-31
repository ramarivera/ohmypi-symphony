import { LinearClient } from "@linear/sdk";
import { isRecord } from "./linear-helpers.js";

export interface TokenResponse {
  readonly accessToken: string;
  readonly tokenType: string;
  readonly expiresIn: number;
  readonly refreshToken: string;
  readonly scopes: ReadonlyArray<string>;
}

const requireString = (value: Record<string, unknown>, key: string): string => {
  const field = value[key];
  if (typeof field !== "string") {
    throw new Error(`Linear token response missing or invalid ${key}`);
  }
  return field;
};

const parseScopes = (raw: unknown): ReadonlyArray<string> => {
  if (typeof raw === "string") {
    return raw.split(/[,\s]+/).filter(Boolean);
  }
  if (Array.isArray(raw)) {
    return raw.map((item, index) => {
      if (typeof item !== "string") {
        throw new Error(
          `Linear token response scope[${index}] is not a string`,
        );
      }
      return item;
    });
  }
  throw new Error("Linear token response scope is not a string or array");
};

const parseExpiresIn = (raw: unknown): number => {
  if (typeof raw === "number" && Number.isFinite(raw) && raw > 0) return raw;
  if (typeof raw === "string") {
    const parsed = Number.parseInt(raw, 10);
    if (Number.isSafeInteger(parsed) && parsed > 0) return parsed;
  }
  throw new Error("Linear token response missing or invalid expires_in");
};

export const parseTokenResponse = (value: unknown): TokenResponse => {
  if (!isRecord(value)) {
    throw new Error("Linear token response is not an object");
  }
  const accessToken = requireString(value, "access_token");
  const tokenType = requireString(value, "token_type");
  const expiresIn = parseExpiresIn(value.expires_in);
  const refreshToken = requireString(value, "refresh_token");
  const scopes = parseScopes(value.scope);
  if (tokenType.toLowerCase() !== "bearer") {
    throw new Error(`Unexpected token type ${tokenType}`);
  }
  return { accessToken, tokenType, expiresIn, refreshToken, scopes };
};

export const discoverAppInstallation = async (
  accessToken: string,
): Promise<{ readonly organizationId: string; readonly appUserId: string }> => {
  const client = new LinearClient({ accessToken });
  const viewer = await client.viewer;
  if (!viewer.app) throw new Error("Linear viewer is not an app user");
  const organization = await viewer.organization;
  return { organizationId: organization.id, appUserId: viewer.id };
};

export const buildInstallationRecord = (
  token: TokenResponse,
  organizationId: string,
  appUserId: string,
  now: number,
) => ({
  organizationId,
  appUserId,
  accessToken: token.accessToken,
  refreshToken: token.refreshToken,
  expiresAt: now + token.expiresIn * 1_000,
  scopes: token.scopes,
  revokedAt: null,
  accessibleTeamIds: null,
  canAccessAllPublicTeams: null,
});
