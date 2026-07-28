import pino from "pino";
import type { LogLevel } from "./domain";

export function validLogLevel(
  value: string | undefined,
  fallback: LogLevel = "info",
): LogLevel {
  if (value === undefined || value.length === 0) return fallback;
  const trimmed = value.trim().toLowerCase();
  switch (trimmed) {
    case "trace":
    case "debug":
    case "info":
    case "warn":
    case "error":
    case "fatal":
    case "silent":
      return trimmed;
    default:
      return fallback;
  }
}

function redactPaths(): string[] {
  const keys = [
    "Authorization",
    "authorization",
    "Cookie",
    "cookie",
    "accessToken",
    "access_token",
    "refreshToken",
    "refresh_token",
    "clientId",
    "client_id",
    "clientSecret",
    "client_secret",
    "clientToken",
    "client_token",
    "webhookSecret",
    "webhook_secret",
    "webhookToken",
    "webhook_token",
    "token",
    "Token",
    "TOKEN",
    "apiKey",
    "api_key",
    "apiToken",
    "api_token",
    "authToken",
    "auth_token",
    "bearerToken",
    "bearer_token",
    "tokenSecret",
    "token_secret",
    "secret",
    "Secret",
    "SECRET",
    "password",
    "Password",
    "PASSWORD",
    "x-api-key",
    "X-Api-Key",
    "x-authorization",
    "X-Authorization",
    "x-token",
    "X-Token",
  ];
  const prefixes = [
    "",
    "*.",
    "*.*.",
    "*.*.*.",
    "*.*.*.*.",
    "*.*.*.*.*.",
    "*.*.*.*.*.*.",
  ];
  return keys.flatMap((key) =>
    prefixes.map((prefix) => (prefix ? `${prefix}${key}` : key)),
  );
}

export function createLogger(options?: {
  level?: LogLevel;
  name?: string;
  stream?: pino.DestinationStream;
}): pino.Logger {
  const pinoOptions: pino.LoggerOptions = {
    level: options?.level ?? validLogLevel(Bun.env.LOG_LEVEL),
    redact: {
      paths: redactPaths(),
      censor: "[REDACTED]",
    },
  };
  if (options?.name) {
    pinoOptions.name = options.name;
  }
  if (options?.stream) {
    return pino(pinoOptions, options.stream);
  }
  return pino(pinoOptions);
}

export type Logger = pino.Logger;
