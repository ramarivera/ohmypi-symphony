import { describe, expect, it, test } from "@effect/vitest"
import { Writable } from "node:stream";
import { Schema } from "effect";
import type pino from "pino";
import { createLogger, validLogLevel } from "../src/logger";

function captureStream(): {
  stream: pino.DestinationStream;
  chunks: string[];
  last(): Record<string, unknown> | null;
} {
  const chunks: string[] = [];
  const stream = new Writable({
    write(chunk, _encoding, callback) {
      chunks.push(chunk.toString());
      callback();
    },
  }) as pino.DestinationStream;
  return {
    stream,
    chunks,
    last() {
      if (chunks.length === 0) return null;
      const line = chunks.at(-1);
      return line ? (JSON.parse(line) as Record<string, unknown>) : null;
    },
  };
}

describe("createLogger", () => {
  test("gates messages by level", () => {
    const { stream, chunks } = captureStream();
    const logger = createLogger({ level: "info", stream });
    logger.trace({ event: "trace.ignored" });
    logger.info({ event: "info.kept" });
    logger.error({ event: "error.kept" });
    expect(chunks).toHaveLength(2);
    expect(JSON.parse(chunks[0] ?? "{}").event).toBe("info.kept");
    expect(JSON.parse(chunks[1] ?? "{}").event).toBe("error.kept");
  });

  test("redacts sensitive keys", () => {
    const { stream, last } = captureStream();
    const logger = createLogger({ level: "info", stream });
    logger.info({
      authorization: "Bearer abc",
      cookie: "session=secret",
      accessToken: "at",
      refreshToken: "rt",
      clientSecret: "cs",
      webhookSecret: "ws",
      token: "t",
      nested: { token: "inner", deep: { authorization: "auth" } },
    });
    const message = last();
    expect(message).not.toBeNull();
    expect(message?.authorization).toBe("[REDACTED]");
    expect(message?.cookie).toBe("[REDACTED]");
    expect(message?.accessToken).toBe("[REDACTED]");
    expect(message?.refreshToken).toBe("[REDACTED]");
    expect(message?.clientSecret).toBe("[REDACTED]");
    expect(message?.webhookSecret).toBe("[REDACTED]");
    expect(message?.token).toBe("[REDACTED]");
    expect(message?.nested).toMatchObject({
      token: "[REDACTED]",
      deep: { authorization: "[REDACTED]" },
    });
  });

  test("does not redact unrelated fields", () => {
    const { stream, last } = captureStream();
    const logger = createLogger({ level: "info", stream });
    logger.info({ event: "ok", public: "visible" });
    const message = last();
    expect(message).not.toBeNull();
    expect(message?.public).toBe("visible");
  });

  test("children include base bindings", () => {
    const { stream, last } = captureStream();
    const logger = createLogger({ level: "info", stream });
    const child = logger.child({ component: "test" });
    child.info({ event: "child.log" });
    const message = last();
    expect(message).not.toBeNull();
    expect(message?.component).toBe("test");
    expect(message?.event).toBe("child.log");
  });
});

describe("validLogLevel", () => {
  test("returns valid lowercased values", () => {
    expect(validLogLevel("TRACE")).toBe("trace");
    expect(validLogLevel("  Info ")).toBe("info");
    expect(validLogLevel("error")).toBe("error");
  });

  test("returns the fallback for unknown or empty values", () => {
    expect(validLogLevel(undefined)).toBe("info");
    expect(validLogLevel("")).toBe("info");
    expect(validLogLevel("verbose", "debug")).toBe("debug");
  });
});

describe("validLogLevel invariants", () => {
  const VALID_LEVELS: Record<string, true> = {
    trace: true,
    debug: true,
    info: true,
    warn: true,
    error: true,
    fatal: true,
    silent: true,
  };

  it.prop(
    "returns the normalized value for valid levels and the fallback otherwise",
    {
      value: Schema.String,
      fallback: Schema.Literal("trace", "debug", "info", "warn", "error", "fatal", "silent"),
    },
    ({ value, fallback }) => {
      const normalized = value.trim().toLowerCase();
      const expected = VALID_LEVELS[normalized] === true ? normalized : fallback;
      expect(validLogLevel(value, fallback)).toBe(expected);
    },
  );

});
