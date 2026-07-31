import { Writable } from "node:stream";
import { describe, expect, it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Schema } from "effect";
import pino from "pino";
import { GatewayConfig } from "../src/services/config.js";
import {
  GatewayLogger,
  PinoLoggerLive,
  redactionPaths,
} from "../src/services/logger.js";

const LOG_LEVELS = [
  "trace",
  "debug",
  "info",
  "warn",
  "error",
  "fatal",
  "silent",
] as const;
type LogLevel = (typeof LOG_LEVELS)[number];

const baseConfigValues = new Map<string, string>([
  ["LINEAR_CLIENT_ID", "client"],
  ["LINEAR_CLIENT_SECRET", "client-secret"],
  ["LINEAR_WEBHOOK_SECRET", "webhook-secret"],
  [
    "TOKEN_ENCRYPTION_KEY",
    Buffer.from(new Uint8Array(32).fill(7)).toString("base64"),
  ],
  ["PUBLIC_URL", "http://localhost:3000"],
]);

type CapturedMessage = Record<string, unknown>;

type Capture = {
  readonly chunks: string[];
  last(): CapturedMessage | null;
};

function makeCapture(): Capture {
  const chunks: string[] = [];
  return {
    chunks,
    last() {
      const line = chunks.at(-1);
      return line ? (JSON.parse(line) as CapturedMessage) : null;
    },
  };
}

function makeConfigLayer(logLevel?: string) {
  const values =
    logLevel === undefined
      ? baseConfigValues
      : new Map([...baseConfigValues, ["LOG_LEVEL", logLevel]]);
  return GatewayConfig.Default.pipe(
    Layer.provide(Layer.setConfigProvider(ConfigProvider.fromMap(values))),
  );
}

function makeLoggerLayer(logLevel: string, capture: Capture) {
  const configLayer = makeConfigLayer(logLevel);
  const gatewayLayer = Layer.scoped(
    GatewayLogger,
    Effect.gen(function* () {
      const config = yield* GatewayConfig;
      const stream = new Writable({
        write(chunk, _encoding, callback) {
          capture.chunks.push(chunk.toString());
          callback();
        },
      });
      const logger = yield* Effect.acquireRelease(
        Effect.sync(() =>
          pino(
            {
              name: "gateway",
              level: config.logLevel,
              redact: { paths: redactionPaths, censor: "[REDACTED]" },
            },
            stream,
          ),
        ),
        (logger) =>
          Effect.sync(() => {
            logger.flush();
            stream.end();
          }),
      );
      return new GatewayLogger({ logger });
    }),
  );
  return PinoLoggerLive.pipe(
    Layer.provide(gatewayLayer),
    Layer.provide(configLayer),
  );
}

describe("GatewayLogger and PinoLoggerLive", () => {
  it.effect("gates messages by level", () => {
    const capture = makeCapture();
    return Effect.gen(function* () {
      yield* Effect.logTrace("trace").pipe(
        Effect.annotateLogs("event", "trace.ignored"),
      );
      yield* Effect.logInfo("info").pipe(
        Effect.annotateLogs("event", "info.kept"),
      );
      yield* Effect.logError("error").pipe(
        Effect.annotateLogs("event", "error.kept"),
      );
      expect(capture.chunks).toHaveLength(2);
      expect(
        capture.chunks[0] ? JSON.parse(capture.chunks[0]).event : undefined,
      ).toBe("info.kept");
      expect(
        capture.chunks[1] ? JSON.parse(capture.chunks[1]).event : undefined,
      ).toBe("error.kept");
    }).pipe(Effect.provide(makeLoggerLayer("info", capture)));
  });

  it.effect("redacts sensitive keys", () => {
    const capture = makeCapture();
    return Effect.logInfo("redaction-check").pipe(
      Effect.annotateLogs({
        authorization: "Bearer abc",
        cookie: "session=secret",
        accessToken: "at",
        refreshToken: "rt",
        clientSecret: "cs",
        webhookSecret: "ws",
        token: "t",
        nested: { token: "inner", deep: { authorization: "auth" } },
      }),
      Effect.tap(() => {
        const message = capture.last();
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
      }),
      Effect.provide(makeLoggerLayer("info", capture)),
    );
  });

  it.effect("does not redact unrelated fields", () => {
    const capture = makeCapture();
    return Effect.logInfo("public-check").pipe(
      Effect.annotateLogs({ event: "ok", public: "visible" }),
      Effect.tap(() => {
        const message = capture.last();
        expect(message).not.toBeNull();
        expect(message?.public).toBe("visible");
      }),
      Effect.provide(makeLoggerLayer("info", capture)),
    );
  });

  it.effect("children include base bindings", () => {
    const capture = makeCapture();
    return Effect.logInfo("child.log").pipe(
      Effect.annotateLogs({ component: "test", event: "child.log" }),
      Effect.tap(() => {
        const message = capture.last();
        expect(message).not.toBeNull();
        expect(message?.component).toBe("test");
        expect(message?.event).toBe("child.log");
      }),
      Effect.provide(makeLoggerLayer("info", capture)),
    );
  });
});

describe("GatewayConfig log-level normalization", () => {
  it.effect("uses info as the fallback when LOG_LEVEL is absent", () =>
    Effect.gen(function* () {
      const config = yield* GatewayConfig;
      expect(config.logLevel).toBe("info");
    }).pipe(Effect.provide(makeConfigLayer())),
  );

  it.effect.prop(
    "accepts every canonical valid level and rejects generated invalid levels",
    {
      valid: Schema.Literal(...LOG_LEVELS),
      invalid: Schema.String.pipe(
        Schema.minLength(1),
        Schema.filter((value) => !LOG_LEVELS.includes(value as LogLevel)),
      ),
    },
    ({ valid, invalid }) =>
      Effect.gen(function* () {
        const validResult = yield* Effect.either(
          Effect.gen(function* () {
            return yield* GatewayConfig;
          }).pipe(Effect.provide(makeConfigLayer(valid))),
        );
        expect(validResult._tag).toBe("Right");
        if (validResult._tag === "Right")
          expect(validResult.right.logLevel).toBe(valid);

        const invalidResult = yield* Effect.either(
          Effect.gen(function* () {
            return yield* GatewayConfig;
          }).pipe(Effect.provide(makeConfigLayer(invalid))),
        );
        expect(invalidResult._tag).toBe("Left");
      }),
    { fastCheck: { numRuns: 20 } },
  );
});

describe("redaction invariants", () => {
  it.effect.prop(
    "redacts generated token values while preserving unrelated annotations",
    {
      secret: Schema.String.pipe(Schema.minLength(1)),
      public: Schema.String.pipe(Schema.minLength(1)),
    },
    ({ secret, public: publicValue }) => {
      const capture = makeCapture();
      return Effect.gen(function* () {
        yield* Effect.logInfo("redaction-property").pipe(
          Effect.annotateLogs({
            token: secret,
            nested: { authorization: secret },
            public: publicValue,
          }),
        );
        const message = capture.last();
        expect(message).not.toBeNull();
        expect(message?.token).toBe("[REDACTED]");
        expect(message?.nested).toMatchObject({ authorization: "[REDACTED]" });
        expect(message?.public).toBe(publicValue);
      }).pipe(Effect.provide(makeLoggerLayer("info", capture)));
    },
    { fastCheck: { numRuns: 20 } },
  );
});
