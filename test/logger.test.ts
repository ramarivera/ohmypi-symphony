import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Writable } from "node:stream";
import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  ConfigProvider,
  Effect,
  FiberId,
  FiberRefs,
  HashMap,
  Layer,
  List,
  Logger,
  LogLevel,
  Option,
  Schema,
} from "effect";
import pino from "pino";
import { GatewayConfig } from "../src/services/config.js";
import {
  buildPinoLoggerPlan,
  GatewayLogger,
  makePinoEffectLogger,
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
type GatewayLogLevel = (typeof LOG_LEVELS)[number];

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

function makeConfigLayer(
  logLevel?: string,
  overrides: ReadonlyMap<string, string> = new Map(),
) {
  const values = new Map([
    ...baseConfigValues,
    ...(logLevel === undefined ? [] : [["LOG_LEVEL", logLevel] as const]),
    ...overrides,
  ]);
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

it.effect("preserves span, annotation, level, and Cause correlation", () => {
  const capture = makeCapture();
  const stream = new Writable({
    write(chunk, _encoding, callback) {
      capture.chunks.push(chunk.toString());
      callback();
    },
  });
  const bridge = makePinoEffectLogger(pino({ base: null }, stream));
  const bridgeLive = Logger.replace(Logger.defaultLogger, bridge);

  return Effect.gen(function* () {
    yield* Effect.logWarning("correlated warning").pipe(
      Effect.annotateLogs({ operation: "gateway.test" }),
      Effect.withSpan("GatewayLogger.correlation"),
    );
    yield* Effect.sync(() =>
      bridge.log({
        fiberId: FiberId.none,
        logLevel: LogLevel.Error,
        message: "controlled failure",
        cause: Cause.fail("database unavailable"),
        context: FiberRefs.empty(),
        spans: List.empty(),
        annotations: HashMap.fromIterable([
          ["errorTag", "@Gateway/DatabaseError"],
        ]),
        date: new Date(),
      }),
    );

    const correlated = JSON.parse(capture.chunks[0] ?? "") as CapturedMessage;
    const failure = JSON.parse(capture.chunks[1] ?? "") as CapturedMessage;
    expect(correlated).toMatchObject({
      level: 40,
      msg: "correlated warning",
      operation: "gateway.test",
      span_name: "GatewayLogger.correlation",
    });
    expect(typeof correlated.trace_id).toBe("string");
    expect(typeof correlated.span_id).toBe("string");
    expect(failure).toMatchObject({
      level: 50,
      msg: "controlled failure",
      errorTag: "@Gateway/DatabaseError",
      uncorrelated: true,
    });
    expect(String(failure.cause)).toContain("database unavailable");
  }).pipe(Effect.provide(bridgeLive));
});

describe("Pino logger plan", () => {
  it("uses synchronous NDJSON stdout when file logging is disabled", () => {
    const plan = buildPinoLoggerPlan({
      logLevel: "info",
      logFile: Option.none(),
    });
    expect(plan.transport).toBeUndefined();
    expect(plan.options.level).toBe("info");
    expect(plan.options.redact).toEqual({
      paths: redactionPaths,
      censor: "[REDACTED]",
    });
  });

  it("fans the same NDJSON line to stdout and pino-roll when configured", () => {
    const plan = buildPinoLoggerPlan({
      logLevel: "warn",
      logFile: Option.some({
        path: "/var/log/gateway.ndjson",
        frequency: "daily",
        size: "25m",
        limit: 14,
      }),
    });
    expect(plan.transport?.targets).toEqual([
      {
        target: "pino/file",
        level: "warn",
        options: { destination: 1 },
      },
      {
        target: "pino-roll",
        level: "warn",
        options: {
          file: "/var/log/gateway.ndjson",
          frequency: "daily",
          size: "25m",
          limit: { count: 14 },
          mkdir: true,
        },
      },
    ]);
  });

  it("writes the same redacted NDJSON line to stdout and a rotated file, then closes it", async () => {
    const directory = await mkdtemp(join(tmpdir(), "ohmypi-gateway-logs-"));
    const filePath = join(directory, "gateway.ndjson");
    try {
      const script = `
        import { ConfigProvider, Effect, Layer } from "effect";
        import { GatewayLogger, PinoLoggerLive } from "./src/services/logger.ts";

        const configProvider = Layer.setConfigProvider(
          ConfigProvider.fromMap(
            new Map([
              ["LINEAR_CLIENT_ID", "client"],
              ["LINEAR_CLIENT_SECRET", "client-secret"],
              ["LINEAR_WEBHOOK_SECRET", "webhook-secret"],
              ["TOKEN_ENCRYPTION_KEY", Buffer.from(new Uint8Array(32).fill(7)).toString("base64")],
              ["PUBLIC_URL", "http://localhost:3000"],
              ["LOG_LEVEL", "info"],
              ["LOG_FILE", ${JSON.stringify(filePath)}],
              ["LOG_FILE_FREQUENCY", "daily"],
              ["LOG_FILE_SIZE", "25m"],
              ["LOG_FILE_LIMIT", "14"],
            ]),
          ),
        );
        const loggerLive = PinoLoggerLive.pipe(
          Layer.provide(GatewayLogger.Default),
          Layer.provide(configProvider),
        );
        await Effect.runPromise(
          Effect.scoped(
            Effect.logInfo("logger.fanout").pipe(
              Effect.annotateLogs({
                event: "logger.fanout",
                token: "private-token",
                nested: { authorization: "Bearer private-token" },
                public: "visible",
              }),
              Effect.withSpan("GatewayLogger.fanout"),
              Effect.provide(loggerLive),
            ),
          ),
        );
      `;
      const child = Bun.spawn([process.execPath, "--eval", script], {
        cwd: process.cwd(),
        stdout: "pipe",
        stderr: "pipe",
      });
      const stdoutPromise = new Response(child.stdout).text();
      const stderrPromise = new Response(child.stderr).text();
      const [exitCode, stdout, stderr] = await Promise.all([
        child.exited,
        stdoutPromise,
        stderrPromise,
      ]);
      expect(exitCode, stderr).toBe(0);

      const files = await readdir(directory);
      expect(files).toEqual(["gateway.1.ndjson"]);
      const fileOutput = await readFile(
        join(directory, files[0] ?? ""),
        "utf8",
      );
      const stdoutLine = JSON.parse(stdout.trim()) as CapturedMessage;
      const fileLine = JSON.parse(fileOutput.trim()) as CapturedMessage;
      expect(stdoutLine).toEqual(fileLine);
      expect(fileLine).toMatchObject({
        level: 30,
        msg: "logger.fanout",
        event: "logger.fanout",
        token: "[REDACTED]",
        nested: { authorization: "[REDACTED]" },
        public: "visible",
        span_name: "GatewayLogger.fanout",
      });
      expect(typeof fileLine.trace_id).toBe("string");
      expect(typeof fileLine.span_id).toBe("string");
    } finally {
      await rm(directory, { force: true, recursive: true });
    }
  });

  it.effect.prop(
    "preserves rotation bounds for generated supported log-file configurations",
    {
      frequency: Schema.Literal("daily", "hourly"),
      size: Schema.Literal("1b", "25m", "1g"),
      limit: Schema.Literal(1, 14, 100),
    },
    ({ frequency, size, limit }) =>
      Effect.sync(() => {
        const plan = buildPinoLoggerPlan({
          logLevel: "info",
          logFile: Option.some({
            path: "/var/log/gateway.ndjson",
            frequency,
            size,
            limit,
          }),
        });
        const rollTarget = plan.transport?.targets[1];
        expect(rollTarget).toMatchObject({
          target: "pino-roll",
          options: {
            frequency,
            size,
            limit: { count: limit },
            mkdir: true,
          },
        });
      }),
    { fastCheck: { numRuns: 20 } },
  );
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
        Schema.filter((value) => {
          const normalized = value.trim().toLowerCase();
          return (
            normalized.length > 0 &&
            !LOG_LEVELS.includes(normalized as GatewayLogLevel)
          );
        }),
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
        expect(invalidResult._tag).toBe("Right");
        if (invalidResult._tag === "Right") {
          expect(invalidResult.right.logLevel).toBe("info");
        }
      }),
    { fastCheck: { numRuns: 20 } },
  );
});

describe("GatewayConfig file logging", () => {
  it.effect(
    "defaults to disabled rotation and validates enabled settings",
    () =>
      Effect.gen(function* () {
        const defaultConfig = yield* GatewayConfig;
        expect(Option.isNone(defaultConfig.logFile)).toBe(true);

        const filePath = join(tmpdir(), "gateway.ndjson");
        const configured = yield* GatewayConfig.pipe(
          Effect.provide(
            makeConfigLayer(
              undefined,
              new Map([
                ["LOG_FILE", filePath],
                ["LOG_FILE_FREQUENCY", "hourly"],
                ["LOG_FILE_SIZE", "1g"],
                ["LOG_FILE_LIMIT", "2"],
              ]),
            ),
          ),
        );
        expect(configured.logFile).toEqual(
          Option.some({
            path: filePath,
            frequency: "hourly",
            size: "1g",
            limit: 2,
          }),
        );
      }).pipe(Effect.provide(makeConfigLayer())),
  );

  it.effect("rejects invalid rotation settings before startup", () =>
    Effect.gen(function* () {
      const invalidFrequency = yield* Effect.either(
        GatewayConfig.pipe(
          Effect.provide(
            makeConfigLayer(
              undefined,
              new Map([["LOG_FILE_FREQUENCY", "weekly"]]),
            ),
          ),
        ),
      );
      const invalidSize = yield* Effect.either(
        GatewayConfig.pipe(
          Effect.provide(
            makeConfigLayer(undefined, new Map([["LOG_FILE_SIZE", "zero"]])),
          ),
        ),
      );
      const invalidLimit = yield* Effect.either(
        GatewayConfig.pipe(
          Effect.provide(
            makeConfigLayer(undefined, new Map([["LOG_FILE_LIMIT", "0"]])),
          ),
        ),
      );
      expect(invalidFrequency._tag).toBe("Left");
      expect(invalidSize._tag).toBe("Left");
      expect(invalidLimit._tag).toBe("Left");
    }).pipe(Effect.provide(makeConfigLayer())),
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
