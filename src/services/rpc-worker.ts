import type { Subprocess } from "bun";
import { Config, Deferred, Effect, Fiber, Option, Queue, Ref } from "effect";
import {
  RpcProtocolError,
  RpcSpawnError,
  RpcTimeoutError,
} from "../domain/errors.js";

export interface RpcEvent {
  readonly type: string;
  readonly [key: string]: unknown;
}

export interface RpcWorkerHandle {
  readonly sessionId: Effect.Effect<Option.Option<string>, never, never>;
  readonly sessionFile: Effect.Effect<Option.Option<string>, never, never>;
  readonly isStreaming: Effect.Effect<boolean, never, never>;
  readonly start: () => Effect.Effect<
    void,
    RpcSpawnError | RpcTimeoutError | RpcProtocolError,
    never
  >;
  readonly stop: () => Effect.Effect<void, never, never>;
  readonly prompt: (
    message: string,
  ) => Effect.Effect<boolean, RpcProtocolError | RpcTimeoutError, never>;
  readonly steer: (message: string) => Effect.Effect<void, RpcProtocolError, never>;
  readonly followUp: (
    message: string,
  ) => Effect.Effect<void, RpcProtocolError, never>;
  readonly abort: () => Effect.Effect<void, RpcProtocolError, never>;
  readonly getState: () => Effect.Effect<
    Record<string, unknown>,
    RpcProtocolError,
    never
  >;
  readonly respondToUi: (
    requestId: string,
    response:
      | { readonly value: string }
      | { readonly confirmed: boolean }
      | { readonly cancelled: true },
  ) => Effect.Effect<void, RpcProtocolError, never>;
  readonly onEvent: (
    listener: (event: RpcEvent) => void,
  ) => Effect.Effect<() => Effect.Effect<void, never, never>, never, never>;
}

interface PendingRequest {
  readonly command: string;
  readonly deferred: Deferred.Deferred<Record<string, unknown>, RpcProtocolError>;
}

interface ChunkState {
  readonly id: string;
  readonly count: number;
  readonly byteLength: number;
  readonly parts: Uint8Array[];
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function frameId(frame: Record<string, unknown>): string | number | null {
  if (typeof frame.id === "string" || typeof frame.id === "number") {
    return frame.id;
  }
  if (typeof frame.requestId === "string") return frame.requestId;
  if (typeof frame.correlationId === "string") return frame.correlationId;
  return null;
}

function sanitizedEnvironment(
  input: Record<string, string | undefined>,
): Record<string, string> {
  const output: Record<string, string> = {};
  for (const [key, value] of Object.entries(input)) {
    if (value === undefined) continue;
    if (
      key.startsWith("LINEAR_") ||
      key === "TOKEN_ENCRYPTION_KEY" ||
      key === "DATABASE_URL"
    )
      continue;
    output[key] = value;
  }
  return output;
}

const MAX_CHUNK_SIZE = 67_108_864;
const MAX_PROMPT_REQUEST_IDS = 256;

function configWithDefault(
  name: string,
  fallback: string,
): Effect.Effect<string, never, never> {
  return Config.unwrap(Config.string(name).pipe(Config.withDefault(fallback))).pipe(
    Effect.catchTags({ ConfigError: () => Effect.succeed(fallback) }),
  );
}

export class RpcWorker extends Effect.Service<RpcWorker>()("RpcWorker", {
  accessors: true,
  dependencies: [],
  effect: Effect.gen(function* () {
    const spawn = Effect.fn("RpcWorker.spawn")(
      function* (input: {
        readonly command: ReadonlyArray<string>;
        readonly cwd: string;
        readonly env?: Record<string, string | undefined>;
        readonly startTimeoutMs?: number;
      }): Effect.fn.Return<RpcWorkerHandle, RpcSpawnError> {
        if (input.command.length === 0) {
          return yield* Effect.fail(
            new RpcSpawnError({ message: "RPC command cannot be empty" }),
          );
        }

        const command = [...input.command, "--mode", "rpc"];
        const cwd = input.cwd;
        const startTimeoutMs = input.startTimeoutMs ?? 30_000;
        const env =
          input.env ??
          sanitizedEnvironment({
            PATH: yield* configWithDefault("PATH", ""),
            HOME: yield* configWithDefault("HOME", ""),
            TMPDIR: yield* configWithDefault("TMPDIR", "/tmp"),
            XDG_CONFIG_HOME: yield* configWithDefault(
              "XDG_CONFIG_HOME",
              "",
            ),
          });
        const processRef =
          yield* Ref.make<Option.Option<Subprocess<"pipe", "pipe", "pipe">>>(
            Option.none(),
          );
        const sessionIdRef = yield* Ref.make<Option.Option<string>>(
          Option.none(),
        );
        const sessionFileRef = yield* Ref.make<Option.Option<string>>(
          Option.none(),
        );
        const isStreamingRef = yield* Ref.make(false);
        const supportsV2Ref = yield* Ref.make(false);
        const requestSeqRef = yield* Ref.make(0);
        const pendingRef =
          yield* Ref.make<Map<string, PendingRequest>>(new Map());
        const chunkRef = yield* Ref.make<Option.Option<ChunkState>>(
          Option.none(),
        );
        const stderrTailRef = yield* Ref.make("");
        const listenersRef =
          yield* Ref.make<Set<(event: RpcEvent) => void>>(new Set());
        const promptRequestIdsRef = yield* Ref.make<Set<string>>(new Set());
        const ready = yield* Deferred.make<void, RpcSpawnError>();
        const stdoutFiberRef =
          yield* Ref.make<Option.Option<Fiber.Fiber<void, never>>>(
            Option.none(),
          );
        const stderrFiberRef =
          yield* Ref.make<Option.Option<Fiber.Fiber<void, never>>>(
            Option.none(),
          );

        const getProcess = Ref.get(processRef).pipe(
          Effect.flatMap((option) =>
            Option.match(option, {
              onNone: () =>
                Effect.fail(
                  new RpcProtocolError({
                    method: "worker",
                    message: "RPC worker is not running",
                  }),
                ),
              onSome: Effect.succeed,
            }),
          ),
        );

        const rejectPending = (
          error: RpcProtocolError,
        ): Effect.Effect<void, never, never> =>
          Effect.gen(function* () {
            const pending = yield* Ref.get(pendingRef);
            for (const [, request] of pending) {
              yield* Deferred.fail(request.deferred, error);
            }
            yield* Ref.set(pendingRef, new Map());
          });

        const fail = (
          error: unknown,
        ): Effect.Effect<void, never, never> =>
          Effect.gen(function* () {
            const base = error instanceof Error ? error.message : String(error);
            const process = yield* Ref.get(processRef);
            yield* Ref.set(processRef, Option.none());
            yield* Ref.set(isStreamingRef, false);

            if (Option.isSome(process)) {
              const readyDone = yield* Deferred.isDone(ready);
              if (!readyDone) {
                yield* Deferred.fail(
                  ready,
                  new RpcSpawnError({
                    message: `OhMyPi RPC worker failed before ready: ${base}`,
                  }),
                );
              }

              yield* Effect.try(() => process.value.kill()).pipe(
                Effect.ignore,
              );

              const stderr = (yield* Ref.get(stderrTailRef)).trim();
              const normalized = stderr ? `${base}: ${stderr}` : base;

              yield* Effect.logError("worker.failed", {
                event: "worker.failed",
                workerPid: process.value.pid,
                error: normalized,
              });

              yield* rejectPending(
                new RpcProtocolError({
                  method: "worker",
                  message: normalized,
                }),
              );
            } else {
              yield* rejectPending(
                new RpcProtocolError({
                  method: "worker",
                  message: base,
                }),
              );
            }
          });

        const emit = (
          event: RpcEvent,
        ): Effect.Effect<void, never, never> =>
          Effect.gen(function* () {
            const listeners = yield* Ref.get(listenersRef);
            for (const listener of listeners) {
              listener(event);
            }
          });

        const decodeFrame = (
          frame: Record<string, unknown>,
        ): Effect.Effect<Option.Option<Record<string, unknown>>, RpcProtocolError, never> =>
          Effect.gen(function* () {
            if (frame.type !== "rpc_chunk") {
              const existing = yield* Ref.get(chunkRef);
              if (Option.isSome(existing)) {
                return yield* Effect.fail(
                  new RpcProtocolError({
                    method: "decodeFrame",
                    message: "RPC chunk sequence was interrupted",
                  }),
                );
              }
              return Option.some(frame);
            }

            const { chunkId, index, count, byteLength, data } = frame;
            if (
              !isString(chunkId) ||
              typeof index !== "number" ||
              typeof count !== "number" ||
              typeof byteLength !== "number" ||
              !isString(data) ||
              !Number.isSafeInteger(index) ||
              !Number.isSafeInteger(count) ||
              !Number.isSafeInteger(byteLength)
            ) {
              return yield* Effect.fail(
                new RpcProtocolError({
                  method: "decodeFrame",
                  message: "Invalid RPC chunk metadata",
                }),
              );
            }

            if (
              index < 0 ||
              count <= 0 ||
              index >= count ||
              byteLength <= 0 ||
              byteLength > MAX_CHUNK_SIZE
            ) {
              return yield* Effect.fail(
                new RpcProtocolError({
                  method: "decodeFrame",
                  message: "RPC chunk bounds are invalid",
                }),
              );
            }

            if (index === 0) {
              yield* Ref.set(chunkRef, {
                id: chunkId,
                count,
                byteLength,
                parts: [],
              });
            }

            const state = yield* Ref.get(chunkRef);
            if (
              Option.isNone(state) ||
              state.value.id !== chunkId ||
              state.value.count !== count ||
              state.value.parts.length !== index
            ) {
              return yield* Effect.fail(
                new RpcProtocolError({
                  method: "decodeFrame",
                  message: "RPC chunks are interleaved or out of order",
                }),
              );
            }

            const decoded = yield* Effect.try({
              try: () => Uint8Array.from(Buffer.from(data, "base64")),
              catch: () =>
                new RpcProtocolError({
                  method: "decodeFrame",
                  message: "Invalid RPC chunk data",
                }),
            });

            state.value.parts.push(decoded);

            if (state.value.parts.length !== state.value.count) {
              return Option.none();
            }

            yield* Ref.set(chunkRef, Option.none());

            const merged = yield* Effect.sync(() =>
              Buffer.concat(
                state.value.parts.map((part) => Buffer.from(part)),
              ),
            );

            if (merged.byteLength !== state.value.byteLength) {
              return yield* Effect.fail(
                new RpcProtocolError({
                  method: "decodeFrame",
                  message: "RPC chunk byte length mismatch",
                }),
              );
            }

            const reassembled: unknown = yield* Effect.try({
              try: () =>
                JSON.parse(
                  new TextDecoder("utf-8", { fatal: true }).decode(merged),
                ),
              catch: (error) =>
                new RpcProtocolError({
                  method: "decodeFrame",
                  message: `Failed to reassemble RPC frame: ${error instanceof Error ? error.message : String(error)}`,
                }),
            });

            if (!record(reassembled)) {
              return yield* Effect.fail(
                new RpcProtocolError({
                  method: "decodeFrame",
                  message: "Reassembled RPC frame must be an object",
                }),
              );
            }

            return Option.some(reassembled);
          });

        const dispatch = (
          frame: Record<string, unknown>,
        ): Effect.Effect<void, RpcProtocolError, never> =>
          Effect.gen(function* () {
            if (frame.type === "response" && isString(frame.id)) {
              const pending = yield* Ref.get(pendingRef);
              const request = pending.get(frame.id);

              if (request === undefined) {
                const promptRequestIds = yield* Ref.get(promptRequestIdsRef);
                if (
                  frame.success === false &&
                  promptRequestIds.has(frame.id)
                ) {
                  yield* Ref.update(promptRequestIdsRef, (set) => {
                    set.delete(frame.id);
                    return set;
                  });
                  yield* emit({
                    type: "error",
                    message:
                      typeof frame.error === "string"
                        ? frame.error
                        : "RPC prompt scheduling failed",
                    command: frame.command,
                  });
                }
                return;
              }

              yield* Ref.update(pendingRef, (map) => {
                map.delete(frame.id);
                return map;
              });

              if (frame.success === false) {
                yield* Deferred.fail(
                  request.deferred,
                  new RpcProtocolError({
                    method: request.command,
                    message:
                      typeof frame.error === "string"
                        ? frame.error
                        : "RPC command failed",
                  }),
                );
              } else {
                if (request.command === "prompt") {
                  yield* Ref.update(promptRequestIdsRef, (set) => {
                    set.add(frame.id);
                    if (set.size > MAX_PROMPT_REQUEST_IDS) {
                      const oldest = set.values().next().value;
                      if (typeof oldest === "string") set.delete(oldest);
                    }
                    return set;
                  });
                }
                yield* Deferred.succeed(request.deferred, frame);
              }

              return;
            }

            if (frame.type === "prompt_result" && isString(frame.id)) {
              yield* Ref.update(promptRequestIdsRef, (set) => {
                set.delete(frame.id);
                return set;
              });
            }

            if (
              frame.type === "session_info_update" ||
              frame.type === "config_update" ||
              frame.type === "agent_start"
            ) {
              if (isString(frame.sessionId)) {
                yield* Ref.set(sessionIdRef, Option.some(frame.sessionId));
              }
              if (isString(frame.sessionFile)) {
                yield* Ref.set(sessionFileRef, Option.some(frame.sessionFile));
              }
            }

            if (frame.type === "agent_start") {
              yield* Ref.set(promptRequestIdsRef, new Set());
              yield* Ref.set(isStreamingRef, true);
            }

            if (frame.type === "agent_end" && frame.willContinue !== true) {
              yield* Ref.set(isStreamingRef, false);
            }

            if (isString(frame.type)) {
              yield* emit({ ...frame, type: frame.type } as RpcEvent);
            }
          });

        const readOutput = (
          process: Subprocess<"pipe", "pipe", "pipe">,
        ): Effect.Effect<void, never, never> =>
          Effect.gen(function* () {
            const reader = process.stdout.getReader();
            const decoder = new TextDecoder("utf-8", { fatal: true });
            let buffer = "";
            let readySeen = false;

            while (true) {
              const result = yield* Effect.tryPromise({
                try: () => reader.read(),
                catch: (error) =>
                  new RpcProtocolError({
                    method: "readOutput",
                    message:
                      error instanceof Error
                        ? error.message
                        : String(error),
                  }),
              });

              if (result.done) {
                if (!readySeen) {
                  const done = yield* Deferred.isDone(ready);
                  if (!done) {
                    yield* Deferred.fail(
                      ready,
                      new RpcSpawnError({
                        message: "OhMyPi output ended before ready",
                      }),
                    );
                  }
                }
                yield* fail(new Error("OhMyPi RPC output ended"));
                return;
              }

              buffer += yield* Effect.try({
                try: () => decoder.decode(result.value, { stream: true }),
                catch: (error) =>
                  new RpcProtocolError({
                    method: "readOutput",
                    message:
                      error instanceof Error ? error.message : String(error),
                  }),
              });

              let newline = buffer.indexOf("\n");
              while (newline >= 0) {
                const line = buffer.slice(0, newline);
                buffer = buffer.slice(newline + 1);

                if (line.length > 0) {
                  const parsed: unknown = yield* Effect.try({
                    try: () => JSON.parse(line),
                    catch: (error) =>
                      new RpcProtocolError({
                        method: "readOutput",
                        message:
                          error instanceof Error
                            ? error.message
                            : String(error),
                      }),
                  });

                  if (!record(parsed)) {
                    yield* fail(new Error("RPC frame must be an object"));
                    return;
                  }

                  yield* Effect.logTrace("rpc frame", {
                    event: "rpc.frame",
                    component: "omp-rpc",
                    direction: "inbound",
                    workerPid: process.pid,
                    correlationId: frameId(parsed),
                    frame: parsed,
                  });

                  if (!readySeen && parsed.type === "ready") {
                    readySeen = true;
                    const supportsV2 =
                      Array.isArray(parsed.supportedProtocolVersions) &&
                      parsed.supportedProtocolVersions.includes(2);
                    yield* Ref.set(supportsV2Ref, supportsV2);
                    yield* Deferred.succeed(ready, undefined);
                  } else {
                    const decoded = yield* decodeFrame(parsed);
                    if (Option.isSome(decoded)) {
                      yield* dispatch(decoded.value);
                    }
                  }
                }

                newline = buffer.indexOf("\n");
              }
            }
          }).pipe(
            Effect.catchTags({
              "@Gateway/RpcProtocolError": (error) => fail(error),
            }),
          );

        const readStderr = (
          process: Subprocess<"pipe", "pipe", "pipe">,
        ): Effect.Effect<void, never, never> =>
          Effect.gen(function* () {
            const reader = process.stderr.getReader();
            const decoder = new TextDecoder();

            while (true) {
              const result = yield* Effect.tryPromise({
                try: () => reader.read(),
                catch: (error) =>
                  new RpcProtocolError({
                    method: "readStderr",
                    message:
                      error instanceof Error ? error.message : String(error),
                  }),
              }).pipe(Effect.catchTags({
                "@Gateway/RpcProtocolError": () => Effect.succeed(undefined),
              }));

              if (result === undefined || result.done) break;

              const text = yield* Effect.try({
                try: () =>
                  decoder.decode(result.value, { stream: true }),
                catch: () => "",
              });

              yield* Ref.update(stderrTailRef, (tail) =>
                `${tail}${text}`.slice(-16_384),
              );
            }
          }).pipe(Effect.ignore);

        const writeFrame = (
          frame: Record<string, unknown>,
        ): Effect.Effect<void, RpcProtocolError, never> =>
          Effect.gen(function* () {
            const process = yield* getProcess;

            yield* Effect.logTrace("rpc frame", {
              event: "rpc.frame",
              component: "omp-rpc",
              direction: "outbound",
              workerPid: process.pid,
              correlationId: frameId(frame),
              frame,
            });

            const line = `${JSON.stringify(frame)}\n`;

            const written = yield* Effect.try({
              try: () => process.stdin.write(line),
              catch: (error) =>
                new RpcProtocolError({
                  method: String(frame.type),
                  message:
                    error instanceof Error ? error.message : String(error),
                }),
            });

            if (written === 0) {
              return yield* Effect.fail(
                new RpcProtocolError({
                  method: String(frame.type),
                  message: "RPC worker stdin closed before write",
                }),
              );
            }

            yield* Effect.try({
              try: () => process.stdin.flush(),
              catch: (error) =>
                new RpcProtocolError({
                  method: String(frame.type),
                  message:
                    error instanceof Error ? error.message : String(error),
                }),
            });
          }).pipe(
            Effect.catchTags({
              "@Gateway/RpcProtocolError": (error) =>
                fail(error).pipe(Effect.zipRight(Effect.fail(error))),
            }),
          );

        const send = Effect.fn("RpcWorker.send")(
          function* (
            type: string,
            body: Record<string, unknown> = {},
          ): Effect.fn.Return<Record<string, unknown>, RpcProtocolError> {
            const process = yield* getProcess;
            const sequence = yield* Ref.modify(requestSeqRef, (n) => [
              n + 1,
              n + 1,
            ]);
            const id = `gateway-${sequence}`;
            const deferred =
              yield* Deferred.make<Record<string, unknown>, RpcProtocolError>();

            yield* Ref.update(pendingRef, (map) => {
              map.set(id, { command: type, deferred });
              return map;
            });

            const frame = { id, type, ...body };

            const sent = yield* writeFrame(frame).pipe(Effect.either);

            if (sent._tag === "Left") {
              yield* Ref.update(pendingRef, (map) => {
                map.delete(id);
                return map;
              });
              return yield* Effect.fail(sent.left);
            }

            return yield* Deferred.await(deferred);
          },
        );

        const start = Effect.fn("RpcWorker.start")(
          function* (): Effect.fn.Return<
            void,
            RpcSpawnError | RpcTimeoutError | RpcProtocolError
          > {
            const running = yield* Ref.get(processRef);
            if (Option.isSome(running)) {
              return yield* Effect.fail(
                new RpcProtocolError({
                  method: "start",
                  message: "RPC worker already started",
                }),
              );
            }

            yield* Effect.logInfo("worker.starting", {
              event: "worker.starting",
              command,
              cwd,
            });

            const process = yield* Effect.try({
              try: () =>
                Bun.spawn(command, {
                  cwd,
                  env,
                  stdin: "pipe",
                  stdout: "pipe",
                  stderr: "pipe",
                }),
              catch: (error) =>
                new RpcSpawnError({
                  message:
                    error instanceof Error ? error.message : String(error),
                }),
            });

            yield* Ref.set(processRef, Option.some(process));

            const stdoutFiber = yield* Effect.fork(readOutput(process));
            const stderrFiber = yield* Effect.fork(readStderr(process));

            yield* Ref.set(stdoutFiberRef, Option.some(stdoutFiber));
            yield* Ref.set(stderrFiberRef, Option.some(stderrFiber));

            const timeout = Effect.sleep(startTimeoutMs).pipe(
              Effect.zipRight(
                Effect.fail(
                  new RpcTimeoutError({
                    method: "start",
                    message:
                      "Timed out waiting for OhMyPi RPC ready frame",
                  }),
                ),
              ),
            );

            const exited = Effect.promise(() => process.exited).pipe(
              Effect.flatMap((code) =>
                Effect.fail(
                  new RpcSpawnError({
                    message: `OhMyPi exited before ready with code ${code}`,
                  }),
                ),
              ),
            );

            const result = yield* Effect.raceFirst(
              Deferred.await(ready),
              exited,
            ).pipe(Effect.raceFirst(timeout), Effect.either);

            if (result._tag === "Left") {
              yield* stop();
              return yield* Effect.fail(result.left);
            }

            const supportsV2 = yield* Ref.get(supportsV2Ref);
            if (supportsV2) {
              const negotiated = yield* send("negotiate_protocol", {
                protocolVersion: 2,
              }).pipe(Effect.either);

              if (negotiated._tag === "Left") {
                yield* stop();
                return yield* Effect.fail(negotiated.left);
              }
            }

            yield* Effect.logInfo("worker.ready", {
              event: "worker.ready",
              workerPid: process.pid,
              protocolVersion: supportsV2 ? 2 : 1,
            });
          },
        );

        const stop = Effect.fn("RpcWorker.stop")(
          function* (): Effect.fn.Return<void, never> {
            const process = yield* Ref.get(processRef);
            if (Option.isNone(process)) {
              return;
            }

            yield* Ref.set(processRef, Option.none());

            yield* Effect.try(() => process.value.stdin.end()).pipe(
              Effect.matchEffect({
                onSuccess: () => Effect.void,
                onFailure: () =>
                  Effect.sync(() => process.value.kill()).pipe(Effect.ignore),
              }),
            );

            const exited = yield* Effect.race(
              Effect.promise(() => process.value.exited).pipe(
                Effect.map(() => true),
              ),
              Effect.sleep(2_000).pipe(Effect.map(() => false)),
            );

            if (!exited) {
              yield* Effect.sync(() => process.value.kill()).pipe(
                Effect.ignore,
              );
              yield* Effect.promise(() => process.value.exited).pipe(
                Effect.ignore,
              );
            }

            const stdoutFiber = yield* Ref.get(stdoutFiberRef);
            const stderrFiber = yield* Ref.get(stderrFiberRef);
            if (Option.isSome(stdoutFiber)) {
              yield* Fiber.interrupt(stdoutFiber.value);
            }
            if (Option.isSome(stderrFiber)) {
              yield* Fiber.interrupt(stderrFiber.value);
            }

            yield* rejectPending(
              new RpcProtocolError({
                method: "stop",
                message: "OhMyPi RPC worker stopped",
              }),
            );

            yield* Effect.logInfo("worker.stopped", {
              event: "worker.stopped",
              workerPid: process.value.pid,
            });
          },
        );

        const prompt = Effect.fn("RpcWorker.prompt")(
          function* (message: string): Effect.fn.Return<
            boolean,
            RpcProtocolError | RpcTimeoutError
          > {
            const response = yield* send("prompt", { message });
            const data = record(response.data) ? response.data : null;
            return data?.agentInvoked !== false;
          },
        );

        const steer = Effect.fn("RpcWorker.steer")(
          function* (message: string): Effect.fn.Return<
            void,
            RpcProtocolError
          > {
            yield* send("steer", { message });
          },
        );

        const followUp = Effect.fn("RpcWorker.followUp")(
          function* (message: string): Effect.fn.Return<
            void,
            RpcProtocolError
          > {
            yield* send("follow_up", { message });
          },
        );

        const abort = Effect.fn("RpcWorker.abort")(
          function* (): Effect.fn.Return<void, RpcProtocolError> {
            yield* send("abort");
          },
        );

        const getState = Effect.fn("RpcWorker.getState")(
          function* (): Effect.fn.Return<
            Record<string, unknown>,
            RpcProtocolError
          > {
            const response = yield* send("get_state");
            const state = record(response.data) ? response.data : {};

            if (isString(state.sessionId)) {
              yield* Ref.set(sessionIdRef, Option.some(state.sessionId));
            }
            if (isString(state.sessionFile)) {
              yield* Ref.set(sessionFileRef, Option.some(state.sessionFile));
            }

            return state;
          },
        );

        const respondToUi = Effect.fn("RpcWorker.respondToUi")(
          function* (
            requestId: string,
            response:
              | { readonly value: string }
              | { readonly confirmed: boolean }
              | { readonly cancelled: true },
          ): Effect.fn.Return<void, RpcProtocolError> {
            yield* writeFrame({
              type: "extension_ui_response",
              id: requestId,
              ...response,
            });
          },
        );

        const onEvent = Effect.fn("RpcWorker.onEvent")(
          function* (
            listener: (event: RpcEvent) => void,
          ): Effect.fn.Return<
            () => Effect.Effect<void, never, never>,
            never
          > {
            yield* Ref.update(listenersRef, (set) => {
              set.add(listener);
              return set;
            });

            return () =>
              Ref.update(listenersRef, (set) => {
                set.delete(listener);
                return set;
              });
          },
        );

        const sessionId = Ref.get(sessionIdRef);
        const sessionFile = Ref.get(sessionFileRef);
        const isStreaming = Ref.get(isStreamingRef);

        const handle: RpcWorkerHandle = {
          sessionId,
          sessionFile,
          isStreaming,
          start,
          stop,
          prompt,
          steer,
          followUp,
          abort,
          getState,
          respondToUi,
          onEvent,
        };

        return handle;
      },
    );

    return { spawn };
  }),
}) {}
