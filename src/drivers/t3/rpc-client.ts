/**
 * Minimal client for T3 Code's WebSocket RPC (`GET /ws`, Effect RPC with
 * JSON serialization). Retains the protocol-2 JSON frames of the pinned
 * server contract, independently of the gateway's Effect runtime.
 *
 * Frames (effect `RpcMessage`): the client sends `Request{id,tag,payload,
 * headers}`, `Ack{requestId}` after every stream `Chunk`, `Interrupt`, and
 * `Ping`; the server answers `Exit{requestId,exit}`, `Chunk{requestId,values}`,
 * `Pong`, or a connection-level `Defect`. A frame may also be a JSON array.
 *
 * Secrets: the bearer token only travels in the ticket request header, and
 * the short-lived ticket only in the socket URL. Neither appears in errors.
 */
import {
  Array as Arr,
  Deferred,
  Duration,
  Effect,
  Exit,
  Queue,
  Redacted,
  Schedule,
  Schema,
  type Scope,
  Stream,
  type Take,
} from "effect";
import {
  ENVIRONMENT_DESCRIPTOR_PATH,
  EnvironmentDescriptor,
  ORCHESTRATION_PROTOCOL_QUERY_PARAM,
  ORCHESTRATION_PROTOCOL_VERSION,
  WEBSOCKET_PATH,
  WEBSOCKET_TICKET_PATH,
  WEBSOCKET_TICKET_QUERY_PARAM,
  WebSocketTicketResult,
} from "./wire.js";

export const T3RpcErrorReason = Schema.Literals([
  "unavailable",
  "auth",
  "protocol",
  "timeout",
  "rejected",
  "lost",
]);
export type T3RpcErrorReason = typeof T3RpcErrorReason.Type;

export class T3RpcError extends Schema.TaggedError<T3RpcError>()(
  "@Gateway/T3RpcError",
  {
    reason: T3RpcErrorReason,
    method: Schema.String,
    /** Whether the request frame reached the socket. */
    sent: Schema.Boolean,
    message: Schema.String,
    /** `_tag` of the server's declared error, for `rejected`. */
    remoteTag: Schema.optional(Schema.String),
  },
) {}

export interface T3ConnectOptions {
  readonly baseUrl: URL;
  /** Read on every connect, so a rotated token file is picked up. */
  readonly token: Effect.Effect<Redacted.Redacted<string>, T3RpcError>;
  readonly pingIntervalMs?: number;
  readonly requestTimeoutMs?: number;
  readonly connectTimeoutMs?: number;
}

export interface T3RpcConnection {
  readonly request: (
    method: string,
    payload: unknown,
    options?: { readonly timeoutMs?: number },
  ) => Effect.Effect<unknown, T3RpcError>;
  /** One server stream; acks every chunk and interrupts on early exit. */
  readonly stream: (
    method: string,
    payload: unknown,
  ) => Stream.Stream<unknown, T3RpcError>;
  readonly isOpen: () => boolean;
  /** Completes with the reason once the socket is gone. */
  readonly closed: Effect.Effect<T3RpcError>;
}

const DEFAULT_PING_INTERVAL_MS = 5_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const DEFAULT_CONNECT_TIMEOUT_MS = 10_000;

/** Joins `path` under the base URL, keeping any path prefix (reverse proxy). */
export const t3Endpoint = (baseUrl: URL, path: string): URL => {
  const url = new URL(baseUrl.toString());
  url.pathname = `${url.pathname.replace(/\/+$/u, "")}${path}`;
  url.search = "";
  url.hash = "";
  return url;
};

const errorText = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

export const httpJson = (
  method: string,
  url: URL,
  init: RequestInit,
  timeoutMs: number,
): Effect.Effect<unknown, T3RpcError> =>
  Effect.gen(function* () {
    const response = yield* Effect.tryPromise({
      try: (signal) => fetch(url, { ...init, redirect: "manual", signal }),
      catch: (error) =>
        new T3RpcError({
          reason: "unavailable",
          method,
          sent: false,
          message: `T3 ${url.pathname} unreachable: ${errorText(error)}`,
        }),
    }).pipe(
      Effect.timeoutOrElse({
        duration: Duration.millis(timeoutMs),
        orElse: () =>
          Effect.fail(
            new T3RpcError({
              reason: "timeout",
              method,
              sent: true,
              message: `T3 ${url.pathname} timed out after ${timeoutMs}ms`,
            }),
          ),
      }),
    );
    if (response.status === 401 || response.status === 403) {
      return yield* new T3RpcError({
        reason: "auth",
        method,
        sent: true,
        message: `T3 ${url.pathname} rejected the credential (HTTP ${response.status})`,
      });
    }
    if (!response.ok) {
      return yield* new T3RpcError({
        reason: "unavailable",
        method,
        sent: true,
        message: `T3 ${url.pathname} answered HTTP ${response.status}`,
      });
    }
    return yield* Effect.tryPromise({
      try: (): Promise<unknown> => response.json(),
      catch: () =>
        new T3RpcError({
          reason: "protocol",
          method,
          sent: true,
          message: `T3 ${url.pathname} returned invalid JSON`,
        }),
    });
  });

const decodeOrProtocol =
  <A, I>(schema: Schema.Codec<A, I>, method: string, what: string) =>
  (value: unknown): Effect.Effect<A, T3RpcError> =>
    Schema.decodeUnknownEffect(schema)(value).pipe(
      Effect.mapError(
        () =>
          new T3RpcError({
            reason: "protocol",
            method,
            sent: true,
            message: `T3 ${what} did not match the pinned wire schema`,
          }),
      ),
    );

/** `GET /.well-known/t3/environment`; unauthenticated. */
export const fetchEnvironmentDescriptor = (
  baseUrl: URL,
  timeoutMs = DEFAULT_CONNECT_TIMEOUT_MS,
) =>
  httpJson(
    "environment",
    t3Endpoint(baseUrl, ENVIRONMENT_DESCRIPTOR_PATH),
    { method: "GET" },
    timeoutMs,
  ).pipe(
    Effect.flatMap(
      decodeOrProtocol(EnvironmentDescriptor, "environment", "descriptor"),
    ),
  );

/** Bearer → short-lived socket ticket (`POST /api/auth/websocket-ticket`). */
export const fetchWebSocketTicket = (
  options: Pick<T3ConnectOptions, "baseUrl" | "token" | "connectTimeoutMs">,
): Effect.Effect<Redacted.Redacted<string>, T3RpcError> =>
  Effect.gen(function* () {
    const token = yield* options.token;
    const body = yield* httpJson(
      "websocket-ticket",
      t3Endpoint(options.baseUrl, WEBSOCKET_TICKET_PATH),
      {
        method: "POST",
        headers: { authorization: `Bearer ${Redacted.value(token)}` },
      },
      options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
    );
    const decoded = yield* decodeOrProtocol(
      WebSocketTicketResult,
      "websocket-ticket",
      "websocket ticket",
    )(body);
    return Redacted.make(decoded.ticket);
  });

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const frameRequestId = (frame: Record<string, unknown>): string | null =>
  typeof frame.requestId === "string" || typeof frame.requestId === "number"
    ? String(frame.requestId)
    : null;

/** Decodes an encoded Effect `Cause` (array of Fail/Die/Interrupt entries). */
const failureFromExit = (
  method: string,
  exit: Record<string, unknown>,
): T3RpcError => {
  const entries = Array.isArray(exit.cause) ? exit.cause : [];
  for (const entry of entries) {
    if (!isRecord(entry)) continue;
    if (entry._tag === "Fail") {
      const error = isRecord(entry.error) ? entry.error : {};
      const remoteTag = typeof error._tag === "string" ? error._tag : "Unknown";
      const detail =
        typeof error.message === "string" ? error.message : "request failed";
      return new T3RpcError({
        reason: "rejected",
        method,
        sent: true,
        remoteTag,
        message: `T3 ${method} failed with ${remoteTag}: ${detail}`,
      });
    }
  }
  for (const entry of entries) {
    if (!isRecord(entry)) continue;
    if (entry._tag === "Die") {
      return new T3RpcError({
        reason: "protocol",
        method,
        sent: true,
        message: `T3 ${method} died: ${String(entry.defect)}`.slice(0, 2_000),
      });
    }
  }
  return new T3RpcError({
    reason: "lost",
    method,
    sent: true,
    message: `T3 ${method} was interrupted by the server`,
  });
};

interface PendingRequest {
  readonly method: string;
  readonly deferred: Deferred.Deferred<unknown, T3RpcError>;
}

interface OpenStream {
  readonly method: string;
  readonly queue: Queue.Queue<Take.Take<unknown, T3RpcError>>;
}

/**
 * Opens one authenticated socket in the caller's scope. Reconnecting is the
 * caller's job: a connection never revives after `closed` completes.
 */
export const connectT3 = (
  options: T3ConnectOptions,
): Effect.Effect<T3RpcConnection, T3RpcError, Scope.Scope> =>
  Effect.gen(function* () {
    const pingIntervalMs = options.pingIntervalMs ?? DEFAULT_PING_INTERVAL_MS;
    const requestTimeoutMs =
      options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    const connectTimeoutMs =
      options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
    const ticket = yield* fetchWebSocketTicket(options);
    const url = t3Endpoint(options.baseUrl, WEBSOCKET_PATH);
    url.protocol = options.baseUrl.protocol === "https:" ? "wss:" : "ws:";
    url.searchParams.set(
      ORCHESTRATION_PROTOCOL_QUERY_PARAM,
      String(ORCHESTRATION_PROTOCOL_VERSION),
    );
    url.searchParams.set(WEBSOCKET_TICKET_QUERY_PARAM, Redacted.value(ticket));
    const where = `${url.protocol}//${url.host}${url.pathname}`;

    const pending = new Map<string, PendingRequest>();
    const streams = new Map<string, OpenStream>();
    const closed = yield* Deferred.make<T3RpcError>();
    let nextId = 0;
    let open = false;
    let awaitingPong = false;

    const socket = yield* Effect.acquireRelease(
      Effect.callback<WebSocket, T3RpcError>((resume) => {
        const ws = new WebSocket(url.toString());
        let settled = false;
        ws.onopen = () => {
          if (settled) return;
          settled = true;
          open = true;
          resume(Effect.succeed(ws));
        };
        const failOpen = () => {
          if (settled) return;
          settled = true;
          resume(
            Effect.fail(
              new T3RpcError({
                reason: "unavailable",
                method: "connect",
                sent: false,
                message: `T3 socket ${where} refused the upgrade`,
              }),
            ),
          );
        };
        ws.onerror = failOpen;
        ws.onclose = failOpen;
        return Effect.sync(() => {
          if (!settled) ws.close();
        });
      }).pipe(
        Effect.timeoutOrElse({
          duration: Duration.millis(connectTimeoutMs),
          orElse: () =>
            Effect.fail(
              new T3RpcError({
                reason: "timeout",
                method: "connect",
                sent: false,
                message: `T3 socket ${where} did not open within ${connectTimeoutMs}ms`,
              }),
            ),
        }),
      ),
      (ws) =>
        Effect.sync(() => {
          terminate(
            new T3RpcError({
              reason: "lost",
              method: "connect",
              sent: true,
              message: "T3 connection closed by the gateway",
            }),
          );
          ws.close(1000);
        }),
    );

    const sendFrame = (frame: Record<string, unknown>): boolean => {
      if (!open) return false;
      try {
        socket.send(JSON.stringify(frame));
        return true;
      } catch {
        return false;
      }
    };

    const forRequest = (reason: T3RpcError, method: string) =>
      new T3RpcError({
        reason: reason.reason,
        method,
        sent: true,
        message: reason.message,
      });

    // Idempotent: fails everything in flight once; Deferreds complete once.
    function terminate(reason: T3RpcError): void {
      open = false;
      for (const [id, entry] of pending) {
        pending.delete(id);
        Deferred.doneUnsafe(
          entry.deferred,
          Effect.fail(forRequest(reason, entry.method)),
        );
      }
      for (const [id, entry] of streams) {
        streams.delete(id);
        Queue.offerUnsafe(
          entry.queue,
          Exit.fail(forRequest(reason, entry.method)),
        );
      }
      Deferred.doneUnsafe(closed, Effect.succeed(reason));
    }

    const handleFrame = (frame: unknown): void => {
      if (!isRecord(frame)) return;
      switch (frame._tag) {
        case "Pong":
          awaitingPong = false;
          return;
        case "Exit": {
          const id = frameRequestId(frame);
          if (id === null || !isRecord(frame.exit)) return;
          const success = frame.exit._tag === "Success";
          const request = pending.get(id);
          if (request !== undefined) {
            pending.delete(id);
            Deferred.doneUnsafe(
              request.deferred,
              success
                ? Effect.succeed(frame.exit.value)
                : Effect.fail(failureFromExit(request.method, frame.exit)),
            );
            return;
          }
          const stream = streams.get(id);
          if (stream !== undefined) {
            streams.delete(id);
            Queue.offerUnsafe(
              stream.queue,
              success
                ? Exit.void
                : Exit.fail(failureFromExit(stream.method, frame.exit)),
            );
          }
          return;
        }
        case "Chunk": {
          const id = frameRequestId(frame);
          if (id === null || !Array.isArray(frame.values)) return;
          const stream = streams.get(id);
          if (stream === undefined) return;
          if (Arr.isReadonlyArrayNonEmpty(frame.values))
            Queue.offerUnsafe(stream.queue, frame.values);
          sendFrame({ _tag: "Ack", requestId: id });
          return;
        }
        case "Defect":
          terminate(
            new T3RpcError({
              reason: "protocol",
              method: "connection",
              sent: true,
              message: `T3 connection defect: ${String(frame.defect)}`.slice(
                0,
                2_000,
              ),
            }),
          );
          socket.close(1011);
          return;
        default:
          return;
      }
    };

    socket.onmessage = (event: MessageEvent) => {
      const text =
        typeof event.data === "string"
          ? event.data
          : new TextDecoder().decode(event.data as ArrayBuffer);
      let decoded: unknown;
      try {
        decoded = JSON.parse(text);
      } catch {
        return;
      }
      const frames = Array.isArray(decoded) ? decoded : [decoded];
      for (const frame of frames) handleFrame(frame);
    };
    socket.onclose = (event: CloseEvent) => {
      terminate(
        new T3RpcError({
          reason: "lost",
          method: "connection",
          sent: true,
          message: `T3 socket closed (code ${event.code})`,
        }),
      );
    };
    socket.onerror = () => {
      terminate(
        new T3RpcError({
          reason: "lost",
          method: "connection",
          sent: true,
          message: "T3 socket errored",
        }),
      );
    };

    yield* Effect.sync(() => {
      if (!open) return;
      if (awaitingPong) {
        terminate(
          new T3RpcError({
            reason: "lost",
            method: "connection",
            sent: true,
            message: `T3 socket missed a pong within ${pingIntervalMs}ms`,
          }),
        );
        socket.close(4000);
        return;
      }
      awaitingPong = sendFrame({ _tag: "Ping" });
    }).pipe(
      Effect.repeat(Schedule.spaced(Duration.millis(pingIntervalMs))),
      Effect.delay(Duration.millis(pingIntervalMs)),
      Effect.forkScoped,
    );

    const notSent = (method: string) =>
      new T3RpcError({
        reason: "lost",
        method,
        sent: false,
        message: `T3 ${method} not sent: the socket is closed`,
      });

    const request: T3RpcConnection["request"] = (method, payload, opts) =>
      Effect.gen(function* () {
        const deferred = yield* Deferred.make<unknown, T3RpcError>();
        const id = String(nextId++);
        pending.set(id, { method, deferred });
        const sent = sendFrame({
          _tag: "Request",
          id,
          tag: method,
          payload,
          headers: [],
        });
        if (!sent) {
          pending.delete(id);
          return yield* notSent(method);
        }
        const timeoutMs = opts?.timeoutMs ?? requestTimeoutMs;
        return yield* Deferred.await(deferred).pipe(
          Effect.timeoutOrElse({
            duration: Duration.millis(timeoutMs),
            orElse: () =>
              Effect.fail(
                new T3RpcError({
                  reason: "timeout",
                  method,
                  sent: true,
                  message: `T3 ${method} timed out after ${timeoutMs}ms`,
                }),
              ),
          }),
          Effect.onExit(() =>
            Effect.sync(() => {
              if (pending.delete(id)) {
                sendFrame({
                  _tag: "Interrupt",
                  requestId: id,
                  interruptors: [],
                });
              }
            }),
          ),
        );
      });

    const stream: T3RpcConnection["stream"] = (method, payload) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const queue =
            yield* Queue.unbounded<Take.Take<unknown, T3RpcError>>();
          const id = String(nextId++);
          streams.set(id, { method, queue });
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              if (streams.delete(id)) {
                sendFrame({
                  _tag: "Interrupt",
                  requestId: id,
                  interruptors: [],
                });
              }
            }),
          );
          const sent = sendFrame({
            _tag: "Request",
            id,
            tag: method,
            payload,
            headers: [],
          });
          if (!sent) {
            streams.delete(id);
            return Stream.fail(notSent(method));
          }
          return Stream.fromQueue(queue).pipe(Stream.flattenTake);
        }),
      );

    return {
      request,
      stream,
      isOpen: () => open,
      closed: Deferred.await(closed),
    } satisfies T3RpcConnection;
  });

/** Reads `Option<Redacted>` style token sources without leaking them. */
export const tokenFromSource = (
  source:
    | { readonly _tag: "Inline"; readonly token: Redacted.Redacted<string> }
    | { readonly _tag: "File"; readonly path: string },
): Effect.Effect<Redacted.Redacted<string>, T3RpcError> =>
  source._tag === "Inline"
    ? Effect.succeed(source.token)
    : Effect.tryPromise({
        try: () => Bun.file(source.path).text(),
        catch: () =>
          new T3RpcError({
            reason: "auth",
            method: "token",
            sent: false,
            message: `Could not read the T3 token file ${source.path}`,
          }),
      }).pipe(
        Effect.map((text) => text.trim()),
        Effect.filterOrFail(
          (text) => text.length > 0,
          () =>
            new T3RpcError({
              reason: "auth",
              method: "token",
              sent: false,
              message: `The T3 token file ${source.path} is empty`,
            }),
        ),
        Effect.map((text) => Redacted.make(text)),
      );
