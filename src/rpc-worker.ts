import type { RpcEvent, RpcWorker } from "./domain";

interface PendingRequest {
  command: string;
  resolve(value: Record<string, unknown>): void;
  reject(error: Error): void;
}

interface ChunkState {
  id: string;
  count: number;
  byteLength: number;
  parts: Uint8Array[];
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
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

export class OhMyPiRpcWorker implements RpcWorker {
  readonly #command: readonly string[];
  readonly #cwd: string;
  readonly #env: Record<string, string>;
  readonly #startTimeoutMs: number;
  readonly #listeners = new Set<(event: RpcEvent) => void>();
  readonly #pending = new Map<string, PendingRequest>();
  #process: Bun.Subprocess<"pipe", "pipe", "pipe"> | null = null;
  #requestSequence = 0;
  #chunk: ChunkState | null = null;
  #sessionId: string | null = null;
  #sessionFile: string | null = null;
  #supportsProtocolV2 = false;
  #streaming = false;
  #stderrTail = "";
  readonly #promptRequestIds = new Set<string>();

  constructor(input: {
    command: readonly string[];
    cwd: string;
    env?: Record<string, string | undefined>;
    startTimeoutMs?: number;
  }) {
    if (input.command.length === 0)
      throw new Error("RPC command cannot be empty");
    this.#command = input.command;
    this.#cwd = input.cwd;
    this.#env = sanitizedEnvironment(
      input.env ?? {
        PATH: Bun.env.PATH,
        HOME: Bun.env.HOME,
        TMPDIR: Bun.env.TMPDIR,
        XDG_CONFIG_HOME: Bun.env.XDG_CONFIG_HOME,
      },
    );
    this.#startTimeoutMs = input.startTimeoutMs ?? 30_000;
  }

  get sessionId(): string | null {
    return this.#sessionId;
  }
  get sessionFile(): string | null {
    return this.#sessionFile;
  }
  get isStreaming(): boolean {
    return this.#streaming;
  }

  async start(): Promise<void> {
    if (this.#process) throw new Error("RPC worker already started");
    const process = Bun.spawn([...this.#command, "--mode", "rpc"], {
      cwd: this.#cwd,
      env: this.#env,
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
    });
    this.#process = process;
    const ready = Promise.withResolvers<void>();
    void this.#readOutput(process, ready).catch((error: unknown) =>
      this.#fail(error),
    );
    void this.#readStderr(process).catch(() => undefined);
    const timeout = setTimeout(
      () =>
        ready.reject(new Error("Timed out waiting for OhMyPi RPC ready frame")),
      this.#startTimeoutMs,
    );
    try {
      await Promise.race([
        ready.promise,
        process.exited.then((code) => {
          throw new Error(`OhMyPi exited before ready with code ${code}`);
        }),
      ]);
      if (this.#supportsProtocolV2)
        await this.#send("negotiate_protocol", { protocolVersion: 2 });
    } catch (error) {
      await this.stop();
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }

  async prompt(message: string): Promise<boolean> {
    const response = await this.#send("prompt", { message });
    const data = record(response.data) ? response.data : null;
    const agentInvoked = data?.agentInvoked !== false;
    if (!agentInvoked && typeof response.id === "string")
      this.#promptRequestIds.delete(response.id);
    return agentInvoked;
  }
  steer(message: string): Promise<void> {
    return this.#commandWithoutData("steer", { message });
  }
  followUp(message: string): Promise<void> {
    return this.#commandWithoutData("follow_up", { message });
  }
  abort(): Promise<void> {
    return this.#commandWithoutData("abort");
  }

  async getState(): Promise<Record<string, unknown>> {
    const response = await this.#send("get_state");
    const state = record(response.data) ? response.data : {};
    if (typeof state.sessionId === "string") this.#sessionId = state.sessionId;
    if (typeof state.sessionFile === "string")
      this.#sessionFile = state.sessionFile;
    return state;
  }

  async respondToUi(
    requestId: string,
    response: { value: string } | { confirmed: boolean } | { cancelled: true },
  ): Promise<void> {
    await this.#writeFrame({
      type: "extension_ui_response",
      id: requestId,
      ...response,
    });
  }

  onEvent(listener: (event: RpcEvent) => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  async stop(): Promise<void> {
    const process = this.#process;
    if (!process) return;
    this.#process = null;
    try {
      process.stdin.end();
    } catch {
      process.kill();
    }
    const exited = await Promise.race([
      process.exited.then(() => true),
      Bun.sleep(2_000).then(() => false),
    ]);
    if (!exited) {
      process.kill();
      await process.exited.catch(() => undefined);
    }
    this.#rejectPending(new Error("OhMyPi RPC worker stopped"));
  }

  async #commandWithoutData(
    type: string,
    body: Record<string, unknown> = {},
  ): Promise<void> {
    await this.#send(type, body);
  }

  async #send(
    type: string,
    body: Record<string, unknown> = {},
  ): Promise<Record<string, unknown>> {
    const process = this.#process;
    if (!process) throw new Error("RPC worker is not running");
    const id = `gateway-${++this.#requestSequence}`;
    const pending = Promise.withResolvers<Record<string, unknown>>();
    this.#pending.set(id, {
      command: type,
      resolve: pending.resolve,
      reject: pending.reject,
    });
    try {
      process.stdin.write(`${JSON.stringify({ id, type, ...body })}\n`);
      await process.stdin.flush();
    } catch (error) {
      this.#pending.delete(id);
      throw error;
    }
    return pending.promise;
  }

  async #writeFrame(frame: Record<string, unknown>): Promise<void> {
    const process = this.#process;
    if (!process) throw new Error("RPC worker is not running");
    process.stdin.write(`${JSON.stringify(frame)}\n`);
    await process.stdin.flush();
  }

  async #readOutput(
    process: Bun.Subprocess<"pipe", "pipe", "pipe">,
    ready: {
      promise: Promise<void>;
      resolve(value?: void | PromiseLike<void>): void;
      reject(reason?: unknown): void;
    },
  ): Promise<void> {
    const reader = process.stdout.getReader();
    const decoder = new TextDecoder("utf-8", { fatal: true });
    let buffer = "";
    let readySeen = false;
    while (true) {
      const result = await reader.read();
      if (result.done) break;
      buffer += decoder.decode(result.value, { stream: true });
      let newline = buffer.indexOf("\n");
      while (newline >= 0) {
        const line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        if (line.length > 0) {
          const parsed: unknown = JSON.parse(line);
          if (!record(parsed)) throw new Error("RPC frame must be an object");
          if (!readySeen && parsed.type === "ready") {
            readySeen = true;
            this.#supportsProtocolV2 =
              Array.isArray(parsed.supportedProtocolVersions) &&
              parsed.supportedProtocolVersions.includes(2);
            ready.resolve();
          } else {
            const frame = this.#decodeFrame(parsed);
            if (frame) this.#dispatch(frame);
          }
        }
        newline = buffer.indexOf("\n");
      }
    }
    if (!readySeen) ready.reject(new Error("OhMyPi output ended before ready"));
    if (this.#process === process)
      this.#fail(new Error("OhMyPi RPC output ended"));
  }

  async #readStderr(
    process: Bun.Subprocess<"pipe", "pipe", "pipe">,
  ): Promise<void> {
    const reader = process.stderr.getReader();
    const decoder = new TextDecoder();
    while (true) {
      const result = await reader.read();
      if (result.done) break;
      this.#stderrTail =
        `${this.#stderrTail}${decoder.decode(result.value, { stream: true })}`.slice(
          -16_384,
        );
    }
  }

  #decodeFrame(frame: Record<string, unknown>): Record<string, unknown> | null {
    if (frame.type !== "rpc_chunk") {
      if (this.#chunk) throw new Error("RPC chunk sequence was interrupted");
      return frame;
    }
    const { chunkId, index, count, byteLength, data } = frame;
    if (
      typeof chunkId !== "string" ||
      !Number.isSafeInteger(index) ||
      !Number.isSafeInteger(count) ||
      !Number.isSafeInteger(byteLength) ||
      typeof data !== "string"
    ) {
      throw new Error("Invalid RPC chunk metadata");
    }
    const chunkIndex = index as number;
    const chunkCount = count as number;
    const expectedBytes = byteLength as number;
    if (
      chunkIndex < 0 ||
      chunkCount <= 0 ||
      chunkIndex >= chunkCount ||
      expectedBytes <= 0 ||
      expectedBytes > 67_108_864
    ) {
      throw new Error("RPC chunk bounds are invalid");
    }
    if (chunkIndex === 0)
      this.#chunk = {
        id: chunkId,
        count: chunkCount,
        byteLength: expectedBytes,
        parts: [],
      };
    const state = this.#chunk;
    if (
      !state ||
      state.id !== chunkId ||
      state.count !== chunkCount ||
      state.parts.length !== chunkIndex
    ) {
      throw new Error("RPC chunks are interleaved or out of order");
    }
    state.parts.push(Uint8Array.from(Buffer.from(data, "base64")));
    if (state.parts.length !== state.count) return null;
    this.#chunk = null;
    const merged = Buffer.concat(state.parts.map((part) => Buffer.from(part)));
    if (merged.byteLength !== state.byteLength)
      throw new Error("RPC chunk byte length mismatch");
    const decoded: unknown = JSON.parse(
      new TextDecoder("utf-8", { fatal: true }).decode(merged),
    );
    if (!record(decoded))
      throw new Error("Reassembled RPC frame must be an object");
    return decoded;
  }

  #dispatch(frame: Record<string, unknown>): void {
    if (frame.type === "response" && typeof frame.id === "string") {
      const pending = this.#pending.get(frame.id);
      if (!pending) {
        if (
          frame.success === false &&
          this.#promptRequestIds.delete(frame.id)
        ) {
          this.#emit({
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
      this.#pending.delete(frame.id);
      if (frame.success === false) {
        pending.reject(
          new Error(
            typeof frame.error === "string"
              ? frame.error
              : "RPC command failed",
          ),
        );
      } else {
        if (pending.command === "prompt") {
          this.#promptRequestIds.add(frame.id);
          if (this.#promptRequestIds.size > 256) {
            const oldest = this.#promptRequestIds.values().next().value;
            if (typeof oldest === "string")
              this.#promptRequestIds.delete(oldest);
          }
        }
        pending.resolve(frame);
      }
      return;
    }
    if (
      frame.type === "session_info_update" ||
      frame.type === "config_update" ||
      frame.type === "agent_start"
    ) {
      if (typeof frame.sessionId === "string")
        this.#sessionId = frame.sessionId;
      if (typeof frame.sessionFile === "string")
        this.#sessionFile = frame.sessionFile;
    }
    if (frame.type === "agent_start") this.#promptRequestIds.clear();
    if (frame.type === "agent_start") this.#streaming = true;
    if (frame.type === "agent_end" && frame.willContinue !== true)
      this.#streaming = false;
    if (typeof frame.type === "string")
      this.#emit({ ...frame, type: frame.type });
  }

  #emit(event: RpcEvent): void {
    for (const listener of this.#listeners) listener(event);
  }

  #fail(error: unknown): void {
    const base = error instanceof Error ? error.message : String(error);
    const stderr = this.#stderrTail.trim();
    const normalized = new Error(stderr ? `${base}: ${stderr}` : base);
    const process = this.#process;
    this.#process = null;
    this.#streaming = false;
    if (process) process.kill();
    this.#rejectPending(normalized);
  }

  #rejectPending(error: Error): void {
    for (const pending of this.#pending.values()) pending.reject(error);
    this.#pending.clear();
  }
}
