/**
 * In-process fake of the slice of T3 Code the driver uses: bearer → ticket
 * exchange, the protocol-2 WebSocket gate, Effect RPC JSON framing, commandId
 * receipts (duplicates replay), and a thread event log with afterSequence
 * resume. Tests drive the "agent" side through the helper methods.
 */
import type { ServerWebSocket } from "bun";

type Json = Record<string, unknown>;

export interface FakeRun extends Json {
  id: string;
  threadId: string;
  ordinal: number;
  userMessageId: string;
  status: string;
  queueHeld?: boolean;
  restartContinuationOfRunId?: string;
}

interface FakeThread {
  readonly id: string;
  readonly projectId: string;
  readonly launch: Json;
  runs: FakeRun[];
  messages: Json[];
  runtimeRequests: Json[];
  turnItems: Json[];
  events: Array<{ sequence: number; event: Json }>;
}

interface SocketData {
  readonly id: number;
  readonly subscriptions: Map<string, { threadId: string }>;
}

export interface ReceivedRequest {
  readonly tag: string;
  readonly payload: Json;
}

const now = () => new Date().toISOString();

export class FakeT3Server {
  readonly token = "fake-bearer-token";
  readonly tickets = new Set<string>();
  readonly requests: ReceivedRequest[] = [];
  readonly threads = new Map<string, FakeThread>();
  readonly receipts = new Map<string, { threadId: string; sequence: number }>();
  readonly upgrades: Array<{ protocol: string | null; ticketOk: boolean }> = [];
  /** Methods whose next response is swallowed after the work is applied. */
  readonly dropResponses = new Set<string>();
  /** Answer the next afterSequence resume with a snapshot instead. */
  forceSnapshotOnResume = false;
  private sequence = 0;
  private ticketCounter = 0;
  private socketCounter = 0;
  private readonly sockets = new Set<ServerWebSocket<SocketData>>();
  private readonly server: ReturnType<typeof Bun.serve<SocketData>>;

  constructor() {
    this.server = Bun.serve<SocketData>({
      port: 0,
      hostname: "127.0.0.1",
      fetch: (request, server) => this.handleHttp(request, server),
      websocket: {
        open: (ws) => {
          this.sockets.add(ws);
        },
        close: (ws) => {
          this.sockets.delete(ws);
        },
        message: (ws, message) => {
          const text =
            typeof message === "string" ? message : message.toString();
          const decoded = JSON.parse(text) as unknown;
          const frames = Array.isArray(decoded) ? decoded : [decoded];
          for (const frame of frames) this.handleFrame(ws, frame as Json);
        },
      },
    });
  }

  get baseUrl(): URL {
    return new URL(`http://127.0.0.1:${this.server.port}`);
  }

  stop(): void {
    this.server.stop(true);
  }

  requestsFor(tag: string): ReceivedRequest[] {
    return this.requests.filter((request) => request.tag === tag);
  }

  commandsOf(type: string): Json[] {
    return this.requestsFor("orchestration.dispatchCommand")
      .map((request) => request.payload)
      .filter((payload) => payload.type === type);
  }

  closeAllSockets(): void {
    for (const ws of this.sockets) ws.close(1012, "restart");
  }

  thread(threadId: string): FakeThread {
    const thread = this.threads.get(threadId);
    if (thread === undefined) throw new Error(`no fake thread ${threadId}`);
    return thread;
  }

  // -- agent-side helpers --------------------------------------------------

  updateRun(threadId: string, runId: string, patch: Partial<FakeRun>): void {
    const thread = this.thread(threadId);
    const run = thread.runs.find((entry) => entry.id === runId);
    if (run === undefined) throw new Error(`no fake run ${runId}`);
    Object.assign(run, patch);
    this.append(thread, { type: "run.updated", runId, payload: { ...run } });
  }

  upsertItem(threadId: string, item: Json): void {
    const thread = this.thread(threadId);
    const full: Json = {
      threadId,
      nodeId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      title: null,
      startedAt: null,
      completedAt: null,
      updatedAt: now(),
      ...item,
    };
    const index = thread.turnItems.findIndex((entry) => entry.id === item.id);
    if (index >= 0) thread.turnItems[index] = full;
    else thread.turnItems.push(full);
    this.append(thread, { type: "turn-item.updated", payload: full });
  }

  upsertRequest(threadId: string, request: Json): void {
    const thread = this.thread(threadId);
    const full: Json = {
      nodeId: "node-1",
      providerTurnId: null,
      nativeRequestRef: null,
      responseCapability: { type: "message" },
      createdAt: now(),
      resolvedAt: null,
      ...request,
    };
    const index = thread.runtimeRequests.findIndex(
      (entry) => entry.id === request.id,
    );
    if (index >= 0) thread.runtimeRequests[index] = full;
    else thread.runtimeRequests.push(full);
    this.append(thread, { type: "runtime-request.updated", payload: full });
  }

  /** An event type the driver does not know: must be skipped, cursor kept. */
  emitUnknown(threadId: string): void {
    this.append(this.thread(threadId), {
      type: "thread.future-thing",
      payload: { anything: true },
    });
  }

  // -- internals -----------------------------------------------------------

  private handleHttp(
    request: Request,
    server: Bun.Server<SocketData>,
  ): Response | undefined {
    const url = new URL(request.url);
    if (url.pathname === "/.well-known/t3/environment") {
      return Response.json({
        environmentId: "fake-env",
        label: "fake",
        platform: { os: "linux", arch: "x64" },
        serverVersion: "0.1.2-nightly.20261021.3473",
        orchestrationProtocolVersion: 2,
        capabilities: {},
      });
    }
    if (url.pathname === "/api/auth/websocket-ticket") {
      if (
        request.method !== "POST" ||
        request.headers.get("authorization") !== `Bearer ${this.token}`
      ) {
        return new Response("unauthorized", { status: 401 });
      }
      const ticket = `ticket-${++this.ticketCounter}`;
      this.tickets.add(ticket);
      return Response.json({
        ticket,
        expiresAt: new Date(Date.now() + 300_000).toISOString(),
      });
    }
    if (url.pathname === "/ws") {
      const protocol = url.searchParams.get("orchestrationProtocol");
      const ticket = url.searchParams.get("wsTicket");
      const ticketOk = ticket !== null && this.tickets.delete(ticket);
      this.upgrades.push({ protocol, ticketOk });
      if (protocol !== "2") {
        return new Response("orchestration_protocol_incompatible", {
          status: 426,
        });
      }
      if (!ticketOk) return new Response("unauthorized", { status: 401 });
      const upgraded = server.upgrade(request, {
        data: { id: ++this.socketCounter, subscriptions: new Map() },
      });
      return upgraded
        ? undefined
        : new Response("upgrade failed", { status: 400 });
    }
    return new Response("not found", { status: 404 });
  }

  private send(ws: ServerWebSocket<SocketData>, frame: Json): void {
    ws.send(JSON.stringify(frame));
  }

  private succeed(
    ws: ServerWebSocket<SocketData>,
    tag: string,
    requestId: string,
    value: unknown,
  ): void {
    if (this.dropResponses.delete(tag)) {
      // Work applied, response lost: the socket dies before the Exit.
      ws.close(1011, "lost");
      return;
    }
    this.send(ws, {
      _tag: "Exit",
      requestId,
      exit: { _tag: "Success", value },
    });
  }

  private fail(
    ws: ServerWebSocket<SocketData>,
    requestId: string,
    errorTag: string,
    message: string,
  ): void {
    this.send(ws, {
      _tag: "Exit",
      requestId,
      exit: {
        _tag: "Failure",
        cause: [{ _tag: "Fail", error: { _tag: errorTag, message } }],
      },
    });
  }

  private handleFrame(ws: ServerWebSocket<SocketData>, frame: Json): void {
    switch (frame._tag) {
      case "Ping":
        this.send(ws, { _tag: "Pong" });
        return;
      case "Ack":
        return;
      case "Interrupt":
        ws.data.subscriptions.delete(String(frame.requestId));
        return;
      case "Request":
        this.handleRequest(
          ws,
          String(frame.id),
          String(frame.tag),
          (frame.payload ?? {}) as Json,
        );
        return;
      default:
        return;
    }
  }

  private handleRequest(
    ws: ServerWebSocket<SocketData>,
    id: string,
    tag: string,
    payload: Json,
  ): void {
    this.requests.push({ tag, payload });
    switch (tag) {
      case "orchestration.launchThread":
        this.launchThread(ws, id, tag, payload);
        return;
      case "orchestration.dispatchCommand":
        this.dispatch(ws, id, tag, payload);
        return;
      case "orchestration.getThreadProjection": {
        const thread = this.threads.get(String(payload.threadId));
        if (thread === undefined) {
          this.fail(
            ws,
            id,
            "OrchestrationV2GetThreadProjectionError",
            "Thread not found.",
          );
          return;
        }
        this.succeed(ws, tag, id, this.projection(thread));
        return;
      }
      case "orchestration.subscribeThread":
        this.subscribe(ws, id, payload);
        return;
      default:
        this.send(ws, {
          _tag: "Exit",
          requestId: id,
          exit: {
            _tag: "Failure",
            cause: [{ _tag: "Die", defect: `Unknown request tag: ${tag}` }],
          },
        });
    }
  }

  private projection(thread: FakeThread): Json {
    return {
      thread: {
        id: thread.id,
        projectId: thread.projectId,
        title: thread.launch.title,
      },
      runs: thread.runs.map((run) => ({ ...run })),
      attempts: [],
      nodes: [],
      subagents: [],
      providerSessions: [],
      providerThreads: [],
      providerTurns: [],
      runtimeRequests: thread.runtimeRequests.map((entry) => ({ ...entry })),
      messages: thread.messages.map((entry) => ({ ...entry })),
      plans: [],
      turnItems: thread.turnItems.map((entry) => ({ ...entry })),
      checkpointScopes: [],
      checkpoints: [],
      contextHandoffs: [],
      contextTransfers: [],
      visibleTurnItems: [],
      updatedAt: now(),
    };
  }

  private append(thread: FakeThread, event: Json): number {
    const sequence = ++this.sequence;
    const stored = {
      sequence,
      event: {
        id: `event-${sequence}`,
        threadId: thread.id,
        occurredAt: now(),
        ...event,
      },
    };
    thread.events.push(stored);
    for (const ws of this.sockets) {
      for (const [requestId, subscription] of ws.data.subscriptions) {
        if (subscription.threadId !== thread.id) continue;
        this.send(ws, {
          _tag: "Chunk",
          requestId,
          values: [{ kind: "event", ...stored }],
        });
      }
    }
    return sequence;
  }

  private launchThread(
    ws: ServerWebSocket<SocketData>,
    id: string,
    tag: string,
    payload: Json,
  ): void {
    const commandId = String(payload.commandId);
    const threadId = String(payload.threadId);
    const receipt = this.receipts.get(commandId);
    if (receipt !== undefined) {
      this.succeed(ws, tag, id, {
        threadId: receipt.threadId,
        projection: this.projection(this.thread(receipt.threadId)),
        resumed: false,
      });
      return;
    }
    if (this.threads.has(threadId)) {
      this.fail(
        ws,
        id,
        "OrchestrationV2ThreadLaunchError",
        "Thread already exists.",
      );
      return;
    }
    const thread: FakeThread = {
      id: threadId,
      projectId: String(payload.projectId),
      launch: payload,
      runs: [],
      messages: [],
      runtimeRequests: [],
      turnItems: [],
      events: [],
    };
    this.threads.set(threadId, thread);
    const sequence = this.append(thread, {
      type: "thread.created",
      payload: { id: threadId },
    });
    this.receipts.set(commandId, { threadId, sequence });
    this.succeed(ws, tag, id, {
      threadId,
      projection: this.projection(thread),
      resumed: false,
    });
  }

  private dispatch(
    ws: ServerWebSocket<SocketData>,
    id: string,
    tag: string,
    command: Json,
  ): void {
    const commandId = String(command.commandId);
    const receipt = this.receipts.get(commandId);
    if (receipt !== undefined) {
      this.succeed(ws, tag, id, { sequence: receipt.sequence });
      return;
    }
    const thread = this.threads.get(String(command.threadId));
    if (thread === undefined) {
      this.fail(
        ws,
        id,
        "OrchestrationV2DispatchCommandError",
        `Thread '${String(command.threadId)}' does not exist for command '${String(command.type)}'.`,
      );
      return;
    }
    let sequence: number;
    switch (command.type) {
      case "message.dispatch": {
        const mode = command.dispatchMode as Json;
        const active = thread.runs.find((run) =>
          ["preparing", "starting", "running", "waiting"].includes(run.status),
        );
        const messageId = String(command.messageId);
        if (mode.type === "start_immediately" && active !== undefined) {
          this.fail(
            ws,
            id,
            "OrchestrationV2DispatchCommandError",
            "The thread already has an active run.",
          );
          return;
        }
        if (mode.type === "steer_active") {
          const message = this.userMessage(
            thread,
            messageId,
            command,
            String(mode.targetRunId),
          );
          sequence = this.append(thread, {
            type: "message.updated",
            payload: message,
          });
          break;
        }
        const run: FakeRun = {
          id: `run-${thread.runs.length + 1}`,
          threadId: thread.id,
          ordinal: thread.runs.length + 1,
          userMessageId: messageId,
          status: mode.type === "queue_after_active" ? "queued" : "starting",
        };
        thread.runs.push(run);
        const message = this.userMessage(thread, messageId, command, run.id);
        this.append(thread, { type: "run.created", payload: { ...run } });
        sequence = this.append(thread, {
          type: "message.updated",
          payload: message,
        });
        break;
      }
      case "run.interrupt": {
        const run = thread.runs.find((entry) => entry.id === command.runId);
        if (run === undefined) {
          this.fail(
            ws,
            id,
            "OrchestrationV2DispatchCommandError",
            "Run not found.",
          );
          return;
        }
        run.status = "interrupted";
        sequence = this.append(thread, {
          type: "run.updated",
          payload: { ...run },
        });
        break;
      }
      case "runtime-request.respond":
      case "thread.user-input.dismiss": {
        const request = thread.runtimeRequests.find(
          (entry) => entry.id === command.requestId,
        );
        if (request === undefined) {
          this.fail(
            ws,
            id,
            "OrchestrationV2DispatchCommandError",
            "Request not found.",
          );
          return;
        }
        request.status =
          command.type === "thread.user-input.dismiss"
            ? "cancelled"
            : "resolved";
        request.resolvedAt = now();
        if (command.decision !== undefined) request.decision = command.decision;
        if (command.answers !== undefined) request.answers = command.answers;
        sequence = this.append(thread, {
          type: "runtime-request.updated",
          payload: { ...request },
        });
        break;
      }
      default:
        this.fail(
          ws,
          id,
          "OrchestrationV2DispatchCommandError",
          `Unsupported command ${String(command.type)}`,
        );
        return;
    }
    this.receipts.set(commandId, { threadId: thread.id, sequence });
    this.succeed(ws, tag, id, { sequence });
  }

  private userMessage(
    thread: FakeThread,
    messageId: string,
    command: Json,
    runId: string,
  ): Json {
    const message = {
      id: messageId,
      threadId: thread.id,
      runId,
      nodeId: null,
      role: "user",
      text: command.text,
      attachments: [],
      streaming: false,
      createdBy: "user",
      creationSource: command.creationSource,
      createdAt: now(),
      updatedAt: now(),
    };
    thread.messages.push(message);
    return message;
  }

  private subscribe(
    ws: ServerWebSocket<SocketData>,
    id: string,
    payload: Json,
  ): void {
    const thread = this.threads.get(String(payload.threadId));
    if (thread === undefined) {
      this.fail(
        ws,
        id,
        "OrchestrationV2GetThreadProjectionError",
        "Thread not found.",
      );
      return;
    }
    const after = payload.afterSequence;
    let initial: Json[];
    if (typeof after === "number" && !this.forceSnapshotOnResume) {
      initial = thread.events
        .filter((stored) => stored.sequence > after)
        .map((stored) => ({ kind: "event", ...stored }));
    } else {
      this.forceSnapshotOnResume = false;
      initial = [
        {
          kind: "snapshot",
          snapshotSequence: thread.events.at(-1)?.sequence ?? 0,
          projection: this.projection(thread),
        },
      ];
    }
    if (initial.length > 0) {
      this.send(ws, { _tag: "Chunk", requestId: id, values: initial });
    }
    ws.data.subscriptions.set(id, { threadId: thread.id });
  }
}
