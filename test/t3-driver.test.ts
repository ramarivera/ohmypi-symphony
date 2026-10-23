import { describe, expect, it } from "@effect/vitest";
import {
  Chunk,
  Duration,
  Effect,
  Exit,
  Fiber,
  Redacted,
  Schedule,
  Scope,
  Stream,
} from "effect";
import type {
  AgentEvent,
  AgentSession,
  OpenSessionInput,
} from "../src/drivers/driver.js";
import {
  makeT3Driver,
  type T3DriverOptions,
} from "../src/drivers/t3/driver.js";
import { FakeT3Server } from "./fixtures/fake-t3.js";

const withServer = Effect.acquireRelease(
  Effect.sync(() => new FakeT3Server()),
  (server) => Effect.sync(() => server.stop()),
);

const driverFor = (
  server: FakeT3Server,
  overrides: Partial<T3DriverOptions> = {},
) =>
  makeT3Driver({
    baseUrl: server.baseUrl,
    token: Effect.succeed(Redacted.make(server.token)),
    projectId: "project-1",
    modelSelection: {
      instanceId: "codex-cliproxy",
      model: "gpt-6-sol",
      options: [{ id: "reasoningEffort", value: "high" }],
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    idNamespace: "symtest",
    reconnectBaseDelayMs: 10,
    reconnectMaxDelayMs: 50,
    pingIntervalMs: 1_000,
    requestTimeoutMs: 2_000,
    connectTimeoutMs: 2_000,
    ...overrides,
  });

const openInput: OpenSessionInput = {
  sessionKey: "session-abc",
  title: "ENG-1 Fix the flaky test",
  workspace: { _tag: "ProjectRoot" },
};
const THREAD_ID = "symtest-session-abc";

const waitUntil = (condition: () => boolean, label: string) =>
  Effect.suspend(() =>
    condition() ? Effect.void : Effect.fail(new Error(`waiting for ${label}`)),
  ).pipe(
    Effect.retry(
      Schedule.spaced(Duration.millis(5)).pipe(
        Schedule.upTo(Duration.seconds(3)),
      ),
    ),
    Effect.orDie,
  );

const subscribeCount = (server: FakeT3Server) =>
  server.requestsFor("orchestration.subscribeThread").length;

/** Collects events until (and including) the first one matching `last`. */
const collectUntil = (
  session: AgentSession,
  last: (event: AgentEvent) => boolean,
) =>
  session.events.pipe(
    Stream.takeUntil(last),
    Stream.runCollect,
    Effect.map(Chunk.toReadonlyArray),
    Effect.timeoutFail({
      duration: Duration.seconds(5),
      onTimeout: () => new Error("timed out collecting events"),
    }),
  );

const take = (session: AgentSession, count: number) =>
  session.events.pipe(
    Stream.take(count),
    Stream.runCollect,
    Effect.map(Chunk.toReadonlyArray),
    Effect.timeoutFail({
      duration: Duration.seconds(5),
      onTimeout: () => new Error(`timed out taking ${count} events`),
    }),
  );

const tags = (events: ReadonlyArray<AgentEvent>) =>
  events.map((event) => event._tag);

const runningRun = (server: FakeT3Server, runId = "run-1") =>
  server.updateRun(THREAD_ID, runId, { status: "running" });

describe("T3 driver auth and protocol", () => {
  it.scopedLive(
    "probes the descriptor and exchanges the bearer for a ticket",
    () =>
      Effect.gen(function* () {
        const server = yield* withServer;
        const result = yield* driverFor(server).probe;
        expect(result.detail).toContain("0.1.2-nightly.20261021.3473");
        expect(result.detail).toContain("protocol 2");
        expect(server.tickets.size).toBe(1);
      }),
  );

  it.scopedLive("reports a rejected bearer as an auth error", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      const driver = driverFor(server, {
        token: Effect.succeed(Redacted.make("wrong-token")),
      });
      const probe = yield* Effect.flip(driver.probe);
      expect(probe.reason).toBe("auth");
      expect(probe.message).not.toContain("wrong-token");
      const open = yield* Effect.flip(driver.open(openInput));
      expect(open.reason).toBe("auth");
      expect(open.submission).toBe("not-submitted");
      expect(server.threads.size).toBe(0);
    }),
  );

  it.scopedLive("connects with a single-use ticket and protocol 2", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      yield* driverFor(server).open(openInput);
      expect(server.upgrades[0]).toEqual({ protocol: "2", ticketOk: true });
    }),
  );
});

describe("T3 driver launch", () => {
  it.scopedLive("launches one thread per session key, idempotently", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      const driver = driverFor(server);
      const first = yield* Scope.make();
      const session = yield* driver.open(openInput).pipe(Scope.extend(first));
      expect(session.ref).toEqual({
        driver: "t3",
        nativeId: THREAD_ID,
        locator: { projectId: "project-1", origin: server.baseUrl.origin },
      });
      yield* Scope.close(first, Exit.void);

      yield* driver.open(openInput);
      const launches = server.requestsFor("orchestration.launchThread");
      expect(launches).toHaveLength(2);
      expect(launches[0]?.payload).toEqual(launches[1]?.payload);
      expect(launches[0]?.payload).toMatchObject({
        commandId: `symtest:${THREAD_ID}:launch`,
        threadId: THREAD_ID,
        projectId: "project-1",
        title: "ENG-1 Fix the flaky test",
        creationSource: "web",
        modelSelection: {
          instanceId: "codex-cliproxy",
          model: "gpt-6-sol",
          options: [{ id: "reasoningEffort", value: "high" }],
        },
        runtimeMode: "full-access",
        interactionMode: "default",
        workspaceStrategy: { type: "root" },
      });
      expect(launches[0]?.payload).not.toHaveProperty("reuseExistingThread");
      expect(server.threads.size).toBe(1);
    }),
  );

  it.scopedLive("adopts a launched thread after a lost launch response", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      server.dropResponses.add("orchestration.launchThread");
      const session = yield* driverFor(server).open({
        ...openInput,
        workspace: { _tag: "DriverWorktree", baseRef: "main", branch: "eng-1" },
      });
      expect(session.ref.nativeId).toBe(THREAD_ID);
      expect(server.requestsFor("orchestration.launchThread")).toHaveLength(1);
      expect(server.thread(THREAD_ID).launch.workspaceStrategy).toEqual({
        type: "worktree",
        baseRef: "main",
        branch: "eng-1",
      });
    }),
  );

  it.scopedLive("hashes session keys that are unsafe as ids", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      const session = yield* driverFor(server).open({
        ...openInput,
        sessionKey: "linear/session with spaces",
      });
      expect(session.ref.nativeId).toMatch(/^symtest-[0-9a-f]{32}$/u);
    }),
  );
});

describe("T3 driver turns and events", () => {
  it.scopedLive("maps a full run onto normalized events and an outcome", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      const session = yield* driverFor(server).open(openInput);
      yield* waitUntil(() => subscribeCount(server) === 1, "subscription");
      const collector = yield* Effect.fork(
        collectUntil(session, (event) => event._tag === "TurnEnded"),
      );

      const handle = yield* session.sendTurn({
        clientRequestId: "delivery-1",
        text: "Fix the flaky test",
        delivery: "auto",
      });
      expect(handle).toMatchObject({
        turnId: "run-1",
        delivery: "started",
        adopted: false,
      });
      expect(server.commandsOf("message.dispatch")[0]).toMatchObject({
        commandId: `symtest:${THREAD_ID}:dispatch:delivery-1`,
        messageId: `symtest:${THREAD_ID}:message:delivery-1`,
        createdBy: "user",
        creationSource: "web",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
      });

      runningRun(server);
      server.upsertItem(THREAD_ID, {
        id: "item-reasoning",
        runId: "run-1",
        type: "reasoning",
        status: "completed",
        ordinal: 1,
        text: "Looking at the test",
        streaming: false,
      });
      server.upsertItem(THREAD_ID, {
        id: "item-cmd",
        runId: "run-1",
        type: "command_execution",
        status: "running",
        ordinal: 2,
        input: "bun test",
      });
      server.upsertItem(THREAD_ID, {
        id: "item-cmd",
        runId: "run-1",
        type: "command_execution",
        status: "completed",
        ordinal: 2,
        input: "bun test",
        output: "1 failed",
        exitCode: 1,
      });
      server.upsertItem(THREAD_ID, {
        id: "item-todo",
        runId: "run-1",
        type: "todo_list",
        status: "running",
        ordinal: 3,
        planId: "plan-1",
        steps: [
          { id: "s1", text: "Reproduce", status: "completed" },
          { id: "s2", text: "Fix", status: "running" },
        ],
      });
      server.emitUnknown(THREAD_ID);
      server.upsertItem(THREAD_ID, {
        id: "item-answer",
        runId: "run-1",
        type: "assistant_message",
        status: "running",
        ordinal: 4,
        messageId: "assistant-1",
        text: "Fixed",
        streaming: true,
      });
      server.upsertItem(THREAD_ID, {
        id: "item-answer",
        runId: "run-1",
        type: "assistant_message",
        status: "completed",
        ordinal: 4,
        messageId: "assistant-1",
        text: "Fixed the race in the test.",
        streaming: false,
      });
      server.updateRun(THREAD_ID, "run-1", { status: "completed" });

      const events = yield* Fiber.join(collector);
      expect(tags(events)).toEqual([
        "TurnStarted",
        "Thinking",
        "ToolStarted",
        "ToolEnded",
        "Plan",
        "AssistantMessage",
        "TurnEnded",
      ]);
      expect(events[3]).toMatchObject({
        name: "command",
        isError: true,
        result: "1 failed",
        key: `t3:${THREAD_ID}:item:item-cmd:ended`,
      });
      expect(events[4]).toMatchObject({
        steps: [
          { content: "Reproduce", status: "completed" },
          { content: "Fix", status: "inProgress" },
        ],
      });
      const outcome = yield* handle.outcome;
      expect(outcome).toEqual({
        turnId: "run-1",
        status: "completed",
        finalText: "Fixed the race in the test.",
        error: null,
      });
      expect(events.at(-1)).toMatchObject({ outcome });
      const keys = events.map((event) => event.key);
      expect(new Set(keys).size).toBe(keys.length);
      for (const event of events) expect(event.cursor).toMatch(/^\d+$/u);
    }),
  );

  it.scopedLive("queues behind an active run and steers into it", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      const session = yield* driverFor(server).open(openInput);
      const first = yield* session.sendTurn({
        clientRequestId: "d-1",
        text: "first",
        delivery: "start",
      });
      runningRun(server);
      const queued = yield* session.sendTurn({
        clientRequestId: "d-2",
        text: "second",
        delivery: "auto",
      });
      expect(queued).toMatchObject({ turnId: "run-2", delivery: "queued" });
      const steered = yield* session.sendTurn({
        clientRequestId: "d-3",
        text: "also consider this",
        delivery: "steer",
      });
      expect(steered).toMatchObject({
        turnId: first.turnId,
        delivery: "steered",
      });
      expect(
        server.commandsOf("message.dispatch").map((c) => c.dispatchMode),
      ).toEqual([
        { type: "start_immediately" },
        { type: "queue_after_active" },
        { type: "steer_active", targetRunId: "run-1" },
      ]);
      const busy = yield* Effect.flip(
        session.sendTurn({
          clientRequestId: "d-4",
          text: "forced start",
          delivery: "start",
        }),
      );
      expect(busy).toMatchObject({
        reason: "rejected",
        submission: "not-submitted",
      });

      const snapshot = yield* session.snapshot;
      expect(snapshot.activeTurnId).toBe("run-1");
      expect(snapshot.turns).toEqual([
        { turnId: "run-1", status: "running", clientRequestId: "d-1" },
        { turnId: "run-2", status: "queued", clientRequestId: "d-2" },
      ]);
      expect([...snapshot.acceptedClientRequestIds].sort()).toEqual([
        "d-1",
        "d-2",
        "d-3",
      ]);
    }),
  );
});

describe("T3 driver reconnect", () => {
  it.scopedLive("resubscribes from afterSequence without duplicates", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      const session = yield* driverFor(server).open(openInput);
      yield* waitUntil(() => subscribeCount(server) === 1, "subscription");
      const handle = yield* session.sendTurn({
        clientRequestId: "d-1",
        text: "go",
        delivery: "auto",
      });
      server.upsertItem(THREAD_ID, {
        id: "item-reasoning",
        runId: "run-1",
        type: "reasoning",
        status: "completed",
        ordinal: 1,
        text: "thinking",
        streaming: false,
      });
      const before = yield* take(session, 2);
      expect(tags(before)).toEqual(["TurnStarted", "Thinking"]);
      const cursorBefore = Number(before.at(-1)?.cursor);

      server.closeAllSockets();
      server.upsertItem(THREAD_ID, {
        id: "item-cmd",
        runId: "run-1",
        type: "command_execution",
        status: "completed",
        ordinal: 2,
        input: "ls",
        output: "ok",
        exitCode: 0,
      });
      server.updateRun(THREAD_ID, "run-1", { status: "completed" });

      const after = yield* collectUntil(
        session,
        (event) => event._tag === "TurnEnded",
      );
      expect(tags(after)).toEqual(["ToolStarted", "ToolEnded", "TurnEnded"]);
      const resume = server.requestsFor("orchestration.subscribeThread")[1];
      expect(resume?.payload.afterSequence).toBeGreaterThanOrEqual(
        cursorBefore,
      );
      expect(server.upgrades.length).toBeGreaterThanOrEqual(2);
      expect((yield* handle.outcome).status).toBe("completed");
    }),
  );

  it.scopedLive("reconciles a snapshot fallback without re-emitting", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      const session = yield* driverFor(server).open(openInput);
      yield* waitUntil(() => subscribeCount(server) === 1, "subscription");
      yield* session.sendTurn({
        clientRequestId: "d-1",
        text: "go",
        delivery: "auto",
      });
      server.upsertItem(THREAD_ID, {
        id: "item-cmd",
        runId: "run-1",
        type: "command_execution",
        status: "running",
        ordinal: 1,
        input: "make",
      });
      const before = yield* take(session, 2);
      expect(tags(before)).toEqual(["TurnStarted", "ToolStarted"]);

      server.forceSnapshotOnResume = true;
      server.closeAllSockets();
      server.upsertItem(THREAD_ID, {
        id: "item-cmd",
        runId: "run-1",
        type: "command_execution",
        status: "failed",
        ordinal: 1,
        input: "make",
        output: "boom",
      });
      server.upsertItem(THREAD_ID, {
        id: "item-error",
        runId: "run-1",
        type: "error",
        status: "completed",
        ordinal: 2,
        failure: {
          class: "provider_error",
          message: "provider crashed",
          code: null,
          retryable: false,
        },
      });
      server.updateRun(THREAD_ID, "run-1", { status: "failed" });

      const after = yield* collectUntil(
        session,
        (event) => event._tag === "TurnEnded",
      );
      expect(tags(after)).toEqual(["ToolEnded", "Error", "TurnEnded"]);
      expect(after.at(-1)).toMatchObject({
        outcome: { status: "failed", error: "provider crashed" },
      });
    }),
  );

  it.scopedLive("resumes a persisted session from its cursor", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      const driver = driverFor(server);
      const firstScope = yield* Scope.make();
      const first = yield* driver
        .open(openInput)
        .pipe(Scope.extend(firstScope));
      yield* first.sendTurn({
        clientRequestId: "d-1",
        text: "go",
        delivery: "auto",
      });
      const seen = yield* take(first, 1);
      yield* Scope.close(firstScope, Exit.void);

      server.updateRun(THREAD_ID, "run-1", { status: "completed" });
      const resumed = yield* driver.open({
        ...openInput,
        resume: { ref: first.ref, cursor: seen[0]?.cursor ?? "0" },
      });
      const events = yield* take(resumed, 1);
      expect(tags(events)).toEqual(["TurnEnded"]);
      expect(server.requestsFor("orchestration.launchThread")).toHaveLength(1);
      expect(
        server.requestsFor("orchestration.subscribeThread").at(-1)?.payload,
      ).toEqual({
        threadId: THREAD_ID,
        afterSequence: Number(seen[0]?.cursor),
      });
    }),
  );
});

describe("T3 driver control", () => {
  it.scopedLive("interrupts the active run", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      const session = yield* driverFor(server).open(openInput);
      const handle = yield* session.sendTurn({
        clientRequestId: "d-1",
        text: "go",
        delivery: "auto",
      });
      runningRun(server);
      expect(yield* session.interrupt()).toBe("requested");
      expect(server.commandsOf("run.interrupt")[0]).toMatchObject({
        commandId: `symtest:${THREAD_ID}:interrupt:run-1`,
        runId: "run-1",
      });
      const outcome = yield* handle.outcome.pipe(Effect.timeout("5 seconds"));
      expect(outcome.status).toBe("interrupted");
      expect(yield* session.interrupt()).toBe("no_active_turn");
    }),
  );

  it.scopedLive("surfaces approvals and forwards the decision", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      const session = yield* driverFor(server).open(openInput);
      yield* session.sendTurn({
        clientRequestId: "d-1",
        text: "go",
        delivery: "auto",
      });
      runningRun(server);
      server.updateRun(THREAD_ID, "run-1", { status: "waiting" });
      server.upsertItem(THREAD_ID, {
        id: "item-approval",
        runId: "run-1",
        type: "approval_request",
        status: "waiting",
        ordinal: 1,
        requestId: "req-1",
        requestKind: "command",
        prompt: "Run `rm -rf build`?",
        options: [
          { decision: "accept", label: "Allow once" },
          { decision: "acceptForSession", label: "Allow for session" },
          { decision: "decline", label: "Deny" },
        ],
      });
      server.upsertRequest(THREAD_ID, {
        id: "req-1",
        kind: "command",
        status: "pending",
      });
      const opened = yield* collectUntil(
        session,
        (event) => event._tag === "RequestOpened",
      );
      expect(opened.at(-1)).toMatchObject({
        _tag: "RequestOpened",
        requestId: "req-1",
        turnId: "run-1",
        kind: "approval",
        prompt: "Run `rm -rf build`?",
        options: [
          { label: "Allow once", value: "once" },
          { label: "Allow for session", value: "session" },
          { label: "Deny", value: "deny" },
        ],
      });
      expect((yield* session.snapshot).pendingRequests).toEqual([
        { requestId: "req-1", kind: "approval" },
      ]);

      yield* session.respond("req-1", { _tag: "Approve", scope: "session" });
      expect(server.commandsOf("runtime-request.respond")[0]).toMatchObject({
        commandId: `symtest:${THREAD_ID}:respond:req-1`,
        requestId: "req-1",
        decision: "acceptForSession",
      });
      const resolved = yield* take(session, 1);
      expect(resolved[0]).toMatchObject({
        _tag: "RequestResolved",
        requestId: "req-1",
        status: "resolved",
      });
    }),
  );

  it.scopedLive("surfaces questions and forwards answers or dismissal", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      const session = yield* driverFor(server).open(openInput);
      yield* session.sendTurn({
        clientRequestId: "d-1",
        text: "go",
        delivery: "auto",
      });
      server.upsertItem(THREAD_ID, {
        id: "item-q",
        runId: "run-1",
        type: "user_input_request",
        status: "waiting",
        ordinal: 1,
        requestId: "req-q",
        questions: [
          {
            id: "q1",
            header: "Branch",
            question: "Which branch should I target?",
            options: [
              { label: "main", description: "Default branch" },
              { label: "next", description: "Release train", value: "next" },
            ],
          },
        ],
      });
      server.upsertRequest(THREAD_ID, {
        id: "req-q",
        kind: "user_input",
        status: "pending",
      });
      const opened = yield* collectUntil(
        session,
        (event) => event._tag === "RequestOpened",
      );
      expect(opened.at(-1)).toMatchObject({
        kind: "question",
        prompt: "Which branch should I target?",
        options: [
          { label: "main", value: "main" },
          { label: "next", value: "next" },
        ],
        questions: [
          {
            id: "q1",
            multiSelect: false,
            allowCustomAnswer: true,
          },
        ],
      });
      yield* session.respond("req-q", {
        _tag: "Answer",
        answers: { q1: "next" },
      });
      expect(server.commandsOf("runtime-request.respond")[0]).toMatchObject({
        requestId: "req-q",
        answers: { q1: "next" },
      });
      expect(
        server.commandsOf("runtime-request.respond")[0],
      ).not.toHaveProperty("decision");

      server.upsertRequest(THREAD_ID, {
        id: "req-q2",
        kind: "user_input",
        status: "pending",
      });
      yield* session.respond("req-q2", { _tag: "Dismiss" });
      expect(server.commandsOf("thread.user-input.dismiss")).toHaveLength(1);
    }),
  );

  it.scopedLive("adopts a dispatched message after a lost response", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      const session = yield* driverFor(server).open(openInput);
      yield* waitUntil(() => subscribeCount(server) === 1, "subscription");
      server.dropResponses.add("orchestration.dispatchCommand");
      const handle = yield* session.sendTurn({
        clientRequestId: "delivery-lost",
        text: "go",
        delivery: "auto",
      });
      expect(handle).toMatchObject({ turnId: "run-1", adopted: true });
      expect(server.commandsOf("message.dispatch")).toHaveLength(1);

      // A webhook retry of the same delivery adopts without dispatching.
      const retried = yield* session.sendTurn({
        clientRequestId: "delivery-lost",
        text: "go",
        delivery: "auto",
      });
      expect(retried).toMatchObject({ turnId: "run-1", adopted: true });
      expect(server.commandsOf("message.dispatch")).toHaveLength(1);
      expect(server.thread(THREAD_ID).messages).toHaveLength(1);

      // The event stream survived the dropped socket.
      server.updateRun(THREAD_ID, "run-1", { status: "completed" });
      const outcome = yield* handle.outcome.pipe(Effect.timeout("5 seconds"));
      expect(outcome.status).toBe("completed");
    }),
  );

  it.scopedLive("retries idempotent commands once after a lost response", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      const session = yield* driverFor(server).open(openInput);
      yield* session.sendTurn({
        clientRequestId: "d-1",
        text: "go",
        delivery: "auto",
      });
      runningRun(server);
      server.dropResponses.add("orchestration.dispatchCommand");
      expect(yield* session.interrupt("run-1")).toBe("requested");
      const interrupts = server.commandsOf("run.interrupt");
      expect(interrupts).toHaveLength(2);
      expect(interrupts[0]?.commandId).toBe(interrupts[1]?.commandId);
      expect(
        server
          .thread(THREAD_ID)
          .events.filter((stored) => stored.event.type === "run.updated"),
      ).toHaveLength(2);
    }),
  );

  it.scopedLive("fails the event stream after the reconnect budget", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      const session = yield* driverFor(server, {
        maxReconnectAttempts: 2,
      }).open(openInput);
      const handle = yield* session.sendTurn({
        clientRequestId: "d-1",
        text: "go",
        delivery: "auto",
      });
      yield* take(session, 1);
      server.stop();
      const error = yield* session.events.pipe(
        Stream.runDrain,
        Effect.flip,
        Effect.timeout("5 seconds"),
      );
      expect(error).toMatchObject({
        _tag: "@Gateway/DriverError",
        reason: "lost",
      });
      const outcomeError = yield* Effect.flip(handle.outcome);
      expect(outcomeError.reason).toBe("lost");
    }),
  );
});
