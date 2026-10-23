import { describe, expect, it } from "@effect/vitest";
import { T3EventMapper } from "../src/drivers/t3/mapping.js";
import type { ThreadStreamItem } from "../src/drivers/t3/wire.js";

const run = (status: string, extra: Record<string, unknown> = {}) => ({
  id: "run-1",
  ordinal: 1,
  userMessageId: "m-1",
  status,
  ...extra,
});

const item = (fields: Record<string, unknown>) => ({
  runId: "run-1",
  status: "completed",
  ordinal: 1,
  title: null,
  ...fields,
});

const event = (
  sequence: number,
  type: string,
  payload: unknown,
): ThreadStreamItem => ({ kind: "event", sequence, event: { type, payload } });

const snapshot = (
  snapshotSequence: number,
  projection: Partial<
    Extract<ThreadStreamItem, { kind: "snapshot" }>["projection"]
  >,
): ThreadStreamItem => ({
  kind: "snapshot",
  snapshotSequence,
  projection: {
    thread: { id: "thread-1" },
    runs: [],
    messages: [],
    runtimeRequests: [],
    turnItems: [],
    ...projection,
  },
});

describe("T3EventMapper", () => {
  it.each(["cancelled", "canceled"])(
    "maps %s todo steps to Linear canceled",
    (status) => {
      const mapper = new T3EventMapper("thread-1");
      expect(
        mapper.apply(
          event(
            1,
            "turn-item.updated",
            item({
              id: "plan",
              type: "todo_list",
              steps: [{ id: "step", text: "Abandoned", status }],
            }),
          ),
        ),
      ).toMatchObject([
        { _tag: "Plan", steps: [{ content: "Abandoned", status: "canceled" }] },
      ]);
    },
  );
  it("drops replayed sequences and advances past unknown events", () => {
    const mapper = new T3EventMapper("thread-1");
    expect(mapper.apply(event(1, "run.created", run("starting")))).toHaveLength(
      1,
    );
    expect(mapper.apply(event(1, "run.created", run("starting")))).toEqual([]);
    expect(mapper.apply(event(2, "thread.brand-new", { x: 1 }))).toEqual([]);
    expect(mapper.cursor).toBe(2);
    expect(mapper.apply(event(3, "run.updated", run("running")))).toEqual([]);
  });

  it("ignores events whose payload no longer matches the pinned schema", () => {
    const mapper = new T3EventMapper("thread-1");
    expect(mapper.apply(event(1, "run.updated", { id: 7 }))).toEqual([]);
    expect(
      mapper.apply(
        event(2, "turn-item.updated", item({ id: "i", type: "reasoning" })),
      ),
    ).toEqual([]);
    expect(mapper.cursor).toBe(2);
  });

  it("emits only missing transitions when a snapshot follows events", () => {
    const mapper = new T3EventMapper("thread-1");
    const live = [
      ...mapper.apply(event(1, "run.created", run("running"))),
      ...mapper.apply(
        event(
          2,
          "turn-item.updated",
          item({
            id: "cmd",
            type: "command_execution",
            status: "running",
            input: "ls",
          }),
        ),
      ),
    ];
    expect(live.map((e) => e._tag)).toEqual(["TurnStarted", "ToolStarted"]);

    const reconciled = mapper.apply(
      snapshot(9, {
        runs: [run("completed")],
        messages: [
          {
            id: "assistant-1",
            runId: "run-1",
            role: "assistant",
            text: "All done",
            streaming: false,
          },
        ],
        turnItems: [
          item({
            id: "cmd",
            type: "command_execution",
            input: "ls",
            output: "a",
          }),
          item({ id: "future", type: "hologram", ordinal: 2 }),
        ],
      }),
    );
    expect(reconciled.map((e) => e._tag)).toEqual(["ToolEnded", "TurnEnded"]);
    expect(reconciled.at(-1)).toMatchObject({
      key: "t3:thread-1:run:run-1:ended",
      cursor: "9",
      outcome: { status: "completed", finalText: "All done", error: null },
    });
    expect(mapper.cursor).toBe(9);
    expect(mapper.apply(snapshot(9, { runs: [run("completed")] }))).toEqual([]);
  });

  it("keys transitions by entity so a fresh mapper reproduces them", () => {
    const projection = {
      runs: [run("running", { queueHeld: false })],
      turnItems: [
        item({ id: "r", type: "reasoning", text: "hmm", streaming: false }),
      ],
    };
    const first = new T3EventMapper("thread-1").apply(snapshot(4, projection));
    const second = new T3EventMapper("thread-1").apply(snapshot(7, projection));
    expect(first.map((e) => e.key)).toEqual(second.map((e) => e.key));
  });

  it("reports queue holds and approval prompts without offered options", () => {
    const mapper = new T3EventMapper("thread-1");
    const held = mapper.apply(
      event(1, "run.created", run("queued", { queueHeld: true })),
    );
    expect(held).toMatchObject([
      { _tag: "QueueHeld", held: true, turnId: "run-1" },
    ]);
    const approval = mapper.apply(
      event(
        2,
        "turn-item.updated",
        item({
          id: "ap",
          type: "approval_request",
          status: "waiting",
          requestId: "req",
          requestKind: "file-change",
        }),
      ),
    );
    expect(approval).toMatchObject([
      {
        _tag: "RequestOpened",
        prompt: "Approve file-change?",
        options: [
          { label: "Approve", value: "once" },
          { label: "Deny", value: "deny" },
        ],
      },
    ]);
    // Resolution of a request this mapper never opened is not reported.
    expect(
      mapper.apply(
        event(3, "runtime-request.updated", {
          id: "other",
          kind: "command",
          status: "resolved",
        }),
      ),
    ).toEqual([]);
  });
});
