import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import type { LinearGatewayPort } from "../src/domain";
import { ActivityProjector } from "../src/projector";
import { GatewayStore } from "../src/store";

class FakeLinear implements LinearGatewayPort {
  readonly activities: Array<
    Parameters<LinearGatewayPort["createActivity"]>[0]
  > = [];
  readonly updates: Array<Parameters<LinearGatewayPort["updateSession"]>[0]> =
    [];
  failuresRemaining = 0;
  block: Promise<void> | null = null;
  activityStarted: (() => void) | null = null;

  async createActivity(
    input: Parameters<LinearGatewayPort["createActivity"]>[0],
  ): Promise<string> {
    this.activities.push(input);
    this.activityStarted?.();
    if (this.block) await this.block;
    if (this.failuresRemaining > 0) {
      this.failuresRemaining -= 1;
      throw new Error("temporary Linear failure");
    }
    return `activity-${this.activities.length}`;
  }

  async updateSession(
    input: Parameters<LinearGatewayPort["updateSession"]>[0],
  ): Promise<void> {
    this.updates.push(input);
  }

  async refreshInstallation(): Promise<string> {
    return "token";
  }
}

let store: GatewayStore;
let linear: FakeLinear;
let projector: ActivityProjector;

beforeEach(async () => {
  store = await GatewayStore.open(":memory:", new Uint8Array(32).fill(9));
  store.createRun({
    sessionId: "session",
    organizationId: "org",
    issueId: "issue",
  });
  linear = new FakeLinear();
  projector = new ActivityProjector(store, linear);
});

afterEach(() => store.close());

describe("ActivityProjector", () => {
  test("retries a durably queued projection after a transient API failure", async () => {
    linear.failuresRemaining = 1;
    expect(
      await projector.thought("session", "accepted", "Accepted"),
    ).toBeFalse();
    expect(linear.activities).toHaveLength(1);
    expect(store.projectionCount("session", "thought")).toBe(1);

    expect(await projector.flushPending(50, Date.now() + 2_000)).toBe(1);
    expect(linear.activities).toHaveLength(2);
    expect(await projector.flushPending(50, Date.now() + 4_000)).toBe(0);
  });

  test("allows only one concurrent call for the same source projection", async () => {
    const release = Promise.withResolvers<void>();
    const started = Promise.withResolvers<void>();
    linear.block = release.promise;
    linear.activityStarted = started.resolve;

    const first = projector.thought("session", "same-source", "Accepted");
    await started.promise;
    const second = projector.thought("session", "same-source", "Accepted");
    release.resolve();

    expect(await Promise.all([first, second])).toEqual([true, false]);
    expect(linear.activities).toHaveLength(1);
  });

  test("uses first-write-wins for a terminal outcome", async () => {
    expect(
      await projector.terminal("session", "done", "response", "Completed"),
    ).toBeTrue();
    expect(
      await projector.terminal("session", "done", "error", "Late failure"),
    ).toBeFalse();
    expect(linear.activities).toHaveLength(1);
    expect(linear.activities[0]?.content).toEqual({
      type: "response",
      body: "Completed",
    });
  });

  test("waits for agent_end before projecting the final assistant response", async () => {
    await projector.projectRpcEvent("session", 1, {
      type: "message_end",
      message: { role: "assistant", content: "Final answer" },
    });
    expect(linear.activities).toHaveLength(0);

    await projector.projectRpcEvent("session", 2, { type: "agent_end" });
    expect(linear.activities.at(-1)?.content).toEqual({
      type: "response",
      body: "Final answer",
    });
  });

  test("does not project extension UI events as generic elicitations", async () => {
    await projector.projectRpcEvent("session", 1, {
      type: "extension_ui_request",
      method: "setStatus",
    });
    expect(linear.activities).toHaveLength(0);
  });

  test("ignores empty plans because Linear rejects them", async () => {
    expect(await projector.plan("session", "plan:empty", [])).toBeFalse();
    expect(linear.updates).toHaveLength(0);
  });

  test("migrates queued plans from the legacy items wrapper", async () => {
    const plan = [{ content: "Implement", status: "inProgress" }] as const;
    store.enqueueProjection({
      sourceKey: "plan:legacy",
      sessionId: "session",
      activityType: "plan",
      payloadHash: "legacy",
      payload: { request: { sessionId: "session", plan: { items: plan } } },
    });

    expect(await projector.flushPending()).toBe(1);
    expect(linear.updates).toEqual([{ sessionId: "session", plan }]);
  });

  test("settles queued empty legacy plans without calling Linear", async () => {
    store.enqueueProjection({
      sourceKey: "plan:legacy-empty",
      sessionId: "session",
      activityType: "plan",
      payloadHash: "legacy-empty",
      payload: {
        request: { sessionId: "session", plan: { items: [] } },
      },
    });

    expect(await projector.flushPending()).toBe(1);
    expect(linear.updates).toHaveLength(0);
  });

  test("projects full plans and mutation-side external URLs idempotently", async () => {
    const plan = [{ content: "Implement", status: "inProgress" }] as const;
    expect(await projector.plan("session", "plan:v1", plan)).toBeTrue();
    expect(await projector.plan("session", "plan:v1", plan)).toBeFalse();
    expect(
      await projector.externalUrls("session", "urls:v1", [
        { label: "Run", url: "https://gateway.example/runs/session" },
      ]),
    ).toBeTrue();

    expect(linear.updates).toEqual([
      { sessionId: "session", plan },
      {
        sessionId: "session",
        externalUrls: [
          { label: "Run", url: "https://gateway.example/runs/session" },
        ],
      },
    ]);
  });
});
