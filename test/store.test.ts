import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GatewayStore } from "../src/store";

const key = new Uint8Array(32).fill(7);
let store: GatewayStore;

beforeEach(async () => {
  store = await GatewayStore.open(":memory:", key);
});
afterEach(() => store.close());

function installation() {
  return {
    organizationId: "org-1",
    appUserId: "app-user-1",
    accessToken: "access-secret",
    refreshToken: "refresh-secret",
    expiresAt: Date.now() + 60_000,
    scopes: ["read", "write"],
    revokedAt: null,
    accessibleTeamIds: null,
    canAccessAllPublicTeams: null,
  } as const;
}

describe("GatewayStore", () => {
  test("encrypts tokens at rest and round-trips installations", async () => {
    const expected = installation();
    await store.putInstallation(expected);
    expect(store.getRawEncryptedAccessToken("org-1")).not.toContain(
      "access-secret",
    );
    expect(await store.getInstallation("org-1")).toEqual(expected);
  });

  test("consumes OAuth state exactly once before expiry", () => {
    store.createOAuthState("hash", 2_000, 1_000);
    expect(store.consumeOAuthState("hash", 1_500)).toBeTrue();
    expect(store.consumeOAuthState("hash", 1_600)).toBeFalse();
    store.createOAuthState("expired", 2_000, 1_000);
    expect(store.consumeOAuthState("expired", 2_001)).toBeFalse();
  });

  test("deduplicates webhook delivery IDs", () => {
    const delivery = {
      id: "delivery-1",
      organizationId: "org-1",
      payloadHash: "hash",
      payload: { type: "AgentSessionEvent" },
    };
    expect(store.acceptDelivery(delivery)).toBeTrue();
    expect(store.acceptDelivery(delivery)).toBeFalse();
  });

  test("reclaims a delivery interrupted before durable processing", () => {
    const delivery = {
      id: "delivery-1",
      organizationId: "org-1",
      payloadHash: "hash",
      payload: { type: "AgentSessionEvent" },
    };
    expect(store.claimDelivery(delivery)).toBe("claimed");
    expect(store.claimDelivery(delivery)).toBe("duplicate");
    expect(store.recoverPendingDeliveries()).toBe(1);
    expect(store.claimDelivery(delivery)).toBe("claimed");
  });

  test("enforces one lease and permits takeover only after expiry", () => {
    store.createRun({
      sessionId: "session-1",
      organizationId: "org-1",
      issueId: "issue-1",
      now: 1_000,
    });
    expect(store.claimRun("session-1", "worker-a", 1_000, 1_000)).toBeTrue();
    expect(store.claimRun("session-1", "worker-b", 1_000, 1_500)).toBeFalse();
    expect(store.claimRun("session-1", "worker-b", 1_000, 2_001)).toBeTrue();
  });

  test("recovers active runs and leases after a process restart", () => {
    store.createRun({
      sessionId: "running-session",
      organizationId: "org-1",
      issueId: "issue-1",
      now: 1_000,
    });
    store.updateRun("running-session", {
      state: "running",
      workspacePath: "/workspace",
      ompSessionFile: "/workspace/session.jsonl",
    });
    expect(
      store.claimRun("running-session", "dead-process", 60_000, 1_000),
    ).toBeTrue();

    store.createRun({
      sessionId: "canceled-session",
      organizationId: "org-1",
      issueId: "issue-2",
      now: 1_000,
    });
    store.updateRun("canceled-session", { state: "running" });
    store.enqueueInput({
      id: "stop",
      sessionId: "canceled-session",
      kind: "stop",
      body: "",
      payload: {},
      createdAt: 1_100,
    });

    expect(store.recoverInterruptedRuns(2_000)).toBe(2);
    expect(store.getRun("running-session")).toMatchObject({
      state: "orphaned",
      nextAttemptAt: 2_000,
      leaseOwner: null,
      leaseExpiresAt: null,
    });
    expect(store.listRunnable(2_000).map((run) => run.sessionId)).toContain(
      "running-session",
    );
    expect(store.getRun("canceled-session")).toMatchObject({
      state: "stopping",
      desiredState: "canceled",
      leaseOwner: null,
      leaseExpiresAt: null,
    });
  });

  test("stop dominates later prompts and revocation cancels all live runs", async () => {
    await store.putInstallation(installation());
    store.createRun({
      sessionId: "session-1",
      organizationId: "org-1",
      issueId: "issue-1",
    });
    expect(
      store.enqueueInput({
        id: "stop",
        sessionId: "session-1",
        kind: "stop",
        body: "stop",
        payload: {},
      }),
    ).toBeTrue();
    expect(
      store.enqueueInput({
        id: "late",
        sessionId: "session-1",
        kind: "prompted",
        body: "keep going",
        payload: {},
      }),
    ).toBeFalse();
    expect(store.getRun("session-1")?.desiredState).toBe("canceled");
    store.revokeInstallation("org-1");
    expect((await store.getInstallation("org-1"))?.revokedAt).not.toBeNull();
  });

  test("updates permission snapshots without rewriting rotating tokens", async () => {
    await store.putInstallation({
      ...installation(),
      accessibleTeamIds: ["team-a"],
    });
    const encryptedAccessToken = store.getRawEncryptedAccessToken("org-1");

    expect(
      store.applyPermissionChange(
        "org-1",
        "app-user-1",
        ["team-b"],
        ["team-a"],
        false,
      ),
    ).toBeTrue();
    expect(store.getRawEncryptedAccessToken("org-1")).toBe(
      encryptedAccessToken,
    );
    expect((await store.getInstallation("org-1"))?.accessibleTeamIds).toEqual([
      "team-b",
    ]);
  });

  test("reserves each projection and terminal outcome once", () => {
    store.createRun({
      sessionId: "session-1",
      organizationId: "org-1",
      issueId: null,
    });
    const projection = {
      sourceKey: "event-1",
      sessionId: "session-1",
      activityType: "response",
      payloadHash: "hash",
      payload: { body: "done" },
    };
    expect(store.enqueueProjection(projection)).toBeTrue();
    expect(store.enqueueProjection(projection)).toBeFalse();
    expect(
      store.claimProjection("event-1", "test-owner", 60_000),
    ).not.toBeNull();
    store.completeProjection("event-1", "test-owner", "linear-activity-1");
    expect(store.projectionCount("session-1", "response")).toBe(1);
  });
  test("migrates legacy content deduplication without dropping projection history", async () => {
    store.close();
    const directory = await mkdtemp(join(tmpdir(), "linear-gateway-store-"));
    const path = join(directory, "gateway.sqlite");
    try {
      const legacy = new Database(path);
      legacy.exec(`
        CREATE TABLE activity_projection (
          source_key TEXT PRIMARY KEY,
          session_id TEXT NOT NULL,
          activity_type TEXT NOT NULL,
          payload_hash TEXT NOT NULL,
          linear_activity_id TEXT,
          status TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          UNIQUE(session_id, activity_type, payload_hash)
        );
      `);
      legacy.close();
      store = await GatewayStore.open(path, key);
      store.createRun({
        sessionId: "session",
        organizationId: "org",
        issueId: null,
      });
      const common = {
        sessionId: "session",
        activityType: "thought",
        payloadHash: "same-content",
        payload: { body: "Accepted" },
      };
      expect(
        store.enqueueProjection({ ...common, sourceKey: "turn-1" }),
      ).toBeTrue();
      expect(
        store.enqueueProjection({ ...common, sourceKey: "turn-2" }),
      ).toBeTrue();
    } finally {
      store.close();
      await rm(directory, { recursive: true, force: true });
      store = await GatewayStore.open(":memory:", key);
    }
  });
});
