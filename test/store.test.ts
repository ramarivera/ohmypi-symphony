import { afterEach, beforeEach, describe, expect, test } from "bun:test";
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
    };
    expect(store.reserveProjection(projection)).toBeTrue();
    expect(store.reserveProjection(projection)).toBeFalse();
    store.completeProjection("event-1", "linear-activity-1");
    expect(store.projectionCount("session-1", "response")).toBe(1);
  });
});
