import { AgentActivitySignal, LinearClient } from "@linear/sdk";
import { Schema } from "effect";
import { describe, expect, it } from "vitest";
import { AgentSessionEvent } from "../src/domain/models.js";

// This test runs offline against the installed @linear/sdk 89.0.0 surface.
// It is an early-warning tripwire for Developer Preview drift; it does not
// exercise a live Linear workspace.

type Assert<T extends true> = T;

// 1. AgentActivitySignal covers exactly the four values the gateway maps.
//    (One-way assignability: string literals never assign TO an enum type;
//    the runtime test below pins exact set equality.)
type _SignalCovers = Assert<
  AgentActivitySignal extends "auth" | "continue" | "select" | "stop"
    ? true
    : false
>;

// 2. AgentSessionUpdateInput accepts the append-only plan/external-URL
//    fields the gateway uses.
type UpdateInput = Parameters<LinearClient["updateAgentSession"]>[1];
type _UpdateInputShape =
  | Assert<"addedExternalUrls" extends keyof UpdateInput ? true : false>
  | Assert<"removedExternalUrls" extends keyof UpdateInput ? true : false>
  | Assert<"plan" extends keyof UpdateInput ? true : false>;

// 3. The SDK webhook map exposes AgentSessionEvent with the fields the
//    gateway reads. The wire-invariant that burned production (embedded
//    agentSession arriving WITHOUT a `type` field, despite the SDK type
//    claiming one) is pinned by the runtime decode below — the gateway's own
//    schema is the wire validator, so the tolerance lives there.
import type { LinearWebhookEventTypeMap } from "@linear/sdk/webhooks";

type WebhookPayload = LinearWebhookEventTypeMap["AgentSessionEvent"];
type _WebhookPayloadShape =
  | Assert<"agentSession" extends keyof WebhookPayload ? true : false>
  | Assert<"promptContext" extends keyof WebhookPayload ? true : false>
  | Assert<"guidance" extends keyof WebhookPayload ? true : false>
  | Assert<"previousComments" extends keyof WebhookPayload ? true : false>
  | Assert<"agentActivity" extends keyof WebhookPayload ? true : false>;

// The type assertions above are checked by TypeScript; the runtime assertions
// below are checked by vitest.

describe("Linear SDK 89.0.0 contract", () => {
  it("AgentActivitySignal has exactly auth/continue/select/stop", () => {
    const values = Object.values(AgentActivitySignal).map(String).sort();
    expect(values).toEqual(["auth", "continue", "select", "stop"]);
  });

  it("LinearClient prototype exposes the methods the gateway calls", () => {
    const required = [
      "createAgentActivity",
      "updateAgentSession",
      "createComment",
      "agentSessionCreateOnIssue",
      "updateIssue",
      "issueRepositorySuggestions",
    ];
    for (const name of required) {
      const value: unknown = Reflect.get(LinearClient.prototype, name);
      expect(typeof value, `method ${name} is a function`).toBe("function");
    }
  });

  // Production invariant (2026-07-30 incident): Linear's wire payload omits
  // `type` on the embedded agentSession even though the SDK type claims it.
  // The gateway schema MUST keep tolerating that.
  it("gateway webhook schema decodes an embedded session without `type`", () => {
    const decoded = Schema.decodeUnknownSync(AgentSessionEvent)({
      type: "AgentSessionEvent",
      action: "created",
      organizationId: "org",
      appUserId: "app-user",
      oauthClientId: "client",
      webhookId: "webhook-config",
      webhookTimestamp: 1_700_000_000_000,
      promptContext: "Implement the issue",
      agentSession: {
        id: "session-1",
        appUserId: "app-user",
        organizationId: "org",
        status: "pending",
        createdAt: "2024-01-01T00:00:00.000Z",
        updatedAt: "2024-01-01T00:00:00.000Z",
      },
    });
    expect(decoded.agentSession.id).toBe("session-1");
  });
});
