import type * as LinearSdk from "@linear/sdk";
import { vi } from "vitest";

const sdkState = vi.hoisted(() => {
  type ActivityInput = {
    readonly content?: unknown;
    readonly signalMetadata?: unknown;
  };
  type UpdateInput = {
    readonly plan?: unknown;
    readonly externalUrls?: unknown;
  };
  type Pending = {
    readonly resolve: (value: unknown) => void;
    readonly reject: (error: unknown) => void;
  };

  const state: {
    readonly activities: ActivityInput[];
    readonly updates: UpdateInput[];
    readonly pending: Pending[];
    readonly waiters: Array<{
      readonly count: number;
      readonly resolve: () => void;
    }>;
    activityHandler: (input: ActivityInput) => Promise<unknown>;
  } = {
    activities: [],
    updates: [],
    pending: [],
    waiters: [],
    activityHandler: async () => ({
      success: true,
      agentActivityId: "activity-id",
    }),
  };

  class TestLinearClient {
    createAgentActivity(input: ActivityInput): Promise<unknown> {
      state.activities.push(input);
      for (const waiter of state.waiters.splice(0)) {
        if (state.activities.length >= waiter.count) waiter.resolve();
        else state.waiters.push(waiter);
      }
      return state.activityHandler(input);
    }

    updateAgentSession(
      _sessionId: string,
      input: UpdateInput,
    ): Promise<unknown> {
      state.updates.push(input);
      return Promise.resolve({ success: true });
    }
  }

  return {
    state,
    TestLinearClient,
    reset: () => {
      state.activities.length = 0;
      state.updates.length = 0;
      state.pending.length = 0;
      state.waiters.length = 0;
      state.activityHandler = async () => ({
        success: true,
        agentActivityId: "activity-id",
      });
    },
    waitForActivity: (count: number) => {
      if (state.activities.length >= count) return Promise.resolve();
      const { promise, resolve } = Promise.withResolvers<void>();
      state.waiters.push({ count, resolve });
      return promise;
    },
  };
});

vi.mock("@linear/sdk", async () => {
  const actual = await vi.importActual<typeof LinearSdk>("@linear/sdk");
  return { ...actual, LinearClient: sdkState.TestLinearClient };
});

import { it as effectIt } from "@effect/vitest";
import { Effect, Exit, Fiber, Layer, Option, Redacted, Schema } from "effect";
import { beforeEach, describe, expect, it } from "vitest";
import { LinearRateLimitError } from "../src/domain/errors.js";
import { AppUserId, OrganizationId, SessionId } from "../src/domain/ids.js";
import type { AgentRun, Installation } from "../src/domain/models.js";
import { GatewayConfig } from "../src/services/config.js";
import { LinearGateway } from "../src/services/linear-gateway.js";
import {
  InstallationRepo,
  RunRepo,
} from "../src/services/store/repositories.js";

const organizationId = Schema.decodeUnknownSync(OrganizationId)(
  "11111111-1111-4111-8111-111111111111",
);
const sessionId = Schema.decodeUnknownSync(SessionId)(
  "22222222-2222-4222-8222-222222222222",
);
const appUserId = Schema.decodeUnknownSync(AppUserId)(
  "33333333-3333-4333-8333-333333333333",
);
const run: AgentRun = {
  sessionId,
  organizationId,
  issueId: Option.none(),
  repositoryId: Option.none(),
  state: "running",
  desiredState: "running",
  ompSessionId: Option.none(),
  ompSessionFile: Option.none(),
  workspacePath: Option.none(),
  teamId: Option.none(),
  projectId: Option.none(),
  attempt: 0,
  leaseOwner: Option.none(),
  leaseExpiresAt: Option.none(),
  lastActivityAt: Option.none(),
  terminalReason: Option.none(),
  nextAttemptAt: Option.none(),
  createdAt: 0,
  updatedAt: 0,
};
const installation: Installation = {
  organizationId,
  appUserId,
  accessToken: "access-token",
  refreshToken: "refresh-token",
  expiresAt: Date.now() + 3_600_000,
  scopes: [],
  revokedAt: Option.none(),
  accessibleTeamIds: Option.none(),
  canAccessAllPublicTeams: Option.none(),
};
const unusedRepoMethod = (..._args: ReadonlyArray<unknown>) => Effect.never;

const gatewayDependencies = Layer.mergeAll(
  Layer.succeed(
    RunRepo,
    RunRepo.make({
      get: () => Effect.succeed(Option.some(run)),
      create: unusedRepoMethod,
      update: unusedRepoMethod,
      listRunnable: unusedRepoMethod,
      listCancellationPending: unusedRepoMethod,
      claimLease: unusedRepoMethod,
      renewLease: unusedRepoMethod,
      releaseLease: unusedRepoMethod,
      recoverInterruptedRuns: unusedRepoMethod,
    }),
  ),
  Layer.succeed(
    InstallationRepo,
    InstallationRepo.make({
      get: () => Effect.succeed(Option.some(installation)),
      put: unusedRepoMethod,
      revoke: unusedRepoMethod,
      applyPermissionChange: unusedRepoMethod,
      getRawEncryptedAccessToken: unusedRepoMethod,
      createOAuthState: unusedRepoMethod,
      consumeOAuthState: unusedRepoMethod,
    }),
  ),
  Layer.succeed(
    GatewayConfig,
    GatewayConfig.make({
      linearClientId: "client-id",
      linearClientSecret: Redacted.make("client-secret"),
      linearWebhookSecret: Redacted.make("webhook-secret"),
      tokenEncryptionKey: Redacted.make("token-key"),
      publicUrl: new URL("https://localhost/"),
      logLevel: "info",
      logFile: Option.none(),
      databasePath: ":memory:",
      nixBinaryPath: "nix",
      nixpkgsFlakeRef:
        "github:NixOS/nixpkgs/0123456789abcdef0123456789abcdef01234567",
      nixRootsDir: "/tmp/nix-roots",
      nixGcMaxBytes: 1_000_000,
      workspaceRoot: "/tmp/workspaces",
      ompCliPath: "omp",
      port: 3000,
      leaseDurationMs: 60_000,
      reconcilerIntervalMs: 1_000,
      webhookReplayWindowMs: 60_000,
    }),
  ),
);

const gatewayLayer = LinearGateway.DefaultWithoutDependencies.pipe(
  Layer.provide(gatewayDependencies),
);

const getGateway = () =>
  Effect.runPromise(
    Effect.gen(function* () {
      return yield* LinearGateway;
    }).pipe(Effect.provide(gatewayLayer)),
  );

const activityBody = (content: unknown): string | undefined => {
  if (
    typeof content !== "object" ||
    content === null ||
    Array.isArray(content) ||
    !("body" in content)
  ) {
    return undefined;
  }
  return typeof content.body === "string" ? content.body : undefined;
};

beforeEach(() => sdkState.reset());

describe("LinearGateway parity", () => {
  it("forwards persisted activity content and signal metadata verbatim", async () => {
    const gateway = await getGateway();
    const content = {
      type: "thought",
      body: "secret-like persisted content",
      signal: "select",
      signalMetadata: {
        token: "persisted-verbatim",
        nested: { value: "kept" },
      },
    };

    await Effect.runPromise(
      gateway
        .createActivity({ sessionId, content })
        .pipe(Effect.provide(gatewayLayer)),
    );

    expect(sdkState.state.activities[0]).toMatchObject({
      content: { type: "thought", body: content.body },
      signalMetadata: content.signalMetadata,
    });
  });

  effectIt.effect.prop(
    "creates every generated thought activity and returns its SDK id",
    { body: Schema.String },
    ({ body }) =>
      Effect.gen(function* () {
        sdkState.reset();
        const gateway = yield* LinearGateway;
        const activityId = yield* gateway.createActivity({
          sessionId,
          content: { type: "thought", body },
        });
        expect(activityId).toBe("activity-id");
        expect(sdkState.state.activities.at(-1)?.content).toEqual({
          type: "thought",
          body,
        });
      }).pipe(Effect.provide(gatewayLayer)),
    { fastCheck: { numRuns: 20 } },
  );

  it("forwards persisted plan and external URL fields verbatim", async () => {
    const gateway = await getGateway();
    const plan = [{ content: "plan secret", status: "completed" }];
    const externalUrls = [
      { label: "private label", url: "https://example.test/private" },
    ];

    await Effect.runPromise(
      gateway
        .updateSession({ sessionId, plan, externalUrls })
        .pipe(Effect.provide(gatewayLayer)),
    );

    expect(sdkState.state.updates).toEqual([{ plan, externalUrls }]);
  });

  it("executes three same-organization requests in order", async () => {
    const gateway = await getGateway();
    sdkState.state.activityHandler = (_input) =>
      (() => {
        const { promise, resolve, reject } = Promise.withResolvers<unknown>();
        sdkState.state.pending.push({ resolve, reject });
        return promise;
      })();

    const first = Effect.runPromise(
      gateway
        .createActivity({
          sessionId,
          content: { type: "thought", body: "one" },
        })
        .pipe(Effect.provide(gatewayLayer)),
    );
    await sdkState.waitForActivity(1);
    const second = Effect.runPromise(
      gateway
        .createActivity({
          sessionId,
          content: { type: "thought", body: "two" },
        })
        .pipe(Effect.provide(gatewayLayer)),
    );
    const third = Effect.runPromise(
      gateway
        .createActivity({
          sessionId,
          content: { type: "thought", body: "three" },
        })
        .pipe(Effect.provide(gatewayLayer)),
    );

    expect(sdkState.state.activities).toHaveLength(1);
    sdkState.state.pending
      .shift()
      ?.resolve({ success: true, agentActivityId: "one" });
    await sdkState.waitForActivity(2);
    expect(
      sdkState.state.activities.map((activity) =>
        activityBody(activity.content),
      ),
    ).toEqual(["one", "two"]);
    sdkState.state.pending
      .shift()
      ?.resolve({ success: true, agentActivityId: "two" });
    await sdkState.waitForActivity(3);
    expect(
      sdkState.state.activities.map((activity) =>
        activityBody(activity.content),
      ),
    ).toEqual(["one", "two", "three"]);
    sdkState.state.pending
      .shift()
      ?.resolve({ success: true, agentActivityId: "three" });
    await expect(Promise.all([first, second, third])).resolves.toEqual([
      "one",
      "two",
      "three",
    ]);
  });

  it("does not let an interrupted waiter bypass the active organization request", async () => {
    sdkState.state.activityHandler = () => {
      const { promise, resolve, reject } = Promise.withResolvers<unknown>();
      sdkState.state.pending.push({ resolve, reject });
      return promise;
    };

    await Effect.runPromise(
      Effect.gen(function* () {
        const gateway = yield* LinearGateway;
        const first = yield* gateway
          .createActivity({
            sessionId,
            content: { type: "thought", body: "one" },
          })
          .pipe(Effect.fork);
        yield* Effect.promise(() => sdkState.waitForActivity(1));

        const interrupted = yield* gateway
          .createActivity({
            sessionId,
            content: { type: "thought", body: "two" },
          })
          .pipe(Effect.fork);
        const third = yield* gateway
          .createActivity({
            sessionId,
            content: { type: "thought", body: "three" },
          })
          .pipe(Effect.fork);
        yield* Fiber.interruptFork(interrupted);

        expect(sdkState.state.activities).toHaveLength(1);
        sdkState.state.pending.shift()?.resolve({
          success: true,
          agentActivityId: "one",
        });
        yield* Effect.promise(() => sdkState.waitForActivity(2));
        expect(
          sdkState.state.activities.map((activity) =>
            activityBody(activity.content),
          ),
        ).toEqual(["one", "three"]);
        sdkState.state.pending.shift()?.resolve({
          success: true,
          agentActivityId: "three",
        });
        yield* Fiber.join(first);
        yield* Fiber.join(third);
      }).pipe(Effect.provide(gatewayLayer)),
    );
  });

  it("retries a 429 once and surfaces the typed rate-limit failure", async () => {
    const gateway = await getGateway();
    let attempts = 0;
    sdkState.state.activityHandler = async () => {
      attempts += 1;
      throw { status: 429, retryAfter: 0 };
    };

    const exit = await Effect.runPromiseExit(
      gateway
        .createActivity({
          sessionId,
          content: { type: "thought", body: "rate limited" },
        })
        .pipe(Effect.provide(gatewayLayer)),
    );

    expect(attempts).toBe(2);
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      expect(exit.cause._tag).toBe("Fail");
      if (exit.cause._tag === "Fail")
        expect(exit.cause.error).toBeInstanceOf(LinearRateLimitError);
    }
  });
});
