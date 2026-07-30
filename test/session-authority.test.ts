import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import type {
  AgentRunRecord,
  LinearGatewayPort,
  RpcEvent,
  RpcWorker,
} from "../src/domain";
import { ActivityProjector } from "../src/projector";
import {
  type RepositoryResolution,
  SessionAuthority,
  type WorkspacePort,
} from "../src/session-authority";
import { GatewayStore } from "../src/store";

class FakeLinear implements LinearGatewayPort {
  readonly activities: Array<
    Parameters<LinearGatewayPort["createActivity"]>[0]
  > = [];
  readonly updates: Array<Parameters<LinearGatewayPort["updateSession"]>[0]> =
    [];
  readonly terminal = Promise.withResolvers<void>();
  async createActivity(
    input: Parameters<LinearGatewayPort["createActivity"]>[0],
  ): Promise<string> {
    this.activities.push(input);
    if (input.content.type === "response" || input.content.type === "error")
      this.terminal.resolve();
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

class FakeWorker implements RpcWorker {
  sessionId: string | null = "omp-session";
  sessionFile: string | null = "/session.jsonl";
  isStreaming = false;
  readonly prompts: string[] = [];
  readonly followUps: string[] = [];
  aborted = false;
  readonly #listeners = new Set<(event: RpcEvent) => void>();
  constructor(readonly localOnly = false) {}
  async start(): Promise<void> {}
  async prompt(message: string): Promise<boolean> {
    if (this.localOnly) {
      this.prompts.push(message);
      this.emit({
        type: "prompt_result",
        id: "local-prompt",
        agentInvoked: false,
      });
      return true;
    }
    this.prompts.push(message);
    this.isStreaming = true;
    this.emit({
      type: "message_end",
      message: { role: "assistant", content: "completed" },
    });
    this.isStreaming = false;
    this.emit({ type: "agent_end" });
    return true;
  }
  async steer(message: string): Promise<void> {
    this.followUps.push(message);
  }
  async followUp(message: string): Promise<void> {
    this.followUps.push(message);
  }
  async abort(): Promise<void> {
    this.aborted = true;
  }
  async getState(): Promise<Record<string, unknown>> {
    return {
      sessionId: this.sessionId,
      sessionFile: this.sessionFile,
      todoPhases: [
        {
          id: "phase",
          name: "Work",
          tasks: [{ id: "task", content: "Implement", status: "in_progress" }],
        },
      ],
    };
  }
  async respondToUi(): Promise<void> {}
  async stop(): Promise<void> {}
  onEvent(listener: (event: RpcEvent) => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }
  emit(event: RpcEvent): void {
    for (const listener of this.#listeners) listener(event);
  }
}

class FakeWorkspace implements WorkspacePort {
  resolution: RepositoryResolution = {
    kind: "match",
    repository: {
      id: "repo",
      organizationId: "org",
      url: "fixture",
      ref: "main",
      teamIds: ["team"],
      projectIds: [],
      labels: [],
      isDefault: false,
      createdAt: 0,
      updatedAt: 0,
    },
  };
  resolve(): RepositoryResolution {
    return this.resolution;
  }
  async materialize(): Promise<string> {
    return "/safe/workspace";
  }
}

let store: GatewayStore;
let linear: FakeLinear;
let workspace: FakeWorkspace;
let workers: FakeWorker[];
let workerRuns: AgentRunRecord[];
let nextWorkerLocalOnly: boolean;
let authority: SessionAuthority;

beforeEach(async () => {
  store = await GatewayStore.open(":memory:", new Uint8Array(32).fill(3));
  await store.putInstallation({
    organizationId: "org",
    appUserId: "app-user",
    accessToken: "access",
    refreshToken: "refresh",
    expiresAt: Date.now() + 60_000,
    scopes: ["read", "write"],
    revokedAt: null,
    accessibleTeamIds: ["team"],
    canAccessAllPublicTeams: false,
  });
  linear = new FakeLinear();
  workspace = new FakeWorkspace();
  workers = [];
  workerRuns = [];
  nextWorkerLocalOnly = false;
  authority = new SessionAuthority({
    store,
    projector: new ActivityProjector(store, linear),
    workspaces: workspace,
    workerFactory: ({ run }) => {
      workerRuns.push(run);
      const worker = new FakeWorker(nextWorkerLocalOnly);
      nextWorkerLocalOnly = false;
      workers.push(worker);
      return worker;
    },
    owner: "test-owner",
    leaseDurationMs: 60_000,
  });
});
afterEach(async () => {
  await authority.shutdown();
  store.close();
});

function created(sessionId = "session") {
  store.createRun({
    sessionId,
    organizationId: "org",
    issueId: "issue",
    teamId: "team",
  });
  store.enqueueInput({
    id: `${sessionId}-created`,
    sessionId,
    kind: "created",
    body: "Implement the issue",
    payload: { agentSession: { issue: { teamId: "team" } } },
  });
}

describe("SessionAuthority", () => {
  test("runs one worker and projects one terminal response", async () => {
    created();
    await authority.processSession("session");
    await linear.terminal.promise;
    expect(workers).toHaveLength(1);
    expect(workers[0]?.prompts).toEqual(["Implement the issue"]);
    expect(store.getRun("session")?.state).toBe("succeeded");
    expect(store.projectionCount("session", "response")).toBe(1);
    expect(
      linear.updates.some(
        (update) =>
          JSON.stringify(update.plan) ===
          JSON.stringify([{ content: "Implement", status: "inProgress" }]),
      ),
    ).toBeTrue();
  });

  test("settles a deferred local-only prompt without waiting for agent_end", async () => {
    nextWorkerLocalOnly = true;
    created();

    await authority.processSession("session");
    await authority.processRunnable();

    expect(workers[0]?.prompts).toEqual(["Implement the issue"]);
    expect(store.getRun("session")).toMatchObject({
      state: "waiting",
      ompSessionFile: "/session.jsonl",
    });
    expect(
      linear.activities.some(
        (activity) =>
          activity.content.type === "thought" &&
          activity.content.body?.includes("completed without starting"),
      ),
    ).toBeTrue();
  });
  test("requeues a prompt after completion and resumes the saved OMP session", async () => {
    created();
    await authority.processSession("session");
    await linear.terminal.promise;
    await Bun.sleep(0);
    await authority.processRunnable();
    expect(authority.activeWorkerCount()).toBe(0);
    expect(store.getRun("session")?.ompSessionFile).toBe("/session.jsonl");

    expect(
      store.enqueueInput({
        id: "follow-up",
        sessionId: "session",
        kind: "prompted",
        body: "Now add tests",
        payload: { agentSession: { issue: { teamId: "team" } } },
      }),
    ).toBeTrue();
    expect(store.getRun("session")?.state).toBe("queued");
    expect(store.listSessionsWithPendingInputs()).toContain("session");

    await authority.processSession("session");
    expect(workers).toHaveLength(2);
    expect(workers[1]?.prompts).toEqual(["Now add tests"]);
    expect(workerRuns[1]?.ompSessionFile).toBe("/session.jsonl");
  });

  test("resumes an orphaned worker after gateway restart", async () => {
    created();
    store.markInputProcessed("session-created");
    store.updateRun("session", {
      state: "running",
      repositoryId: "repo",
      workspacePath: "/safe/workspace",
      ompSessionId: "omp-session",
      ompSessionFile: "/session.jsonl",
      incrementAttempt: true,
    });
    expect(store.claimRun("session", "dead-process", 60_000, 1_000)).toBeTrue();
    store.recoverInterruptedRuns(2_000);

    await authority.processSession("session");

    expect(workerRuns).toHaveLength(1);
    expect(workerRuns[0]?.ompSessionFile).toBe("/session.jsonl");
    expect(workers[0]?.followUps).toEqual([
      "Continue the interrupted Linear task from the saved session state.",
    ]);
    expect(store.getRun("session")?.state).toBe("running");
  });

  test("aborts an active worker before confirming a stop", async () => {
    nextWorkerLocalOnly = true;
    created();
    await authority.processSession("session");
    await authority.processRunnable();
    expect(authority.activeWorkerCount()).toBe(1);

    store.enqueueInput({
      id: "stop",
      sessionId: "session",
      kind: "stop",
      body: "",
      payload: {},
    });
    await authority.processSession("session");
    await linear.terminal.promise;

    expect(workers[0]?.aborted).toBeTrue();
    expect(authority.activeWorkerCount()).toBe(0);
    expect(store.getRun("session")?.state).toBe("canceled");
    expect(linear.activities.at(-1)).toMatchObject({
      content: { type: "response", body: "Stopped as requested." },
    });
  });

  test("stop dominates queued work without creating a worker", async () => {
    created();
    store.enqueueInput({
      id: "stop",
      sessionId: "session",
      kind: "stop",
      body: "stop",
      payload: {},
    });
    await authority.processSession("session");
    await linear.terminal.promise;
    expect(workers).toHaveLength(0);
    expect(store.getRun("session")?.state).toBe("canceled");
  });

  test("repository ambiguity elicits a selection before workspace creation", async () => {
    created();
    workspace.resolution = {
      kind: "ambiguous",
      repositories: [
        {
          id: "one",
          organizationId: "org",
          url: "one",
          ref: "main",
          teamIds: [],
          projectIds: [],
          labels: [],
          isDefault: false,
          createdAt: 0,
          updatedAt: 0,
        },
        {
          id: "two",
          organizationId: "org",
          url: "two",
          ref: "main",
          teamIds: [],
          projectIds: [],
          labels: [],
          isDefault: false,
          createdAt: 0,
          updatedAt: 0,
        },
      ],
    };
    await authority.processSession("session");
    expect(workers).toHaveLength(0);
    expect(store.getRun("session")?.state).toBe("waiting");
    expect(linear.activities.at(-1)).toMatchObject({
      content: { type: "elicitation" },
      signal: "select",
    });
  });
});
