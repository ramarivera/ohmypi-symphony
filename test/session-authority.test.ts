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
  async start(): Promise<void> {}
  async prompt(message: string): Promise<boolean> {
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
      url: "fixture",
      ref: "main",
      teamIds: ["team"],
      projectIds: [],
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
  authority = new SessionAuthority({
    store,
    projector: new ActivityProjector(store, linear),
    workspaces: workspace,
    workerFactory: ({ run }) => {
      workerRuns.push(run);
      const worker = new FakeWorker();
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
          JSON.stringify({
            items: [{ content: "Implement", status: "inProgress" }],
          }),
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
        { id: "one", url: "one", ref: "main", teamIds: [], projectIds: [] },
        { id: "two", url: "two", ref: "main", teamIds: [], projectIds: [] },
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
