import type {
  AgentRunRecord,
  RepositoryDefinition,
  RpcEvent,
  RpcWorker,
} from "./domain";
import type { ActivityProjector } from "./projector";
import type { GatewayStore } from "./store";

export type RepositoryResolution =
  | { kind: "match"; repository: RepositoryDefinition }
  | { kind: "none" }
  | { kind: "ambiguous"; repositories: readonly RepositoryDefinition[] };

export interface WorkspacePort {
  resolve(context: {
    teamId: string | null;
    projectId: string | null;
    repositoryId: string | null;
  }): RepositoryResolution;
  materialize(
    sessionId: string,
    repository: RepositoryDefinition,
  ): Promise<string>;
}

export type WorkerFactory = (input: {
  run: AgentRunRecord;
  cwd: string;
}) => RpcWorker;

interface InputContext {
  teamId: string | null;
  projectId: string | null;
  repositoryId: string | null;
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function nullableString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function inputContext(payload: unknown): InputContext {
  if (!record(payload))
    return { teamId: null, projectId: null, repositoryId: null };
  const session = record(payload.agentSession) ? payload.agentSession : null;
  const issue = session && record(session.issue) ? session.issue : null;
  const project = issue && record(issue.project) ? issue.project : null;
  return {
    teamId: issue ? nullableString(issue.teamId) : null,
    projectId: issue
      ? (nullableString(issue.projectId) ??
        (project ? nullableString(project.id) : null))
      : null,
    repositoryId: nullableString(payload.repositoryId),
  };
}

export class SessionAuthority {
  readonly #store: GatewayStore;
  readonly #projector: ActivityProjector;
  readonly #workspaces: WorkspacePort;
  readonly #workerFactory: WorkerFactory;
  readonly #owner: string;
  readonly #leaseDurationMs: number;
  readonly #workers = new Map<string, RpcWorker>();
  readonly #eventSequence = new Map<string, number>();

  constructor(input: {
    store: GatewayStore;
    projector: ActivityProjector;
    workspaces: WorkspacePort;
    workerFactory: WorkerFactory;
    owner: string;
    leaseDurationMs: number;
  }) {
    this.#store = input.store;
    this.#projector = input.projector;
    this.#workspaces = input.workspaces;
    this.#workerFactory = input.workerFactory;
    this.#owner = input.owner;
    this.#leaseDurationMs = input.leaseDurationMs;
  }

  activeWorkerCount(): number {
    return this.#workers.size;
  }

  async processRunnable(): Promise<void> {
    for (const run of this.#store.listCancellationPending())
      await this.#cancel(run);
    for (const run of this.#store.listRunnable())
      await this.processSession(run.sessionId);
  }

  async processSession(sessionId: string): Promise<void> {
    const initial = this.#store.getRun(sessionId);
    if (initial?.desiredState === "canceled") {
      await this.#cancel(initial);
      return;
    }
    if (!this.#store.claimRun(sessionId, this.#owner, this.#leaseDurationMs))
      return;
    const run = this.#store.getRun(sessionId);
    if (!run) return;
    try {
      if (run.desiredState === "canceled") {
        await this.#cancel(run);
        return;
      }
      const installation = await this.#store.getInstallation(
        run.organizationId,
      );
      if (!installation || installation.revokedAt !== null) {
        this.#store.updateRun(sessionId, {
          state: "failed",
          terminalReason: "Linear installation is unavailable",
        });
        return;
      }
      if (
        run.teamId !== null &&
        installation.accessibleTeamIds !== null &&
        installation.canAccessAllPublicTeams !== true &&
        !installation.accessibleTeamIds.includes(run.teamId)
      ) {
        this.#store.updateRun(sessionId, {
          state: "canceled",
          terminalReason: "Linear team access was removed",
        });
        return;
      }
      const inputs = this.#store.pendingInputs(sessionId);
      if (inputs.length === 0) return;
      let worker = this.#workers.get(sessionId);
      for (const input of inputs) {
        const latest = this.#store.getRun(sessionId);
        if (
          !latest ||
          latest.desiredState === "canceled" ||
          input.kind === "stop"
        ) {
          await this.#cancel(latest ?? run);
          this.#store.markInputProcessed(input.id);
          break;
        }
        if (!worker) {
          const context = inputContext(input.payload);
          const resolution = this.#workspaces.resolve(context);
          if (resolution.kind === "none") {
            await this.#projector.elicitation(
              sessionId,
              `repo:none:${input.id}`,
              "No repository is configured for this Linear issue.",
            );
            this.#store.updateRun(sessionId, { state: "waiting" });
            return;
          }
          if (resolution.kind === "ambiguous") {
            await this.#projector.elicitation(
              sessionId,
              `repo:ambiguous:${input.id}`,
              "Select the repository for this issue.",
              resolution.repositories.map((repository) => repository.id),
            );
            this.#store.updateRun(sessionId, { state: "waiting" });
            return;
          }
          const workspacePath = await this.#workspaces.materialize(
            sessionId,
            resolution.repository,
          );
          this.#store.setWorkspace({
            sessionId,
            path: workspacePath,
            repositoryId: resolution.repository.id,
            url: resolution.repository.url,
            ref: resolution.repository.ref,
            state: "ready",
          });
          this.#store.updateRun(sessionId, {
            state: "starting",
            repositoryId: resolution.repository.id,
            workspacePath,
            incrementAttempt: true,
          });
          worker = this.#workerFactory({
            run: this.#store.getRun(sessionId) ?? run,
            cwd: workspacePath,
          });
          this.#workers.set(sessionId, worker);
          worker.onEvent((event) => {
            void this.#handleEvent(sessionId, event);
          });
          await worker.start();
          this.#store.updateRun(sessionId, { state: "running" });
          await this.#projector.thought(
            sessionId,
            `accepted:${input.id}`,
            "Request accepted; preparing the OhMyPi worker.",
          );
          await worker.prompt(input.body);
        } else if (input.kind === "prompted") {
          await worker.followUp(input.body);
          this.#store.updateRun(sessionId, { state: "running" });
        }
        this.#store.markInputProcessed(input.id);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const current = this.#store.getRun(sessionId);
      const attempt = current?.attempt ?? 0;
      const delay = Math.min(300_000, 10_000 * 2 ** Math.min(attempt, 5));
      this.#store.updateRun(sessionId, {
        state: "orphaned",
        terminalReason: message,
        nextAttemptAt: Date.now() + delay + Math.floor(Math.random() * 1_000),
      });
      const worker = this.#workers.get(sessionId);
      if (worker) await worker.stop().catch(() => undefined);
      this.#workers.delete(sessionId);
    } finally {
      if (!this.#workers.has(sessionId))
        this.#store.releaseLease(sessionId, this.#owner);
    }
  }

  async shutdown(): Promise<void> {
    const workers = [...this.#workers.entries()];
    this.#workers.clear();
    await Promise.all(
      workers.map(async ([sessionId, worker]) => {
        await worker.stop().catch(() => undefined);
        this.#store.releaseLease(sessionId, this.#owner);
      }),
    );
  }

  async #cancel(run: AgentRunRecord): Promise<void> {
    const worker = this.#workers.get(run.sessionId);
    if (worker) {
      await worker.abort().catch(() => undefined);
      await worker.stop().catch(() => undefined);
      this.#workers.delete(run.sessionId);
    }
    const current = this.#store.getRun(run.sessionId);
    if (current && current.state !== "canceled") {
      this.#store.updateRun(run.sessionId, {
        state: "canceled",
        terminalReason: "Stopped by Linear user",
      });
      await this.#projector.terminal(
        run.sessionId,
        `stop:${run.sessionId}`,
        "response",
        "Stopped as requested.",
      );
    }
    this.#store.releaseLease(run.sessionId, this.#owner);
  }

  async #handleEvent(sessionId: string, event: RpcEvent): Promise<void> {
    const sequence = (this.#eventSequence.get(sessionId) ?? 0) + 1;
    this.#eventSequence.set(sessionId, sequence);
    await this.#projector.projectRpcEvent(sessionId, sequence, event);
    if (event.type === "agent_end") {
      const worker = this.#workers.get(sessionId);
      if (!worker) return;
      const state: Record<string, unknown> = await worker
        .getState()
        .catch(() => ({}));
      const session =
        typeof state.sessionId === "string"
          ? state.sessionId
          : worker.sessionId;
      const sessionFile =
        typeof state.sessionFile === "string"
          ? state.sessionFile
          : worker.sessionFile;
      this.#store.updateRun(sessionId, {
        state: "succeeded",
        ompSessionId: session,
        ompSessionFile: sessionFile,
      });
      await worker.stop();
      this.#workers.delete(sessionId);
      this.#store.releaseLease(sessionId, this.#owner);
    }
  }
}
