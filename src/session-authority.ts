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
function planItems(value: unknown): Array<{
  content: string;
  status: "pending" | "inProgress" | "completed" | "canceled";
}> {
  if (!Array.isArray(value)) return [];
  const candidates = value.flatMap((phase) =>
    record(phase) && Array.isArray(phase.tasks) ? phase.tasks : [phase],
  );
  const items: Array<{
    content: string;
    status: "pending" | "inProgress" | "completed" | "canceled";
  }> = [];
  for (const candidate of candidates) {
    if (!record(candidate) || typeof candidate.content !== "string") continue;
    const status =
      candidate.status === "in_progress" || candidate.status === "inProgress"
        ? "inProgress"
        : candidate.status === "completed"
          ? "completed"
          : candidate.status === "canceled" || candidate.status === "cancelled"
            ? "canceled"
            : "pending";
    items.push({ content: candidate.content, status });
  }
  return items;
}

function failureCorrelationId(
  sessionId: string,
  attempt: number,
  message: string,
): string {
  return new Bun.CryptoHasher("sha256")
    .update(`${sessionId}\0${attempt}\0${message}`)
    .digest("hex")
    .slice(0, 12);
}

export class SessionAuthority {
  readonly #store: GatewayStore;
  readonly #projector: ActivityProjector;
  readonly #workspaces: WorkspacePort;
  readonly #workerFactory: WorkerFactory;
  readonly #owner: string;
  readonly #leaseDurationMs: number;
  readonly #maxAttempts: number;
  readonly #workers = new Map<string, RpcWorker>();
  readonly #eventSequence = new Map<string, number>();
  readonly #runUrlForSession: ((sessionId: string) => string) | null;
  readonly #eventQueues = new Map<string, Promise<void>>();
  readonly #pendingUi = new Map<string, { id: string; method: string }>();

  constructor(input: {
    store: GatewayStore;
    projector: ActivityProjector;
    workspaces: WorkspacePort;
    workerFactory: WorkerFactory;
    owner: string;
    leaseDurationMs: number;
    maxAttempts?: number;
    runUrlForSession?: (sessionId: string) => string;
  }) {
    this.#store = input.store;
    this.#projector = input.projector;
    this.#workspaces = input.workspaces;
    this.#workerFactory = input.workerFactory;
    this.#owner = input.owner;
    this.#leaseDurationMs = input.leaseDurationMs;
    this.#maxAttempts = input.maxAttempts ?? 5;
    this.#runUrlForSession = input.runUrlForSession ?? null;
  }

  activeWorkerCount(): number {
    return this.#workers.size;
  }
  async processRunnable(): Promise<void> {
    await Promise.allSettled(this.#eventQueues.values());
    await this.#projector.flushPending();
    for (const [sessionId, worker] of this.#workers) {
      const run = this.#store.getRun(sessionId);
      if (!run || run.desiredState === "canceled") continue;
      if (
        !this.#store.renewLease(sessionId, this.#owner, this.#leaseDurationMs)
      ) {
        await worker.abort().catch(() => undefined);
        await worker.stop().catch(() => undefined);
        this.#workers.delete(sessionId);
      }
    }
    for (const run of this.#store.listCancellationPending())
      await this.#cancel(run);
    const sessions = new Set(this.#store.listSessionsWithPendingInputs());
    for (const run of this.#store.listRunnable()) sessions.add(run.sessionId);
    for (const sessionId of sessions) await this.processSession(sessionId);
    await this.#projector.flushPending();
  }

  async processSession(sessionId: string): Promise<void> {
    const initial = this.#store.getRun(sessionId);
    if (initial?.desiredState === "canceled") {
      await this.#cancel(initial);
      return;
    }
    const localWorker = this.#workers.get(sessionId);
    if (localWorker) {
      if (
        !this.#store.renewLease(sessionId, this.#owner, this.#leaseDurationMs)
      )
        return;
    } else if (
      !this.#store.claimRun(sessionId, this.#owner, this.#leaseDurationMs)
    ) {
      return;
    }
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
        await this.#projector.terminal(
          sessionId,
          `installation-unavailable:${run.organizationId}`,
          "error",
          "The Linear installation is unavailable. Reinstall or reauthorize the app, then try again.",
        );
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
        await this.#projector.terminal(
          sessionId,
          `team-access-removed:${run.teamId}`,
          "response",
          "Stopped because this Linear installation no longer has access to the issue's team.",
        );
        return;
      }

      const inputs = this.#store.pendingInputs(sessionId);
      let worker = this.#workers.get(sessionId);
      if (inputs.length === 0) {
        if (!worker && run.state === "orphaned" && run.workspacePath !== null) {
          this.#store.updateRun(sessionId, {
            state: "starting",
            incrementAttempt: true,
            nextAttemptAt: null,
          });
          const resumed = this.#store.getRun(sessionId) ?? run;
          worker = await this.#startWorker(resumed, run.workspacePath);
          await this.#projector.thought(
            sessionId,
            `retry:${resumed.attempt}`,
            `Retrying the interrupted OhMyPi run (attempt ${resumed.attempt}).`,
          );
          if (run.ompSessionFile !== null) {
            await worker.followUp(
              "Continue the interrupted Linear task from the saved session state.",
            );
          } else {
            const latestInput = this.#store.latestActionableInput(sessionId);
            if (!latestInput)
              throw new Error("Interrupted run has no input to resume");
            const agentInvoked = await worker.prompt(latestInput.body);
            if (!agentInvoked)
              await this.#finishLocalCommand(
                sessionId,
                worker,
                `retry:${resumed.attempt}`,
              );
          }
        }
        return;
      }

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
          if (this.#runUrlForSession) {
            await this.#projector.externalUrls(
              sessionId,
              `run-url:${sessionId}`,
              [{ label: "OhMyPi run", url: this.#runUrlForSession(sessionId) }],
            );
          }
          await this.#projector.thought(
            sessionId,
            `accepted:${input.id}`,
            "Request accepted; preparing the OhMyPi worker.",
          );
          const baseContext = inputContext(input.payload);
          const context =
            input.kind === "prompted" &&
            latest.state === "waiting" &&
            baseContext.repositoryId === null
              ? { ...baseContext, repositoryId: input.body.trim() }
              : baseContext;
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
          worker = await this.#startWorker(
            this.#store.getRun(sessionId) ?? run,
            workspacePath,
          );
          const agentInvoked = await worker.prompt(input.body);
          if (!agentInvoked) {
            await this.#finishLocalCommand(sessionId, worker, input.id);
          }
        } else if (input.kind === "prompted") {
          const pendingUi = this.#pendingUi.get(sessionId);
          if (pendingUi) {
            const normalized = input.body.trim().toLowerCase();
            const response =
              pendingUi.method === "confirm"
                ? {
                    confirmed: /^(?:y|yes|true|confirm|confirmed)$/u.test(
                      normalized,
                    ),
                  }
                : { value: input.body };
            await worker.respondToUi(pendingUi.id, response);
            this.#pendingUi.delete(sessionId);
          } else if (worker.isStreaming) {
            await worker.steer(input.body);
          } else {
            await worker.followUp(input.body);
          }
          this.#store.updateRun(sessionId, { state: "running" });
        }
        this.#store.markInputProcessed(input.id);
      }
    } catch (error) {
      await this.#handleFailure(sessionId, error);
    } finally {
      if (!this.#workers.has(sessionId))
        this.#store.releaseLease(sessionId, this.#owner);
    }
  }

  async #startWorker(run: AgentRunRecord, cwd: string): Promise<RpcWorker> {
    const worker = this.#workerFactory({ run, cwd });
    this.#workers.set(run.sessionId, worker);
    worker.onEvent((event) => this.#enqueueEvent(run.sessionId, event));
    await worker.start();
    this.#store.updateRun(run.sessionId, { state: "running" });
    await this.#captureWorkerState(run.sessionId, worker);
    return worker;
  }
  async #finishLocalCommand(
    sessionId: string,
    worker: RpcWorker,
    sourceId: string,
  ): Promise<void> {
    await this.#captureWorkerState(sessionId, worker);
    this.#store.updateRun(sessionId, { state: "waiting", nextAttemptAt: null });
    await this.#projector.thought(
      sessionId,
      `local-command:${sourceId}`,
      "The OhMyPi command completed without starting an agent turn.",
    );
  }

  async shutdown(): Promise<void> {
    const workers = [...this.#workers.entries()];
    this.#workers.clear();
    this.#pendingUi.clear();
    await Promise.all(
      workers.map(async ([sessionId, worker]) => {
        await worker.stop().catch(() => undefined);
        this.#store.releaseLease(sessionId, this.#owner);
      }),
    );
    await Promise.allSettled(this.#eventQueues.values());
    await this.#projector.flushPending();
  }

  async #cancel(run: AgentRunRecord): Promise<void> {
    const worker = this.#workers.get(run.sessionId);
    if (worker) {
      await worker.abort().catch(() => undefined);
      await worker.stop().catch(() => undefined);
      this.#workers.delete(run.sessionId);
    }
    this.#pendingUi.delete(run.sessionId);
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

  #enqueueEvent(sessionId: string, event: RpcEvent): void {
    const previous = this.#eventQueues.get(sessionId) ?? Promise.resolve();
    const next = previous
      .catch(() => undefined)
      .then(() => this.#handleEvent(sessionId, event))
      .catch((error: unknown) => this.#handleFailure(sessionId, error))
      .finally(() => {
        if (this.#eventQueues.get(sessionId) === next)
          this.#eventQueues.delete(sessionId);
      });
    this.#eventQueues.set(sessionId, next);
  }
  async #captureWorkerState(
    sessionId: string,
    worker: RpcWorker,
  ): Promise<void> {
    const state: Record<string, unknown> = await worker
      .getState()
      .catch((): Record<string, unknown> => ({}));
    const ompSessionId =
      typeof state.sessionId === "string" ? state.sessionId : worker.sessionId;
    const ompSessionFile =
      typeof state.sessionFile === "string"
        ? state.sessionFile
        : worker.sessionFile;
    this.#store.updateRun(sessionId, { ompSessionId, ompSessionFile });
    if (Array.isArray(state.todoPhases)) {
      const items = planItems(state.todoPhases);
      const fingerprint = new Bun.CryptoHasher("sha256")
        .update(JSON.stringify(items))
        .digest("hex")
        .slice(0, 16);
      await this.#projector.plan(
        sessionId,
        `plan:${sessionId}:${fingerprint}`,
        items,
      );
    }
  }

  async #handleEvent(sessionId: string, event: RpcEvent): Promise<void> {
    const run = this.#store.getRun(sessionId);
    if (!run || run.desiredState === "canceled" || run.state === "canceled")
      return;
    const sequence = (this.#eventSequence.get(sessionId) ?? 0) + 1;
    this.#eventSequence.set(sessionId, sequence);
    if (
      event.type === "extension_ui_request" &&
      typeof event.id === "string" &&
      typeof event.method === "string" &&
      ["select", "confirm", "input", "editor"].includes(event.method)
    ) {
      this.#pendingUi.set(sessionId, { id: event.id, method: event.method });
      const title =
        typeof event.title === "string" ? event.title : "Input required";
      const message = typeof event.message === "string" ? event.message : "";
      const options = Array.isArray(event.options)
        ? event.options.filter(
            (option): option is string => typeof option === "string",
          )
        : [];
      await this.#projector.elicitation(
        sessionId,
        `rpc-ui:${event.id}`,
        [title, message].filter(Boolean).join("\n\n"),
        options.length > 0 ? options : undefined,
      );
      this.#store.updateRun(sessionId, { state: "waiting" });
      return;
    }
    if (event.type === "prompt_result" && event.agentInvoked === false) {
      const worker = this.#workers.get(sessionId);
      if (worker) {
        await this.#finishLocalCommand(
          sessionId,
          worker,
          typeof event.id === "string" ? event.id : `prompt-result:${sequence}`,
        );
      }
      return;
    }
    if (event.type === "error") {
      throw new Error(
        typeof event.message === "string"
          ? event.message
          : "OhMyPi worker failed",
      );
    }
    const worker = this.#workers.get(sessionId);
    const terminalAgentEnd =
      event.type === "agent_end" && event.willContinue !== true;
    if (
      worker &&
      (event.type === "agent_start" ||
        event.type === "turn_end" ||
        event.type === "agent_end")
    ) {
      await this.#captureWorkerState(sessionId, worker);
    }
    if (terminalAgentEnd) {
      this.#store.updateRun(sessionId, {
        state: "succeeded",
        nextAttemptAt: null,
      });
      try {
        await this.#projector.projectRpcEvent(sessionId, sequence, event);
      } finally {
        if (worker) await worker.stop().catch(() => undefined);
        this.#workers.delete(sessionId);
        this.#store.releaseLease(sessionId, this.#owner);
      }
      return;
    }
    await this.#projector.projectRpcEvent(sessionId, sequence, event);
  }

  async #handleFailure(sessionId: string, error: unknown): Promise<void> {
    const message = error instanceof Error ? error.message : String(error);
    const worker = this.#workers.get(sessionId);
    if (worker) await worker.stop().catch(() => undefined);
    this.#workers.delete(sessionId);
    const current = this.#store.getRun(sessionId);
    if (
      !current ||
      current.state === "succeeded" ||
      current.state === "canceled"
    )
      return;
    if (current.desiredState === "canceled") {
      await this.#cancel(current);
      return;
    }
    if (current.attempt >= this.#maxAttempts) {
      const correlationId = failureCorrelationId(
        sessionId,
        current.attempt,
        message,
      );
      this.#store.updateRun(sessionId, {
        state: "failed",
        terminalReason: `${message} [${correlationId}]`,
        nextAttemptAt: null,
      });
      await this.#projector.terminal(
        sessionId,
        `failure:${correlationId}`,
        "error",
        `The OhMyPi run failed after ${current.attempt} attempts. Reference: ${correlationId}`,
      );
      return;
    }
    const delay = Math.min(300_000, 10_000 * 2 ** Math.min(current.attempt, 5));
    this.#store.updateRun(sessionId, {
      state: "orphaned",
      terminalReason: message,
      nextAttemptAt: Date.now() + delay + Math.floor(Math.random() * 1_000),
    });
  }
}
