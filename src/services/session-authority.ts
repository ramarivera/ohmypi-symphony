import { createHash, randomUUID } from "node:crypto";
import { Clock, Effect, Fiber, Option, Queue, Ref } from "effect";
import {
  type DatabaseError,
  GitHubAppApiError,
  type GitHubAppConfigurationError,
  GitHubAppRemoteError,
  type InstallationRevokedError,
  InterruptedRunNoActionableInputError,
  type LinearApiError,
  type RowDecodeError,
  RpcProtocolError,
  type RpcSpawnError,
  type RpcTimeoutError,
  type RunLeaseError,
  type TokenCipherError,
  type WorkspaceError,
} from "../domain/errors.js";
import type { SessionId, SourceKey } from "../domain/ids.js";
import type { AgentRun } from "../domain/models.js";
import { GatewayConfig } from "./config.js";
import { GitHubApp } from "./github-app.js";
import { ActivityProjector } from "./projector.js";
import type { RpcEvent, RpcWorkerHandle } from "./rpc-worker.js";
import { RpcWorker } from "./rpc-worker.js";
import {
  InstallationRepo,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
  WorkspaceRepo,
} from "./store/repositories.js";
import { makeWorkspace, workspaceBranchName } from "./workspace.js";

interface InputContext {
  readonly organizationId: string | null;
  readonly teamId: string | null;
  readonly projectId: string | null;
  readonly repositoryId: string | null;
  readonly issueLabels: ReadonlyArray<string>;
  readonly projectLabels: ReadonlyArray<string>;
}

interface WorkerState {
  readonly worker: RpcWorkerHandle;
  readonly queue: Queue.Queue<RpcEvent>;
  readonly consumer: Fiber.Fiber<never, AuthorityError>;
  readonly unsubscribe: () => Effect.Effect<void, never, never>;
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function nullableString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function jsonValue(value: unknown): string {
  if (typeof value === "string") return value;
  if (value === undefined || value === null) return "{}";
  return JSON.stringify(value);
}

function labelSet(value: unknown): ReadonlyArray<string> {
  if (!Array.isArray(value)) return [];
  const seen = new Set<string>();
  const out: string[] = [];
  for (const item of value) {
    let raw: string | null = null;
    if (typeof item === "string") {
      raw = item;
    } else if (record(item) && typeof item.name === "string") {
      raw = item.name;
    }
    if (!raw) continue;
    const normalized = raw.trim().toLowerCase();
    if (!normalized) continue;
    if (seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(normalized);
  }
  return out.sort();
}

function inputContext(payload: unknown): InputContext {
  if (!record(payload)) {
    return {
      organizationId: null,
      teamId: null,
      projectId: null,
      repositoryId: null,
      issueLabels: [],
      projectLabels: [],
    };
  }
  const session = record(payload.agentSession) ? payload.agentSession : null;
  const issue = session && record(session.issue) ? session.issue : null;
  const project = issue && record(issue.project) ? issue.project : null;
  return {
    organizationId:
      nullableString(payload.organizationId) ??
      (session ? nullableString(session.organizationId) : null),
    teamId: issue ? nullableString(issue.teamId) : null,
    projectId: issue
      ? (nullableString(issue.projectId) ??
        (project ? nullableString(project.id) : null))
      : null,
    repositoryId: nullableString(payload.repositoryId),
    issueLabels: issue ? labelSet(issue.labels) : [],
    projectLabels: project ? labelSet(project.labels) : [],
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
  return createHash("sha256")
    .update(`${sessionId}\0${attempt}\0${message}`)
    .digest("hex")
    .slice(0, 12);
}

type AuthorityError =
  | DatabaseError
  | RowDecodeError
  | RpcProtocolError
  | RpcSpawnError
  | RpcTimeoutError
  | RunLeaseError
  | TokenCipherError
  | GitHubAppApiError
  | GitHubAppConfigurationError
  | GitHubAppRemoteError
  | WorkspaceError
  | LinearApiError
  | InstallationRevokedError
  | InterruptedRunNoActionableInputError;

export class SessionAuthority extends Effect.Service<SessionAuthority>()(
  "SessionAuthority",
  {
    accessors: true,
    dependencies: [
      ActivityProjector.Default,
      InstallationRepo.Default,
      RunEventRepo.Default,
      RunInputRepo.Default,
      RunRepo.Default,
      WorkspaceRepo.Default,
      GitHubApp.Default,
      GatewayConfig.Default,
      RpcWorker.Default,
    ],
    effect: Effect.gen(function* () {
      const runRepo = yield* RunRepo;
      const runInputRepo = yield* RunInputRepo;
      const installationRepo = yield* InstallationRepo;
      const runEventRepo = yield* RunEventRepo;
      const workspaceRepo = yield* WorkspaceRepo;
      const projector = yield* ActivityProjector;
      const githubApp = yield* GitHubApp;
      const rpc = yield* RpcWorker;
      const config = yield* GatewayConfig;

      const owner = `authority:${yield* Effect.sync(() => randomUUID())}`;
      const leaseDurationMs = config.leaseDurationMs;
      const maxAttempts = 5;

      const runUrlForSession =
        config.publicUrl != null
          ? (sessionId: SessionId) =>
              new URL(
                `/runs/${encodeURIComponent(sessionId)}`,
                config.publicUrl,
              ).toString()
          : null;

      const workspace = yield* makeWorkspace({
        workspaceRoot: config.workspaceRoot,
        repo: workspaceRepo,
      });

      const workersRef = yield* Ref.make<ReadonlyMap<SessionId, WorkerState>>(
        new Map(),
      );
      const eventSequenceRef = yield* Ref.make<ReadonlyMap<SessionId, number>>(
        new Map(),
      );
      const pendingUiRef = yield* Ref.make<
        ReadonlyMap<SessionId, { readonly id: string; readonly method: string }>
      >(new Map());

      const getWorker = (
        sessionId: SessionId,
      ): Effect.Effect<Option.Option<WorkerState>, never, never> =>
        Ref.get(workersRef).pipe(
          Effect.map((workers) => Option.fromNullable(workers.get(sessionId))),
        );

      const releaseIfNoWorker = (
        sessionId: SessionId,
      ): Effect.Effect<void, never, never> =>
        Effect.gen(function* () {
          const workers = yield* Ref.get(workersRef);
          if (!workers.has(sessionId)) {
            yield* runRepo.releaseLease(sessionId, owner).pipe(
              Effect.catchTag("@Gateway/DatabaseError", (error) =>
                Effect.logWarning("authority.cleanup_failed", {
                  event: "authority.cleanup_failed",
                  sessionId,
                  operation: "releaseLease",
                  error: error.message,
                }),
              ),
            );
          }
        });

      const recordRunEvent = Effect.fn("SessionAuthority.recordRunEvent")(
        function* (sessionId: SessionId, sequence: number, event: RpcEvent) {
          const type = event.type;
          let sourceKey = `rpc:${sessionId}:${sequence}:${type}`;
          let kind = type;
          let level: "debug" | "info" | "warn" | "result" | "error" = "info";
          let text: string | null = type;

          switch (type) {
            case "agent_start":
              kind = "agent";
              text = "OhMyPi agent started";
              level = "info";
              break;
            case "turn_start":
              kind = "turn";
              text = "OhMyPi agent turn started";
              level = "info";
              break;
            case "turn_end":
              kind = "turn";
              text = "OhMyPi agent turn ended";
              level = "info";
              break;
            case "agent_end":
              kind = "agent";
              text =
                event.willContinue === true
                  ? "OhMyPi agent turn ended (will continue)"
                  : "OhMyPi agent ended";
              level = "result";
              break;
            case "tool_execution_start":
              kind = "tool";
              text = `Tool started: ${jsonValue(event.toolName ?? event.tool)}`;
              level = "info";
              break;
            case "tool_execution_end":
              kind = "tool";
              text = `Tool completed: ${jsonValue(event.toolName ?? event.tool)}`;
              level = event.error ? "error" : "result";
              break;
            case "message_end":
              kind = "message";
              text = "OhMyPi message ended";
              level = "info";
              break;
            case "prompt_result":
              kind = "prompt";
              text =
                event.agentInvoked === false
                  ? "OhMyPi command completed without agent"
                  : "OhMyPi prompt result";
              level = event.agentInvoked === false ? "result" : "info";
              break;
            case "extension_ui_request": {
              if (typeof event.id === "string") {
                sourceKey = `rpc-ui:${event.id}`;
              }
              const title =
                typeof event.title === "string"
                  ? event.title
                  : "Input required";
              const message =
                typeof event.message === "string" ? event.message : "";
              text = [title, message].filter(Boolean).join("\n\n");
              level = "warn";
              break;
            }
            case "error":
              text =
                typeof event.message === "string"
                  ? event.message
                  : "OhMyPi worker failed";
              break;
            default:
              text = type;
          }

          const now = yield* Clock.currentTimeMillis;
          yield* runEventRepo.upsert({
            sourceKey: sourceKey as SourceKey,
            sessionId,
            kind,
            level,
            text,
            payload: event,
            status: "observed",
            now,
          });
        },
      );

      const captureWorkerState = Effect.fn(
        "SessionAuthority.captureWorkerState",
      )(function* (sessionId: SessionId, worker: RpcWorkerHandle) {
        const state = yield* worker.getState();
        const fromStateSessionId = isString(state.sessionId)
          ? Option.some(state.sessionId)
          : Option.none<string>();
        const fromStateSessionFile = isString(state.sessionFile)
          ? Option.some(state.sessionFile)
          : Option.none<string>();
        const workerSessionId = yield* worker.sessionId;
        const workerSessionFile = yield* worker.sessionFile;
        const ompSessionId = Option.isSome(fromStateSessionId)
          ? fromStateSessionId
          : workerSessionId;
        const ompSessionFile = Option.isSome(fromStateSessionFile)
          ? fromStateSessionFile
          : workerSessionFile;
        yield* runRepo.update(sessionId, { ompSessionId, ompSessionFile });
        if (Array.isArray(state.todoPhases)) {
          const items = planItems(state.todoPhases);
          if (items.length > 0) {
            const fingerprint = yield* Effect.sync(() =>
              createHash("sha256")
                .update(JSON.stringify(items))
                .digest("hex")
                .slice(0, 16),
            );
            yield* projector.plan(
              sessionId,
              `plan:${sessionId}:${fingerprint}`,
              items,
            );
          }
        }
      });

      const abortForCleanup = (
        sessionId: SessionId,
        worker: RpcWorkerHandle,
      ): Effect.Effect<void, never, never> =>
        worker.abort().pipe(
          Effect.catchTags({
            "@Gateway/RpcProtocolError": (error) =>
              Effect.logWarning("authority.cleanup_failed", {
                event: "authority.cleanup_failed",
                sessionId,
                operation: "abort",
                error: error.message,
              }),
          }),
        );

      const finishLocalCommand = Effect.fn(
        "SessionAuthority.finishLocalCommand",
      )(function* (
        sessionId: SessionId,
        worker: RpcWorkerHandle,
        sourceId: string,
      ) {
        yield* captureWorkerState(sessionId, worker);
        yield* runRepo.update(sessionId, {
          state: "waiting",
          nextAttemptAt: Option.none(),
        });
        yield* projector.thought(
          sessionId,
          `local-command:${sourceId}`,
          "The OhMyPi command completed without starting an agent turn.",
        );
      });

      const cancel = Effect.fn("SessionAuthority.cancel")(function* (
        run: AgentRun,
      ) {
        const state = yield* getWorker(run.sessionId);
        if (Option.isSome(state)) {
          yield* abortForCleanup(run.sessionId, state.value.worker);
          yield* state.value.worker.stop();
          yield* Ref.update(workersRef, (workers) => {
            const next = new Map(workers);
            next.delete(run.sessionId);
            return next;
          });
        }
        yield* Ref.update(pendingUiRef, (pending) => {
          const next = new Map(pending);
          next.delete(run.sessionId);
          return next;
        });
        if (
          run.state !== "succeeded" &&
          run.state !== "failed" &&
          run.state !== "canceled"
        ) {
          yield* runRepo.update(run.sessionId, {
            state: "canceled",
            terminalReason: Option.some("Stopped by Linear user"),
          });
          yield* Effect.logInfo("run.canceled", {
            event: "run.canceled",
            sessionId: run.sessionId,
            attempt: run.attempt,
          });
          yield* projector.terminal(
            run.sessionId,
            `stop:${run.sessionId}`,
            "response",
            "Stopped as requested.",
          );
        }
        yield* releaseIfNoWorker(run.sessionId);
      });

      const handleFailure = Effect.fn("SessionAuthority.handleFailure")(
        function* (sessionId: SessionId, error: AuthorityError) {
          const message = error.message;
          const worker = yield* getWorker(sessionId);
          if (Option.isSome(worker)) {
            yield* abortForCleanup(sessionId, worker.value.worker);
            yield* worker.value.worker.stop();
            yield* Ref.update(workersRef, (workers) => {
              const next = new Map(workers);
              next.delete(sessionId);
              return next;
            });
          }

          yield* Effect.logWarning("authority.failure", {
            event: "authority.failure",
            sessionId,
            error: message,
          });

          const current = yield* runRepo.get(sessionId);
          if (
            Option.isNone(current) ||
            current.value.state === "succeeded" ||
            current.value.state === "canceled"
          ) {
            return;
          }

          const run = current.value;
          if (run.desiredState === "canceled") {
            yield* cancel(run);
            return;
          }

          if (run.attempt >= maxAttempts) {
            const correlationId = failureCorrelationId(
              sessionId,
              run.attempt,
              message,
            );
            yield* runRepo.update(run.sessionId, {
              state: "failed",
              terminalReason: Option.some(`${message} [${correlationId}]`),
              nextAttemptAt: Option.none(),
            });
            yield* Effect.logInfo("run.failed", {
              event: "run.failed",
              sessionId,
              attempt: run.attempt,
              correlationId,
              terminalReason: `${message} [${correlationId}]`,
            });
            yield* projector.terminal(
              sessionId,
              `failure:${correlationId}`,
              "error",
              `The OhMyPi run failed after ${run.attempt} attempts. Reference: ${correlationId}`,
            );
            return;
          }

          const delay = Math.min(
            300_000,
            10_000 * 2 ** Math.min(run.attempt, 5),
          );
          const jitter = yield* Effect.sync(() =>
            Math.floor(Math.random() * 1_000),
          );
          const now = yield* Clock.currentTimeMillis;
          const nextAttemptAt = now + delay + jitter;
          yield* runRepo.update(run.sessionId, {
            state: "orphaned",
            terminalReason: Option.some(message),
            nextAttemptAt: Option.some(nextAttemptAt),
          });
          yield* Effect.logInfo("run.retried", {
            event: "run.retried",
            sessionId,
            attempt: run.attempt,
            delay,
            nextAttemptAt,
          });
        },
      );

      const handleEvent = Effect.fn("SessionAuthority.handleEvent")(function* (
        sessionId: SessionId,
        event: RpcEvent,
      ) {
        const current = yield* runRepo.get(sessionId);
        if (
          Option.isNone(current) ||
          current.value.desiredState === "canceled" ||
          current.value.state === "canceled" ||
          current.value.state === "succeeded"
        ) {
          return;
        }
        const run = current.value;
        const sequence = yield* Ref.modify(eventSequenceRef, (m) => {
          const next = new Map(m);
          const value = (next.get(sessionId) ?? 0) + 1;
          next.set(sessionId, value);
          return [value, next] as const;
        });
        yield* recordRunEvent(sessionId, sequence, event);

        if (event.type === "extension_ui_request") {
          const id = event.id;
          const method = event.method;
          if (
            typeof id === "string" &&
            typeof method === "string" &&
            ["select", "confirm", "input", "editor"].includes(method)
          ) {
            yield* Ref.update(pendingUiRef, (pending) => {
              const next = new Map(pending);
              next.set(sessionId, { id, method });
              return next;
            });
            const title =
              typeof event.title === "string" ? event.title : "Input required";
            const message =
              typeof event.message === "string" ? event.message : "";
            const options = Array.isArray(event.options)
              ? event.options.filter(
                  (option): option is string => typeof option === "string",
                )
              : [];
            yield* projector.elicitation(
              sessionId,
              `rpc-ui:${event.id}`,
              [title, message].filter(Boolean).join("\n\n"),
              options.length > 0 ? options : undefined,
            );
            yield* runRepo.update(sessionId, { state: "waiting" });
            return;
          }
        }

        if (event.type === "prompt_result" && event.agentInvoked === false) {
          const worker = yield* getWorker(sessionId);
          if (Option.isSome(worker)) {
            yield* finishLocalCommand(
              sessionId,
              worker.value.worker,
              typeof event.id === "string"
                ? event.id
                : `prompt-result:${sequence}`,
            );
          }
          return;
        }
        if (event.type === "error") {
          return yield* Effect.fail(
            new RpcProtocolError({
              method: "worker",
              message:
                typeof event.message === "string"
                  ? event.message
                  : "OhMyPi worker failed",
            }),
          );
        }

        const worker = yield* getWorker(sessionId);
        const terminalAgentEnd =
          event.type === "agent_end" && event.willContinue !== true;
        if (
          Option.isSome(worker) &&
          (event.type === "agent_start" ||
            event.type === "turn_end" ||
            event.type === "agent_end")
        ) {
          yield* captureWorkerState(sessionId, worker.value.worker);
        }

        if (terminalAgentEnd) {
          const repositoryId = run.repositoryId;
          const workspacePath = run.workspacePath;
          if (githubApp.isEnabled()) {
            if (Option.isNone(repositoryId) || Option.isNone(workspacePath)) {
              return yield* Effect.fail(
                new GitHubAppApiError({
                  message: "GitHub workspace publication has no repository",
                  operation: "workspace publication",
                }),
              );
            }
            const repositoryOption = yield* workspaceRepo.getRepository(
              run.organizationId,
              repositoryId.value,
            );
            if (Option.isNone(repositoryOption)) {
              return yield* Effect.fail(
                new GitHubAppApiError({
                  message:
                    "GitHub workspace publication repository is unavailable",
                  operation: "workspace publication",
                }),
              );
            }
            const issueReference = Option.match(run.issueId, {
              onNone: () => sessionId,
              onSome: (issueId) => issueId,
            });
            const pullRequest = yield* githubApp
              .publishPullRequest({
                repositoryUrl: repositoryOption.value.url,
                base: repositoryOption.value.ref,
                branch: workspaceBranchName(sessionId),
                workspacePath: workspacePath.value,
                title: `OhMyPi changes for ${issueReference}`,
                body: `Automated workspace changes for session ${sessionId}.`,
              })
              .pipe(
                Effect.mapError((error) =>
                  error instanceof GitHubAppRemoteError
                    ? new GitHubAppRemoteError({
                        message: `GitHub publication rejected configured repository ${repositoryOption.value.id}`,
                      })
                    : error,
                ),
              );
            if (pullRequest !== undefined) {
              yield* projector.externalUrls(
                sessionId,
                `github-pr:${sessionId}`,
                [{ label: "GitHub pull request", url: pullRequest.url }],
              );
            }
          }
          yield* runRepo.update(sessionId, {
            state: "succeeded",
            nextAttemptAt: Option.none(),
          });
          yield* Effect.logInfo("run.completed", {
            event: "run.completed",
            sessionId,
            attempt: run.attempt,
          });
          yield* projector.projectRpcEvent(sessionId, sequence, event).pipe(
            Effect.ensuring(
              Effect.gen(function* () {
                if (Option.isSome(worker)) {
                  yield* worker.value.worker.stop();
                }
                yield* Ref.update(workersRef, (workers) => {
                  const next = new Map(workers);
                  next.delete(sessionId);
                  return next;
                });
                yield* releaseIfNoWorker(sessionId);
              }),
            ),
          );
          return;
        }

        yield* projector.projectRpcEvent(sessionId, sequence, event);
      });

      const startWorker = Effect.fn("SessionAuthority.startWorker")(function* (
        run: AgentRun,
        cwd: string,
      ) {
        const command: string[] = [config.ompCliPath];
        if (Option.isSome(run.ompSessionFile)) {
          command.push("--session", run.ompSessionFile.value);
        }

        const worker = yield* rpc.spawn({
          command,
          cwd,
        });

        const queue = yield* Queue.unbounded<RpcEvent>();

        const unsubscribe = yield* worker.onEvent((event) => {
          Queue.unsafeOffer(queue, event);
        });

        const consumer = yield* Effect.fork(
          Effect.forever(
            Queue.take(queue).pipe(
              Effect.flatMap((event) => handleEvent(run.sessionId, event)),
              Effect.catchTags({
                "@Gateway/DatabaseError": (error) =>
                  handleFailure(run.sessionId, error),
                "@Gateway/RowDecodeError": (error) =>
                  handleFailure(run.sessionId, error),
                "@Gateway/RpcProtocolError": (error) =>
                  handleFailure(run.sessionId, error),
                "@Gateway/GitHubAppApiError": (error) =>
                  handleFailure(run.sessionId, error),
                "@Gateway/GitHubAppConfigurationError": (error) =>
                  handleFailure(run.sessionId, error),
                "@Gateway/GitHubAppRemoteError": (error) =>
                  handleFailure(run.sessionId, error),
              }),
            ),
          ),
        );

        yield* Ref.update(workersRef, (workers) => {
          const next = new Map(workers);
          next.set(run.sessionId, {
            worker,
            queue,
            consumer,
            unsubscribe,
          });
          return next;
        });

        yield* worker.start();
        yield* runRepo.update(run.sessionId, { state: "running" });
        yield* captureWorkerState(run.sessionId, worker);

        const ompSessionId = yield* worker.sessionId;
        const ompSessionFile = yield* worker.sessionFile;

        yield* Effect.logInfo("work.ready", {
          event: "work.ready",
          sessionId: run.sessionId,
          attempt: run.attempt,
          cwd,
          ompSessionId: Option.getOrElse(ompSessionId, () => null),
          ompSessionFile: Option.getOrElse(ompSessionFile, () => null),
        });

        return worker;
      });

      const processSession = Effect.fn("SessionAuthority.processSession")(
        function* (
          sessionId: SessionId,
        ): Effect.fn.Return<void, AuthorityError> {
          yield* Effect.annotateCurrentSpan("sessionId", sessionId);
          return yield* Effect.gen(function* () {
            const initial = yield* runRepo.get(sessionId);
            if (
              Option.isSome(initial) &&
              initial.value.desiredState === "canceled"
            ) {
              yield* cancel(initial.value);
              return;
            }
            const workerState = yield* getWorker(sessionId);
            const leased = Option.isSome(workerState)
              ? yield* runRepo.renewLease(sessionId, owner, leaseDurationMs)
              : yield* runRepo.claimLease(sessionId, owner, leaseDurationMs);
            if (!leased) return;
            const runOption = yield* runRepo.get(sessionId);
            if (Option.isNone(runOption)) return;
            const run = runOption.value;
            yield* Effect.logInfo("work.assigned", {
              event: "work.assigned",
              sessionId,
              attempt: run.attempt,
              state: run.state,
            });
            if (run.desiredState === "canceled") {
              yield* cancel(run);
              return;
            }
            const installation = yield* installationRepo.get(
              run.organizationId,
            );
            if (
              Option.isNone(installation) ||
              Option.isSome(installation.value.revokedAt)
            ) {
              yield* runRepo.update(sessionId, {
                state: "failed",
                terminalReason: Option.some(
                  "Linear installation is unavailable",
                ),
              });
              yield* projector.terminal(
                sessionId,
                `installation-unavailable:${run.organizationId}`,
                "error",
                "The Linear installation is unavailable. Reinstall or reauthorize the app, then try again.",
              );
              return;
            }
            const teamAccess = Option.match(
              installation.value.accessibleTeamIds,
              {
                onNone: () => [] as ReadonlyArray<string>,
                onSome: (ids) => ids,
              },
            );
            const canAccessAll = Option.match(
              installation.value.canAccessAllPublicTeams,
              {
                onNone: () => false,
                onSome: (value) => value,
              },
            );
            if (
              Option.isSome(run.teamId) &&
              !canAccessAll &&
              !teamAccess.includes(run.teamId.value)
            ) {
              yield* runRepo.update(sessionId, {
                state: "canceled",
                terminalReason: Option.some("Linear team access was removed"),
              });
              yield* projector.terminal(
                sessionId,
                `team-access-removed:${run.teamId.value}`,
                "response",
                "Stopped because this Linear installation no longer has access to the issue's team.",
              );
              return;
            }
            const inputs = yield* runInputRepo.pending(sessionId);
            let worker: RpcWorkerHandle | undefined = Option.getOrElse(
              Option.map(workerState, (state) => state.worker),
              () => undefined,
            );
            if (inputs.length === 0) {
              if (
                worker === undefined &&
                run.state === "orphaned" &&
                Option.isSome(run.workspacePath)
              ) {
                yield* runRepo.update(sessionId, {
                  state: "starting",
                  incrementAttempt: true,
                  nextAttemptAt: Option.none(),
                });
                const resumedOption = yield* runRepo.get(sessionId);
                if (Option.isNone(resumedOption)) return;
                const resumed = resumedOption.value;
                yield* Effect.logInfo("run.retried", {
                  event: "run.retried",
                  sessionId,
                  attempt: resumed.attempt,
                  workspacePath: run.workspacePath.value,
                });
                worker = yield* startWorker(resumed, run.workspacePath.value);
                yield* projector.thought(
                  sessionId,
                  `retry:${resumed.attempt}`,
                  `Retrying the interrupted OhMyPi run (attempt ${resumed.attempt}).`,
                );
                if (Option.isSome(run.ompSessionFile)) {
                  yield* worker.followUp(
                    "Continue the interrupted Linear task from the saved session state.",
                  );
                } else {
                  const latestActionable =
                    yield* runInputRepo.latestActionableInput(sessionId);
                  if (Option.isNone(latestActionable)) {
                    return yield* Effect.fail(
                      new InterruptedRunNoActionableInputError({
                        sessionId,
                        message:
                          "Interrupted run has no actionable input to resume",
                      }),
                    );
                  }
                  const agentInvoked = yield* worker.prompt(
                    latestActionable.value.body,
                  );
                  if (!agentInvoked) {
                    yield* finishLocalCommand(
                      sessionId,
                      worker,
                      `retry:${resumed.attempt}`,
                    );
                  }
                }
              }
              return;
            }
            for (const input of inputs) {
              const latestOption = yield* runRepo.get(sessionId);
              const latest = Option.isSome(latestOption)
                ? latestOption.value
                : run;
              if (latest.desiredState === "canceled" || input.kind === "stop") {
                yield* cancel(latest);
                yield* runInputRepo.markProcessed(input.id);
                break;
              }
              if (worker === undefined) {
                if (runUrlForSession !== null) {
                  yield* projector.externalUrls(
                    sessionId,
                    `run-url:${sessionId}`,
                    [{ label: "OhMyPi run", url: runUrlForSession(sessionId) }],
                  );
                }
                yield* projector.thought(
                  sessionId,
                  `accepted:${input.id}`,
                  "Request accepted; preparing the OhMyPi worker.",
                );
                const baseContext = inputContext(input.payload);
                const resolvedContext = {
                  ...baseContext,
                  organizationId:
                    baseContext.organizationId ??
                    (run.organizationId as string),
                };
                const context =
                  input.kind === "prompted" &&
                  latest.state === "waiting" &&
                  resolvedContext.repositoryId === null
                    ? { ...resolvedContext, repositoryId: input.body.trim() }
                    : resolvedContext;
                const resolution = yield* workspace.resolve(context);
                if (resolution.kind === "none") {
                  yield* projector.elicitation(
                    sessionId,
                    `repo:none:${input.id}`,
                    "No repository is configured for this Linear issue.",
                  );
                  yield* runRepo.update(sessionId, { state: "waiting" });
                  return;
                }
                if (resolution.kind === "ambiguous") {
                  yield* projector.elicitation(
                    sessionId,
                    `repo:ambiguous:${input.id}`,
                    "Select the repository for this issue.",
                    resolution.repositories.map((r) => r.id),
                  );
                  yield* runRepo.update(sessionId, { state: "waiting" });
                  return;
                }
                const workspacePath = yield* workspace.materialize(
                  sessionId,
                  resolution.repository,
                );
                yield* runRepo.update(sessionId, {
                  state: "starting",
                  repositoryId: Option.some(resolution.repository.id),
                  workspacePath: Option.some(workspacePath),
                  incrementAttempt: true,
                });
                const updatedOption = yield* runRepo.get(sessionId);
                if (Option.isNone(updatedOption)) return;
                worker = yield* startWorker(updatedOption.value, workspacePath);
                const agentInvoked = yield* worker.prompt(input.body);
                if (!agentInvoked) {
                  yield* finishLocalCommand(sessionId, worker, input.id);
                }
              } else if (input.kind === "prompted") {
                const pendingUi = yield* Ref.get(pendingUiRef).pipe(
                  Effect.map((m) => m.get(sessionId)),
                );
                if (pendingUi !== undefined) {
                  const normalized = input.body.trim().toLowerCase();
                  const response =
                    pendingUi.method === "confirm"
                      ? {
                          confirmed: /^(?:y|yes|true|confirm|confirmed)$/u.test(
                            normalized,
                          ),
                        }
                      : { value: input.body };
                  yield* worker.respondToUi(pendingUi.id, response);
                  yield* Ref.update(pendingUiRef, (m) => {
                    const next = new Map(m);
                    next.delete(sessionId);
                    return next;
                  });
                } else if (yield* worker.isStreaming) {
                  yield* worker.steer(input.body);
                } else {
                  yield* worker.followUp(input.body);
                }
                yield* runRepo.update(sessionId, { state: "running" });
              }
              yield* runInputRepo.markProcessed(input.id);
            }
          }).pipe(Effect.ensuring(releaseIfNoWorker(sessionId)));
        },
      );

      const processRunnable = Effect.fn("SessionAuthority.processRunnable")(
        function* (): Effect.fn.Return<void, AuthorityError> {
          yield* projector.flushPending();
          const now = yield* Clock.currentTimeMillis;
          const workers = yield* Ref.get(workersRef);
          for (const [sessionId, workerState] of workers) {
            const run = yield* runRepo.get(sessionId);
            if (Option.isNone(run) || run.value.desiredState === "canceled") {
              continue;
            }
            const renewed = yield* runRepo.renewLease(
              sessionId,
              owner,
              leaseDurationMs,
            );
            if (!renewed) {
              yield* abortForCleanup(sessionId, workerState.worker);
              yield* workerState.worker.stop();
              yield* Ref.update(workersRef, (m) => {
                const next = new Map(m);
                next.delete(sessionId);
                return next;
              });
            }
          }
          const cancellationPending = yield* runRepo.listCancellationPending();
          for (const run of cancellationPending) {
            yield* cancel(run);
          }
          const runnable = yield* runRepo.listRunnable(now);
          const sessionsWithInputs =
            yield* runInputRepo.listSessionsWithPendingInputs();
          const sessionIds = new Set<SessionId>([
            ...sessionsWithInputs,
            ...runnable.map((r) => r.sessionId),
          ]);
          for (const sessionId of sessionIds) {
            yield* processSession(sessionId);
          }
          yield* projector.flushPending();
        },
      );

      const shutdown = Effect.fn("SessionAuthority.shutdown")(
        function* (): Effect.fn.Return<void, AuthorityError> {
          const workers = yield* Ref.get(workersRef);
          for (const [, workerState] of workers) {
            yield* Fiber.interrupt(workerState.consumer);
            yield* workerState.unsubscribe();
            yield* workerState.worker.stop();
          }
          yield* Ref.set(workersRef, new Map());
          yield* Ref.set(eventSequenceRef, new Map());
          yield* Ref.set(pendingUiRef, new Map());
          yield* projector.flushPending();
        },
      );

      const activeWorkerCount = Effect.fn("SessionAuthority.activeWorkerCount")(
        function* (): Effect.fn.Return<number, never> {
          const workers = yield* Ref.get(workersRef);
          return workers.size;
        },
      );

      return {
        processRunnable,
        processSession,
        shutdown,
        activeWorkerCount,
      };
    }),
  },
) {}
