import { createHash, randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { Clock, Effect, Fiber, Option, Queue, Ref } from "effect";
import {
  type DatabaseError,
  type InstallationRevokedError,
  InterruptedRunNoActionableInputError,
  type LinearApiError,
  type NixEnvironmentError,
  type RowDecodeError,
  RpcProtocolError,
  RpcSpawnError,
  type RpcTimeoutError,
  type RunLeaseError,
  type TokenCipherError,
  type WorkspaceError,
} from "../domain/errors.js";
import type { SessionId, SourceKey } from "../domain/ids.js";
import type { AgentRun } from "../domain/models.js";
import { GatewayConfig } from "./config.js";
import { NixEnvironment } from "./nix-environment.js";
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
import { makeWorkspace } from "./workspace.js";

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

const LINEAR_WORKER_CONTRACT = `Linear integration:
- Use OMP todos for meaningful multi-step work; they are displayed as the Linear agent plan.
- Use OMP UI requests when human input, selection, confirmation, or authorization is required; they are displayed as Linear elicitations.
- Tool execution and lifecycle progress are projected automatically as Linear thought and action activities. Do not call Linear directly to report progress.
- Call rromp_report_deviation as soon as you take a shortcut, depart from the original request, change a material assumption, or make a consequential implementation decision. The report becomes a visible Linear issue comment.
- Write the final response for the Linear user: state the outcome, include relevant artifact URLs, and name any required user action.
- If the run is stopped, cease work immediately; the gateway handles the terminal Linear response.`;

export const linearWorkerPrompt = (
  kind: "created" | "prompted" | "stop",
  body: string,
): string =>
  kind === "created"
    ? `${LINEAR_WORKER_CONTRACT}\n\nLinear task:\n${body}`
    : body;

export const resolveDeviationExtensionPath = (): string | null => {
  const candidates = [
    fileURLToPath(
      new URL("../extensions/report-deviation.ts", import.meta.url),
    ),
    fileURLToPath(new URL("./extensions/report-deviation.js", import.meta.url)),
  ];
  return candidates.find((candidate) => existsSync(candidate)) ?? null;
};

export const deviationFromRpcEvent = (event: RpcEvent): string | null => {
  if (event.type !== "tool_execution_start") return null;
  const toolName = event.toolName ?? event.tool;
  if (toolName !== "rromp_report_deviation" || !record(event.args)) return null;
  const deviation = event.args.deviation;
  return typeof deviation === "string" && deviation.trim().length > 0
    ? deviation.trim()
    : null;
};
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
  | WorkspaceError
  | LinearApiError
  | InstallationRevokedError
  | InterruptedRunNoActionableInputError
  | NixEnvironmentError;

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
      GatewayConfig.Default,
      RpcWorker.Default,
      NixEnvironment.Default,
    ],
    effect: Effect.gen(function* () {
      const runRepo = yield* RunRepo;
      const runInputRepo = yield* RunInputRepo;
      const installationRepo = yield* InstallationRepo;
      const runEventRepo = yield* RunEventRepo;
      const workspaceRepo = yield* WorkspaceRepo;
      const projector = yield* ActivityProjector;
      const rpc = yield* RpcWorker;
      const nixEnvironment = yield* NixEnvironment;
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
                Effect.logWarning("authority.cleanup_failed").pipe(
                  Effect.annotateLogs({
                    event: "authority.cleanup_failed",
                    sessionId,
                    operation: "releaseLease",
                    error: error.message,
                  }),
                ),
              ),
            );
          }
        });

      const recordRunEvent = Effect.fn("SessionAuthority.recordRunEvent")(
        function* (
          sessionId: SessionId,
          sequence: number,
          event: RpcEvent,
        ): Effect.fn.Return<void, DatabaseError> {
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
      )(function* (
        sessionId: SessionId,
        worker: RpcWorkerHandle,
      ): Effect.fn.Return<
        void,
        DatabaseError | RowDecodeError | RpcProtocolError
      > {
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
              Effect.logWarning("authority.cleanup_failed").pipe(
                Effect.annotateLogs({
                  event: "authority.cleanup_failed",
                  sessionId,
                  operation: "abort",
                  error: error.message,
                }),
              ),
          }),
        );

      const finishLocalCommand = Effect.fn(
        "SessionAuthority.finishLocalCommand",
      )(function* (
        sessionId: SessionId,
        worker: RpcWorkerHandle,
        sourceId: string,
      ): Effect.fn.Return<
        void,
        DatabaseError | RowDecodeError | RpcProtocolError
      > {
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
      ): Effect.fn.Return<void, DatabaseError | RowDecodeError> {
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
          yield* Effect.logInfo("run.canceled").pipe(
            Effect.annotateLogs({
              event: "run.canceled",
              sessionId: run.sessionId,
              attempt: run.attempt,
            }),
          );
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
        function* (
          sessionId: SessionId,
          error: AuthorityError,
        ): Effect.fn.Return<void, DatabaseError | RowDecodeError> {
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

          yield* Effect.logWarning("authority.failure").pipe(
            Effect.annotateLogs({
              event: "authority.failure",
              sessionId,
              error: message,
            }),
          );

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
            yield* Effect.logInfo("run.failed").pipe(
              Effect.annotateLogs({
                event: "run.failed",
                sessionId,
                attempt: run.attempt,
                correlationId,
                terminalReason: `${message} [${correlationId}]`,
              }),
            );
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
          yield* Effect.logInfo("run.retried").pipe(
            Effect.annotateLogs({
              event: "run.retried",
              sessionId,
              attempt: run.attempt,
              delay,
              nextAttemptAt,
            }),
          );
        },
      );

      const handleEvent = Effect.fn("SessionAuthority.handleEvent")(function* (
        sessionId: SessionId,
        event: RpcEvent,
      ): Effect.fn.Return<
        void,
        DatabaseError | RowDecodeError | RpcProtocolError
      > {
        const current = yield* runRepo.get(sessionId);
        if (
          Option.isNone(current) ||
          current.value.desiredState === "canceled" ||
          current.value.state === "canceled"
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

        const deviation = deviationFromRpcEvent(event);
        if (deviation !== null) {
          if (Option.isSome(run.issueId)) {
            const eventId =
              typeof event.id === "string"
                ? event.id
                : createHash("sha256")
                    .update(deviation)
                    .digest("hex")
                    .slice(0, 16);
            yield* projector.deviation(
              sessionId,
              `deviation:${sessionId}:${eventId}`,
              deviation,
            );
          } else {
            yield* Effect.logWarning("deviation.comment.skipped").pipe(
              Effect.annotateLogs({
                sessionId,
                reason: "run has no Linear issue",
              }),
            );
          }
        }
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
          yield* Effect.logInfo("run.completed").pipe(
            Effect.annotateLogs({
              event: "run.completed",
              sessionId,
              attempt: run.attempt,
            }),
          );
          yield* runRepo.update(sessionId, {
            state: "succeeded",
            nextAttemptAt: Option.none(),
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
      ): Effect.fn.Return<
        RpcWorkerHandle,
        | DatabaseError
        | RowDecodeError
        | RpcProtocolError
        | RpcSpawnError
        | RpcTimeoutError
        | NixEnvironmentError
      > {
        const command: string[] = [config.ompCliPath];
        if (Option.isSome(run.ompSessionFile)) {
          command.push("--session", run.ompSessionFile.value);
        }

        const deviationExtensionPath = resolveDeviationExtensionPath();
        if (deviationExtensionPath === null) {
          return yield* Effect.fail(
            new RpcSpawnError({
              message: "Deviation reporting extension is missing",
            }),
          );
        }
        command.push("--extension", deviationExtensionPath);

        const environment = { ...process.env };
        if (Option.isSome(run.repositoryId)) {
          const repository = yield* workspaceRepo.getRepository(
            run.organizationId,
            run.repositoryId.value,
          );

          if (Option.isSome(repository)) {
            const prepared = yield* nixEnvironment
              .prepare(repository.value)
              .pipe(
                Effect.tap((result) =>
                  Effect.logInfo("nix.environment_prepared").pipe(
                    Effect.annotateLogs({
                      event: "nix.environment_prepared",
                      sessionId: run.sessionId,
                      repositoryId: repository.value.id,
                      cacheKey: result.cacheKey,
                      reused: result.reused,
                      pathEntryCount: result.pathEntries.length,
                    }),
                  ),
                ),
                Effect.catchTag(
                  "@Gateway/NixEnvironmentError",
                  (error: NixEnvironmentError) =>
                    Effect.logWarning("nix.environment_failed").pipe(
                      Effect.annotateLogs({
                        event: "nix.environment_failed",
                        sessionId: run.sessionId,
                        repositoryId: repository.value.id,
                        errorTag: error._tag,
                        reason: error.reason,
                      }),
                      Effect.zipRight(handleFailure(run.sessionId, error)),
                      Effect.zipRight(Effect.fail(error)),
                    ),
                ),
              );
            environment.PATH = [...prepared.pathEntries, environment.PATH ?? ""]
              .filter((entry) => entry.length > 0)
              .join(":");
          }
        }

        const worker = yield* rpc.spawn({
          command,
          cwd,
          env: environment,
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

        yield* Effect.logInfo("work.ready").pipe(
          Effect.annotateLogs({
            event: "work.ready",
            sessionId: run.sessionId,
            attempt: run.attempt,
            cwd,
            ompSessionId: Option.getOrElse(ompSessionId, () => null),
            ompSessionFile: Option.getOrElse(ompSessionFile, () => null),
          }),
        );

        return worker;
      });

      const processSession = Effect.fn("SessionAuthority.processSession")(
        function* (
          sessionId: SessionId,
        ): Effect.fn.Return<void, AuthorityError> {
          yield* Effect.annotateCurrentSpan("sessionId", sessionId);
          return yield* Effect.gen(function* () {
            const initial = yield* runRepo.get(sessionId);
            const inputs = yield* runInputRepo.pending(sessionId);
            if (
              Option.isSome(initial) &&
              initial.value.desiredState === "canceled"
            ) {
              // A pending follow-up prompt resumes a user-stopped run.
              // reopen only fires from state='canceled'; anything else
              // (mid-flight 'stopping', externally canceled active runs)
              // keeps honoring the cancellation.
              const hasActionableInput = inputs.some(
                (input) => input.kind !== "stop",
              );
              const resumed =
                hasActionableInput && (yield* runRepo.reopen(sessionId));
              if (!resumed) {
                yield* cancel(initial.value);
                // The cancellation honors any pending stop inputs; mark them
                // so a later prompt isn't preceded by a stale stop re-cancel.
                yield* Effect.forEach(
                  inputs.filter((input) => input.kind === "stop"),
                  (input) => runInputRepo.markProcessed(input.id),
                  { discard: true },
                );
                return;
              }
              yield* Effect.logInfo("run.resumed").pipe(
                Effect.annotateLogs({
                  event: "run.resumed",
                  sessionId,
                }),
              );
            }
            const workerState = yield* getWorker(sessionId);
            const leased = Option.isSome(workerState)
              ? yield* runRepo.renewLease(sessionId, owner, leaseDurationMs)
              : yield* runRepo.claimLease(sessionId, owner, leaseDurationMs);
            if (!leased) return;
            const runOption = yield* runRepo.get(sessionId);
            if (Option.isNone(runOption)) return;
            const run = runOption.value;
            yield* Effect.logInfo("work.assigned").pipe(
              Effect.annotateLogs({
                event: "work.assigned",
                sessionId,
                attempt: run.attempt,
                state: run.state,
              }),
            );
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
                yield* Effect.logInfo("run.retried").pipe(
                  Effect.annotateLogs({
                    event: "run.retried",
                    sessionId,
                    attempt: resumed.attempt,
                    workspacePath: run.workspacePath.value,
                  }),
                );
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
                    linearWorkerPrompt(
                      latestActionable.value.kind,
                      latestActionable.value.body,
                    ),
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
              let latest = Option.isSome(latestOption)
                ? latestOption.value
                : run;
              if (input.kind === "stop") {
                yield* cancel(latest);
                yield* runInputRepo.markProcessed(input.id);
                break;
              }
              if (latest.desiredState === "canceled") {
                // A follow-up prompt on a user-stopped session resumes the
                // run; any other input on a canceled run still honors the
                // cancellation. reopen refuses mid-flight cancellations
                // (state='stopping'), which fall through to cancel.
                const reopened =
                  input.kind === "prompted"
                    ? yield* runRepo.reopen(sessionId)
                    : false;
                if (!reopened) {
                  yield* cancel(latest);
                  yield* runInputRepo.markProcessed(input.id);
                  break;
                }
                yield* Effect.logInfo("run.resumed").pipe(
                  Effect.annotateLogs({
                    event: "run.resumed",
                    sessionId,
                    inputId: input.id,
                  }),
                );
                const reopenedOption = yield* runRepo.get(sessionId);
                latest = Option.isSome(reopenedOption)
                  ? reopenedOption.value
                  : latest;
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
                const existingWorkspacePath =
                  Option.isSome(latest.repositoryId) &&
                  Option.isSome(latest.workspacePath)
                    ? latest.workspacePath
                    : Option.none<string>();
                if (Option.isSome(existingWorkspacePath)) {
                  // Resume path (mirrors the orphan retry): repository and
                  // workspace were resolved by an earlier attempt, and
                  // startWorker reattaches the persisted OMP session via
                  // --session when one was captured.
                  yield* runRepo.update(sessionId, {
                    state: "starting",
                    incrementAttempt: true,
                    nextAttemptAt: Option.none(),
                  });
                  const updatedOption = yield* runRepo.get(sessionId);
                  if (Option.isNone(updatedOption)) return;
                  worker = yield* startWorker(
                    updatedOption.value,
                    existingWorkspacePath.value,
                  );
                } else {
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
                  worker = yield* startWorker(
                    updatedOption.value,
                    workspacePath,
                  );
                }
                const agentInvoked = yield* worker.prompt(
                  linearWorkerPrompt(input.kind, input.body),
                );
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
