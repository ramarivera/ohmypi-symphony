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
import {
  type AgentRun,
  isDeferredNotificationStopPayload,
} from "../domain/models.js";
import { GatewayConfig } from "./config.js";
import { LinearGateway } from "./linear-gateway.js";
import { NixEnvironment } from "./nix-environment.js";
import { ActivityProjector } from "./projector.js";
import type {
  RpcEvent,
  RpcHostToolCall,
  RpcHostToolDefinition,
  RpcHostToolResult,
  RpcWorkerHandle,
} from "./rpc-worker.js";
import { RpcWorker } from "./rpc-worker.js";
import {
  InstallationRepo,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
  WorkspaceRepo,
} from "./store/repositories.js";
import {
  makeWorkspace,
  parseRepositorySuggestionCandidate,
  type RepositoryResolution,
} from "./workspace.js";

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
- Gateway-owned Linear tools are available: linear_get_issue(issueId) reads any issue visible to this installation; linear_create_comment(body) posts a comment to this run's issue; linear_update_issue(stateId?, delegateId?) updates this run's issue; linear_add_external_url(label, url) appends a URL to this run's agent session.
- Use linear_get_issue to inspect issue state/details, linear_create_comment for user-facing issue updates, linear_update_issue when changing workflow state or delegation, and linear_add_external_url for durable artifact links.
- Call linear_add_external_url immediately when you open or update a pull request, using label "Pull request" and the PR URL.
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
const PULL_REQUEST_URL_RE =
  /\bhttps:\/\/(?:github\.com\/[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\/pull\/[1-9]\d*(?![A-Za-z0-9_-])(?:[/?#][^\s<>"']*)?|gitlab\.com\/(?:[A-Za-z0-9_.-]+\/)+-\/merge_requests\/[1-9]\d*(?![A-Za-z0-9_-])(?:[/?#][^\s<>"']*)?)/gi;

export const extractPullRequestUrls = (
  text: string,
  maxCount = 16,
): ReadonlyArray<string> => {
  const urls = new Set<string>();
  for (const match of text.matchAll(PULL_REQUEST_URL_RE)) {
    const url = match[0]?.replace(/[.,;:!?)}\]]+$/g, "");
    if (url) urls.add(url);
    if (urls.size >= maxCount) break;
  }
  return [...urls];
};
export const assistantResponseTextFromRpcEvent = (event: RpcEvent): string => {
  const collect = (value: unknown): string => {
    if (typeof value === "string") return value;
    if (Array.isArray(value))
      return value.map(collect).filter(Boolean).join("\n");
    if (!record(value)) return "";
    if (typeof value.text === "string") return value.text;
    return collect(value.content ?? value.message ?? value.messages);
  };
  const candidates = [
    ...(Array.isArray(event.messages) ? event.messages : []),
    event.message,
  ];
  return candidates
    .filter((candidate) => !record(candidate) || candidate.role === "assistant")
    .map(collect)
    .filter(Boolean)
    .join("\n");
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
function isAutomationDelegated(payload: unknown): boolean {
  return record(payload) && payload.automationDelegated === true;
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
      LinearGateway.Default,
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
      const linearOption = yield* Effect.serviceOption(LinearGateway);

      const stopShouldApply = Effect.fn("SessionAuthority.stopShouldApply")(
        function* (
          sessionId: SessionId,
          input: { readonly kind: string; readonly payload: unknown },
        ): Effect.fn.Return<Option.Option<boolean>, never> {
          if (
            input.kind !== "stop" ||
            !isDeferredNotificationStopPayload(input.payload)
          ) {
            return Option.some(true);
          }
          const notification = record(input.payload)
            ? input.payload.notification
            : undefined;
          const issueId =
            record(notification) && typeof notification.issueId === "string"
              ? notification.issueId
              : null;
          if (issueId === null) return Option.some(false);
          if (Option.isNone(linearOption)) return Option.none();
          const linear = linearOption.value;
          return yield* linear.getIssue({ sessionId, issueId }).pipe(
            Effect.map((issue) => Option.some(issue.stateType === "canceled")),
            Effect.catchAll((error) =>
              Effect.gen(function* () {
                yield* Effect.logWarning(
                  "authority.notification_issue_fetch_failed",
                ).pipe(
                  Effect.annotateLogs({
                    event: "authority.notification_issue_fetch_failed",
                    sessionId,
                    issueId,
                    error: String(error),
                  }),
                );
                return Option.none<boolean>();
              }),
            ),
          );
        },
      );

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
      const ensureIssueLifecycle = (
        run: AgentRun,
        payload: unknown,
        installation: { readonly appUserId: string },
      ): Effect.Effect<void, never> =>
        Effect.gen(function* () {
          if (Option.isNone(linearOption)) return;
          const linear = linearOption.value;
          if (Option.isNone(run.issueId) || isAutomationDelegated(payload)) {
            return;
          }
          const runIssueId = run.issueId.value;
          const issue = yield* linear
            .getIssue({
              sessionId: run.sessionId,
              issueId: runIssueId,
            })
            .pipe(
              Effect.map(Option.some),
              Effect.catchAll((error) =>
                Effect.logWarning("issue.lifecycle_fetch_failed").pipe(
                  Effect.annotateLogs({
                    event: "issue.lifecycle_fetch_failed",
                    sessionId: run.sessionId,
                    issueId: runIssueId,
                    error:
                      error instanceof Error ? error.message : String(error),
                  }),
                  Effect.as(Option.none()),
                ),
              ),
            );
          if (Option.isNone(issue)) return;

          const stateType = issue.value.stateType?.toLowerCase();
          if (
            stateType !== "started" &&
            stateType !== "completed" &&
            stateType !== "canceled"
          ) {
            const teamId =
              issue.value.teamId ?? Option.getOrElse(run.teamId, () => null);
            if (teamId !== null) {
              const states = yield* linear
                .teamStartedStates({ sessionId: run.sessionId, teamId })
                .pipe(
                  Effect.catchAll((error) =>
                    Effect.logWarning("issue.lifecycle_states_failed").pipe(
                      Effect.annotateLogs({
                        event: "issue.lifecycle_states_failed",
                        sessionId: run.sessionId,
                        issueId: runIssueId,
                        error:
                          error instanceof Error
                            ? error.message
                            : String(error),
                      }),
                      Effect.as([]),
                    ),
                  ),
                );
              const started = states[0];
              if (started !== undefined) {
                yield* linear
                  .updateIssue({
                    sessionId: run.sessionId,
                    issueId: runIssueId,
                    stateId: started.id,
                  })
                  .pipe(
                    Effect.catchAll((error) =>
                      Effect.logWarning(
                        "issue.lifecycle_state_update_failed",
                      ).pipe(
                        Effect.annotateLogs({
                          event: "issue.lifecycle_state_update_failed",
                          sessionId: run.sessionId,
                          issueId: runIssueId,
                          error:
                            error instanceof Error
                              ? error.message
                              : String(error),
                        }),
                      ),
                    ),
                  );
              }
            }
          }

          if (
            issue.value.delegateId === null ||
            issue.value.delegateId === ""
          ) {
            yield* linear
              .updateIssue({
                sessionId: run.sessionId,
                issueId: runIssueId,
                delegateId: installation.appUserId,
              })
              .pipe(
                Effect.catchAll((error) =>
                  Effect.logWarning(
                    "issue.lifecycle_delegate_update_failed",
                  ).pipe(
                    Effect.annotateLogs({
                      event: "issue.lifecycle_delegate_update_failed",
                      sessionId: run.sessionId,
                      issueId: runIssueId,
                      error:
                        error instanceof Error ? error.message : String(error),
                    }),
                  ),
                ),
              );
          }
        });

      const augmentResolution = (
        run: AgentRun,
        context: InputContext,
        resolution: RepositoryResolution,
      ): Effect.Effect<
        {
          readonly resolution: RepositoryResolution;
          readonly options: ReadonlyArray<string>;
        },
        DatabaseError | RowDecodeError
      > =>
        Effect.gen(function* () {
          if (Option.isNone(linearOption)) return { resolution, options: [] };
          const linear = linearOption.value;
          if (
            resolution.kind === "match" ||
            Option.isNone(run.issueId) ||
            context.organizationId === null
          ) {
            return { resolution, options: [] };
          }
          const runIssueId = run.issueId.value;
          const repositories = yield* workspaceRepo.listRepositories(
            run.organizationId,
          );
          if (repositories.length === 0) return { resolution, options: [] };
          const byKey = new Map<string, (typeof repositories)[number] | null>();
          const candidates = repositories.flatMap((repository) => {
            const candidate = parseRepositorySuggestionCandidate(
              repository.url,
            );
            if (candidate === null) return [];
            const key =
              `${candidate.hostname}/${candidate.repositoryFullName}`.toLowerCase();
            if (byKey.has(key)) {
              byKey.set(key, null);
            } else {
              byKey.set(key, repository);
            }
            return [candidate];
          });
          if (candidates.length === 0) return { resolution, options: [] };
          const suggestions = yield* linear
            .repositorySuggestions({
              sessionId: run.sessionId,
              issueId: runIssueId,
              candidates,
            })
            .pipe(
              Effect.catchAll((error) =>
                Effect.logDebug("repository.suggestions_failed").pipe(
                  Effect.annotateLogs({
                    event: "repository.suggestions_failed",
                    sessionId: run.sessionId,
                    issueId: runIssueId,
                    error:
                      error instanceof Error ? error.message : String(error),
                  }),
                  Effect.as([]),
                ),
              ),
            );
          const ranked = suggestions
            .slice()
            .sort((a, b) => b.confidence - a.confidence);
          const mapped: Array<{
            readonly repository: (typeof repositories)[number];
            readonly confidence: number;
          }> = [];
          for (const suggestion of ranked) {
            const key =
              `${suggestion.hostname}/${suggestion.repositoryFullName}`.toLowerCase();
            const only = byKey.get(key) ?? undefined;
            if (
              only !== undefined &&
              !mapped.some((item) => item.repository.id === only.id)
            ) {
              mapped.push({
                repository: only,
                confidence: suggestion.confidence,
              });
            }
          }
          const firstMatch = mapped[0];
          if (
            mapped.length === 1 &&
            firstMatch !== undefined &&
            firstMatch.confidence >=
              config.repositorySuggestionConfidenceThreshold
          ) {
            yield* Effect.logInfo("repository.suggestion_auto_routed").pipe(
              Effect.annotateLogs({
                event: "repository.suggestion_auto_routed",
                sessionId: run.sessionId,
                issueId: run.issueId.value,
                repositoryId: firstMatch.repository.id,
                confidence: firstMatch.confidence,
              }),
            );
            return {
              resolution: { kind: "match", repository: firstMatch.repository },
              options: [],
            };
          }
          const options = mapped.map((item) => item.repository.id);
          if (resolution.kind === "ambiguous" && options.length > 0) {
            const staticById = new Map(
              resolution.repositories.map((repository) => [
                repository.id,
                repository,
              ]),
            );
            const ordered = [
              ...mapped
                .map((item) => staticById.get(item.repository.id))
                .filter(
                  (repository): repository is (typeof repositories)[number] =>
                    repository !== undefined,
                ),
              ...resolution.repositories.filter(
                (repository) => !options.includes(repository.id),
              ),
            ];
            return {
              resolution: { kind: "ambiguous", repositories: ordered },
              options,
            };
          }
          return { resolution, options };
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
      const reportedPullRequestUrlsRef = yield* Ref.make<
        ReadonlyMap<SessionId, ReadonlySet<string>>
      >(new Map());
      const stopDeferralCountsRef = yield* Ref.make<
        ReadonlyMap<SessionId, ReadonlyMap<string, number>>
      >(new Map());
      // Serializes cancel() with Linear host-tool mutations per session:
      // the desiredState check and the Linear call happen inside the same
      // gate, so a stop landing mid-dispatch is either seen (mutation
      // refused) or blocks until the in-flight mutation completes.
      const sessionMutationGatesRef = yield* Ref.make<
        ReadonlyMap<SessionId, Effect.Semaphore>
      >(new Map());
      const sessionMutationGate = (
        sessionId: SessionId,
      ): Effect.Effect<Effect.Semaphore, never, never> =>
        Ref.modify(sessionMutationGatesRef, (gates) => {
          const existing = gates.get(sessionId);
          if (existing !== undefined) return [existing, gates];
          const created = Effect.unsafeMakeSemaphore(1);
          const next = new Map(gates);
          next.set(sessionId, created);
          return [created, next];
        });
      const withSessionMutationGate = <A, E, R>(
        sessionId: SessionId,
        effect: Effect.Effect<A, E, R>,
      ): Effect.Effect<A, E, R> =>
        Effect.flatMap(sessionMutationGate(sessionId), (gate) =>
          gate.withPermits(1)(effect),
        );
      const recordStopDeferral = (
        sessionId: SessionId,
        inputId: string,
      ): Effect.Effect<number, never, never> =>
        Ref.modify(stopDeferralCountsRef, (counts) => {
          const next = new Map(counts);
          const sessionCounts = new Map(next.get(sessionId) ?? []);
          const count = (sessionCounts.get(inputId) ?? 0) + 1;
          sessionCounts.set(inputId, count);
          next.set(sessionId, sessionCounts);
          return [count, next];
        });
      const clearSessionStopDeferrals = (
        sessionId: SessionId,
      ): Effect.Effect<void, never, never> =>
        Ref.update(stopDeferralCountsRef, (counts) => {
          const next = new Map(counts);
          next.delete(sessionId);
          return next;
        });

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
        // Share the mutation gate with host-tool dispatch: cancellation
        // either lands before a mutation's in-gate re-read (mutation
        // refused) or waits for the in-flight mutation to complete.
        yield* withSessionMutationGate(run.sessionId, Effect.void);
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
          const finalText = assistantResponseTextFromRpcEvent(event);
          const discovered = extractPullRequestUrls(finalText);
          if (discovered.length > 0 && Option.isSome(linearOption)) {
            const existing = yield* Ref.get(reportedPullRequestUrlsRef);
            const already = existing.get(sessionId) ?? new Set<string>();
            const fresh = discovered.filter((url) => !already.has(url));
            if (fresh.length > 0) {
              const nextSet = new Set(already);
              for (const url of fresh) nextSet.add(url);
              yield* Ref.update(reportedPullRequestUrlsRef, (current) => {
                const next = new Map(current);
                next.set(sessionId, nextSet);
                return next;
              });
              yield* linearOption.value
                .addSessionExternalUrls({
                  sessionId,
                  urls: fresh.map((url) => ({ label: "Pull request", url })),
                })
                .pipe(
                  Effect.catchAll((error) =>
                    Effect.logWarning("pull_request_url.report_failed").pipe(
                      Effect.annotateLogs({
                        event: "pull_request_url.report_failed",
                        sessionId,
                        count: fresh.length,
                        error: String(error),
                      }),
                    ),
                  ),
                );
            }
          }
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
                yield* Ref.update(reportedPullRequestUrlsRef, (reported) => {
                  const next = new Map(reported);
                  next.delete(sessionId);
                  return next;
                });
                yield* clearSessionStopDeferrals(sessionId);
                yield* Ref.update(sessionMutationGatesRef, (gates) => {
                  const next = new Map(gates);
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

      const registerLinearHostTools = Effect.fn(
        "SessionAuthority.registerLinearHostTools",
      )(function* (
        run: AgentRun,
        worker: RpcWorkerHandle,
      ): Effect.fn.Return<void, RpcProtocolError> {
        if (
          typeof worker.onHostToolCall !== "function" ||
          typeof worker.setHostTools !== "function"
        ) {
          return;
        }
        const textResult = (
          text: string,
          details?: unknown,
          isError = false,
        ): RpcHostToolResult => ({
          content: [{ type: "text", text }],
          ...(details === undefined ? {} : { details }),
          ...(isError ? { isError: true } : {}),
        });
        const definitions: ReadonlyArray<RpcHostToolDefinition> = [
          {
            name: "linear_get_issue",
            label: "Linear: get issue",
            description:
              "Read a Linear issue visible to this installation, including state, team, and labels.",
            parameters: {
              type: "object",
              additionalProperties: false,
              required: ["issueId"],
              properties: { issueId: { type: "string", minLength: 1 } },
            },
          },
          {
            name: "linear_create_comment",
            label: "Linear: create comment",
            description: "Post a comment to the Linear issue for this run.",
            parameters: {
              type: "object",
              additionalProperties: false,
              required: ["body"],
              properties: { body: { type: "string", minLength: 1 } },
            },
          },
          {
            name: "linear_update_issue",
            label: "Linear: update issue",
            description:
              "Update the workflow state or delegation of this run's issue.",
            parameters: {
              type: "object",
              additionalProperties: false,
              properties: {
                stateId: { type: "string", minLength: 1 },
                delegateId: { type: ["string", "null"] },
              },
              anyOf: [{ required: ["stateId"] }, { required: ["delegateId"] }],
            },
          },
          {
            name: "linear_add_external_url",
            label: "Linear: add external URL",
            description:
              "Append an http(s) artifact URL to this run's Linear session.",
            parameters: {
              type: "object",
              additionalProperties: false,
              required: ["label", "url"],
              properties: {
                label: { type: "string", minLength: 1 },
                url: { type: "string", pattern: "^https?://" },
              },
            },
          },
        ];

        yield* worker.onHostToolCall((request: RpcHostToolCall) =>
          Effect.gen(function* () {
            const fail = (message: string) =>
              textResult(message, undefined, true);
            if (Option.isNone(linearOption)) {
              return fail("Linear gateway is unavailable");
            }
            const raw = request.arguments;
            const current = yield* runRepo
              .get(run.sessionId)
              .pipe(
                Effect.catchAll(() => Effect.succeed(Option.none<AgentRun>())),
              );
            if (Option.isNone(current)) return fail("Run no longer exists");
            const currentRun = current.value;
            const issueId = Option.isSome(currentRun.issueId)
              ? currentRun.issueId.value
              : null;
            const mutating =
              request.toolName === "linear_create_comment" ||
              request.toolName === "linear_update_issue" ||
              request.toolName === "linear_add_external_url";
            const linear = linearOption.value;
            const dispatch: Effect.Effect<RpcHostToolResult, never, never> =
              Effect.gen(function* () {
                switch (request.toolName) {
                  case "linear_get_issue": {
                    const requested = raw.issueId;
                    if (
                      typeof requested !== "string" ||
                      requested.trim().length === 0
                    )
                      return fail("issueId must be a non-empty string");
                    return yield* linear
                      .getIssue({
                        sessionId: run.sessionId,
                        issueId: requested.trim(),
                      })
                      .pipe(
                        Effect.map((issue) =>
                          textResult(JSON.stringify(issue), issue),
                        ),
                        Effect.catchAll((error) =>
                          Effect.succeed(
                            fail(`Unable to read issue: ${String(error)}`),
                          ),
                        ),
                      );
                  }
                  case "linear_create_comment": {
                    if (issueId === null)
                      return fail("Run has no Linear issue");
                    const body = raw.body;
                    if (typeof body !== "string" || body.trim().length === 0)
                      return fail("body must be a non-empty string");
                    return yield* linear
                      .createIssueComment({ sessionId: run.sessionId, body })
                      .pipe(
                        Effect.map((commentId) =>
                          textResult(`Created Linear comment ${commentId}`, {
                            commentId,
                          }),
                        ),
                        Effect.catchAll((error) =>
                          Effect.succeed(
                            fail(`Unable to create comment: ${String(error)}`),
                          ),
                        ),
                      );
                  }
                  case "linear_update_issue": {
                    if (issueId === null)
                      return fail("Run has no Linear issue");
                    const stateId = raw.stateId;
                    const delegateId = raw.delegateId;
                    if (
                      stateId !== undefined &&
                      (typeof stateId !== "string" ||
                        stateId.trim().length === 0)
                    )
                      return fail("stateId must be a non-empty string");
                    if (
                      delegateId !== undefined &&
                      delegateId !== null &&
                      (typeof delegateId !== "string" ||
                        delegateId.trim().length === 0)
                    )
                      return fail(
                        "delegateId must be a non-empty string or null",
                      );
                    if (stateId === undefined && delegateId === undefined)
                      return fail("Provide stateId and/or delegateId");
                    return yield* linear
                      .updateIssue({
                        sessionId: run.sessionId,
                        issueId,
                        ...(stateId === undefined ? {} : { stateId }),
                        ...(delegateId === undefined ? {} : { delegateId }),
                      })
                      .pipe(
                        Effect.map(() => textResult("Updated Linear issue")),
                        Effect.catchAll((error) =>
                          Effect.succeed(
                            fail(`Unable to update issue: ${String(error)}`),
                          ),
                        ),
                      );
                  }
                  case "linear_add_external_url": {
                    if (issueId === null)
                      return fail("Run has no Linear issue");
                    const label = raw.label;
                    const url = raw.url;
                    if (typeof label !== "string" || label.trim().length === 0)
                      return fail("label must be a non-empty string");
                    if (typeof url !== "string")
                      return fail("url must be an http(s) URL");
                    try {
                      const parsed = new URL(url);
                      if (
                        parsed.protocol !== "http:" &&
                        parsed.protocol !== "https:"
                      )
                        return fail("url must be an http(s) URL");
                    } catch {
                      return fail("url must be a valid http(s) URL");
                    }
                    return yield* linear
                      .addSessionExternalUrls({
                        sessionId: run.sessionId,
                        urls: [{ label: label.trim(), url }],
                      })
                      .pipe(
                        Effect.tap(() =>
                          Ref.update(reportedPullRequestUrlsRef, (current) => {
                            const next = new Map(current);
                            const urls = new Set(next.get(run.sessionId) ?? []);
                            urls.add(url);
                            next.set(run.sessionId, urls);
                            return next;
                          }),
                        ),
                        Effect.map(() =>
                          textResult("Added external URL to Linear session"),
                        ),
                        Effect.catchAll((error) =>
                          Effect.succeed(
                            fail(
                              `Unable to add external URL: ${String(error)}`,
                            ),
                          ),
                        ),
                      );
                  }
                  default:
                    return fail(`Unknown host tool: ${request.toolName}`);
                }
              });
            if (!mutating) return yield* dispatch;
            // Serialize with cancel(): re-read the run inside the gate
            // immediately before any Linear mutation.
            return yield* withSessionMutationGate(
              run.sessionId,
              Effect.gen(function* () {
                const fresh = yield* runRepo
                  .get(run.sessionId)
                  .pipe(
                    Effect.catchAll(() =>
                      Effect.succeed(Option.none<AgentRun>()),
                    ),
                  );
                if (Option.isNone(fresh)) return fail("Run no longer exists");
                if (fresh.value.desiredState === "canceled") {
                  return fail(
                    "Run is canceled; mutating Linear tools are refused",
                  );
                }
                return yield* dispatch;
              }),
            );
          }).pipe(
            Effect.catchAll((error) =>
              Effect.succeed({
                content: [
                  { type: "text", text: `Host tool failed: ${String(error)}` },
                ],
                isError: true,
              }),
            ),
          ),
        );
        yield* worker.setHostTools(definitions).pipe(
          Effect.catchTag("@Gateway/RpcProtocolError", (error) =>
            Effect.logWarning("linear.host_tools_registration_failed").pipe(
              Effect.annotateLogs({
                event: "linear.host_tools_registration_failed",
                sessionId: run.sessionId,
                error: error.message,
              }),
            ),
          ),
        );
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
        yield* registerLinearHostTools(run, worker);
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
                const retryPayload = yield* runInputRepo
                  .latestActionableInput(sessionId)
                  .pipe(
                    Effect.map((input) =>
                      Option.isSome(input) ? input.value.payload : null,
                    ),
                  );
                yield* ensureIssueLifecycle(
                  resumed,
                  retryPayload,
                  installation.value,
                );
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
              let stopDecision = yield* stopShouldApply(sessionId, input);
              if (Option.isNone(stopDecision)) {
                const deferralCount = yield* recordStopDeferral(
                  sessionId,
                  input.id,
                );
                if (deferralCount < 10) return;
                yield* Effect.logWarning(
                  "authority.stop_deferral_bound_reached",
                ).pipe(
                  Effect.annotateLogs({
                    event: "authority.stop_deferral_bound_reached",
                    sessionId,
                    inputId: input.id,
                    deferralCount,
                  }),
                );
                stopDecision = Option.some(true);
              }
              const shouldStop = Option.getOrElse(stopDecision, () => true);
              if (!shouldStop) {
                yield* runInputRepo.markProcessed(input.id);
                continue;
              }
              if (
                input.kind === "stop" &&
                isDeferredNotificationStopPayload(input.payload)
              ) {
                yield* runInputRepo.applyStop(sessionId);
                const refreshed = yield* runRepo.get(sessionId);
                if (Option.isSome(refreshed)) latest = refreshed.value;
              }
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
                  yield* ensureIssueLifecycle(
                    updatedOption.value,
                    input.payload,
                    installation.value,
                  );
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
                  const staticResolution = yield* workspace.resolve(context);
                  const augmented = yield* augmentResolution(
                    run,
                    context,
                    staticResolution,
                  );
                  const resolution = augmented.resolution;
                  if (resolution.kind === "none") {
                    yield* projector.elicitation(
                      sessionId,
                      `repo:none:${input.id}`,
                      "No repository is configured for this Linear issue.",
                      augmented.options.length > 0
                        ? augmented.options
                        : undefined,
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
                  yield* ensureIssueLifecycle(
                    updatedOption.value,
                    input.payload,
                    installation.value,
                  );
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
          }).pipe(
            // NOTE: stop-deferral counts are NOT cleared here — they must
            // survive across processSession invocations for the deferral
            // bound to trip. They are cleared in the terminal agent_end
            // cleanup block instead.
            Effect.ensuring(releaseIfNoWorker(sessionId)),
          );
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
          yield* Ref.set(reportedPullRequestUrlsRef, new Map());
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
