/**
 * Folds T3 thread stream items (snapshot + domain events) into normalized
 * AgentEvents. Snapshots and events go through the same transition logic, so
 * a resume that falls back to a snapshot (T3 does when more than 128 events
 * were missed) emits exactly the transitions this mapper hasn't emitted yet.
 * Event keys depend only on T3 entity ids, never on sequences, so the same
 * transition seen again in another mapper instance yields the same key.
 */
import { createHash } from "node:crypto";
import { Option, Schema } from "effect";
import type {
  AgentEvent,
  PlanStep,
  TurnOutcome,
  TurnStatus,
  UserQuestion,
} from "../driver.js";
import {
  ACTIVE_RUN_STATUSES,
  type ConversationMessage,
  ConversationMessage as ConversationMessageSchema,
  type Run,
  Run as RunSchema,
  type RuntimeRequest,
  RuntimeRequest as RuntimeRequestSchema,
  TERMINAL_ITEM_STATUSES,
  TERMINAL_RUN_STATUSES,
  type ThreadProjection,
  type ThreadStreamItem,
  TurnItemBase,
  TurnItemFields,
} from "./wire.js";

export interface RunView {
  readonly id: string;
  readonly ordinal: number;
  readonly status: string;
  readonly userMessageId: string;
  readonly queueHeld: boolean;
  readonly restartContinuationOfRunId: string | null;
  readonly finalText: string | null;
  readonly error: string | null;
}

interface ItemProgress {
  started: boolean;
  ended: boolean;
  planSignature: string | null;
}

interface RequestProgress {
  opened: boolean;
  resolved: boolean;
}

const TOOL_ITEM_TYPES: ReadonlySet<string> = new Set([
  "command_execution",
  "file_change",
  "file_search",
  "web_search",
  "dynamic_tool",
  "subagent",
]);

const APPROVAL_VALUES: Readonly<Record<string, string>> = {
  accept: "once",
  acceptForSession: "session",
  acceptAlways: "always",
  decline: "deny",
  cancel: "cancel",
};

const decodeOption =
  <A>(schema: Schema.Codec<A>) =>
  (value: unknown): Option.Option<A> =>
    Schema.decodeUnknownOption(schema)(value);

const decodeRun = decodeOption(RunSchema);
const decodeMessage = decodeOption(ConversationMessageSchema);
const decodeRequest = decodeOption(RuntimeRequestSchema);
const decodeItemBase = decodeOption(TurnItemBase);
const itemFields = <K extends keyof typeof TurnItemFields>(
  type: K,
  value: unknown,
) => Schema.decodeUnknownOption(TurnItemFields[type])(value);

const stringify = (value: unknown): string => {
  if (value === undefined || value === null) return "";
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

const planStatus = (status: string): PlanStep["status"] => {
  switch (status) {
    case "running":
      return "inProgress";
    case "completed":
      return "completed";
    case "cancelled":
    case "canceled":
      return "canceled";
    default:
      return "pending";
  }
};

const isTurnStatus = (status: string): status is TurnStatus =>
  TERMINAL_RUN_STATUSES.has(status);

export class T3EventMapper {
  private sequence: number | null;
  private readonly runs = new Map<string, RunView>();
  private readonly startedRuns = new Set<string>();
  private readonly endedRuns = new Set<string>();
  private readonly heldRuns = new Map<string, boolean>();
  private readonly items = new Map<string, ItemProgress>();
  private readonly requests = new Map<string, RequestProgress>();

  constructor(
    private readonly threadId: string,
    initialSequence: number | null = null,
  ) {
    this.sequence = initialSequence;
  }

  /** Last applied sequence: the `afterSequence` for a resume. */
  get cursor(): number | null {
    return this.sequence;
  }

  runViews(): ReadonlyMap<string, RunView> {
    return this.runs;
  }

  apply(item: ThreadStreamItem): ReadonlyArray<AgentEvent> {
    switch (item.kind) {
      case "synchronized":
        return [];
      case "snapshot":
        this.sequence = Math.max(this.sequence ?? 0, item.snapshotSequence);
        return this.reconcile(item.projection, String(item.snapshotSequence));
      case "event": {
        if (this.sequence !== null && item.sequence <= this.sequence) return [];
        this.sequence = item.sequence;
        return this.applyEvent(
          item.event.type,
          item.event.payload,
          String(item.sequence),
        );
      }
    }
  }

  private key(suffix: string): string {
    return `t3:${this.threadId}:${suffix}`;
  }

  private reconcile(
    projection: ThreadProjection,
    cursor: string,
  ): ReadonlyArray<AgentEvent> {
    const events: AgentEvent[] = [];
    const runs = [...projection.runs].sort((a, b) => a.ordinal - b.ordinal);
    for (const run of runs) events.push(...this.applyRun(run, cursor, false));
    for (const message of projection.messages) this.applyMessage(message);
    const runOrdinal = new Map(runs.map((run) => [run.id, run.ordinal]));
    const items = projection.turnItems
      .flatMap((raw) => {
        const base = decodeItemBase(raw);
        return Option.isSome(base) ? [{ base: base.value, raw }] : [];
      })
      .sort(
        (a, b) =>
          (runOrdinal.get(a.base.runId ?? "") ?? 0) -
            (runOrdinal.get(b.base.runId ?? "") ?? 0) ||
          a.base.ordinal - b.base.ordinal,
      );
    for (const { base, raw } of items) {
      events.push(...this.applyItem(base, raw, cursor));
    }
    for (const request of projection.runtimeRequests) {
      events.push(...this.applyRequest(request, cursor));
    }
    for (const run of runs) events.push(...this.applyRun(run, cursor, true));
    return events;
  }

  private applyEvent(
    type: string,
    payload: unknown,
    cursor: string,
  ): ReadonlyArray<AgentEvent> {
    switch (type) {
      case "run.created":
      case "run.updated":
        return Option.match(decodeRun(payload), {
          onNone: () => [],
          onSome: (run) => this.applyRun(run, cursor, true),
        });
      case "message.updated":
        Option.map(decodeMessage(payload), (message) =>
          this.applyMessage(message),
        );
        return [];
      case "turn-item.updated":
        return Option.match(decodeItemBase(payload), {
          onNone: () => [],
          onSome: (base) => this.applyItem(base, payload, cursor),
        });
      case "runtime-request.updated":
        return Option.match(decodeRequest(payload), {
          onNone: () => [],
          onSome: (request) => this.applyRequest(request, cursor),
        });
      default:
        // Unknown or uninteresting event: the cursor still advances.
        return [];
    }
  }

  private applyRun(
    run: Run,
    cursor: string,
    allowEnd: boolean,
  ): ReadonlyArray<AgentEvent> {
    const previous = this.runs.get(run.id);
    const view: RunView = {
      id: run.id,
      ordinal: run.ordinal,
      status: run.status,
      userMessageId: run.userMessageId,
      queueHeld: run.queueHeld === true,
      restartContinuationOfRunId: run.restartContinuationOfRunId ?? null,
      finalText: previous?.finalText ?? null,
      error: previous?.error ?? null,
    };
    const events: AgentEvent[] = [];
    if (
      ACTIVE_RUN_STATUSES.has(run.status) &&
      run.status !== "preparing" &&
      !this.startedRuns.has(run.id)
    ) {
      this.startedRuns.add(run.id);
      events.push({
        _tag: "TurnStarted",
        key: this.key(`run:${run.id}:started`),
        cursor,
        turnId: run.id,
      });
    }
    const held = view.queueHeld;
    if ((this.heldRuns.get(run.id) ?? false) !== held) {
      this.heldRuns.set(run.id, held);
      events.push({
        _tag: "QueueHeld",
        key: this.key(`run:${run.id}:queue-held:${held}`),
        cursor,
        turnId: run.id,
        held,
      });
    }
    // Commit the view before deciding the end, so finalText set by items in
    // the same snapshot is kept.
    this.runs.set(run.id, view);
    if (allowEnd && isTurnStatus(run.status) && !this.endedRuns.has(run.id)) {
      this.endedRuns.add(run.id);
      events.push({
        _tag: "TurnEnded",
        key: this.key(`run:${run.id}:ended`),
        cursor,
        outcome: this.outcomeOf(view, run.status),
      });
    }
    return events;
  }

  private outcomeOf(view: RunView, status: TurnStatus): TurnOutcome {
    return {
      turnId: view.id,
      status,
      finalText: view.finalText,
      error:
        view.error ??
        (status === "failed"
          ? "The T3 run failed without an error item"
          : null),
    };
  }

  private updateRun(runId: string | null, patch: Partial<RunView>): void {
    if (runId === null) return;
    const current = this.runs.get(runId);
    if (current === undefined) {
      this.runs.set(runId, {
        id: runId,
        ordinal: 0,
        status: "unknown",
        userMessageId: "",
        queueHeld: false,
        restartContinuationOfRunId: null,
        finalText: null,
        error: null,
        ...patch,
      });
      return;
    }
    this.runs.set(runId, { ...current, ...patch });
  }

  private applyMessage(message: ConversationMessage): void {
    if (
      message.role === "assistant" &&
      !message.streaming &&
      message.text.trim().length > 0
    ) {
      this.updateRun(message.runId, { finalText: message.text });
    }
  }

  private progress(itemId: string): ItemProgress {
    let progress = this.items.get(itemId);
    if (progress === undefined) {
      progress = { started: false, ended: false, planSignature: null };
      this.items.set(itemId, progress);
    }
    return progress;
  }

  private applyItem(
    base: TurnItemBase,
    raw: unknown,
    cursor: string,
  ): ReadonlyArray<AgentEvent> {
    const progress = this.progress(base.id);
    const turnId = base.runId;
    const terminal = TERMINAL_ITEM_STATUSES.has(base.status);
    const itemKey = (suffix: string) => this.key(`item:${base.id}:${suffix}`);

    if (TOOL_ITEM_TYPES.has(base.type)) {
      const tool = this.describeTool(base, raw);
      const events: AgentEvent[] = [];
      if (!progress.started) {
        progress.started = true;
        events.push({
          _tag: "ToolStarted",
          key: itemKey("started"),
          cursor,
          turnId,
          toolCallId: base.id,
          name: tool.name,
          input: tool.input,
        });
      }
      if (terminal && !progress.ended) {
        progress.ended = true;
        events.push({
          _tag: "ToolEnded",
          key: itemKey("ended"),
          cursor,
          turnId,
          toolCallId: base.id,
          name: tool.name,
          isError: base.status !== "completed" || tool.failed,
          result: tool.result,
        });
      }
      return events;
    }

    switch (base.type) {
      case "assistant_message":
        return Option.match(itemFields("assistant_message", raw), {
          onNone: () => [],
          onSome: (fields): ReadonlyArray<AgentEvent> => {
            if (fields.streaming || progress.ended) return [];
            if (fields.text.trim().length === 0) return [];
            progress.ended = true;
            this.updateRun(turnId, { finalText: fields.text });
            return [
              {
                _tag: "AssistantMessage",
                key: itemKey("message"),
                cursor,
                turnId,
                messageId: fields.messageId,
                text: fields.text,
              },
            ];
          },
        });
      case "reasoning":
      case "proposed_plan": {
        const fields =
          base.type === "reasoning"
            ? Option.map(itemFields("reasoning", raw), (f) => ({
                text: f.text,
                streaming: f.streaming,
              }))
            : Option.map(itemFields("proposed_plan", raw), (f) => ({
                text: f.markdown,
                streaming: f.streaming,
              }));
        return Option.match(fields, {
          onNone: () => [],
          onSome: (f): ReadonlyArray<AgentEvent> => {
            if (f.streaming || progress.ended || f.text.trim().length === 0) {
              return [];
            }
            progress.ended = true;
            return [
              {
                _tag: "Thinking",
                key: itemKey("thinking"),
                cursor,
                turnId,
                text: f.text,
              },
            ];
          },
        });
      }
      case "todo_list":
        return Option.match(itemFields("todo_list", raw), {
          onNone: () => [],
          onSome: (fields): ReadonlyArray<AgentEvent> => {
            const steps: PlanStep[] = fields.steps.map((step) => ({
              content: step.text,
              status: planStatus(step.status),
            }));
            const signature = createHash("sha256")
              .update(JSON.stringify(steps))
              .digest("hex")
              .slice(0, 16);
            if (progress.planSignature === signature) return [];
            progress.planSignature = signature;
            return [
              {
                _tag: "Plan",
                key: itemKey(`plan:${signature}`),
                cursor,
                turnId,
                steps,
              },
            ];
          },
        });
      case "error":
        return Option.match(itemFields("error", raw), {
          onNone: () => [],
          onSome: (fields): ReadonlyArray<AgentEvent> => {
            if (progress.ended) return [];
            progress.ended = true;
            this.updateRun(turnId, { error: fields.failure.message });
            return [
              {
                _tag: "Error",
                key: itemKey("error"),
                cursor,
                turnId,
                message: fields.failure.message,
                retryable: fields.failure.retryable === true,
              },
            ];
          },
        });
      case "approval_request":
        return Option.match(itemFields("approval_request", raw), {
          onNone: () => [],
          onSome: (fields) => {
            const offered =
              fields.options !== undefined && fields.options.length > 0
                ? fields.options
                : [
                    { decision: "accept", label: "Approve" },
                    { decision: "decline", label: "Deny" },
                  ];
            return this.openRequest(fields.requestId, terminal, cursor, {
              turnId,
              kind: "approval",
              prompt:
                fields.prompt ??
                fields.reason ??
                base.title ??
                `Approve ${fields.requestKind}?`,
              options: offered.flatMap((option) => {
                const value = APPROVAL_VALUES[option.decision];
                return value === undefined
                  ? []
                  : [{ label: option.label, value }];
              }),
              questions: [],
            });
          },
        });
      case "user_input_request":
        return Option.match(itemFields("user_input_request", raw), {
          onNone: () => [],
          onSome: (fields) => {
            const questions: UserQuestion[] = fields.questions.map((q) => ({
              id: q.id,
              header: q.header,
              question: q.question,
              options: q.options.map((option) => ({
                label: option.label,
                value: option.value ?? null,
              })),
              multiSelect: q.multiSelect === true,
              allowCustomAnswer: q.allowCustomAnswer !== false,
            }));
            const single = questions.length === 1 ? questions[0] : undefined;
            return this.openRequest(fields.requestId, terminal, cursor, {
              turnId,
              kind: "question",
              prompt: questions.map((q) => q.question).join("\n\n"),
              options:
                single === undefined
                  ? []
                  : single.options.map((option) => ({
                      label: option.label,
                      value: option.value ?? option.label,
                    })),
              questions,
            });
          },
        });
      default:
        return [];
    }
  }

  private openRequest(
    requestId: string,
    itemTerminal: boolean,
    cursor: string,
    body: Pick<
      Extract<AgentEvent, { _tag: "RequestOpened" }>,
      "turnId" | "kind" | "prompt" | "options" | "questions"
    >,
  ): ReadonlyArray<AgentEvent> {
    const progress = this.requests.get(requestId) ?? {
      opened: false,
      resolved: false,
    };
    this.requests.set(requestId, progress);
    if (progress.opened || progress.resolved || itemTerminal) return [];
    progress.opened = true;
    return [
      {
        _tag: "RequestOpened",
        key: this.key(`request:${requestId}:opened`),
        cursor,
        requestId,
        ...body,
      },
    ];
  }

  private applyRequest(
    request: RuntimeRequest,
    cursor: string,
  ): ReadonlyArray<AgentEvent> {
    const progress = this.requests.get(request.id);
    if (
      progress === undefined ||
      !progress.opened ||
      progress.resolved ||
      (request.status !== "resolved" &&
        request.status !== "expired" &&
        request.status !== "cancelled")
    ) {
      return [];
    }
    progress.resolved = true;
    return [
      {
        _tag: "RequestResolved",
        key: this.key(`request:${request.id}:resolved`),
        cursor,
        requestId: request.id,
        status: request.status,
      },
    ];
  }

  private describeTool(
    base: TurnItemBase,
    raw: unknown,
  ): {
    readonly name: string;
    readonly input: string;
    readonly result: string;
    readonly failed: boolean;
  } {
    const fallback = {
      name: base.title ?? base.type,
      input: "",
      result: base.status,
      failed: false,
    };
    switch (base.type) {
      case "command_execution":
        return Option.match(itemFields("command_execution", raw), {
          onNone: () => fallback,
          onSome: (f) => ({
            name: "command",
            input: f.input,
            result: f.output ?? base.status,
            failed:
              f.outputIndicatesFailure === true ||
              (f.exitCode !== undefined && f.exitCode !== 0),
          }),
        });
      case "file_change":
        return Option.match(itemFields("file_change", raw), {
          onNone: () => fallback,
          onSome: (f) => ({
            name: "file_change",
            input: f.fileName,
            result: `+${f.additions ?? 0} -${f.deletions ?? 0}`,
            failed: false,
          }),
        });
      case "file_search":
        return Option.match(itemFields("file_search", raw), {
          onNone: () => fallback,
          onSome: (f) => ({
            name: "file_search",
            input: f.pattern ?? "",
            result: base.status,
            failed: false,
          }),
        });
      case "web_search":
        return Option.match(itemFields("web_search", raw), {
          onNone: () => fallback,
          onSome: (f) => ({
            name: "web_search",
            input: (f.patterns ?? []).join(", "),
            result: base.status,
            failed: false,
          }),
        });
      case "dynamic_tool":
        return Option.match(itemFields("dynamic_tool", raw), {
          onNone: () => fallback,
          onSome: (f) => ({
            name: f.toolName ?? base.title ?? "tool",
            input: stringify(f.input),
            result: stringify(f.output) || base.status,
            failed: false,
          }),
        });
      case "subagent":
        return Option.match(itemFields("subagent", raw), {
          onNone: () => fallback,
          onSome: (f) => ({
            name: "subagent",
            input: f.prompt,
            result: f.result ?? base.status,
            failed: false,
          }),
        });
      default:
        return fallback;
    }
  }
}
