/**
 * Agent driver contract: one interface for every agent runtime the gateway
 * can put behind a Linear agent session (omp over stdio RPC, T3 Code over
 * WebSocket RPC, ...).
 *
 * Vocabulary: a *session* is the durable conversation (omp session file, T3
 * thread); a *turn* is one unit of work inside it (omp agent run, T3 run).
 * Interrupt, outcome and adoption all work on turns; resume works on sessions.
 */
import { type Effect, Schema, type Scope, type Stream } from "effect";
import type { RpcHostToolDefinition } from "../services/rpc-worker.js";

export const DRIVER_KINDS = ["omp", "t3"] as const;
export type DriverKind = (typeof DRIVER_KINDS)[number];

/** Persisted by the session authority; enough to re-attach after a restart. */
export interface SessionRef {
  readonly driver: DriverKind;
  /** omp: session file path. t3: thread id. */
  readonly nativeId: string;
  /** Driver-specific, opaque to the authority (t3: project id, origin). */
  readonly locator: Readonly<Record<string, string>>;
}

export interface DriverCapabilities {
  /** Can inject text into the active turn. */
  readonly steer: boolean;
  /** Can queue a turn behind the active one natively. */
  readonly queue: boolean;
  /**
   * How the gateway's host tools (linear_*) reach the agent: `inline` means
   * the driver accepts `OpenSessionInput.hostTools` and routes calls back to
   * the gateway; `mcp` means the tools must be served as an MCP server the
   * agent's provider loads; `none` means the agent cannot call them.
   */
  readonly hostTools: "inline" | "mcp" | "none";
  /** Events carry a cursor and `open({ resume: { cursor } })` replays from it. */
  readonly resumableStream: boolean;
  /** Emits approval `RequestOpened` events that `respond` can decide. */
  readonly approvals: boolean;
  /** Emits question `RequestOpened` events that `respond` can answer. */
  readonly questions: boolean;
  /** Who materializes the workspace: the gateway (cwd) or the driver. */
  readonly workspaces: "gateway" | "driver";
}

export const RUNTIME_MODES = [
  "approval-required",
  "auto-accept-edits",
  "auto",
  "full-access",
] as const;
export type RuntimeMode = (typeof RUNTIME_MODES)[number];

export type WorkspaceSpec =
  /** An existing directory (t3: a git worktree of the configured project). */
  | { readonly _tag: "Directory"; readonly path: string }
  /** The project's root checkout (t3 only). */
  | { readonly _tag: "ProjectRoot"; readonly branch?: string }
  /** Let the driver create a worktree from `baseRef` (t3 only). */
  | {
      readonly _tag: "DriverWorktree";
      readonly baseRef: string;
      readonly branch?: string;
    };

export interface OpenSessionInput {
  /**
   * Stable key for this agent session (the Linear agent session id). Drivers
   * derive native ids from it so a retried `open` re-attaches instead of
   * creating a second conversation.
   */
  readonly sessionKey: string;
  readonly title: string;
  readonly workspace: WorkspaceSpec;
  /** Driver-scoped project override (t3: ProjectId). */
  readonly project?: string;
  /** Overrides the driver's configured runtime mode. */
  readonly runtimeMode?: RuntimeMode;
  /** Ignored unless `capabilities.hostTools === "inline"`. */
  readonly hostTools?: ReadonlyArray<RpcHostToolDefinition>;
  /** Re-attach from a persisted ref; `cursor` is the last projected event's. */
  readonly resume?: { readonly ref: SessionRef; readonly cursor?: string };
}

/**
 * `auto` starts a turn when the session is idle and queues it otherwise; it
 * never steers or interrupts on its own. `steer` falls back to starting a
 * turn when nothing is active. `start` on a busy session is rejected.
 */
export type Delivery = "auto" | "start" | "queue" | "steer";

export interface TurnInput {
  /**
   * Stable per logical input (Linear webhook delivery / activity id) and
   * reused verbatim on retry. Drivers derive their idempotency keys from it.
   */
  readonly clientRequestId: string;
  readonly text: string;
  readonly delivery: Delivery;
}

export const TERMINAL_TURN_STATUSES = [
  "completed",
  "failed",
  "interrupted",
  "cancelled",
  "rolled_back",
] as const;
export type TurnStatus = (typeof TERMINAL_TURN_STATUSES)[number];

export interface TurnOutcome {
  readonly turnId: string;
  readonly status: TurnStatus;
  /** Last completed assistant message of the turn, when there was one. */
  readonly finalText: string | null;
  readonly error: string | null;
}

export interface TurnHandle {
  readonly clientRequestId: string;
  /** The unit of work that carries this input (t3 run id). */
  readonly turnId: string;
  readonly delivery: "started" | "queued" | "steered";
  /**
   * True when the driver found the input already accepted (a retry after a
   * lost response or a gateway restart) and attached to it instead of
   * submitting it again.
   */
  readonly adopted: boolean;
  /** Completes on the terminal state of this turn, following restarts. */
  readonly outcome: Effect.Effect<TurnOutcome, DriverError>;
}

export interface UserQuestion {
  readonly id: string;
  readonly header: string;
  readonly question: string;
  readonly options: ReadonlyArray<{
    readonly label: string;
    readonly value: string | null;
  }>;
  readonly multiSelect: boolean;
  readonly allowCustomAnswer: boolean;
}

/** Same shape as the projector's `LinearPlanItem`. */
export interface PlanStep {
  readonly content: string;
  readonly status: "pending" | "inProgress" | "completed" | "canceled";
}

interface EventBase {
  /**
   * Stable identity of this observation. The same transition observed again
   * (snapshot after reconnect, re-attach) yields the same key, so consumers
   * can dedupe by key (the projector's source keys).
   */
  readonly key: string;
  /** Resume cursor after this event, or null when the driver has none. */
  readonly cursor: string | null;
}

/**
 * Normalized events. Mapping onto Linear activities: TurnStarted/Thinking →
 * thought, ToolStarted/ToolEnded → action, AssistantMessage → response draft,
 * Plan → plan, RequestOpened → elicitation, Error → error, TurnEnded →
 * terminal response or error.
 */
export type AgentEvent =
  | (EventBase & { readonly _tag: "TurnStarted"; readonly turnId: string })
  | (EventBase & {
      readonly _tag: "Thinking";
      readonly turnId: string | null;
      readonly text: string;
    })
  | (EventBase & {
      readonly _tag: "ToolStarted";
      readonly turnId: string | null;
      readonly toolCallId: string;
      readonly name: string;
      readonly input: string;
    })
  | (EventBase & {
      readonly _tag: "ToolEnded";
      readonly turnId: string | null;
      readonly toolCallId: string;
      readonly name: string;
      readonly isError: boolean;
      readonly result: string;
    })
  | (EventBase & {
      readonly _tag: "AssistantMessage";
      readonly turnId: string | null;
      readonly messageId: string;
      readonly text: string;
    })
  | (EventBase & {
      readonly _tag: "Plan";
      readonly turnId: string | null;
      readonly steps: ReadonlyArray<PlanStep>;
    })
  | (EventBase & {
      readonly _tag: "RequestOpened";
      readonly turnId: string | null;
      readonly requestId: string;
      readonly kind: "approval" | "question";
      readonly prompt: string;
      /**
       * Approval choices (value = approval scope or "deny"), or the single
       * question's options. Empty for multi-question forms; use `questions`.
       */
      readonly options: ReadonlyArray<{
        readonly label: string;
        readonly value: string;
      }>;
      readonly questions: ReadonlyArray<UserQuestion>;
    })
  | (EventBase & {
      readonly _tag: "RequestResolved";
      readonly requestId: string;
      readonly status: "resolved" | "expired" | "cancelled";
    })
  | (EventBase & {
      /** Non-terminal failure inside a turn (provider error, retry). */
      readonly _tag: "Error";
      readonly turnId: string | null;
      readonly message: string;
      readonly retryable: boolean;
    })
  | (EventBase & {
      /** The queue behind the active turn stopped advancing (t3 queueHeld). */
      readonly _tag: "QueueHeld";
      readonly turnId: string;
      readonly held: boolean;
    })
  | (EventBase & { readonly _tag: "TurnEnded"; readonly outcome: TurnOutcome });

export type RequestResponse =
  | {
      readonly _tag: "Approve";
      readonly scope: "once" | "session" | "always";
    }
  | { readonly _tag: "Deny" }
  | { readonly _tag: "Cancel" }
  | {
      readonly _tag: "Answer";
      readonly answers: Readonly<Record<string, unknown>>;
    }
  /** Close a question without answering it. */
  | { readonly _tag: "Dismiss" };

export interface SessionTurnSummary {
  readonly turnId: string;
  readonly status: string;
  /** Set when the turn carries an input this gateway submitted. */
  readonly clientRequestId: string | null;
}

/** Authoritative state for reconcile: resend vs adopt, pending requests. */
export interface SessionSnapshot {
  readonly activeTurnId: string | null;
  readonly turns: ReadonlyArray<SessionTurnSummary>;
  readonly queueHeld: boolean;
  readonly pendingRequests: ReadonlyArray<{
    readonly requestId: string;
    readonly kind: "approval" | "question";
  }>;
  /** Inputs the driver can prove it already accepted: adopt, don't resend. */
  readonly acceptedClientRequestIds: ReadonlyArray<string>;
  readonly cursor: string | null;
}

export interface AgentSession {
  readonly ref: SessionRef;
  readonly capabilities: DriverCapabilities;
  /**
   * The session's single ordered event stream. Drivers reconnect and resume
   * internally; the stream fails with DriverError only when they give up.
   * Single consumer: events are buffered until read.
   */
  readonly events: Stream.Stream<AgentEvent, DriverError>;
  readonly sendTurn: (
    input: TurnInput,
  ) => Effect.Effect<TurnHandle, DriverError>;
  /** Interrupt one turn; omitting turnId targets the active turn. */
  readonly interrupt: (
    turnId?: string,
  ) => Effect.Effect<"requested" | "no_active_turn", DriverError>;
  readonly respond: (
    requestId: string,
    response: RequestResponse,
  ) => Effect.Effect<void, DriverError>;
  readonly snapshot: Effect.Effect<SessionSnapshot, DriverError>;
}

export interface AgentDriver {
  readonly kind: DriverKind;
  readonly capabilities: DriverCapabilities;
  /** Cheap reachability, protocol and credential check. */
  readonly probe: Effect.Effect<{ readonly detail: string }, DriverError>;
  /**
   * Creates or re-attaches the session in the caller's scope. Closing the
   * scope detaches (t3) or stops the process (omp); it never deletes the
   * conversation.
   */
  readonly open: (
    input: OpenSessionInput,
  ) => Effect.Effect<AgentSession, DriverError, Scope.Scope>;
}

export const DriverErrorReason = Schema.Literal(
  "unavailable",
  "auth",
  "protocol",
  "timeout",
  "rejected",
  "unsupported",
  "lost",
);
export type DriverErrorReason = typeof DriverErrorReason.Type;

/**
 * `submission` answers "could the side effect have happened?" so callers know
 * whether a blind retry might duplicate work: `not-submitted` is safe to retry,
 * `possibly-submitted` must reconcile through `snapshot` first, `submitted`
 * means the work exists and only the follow-up failed.
 */
export const DriverSubmission = Schema.Literal(
  "not-submitted",
  "possibly-submitted",
  "submitted",
);
export type DriverSubmission = typeof DriverSubmission.Type;

export class DriverError extends Schema.TaggedError<DriverError>()(
  "@Gateway/DriverError",
  {
    driver: Schema.Literal(...DRIVER_KINDS),
    operation: Schema.String,
    reason: DriverErrorReason,
    submission: DriverSubmission,
    message: Schema.String,
  },
) {}
