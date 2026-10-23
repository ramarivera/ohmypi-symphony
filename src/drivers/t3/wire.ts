/**
 * Minimal vendored copy of the T3 Code orchestration V2 wire contract.
 *
 * Source: pingdotgg/t3code tag v0.1.2-nightly.20261021.3473 (commit
 * 1688f965682d331da16a8c39f049592361f69c8d, the server version running on
 * arch-cachy), files packages/contracts/src/{orchestrationV2,environment,
 * environmentHttp,auth,providerPolicy,modelSelection,model}.ts. The command and
 * input shapes used here are unchanged in tag v0.1.2 (044f0725).
 *
 * Deliberately narrowed: decoders keep only the fields the driver reads,
 * ignore excess properties, and keep enum-like fields as strings so a newer
 * server's additions degrade to "unknown" instead of failing the stream.
 * Unknown event and turn-item types are skipped by the mapper, matching T3's
 * own forward-compatible client decoding.
 */
import { Schema } from "effect";

export const ORCHESTRATION_PROTOCOL_VERSION = 2;
/** `ORCHESTRATION_PROTOCOL_QUERY_PARAM`, environment.ts:14. */
export const ORCHESTRATION_PROTOCOL_QUERY_PARAM = "orchestrationProtocol";
/** `WEBSOCKET_TICKET_QUERY_PARAM`, apps/server/src/auth/EnvironmentAuth.ts. */
export const WEBSOCKET_TICKET_QUERY_PARAM = "wsTicket";
export const WEBSOCKET_TICKET_PATH = "/api/auth/websocket-ticket";
export const ENVIRONMENT_DESCRIPTOR_PATH = "/.well-known/t3/environment";
export const WEBSOCKET_PATH = "/ws";

/** `ORCHESTRATION_V2_WS_METHODS`, orchestrationV2.ts:3074. */
export const T3_METHODS = {
  dispatchCommand: "orchestration.dispatchCommand",
  getThreadProjection: "orchestration.getThreadProjection",
  launchThread: "orchestration.launchThread",
  subscribeThread: "orchestration.subscribeThread",
} as const;

/** `OrchestrationV2RunStatus`, orchestrationV2.ts:452. */
export const ACTIVE_RUN_STATUSES: ReadonlySet<string> = new Set([
  "preparing",
  "starting",
  "running",
  "waiting",
]);
export const TERMINAL_RUN_STATUSES: ReadonlySet<string> = new Set([
  "completed",
  "interrupted",
  "failed",
  "cancelled",
  "rolled_back",
]);
/** `OrchestrationV2TurnItemStatus`, orchestrationV2.ts:1217. */
export const TERMINAL_ITEM_STATUSES: ReadonlySet<string> = new Set([
  "completed",
  "failed",
  "cancelled",
  "interrupted",
]);

// ---------------------------------------------------------------------------
// HTTP control plane

/** `AuthWebSocketTicketResult`, auth.ts:243. */
export const WebSocketTicketResult = Schema.Struct({
  ticket: Schema.NonEmptyTrimmedString,
  expiresAt: Schema.String,
});

/** `ExecutionEnvironmentDescriptor`, environment.ts:158 (narrowed). */
export const EnvironmentDescriptor = Schema.Struct({
  serverVersion: Schema.String,
  orchestrationProtocolVersion: Schema.optional(Schema.Number),
});

// ---------------------------------------------------------------------------
// Read model

/** `OrchestrationV2Run`, orchestrationV2.ts:536 (narrowed). */
export const Run = Schema.Struct({
  id: Schema.String,
  ordinal: Schema.Number,
  userMessageId: Schema.String,
  status: Schema.String,
  queueHeld: Schema.optional(Schema.Boolean),
  restartContinuationOfRunId: Schema.optional(Schema.String),
  delegatedTaskId: Schema.optional(Schema.String),
});
export type Run = typeof Run.Type;

/** `OrchestrationV2ConversationMessage`, orchestrationV2.ts:1090 (narrowed). */
export const ConversationMessage = Schema.Struct({
  id: Schema.String,
  runId: Schema.NullOr(Schema.String),
  role: Schema.String,
  text: Schema.String,
  streaming: Schema.Boolean,
});
export type ConversationMessage = typeof ConversationMessage.Type;

/** `OrchestrationV2RuntimeRequest`, orchestrationV2.ts:1011 (narrowed). */
export const RuntimeRequest = Schema.Struct({
  id: Schema.String,
  kind: Schema.String,
  status: Schema.String,
});
export type RuntimeRequest = typeof RuntimeRequest.Type;

/** `OrchestrationV2TurnItemBaseFields`, orchestrationV2.ts:1286 (narrowed). */
export const TurnItemBase = Schema.Struct({
  id: Schema.String,
  runId: Schema.NullOr(Schema.String),
  type: Schema.String,
  status: Schema.String,
  ordinal: Schema.Number,
  title: Schema.optional(Schema.NullOr(Schema.String)),
});
export type TurnItemBase = typeof TurnItemBase.Type;

/** Per-type fields of `OrchestrationV2TurnItem`, orchestrationV2.ts:1338. */
export const TurnItemFields = {
  assistant_message: Schema.Struct({
    messageId: Schema.String,
    text: Schema.String,
    streaming: Schema.Boolean,
  }),
  reasoning: Schema.Struct({ text: Schema.String, streaming: Schema.Boolean }),
  proposed_plan: Schema.Struct({
    markdown: Schema.String,
    streaming: Schema.Boolean,
  }),
  todo_list: Schema.Struct({
    steps: Schema.Array(
      Schema.Struct({
        id: Schema.String,
        text: Schema.String,
        status: Schema.String,
      }),
    ),
  }),
  user_input_request: Schema.Struct({
    requestId: Schema.String,
    questions: Schema.Array(
      Schema.Struct({
        id: Schema.String,
        header: Schema.String,
        question: Schema.String,
        options: Schema.Array(
          Schema.Struct({
            label: Schema.String,
            value: Schema.optional(Schema.String),
          }),
        ),
        multiSelect: Schema.optional(Schema.Boolean),
        allowCustomAnswer: Schema.optional(Schema.Boolean),
      }),
    ),
  }),
  approval_request: Schema.Struct({
    requestId: Schema.String,
    requestKind: Schema.String,
    prompt: Schema.optional(Schema.String),
    reason: Schema.optional(Schema.String),
    options: Schema.optional(
      Schema.Array(
        Schema.Struct({ decision: Schema.String, label: Schema.String }),
      ),
    ),
  }),
  command_execution: Schema.Struct({
    input: Schema.String,
    output: Schema.optional(Schema.String),
    outputIndicatesFailure: Schema.optional(Schema.Boolean),
    exitCode: Schema.optional(Schema.Number),
  }),
  file_change: Schema.Struct({
    fileName: Schema.String,
    additions: Schema.optional(Schema.Number),
    deletions: Schema.optional(Schema.Number),
  }),
  file_search: Schema.Struct({ pattern: Schema.optional(Schema.String) }),
  web_search: Schema.Struct({
    patterns: Schema.optional(Schema.Array(Schema.String)),
  }),
  dynamic_tool: Schema.Struct({
    toolName: Schema.NullOr(Schema.String),
    input: Schema.Unknown,
    output: Schema.optional(Schema.Unknown),
  }),
  subagent: Schema.Struct({
    prompt: Schema.String,
    result: Schema.NullOr(Schema.String),
  }),
  error: Schema.Struct({
    failure: Schema.Struct({
      class: Schema.String,
      message: Schema.String,
      retryable: Schema.NullOr(Schema.Boolean),
    }),
  }),
} as const;

/** `OrchestrationV2ThreadProjection`, orchestrationV2.ts:1745 (narrowed). */
export const ThreadProjection = Schema.Struct({
  thread: Schema.Struct({ id: Schema.String }),
  runs: Schema.Array(Run),
  messages: Schema.Array(ConversationMessage),
  runtimeRequests: Schema.Array(RuntimeRequest),
  /** Decoded item by item by the mapper so one unknown item can't fail all. */
  turnItems: Schema.Array(Schema.Unknown),
});
export type ThreadProjection = typeof ThreadProjection.Type;

/** `OrchestrationV2DomainEvent`, orchestrationV2.ts:1600 (envelope only). */
export const DomainEvent = Schema.Struct({
  type: Schema.String,
  payload: Schema.optional(Schema.Unknown),
});
export type DomainEvent = typeof DomainEvent.Type;

/** `OrchestrationV2ThreadStreamItem`, orchestrationV2.ts:3307 (narrowed). */
export const ThreadStreamItem = Schema.Union(
  Schema.Struct({ kind: Schema.Literal("synchronized") }),
  Schema.Struct({
    kind: Schema.Literal("snapshot"),
    snapshotSequence: Schema.Number,
    projection: ThreadProjection,
  }),
  Schema.Struct({
    kind: Schema.Literal("event"),
    sequence: Schema.Number,
    event: DomainEvent,
  }),
);
export type ThreadStreamItem = typeof ThreadStreamItem.Type;

// ---------------------------------------------------------------------------
// Inputs and commands

/** `RuntimeMode`, providerPolicy.ts:25. */
export const RuntimeMode = Schema.Literal(
  "approval-required",
  "auto-accept-edits",
  "auto",
  "full-access",
);
/** `ProviderInteractionMode`, providerPolicy.ts:34. */
export const InteractionMode = Schema.Literal("default", "plan");
/** `ProviderApprovalDecision`, providerPolicy.ts:50. */
export const ApprovalDecision = Schema.Literal(
  "accept",
  "acceptForSession",
  "acceptAlways",
  "decline",
  "cancel",
);

/** `ProviderOptionSelection`, model.ts:49. */
export const ProviderOptionSelection = Schema.Struct({
  id: Schema.NonEmptyTrimmedString,
  value: Schema.Union(Schema.NonEmptyTrimmedString, Schema.Boolean),
});
export type ProviderOptionSelection = typeof ProviderOptionSelection.Type;

/** `ModelSelection` wire form, modelSelection.ts:9. */
export const ModelSelection = Schema.Struct({
  instanceId: Schema.NonEmptyTrimmedString,
  model: Schema.NonEmptyTrimmedString,
  options: Schema.optional(Schema.Array(ProviderOptionSelection)),
});
export type ModelSelection = typeof ModelSelection.Type;

/** `OrchestrationV2ThreadLaunchWorkspaceStrategy`, orchestrationV2.ts:514. */
export const WorkspaceStrategy = Schema.Union(
  Schema.Struct({
    type: Schema.Literal("root"),
    branch: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("existing_worktree"),
    worktreePath: Schema.NonEmptyTrimmedString,
    branch: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("worktree"),
    baseRef: Schema.NonEmptyTrimmedString,
    branch: Schema.optional(Schema.String),
  }),
);
export type WorkspaceStrategy = typeof WorkspaceStrategy.Type;

/**
 * `OrchestrationV2CreationSource`, orchestrationV2.ts:79. The gateway sends
 * "web": the server's own default, and the source T3 treats as a user acting
 * from a client (it releases held queued runs). "server" and "provider" are
 * reserved for T3-internal notifications.
 */
export const CREATION_SOURCE = "web" as const;

/**
 * `OrchestrationV2ThreadLaunchInput`, orchestrationV2.ts:3155 (subset).
 * `reuseExistingThread` is left out on purpose: it means "launch into an
 * existing *empty* thread" (ThreadLaunchService.validateReusableThread), not
 * "create or reuse". Idempotency comes from the commandId receipt instead.
 */
export const LaunchThreadInput = Schema.Struct({
  commandId: Schema.NonEmptyTrimmedString,
  creationSource: Schema.Literal(CREATION_SOURCE),
  threadId: Schema.NonEmptyTrimmedString,
  projectId: Schema.NonEmptyTrimmedString,
  title: Schema.NonEmptyTrimmedString,
  modelSelection: ModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: InteractionMode,
  workspaceStrategy: WorkspaceStrategy,
});
export type LaunchThreadInput = typeof LaunchThreadInput.Type;

/** `OrchestrationV2ThreadLaunchResult`, orchestrationV2.ts:3178. */
export const LaunchThreadResult = Schema.Struct({
  threadId: Schema.String,
  projection: ThreadProjection,
  resumed: Schema.Boolean,
});

/** `message.dispatch` `dispatchMode`, orchestrationV2.ts:2798. */
export const DispatchMode = Schema.Union(
  Schema.Struct({ type: Schema.Literal("start_immediately") }),
  Schema.Struct({ type: Schema.Literal("queue_after_active") }),
  Schema.Struct({
    type: Schema.Literal("steer_active"),
    targetRunId: Schema.NonEmptyTrimmedString,
  }),
);
export type DispatchMode = typeof DispatchMode.Type;

/** `OrchestrationV2Command` members used by the driver, orchestrationV2.ts:2563. */
export const MessageDispatchCommand = Schema.Struct({
  type: Schema.Literal("message.dispatch"),
  createdBy: Schema.Literal("user"),
  creationSource: Schema.Literal(CREATION_SOURCE),
  commandId: Schema.NonEmptyTrimmedString,
  threadId: Schema.NonEmptyTrimmedString,
  messageId: Schema.NonEmptyTrimmedString,
  text: Schema.String,
  attachments: Schema.Tuple(),
  dispatchMode: DispatchMode,
});
export const RunInterruptCommand = Schema.Struct({
  type: Schema.Literal("run.interrupt"),
  commandId: Schema.NonEmptyTrimmedString,
  threadId: Schema.NonEmptyTrimmedString,
  runId: Schema.NonEmptyTrimmedString,
  reason: Schema.optional(Schema.String),
});
export const RuntimeRequestRespondCommand = Schema.Struct({
  type: Schema.Literal("runtime-request.respond"),
  commandId: Schema.NonEmptyTrimmedString,
  threadId: Schema.NonEmptyTrimmedString,
  requestId: Schema.NonEmptyTrimmedString,
  decision: Schema.optional(ApprovalDecision),
  answers: Schema.optional(
    Schema.Record({ key: Schema.String, value: Schema.Unknown }),
  ),
});
export const UserInputDismissCommand = Schema.Struct({
  type: Schema.Literal("thread.user-input.dismiss"),
  commandId: Schema.NonEmptyTrimmedString,
  threadId: Schema.NonEmptyTrimmedString,
  requestId: Schema.NonEmptyTrimmedString,
});
export const DispatchCommand = Schema.Union(
  MessageDispatchCommand,
  RunInterruptCommand,
  RuntimeRequestRespondCommand,
  UserInputDismissCommand,
);
export type DispatchCommand = typeof DispatchCommand.Type;

/** `OrchestrationV2DispatchCommandResult`, orchestrationV2.ts:3185. */
export const DispatchCommandResult = Schema.Struct({ sequence: Schema.Number });

/** `OrchestrationV2GetThreadProjectionInput`, orchestrationV2.ts:3190. */
export const GetThreadProjectionInput = Schema.Struct({
  threadId: Schema.NonEmptyTrimmedString,
});

/** `OrchestrationV2SubscribeThreadInput`, orchestrationV2.ts:3202 (subset). */
export const SubscribeThreadInput = Schema.Struct({
  threadId: Schema.NonEmptyTrimmedString,
  afterSequence: Schema.optional(Schema.NonNegativeInt),
});
