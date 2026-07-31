import { Effect, Option } from "effect";
import type { AgentRun, Installation, ProjectionJob, RepositoryRecord, RunEvent, RunInput } from "../domain/models.js";

export class InstallationRepo extends Effect.Service<InstallationRepo>()("InstallationRepo", {
  accessors: true,
  effect: Effect.gen(function* () {
    const put = Effect.fn("InstallationRepo.put")(function* (record: Installation): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: InstallationRepo.put"); });
    const get = Effect.fn("InstallationRepo.get")(function* (organizationId: string): Effect.fn.Return<Option.Option<Installation>, never> { return yield* Effect.dieMessage("unimplemented: InstallationRepo.get"); });
    const revoke = Effect.fn("InstallationRepo.revoke")(function* (organizationId: string, at: number): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: InstallationRepo.revoke"); });
    return { put, get, revoke };
  }),
}) {}

export class DeliveryRepo extends Effect.Service<DeliveryRepo>()("DeliveryRepo", {
  accessors: true,
  effect: Effect.gen(function* () {
    const accept = Effect.fn("DeliveryRepo.accept")(function* (input: { readonly id: string; readonly organizationId: string; readonly payloadHash: string; readonly receivedAt: number }): Effect.fn.Return<boolean, never> { return yield* Effect.dieMessage("unimplemented: DeliveryRepo.accept"); });
    const claim = Effect.fn("DeliveryRepo.claim")(function* (id: string): Effect.fn.Return<boolean, never> { return yield* Effect.dieMessage("unimplemented: DeliveryRepo.claim"); });
    const mark = Effect.fn("DeliveryRepo.mark")(function* (id: string, status: "processed" | "failed", error?: string): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: DeliveryRepo.mark"); });
    return { accept, claim, mark };
  }),
}) {}

export class RunRepo extends Effect.Service<RunRepo>()("RunRepo", {
  accessors: true,
  effect: Effect.gen(function* () {
    const create = Effect.fn("RunRepo.create")(function* (run: AgentRun): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: RunRepo.create"); });
    const get = Effect.fn("RunRepo.get")(function* (sessionId: string): Effect.fn.Return<Option.Option<AgentRun>, never> { return yield* Effect.dieMessage("unimplemented: RunRepo.get"); });
    const update = Effect.fn("RunRepo.update")(function* (sessionId: string, patch: Partial<AgentRun>): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: RunRepo.update"); });
    const listRunnable = Effect.fn("RunRepo.listRunnable")(function* (now: number): Effect.fn.Return<ReadonlyArray<AgentRun>, never> { return yield* Effect.dieMessage("unimplemented: RunRepo.listRunnable"); });
    return { create, get, update, listRunnable };
  }),
}) {}

export class RunInputRepo extends Effect.Service<RunInputRepo>()("RunInputRepo", {
  accessors: true,
  effect: Effect.gen(function* () {
    const enqueue = Effect.fn("RunInputRepo.enqueue")(function* (input: RunInput): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: RunInputRepo.enqueue"); });
    const pending = Effect.fn("RunInputRepo.pending")(function* (sessionId: string): Effect.fn.Return<ReadonlyArray<RunInput>, never> { return yield* Effect.dieMessage("unimplemented: RunInputRepo.pending"); });
    const markProcessed = Effect.fn("RunInputRepo.markProcessed")(function* (id: string, at: number): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: RunInputRepo.markProcessed"); });
    return { enqueue, pending, markProcessed };
  }),
}) {}

export class RunEventRepo extends Effect.Service<RunEventRepo>()("RunEventRepo", {
  accessors: true,
  effect: Effect.gen(function* () {
    const upsert = Effect.fn("RunEventRepo.upsert")(function* (event: RunEvent): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: RunEventRepo.upsert"); });
    const list = Effect.fn("RunEventRepo.list")(function* (sessionId: string): Effect.fn.Return<ReadonlyArray<RunEvent>, never> { return yield* Effect.dieMessage("unimplemented: RunEventRepo.list"); });
    return { upsert, list };
  }),
}) {}

export class ProjectionRepo extends Effect.Service<ProjectionRepo>()("ProjectionRepo", {
  accessors: true,
  effect: Effect.gen(function* () {
    const enqueue = Effect.fn("ProjectionRepo.enqueue")(function* (job: ProjectionJob): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: ProjectionRepo.enqueue"); });
    const due = Effect.fn("ProjectionRepo.due")(function* (now: number, limit: number): Effect.fn.Return<ReadonlyArray<string>, never> { return yield* Effect.dieMessage("unimplemented: ProjectionRepo.due"); });
    const complete = Effect.fn("ProjectionRepo.complete")(function* (sourceKey: string): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: ProjectionRepo.complete"); });
    return { enqueue, due, complete };
  }),
}) {}

export class WorkspaceRepo extends Effect.Service<WorkspaceRepo>()("WorkspaceRepo", {
  accessors: true,
  effect: Effect.gen(function* () {
    const list = Effect.fn("WorkspaceRepo.list")(function* (organizationId: string): Effect.fn.Return<ReadonlyArray<RepositoryRecord>, never> { return yield* Effect.dieMessage("unimplemented: WorkspaceRepo.list"); });
    const get = Effect.fn("WorkspaceRepo.get")(function* (organizationId: string, id: string): Effect.fn.Return<Option.Option<RepositoryRecord>, never> { return yield* Effect.dieMessage("unimplemented: WorkspaceRepo.get"); });
    return { list, get };
  }),
}) {}

export class LinearGateway extends Effect.Service<LinearGateway>()("LinearGateway", {
  accessors: true,
  effect: Effect.gen(function* () {
    const createActivity = Effect.fn("LinearGateway.createActivity")(function* (input: { readonly sessionId: string; readonly content: unknown }): Effect.fn.Return<string, never> { return yield* Effect.dieMessage("unimplemented: LinearGateway.createActivity"); });
    const updateSession = Effect.fn("LinearGateway.updateSession")(function* (input: { readonly sessionId: string; readonly plan?: ReadonlyArray<{ readonly content: string; readonly status: string }>; readonly externalUrls?: ReadonlyArray<{ readonly label: string; readonly url: string }> }): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: LinearGateway.updateSession"); });
    const refreshInstallation = Effect.fn("LinearGateway.refreshInstallation")(function* (organizationId: string): Effect.fn.Return<string, never> { return yield* Effect.dieMessage("unimplemented: LinearGateway.refreshInstallation"); });
    return { createActivity, updateSession, refreshInstallation };
  }),
}) {}

export class WebhookPipeline extends Effect.Service<WebhookPipeline>()("WebhookPipeline", {
  accessors: true,
  effect: Effect.gen(function* () {
    const handle = Effect.fn("WebhookPipeline.handle")(function* (request: Request): Effect.fn.Return<Response, never> { return yield* Effect.dieMessage("unimplemented: WebhookPipeline.handle"); });
    return { handle };
  }),
}) {}

export class ActivityProjector extends Effect.Service<ActivityProjector>()("ActivityProjector", {
  accessors: true,
  effect: Effect.gen(function* () {
    const flushPending = Effect.fn("ActivityProjector.flushPending")(function* (limit: number, now: number): Effect.fn.Return<number, never> { return yield* Effect.dieMessage("unimplemented: ActivityProjector.flushPending"); });
    const projectRpcEvent = Effect.fn("ActivityProjector.projectRpcEvent")(function* (sessionId: string, sequence: number, event: unknown): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: ActivityProjector.projectRpcEvent"); });
    return { flushPending, projectRpcEvent };
  }),
}) {}

export class SessionAuthority extends Effect.Service<SessionAuthority>()("SessionAuthority", {
  accessors: true,
  effect: Effect.gen(function* () {
    const processRunnable = Effect.fn("SessionAuthority.processRunnable")(function* (): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: SessionAuthority.processRunnable"); });
    const processSession = Effect.fn("SessionAuthority.processSession")(function* (sessionId: string): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: SessionAuthority.processSession"); });
    const shutdown = Effect.fn("SessionAuthority.shutdown")(function* (): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: SessionAuthority.shutdown"); });
    return { processRunnable, processSession, shutdown };
  }),
}) {}

export class RpcWorker extends Effect.Service<RpcWorker>()("RpcWorker", {
  accessors: true,
  effect: Effect.gen(function* () {
    const start = Effect.fn("RpcWorker.start")(function* (sessionId: string): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: RpcWorker.start"); });
    const stop = Effect.fn("RpcWorker.stop")(function* (sessionId: string): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: RpcWorker.stop"); });
    return { start, stop };
  }),
}) {}

export class Reconciler extends Effect.Service<Reconciler>()("Reconciler", {
  accessors: true,
  effect: Effect.gen(function* () {
    const tick = Effect.fn("Reconciler.tick")(function* (): Effect.fn.Return<void, never> { return yield* Effect.dieMessage("unimplemented: Reconciler.tick"); });
    return { tick };
  }),
}) {}

export class Workspace extends Effect.Service<Workspace>()("Workspace", {
  accessors: true,
  effect: Effect.gen(function* () {
    const resolve = Effect.fn("Workspace.resolve")(function* (context: unknown): Effect.fn.Return<Option.Option<RepositoryRecord>, never> { return yield* Effect.dieMessage("unimplemented: Workspace.resolve"); });
    const materialize = Effect.fn("Workspace.materialize")(function* (sessionId: string, repository: RepositoryRecord): Effect.fn.Return<string, never> { return yield* Effect.dieMessage("unimplemented: Workspace.materialize"); });
    return { resolve, materialize };
  }),
}) {}

export class OAuth extends Effect.Service<OAuth>()("OAuth", {
  accessors: true,
  effect: Effect.gen(function* () {
    const startAuthorization = Effect.fn("OAuth.startAuthorization")(function* (): Effect.fn.Return<{ readonly state: string; readonly url: URL }, never> { return yield* Effect.dieMessage("unimplemented: OAuth.startAuthorization"); });
    const completeAuthorization = Effect.fn("OAuth.completeAuthorization")(function* (url: URL): Effect.fn.Return<Installation, never> { return yield* Effect.dieMessage("unimplemented: OAuth.completeAuthorization"); });
    return { startAuthorization, completeAuthorization };
  }),
}) {}

export class Admin extends Effect.Service<Admin>()("Admin", {
  accessors: true,
  effect: Effect.gen(function* () {
    const handle = Effect.fn("Admin.handle")(function* (request: Request): Effect.fn.Return<Option.Option<Response>, never> { return yield* Effect.dieMessage("unimplemented: Admin.handle"); });
    return { handle };
  }),
}) {}
