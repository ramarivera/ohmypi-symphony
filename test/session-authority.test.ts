import { mkdir, mkdtemp, readFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "@effect/vitest";
import {
  Clock,
  ConfigProvider,
  Context,
  Deferred,
  Effect,
  Either,
  Fiber,
  Layer,
  Option,
  Schema,
  TestClock,
} from "effect";
import {
  DatabaseError,
  InterruptedRunNoActionableInputError,
  LinearApiError,
  NixEnvironmentError,
  RpcProtocolError,
} from "../src/domain/errors.js";
import {
  AppUserId,
  InputId,
  IssueId,
  McpServerId,
  OrganizationId,
  SessionId,
  SourceKey,
  TeamId,
  WorkspaceId,
} from "../src/domain/ids.js";
import { type Installation, NixPackageName } from "../src/domain/models.js";
import { GatewayConfig } from "../src/services/config.js";
import {
  GitHubApp,
  type GitHubAppTokenService,
} from "../src/services/github-app.js";
import { LinearGateway } from "../src/services/linear-gateway.js";
import { McpOAuth } from "../src/services/mcp-oauth.js";
import { NixEnvironment } from "../src/services/nix-environment.js";
import { ActivityProjector } from "../src/services/projector.js";
import {
  type RpcEvent,
  type RpcHostToolCall,
  type RpcHostToolDefinition,
  type RpcHostToolResult,
  RpcWorker,
  type RpcWorkerHandle,
} from "../src/services/rpc-worker.js";
import {
  deviationFromRpcEvent,
  linearWorkerPrompt,
  resolveDeviationExtensionPath,
  SessionAuthority,
} from "../src/services/session-authority.js";
import {
  InstallationRepo,
  McpServerRepo,
  PromptTemplateRepo,
  RunEventRepo,
  RunInputRepo,
  RunRepo,
  WorkspaceRepo,
} from "../src/services/store/repositories.js";
import {
  SqliteClient,
  SqliteClientLive,
  type SqliteClientShape,
} from "../src/services/store/sqlite-client.js";
import { WorkerIdentityRepo } from "../src/services/store/worker-identity-repo.js";
import { TokenCrypto } from "../src/services/token-crypto.js";
import { tokenContext } from "../src/services/token-migration.js";
import { testPaths } from "./fixtures/paths.js";

const testPath = testPaths();

describe("Linear worker prompt contract", () => {
  it("adds the Linear activity contract to a created session", () => {
    const prompt = linearWorkerPrompt("created", "Fix the webhook race");

    expect(prompt).toContain("Use OMP todos");
    expect(prompt).toContain("Linear agent plan");
    expect(prompt).toContain("Linear elicitations");
    expect(prompt).toContain("thought and action activities");
    expect(prompt).toContain("include relevant artifact URLs");
    expect(prompt).toContain("rromp_report_deviation");
    expect(prompt).toContain("cease work immediately");
    expect(prompt.endsWith("Linear task:\nFix the webhook race")).toBe(true);
  });

  it.each(["prompted", "stop"] as const)(
    "leaves %s input unchanged",
    (kind) => {
      expect(linearWorkerPrompt(kind, "Continue from here")).toBe(
        "Continue from here",
      );
    },
  );

  it("extracts only valid deviation tool calls", () => {
    expect(
      deviationFromRpcEvent({
        type: "tool_execution_start",
        toolName: "rromp_report_deviation",
        args: { deviation: "  Used a narrower migration scope.  " },
      }),
    ).toBe("Used a narrower migration scope.");
    expect(
      deviationFromRpcEvent({
        type: "tool_execution_start",
        toolName: "read",
        args: { deviation: "not a report" },
      }),
    ).toBeNull();
  });

  it("resolves the bundled deviation extension entrypoint", () => {
    expect(resolveDeviationExtensionPath()).not.toBeNull();
  });
});

describe("SessionAuthority behavior invariants", () => {
  it.effect.prop(
    "persists each worker event once with distinct sequence keys",
    {
      sessionId: Schema.UUID,
      type: Schema.Literal(
        "agent_start",
        "turn_start",
        "turn_end",
        "tool_execution_start",
        "tool_execution_end",
        "message_end",
        "prompt_result",
      ),
    },
    ({ sessionId, type }) =>
      withAuthority(() =>
        Effect.gen(function* () {
          const sid = Schema.decodeUnknownSync(SessionId)(sessionId);
          const organizationId = Schema.decodeUnknownSync(OrganizationId)(
            `organization-${sessionId}`,
          );
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          const installationRepo = yield* InstallationRepo;
          const runEventRepo = yield* RunEventRepo;
          yield* runRepo.create({
            sessionId: sid,
            organizationId,
            issueId: Option.none(),
          });
          yield* installationRepo.put(install(organizationId));
          yield* runRepo.update(sid, {
            state: "orphaned",
            workspacePath: Option.some(testPath("temporary")),
            ompSessionFile: Option.some(testPath("session.jsonl")),
          });
          yield* authority.processSession(sid);

          const projected = yield* Deferred.make<void>();
          projectionWaiter = projected;
          projectionExpected = 2;
          const event: RpcEvent = { type };
          yield* Effect.sync(() => {
            if (workerEventListener === undefined)
              throw new Error("worker event listener was not registered");
            workerEventListener(event);
            workerEventListener(event);
          });
          yield* Deferred.await(projected);

          const persisted = (yield* runEventRepo.list(sid)).filter(
            (item) =>
              typeof item.payload === "object" &&
              item.payload !== null &&
              "type" in item.payload &&
              item.payload.type === type,
          );
          expect(persisted).toHaveLength(2);
          expect(new Set(persisted.map((item) => item.sourceKey)).size).toBe(2);
          expect(persisted.map((item) => item.payload)).toEqual([event, event]);
        }),
      ),
    { timeout: 15_000, fastCheck: { numRuns: 10 } },
  );

  it.effect.prop(
    "records UI requests and transitions the run to waiting",
    {
      sessionId: Schema.UUID,
      requestId: Schema.String.pipe(Schema.minLength(1)),
    },
    ({ sessionId, requestId }) =>
      withAuthority(() =>
        Effect.gen(function* () {
          const sid = Schema.decodeUnknownSync(SessionId)(sessionId);
          const organizationId = Schema.decodeUnknownSync(OrganizationId)(
            `organization-${sessionId}`,
          );
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          const installationRepo = yield* InstallationRepo;
          const runEventRepo = yield* RunEventRepo;
          yield* runRepo.create({
            sessionId: sid,
            organizationId,
            issueId: Option.none(),
          });
          yield* installationRepo.put(install(organizationId));
          yield* runRepo.update(sid, {
            state: "orphaned",
            workspacePath: Option.some(testPath("temporary")),
            ompSessionFile: Option.some(testPath("session.jsonl")),
          });
          yield* authority.processSession(sid);

          const elicitationObserved = yield* Deferred.make<void>();
          elicitationWaiter = elicitationObserved;
          const event: RpcEvent = {
            type: "extension_ui_request",
            id: requestId,
            method: "confirm",
            title: "Continue?",
            message: "Proceed with the task?",
            options: ["yes", "no"],
          };
          yield* Effect.sync(() => {
            if (workerEventListener === undefined)
              throw new Error("worker event listener was not registered");
            workerEventListener(event);
          });
          yield* Deferred.await(elicitationObserved);

          const run = yield* runRepo.get(sid);
          const persisted = yield* runEventRepo.list(sid);
          const uiEvent = persisted.find(
            (item) =>
              typeof item.payload === "object" &&
              item.payload !== null &&
              "type" in item.payload &&
              item.payload.type === "extension_ui_request",
          );
          expect(Option.isSome(run)).toBe(true);
          if (Option.isSome(run)) expect(run.value.state).toBe("waiting");
          expect(uiEvent).toMatchObject({
            sourceKey: `rpc-ui:${requestId}`,
            kind: "extension_ui_request",
            level: "warn",
            payload: event,
          });
          expect(projectorElicitations).toContainEqual({
            sessionId: sid,
            sourceKey: `rpc-ui:${requestId}`,
            text: "Continue?\n\nProceed with the task?",
            options: ["yes", "no"],
          });
        }),
      ),
    { timeout: 15_000, fastCheck: { numRuns: 20 } },
  );

  it.effect.prop(
    "round-trips generated worker events through event and projector layers",
    {
      sessionId: Schema.UUID,
      type: Schema.Literal(
        "agent_start",
        "turn_start",
        "turn_end",
        "agent_end",
        "tool_execution_start",
        "message_end",
        "prompt_result",
        "progress",
      ),
    },
    ({ sessionId, type }) =>
      withAuthority(() =>
        Effect.gen(function* () {
          const sid = Schema.decodeUnknownSync(SessionId)(sessionId);
          const organizationId = Schema.decodeUnknownSync(OrganizationId)(
            `organization-${sessionId}`,
          );
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          const installationRepo = yield* InstallationRepo;
          const runEventRepo = yield* RunEventRepo;
          yield* runRepo.create({
            sessionId: sid,
            organizationId,
            issueId: Option.none(),
          });
          yield* installationRepo.put(install(organizationId));
          yield* runRepo.update(sid, {
            state: "orphaned",
            workspacePath: Option.some(testPath("temporary")),
            ompSessionFile: Option.some(testPath("session.jsonl")),
          });
          yield* authority.processSession(sid);

          const projected = yield* Deferred.make<void>();
          projectionWaiter = projected;
          projectionExpected = 1;
          const event: RpcEvent = { type };
          yield* Effect.sync(() => {
            if (workerEventListener === undefined)
              throw new Error("worker event listener was not registered");
            workerEventListener(event);
          });
          yield* Deferred.await(projected);

          const persisted = (yield* runEventRepo.list(sid)).filter(
            (item) =>
              typeof item.payload === "object" &&
              item.payload !== null &&
              "type" in item.payload &&
              item.payload.type === type,
          );
          expect(persisted).toHaveLength(1);
          expect(persisted[0]?.payload).toEqual(event);
          expect(persisted[0]?.sessionId).toBe(sid);
          expect(persisted[0]?.level).toMatch(
            /^(debug|info|warn|result|error)$/,
          );
          expect(projectorEvents).toContainEqual({
            sessionId: sid,
            sequence: expect.any(Number),
            event,
          });
        }),
      ),
    { timeout: 15_000, fastCheck: { numRuns: 20 } },
  );

  it.effect.prop(
    "persists cancellation outcomes for both stop and canceled desired state",
    {
      sessionId: Schema.UUID,
      desiredState: Schema.Literal("running", "canceled"),
      inputKind: Schema.Literal("prompted", "stop"),
    },
    ({ sessionId, desiredState, inputKind }) =>
      withAuthority((db) =>
        Effect.gen(function* () {
          const sid = Schema.decodeUnknownSync(SessionId)(sessionId);
          const organizationId = Schema.decodeUnknownSync(OrganizationId)(
            `organization-${sessionId}`,
          );
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          const runInputRepo = yield* RunInputRepo;
          const runEventRepo = yield* RunEventRepo;
          const installationRepo = yield* InstallationRepo;
          const inputId = Schema.decodeUnknownSync(InputId)(
            `input-${sessionId}`,
          );
          yield* runRepo.create({
            sessionId: sid,
            organizationId,
            issueId: Option.none(),
          });
          yield* installationRepo.put(install(organizationId));
          yield* runInputRepo.enqueue({
            id: inputId,
            sessionId: sid,
            kind: inputKind,
            body: inputKind === "stop" ? "stop" : "cancel this run",
            payload: { source: "property" },
          });
          if (desiredState === "canceled" && inputKind === "prompted") {
            yield* Effect.sync(() =>
              db
                .query(
                  "UPDATE agent_run SET desired_state='canceled' WHERE session_id=?",
                )
                .run(sid),
            );
          }
          yield* authority.processSession(sid);

          const run = yield* runRepo.get(sid);
          const persisted = yield* runEventRepo.list(sid);
          const shouldCancel =
            desiredState === "canceled" || inputKind === "stop";
          expect(Option.isSome(run)).toBe(true);
          if (Option.isSome(run)) {
            expect(run.value.state).toBe(shouldCancel ? "canceled" : "waiting");
            expect(run.value.desiredState).toBe(
              shouldCancel ? "canceled" : "running",
            );
          }
          expect(
            persisted.some(
              (item) =>
                item.kind === "state" &&
                typeof item.payload === "object" &&
                item.payload !== null &&
                "to" in item.payload &&
                item.payload.to === (shouldCancel ? "canceled" : "waiting"),
            ),
          ).toBe(true);
          if (shouldCancel) {
            expect(projectorTerminals).toContainEqual({
              sessionId: sid,
              sourceKey: `stop:${sid}`,
              kind: "response",
              text: "Stopped as requested.",
            });
          }
        }),
      ),
    { timeout: 15_000, fastCheck: { numRuns: 20 } },
  );

  it.effect(
    "resumes a user-stopped run when a follow-up prompt arrives",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          const runInputRepo = yield* RunInputRepo;
          const installationRepo = yield* InstallationRepo;
          const workspaceRepo = yield* WorkspaceRepo;
          const repositoryId =
            Schema.decodeUnknownSync(WorkspaceId)("resume-repository");
          yield* installationRepo.put(install(testOrganizationId));
          yield* workspaceRepo.createRepository({
            organizationId: testOrganizationId,
            id: repositoryId,
            url: "https://example.com/repository.git",
            ref: "main",
            nixPackages: [],
          });
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* runRepo.update(testSessionId, {
            state: "running",
            repositoryId: Option.some(repositoryId),
            workspacePath: Option.some(testPath("resume-workspace")),
            ompSessionFile: Option.some(
              testPath("resume-workspace/session.jsonl"),
            ),
          });

          yield* runInputRepo.enqueue({
            id: Schema.decodeUnknownSync(InputId)("stop-input"),
            sessionId: testSessionId,
            kind: "stop",
            body: "stop",
            payload: {},
          });
          yield* authority.processSession(testSessionId);

          const stopped = yield* runRepo.get(testSessionId);
          expect(Option.isSome(stopped)).toBe(true);
          if (Option.isSome(stopped)) {
            expect(stopped.value.state).toBe("canceled");
            expect(stopped.value.desiredState).toBe("canceled");
          }
          expect(projectorTerminals).toContainEqual({
            sessionId: testSessionId,
            sourceKey: `stop:${testSessionId}`,
            kind: "response",
            text: "Stopped as requested.",
          });

          yield* runInputRepo.enqueue({
            id: Schema.decodeUnknownSync(InputId)("resume-input"),
            sessionId: testSessionId,
            kind: "prompted",
            body: "please continue the task",
            payload: {},
          });
          yield* authority.processSession(testSessionId);

          const resumed = yield* runRepo.get(testSessionId);
          expect(Option.isSome(resumed)).toBe(true);
          if (Option.isSome(resumed)) {
            // startWorker marks the run running once the worker accepts the
            // prompt.
            expect(resumed.value.state).toBe("running");
            expect(resumed.value.desiredState).toBe("running");
            expect(resumed.value.terminalReason).toEqual(Option.none());
          }
          expect(workerSpawnInputs).toHaveLength(1);
          expect(workerSpawnInputs[0]?.command).toContain("--session");
          expect(workerSpawnInputs[0]?.command).toContain(
            testPath("resume-workspace/session.jsonl"),
          );
          expect(workerSpawnInputs[0]?.cwd).toBe(testPath("resume-workspace"));
          expect(workerPrompts).toEqual(["please continue the task"]);
        }),
      ),
    { timeout: 15_000 },
  );

  it.scopedLive("renders the prompted template at prompt time", () =>
    withAuthority(() =>
      Effect.gen(function* () {
        const authority = yield* SessionAuthority;
        const installationRepo = yield* InstallationRepo;
        const runRepo = yield* RunRepo;
        const runInputRepo = yield* RunInputRepo;
        const templateRepo = yield* PromptTemplateRepo;
        const workspaceRepo = yield* WorkspaceRepo;
        yield* installationRepo.put(install(testOrganizationId));
        yield* templateRepo.upsert({
          organizationId: testOrganizationId,
          kind: "prompted",
          body: "Follow-up:\n{{userRequest}}",
          updatedAt: 0,
        });
        const repositoryId = Schema.decodeUnknownSync(WorkspaceId)(
          "prompt-template-repository",
        );
        yield* workspaceRepo.createRepository({
          organizationId: testOrganizationId,
          id: repositoryId,
          url: "https://example.com/repository.git",
          ref: "main",
          nixPackages: [],
        });
        yield* runRepo.create({
          sessionId: testSessionId,
          organizationId: testOrganizationId,
          issueId: Option.none(),
        });
        yield* runRepo.update(testSessionId, {
          state: "running",
          repositoryId: Option.some(repositoryId),
          workspacePath: Option.some(testPath("resume-workspace")),
          ompSessionFile: Option.some(
            testPath("resume-workspace/session.jsonl"),
          ),
        });

        yield* runInputRepo.enqueue({
          id: Schema.decodeUnknownSync(InputId)("stop-input"),
          sessionId: testSessionId,
          kind: "stop",
          body: "stop",
          payload: {},
        });
        yield* authority.processSession(testSessionId);

        yield* runInputRepo.enqueue({
          id: Schema.decodeUnknownSync(InputId)("resume-input"),
          sessionId: testSessionId,
          kind: "prompted",
          body: "please continue the task",
          payload: {},
        });
        yield* authority.processSession(testSessionId);

        // The raw input body stays literal; the worker receives the
        // template-rendered prompt.
        expect(workerPrompts).toEqual(["Follow-up:\nplease continue the task"]);
      }),
    ),
  );
  it.scopedLive(
    "renders a prompted template when retrying an orphaned run",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          const runInputRepo = yield* RunInputRepo;
          const templateRepo = yield* PromptTemplateRepo;
          yield* installationRepo.put(install(testOrganizationId));
          yield* templateRepo.upsert({
            organizationId: testOrganizationId,
            kind: "prompted",
            body: "Retry:\n{{userRequest}}",
            updatedAt: 0,
          });
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          const inputId =
            Schema.decodeUnknownSync(InputId)("orphan-retry-input");
          yield* runInputRepo.enqueue({
            id: inputId,
            sessionId: testSessionId,
            kind: "prompted",
            body: "continue after interruption",
            payload: {},
          });
          yield* runInputRepo.markProcessed(inputId);
          yield* Effect.promise(() =>
            mkdir(testPath("orphan-retry-template"), { recursive: true }),
          );
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            workspacePath: Option.some(testPath("orphan-retry-template")),
          });

          yield* authority.processSession(testSessionId);

          expect(workerSpawnInputs).toHaveLength(1);
          expect(workerPrompts).toEqual([
            "Retry:\ncontinue after interruption",
          ]);
        }),
      ),
  );
  it.effect(
    "materializes configured MCP servers through the real authority layer",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          const installationRepo = yield* InstallationRepo;
          const mcpServerRepo = yield* McpServerRepo;
          const repositoryId = Schema.decodeUnknownSync(WorkspaceId)(
            "authority-mcp-repository",
          );
          const workspace = yield* Effect.promise(() =>
            mkdtemp(join(testPath("temporary"), "authority-mcp-config-")),
          );
          yield* installationRepo.put(install(testOrganizationId));
          yield* mcpServerRepo.createMcpServer({
            organizationId: testOrganizationId,
            id: Schema.decodeUnknownSync(McpServerId)("authority-mcp-server"),
            name: "configured",
            transport: "stdio",
            command: "node",
            args: ["server.js"],
            env: { TOKEN: "secret" },
            repositoryId: Option.some(repositoryId),
            now: 1,
          });
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            repositoryId: Option.some(repositoryId),
            workspacePath: Option.some(workspace),
            ompSessionFile: Option.some(join(workspace, "session.jsonl")),
          });
          yield* authority.processSession(testSessionId);
          const config = JSON.parse(
            yield* Effect.promise(() =>
              readFile(join(workspace, "mcp.json"), "utf8"),
            ),
          ) as { mcpServers: Record<string, { command: string }> };
          expect(config.mcpServers.configured?.command).toBe("node");
        }),
      ),
    { timeout: 15_000 },
  );
  it.effect(
    "deduplicates proxy renewal failures per session and server and revokes its bearer on shutdown",
    () =>
      withAuthority((db) =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const repo = yield* McpServerRepo;
          const cipher = yield* TokenCrypto;
          const sid = testSessionId;
          const org = testOrganizationId;
          const serverId =
            Schema.decodeUnknownSync(McpServerId)("proxy-events");
          const url = "https://mcp.example/server";
          const workspace = testPath("proxy-workspace");
          yield* InstallationRepo.put(install(org));
          yield* repo.createMcpServer({
            organizationId: org,
            id: serverId,
            name: "renewal-test",
            transport: "http",
            url,
            now: 1,
          });
          const encrypt = (field: string, value: string) =>
            cipher.encrypt(
              value,
              tokenContext("mcp_oauth_credential", org, serverId, field),
            );
          const client = yield* encrypt(
            "client_json",
            JSON.stringify({
              clientId: "client",
              clientSecret: "gateway-client-secret",
              authorizationEndpoint: "https://mcp.example/authorize",
              tokenEndpoint: "https://mcp.example/token",
            }),
          );
          const access = yield* encrypt(
            "access_token",
            "gateway-access-secret",
          );
          // Missing refresh material fails locally without an external request.
          db.query(
            "INSERT INTO mcp_oauth_credential (organization_id, server_id, server_url, client_json, access_token, expires_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
          ).run(
            org,
            serverId,
            url,
            `mcpenc:v1:${client}`,
            `mcpenc:v1:${access}`,
            0,
            1,
            1,
          );
          yield* RunRepo.create({
            sessionId: sid,
            organizationId: org,
            issueId: Option.none(),
          });
          yield* RunRepo.update(sid, {
            state: "orphaned",
            workspacePath: Option.some(workspace),
            ompSessionFile: Option.some(join(workspace, "session.jsonl")),
          });
          yield* authority.processSession(sid);
          const contents = yield* Effect.promise(() =>
            readFile(join(workspace, "mcp.json"), "utf8"),
          );
          const config = JSON.parse(contents) as {
            mcpServers: Record<
              string,
              { url: string; headers: Record<string, string> }
            >;
          };
          const server = config.mcpServers["renewal-test"];
          if (server === undefined)
            throw new Error("Expected gateway MCP proxy configuration");
          expect(contents).not.toContain("gateway-access-secret");
          expect(contents).not.toContain("gateway-client-secret");
          expect(JSON.stringify(workerSpawnInputs)).not.toContain(
            "gateway-access-secret",
          );
          const response = yield* Effect.promise(() =>
            fetch(server.url, {
              method: "POST",
              headers: server.headers,
              body: "{}",
            }),
          );
          expect(response.status).toBe(502);
          const retries = yield* Effect.promise(() =>
            Promise.all(
              Array.from({ length: 12 }, () =>
                fetch(server.url, {
                  method: "POST",
                  headers: server.headers,
                  body: "{}",
                }),
              ),
            ),
          );
          expect(retries.map((retry) => retry.status)).toEqual(
            Array(12).fill(502),
          );
          const events = yield* RunEventRepo.list(sid);
          expect(
            events.filter((event) => event.kind === "mcp_refresh_failed"),
          ).toHaveLength(1);
          expect(
            events.find((event) => event.kind === "mcp_refresh_failed"),
          ).toMatchObject({
            sourceKey: `mcp-refresh:${sid}:${serverId}`,
            level: "error",
            text: Option.some(
              "MCP credential renewal failed for renewal-test. Reconnect this server in the gateway and retry the tool call.",
            ),
            payload: { serverId },
          });
          expect(JSON.stringify(events)).not.toContain("gateway-client-secret");
          yield* authority.shutdown();
          yield* Effect.promise(() =>
            expect(
              fetch(server.url, { headers: server.headers }),
            ).rejects.toThrow(),
          );
        }),
      ),
  );
  it.effect(
    "routes MCP decrypt failures through retry handling",
    () =>
      withAuthority((db) =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          const installationRepo = yield* InstallationRepo;
          const mcpServerRepo = yield* McpServerRepo;
          const workspace = yield* Effect.promise(() =>
            mkdtemp(join(testPath("temporary"), "authority-mcp-corrupt-")),
          );
          yield* installationRepo.put(install(testOrganizationId));
          yield* mcpServerRepo.createMcpServer({
            organizationId: testOrganizationId,
            id: Schema.decodeUnknownSync(McpServerId)("corrupt-mcp-server"),
            name: "corrupt",
            transport: "stdio",
            command: "node",
            args: [],
            env: { TOKEN: "secret" },
            repositoryId: Option.none(),
            now: 1,
          });
          db.query(
            "UPDATE mcp_server SET env_json=? WHERE organization_id=? AND id=?",
          ).run(
            JSON.stringify({ TOKEN: "mcpenc:v1:not-valid-ciphertext" }),
            testOrganizationId,
            "corrupt-mcp-server",
          );
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            workspacePath: Option.some(workspace),
            ompSessionFile: Option.some(join(workspace, "session.jsonl")),
          });
          yield* Effect.either(authority.processSession(testSessionId));
          const updated = yield* runRepo.get(testSessionId);
          expect(Option.isSome(updated)).toBe(true);
          if (Option.isSome(updated)) {
            expect(updated.value.state).toBe("orphaned");
            expect(updated.value.desiredState).toBe("running");
            expect(Option.isSome(updated.value.nextAttemptAt)).toBe(true);
            expect(updated.value.terminalReason).not.toEqual(Option.none());
          }
          expect(workerSpawnInputs).toHaveLength(0);
        }),
      ),
    { timeout: 15_000 },
  );

  it.effect(
    "materializes an empty MCP config when no servers exist",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          const installationRepo = yield* InstallationRepo;
          const workspace = yield* Effect.promise(() =>
            mkdtemp(join(testPath("temporary"), "authority-mcp-empty-")),
          );
          yield* installationRepo.put(install(testOrganizationId));
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            workspacePath: Option.some(workspace),
            ompSessionFile: Option.some(join(workspace, "session.jsonl")),
          });
          yield* authority.processSession(testSessionId);
          expect(
            JSON.parse(
              yield* Effect.promise(() =>
                readFile(join(workspace, "mcp.json"), "utf8"),
              ),
            ),
          ).toEqual({ mcpServers: {} });
        }),
      ),
    { timeout: 15_000 },
  );
});
let authorityTestRoot = "";
const testConfigProvider = (
  githubEnabled = false,
  valuesOverride: ReadonlyArray<readonly [string, string]> = [],
) => {
  const values: Array<readonly [string, string]> = [
    ["LINEAR_CLIENT_ID", "test-client"],
    ["LINEAR_CLIENT_SECRET", "test-secret"],
    ["LINEAR_WEBHOOK_SECRET", "test-webhook-secret"],
    ["TOKEN_ENCRYPTION_KEY", "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc="],
    ["PUBLIC_URL", "http://localhost:3000"],
    [
      "NIXPKGS_FLAKE_REF",
      "github:NixOS/nixpkgs/0123456789012345678901234567890123456789",
    ],
    ["WORKSPACE_ROOT", authorityTestRoot],
  ];
  if (githubEnabled) {
    values.push(
      ["GITHUB_APP_ID", "test-app"],
      ["GITHUB_APP_PRIVATE_KEY", "test-private-key"],
    );
  }
  return ConfigProvider.fromMap(new Map([...values, ...valuesOverride]));
};
let workerEventListener: ((event: RpcEvent) => void) | undefined;
let projectionWaiter: Deferred.Deferred<void, never> | undefined;
let projectionExpected = 0;
const projectorEvents: Array<{
  readonly sessionId: string;
  readonly sequence: number;
  readonly event: RpcEvent;
}> = [];
const projectorElicitations: Array<{
  readonly sessionId: string;
  readonly sourceKey: string;
  readonly text: string;
  readonly options: ReadonlyArray<string> | undefined;
}> = [];
const projectorTerminals: Array<{
  readonly sessionId: string;
  readonly sourceKey: string;
  readonly kind: string;
  readonly text: string;
}> = [];
const toolExternalUrlCalls: Array<{
  readonly sessionId: string;
  readonly urls: ReadonlyArray<{
    readonly label: string;
    readonly url: string;
  }>;
}> = [];
let linearIssueLookupFails = false;
let linearIssueStateType = "started";
let elicitationWaiter: Deferred.Deferred<void, never> | undefined;

const signalProjection = (): void => {
  if (
    projectionWaiter !== undefined &&
    projectorEvents.length >= projectionExpected
  ) {
    Deferred.unsafeDone(projectionWaiter, Effect.void);
    projectionWaiter = undefined;
  }
};

const signalElicitation = (): void => {
  if (elicitationWaiter !== undefined) {
    Deferred.unsafeDone(elicitationWaiter, Effect.void);
    elicitationWaiter = undefined;
  }
};

let terminalFailure: DatabaseError | undefined;
const mockProjector = ActivityProjector.make({
  thought: () => Effect.succeed(true),
  deviation: () => Effect.succeed(true),
  elicitation: (sessionId, sourceKey, text, options) =>
    Effect.sync(() => {
      projectorElicitations.push({ sessionId, sourceKey, text, options });
      signalElicitation();
      return true;
    }),
  terminal: (sessionId, sourceKey, kind, text) =>
    Effect.gen(function* () {
      projectorTerminals.push({ sessionId, sourceKey, kind, text });
      if (terminalFailure !== undefined) yield* Effect.fail(terminalFailure);
      return true;
    }),
  plan: () => Effect.succeed(true),
  externalUrls: () => Effect.succeed(true),
  flushPending: () => Effect.succeed(0),
  projectRpcEvent: (sessionId, sequence, event) =>
    Effect.sync(() => {
      projectorEvents.push({ sessionId, sequence, event: event as RpcEvent });
      signalProjection();
    }),
});

const workerPrompts: Array<string> = [];
let registeredHostTools: ReadonlyArray<RpcHostToolDefinition> = [];
let hostToolListener:
  | ((
      request: RpcHostToolCall,
    ) => Effect.Effect<RpcHostToolResult, never, never>)
  | undefined;
let workerStopBlock:
  | { entered: Deferred.Deferred<void>; release: Deferred.Deferred<void> }
  | undefined;
let hostToolRegistrationError: RpcProtocolError | undefined;
const orderEvents: Array<string> = [];
const mockWorker: RpcWorkerHandle = {
  sessionId: Effect.succeed(Option.none()),
  sessionFile: Effect.succeed(Option.none()),
  isStreaming: Effect.succeed(false),
  start: () => Effect.void,
  stop: () =>
    Effect.gen(function* () {
      if (workerStopBlock !== undefined) {
        yield* Deferred.succeed(workerStopBlock.entered, undefined);
        yield* Deferred.await(workerStopBlock.release);
      }
      orderEvents.push("worker-stopped");
    }),
  prompt: (message) =>
    Effect.sync(() => {
      workerPrompts.push(message);
      return true;
    }),
  steer: () => Effect.void,
  followUp: () => Effect.void,
  setHostTools: (tools) =>
    Effect.gen(function* () {
      if (hostToolRegistrationError !== undefined) {
        return yield* Effect.fail(hostToolRegistrationError);
      }
      registeredHostTools = tools;
    }),
  onHostToolCall: (listener) =>
    Effect.sync(() => {
      hostToolListener = listener;
      return () =>
        Effect.sync(() => {
          if (hostToolListener === listener) hostToolListener = undefined;
        });
    }),
  abort: () => Effect.void,
  getState: () => Effect.succeed({}),
  respondToUi: () => Effect.void,
  onEvent: (listener) =>
    Effect.sync(() => {
      workerEventListener = listener;
      return () =>
        Effect.sync(() => {
          if (workerEventListener === listener) workerEventListener = undefined;
        });
    }),
};

const mockRpcWorker = RpcWorker.make({
  spawn: (input) =>
    Effect.sync(() => {
      workerSpawnInputs.push(input);
      return mockWorker;
    }),
});
const workerSpawnInputs: Array<{
  readonly command: ReadonlyArray<string>;
  readonly cwd: string;
  readonly env?: Record<string, string | undefined>;
}> = [];
let nixPathEntries: ReadonlyArray<string> = [];
let nixPrepareError: NixEnvironmentError | undefined;
let nixPrepareBlock:
  | { entered: Deferred.Deferred<void>; release: Deferred.Deferred<void> }
  | undefined;
const nixPreparedRepositories: Array<string> = [];
const mockNixEnvironment = NixEnvironment.make({
  enforceBudget: () =>
    Effect.succeed({ removed: 0, sizeBytes: 0, withinBudget: true }),
  prepare: (repository) => {
    nixPreparedRepositories.push(repository.id);
    if (nixPrepareError !== undefined) return Effect.fail(nixPrepareError);
    return Effect.gen(function* () {
      if (nixPrepareBlock !== undefined) {
        yield* Deferred.succeed(nixPrepareBlock.entered, undefined);
        yield* Deferred.await(nixPrepareBlock.release);
      }
      return {
        cacheKey: "test-cache",
        storePaths: [],
        pathEntries: nixPathEntries,
        reused: false,
      };
    });
  },
  list: () => Effect.succeed([]),
  prune: () => Effect.succeed(false),
});

const toolCommentCalls: Array<{
  readonly sessionId: string;
  readonly body: string;
}> = [];
let commentEnteredSignal: Deferred.Deferred<void> | undefined;
let commentRelease: Deferred.Deferred<void> | undefined;
const mockToolsGateway = LinearGateway.make({
  createActivity: () => Effect.die(new Error("unused in authority tests")),
  updateSession: () => Effect.die(new Error("unused in authority tests")),
  createIssueComment: (input) =>
    Effect.gen(function* () {
      if (commentEnteredSignal !== undefined) {
        Deferred.unsafeDone(commentEnteredSignal, Effect.void);
      }
      if (commentRelease !== undefined) {
        yield* Deferred.await(commentRelease);
      }
      toolCommentCalls.push(input);
      orderEvents.push("comment-completed");
      return `comment-${toolCommentCalls.length}`;
    }),
  refreshInstallation: () => Effect.die(new Error("unused in authority tests")),
  listSessionActivities: () =>
    Effect.die(new Error("unused in authority tests")),
  getIssue: () =>
    linearIssueLookupFails
      ? Effect.fail(
          new LinearApiError({
            message: "issue lookup unavailable",
            operation: "getIssue",
          }),
        )
      : Effect.succeed({
          id: "issue-1",
          identifier: "AUTH-1",
          title: "Authority issue",
          description: null,
          stateId: "state-started",
          stateName: "In Progress",
          stateType: linearIssueStateType,
          delegateId: "authority-app-user",
          url: null,
          teamId: null,
          labels: [],
        }),
  updateIssue: () => Effect.void,
  addSessionExternalUrls: (input: {
    readonly sessionId: string;
    readonly urls: ReadonlyArray<{
      readonly label: string;
      readonly url: string;
    }>;
  }) =>
    Effect.sync(() => {
      toolExternalUrlCalls.push(input);
    }),
  teamStartedStates: () => Effect.succeed([]),
  repositorySuggestions: () => Effect.succeed([]),
  createSessionOnIssue: () =>
    Effect.die(new Error("unused in authority tests")),
});

const withAuthority = <A, E>(
  effect: (
    db: SqliteClientShape["db"],
  ) => Effect.Effect<
    A,
    E,
    | SessionAuthority
    | RunRepo
    | RunInputRepo
    | RunEventRepo
    | InstallationRepo
    | McpServerRepo
    | McpOAuth
    | TokenCrypto
    | PromptTemplateRepo
    | WorkspaceRepo
    | NixEnvironment
    | GitHubApp
    | ActivityProjector
    | RpcWorker
  >,
  options?: {
    readonly afterShutdown?: (db: SqliteClientShape["db"]) => void;
    readonly config?: ReadonlyArray<readonly [string, string]>;
    readonly withLinearGateway?: boolean;
    readonly githubApp?: GitHubAppTokenService;
  },
) =>
  Effect.scoped(
    Effect.gen(function* () {
      authorityTestRoot = testPath("temporary");
      linearIssueLookupFails = false;
      linearIssueStateType = "started";
      toolExternalUrlCalls.length = 0;
      terminalFailure = undefined;
      workerEventListener = undefined;
      hostToolListener = undefined;

      projectionWaiter = undefined;
      elicitationWaiter = undefined;
      registeredHostTools = [];
      hostToolRegistrationError = undefined;
      workerStopBlock = undefined;
      projectionExpected = 0;
      projectorEvents.length = 0;
      projectorElicitations.length = 0;
      workerSpawnInputs.length = 0;
      workerPrompts.length = 0;
      nixPreparedRepositories.length = 0;
      nixPathEntries = [];
      nixPrepareError = undefined;
      nixPrepareBlock = undefined;
      projectorTerminals.length = 0;
      toolCommentCalls.length = 0;
      orderEvents.length = 0;
      commentEnteredSignal = undefined;
      commentRelease = undefined;
      const sqliteContext = yield* Layer.build(SqliteClientLive(":memory:"));
      const sqlite = Context.get(sqliteContext, SqliteClient);
      const githubAppOverride: Layer.Layer<GitHubApp> =
        options?.githubApp === undefined
          ? (Layer.empty as Layer.Layer<GitHubApp>)
          : Layer.succeed(GitHubApp, options.githubApp as GitHubApp);
      const linearGatewayOverride: Layer.Layer<LinearGateway> =
        options?.withLinearGateway === true
          ? Layer.succeed(LinearGateway, mockToolsGateway)
          : (Layer.empty as Layer.Layer<LinearGateway>);
      const dependencies = Layer.mergeAll(
        GatewayConfig.Default,
        TokenCrypto.Default,
        InstallationRepo.Default,
        McpServerRepo.Default,
        McpOAuth.Default,
        PromptTemplateRepo.Default,
        RunEventRepo.Default,
        RunInputRepo.Default,
        RunRepo.Default,
        WorkspaceRepo.Default,
        WorkerIdentityRepo.Default,
        GitHubApp.Default,
        githubAppOverride,
        Layer.succeed(ActivityProjector, mockProjector),
        Layer.succeed(RpcWorker, mockRpcWorker),
        Layer.succeed(NixEnvironment, mockNixEnvironment),
        linearGatewayOverride,
      ).pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(SqliteClient, sqlite),
            Layer.setConfigProvider(
              testConfigProvider(
                options?.githubApp !== undefined,
                options?.config,
              ),
            ),
          ),
        ),
      );
      const authority = SessionAuthority.DefaultWithoutDependencies.pipe(
        Layer.provide(dependencies),
      );
      const result = yield* effect(sqlite.db).pipe(
        Effect.provide(Layer.mergeAll(dependencies, authority)),
      );
      yield* Effect.sync(() => options?.afterShutdown?.(sqlite.db));
      return result;
    }),
  );

const testSessionId = Schema.decodeUnknownSync(SessionId)("authority-session");
const testIssueId = Schema.decodeUnknownSync(IssueId)("authority-issue");
const testOrganizationId = Schema.decodeUnknownSync(OrganizationId)(
  "authority-organization",
);
const install = (organizationId: OrganizationId): Installation => ({
  organizationId,
  appUserId: Schema.decodeUnknownSync(AppUserId)("authority-app-user"),
  accessToken: "access-token",
  refreshToken: "refresh-token",
  expiresAt: Date.now() + 3_600_000,
  scopes: ["read"],
  revokedAt: Option.none(),
  accessibleTeamIds: Option.none(),
  canAccessAllPublicTeams: Option.none(),
});

describe("SessionAuthority team-access gate", () => {
  it.scopedLive(
    "proceeds when the installation team-access snapshot is unknown (null)",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          const teamId = Schema.decodeUnknownSync(TeamId)("authority-team");
          // install() leaves accessibleTeamIds/canAccessAllPublicTeams unset
          // (Option.none) — an unknown snapshot must not cancel teamed runs;
          // Linear enforces access server-side.
          yield* installationRepo.put(install(testOrganizationId));
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.some(testIssueId),
            teamId: Option.some(teamId),
          });
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            workspacePath: Option.some(testPath("team-access-unknown")),
            ompSessionFile: Option.some(
              testPath("team-access-unknown/session.jsonl"),
            ),
          });

          yield* authority.processSession(testSessionId);
          expect(workerSpawnInputs).toHaveLength(1);
          const run = yield* runRepo.get(testSessionId);
          expect(Option.isSome(run)).toBe(true);
          if (Option.isSome(run)) {
            expect(run.value.terminalReason).not.toEqual(
              Option.some("Linear team access was removed"),
            );
          }
        }),
      ),
  );

  it.scopedLive("releases the persisted worker uid after cancellation", () =>
    withAuthority((db) =>
      Effect.gen(function* () {
        const authority = yield* SessionAuthority;
        const installationRepo = yield* InstallationRepo;
        const runRepo = yield* RunRepo;
        yield* installationRepo.put(install(testOrganizationId));
        yield* runRepo.create({
          sessionId: testSessionId,
          organizationId: testOrganizationId,
          issueId: Option.some(testIssueId),
        });
        db.query(
          "INSERT INTO worker_identity(session_id, uid, active) VALUES (?, 21001, 1)",
        ).run(testSessionId);
        db.query(
          "UPDATE agent_run SET desired_state='canceled' WHERE session_id=?",
        ).run(testSessionId);
        yield* authority.processSession(testSessionId);
        expect(
          db
            .query("SELECT active FROM worker_identity WHERE session_id=?")
            .get(testSessionId),
        ).toEqual({ active: 0 });
      }),
    ),
  );

  it.scopedLive("cancels when a known snapshot excludes the run's team", () =>
    withAuthority(() =>
      Effect.gen(function* () {
        const authority = yield* SessionAuthority;
        const installationRepo = yield* InstallationRepo;
        const runRepo = yield* RunRepo;
        const teamId = Schema.decodeUnknownSync(TeamId)("authority-team");
        yield* installationRepo.put({
          ...install(testOrganizationId),
          accessibleTeamIds: Option.some([]),
          canAccessAllPublicTeams: Option.some(false),
        });
        yield* runRepo.create({
          sessionId: testSessionId,
          organizationId: testOrganizationId,
          issueId: Option.some(testIssueId),
          teamId: Option.some(teamId),
        });
        yield* runRepo.update(testSessionId, {
          state: "orphaned",
          workspacePath: Option.some(testPath("team-access-known")),
          ompSessionFile: Option.some(
            testPath("team-access-known/session.jsonl"),
          ),
        });

        yield* authority.processSession(testSessionId);
        expect(workerSpawnInputs).toHaveLength(0);
        const run = yield* runRepo.get(testSessionId);
        expect(Option.isSome(run)).toBe(true);
        if (Option.isSome(run)) {
          expect(run.value.state).toBe("canceled");
          expect(run.value.terminalReason).toEqual(
            Option.some("Linear team access was removed"),
          );
        }
      }),
    ),
  );
  it.scopedLive(
    "cancels when only the public-team access snapshot is known and false",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          const teamId = Schema.decodeUnknownSync(TeamId)("authority-team");
          yield* installationRepo.put({
            ...install(testOrganizationId),
            accessibleTeamIds: Option.none(),
            canAccessAllPublicTeams: Option.some(false),
          });
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.some(testIssueId),
            teamId: Option.some(teamId),
          });
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            workspacePath: Option.some(testPath("team-access-partial")),
            ompSessionFile: Option.some(
              testPath("team-access-partial/session.jsonl"),
            ),
          });

          yield* authority.processSession(testSessionId);
          expect(workerSpawnInputs).toHaveLength(0);
          const run = yield* runRepo.get(testSessionId);
          expect(Option.isSome(run)).toBe(true);
          if (Option.isSome(run)) {
            expect(run.value.state).toBe("canceled");
            expect(run.value.terminalReason).toEqual(
              Option.some("Linear team access was removed"),
            );
          }
        }),
      ),
  );
});

describe("SessionAuthority infrastructure failures", () => {
  it.scopedLive(
    "keeps a run lookup database failure distinct from an absent run",
    () =>
      withAuthority((db) =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          yield* Effect.sync(() => db.exec("DROP TABLE agent_run"));

          const result = yield* Effect.either(
            authority.processSession(testSessionId),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left._tag).toBe("@Gateway/DatabaseError");
          }
        }),
      ),
  );

  it.scopedLive(
    "surfaces terminal state persistence failure instead of reporting terminal handling",
    () =>
      withAuthority((db) =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* Effect.sync(() =>
            db.exec(
              "CREATE TRIGGER reject_terminal_update BEFORE UPDATE ON agent_run WHEN NEW.state = 'failed' BEGIN SELECT RAISE(FAIL, 'terminal persistence failed'); END",
            ),
          );

          const result = yield* Effect.either(
            authority.processSession(testSessionId),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left._tag).toBe("@Gateway/DatabaseError");
          }
        }),
      ),
  );

  it.scopedLive(
    "propagates projection persistence failure after terminal state persistence",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          terminalFailure = new DatabaseError({ message: "projection failed" });

          const result = yield* Effect.either(
            authority.processSession(testSessionId),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left._tag).toBe("@Gateway/DatabaseError");
          }
        }),
      ),
  );

  it.scopedLive(
    "stops and unregisters a worker when host-tool registration fails",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          hostToolRegistrationError = new RpcProtocolError({
            method: "set_host_tools",
            message: "forced host-tool registration failure",
          });
          yield* Effect.promise(() =>
            mkdir(testPath("host-tool-registration"), { recursive: true }),
          );
          yield* installationRepo.put(install(testOrganizationId));
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            workspacePath: Option.some(testPath("host-tool-registration")),
            ompSessionFile: Option.some(
              testPath("host-tool-registration/session.jsonl"),
            ),
          });

          const result = yield* Effect.either(
            authority.processSession(testSessionId),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left).toMatchObject({
              _tag: "@Gateway/RpcProtocolError",
              method: "set_host_tools",
              message: "forced host-tool registration failure",
            });
          }
          expect(orderEvents).toContain("worker-stopped");
          expect(yield* authority.activeWorkerCount()).toBe(0);
        }),
      ),
  );

  it.scopedLive(
    "returns a typed error for an interrupted run without actionable input",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          yield* installationRepo.put(install(testOrganizationId));
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            workspacePath: Option.some(authorityTestRoot),
          });

          const result = yield* Effect.either(
            authority.processSession(testSessionId),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left).toBeInstanceOf(
              InterruptedRunNoActionableInputError,
            );
          }
        }),
      ),
  );

  it.scopedLive("surfaces a run-event persistence failure", () =>
    withAuthority((db) =>
      Effect.gen(function* () {
        const runEventRepo = yield* RunEventRepo;
        const runRepo = yield* RunRepo;
        yield* runRepo.create({
          sessionId: testSessionId,
          organizationId: testOrganizationId,
          issueId: Option.none(),
        });
        yield* Effect.sync(() =>
          db.exec(
            "CREATE TRIGGER reject_run_event BEFORE INSERT ON run_event BEGIN SELECT RAISE(FAIL, 'event persistence failed'); END",
          ),
        );

        const result = yield* Effect.either(
          runEventRepo.upsert({
            sourceKey: Schema.decodeUnknownSync(SourceKey)("authority-event"),
            sessionId: testSessionId,
            kind: "agent",
            level: "info",
            text: "event",
            payload: {},
            status: "observed",
            now: 0,
          }),
        );
        expect(Either.isLeft(result)).toBe(true);
        if (Either.isLeft(result)) {
          expect(result.left._tag).toBe("@Gateway/DatabaseError");
        }
      }),
    ),
  );

  it.scopedLive(
    "rolls back a run transition when its admin event cannot be persisted",
    () =>
      withAuthority((db) =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* Effect.sync(() =>
            db.exec(
              "CREATE TRIGGER reject_authority_state_event BEFORE INSERT ON run_event BEGIN SELECT RAISE(FAIL, 'state event persistence failed'); END",
            ),
          );

          const result = yield* Effect.either(
            authority.processSession(testSessionId),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left._tag).toBe("@Gateway/DatabaseError");
          }
          const run = yield* runRepo.get(testSessionId);
          expect(Option.isSome(run)).toBe(true);
          if (Option.isSome(run)) expect(run.value.state).toBe("queued");
        }),
      ),
  );

  it.scopedLive(
    "surfaces markProcessed failure after a successful worker action",
    () =>
      withAuthority((db) =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          const runInputRepo = yield* RunInputRepo;
          yield* installationRepo.put(install(testOrganizationId));
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* Effect.promise(() =>
            mkdir(testPath("mark-failure"), { recursive: true }),
          );
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            repositoryId: Option.some(
              Schema.decodeUnknownSync(WorkspaceId)("repo-for-mark-failure"),
            ),
            workspacePath: Option.some(testPath("mark-failure")),
            ompSessionFile: Option.some(testPath("mark-failure/session.jsonl")),
          });
          yield* authority.processSession(testSessionId);
          yield* runInputRepo.enqueue({
            id: Schema.decodeUnknownSync(InputId)("mark-failure-input"),
            sessionId: testSessionId,
            kind: "prompted",
            body: "continue",
            payload: {},
          });
          yield* Effect.sync(() =>
            db.exec(
              "CREATE TRIGGER reject_input_processed BEFORE UPDATE OF processed_at ON run_input WHEN NEW.processed_at IS NOT NULL BEGIN SELECT RAISE(FAIL, 'mark processed failed'); END",
            ),
          );

          const result = yield* Effect.either(
            authority.processSession(testSessionId),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left._tag).toBe("@Gateway/DatabaseError");
          }
          const run = yield* runRepo.get(testSessionId);
          expect(Option.isSome(run)).toBe(true);
          if (Option.isSome(run)) expect(run.value.state).toBe("orphaned");
          expect(yield* runInputRepo.pending(testSessionId)).toHaveLength(1);
        }),
      ),
  );

  it.scopedLive(
    "continues terminal handling when best-effort lease release fails",
    () =>
      withAuthority((db) =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          const runInputRepo = yield* RunInputRepo;
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* runInputRepo.enqueue({
            id: Schema.decodeUnknownSync(InputId)("lease-cleanup-input"),
            sessionId: testSessionId,
            kind: "stop",
            body: "stop",
            payload: {},
          });
          yield* Effect.sync(() =>
            db.exec(
              "CREATE TRIGGER reject_lease_release_success BEFORE UPDATE ON agent_run WHEN OLD.lease_owner IS NOT NULL AND NEW.lease_owner IS NULL BEGIN SELECT RAISE(FAIL, 'lease release failed'); END",
            ),
          );

          yield* authority.processSession(testSessionId);

          const run = yield* runRepo.get(testSessionId);
          expect(Option.isSome(run)).toBe(true);
          if (Option.isSome(run)) {
            expect(run.value.state).toBe("canceled");
            expect(run.value.desiredState).toBe("canceled");
          }
        }),
      ),
  );

  it.scopedLive(
    "keeps the primary projection failure when lease cleanup fails",
    () =>
      withAuthority((db) =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const runRepo = yield* RunRepo;
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* Effect.sync(() =>
            db.exec(
              "CREATE TRIGGER reject_lease_release BEFORE UPDATE ON agent_run WHEN NEW.lease_owner IS NULL BEGIN SELECT RAISE(FAIL, 'lease release failed'); END",
            ),
          );
          terminalFailure = new DatabaseError({
            message: "primary projection failure",
          });

          const result = yield* Effect.either(
            authority.processSession(testSessionId),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left.message).toBe("primary projection failure");
          }
        }),
      ),
  );
});
describe("SessionAuthority deferred stop handling", () => {
  it.scopedLive(
    "skips a deferred stop after repeated lookup failures instead of canceling",
    () =>
      withAuthority(
        () =>
          Effect.gen(function* () {
            const authority = yield* SessionAuthority;
            const installationRepo = yield* InstallationRepo;
            const runRepo = yield* RunRepo;
            const runInputRepo = yield* RunInputRepo;
            linearIssueLookupFails = true;
            yield* installationRepo.put(install(testOrganizationId));
            yield* runRepo.create({
              sessionId: testSessionId,
              organizationId: testOrganizationId,
              issueId: Option.some(testIssueId),
            });
            yield* runRepo.update(testSessionId, {
              state: "orphaned",
              workspacePath: Option.some(testPath("deferred-stop")),
              ompSessionFile: Option.some(
                testPath("deferred-stop/session.jsonl"),
              ),
            });
            const inputId = Schema.decodeUnknownSync(InputId)(
              "deferred-stop-input",
            );
            yield* runInputRepo.enqueue({
              id: inputId,
              sessionId: testSessionId,
              kind: "stop",
              body: "Linear issue status changed",
              payload: {
                type: "AppUserNotification",
                action: "issueStatusChanged",
                notification: { issueId: "issue-1" },
              },
            });

            for (let attempt = 0; attempt < 10; attempt += 1) {
              yield* authority.processSession(testSessionId);
            }

            const pending = yield* runInputRepo.pending(testSessionId);
            expect(pending).toHaveLength(0);
            const run = yield* runRepo.get(testSessionId);
            expect(Option.isSome(run)).toBe(true);
            if (Option.isSome(run)) {
              expect(run.value.state).not.toBe("canceled");
              expect(run.value.desiredState).toBe("running");
            }
          }),
        { withLinearGateway: true },
      ),
  );
});
describe("SessionAuthority host-tool mutation gate", () => {
  it.scopedLive("refuses mutating host tools once the run is canceled", () =>
    withAuthority(
      () =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          const runInputRepo = yield* RunInputRepo;
          yield* installationRepo.put(install(testOrganizationId));
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.some(testIssueId),
          });
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            workspacePath: Option.some(testPath("mutation-gate")),
            ompSessionFile: Option.some(
              testPath("mutation-gate/session.jsonl"),
            ),
          });

          yield* authority.processSession(testSessionId);
          expect(registeredHostTools.map((tool) => tool.name)).toEqual([
            "linear_get_issue",
            "linear_create_comment",
            "linear_update_issue",
            "linear_add_external_url",
          ]);
          expect(workerSpawnInputs).toHaveLength(1);
          const listener = hostToolListener;
          expect(listener).toBeDefined();
          if (listener === undefined) return;

          const allowed = yield* listener({
            id: "call-allowed",
            toolCallId: "tool-1",
            toolName: "linear_create_comment",
            arguments: { body: "first" },
          });
          expect(allowed.isError).not.toBe(true);
          expect(toolCommentCalls).toHaveLength(1);

          // Stop dominance: desired_state flips to canceled, then the
          // in-gate re-read must refuse further mutations.
          yield* runInputRepo.applyStop(testSessionId);
          const refused = yield* listener({
            id: "call-refused",
            toolCallId: "tool-2",
            toolName: "linear_create_comment",
            arguments: { body: "second" },
          });
          expect(refused.isError).toBe(true);
          expect(toolCommentCalls).toHaveLength(1);

          // Read tools still work after cancellation.
          const read = yield* listener({
            id: "call-read",
            toolCallId: "tool-3",
            toolName: "linear_get_issue",
            arguments: { issueId: "issue-1" },
          });
          expect(read.isError).not.toBe(true);
        }),
      { withLinearGateway: true },
    ),
  );
  it.scopedLive("normalizes external URLs before sending", () =>
    withAuthority(
      () =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          yield* installationRepo.put(install(testOrganizationId));
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.some(testIssueId),
          });
          yield* Effect.promise(() =>
            mkdir(testPath("external-url"), { recursive: true }),
          );
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            workspacePath: Option.some(testPath("external-url")),
            ompSessionFile: Option.some(testPath("external-url/session.jsonl")),
          });
          yield* authority.processSession(testSessionId);
          const listener = hostToolListener;
          expect(listener).toBeDefined();
          if (listener === undefined) return;

          const result = yield* listener({
            id: "call-url",
            toolCallId: "tool-url",
            toolName: "linear_add_external_url",
            arguments: {
              label: "Pull request",
              url: "  https://github.com/acme/project/pull/42  ",
            },
          });
          expect(result.isError).not.toBe(true);
          expect(toolExternalUrlCalls).toEqual([
            {
              sessionId: testSessionId,
              urls: [
                {
                  label: "Pull request",
                  url: "https://github.com/acme/project/pull/42",
                },
              ],
            },
          ]);
        }),
      { withLinearGateway: true },
    ),
  );

  it.scopedLive(
    "reports a pull request from the saved assistant draft at agent end",
    () =>
      withAuthority(
        () =>
          Effect.gen(function* () {
            const authority = yield* SessionAuthority;
            const installationRepo = yield* InstallationRepo;
            const runRepo = yield* RunRepo;
            yield* installationRepo.put(install(testOrganizationId));
            yield* runRepo.create({
              sessionId: testSessionId,
              organizationId: testOrganizationId,
              issueId: Option.some(testIssueId),
            });
            yield* Effect.promise(() =>
              mkdir(testPath("pr-draft"), { recursive: true }),
            );
            yield* runRepo.update(testSessionId, {
              state: "orphaned",
              workspacePath: Option.some(testPath("pr-draft")),
              ompSessionFile: Option.some(testPath("pr-draft/session.jsonl")),
            });
            yield* authority.processSession(testSessionId);
            expect(workerEventListener).toBeDefined();
            if (workerEventListener === undefined) return;

            const projected = yield* Deferred.make<void>();
            projectionWaiter = projected;
            projectionExpected = 2;
            workerEventListener({
              type: "message_end",
              message: {
                role: "assistant",
                content:
                  "Opened https://github.com/acme/project/pull/42 for review.",
              },
            });
            workerEventListener({ type: "agent_end" });
            yield* Deferred.await(projected);

            expect(toolExternalUrlCalls).toEqual([
              {
                sessionId: testSessionId,
                urls: [
                  {
                    label: "Pull request",
                    url: "https://github.com/acme/project/pull/42",
                  },
                ],
              },
            ]);
          }),
        { withLinearGateway: true },
      ),
  );

  it.scopedLive("cancel waits for an in-flight mutation holding the gate", () =>
    withAuthority(
      () =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          const runInputRepo = yield* RunInputRepo;
          yield* installationRepo.put(install(testOrganizationId));
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.some(testIssueId),
          });
          yield* Effect.promise(() =>
            mkdir(testPath("mutation-gate-order"), { recursive: true }),
          );
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            workspacePath: Option.some(testPath("mutation-gate-order")),
            ompSessionFile: Option.some(
              testPath("mutation-gate-order/session.jsonl"),
            ),
          });
          yield* authority.processSession(testSessionId);
          const listener = hostToolListener;
          expect(listener).toBeDefined();
          if (listener === undefined) return;

          // Block the mutation inside the gateway's Linear call while it
          // holds the session gate.
          commentEnteredSignal = yield* Deferred.make<void>();
          commentRelease = yield* Deferred.make<void>();
          const mutationFiber = yield* Effect.fork(
            listener({
              id: "call-blocked",
              toolCallId: "tool-blocked",
              toolName: "linear_create_comment",
              arguments: { body: "in-flight" },
            }),
          );
          yield* Deferred.await(commentEnteredSignal);

          // A user stop arrives while the in-flight mutation holds the gate.
          yield* runInputRepo.enqueue({
            id: Schema.decodeUnknownSync(InputId)(
              `${testSessionId}:stop:stop-1`,
            ),
            sessionId: testSessionId,
            kind: "stop",
            body: "Stop requested",
            payload: {},
          });
          const cancelFiber = yield* Effect.fork(
            authority.processSession(testSessionId),
          );
          yield* Effect.yieldNow();
          Deferred.unsafeDone(commentRelease, Effect.void);
          yield* Fiber.await(mutationFiber);
          yield* Fiber.join(cancelFiber);
          expect(orderEvents).toContain("worker-stopped");
          expect(orderEvents).toContain("comment-completed");
        }),
      { withLinearGateway: true },
    ),
  );
});

describe("SessionAuthority Nix environment preparation", () => {
  it.scopedLive(
    "prepares repository dependencies before the first worker and its orphan retry",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          const workspaceRepo = yield* WorkspaceRepo;
          const repositoryId = Schema.decodeUnknownSync(WorkspaceId)(
            "nix-authority-repository",
          );
          yield* installationRepo.put(install(testOrganizationId));
          yield* workspaceRepo.createRepository({
            organizationId: testOrganizationId,
            id: repositoryId,
            url: "https://example.com/repository.git",
            ref: "main",
            nixPackages: [Schema.decodeUnknownSync(NixPackageName)("nodejs")],
          });
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            repositoryId: Option.some(repositoryId),
            workspacePath: Option.some(testPath("nix-authority")),
            ompSessionFile: Option.some(
              testPath("nix-authority/session.jsonl"),
            ),
          });
          nixPathEntries = ["/nix/store/test-node/bin"];

          yield* authority.processSession(testSessionId);
          expect(nixPreparedRepositories).toEqual([repositoryId]);
          expect(workerSpawnInputs).toHaveLength(1);
          expect(workerSpawnInputs[0]?.env?.PATH).toBe(
            `/nix/store/test-node/bin${process.env.PATH ? `:${process.env.PATH}` : ""}`,
          );
          expect(workerSpawnInputs[0]?.env?.HOME).toBe(process.env.HOME);
          expect(workerSpawnInputs[0]?.env).not.toHaveProperty("NIX_REMOTE");
          expect(workerSpawnInputs[0]?.env).not.toHaveProperty("NIX_CONFIG");
          expect(workerSpawnInputs[0]?.env).not.toHaveProperty("NIX_PATH");
          expect(workerSpawnInputs[0]?.env).not.toHaveProperty("NIX_PROFILES");
          expect(workerSpawnInputs[0]?.command).toContain("--extension");
          expect(workerSpawnInputs[0]?.command).toContain(
            resolveDeviationExtensionPath(),
          );

          yield* authority.shutdown();
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            ompSessionFile: Option.some(
              testPath("nix-authority/session.jsonl"),
            ),
          });
          yield* authority.processSession(testSessionId);
          expect(nixPreparedRepositories).toEqual([repositoryId, repositoryId]);
          expect(workerSpawnInputs).toHaveLength(2);
        }),
      ),
  );

  it.scopedLive(
    "keeps the inherited environment unchanged for an empty dependency result",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          const workspaceRepo = yield* WorkspaceRepo;
          const repositoryId = Schema.decodeUnknownSync(WorkspaceId)(
            "nix-empty-dependencies",
          );
          yield* installationRepo.put(install(testOrganizationId));
          yield* workspaceRepo.createRepository({
            organizationId: testOrganizationId,
            id: repositoryId,
            url: "https://example.com/empty.git",
            ref: "main",
            nixPackages: [],
          });
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            repositoryId: Option.some(repositoryId),
            workspacePath: Option.some(testPath("nix-empty")),
            ompSessionFile: Option.some(testPath("nix-empty/session.jsonl")),
          });

          yield* authority.processSession(testSessionId);
          expect(nixPreparedRepositories).toEqual([repositoryId]);
          expect(workerSpawnInputs).toHaveLength(1);
          expect(workerSpawnInputs[0]?.env?.PATH).toBe(process.env.PATH);
          expect(workerSpawnInputs[0]?.env?.HOME).toBe(process.env.HOME);
        }),
      ),
  );

  it.scopedLive(
    "records a typed preparation failure without spawning an RPC worker",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const installationRepo = yield* InstallationRepo;
          const runRepo = yield* RunRepo;
          const workspaceRepo = yield* WorkspaceRepo;
          const repositoryId = Schema.decodeUnknownSync(WorkspaceId)(
            "nix-failing-dependencies",
          );
          yield* installationRepo.put(install(testOrganizationId));
          yield* workspaceRepo.createRepository({
            organizationId: testOrganizationId,
            id: repositoryId,
            url: "https://example.com/failing.git",
            ref: "main",
            nixPackages: [Schema.decodeUnknownSync(NixPackageName)("nodejs")],
          });
          yield* runRepo.create({
            sessionId: testSessionId,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* runRepo.update(testSessionId, {
            state: "orphaned",
            repositoryId: Option.some(repositoryId),
            workspacePath: Option.some(testPath("nix-failing")),
            ompSessionFile: Option.some(testPath("nix-failing/session.jsonl")),
          });
          yield* Effect.promise(() =>
            mkdir(testPath("nix-failing"), { recursive: true }),
          );
          nixPrepareError = new NixEnvironmentError({
            message: "Nix preparation failed",
            reason: "process_failed",
          });

          const result = yield* Effect.either(
            authority.processSession(testSessionId),
          );
          expect(Either.isLeft(result)).toBe(true);
          if (Either.isLeft(result)) {
            expect(result.left._tag).toBe("@Gateway/NixEnvironmentError");
          }
          expect(workerSpawnInputs).toHaveLength(0);
          const run = yield* runRepo.get(testSessionId);
          expect(Option.isSome(run)).toBe(true);
          if (Option.isSome(run)) {
            expect(run.value.state).toBe("orphaned");
            expect(run.value.terminalReason).toEqual(
              Option.some("Nix preparation failed"),
            );
          }
        }),
      ),
  );
});

const seedInterrupted = (
  sid: SessionId,
  state: "starting" | "running" | "waiting" | "orphaned" = "orphaned",
) =>
  Effect.gen(function* () {
    yield* InstallationRepo.put(install(testOrganizationId));
    yield* RunRepo.create({
      sessionId: sid,
      organizationId: testOrganizationId,
      issueId: Option.none(),
    });
    yield* RunRepo.update(sid, {
      state,
      workspacePath: Option.some(authorityTestRoot),
      ompSessionFile: Option.some(join(authorityTestRoot, "session.jsonl")),
    });
  });

describe("Recovery and concurrency regressions", () => {
  it.effect.each(["starting", "running", "waiting", "orphaned"] as const)(
    "exhausts retries for an interrupted %s run without actionable input",
    (state) =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          yield* seedInterrupted(testSessionId, state);
          yield* RunRepo.update(testSessionId, {
            ompSessionFile: Option.none(),
          });
          for (let attempt = 1; attempt <= 5; attempt++) {
            yield* authority.processRunnable();
            const run = yield* RunRepo.get(testSessionId);
            if (Option.isNone(run)) throw new Error("run missing");
            expect(run.value.attempt).toBe(attempt);
            expect(run.value.state).toBe(attempt === 5 ? "failed" : "orphaned");
            if (attempt < 5) {
              const next = Option.getOrThrow(run.value.nextAttemptAt);
              expect(next).toBeGreaterThan(yield* Clock.currentTimeMillis);
              yield* authority.processRunnable();
              expect(yield* RunRepo.get(testSessionId)).toEqual(run);
              yield* TestClock.setTime(next + 1);
            }
          }
          expect(
            Option.getOrThrow(yield* RunRepo.get(testSessionId)).leaseOwner,
          ).toEqual(Option.none());
        }),
      ),
  );

  it.effect(
    "recovers a starting run that has neither workspace nor pending input",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          yield* seedInterrupted(testSessionId, "starting");
          yield* RunRepo.update(testSessionId, {
            workspacePath: Option.none(),
          });
          yield* authority.processRunnable();
          const run = Option.getOrThrow(yield* RunRepo.get(testSessionId));
          expect(run.state).toBe("orphaned");
          expect(run.attempt).toBe(1);
          expect(Option.getOrNull(run.terminalReason)).toContain(
            "no workspace",
          );
        }),
      ),
  );

  it.scopedLive(
    "isolates materialization errors and continues to newer sessions",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          const bad = Schema.decodeUnknownSync(SessionId)("a-broken-clone");
          const good = Schema.decodeUnknownSync(SessionId)("z-newer-session");
          const repoId =
            Schema.decodeUnknownSync(WorkspaceId)("broken-local-repo");
          yield* InstallationRepo.put(install(testOrganizationId));
          yield* WorkspaceRepo.createRepository({
            organizationId: testOrganizationId,
            id: repoId,
            url: join(authorityTestRoot, "missing-repository"),
            ref: "main",
            isDefault: true,
          });
          yield* RunRepo.create({
            sessionId: bad,
            organizationId: testOrganizationId,
            issueId: Option.none(),
          });
          yield* RunInputRepo.enqueue({
            id: Schema.decodeUnknownSync(InputId)("clone-input"),
            sessionId: bad,
            kind: "created",
            body: "work",
            payload: { organizationId: testOrganizationId },
          });
          yield* seedInterrupted(good);
          yield* authority.processRunnable();
          const failed = Option.getOrThrow(yield* RunRepo.get(bad));
          expect(failed.state).toBe("orphaned");
          expect(failed.attempt).toBe(1);
          expect(Option.getOrNull(failed.terminalReason)).toContain(
            "Repository must be an HTTPS GitHub URL with a valid ref",
          );
          expect(Option.getOrThrow(yield* RunRepo.get(good)).state).toBe(
            "running",
          );
          expect(yield* authority.activeWorkerCount()).toBe(1);
        }),
      ),
  );

  it.effect.each(["canceled", "started", "unavailable"] as const)(
    "verifies a deferred %s stop before retry backoff and full worker capacity",
    (issueState) =>
      withAuthority(
        () =>
          Effect.gen(function* () {
            const authority = yield* SessionAuthority;
            const busy = Schema.decodeUnknownSync(SessionId)("busy-worker");
            yield* seedInterrupted(busy);
            yield* authority.processSession(busy);
            yield* seedInterrupted(testSessionId);
            const deadline = (yield* Clock.currentTimeMillis) + 60_000;
            yield* RunRepo.update(testSessionId, {
              nextAttemptAt: Option.some(deadline),
            });
            const inputId = Schema.decodeUnknownSync(InputId)("backoff-stop");
            yield* RunInputRepo.enqueue({
              id: inputId,
              sessionId: testSessionId,
              kind: "stop",
              body: "Issue status changed",
              payload: {
                type: "AppUserNotification",
                action: "issueStatusChanged",
                notification: { issueId: "issue-1" },
              },
            });
            linearIssueStateType = issueState;
            linearIssueLookupFails = issueState === "unavailable";
            yield* authority.processSession(testSessionId);
            const run = Option.getOrThrow(yield* RunRepo.get(testSessionId));
            expect(run.state).toBe(
              issueState === "canceled" ? "canceled" : "orphaned",
            );
            expect(run.desiredState).toBe(
              issueState === "canceled" ? "canceled" : "running",
            );
            expect(yield* RunInputRepo.pending(testSessionId)).toHaveLength(
              issueState === "unavailable" ? 1 : 0,
            );
            expect(run.attempt).toBe(0);
            expect(run.leaseOwner).toEqual(Option.none());
            expect(workerSpawnInputs).toHaveLength(1);
            expect(yield* authority.activeWorkerCount()).toBe(1);
          }),
        { withLinearGateway: true, config: [["MAX_CONCURRENT_WORKERS", "1"]] },
      ),
  );

  it.effect(
    "leaves a deferred stop untouched while another authority owns its lease",
    () =>
      withAuthority(
        (db) =>
          Effect.gen(function* () {
            yield* seedInterrupted(testSessionId);
            yield* RunInputRepo.enqueue({
              id: Schema.decodeUnknownSync(InputId)("foreign-owned-stop"),
              sessionId: testSessionId,
              kind: "stop",
              body: "Issue status changed",
              payload: {
                type: "AppUserNotification",
                action: "issueStatusChanged",
                notification: { issueId: "issue-1" },
              },
            });
            db.query(
              "UPDATE agent_run SET lease_owner='other-authority', lease_expires_at=? WHERE session_id=?",
            ).run((yield* Clock.currentTimeMillis) + 60_000, testSessionId);
            linearIssueStateType = "canceled";
            const before = yield* RunRepo.get(testSessionId);
            yield* SessionAuthority.processSession(testSessionId);
            expect(yield* RunRepo.get(testSessionId)).toEqual(before);
            expect(yield* RunInputRepo.pending(testSessionId)).toHaveLength(1);
          }),
        { withLinearGateway: true },
      ),
  );

  it.effect.each(["matching", "mismatched", "missing", "invalid"] as const)(
    "uses only the current retry event with a %s schedule",
    (schedule) =>
      withAuthority((db) =>
        Effect.gen(function* () {
          yield* seedInterrupted(testSessionId);
          const repositoryId =
            Schema.decodeUnknownSync(WorkspaceId)("targeted-retry");
          yield* WorkspaceRepo.createRepository({
            organizationId: testOrganizationId,
            id: repositoryId,
            url: "https://example.invalid/unused",
            ref: "main",
          });
          yield* RunRepo.update(testSessionId, {
            repositoryId: Option.some(repositoryId),
          });
          const deadline = (yield* Clock.currentTimeMillis) + 60_000;
          yield* RunRepo.update(testSessionId, {
            nextAttemptAt: Option.some(deadline),
          });
          if (schedule !== "missing")
            yield* RunEventRepo.upsert({
              sourceKey: Schema.decodeUnknownSync(SourceKey)(
                `retry:${testSessionId}:0`,
              ),
              sessionId: testSessionId,
              kind: "retry",
              level: "info",
              payload: {
                nextAttemptAt:
                  schedule === "mismatched" ? deadline + 1 : deadline,
                scheduledAt: schedule === "invalid" ? "bad" : 0,
              },
            });
          if (schedule === "missing") {
            db.query("DELETE FROM run_event WHERE source_key=?").run(
              `retry:${testSessionId}:0`,
            );
          }
          // An unrelated, unreadable payload must not be loaded on a retry tick.
          yield* RunEventRepo.upsert({
            sourceKey: Schema.decodeUnknownSync(SourceKey)(
              `rpc:${testSessionId}:7:progress`,
            ),
            sessionId: testSessionId,
            kind: "progress",
            level: "info",
          });
          db.query(
            "UPDATE run_event SET payload_json='invalid' WHERE kind='progress'",
          ).run();
          yield* RunInputRepo.enqueue({
            id: Schema.decodeUnknownSync(InputId)("fresh-prompt"),
            sessionId: testSessionId,
            kind: "prompted",
            body: "Continue",
            payload: {},
            createdAt: 1,
          });
          yield* SessionAuthority.processSession(testSessionId);
          expect(workerSpawnInputs).toHaveLength(
            schedule === "matching" ? 1 : 0,
          );
          expect(
            Option.getOrThrow(yield* RunRepo.get(testSessionId)).attempt,
          ).toBe(schedule === "matching" ? 1 : 0);
        }),
      ),
  );

  it.effect("seeds event sequence from persisted events after restart", () =>
    withAuthority((db) =>
      Effect.gen(function* () {
        const authority = yield* SessionAuthority;
        yield* seedInterrupted(testSessionId);
        yield* RunEventRepo.upsert({
          sessionId: testSessionId,
          sourceKey: Schema.decodeUnknownSync(SourceKey)(
            `rpc:${testSessionId}:40:progress`,
          ),
          kind: "progress",
          level: "info",
          payload: { type: "progress", message: "old" },
        });
        // Sequence seeding must read keys without decoding historical payloads.
        db.query(
          "UPDATE run_event SET payload_json='unreadable history' WHERE source_key=?",
        ).run(`rpc:${testSessionId}:40:progress`);
        for (const sequence of [41, 42]) {
          yield* authority.processSession(testSessionId);
          const observed = yield* Deferred.make<void>();
          projectionWaiter = observed;
          projectionExpected = sequence - 40;
          if (workerEventListener === undefined)
            throw new Error("listener missing");
          workerEventListener({ type: "progress", message: `new-${sequence}` });
          yield* Deferred.await(observed);
          expect(projectorEvents.at(-1)?.sequence).toBe(sequence);
          yield* authority.shutdown();
          yield* RunRepo.update(testSessionId, {
            state: "orphaned",
            ompSessionFile: Option.some(
              join(authorityTestRoot, "session.jsonl"),
            ),
          });
        }
        db.query("UPDATE run_event SET payload_json=? WHERE source_key=?").run(
          JSON.stringify({ type: "progress", message: "old" }),
          `rpc:${testSessionId}:40:progress`,
        );
        const events = (yield* RunEventRepo.list(testSessionId)).filter(
          (event) => event.kind === "progress",
        );
        expect(events).toHaveLength(3);
        expect(
          events.find((event) => event.sourceKey.endsWith(":40:progress"))
            ?.payload,
        ).toEqual({ type: "progress", message: "old" });
      }),
    ),
  );

  it.effect("drops a crashed worker and retries it with backoff", () =>
    withAuthority(() =>
      Effect.gen(function* () {
        const authority = yield* SessionAuthority;
        yield* seedInterrupted(testSessionId);
        yield* authority.processSession(testSessionId);
        if (workerEventListener === undefined)
          throw new Error("listener missing");
        workerEventListener({
          type: "worker_exited",
          message: "worker exited with code 17",
        });
        yield* Effect.repeat(Effect.yieldNow(), {
          until: () => orderEvents.includes("worker-stopped"),
        });
        yield* Effect.repeat(Effect.yieldNow(), {
          until: () => workerEventListener === undefined,
        });
        // Wait for the event consumer's failure transaction, without a real clock.
        yield* Effect.repeat(RunRepo.get(testSessionId), {
          until: (run) => Option.isSome(run) && run.value.state === "orphaned",
        });
        expect(yield* authority.activeWorkerCount()).toBe(0);
        const run = Option.getOrThrow(yield* RunRepo.get(testSessionId));
        expect(run.attempt).toBe(1);
        expect(Option.getOrNull(run.terminalReason)).toContain("code 17");
        expect(Option.isSome(run.nextAttemptAt)).toBe(true);
      }),
    ),
  );

  it.effect(
    "leaves waiting runs without workspaces idle until actionable input arrives",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          yield* seedInterrupted(testSessionId);
          yield* RunRepo.update(testSessionId, {
            state: "waiting",
            workspacePath: Option.none(),
            ompSessionFile: Option.none(),
          });
          for (let tick = 0; tick < 4; tick++)
            yield* SessionAuthority.processRunnable();
          const run = Option.getOrThrow(yield* RunRepo.get(testSessionId));
          expect(run.state).toBe("waiting");
          expect(run.attempt).toBe(0);
          expect(run.terminalReason).toEqual(Option.none());
          expect(workerSpawnInputs).toHaveLength(0);
          const repositoryId =
            Schema.decodeUnknownSync(WorkspaceId)("waiting-repository");
          yield* WorkspaceRepo.createRepository({
            organizationId: testOrganizationId,
            id: repositoryId,
            url: "https://github.com/fixture/waiting.git",
            ref: "main",
          });
          yield* RunRepo.update(testSessionId, {
            workspacePath: Option.some(authorityTestRoot),
            repositoryId: Option.some(repositoryId),
          });
          yield* RunInputRepo.enqueue({
            id: Schema.decodeUnknownSync(InputId)("waiting-prompt"),
            sessionId: testSessionId,
            kind: "prompted",
            body: "Start working",
            payload: {},
          });
          yield* SessionAuthority.processSession(testSessionId);
          expect(workerSpawnInputs).toHaveLength(1);
          expect(
            Option.getOrThrow(yield* RunRepo.get(testSessionId)).state,
          ).toBe("running");
        }),
      ),
  );

  it.effect(
    "caps concurrent workers and admits a waiting session when capacity is released",
    () =>
      withAuthority(
        () =>
          Effect.gen(function* () {
            const authority = yield* SessionAuthority;
            const next = Schema.decodeUnknownSync(SessionId)("second-worker");
            yield* seedInterrupted(testSessionId);
            yield* seedInterrupted(next);
            yield* Effect.all(
              [
                authority.processSession(testSessionId),
                authority.processSession(next),
              ],
              { concurrency: "unbounded" },
            );
            expect(workerSpawnInputs).toHaveLength(1);
            expect(yield* authority.activeWorkerCount()).toBe(1);
            const first = Option.getOrThrow(yield* RunRepo.get(testSessionId));
            const second = Option.getOrThrow(yield* RunRepo.get(next));
            const blocked = first.state === "running" ? next : testSessionId;
            expect(
              [first.state, second.state].filter(
                (state) => state === "running",
              ),
            ).toHaveLength(1);
            expect(
              Option.getOrThrow(yield* RunRepo.get(blocked)).state,
            ).not.toBe("running");
            yield* authority.shutdown();
            yield* authority.processSession(blocked);
            expect(workerSpawnInputs).toHaveLength(2);
            expect(Option.getOrThrow(yield* RunRepo.get(blocked)).state).toBe(
              "running",
            );
          }),
        { config: [["MAX_CONCURRENT_WORKERS", "1"]] },
      ),
  );

  it.effect(
    "renews healthy workers while another session is blocked preparing Nix",
    () =>
      withAuthority(
        () =>
          Effect.gen(function* () {
            const authority = yield* SessionAuthority;
            yield* seedInterrupted(testSessionId);
            yield* authority.processSession(testSessionId);
            const blocked =
              Schema.decodeUnknownSync(SessionId)("slow-preparation");
            const repoId = Schema.decodeUnknownSync(WorkspaceId)("slow-nix");
            yield* seedInterrupted(blocked);
            yield* WorkspaceRepo.createRepository({
              organizationId: testOrganizationId,
              id: repoId,
              url: "https://example.invalid/unused",
              ref: "main",
            });
            yield* RunRepo.update(blocked, {
              repositoryId: Option.some(repoId),
            });
            const entered = yield* Deferred.make<void>();
            const release = yield* Deferred.make<void>();
            nixPrepareBlock = { entered, release };
            const preparation = yield* Effect.fork(
              authority.processSession(blocked),
            );
            yield* Deferred.await(entered);
            for (let step = 0; step < 12; step++) yield* TestClock.adjust(100);
            const now = yield* Clock.currentTimeMillis;
            const run = Option.getOrThrow(yield* RunRepo.get(testSessionId));
            expect(Option.getOrThrow(run.leaseExpiresAt)).toBeGreaterThan(now);
            expect(yield* authority.activeWorkerCount()).toBe(1);
            yield* Deferred.succeed(release, undefined);
            yield* Fiber.join(preparation);
            expect(yield* authority.activeWorkerCount()).toBe(2);
          }),
        { config: [["LEASE_DURATION_MS", "600"]] },
      ),
  );

  it.effect("layer finalization shuts down workers and releases leases", () =>
    Effect.gen(function* () {
      yield* withAuthority(
        () =>
          Effect.gen(function* () {
            const authority = yield* SessionAuthority;
            yield* seedInterrupted(testSessionId);
            yield* authority.processSession(testSessionId);
            expect(orderEvents).not.toContain("worker-stopped");
          }),
        {
          afterShutdown: (db) => {
            const row = db
              .query("SELECT lease_owner FROM agent_run WHERE session_id=?")
              .get(testSessionId);
            expect(row).toEqual({ lease_owner: null });
          },
        },
      );
      expect(orderEvents).toContain("worker-stopped");
      expect(workerEventListener).toBeUndefined();
    }),
  );
});

describe("Authority lease loss", () => {
  it.effect(
    "refuses a late success event after another process takes over",
    () =>
      withAuthority((db) =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          yield* seedInterrupted(testSessionId);
          yield* authority.processSession(testSessionId);
          const now = yield* Clock.currentTimeMillis;
          db.query(
            "UPDATE agent_run SET lease_owner='replacement', lease_expires_at=? WHERE session_id=?",
          ).run(now + 60_000, testSessionId);
          if (workerEventListener === undefined)
            throw new Error("listener missing");
          workerEventListener({ type: "agent_end" });
          yield* Effect.repeat(authority.activeWorkerCount(), {
            until: (count) => count === 0,
          });
          expect(
            Option.getOrThrow(yield* RunRepo.get(testSessionId)).state,
          ).toBe("running");
          expect(
            Option.getOrThrow(yield* RunRepo.get(testSessionId)).leaseOwner,
          ).toEqual(Option.some("replacement"));
          expect(yield* authority.activeWorkerCount()).toBe(0);
          expect(
            (yield* RunEventRepo.list(testSessionId)).some((event) =>
              Option.getOrElse(event.text, () => "").includes("succeeded"),
            ),
          ).toBe(false);
        }),
      ),
  );
});

describe("Recovery admission fencing", () => {
  it.effect("doesn't spawn from a preparation whose lease was replaced", () =>
    withAuthority((db) =>
      Effect.gen(function* () {
        const authority = yield* SessionAuthority;
        yield* seedInterrupted(testSessionId);
        const repoId =
          Schema.decodeUnknownSync(WorkspaceId)("preparation-fence");
        yield* WorkspaceRepo.createRepository({
          organizationId: testOrganizationId,
          id: repoId,
          url: "https://example.invalid/unused",
          ref: "main",
        });
        yield* RunRepo.update(testSessionId, {
          repositoryId: Option.some(repoId),
        });
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        nixPrepareBlock = { entered, release };
        const preparing = yield* Effect.fork(
          authority.processSession(testSessionId).pipe(Effect.either),
        );
        yield* Deferred.await(entered);
        db.query(
          "UPDATE agent_run SET lease_owner='replacement' WHERE session_id=?",
        ).run(testSessionId);
        yield* Deferred.succeed(release, undefined);
        expect(Either.isLeft(yield* Fiber.join(preparing))).toBe(true);
        expect(workerSpawnInputs).toEqual([]);
        expect(
          Option.getOrThrow(yield* RunRepo.get(testSessionId)).leaseOwner,
        ).toEqual(Option.some("replacement"));
      }),
    ),
  );

  it.effect(
    "fails an interrupted final attempt without starting a sixth worker",
    () =>
      withAuthority((db) =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          yield* seedInterrupted(testSessionId);
          db.query("UPDATE agent_run SET attempt=5 WHERE session_id=?").run(
            testSessionId,
          );
          yield* authority.processRunnable();
          const run = Option.getOrThrow(yield* RunRepo.get(testSessionId));
          expect(run.state).toBe("failed");
          expect(run.attempt).toBe(5);
          expect(workerSpawnInputs).toEqual([]);
        }),
      ),
  );
});

describe("Recovery review regressions", () => {
  it.effect(
    "serializes new input behind worker teardown and the failure transition",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          yield* seedInterrupted(testSessionId);
          yield* authority.processSession(testSessionId);
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          workerStopBlock = { entered, release };
          if (workerEventListener === undefined)
            throw new Error("worker listener missing");
          workerEventListener({
            type: "worker_exited",
            message: "worker failed",
          });
          yield* Deferred.await(entered);
          expect(yield* authority.activeWorkerCount()).toBe(1);
          // This prompt predates the scheduled retry and must remain under backoff.
          yield* RunInputRepo.enqueue({
            id: Schema.decodeUnknownSync(InputId)("during-teardown"),
            sessionId: testSessionId,
            kind: "prompted",
            body: "continue",
            payload: {},
            createdAt: yield* Clock.currentTimeMillis,
          });
          const processing = yield* Effect.fork(
            authority.processSession(testSessionId),
          );
          yield* Effect.yieldNow();
          expect(Option.isNone(yield* Fiber.poll(processing))).toBe(true);
          expect(workerSpawnInputs).toHaveLength(1);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(processing);
          expect(workerSpawnInputs).toHaveLength(1);
          expect(yield* authority.activeWorkerCount()).toBe(0);
          expect(
            Option.getOrThrow(yield* RunRepo.get(testSessionId)).state,
          ).toBe("orphaned");
        }),
      ),
  );

  it.effect(
    "only fresh actionable input bypasses the persisted retry deadline",
    () =>
      withAuthority(() =>
        Effect.gen(function* () {
          const authority = yield* SessionAuthority;
          yield* seedInterrupted(testSessionId);
          const repositoryId =
            Schema.decodeUnknownSync(WorkspaceId)("retry-workspace");
          yield* WorkspaceRepo.createRepository({
            organizationId: testOrganizationId,
            id: repositoryId,
            url: "https://example.invalid/retry",
            ref: "main",
          });
          yield* RunRepo.update(testSessionId, {
            repositoryId: Option.some(repositoryId),
          });
          yield* TestClock.adjust(1000);
          yield* RunInputRepo.enqueue({
            id: Schema.decodeUnknownSync(InputId)("old-retry-input"),
            sessionId: testSessionId,
            kind: "prompted",
            body: "old input",
            payload: {},
            createdAt: 1000,
          });
          yield* RunRepo.update(testSessionId, {
            state: "orphaned",
            nextAttemptAt: Option.some(300000),
          });
          // A renewal changes updatedAt after scheduling and must not reset the cutoff.
          yield* TestClock.adjust(1000);
          yield* RunRepo.claimLease(testSessionId, "foreign", 10);
          yield* RunRepo.releaseLease(testSessionId, "foreign");
          yield* authority.processSession(testSessionId);
          expect(workerSpawnInputs).toHaveLength(0);
          yield* RunInputRepo.enqueue({
            id: Schema.decodeUnknownSync(InputId)("fresh-retry-input"),
            sessionId: testSessionId,
            kind: "prompted",
            body: "new input",
            payload: {},
            createdAt: 1500,
          });
          yield* authority.processSession(testSessionId);
          expect(workerSpawnInputs).toHaveLength(1);
          expect(
            Option.getOrThrow(yield* RunRepo.get(testSessionId)).state,
          ).toBe("running");
        }),
      ),
  );
});
