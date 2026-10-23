import { describe, expect, it } from "@effect/vitest";
import { Effect, Redacted } from "effect";
import { makeT3Driver } from "../src/drivers/t3/driver.js";
import { ensureT3Project } from "../src/drivers/t3/projects.js";
import { FakeT3Server } from "./fixtures/fake-t3.js";

const withServer = Effect.acquireRelease(
  Effect.sync(() => new FakeT3Server()),
  (server) => Effect.sync(() => server.stop()),
);
const options = (
  server: FakeT3Server,
  workspaceRoot = "/app/workspaces/issue-1",
) => ({
  baseUrl: server.baseUrl,
  token: Effect.succeed(Redacted.make(server.token)),
  workspaceRoot,
  title: "Linear issue 1",
  idNamespace: "sym",
  requestTimeoutMs: 1_000,
});

describe("T3 v0.1.2 project provisioning", () => {
  it.live(
    "creates an existing workspace project without scripts, and reuses it",
    () =>
      Effect.gen(function* () {
        const server = yield* withServer;
        const first = yield* ensureT3Project(options(server));
        expect(yield* ensureT3Project(options(server))).toBe(first);
        expect(server.projectMutations).toEqual([
          {
            type: "project.create",
            commandId: `${first}:create`,
            projectId: first,
            title: "Linear issue 1",
            workspaceRoot: "/app/workspaces/issue-1",
            createWorkspaceRootIfMissing: false,
            scripts: [],
          },
        ]);
      }),
  );
  it.live("adopts a pre-existing project with a different id", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      server.projects.set("human-project", {
        id: "human-project",
        workspaceRoot: "/app/workspaces/issue-1",
        deletedAt: null,
      });
      expect(yield* ensureT3Project(options(server))).toBe("human-project");
      expect(server.projectMutations).toHaveLength(0);
    }),
  );
  it.live("reconciles a lost mutation response and concurrent creation", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      server.loseProjectMutationResponse = true;
      const ids = yield* Effect.all(
        [ensureT3Project(options(server)), ensureT3Project(options(server))],
        { concurrency: 2 },
      );
      expect(new Set(ids).size).toBe(1);
      expect(server.projects.size).toBe(1);
    }),
  );
  it.live(
    "rejects auth failures and denied mutations without launching a thread",
    () =>
      Effect.gen(function* () {
        const server = yield* withServer;
        const denied = yield* ensureT3Project({
          ...options(server),
          token: Effect.succeed(Redacted.make("wrong")),
        }).pipe(Effect.flip);
        expect(denied.reason).toBe("auth");
        expect(server.projectMutations).toHaveLength(0);
        server.projectMutationStatus = 403;
        const error = yield* ensureT3Project(options(server)).pipe(Effect.flip);
        expect(error.reason).toBe("auth");
        expect(server.threads.size).toBe(0);
      }),
  );
  it.live(
    "provisions the run workspace before launching a thread without a configured project",
    () =>
      Effect.gen(function* () {
        const server = yield* withServer;
        const driver = makeT3Driver({
          ...options(server),
          modelSelection: { instanceId: "codex-cliproxy", model: "gpt-6-sol" },
          runtimeMode: "auto",
          interactionMode: "default",
        });
        const session = yield* driver.open({
          sessionKey: "issue-1",
          title: "Issue 1",
          workspace: { _tag: "Directory", path: "/app/workspaces/issue-1" },
        });
        const projectId = server.projectMutations[0]?.projectId;
        expect(session.ref.locator.projectId).toBe(projectId);
        expect(
          server.requestsFor("orchestration.launchThread")[0]?.payload
            .projectId,
        ).toBe(projectId);
        expect(
          server.requestsFor("orchestration.launchThread")[0]?.payload
            .runtimeMode,
        ).toBe("auto");
      }),
  );
  it.live("rejects relative workspace paths before making a mutation", () =>
    Effect.gen(function* () {
      const server = yield* withServer;
      const error = yield* ensureT3Project(
        options(server, "relative/workspace"),
      ).pipe(Effect.flip);
      expect(error.reason).toBe("protocol");
      expect(server.projectMutations).toHaveLength(0);
    }),
  );
});
