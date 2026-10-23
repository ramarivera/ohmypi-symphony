/** T3 v0.1.2 (044f0725): contracts/project.ts and environmentHttp.ts.
 * GET /api/projects returns ProjectSnapshot, POST /api/projects/mutate accepts
 * ProjectMutation and returns Project. Both use the orchestration audience.
 * No scripts, directory creation, or extra scopes are required.
 */
import { createHash } from "node:crypto";
import { isAbsolute, resolve } from "node:path";
import { Effect, Redacted, Schema } from "effect";
import {
  httpJson,
  T3RpcError as RpcError,
  type T3RpcError,
  t3Endpoint,
} from "./rpc-client.js";

const Project = Schema.Struct({
  id: Schema.String,
  workspaceRoot: Schema.String,
  deletedAt: Schema.NullOr(Schema.String),
});
const Snapshot = Schema.Struct({ projects: Schema.Array(Project) });

export const ensureT3Project = (options: {
  readonly baseUrl: URL;
  readonly token: Effect.Effect<Redacted.Redacted<string>, T3RpcError>;
  readonly workspaceRoot: string;
  readonly title: string;
  readonly idNamespace: string;
  readonly requestTimeoutMs?: number;
}) =>
  Effect.gen(function* () {
    const { workspaceRoot } = options;
    const fail = (message: string) =>
      new RpcError({
        reason: "protocol",
        method: "projects",
        sent: false,
        message,
      });
    if (!isAbsolute(workspaceRoot) || resolve(workspaceRoot) !== workspaceRoot)
      return yield* fail("T3 workspace must be a normalized absolute path");
    const request = (path: string, body?: unknown) =>
      Effect.gen(function* () {
        const token = yield* options.token;
        return yield* httpJson(
          "projects",
          t3Endpoint(options.baseUrl, path),
          {
            method: body === undefined ? "GET" : "POST",
            headers: {
              authorization: `Bearer ${Redacted.value(token)}`,
              "content-type": "application/json",
            },
            ...(body === undefined ? {} : { body: JSON.stringify(body) }),
          },
          options.requestTimeoutMs ?? 30_000,
        );
      });
    const list = request("/api/projects").pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Snapshot)),
      Effect.mapError((error) =>
        error instanceof RpcError ? error : fail("Invalid T3 project snapshot"),
      ),
    );
    const find = list.pipe(
      Effect.map((snapshot) =>
        snapshot.projects.find(
          (project) =>
            project.workspaceRoot === workspaceRoot &&
            project.deletedAt === null,
        ),
      ),
    );
    const existing = yield* find;
    if (existing !== undefined) return existing.id;
    const projectId = `${options.idNamespace}-project-${createHash("sha256").update(workspaceRoot).digest("hex").slice(0, 32)}`;
    const created = yield* request("/api/projects/mutate", {
      type: "project.create",
      commandId: `${projectId}:create`,
      projectId,
      title: options.title.trim() || projectId,
      workspaceRoot,
      createWorkspaceRootIfMissing: false,
      scripts: [],
    }).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Project)),
      Effect.mapError((error) =>
        error instanceof RpcError
          ? error
          : fail("Invalid T3 project mutation result"),
      ),
      // Concurrent creation or a lost POST response: adopt only a matching active
      // workspace. Never blindly create a second project or change its scripts.
      Effect.catch((error) =>
        find.pipe(
          Effect.flatMap((found) =>
            found === undefined ? Effect.fail(error) : Effect.succeed(found),
          ),
        ),
      ),
    );
    if (created.workspaceRoot !== workspaceRoot || created.deletedAt !== null)
      return yield* fail(
        "T3 returned a different or deleted workspace project",
      );
    return created.id;
  });
