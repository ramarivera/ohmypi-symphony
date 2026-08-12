import { Effect, Schema } from "effect";
import type { OrganizationId } from "../domain/ids.js";
import { ExecutorInstanceRepo } from "./store/executor-instance-repo.js";

export class ExecutorRequestError extends Schema.TaggedError<ExecutorRequestError>()(
  "@Gateway/ExecutorRequestError",
  {
    reason: Schema.Literal(
      "not_configured",
      "invalid_endpoint",
      "timeout",
      "http",
      "invalid_response",
    ),
    message: Schema.String,
  },
) {}

const Toolkit = Schema.Struct({
  id: Schema.String,
  owner: Schema.String,
  slug: Schema.String,
  name: Schema.String,
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
});
const ToolkitResponse = Schema.Struct({ toolkits: Schema.Array(Toolkit) });
export type ExecutorToolkit = typeof Toolkit.Type;

export const executorUrlForPath = (base: string, path: string): string => {
  const endpoint = new URL(base);
  const basePath = endpoint.pathname.replace(/\/+$/u, "");
  endpoint.pathname = `${basePath}/${path.replace(/^\/+/u, "")}`;
  return endpoint.toString();
};

const endpointFor = (base: string): string =>
  executorUrlForPath(base, "api/toolkits");

export class Executor extends Effect.Service<Executor>()("Executor", {
  accessors: true,
  dependencies: [ExecutorInstanceRepo.Default],
  effect: Effect.gen(function* () {
    const instances = yield* ExecutorInstanceRepo;

    const listToolkits = Effect.fn("Executor.listToolkits")(function* (
      organizationId: OrganizationId,
    ): Effect.fn.Return<ReadonlyArray<ExecutorToolkit>, ExecutorRequestError> {
      const instance = yield* instances.get(organizationId).pipe(
        Effect.mapError(
          (error) =>
            new ExecutorRequestError({
              reason: "http",
              message: String(error),
            }),
        ),
      );
      if (instance._tag === "None") {
        return yield* Effect.fail(
          new ExecutorRequestError({
            reason: "not_configured",
            message: "Executor is not configured",
          }),
        );
      }
      let endpoint: URL;
      try {
        endpoint = new URL(instance.value.endpoint);
      } catch {
        return yield* Effect.fail(
          new ExecutorRequestError({
            reason: "invalid_endpoint",
            message: "Executor endpoint must be a URL",
          }),
        );
      }
      if (endpoint.protocol !== "https:") {
        return yield* Effect.fail(
          new ExecutorRequestError({
            reason: "invalid_endpoint",
            message: "Executor endpoint must use https",
          }),
        );
      }
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 5000);
      const isAbortError = (error: unknown): boolean =>
        (error instanceof Error && error.name === "AbortError") ||
        String(error).startsWith("AbortError");
      const result = yield* Effect.gen(function* () {
        const response = yield* Effect.tryPromise({
          try: () =>
            fetch(endpointFor(endpoint.toString()), {
              headers: {
                Authorization: `Bearer ${instance.value.token}`,
                accept: "application/json",
              },
              signal: controller.signal,
            }),
          catch: (error) =>
            new ExecutorRequestError({
              reason: isAbortError(error) ? "timeout" : "http",
              message: String(error),
            }),
        });
        if (!response.ok) {
          return yield* Effect.fail(
            new ExecutorRequestError({
              reason: "http",
              message: `Executor returned HTTP ${response.status}`,
            }),
          );
        }
        const body = yield* Effect.tryPromise({
          try: () => response.json(),
          catch: (error) =>
            new ExecutorRequestError({
              reason: isAbortError(error) ? "timeout" : "invalid_response",
              message: String(error),
            }),
        });
        return yield* Schema.decodeUnknown(ToolkitResponse)(body).pipe(
          Effect.map((decoded) => decoded.toolkits),
          Effect.mapError(
            (error) =>
              new ExecutorRequestError({
                reason: "invalid_response",
                message: String(error),
              }),
          ),
        );
      }).pipe(Effect.ensuring(Effect.sync(() => clearTimeout(timeout))));
      return result;
    });
    return { listToolkits };
  }),
}) {}
