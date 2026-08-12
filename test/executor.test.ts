import { Effect, Layer, Option, Schema } from "effect";
import { describe, expect, it, vi } from "vitest";
import { OrganizationId } from "../src/domain/ids.js";
import { Executor } from "../src/services/executor.js";
import { ExecutorInstanceRepo } from "../src/services/store/executor-instance-repo.js";

const organizationId = Schema.decodeUnknownSync(OrganizationId)("org_executor");
type ExecutorInstanceFixture = {
  readonly organizationId: typeof organizationId;
  readonly endpoint: string;
  readonly token: string;
  readonly updatedAt: number;
};
const instance = (endpoint: string): ExecutorInstanceFixture => ({
  organizationId,
  endpoint,
  token: "token",
  updatedAt: 1,
});
const unused = (..._args: ReadonlyArray<unknown>) => Effect.never;

const runList = (
  configured: Option.Option<ExecutorInstanceFixture>,
): Promise<ReadonlyArray<unknown>> => {
  const repo = ExecutorInstanceRepo.make({
    get: () => Effect.succeed(configured),
    put: unused,
    remove: unused,
  });
  const layer = Executor.DefaultWithoutDependencies.pipe(
    Layer.provide(Layer.succeed(ExecutorInstanceRepo, repo)),
  );
  return Effect.runPromise(
    Effect.gen(function* () {
      const executor = yield* Executor;
      return yield* executor.listToolkits(organizationId);
    }).pipe(Effect.provide(layer)),
  ) as Promise<ReadonlyArray<unknown>>;
};

const validResponse = {
  toolkits: [
    {
      id: "id",
      owner: "owner",
      slug: "toolkit",
      name: "Toolkit",
      createdAt: 1,
      updatedAt: 2,
    },
  ],
};

describe("Executor.listToolkits", () => {
  it("reports not configured without making a request", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    await expect(runList(Option.none())).rejects.toThrow(
      "Executor is not configured",
    );
    expect(fetchMock).not.toHaveBeenCalled();
    vi.unstubAllGlobals();
  });

  it("rejects non-HTTPS endpoints", async () => {
    vi.stubGlobal("fetch", vi.fn());
    await expect(
      runList(Option.some(instance("http://executor.example"))),
    ).rejects.toThrow("must use https");
    vi.unstubAllGlobals();
  });

  it("maps request timeout errors", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockRejectedValue(new DOMException("timed out", "AbortError")),
    );
    await expect(
      runList(Option.some(instance("https://executor.example"))),
    ).rejects.toThrow("AbortError: timed out");
    vi.unstubAllGlobals();
  });

  it("maps HTTP errors", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({ ok: false, status: 503 }),
    );
    await expect(
      runList(Option.some(instance("https://executor.example"))),
    ).rejects.toThrow("HTTP 503");
    vi.unstubAllGlobals();
  });

  it("maps schema validation errors", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({ toolkits: [{}] }),
      }),
    );
    await expect(
      runList(Option.some(instance("https://executor.example"))),
    ).rejects.toThrow("toolkits");
    vi.unstubAllGlobals();
  });

  it("uses URL path joining and the canonical Authorization header", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve(validResponse),
    });
    vi.stubGlobal("fetch", fetchMock);
    await expect(
      runList(Option.some(instance("https://executor.example/base"))),
    ).resolves.toHaveLength(1);
    expect(fetchMock).toHaveBeenCalledWith(
      "https://executor.example/base/api/toolkits",
      expect.objectContaining({
        headers: expect.objectContaining({ Authorization: "Bearer token" }),
      }),
    );
    vi.unstubAllGlobals();
  });

  it("times out while the response body is stalled", async () => {
    vi.useFakeTimers();
    const fetchMock = vi
      .fn()
      .mockImplementation((_url: string, init: RequestInit) =>
        Promise.resolve({
          ok: true,
          json: () => {
            const { promise, reject } = Promise.withResolvers<unknown>();
            init.signal?.addEventListener("abort", () =>
              reject(new DOMException("timed out", "AbortError")),
            );
            return promise;
          },
        }),
      );
    vi.stubGlobal("fetch", fetchMock);
    const pending = runList(
      Option.some(instance("https://executor.example")),
    ).then(
      () => null,
      (error) => error,
    );
    await vi.advanceTimersByTimeAsync(5000);
    const error = await pending;
    expect(String(error)).toContain("AbortError: timed out");
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });
});
