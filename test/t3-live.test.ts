/**
 * Opt-in smoke tests against a real T3 Code server. Skipped by default.
 *
 *   T3_LIVE_URL, T3_LIVE_TOKEN          probe only: descriptor + ticket, no
 *                                       thread, no model tokens.
 *   + T3_LIVE_SPEND=1, T3_LIVE_PROJECT_ID, T3_LIVE_PROVIDER_INSTANCE,
 *     T3_LIVE_MODEL                     launches a thread and runs one tiny
 *                                       turn. This spends model tokens.
 *
 * Mint the token on the T3 host with
 * `t3 auth session issue --scope orchestration:read --scope orchestration:operate --ttl 1d --token-only`.
 */
import { describe, expect, it } from "@effect/vitest";
import { Effect, Redacted } from "effect";
import { makeT3Driver } from "../src/drivers/t3/driver.js";

const env = (name: string): string | undefined => {
  const value = Bun.env[name]?.trim();
  return value === undefined || value.length === 0 ? undefined : value;
};

const url = env("T3_LIVE_URL");
const token = env("T3_LIVE_TOKEN");
const projectId = env("T3_LIVE_PROJECT_ID");
const instanceId = env("T3_LIVE_PROVIDER_INSTANCE");
const model = env("T3_LIVE_MODEL");
const spend = env("T3_LIVE_SPEND") === "1";

const driver = () =>
  makeT3Driver({
    baseUrl: new URL(url ?? "http://127.0.0.1:3773"),
    token: Effect.succeed(Redacted.make(token ?? "")),
    projectId: projectId ?? "unset",
    modelSelection: {
      instanceId: instanceId ?? "unset",
      model: model ?? "unset",
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    idNamespace: "symphony-live",
  });

describe.skipIf(url === undefined || token === undefined)(
  "T3 live probe",
  () => {
    it.live("reaches the server with protocol 2 and a valid credential", () =>
      Effect.gen(function* () {
        const result = yield* driver().probe;
        expect(result.detail).toContain("orchestration protocol 2");
      }),
    );
  },
);

describe.skipIf(
  url === undefined ||
    token === undefined ||
    !spend ||
    projectId === undefined ||
    instanceId === undefined ||
    model === undefined,
)("T3 live turn (spends model tokens)", () => {
  it.scopedLive(
    "launches a thread and completes one turn",
    () =>
      Effect.gen(function* () {
        const session = yield* driver().open({
          sessionKey: `live-${Date.now()}`,
          title: "ohmypi-symphony T3 driver smoke test",
          workspace: { _tag: "ProjectRoot" },
        });
        const handle = yield* session.sendTurn({
          clientRequestId: "live-probe",
          text: "Reply with exactly PROBE-OK and do nothing else.",
          delivery: "auto",
        });
        const outcome = yield* handle.outcome.pipe(Effect.timeout("3 minutes"));
        expect(outcome.status).toBe("completed");
        expect(outcome.finalText).toContain("PROBE-OK");
      }),
    { timeout: 200_000 },
  );
});
