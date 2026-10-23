import {
  chmodSync,
  mkdirSync,
  readFileSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Redacted, Scope } from "effect";
import {
  launchT3Daemon,
  listenerOwnerUids,
  renderT3LaunchSettings,
  type T3LaunchInput,
  T3PortPool,
} from "../src/drivers/t3/launcher.js";
import { t3SessionPaths } from "../src/drivers/t3/paths.js";
import type { T3LaunchConfig } from "../src/services/config.js";
import { sessionWorkerEnvironment } from "../src/services/worker-security.js";
import { workspaceProcess } from "../src/services/workspace-process.js";
import { testPaths } from "./fixtures/paths.js";

const pathFor = testPaths();
const fakeCli = fileURLToPath(
  new URL("./fixtures/fake-t3-cli.ts", import.meta.url),
);

const cliFor = (behavior: "ok" | "crash"): string => {
  const path = pathFor(`bin/t3-${behavior}.sh`);
  writeFileSync(
    path,
    `#!/bin/sh\nexec ${process.execPath} ${fakeCli} ${behavior} "$@"\n`,
  );
  chmodSync(path, 0o755);
  return path;
};

// Distinct per worker so parallel test files never contend for ports.
const portBase = 20_000 + ((process.pid * 7) % 20_000);

const launchConfig = (cliPath: string): T3LaunchConfig => ({
  _tag: "Launch",
  cliPath,
  portBase,
  portCount: 8,
  launchTimeoutMs: 10_000,
  codexExecutable: "codex",
  claudeExecutable: "claude-cliproxy",
  codexModel: "gpt-6-sol",
  codexReasoningEffort: "high",
  textGenerationModel: "gpt-6-luna",
});

const workspaceFor = (name: string): string => {
  const cwd = pathFor(`workspaces/${name}`);
  mkdirSync(join(cwd, ".omp-home"), { recursive: true, mode: 0o700 });
  chmodSync(cwd, 0o700);
  return cwd;
};

const inputFor = (
  cwd: string,
  launch: T3LaunchConfig,
  ports: T3PortPool,
  overrides: Partial<T3LaunchInput> = {},
): T3LaunchInput => {
  const settings = renderT3LaunchSettings({
    cwd,
    cliproxyApiBaseUrl: "https://cliproxyapi.example.test",
    launch,
    runtimeMode: "auto",
    defaultModel: { instanceId: "claude-cliproxy", model: "claude-opus-5-5" },
  });
  return {
    sessionId: `session-${cwd.split("/").at(-1)}`,
    cwd,
    workerUid: undefined,
    launch,
    ports,
    environment: sessionWorkerEnvironment(cwd, {
      ...process.env,
      LINEAR_CLIENT_SECRET: "gateway-only",
      GITHUB_APP_PRIVATE_KEY: "gateway-only",
    }),
    modelEnvironment: {
      CLIPROXYAPI_BASE_URL: "https://cliproxyapi.example.test",
      CLIPROXYAPI_API_KEY: "proxy-client-key",
    },
    settings,
    writeSettings: async (value) => {
      await workspaceProcess(cwd, "t3-config", value);
    },
    pollIntervalMs: 50,
    stopTimeoutMs: 1_000,
    ...overrides,
  };
};

const ticketStatus = (port: number, token: string) =>
  fetch(`http://127.0.0.1:${port}/api/auth/websocket-ticket`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}` },
  }).then((response) => response.status);

const alive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

const procTables = (): ReadonlyArray<string> => [
  readFileSync("/proc/net/tcp", "utf8"),
];

describe("T3 launcher pure parts", () => {
  it("reads listener owners from /proc/net/tcp and tcp6", () => {
    const tcp = [
      "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode",
      "   0: 0100007F:A029 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1001        0 1 1",
      "   1: 0100007F:A029 0100007F:9C40 01 00000000:00000000 00:00000000 00000000  1002        0 2 1",
      "   2: 0100007F:A02A 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1002        0 3 1",
    ].join("\n");
    const tcp6 = [
      "  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode",
      "   0: 00000000000000000000000000000000:A029 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000  1003        0 4 1",
    ].join("\n");
    expect(listenerOwnerUids([tcp], 41001)).toEqual([1001]);
    expect(listenerOwnerUids([tcp, tcp6], 41001)).toEqual([1001, 1003]);
    expect(listenerOwnerUids([tcp], 41002)).toEqual([1002]);
    expect(listenerOwnerUids([tcp], 41003)).toEqual([]);
  });

  it("allocates distinct ports and skips occupied ones", () => {
    const pool = new T3PortPool(5000, 3);
    expect(pool.allocate((port) => port !== 5000)).toBe(5001);
    expect(pool.allocate(() => true)).toBe(5000);
    expect(pool.allocate(() => true)).toBe(5002);
    expect(pool.allocate(() => true)).toBeUndefined();
    pool.release(5001);
    expect(pool.allocate(() => true)).toBe(5001);
  });

  it("renders only CLIProxyAPI providers and no credential", () => {
    const rendered = renderT3LaunchSettings({
      cwd: "/app/workspaces/s1",
      cliproxyApiBaseUrl: "https://cliproxyapi.example.test/",
      launch: launchConfig("t3"),
      runtimeMode: "auto",
      defaultModel: { instanceId: "codex-cliproxy", model: "gpt-6-sol" },
    });
    const text = JSON.stringify(rendered);
    expect(text).not.toContain("proxy-client-key");
    expect(text).not.toMatch(/ANTHROPIC_(?:AUTH_TOKEN|API_KEY)|apiKey/u);
    const instances = rendered.settings.providerInstances;
    expect(
      Object.entries(instances)
        .filter(([, instance]) => instance.enabled)
        .map(([id]) => id)
        .sort(),
    ).toEqual(["claude-cliproxy", "codex-cliproxy"]);
    expect(instances["claude-cliproxy"].config).toEqual({
      homePath: "/app/workspaces/s1/.omp-home/t3/claude-cliproxy",
      binaryPath: "claude-cliproxy",
    });
    expect(rendered.codexConfig).toContain(
      'base_url = "https://cliproxyapi.example.test/v1"',
    );
    expect(rendered.codexConfig).toContain('env_key = "CLIPROXYAPI_API_KEY"');
  });
});

describe("T3 launcher lifecycle", () => {
  it.live(
    "starts t3 serve on a pool port with a private bearer and stops it with the scope",
    () =>
      Effect.gen(function* () {
        const launch = launchConfig(cliFor("ok"));
        const ports = new T3PortPool(launch.portBase, launch.portCount);
        const cwd = workspaceFor("lifecycle");
        const scope = yield* Scope.make();
        const daemon = yield* launchT3Daemon(inputFor(cwd, launch, ports)).pipe(
          Scope.provide(scope),
        );
        const paths = t3SessionPaths(cwd);
        expect(daemon.port).toBeGreaterThanOrEqual(launch.portBase);
        expect(daemon.port).toBeLessThan(launch.portBase + launch.portCount);
        expect(daemon.baseUrl.toString()).toBe(
          `http://127.0.0.1:${daemon.port}/`,
        );
        const token = Redacted.value(yield* daemon.token);
        expect(
          yield* Effect.promise(() => ticketStatus(daemon.port, token)),
        ).toBe(200);
        expect(
          yield* Effect.promise(() => ticketStatus(daemon.port, "guess")),
        ).toBe(401);
        expect(listenerOwnerUids(procTables(), daemon.port)).toEqual([
          process.getuid?.(),
        ]);

        // Settings were written into the session's private home, mode 0600.
        const settingsPath = join(paths.t3Home, "userdata", "settings.json");
        expect(statSync(settingsPath).mode & 0o777).toBe(0o600);
        expect(
          readFileSync(join(paths.codexHome, "config.toml"), "utf8"),
        ).toContain("cliproxyapi");

        // Only serve gets the proxy key; gateway secrets reach neither process.
        const serveEnv = JSON.parse(
          readFileSync(join(paths.t3Home, "serve-env.json"), "utf8"),
        );
        expect(serveEnv.home).toBe(paths.home);
        expect(serveEnv.t3Home).toBe(paths.t3Home);
        expect(serveEnv.workspace).toBe(cwd);
        expect(serveEnv.keys).toContain("CLIPROXYAPI_API_KEY");
        const issueEnv: string[] = JSON.parse(
          readFileSync(join(paths.t3Home, "issue-env.json"), "utf8"),
        );
        expect(issueEnv).not.toContain("CLIPROXYAPI_API_KEY");
        for (const keys of [serveEnv.keys, issueEnv]) {
          expect(keys).not.toContain("LINEAR_CLIENT_SECRET");
          expect(keys).not.toContain("GITHUB_APP_PRIVATE_KEY");
        }

        const provider = Number(
          readFileSync(join(paths.t3Home, "provider.pid"), "utf8"),
        );
        expect(alive(provider)).toBe(true);
        yield* Scope.close(scope, Exit.void);
        expect(yield* Effect.promise(() => daemon.exited)).toBeDefined();
        // The whole process group goes, including provider children.
        for (let i = 0; i < 40 && alive(provider); i++) yield* Effect.sleep(25);
        expect(alive(provider)).toBe(false);
        expect(listenerOwnerUids(procTables(), daemon.port)).toEqual([]);
        // The port returns to the pool.
        expect(ports.allocate(() => true)).toBe(launch.portBase);
      }),
    20_000,
  );

  it.live(
    "isolates concurrent sessions: distinct ports, state and bearers",
    () =>
      Effect.gen(function* () {
        const launch = launchConfig(cliFor("ok"));
        const ports = new T3PortPool(launch.portBase, launch.portCount);
        const cwdA = workspaceFor("session-a");
        const cwdB = workspaceFor("session-b");
        const [a, b] = yield* Effect.all(
          [
            launchT3Daemon(inputFor(cwdA, launch, ports)),
            launchT3Daemon(inputFor(cwdB, launch, ports)),
          ],
          { concurrency: 2 },
        );
        expect(a.port).not.toBe(b.port);
        const tokenA = Redacted.value(yield* a.token);
        const tokenB = Redacted.value(yield* b.token);
        expect(tokenA).not.toBe(tokenB);
        const status = (port: number, token: string) =>
          Effect.promise(() => ticketStatus(port, token));
        expect(yield* status(a.port, tokenA)).toBe(200);
        expect(yield* status(b.port, tokenB)).toBe(200);
        expect(yield* status(a.port, tokenB)).toBe(401);
        expect(yield* status(b.port, tokenA)).toBe(401);
        // State lives inside each session's mode-0700 workspace.
        for (const cwd of [cwdA, cwdB]) {
          expect(statSync(cwd).mode & 0o777).toBe(0o700);
          expect(
            readFileSync(join(t3SessionPaths(cwd).t3Home, "tokens.txt"), "utf8")
              .trim()
              .split("\n"),
          ).toHaveLength(1);
        }
      }).pipe(Effect.scoped),
    20_000,
  );

  it.live(
    "refuses to send the bearer when another identity holds the port",
    () =>
      Effect.gen(function* () {
        const launch = launchConfig(cliFor("ok"));
        const ports = new T3PortPool(launch.portBase, launch.portCount);
        const cwd = workspaceFor("squatted");
        // Once the daemon listens, another uid also appears on its port.
        const result = yield* launchT3Daemon(
          inputFor(cwd, launch, ports, {
            readListeners: () => {
              const tables = procTables();
              const squatters = Array.from(
                { length: launch.portCount },
                (_, index) => launch.portBase + index,
              )
                .filter((port) => listenerOwnerUids(tables, port).length > 0)
                .map(
                  (port) =>
                    ` 9: 0100007F:${port.toString(16).toUpperCase().padStart(4, "0")} 00000000:0000 0A 0:0 0:0 0 4242 0 9 1`,
                );
              return [...tables, ["header", ...squatters].join("\n")];
            },
          }),
        ).pipe(Effect.scoped, Effect.result);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure")
          expect(result.failure.message).toContain("held by another identity");
        // The released port is reusable and the daemon is gone.
        expect(ports.allocate(() => true)).toBe(launch.portBase);
        const provider = Number(
          readFileSync(
            join(t3SessionPaths(cwd).t3Home, "provider.pid"),
            "utf8",
          ),
        );
        for (let i = 0; i < 40 && alive(provider); i++) yield* Effect.sleep(25);
        expect(alive(provider)).toBe(false);
      }),
    20_000,
  );

  it.live("re-verifies listener ownership before every bearer use", () =>
    Effect.gen(function* () {
      const launch = launchConfig(cliFor("ok"));
      const ports = new T3PortPool(launch.portBase, launch.portCount);
      const cwd = workspaceFor("reverify");
      let foreign = false;
      const daemon = yield* launchT3Daemon(
        inputFor(cwd, launch, ports, {
          readListeners: () =>
            foreign
              ? procTables().map((table) =>
                  table.replace(
                    new RegExp(` ${process.getuid?.()} `, "gu"),
                    " 4242 ",
                  ),
                )
              : procTables(),
        }),
      );
      expect(Redacted.value(yield* daemon.token)).toMatch(/^bearer-/u);
      foreign = true;
      const reused = yield* daemon.token.pipe(Effect.result);
      expect(reused._tag).toBe("Failure");
    }).pipe(Effect.scoped),
  );

  it.live("reports an early exit without leaking the pairing code", () =>
    Effect.gen(function* () {
      const launch = launchConfig(cliFor("crash"));
      const ports = new T3PortPool(launch.portBase, launch.portCount);
      const result = yield* launchT3Daemon(
        inputFor(workspaceFor("crash"), launch, ports),
      ).pipe(Effect.scoped, Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") {
        expect(result.failure.message).toContain("exited before it was ready");
        expect(result.failure.message).toContain("provider setup failed");
        expect(result.failure.message).not.toContain("PAIRING1234X");
      }
      expect(ports.allocate(() => true)).toBe(launch.portBase);
    }),
  );

  it.live(
    "fails closed in isolated mode when listeners can't be verified",
    () =>
      Effect.gen(function* () {
        const launch = launchConfig(cliFor("ok"));
        const result = yield* launchT3Daemon(
          inputFor(
            workspaceFor("no-proc"),
            launch,
            new T3PortPool(launch.portBase, launch.portCount),
            { workerUid: "1001", readListeners: () => undefined },
          ),
        ).pipe(Effect.scoped, Effect.result);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure")
          expect(result.failure.message).toContain("listener ownership");
      }),
  );
});
