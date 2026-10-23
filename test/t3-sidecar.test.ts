import { execFileSync, spawnSync } from "node:child_process";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

const scratch = join(
  homedir(),
  "dev/agentic-scratchpads/ohmypi-symphony/t3-sidecar-tests",
);
mkdirSync(scratch, { recursive: true });
const roots: string[] = [];
const entrypoint = resolve("deploy/t3/docker-entrypoint.sh");
function fixture() {
  const root = mkdtempSync(join(scratch, "render-"));
  roots.push(root);
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    T3CODE_HOME: root,
    CLIPROXYAPI_BASE_URL: "https://proxy.example/",
    CLIPROXYAPI_API_KEY: "test-only-proxy-key",
    CLIPROXYAPI_API_KEY_FILE: "",
  };
  return { root, env };
}
afterEach(() => {
  for (const root of roots.splice(0))
    rmSync(root, { recursive: true, force: true });
});

describe("T3 sidecar configuration", () => {
  it("enforces proxy providers on restart and preserves unrelated settings", () => {
    const { root, env } = fixture();
    mkdirSync(join(root, "userdata"));
    writeFileSync(
      join(root, "userdata/settings.json"),
      JSON.stringify({
        defaultTheme: "custom",
        providerInstances: {
          codex: { enabled: true },
          direct: { driver: "claudeAgent", enabled: true },
        },
        projectProviderInstanceOverrides: { project: "direct" },
      }),
    );
    execFileSync("bash", [entrypoint, "render"], { env });
    const settings = JSON.parse(
      readFileSync(join(root, "userdata/settings.json"), "utf8"),
    );
    expect(settings.defaultTheme).toBe("custom");
    expect(settings.defaultRuntimeMode).toBe("auto");
    expect(
      Object.entries(settings.providerInstances)
        .filter(([, value]) => (value as { enabled: boolean }).enabled)
        .map(([key]) => key)
        .sort(),
    ).toEqual(["claude-cliproxy", "codex-cliproxy"]);
    expect(settings.projectProviderInstanceOverrides).toEqual({});
    expect(settings.textGenerationModelSelection.instanceId).toBe(
      "codex-cliproxy",
    );
    const config = readFileSync(
      join(root, "codex-cliproxy/config.toml"),
      "utf8",
    );
    expect(config).toContain('base_url = "https://proxy.example/v1"');
    expect(config).toContain('env_key = "CLIPROXYAPI_API_KEY"');
    expect(config).not.toContain("test-only-proxy-key");
    expect(statSync(join(root, "userdata/settings.json")).mode & 0o777).toBe(
      0o600,
    );
    expect(
      statSync(join(root, "codex-cliproxy/config.toml")).mode & 0o777,
    ).toBe(0o600);
  });

  it("loads the proxy secret file and strips inherited direct credentials before serve", () => {
    const { root, env } = fixture();
    const key = join(root, "proxy-key");
    writeFileSync(key, "file-test-key\n", { mode: 0o600 });
    const bin = join(root, "bin");
    mkdirSync(bin);
    writeFileSync(
      join(bin, "t3"),
      '#!/bin/sh\n[ "$CLIPROXYAPI_API_KEY" = file-test-key ] && [ -z "$OPENAI_API_KEY$ANTHROPIC_API_KEY$CLAUDE_CODE_OAUTH_TOKEN$CLAUDE_CODE_USE_VERTEX" ]\n',
      { mode: 0o700 },
    );
    execFileSync("bash", [entrypoint, "serve"], {
      env: {
        ...env,
        PATH: `${bin}:${env.PATH}`,
        CLIPROXYAPI_API_KEY: "",
        CLIPROXYAPI_API_KEY_FILE: key,
        OPENAI_API_KEY: "direct-test",
        ANTHROPIC_API_KEY: "direct-test",
        CLAUDE_CODE_OAUTH_TOKEN: "direct-test",
        CLAUDE_CODE_USE_VERTEX: "1",
      },
    });
  });

  it("fails closed for missing secrets, conflicting credentials and invalid selectors", () => {
    const { env } = fixture();
    for (const override of [
      { CLIPROXYAPI_BASE_URL: "" },
      { CLIPROXYAPI_API_KEY: "" },
      { CLIPROXYAPI_API_KEY_FILE: "/missing" },
      { T3_DEFAULT_PROVIDER_INSTANCE: "direct" },
      { T3_RUNTIME_MODE: "invalid" },
    ]) {
      expect(
        spawnSync("bash", [entrypoint, "render"], {
          env: { ...env, ...override },
        }).status,
      ).toBe(64);
    }
  });
});
