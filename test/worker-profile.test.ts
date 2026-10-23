import {
  chmod,
  lstat,
  mkdir,
  readFile,
  stat,
  symlink,
  writeFile,
} from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  seedWorkerProfileFiles,
  validateModelsYaml,
  validateWorkerProfile,
  type WorkerProfilePolicy,
  workerModelArguments,
  workerModelEnvironment,
} from "../src/services/worker-profile.js";
import {
  sessionWorkerEnvironment,
  workerEnvironment,
} from "../src/services/worker-security.js";
import { testPaths } from "./fixtures/paths.js";

const path = testPaths();
const BASE = "http://cliproxyapi:8317";

const MODELS = `providers:
  cliproxyapi:
    name: CLIProxyAPI
    api: openai-responses
    baseUrl: ${BASE}/v1
    apiKey: CLIPROXYAPI_API_KEY
    models:
      - id: gpt-6-sol
        name: GPT-6 Sol
        contextWindow: 400000
        maxTokens: 128000
`;

const policy = (
  overrides: Partial<WorkerProfilePolicy> = {},
): WorkerProfilePolicy => ({
  cliproxyApiBaseUrl: BASE,
  defaultModel: "cliproxyapi/gpt-6-sol",
  maxBytes: 1024 * 1024,
  maxEntries: 100,
  requireWorldReadable: false,
  ...overrides,
});

const write = async (file: string, content: string, mode = 0o644) => {
  await mkdir(join(file, ".."), { recursive: true, mode: 0o755 });
  await writeFile(file, content, { mode });
  await chmod(file, mode);
};

/** A profile shaped like the curated subset of the host ~/.omp/agent tree. */
const makeProfile = async (): Promise<string> => {
  const root = path("profile");
  await chmod(root, 0o755);
  await write(join(root, "models.yml"), MODELS);
  await write(join(root, "config.yml"), "defaultThinkingLevel: auto\n");
  await write(join(root, "AGENTS.md"), "# Worker instructions\n");
  await write(join(root, "skills", "effect", "SKILL.md"), "# Effect\n");
  await write(join(root, "skills", "effect", "run.sh"), "#!/bin/sh\n", 0o755);
  await write(join(root, "rules", "no-any.md"), "no any\n");
  await write(join(root, "extensions", "rtk.ts"), "export default 1;\n");
  return root;
};

const rejects = async (
  root: string,
  message: RegExp,
  overrides: Partial<WorkerProfilePolicy> = {},
) => {
  await expect(validateWorkerProfile(root, policy(overrides))).rejects.toThrow(
    message,
  );
};

describe("worker profile validation", () => {
  it("accepts the contract layout and lists only validated entries", async () => {
    const root = await makeProfile();
    const manifest = await validateWorkerProfile(root, policy());
    expect(manifest.directories).toEqual([
      "extensions",
      "rules",
      "skills",
      "skills/effect",
    ]);
    expect(manifest.files.map((file) => file.path)).toEqual([
      "AGENTS.md",
      "config.yml",
      "extensions/rtk.ts",
      "models.yml",
      "rules/no-any.md",
      "skills/effect/SKILL.md",
      "skills/effect/run.sh",
    ]);
    expect(
      manifest.files.find((file) => file.path === "skills/effect/run.sh"),
    ).toMatchObject({ executable: true });
  });

  it.each([
    ["auth.json", "{}"],
    ["agent.db", "sqlite"],
    ["mcp.json", "{}"],
    ["history.db", "sqlite"],
    ["auth-broker.token", "token"],
  ])("rejects host state file %s at the top level", async (name, content) => {
    const root = await makeProfile();
    await write(join(root, name), content);
    await rejects(root, /not part of the profile contract|credential/u);
  });

  it.each([
    "skills/effect/auth.json",
    "skills/effect/.env",
    "skills/effect/.env.local",
    "extensions/remnic/agent.db",
    "extensions/remnic/agent.db-wal",
    "rules/deploy.pem",
    "skills/ssh/id_ed25519",
  ])("rejects credential-shaped file %s inside the profile", async (name) => {
    const root = await makeProfile();
    await write(join(root, name), "secret");
    await rejects(root, /credential files/u);
  });

  it("rejects session directories even inside allowed trees", async () => {
    const root = await makeProfile();
    await mkdir(join(root, "extensions", "sessions"), { mode: 0o755 });
    await rejects(root, /credential files/u);
  });

  it("allows an .env.example template", async () => {
    const root = await makeProfile();
    await write(join(root, "skills", "effect", ".env.example"), "KEY=\n");
    await expect(validateWorkerProfile(root, policy())).resolves.toBeDefined();
  });

  it("rejects symlinks to files, directories, and the source itself", async () => {
    const fileLink = await makeProfile();
    await symlink("/etc/passwd", join(fileLink, "skills", "effect", "x.md"));
    await rejects(fileLink, /symlinks/u);

    const dirLink = path("dir-link");
    await chmod(dirLink, 0o755);
    await write(join(dirLink, "models.yml"), MODELS);
    await symlink("/etc", join(dirLink, "skills"));
    await rejects(dirLink, /symlinks/u);

    const target = await makeProfile();
    const link = join(path("links"), "profile");
    await symlink(target, link);
    await rejects(link, /canonical/u);
  });

  it("rejects files owned by an untrusted uid", async () => {
    const root = await makeProfile();
    const owner = (await stat(join(root, "AGENTS.md"))).uid;
    await rejects(root, /foreign owner/u, { trustedUids: [owner + 1] });
  });

  it("rejects group- or world-writable entries", async () => {
    const root = await makeProfile();
    await chmod(join(root, "rules", "no-any.md"), 0o666);
    await rejects(root, /writable/u);
    await chmod(join(root, "rules", "no-any.md"), 0o644);
    await chmod(join(root, "skills"), 0o775);
    await rejects(root, /writable/u);
  });

  it("requires worker-readable modes when a separate uid copies the profile", async () => {
    const root = await makeProfile();
    await chmod(join(root, "AGENTS.md"), 0o600);
    await rejects(root, /world-readable/u, { requireWorldReadable: true });
    await expect(validateWorkerProfile(root, policy())).resolves.toBeDefined();
  });

  it("rejects wrong entry types for contract names", async () => {
    const asFile = await makeProfile();
    await write(join(asFile, "themes"), "not a directory");
    await rejects(asFile, /must be a directory/u);

    const asDir = await makeProfile();
    await mkdir(join(asDir, "RULES.md"), { mode: 0o755 });
    await rejects(asDir, /must be a regular file/u);
  });

  it("enforces size and entry caps", async () => {
    const root = await makeProfile();
    await rejects(root, /MAX_BYTES/u, { maxBytes: 64 });
    await rejects(root, /MAX_ENTRIES/u, { maxEntries: 3 });
  });

  it("requires the default model's provider to exist", async () => {
    const root = await makeProfile();
    await rejects(root, /WORKER_DEFAULT_MODEL/u, {
      defaultModel: "openai-codex/gpt-6-sol",
    });
    await expect(
      validateWorkerProfile(
        root,
        policy({ defaultModel: "pi-claude-cli/claude-opus-5-5" }),
      ),
    ).resolves.toBeDefined();
  });

  it("rejects credentials and commands in config.yml", async () => {
    const root = await makeProfile();
    await write(join(root, "config.yml"), "github:\n  token: ghp_secret\n");
    await rejects(root, /credentials/u);
    await write(join(root, "config.yml"), 'statusLine: "!curl evil"\n');
    await rejects(root, /commands/u);
  });
});

describe("worker models.yml", () => {
  const provider = (body: string) =>
    `providers:\n  cliproxyapi:\n    api: openai-responses\n    baseUrl: ${BASE}/v1\n${body}`;

  it("accepts an apiKey that names the CLIProxyAPI env var", () => {
    expect(validateModelsYaml(MODELS, BASE)).toEqual(new Set(["cliproxyapi"]));
    expect(validateModelsYaml("", BASE)).toEqual(new Set());
  });

  it.each([
    [
      "a literal apiKey",
      provider("    apiKey: sk-live-secret\n"),
      /credentials/u,
    ],
    [
      "another env var",
      provider("    apiKey: OPENAI_API_KEY\n"),
      /credentials/u,
    ],
    [
      "a command apiKey",
      provider('    apiKey: "!op read x"\n'),
      /credentials/u,
    ],
    [
      "a command anywhere",
      provider('    compat:\n      label: "!cat /etc/shadow"\n'),
      /commands/u,
    ],
    [
      "headers",
      provider("    headers:\n      Authorization: Bearer x\n"),
      /credentials/u,
    ],
    ["provider auth", provider("    auth: oauth\n"), /must not set auth/u],
    [
      "discovery",
      provider("    discovery:\n      type: ollama\n"),
      /must not set discovery/u,
    ],
    [
      "the anthropic-messages api",
      `providers:\n  claude:\n    api: anthropic-messages\n    baseUrl: ${BASE}\n`,
      /api must be/u,
    ],
    [
      "a model-level anthropic api",
      provider("    models:\n      - id: x\n        api: anthropic-messages\n"),
      /api must be/u,
    ],
    [
      "a model-level baseUrl elsewhere",
      provider(
        "    models:\n      - id: x\n        baseUrl: https://api.anthropic.com\n",
      ),
      /baseUrl must use/u,
    ],
    [
      "a provider outside CLIProxyAPI",
      "providers:\n  openai:\n    api: openai-responses\n    baseUrl: https://api.openai.com/v1\n",
      /baseUrl must use/u,
    ],
    [
      "a lookalike host",
      `providers:\n  p:\n    api: openai-responses\n    baseUrl: http://cliproxyapi:8317.evil/v1\n`,
      /baseUrl must use/u,
    ],
    [
      "a builtin override without baseUrl",
      "providers:\n  anthropic:\n    models: []\n",
      /baseUrl must use/u,
    ],
    [
      "a pi-claude-cli override",
      `providers:\n  pi-claude-cli:\n    api: openai-responses\n    baseUrl: ${BASE}/v1\n`,
      /must not override/u,
    ],
    ["other top-level keys", "equivalence: {}\n", /must not set equivalence/u],
  ])("rejects %s", (_name, text, message) => {
    expect(() => validateModelsYaml(text, BASE)).toThrow(message);
  });

  it("requires CLIProxyAPI before declaring any provider", () => {
    expect(() => validateModelsYaml(MODELS, undefined)).toThrow(
      /requires CLIPROXYAPI_BASE_URL/u,
    );
  });

  it("matches CLIProxyAPI path prefixes on segment boundaries", () => {
    const prefixed = `providers:\n  p:\n    api: openai-responses\n    baseUrl: https://proxy.test/root-evil/v1\n`;
    expect(() =>
      validateModelsYaml(prefixed, "https://proxy.test/root"),
    ).toThrow(/baseUrl must use/u);
    expect(
      validateModelsYaml(
        prefixed.replace("root-evil", "root"),
        "https://proxy.test/root",
      ),
    ).toEqual(new Set(["p"]));
  });
});

describe("worker profile seeding", () => {
  it("copies the manifest into a private agent dir and keeps omp state", async () => {
    const root = await makeProfile();
    const workspace = path("workspace");
    const agentDir = join(workspace, ".omp-gateway");
    await mkdir(join(agentDir, "sessions"), { recursive: true });
    await writeFile(join(agentDir, "agent.db"), "db");
    await writeFile(join(agentDir, "sessions", "s.jsonl"), "{}");
    await mkdir(join(agentDir, "skills", "stale"), { recursive: true });
    await writeFile(join(agentDir, "skills", "stale", "SKILL.md"), "old");
    await writeFile(join(agentDir, "RULES.md"), "stale rules");

    const manifest = await validateWorkerProfile(root, policy());
    expect(await seedWorkerProfileFiles(workspace, root, manifest)).toBe(
      agentDir,
    );

    expect((await stat(agentDir)).mode & 0o777).toBe(0o700);
    expect(await readFile(join(agentDir, "models.yml"), "utf8")).toBe(MODELS);
    expect((await stat(join(agentDir, "models.yml"))).mode & 0o777).toBe(0o600);
    expect((await stat(join(agentDir, "skills", "effect"))).mode & 0o777).toBe(
      0o700,
    );
    expect(
      (await stat(join(agentDir, "skills", "effect", "run.sh"))).mode & 0o777,
    ).toBe(0o700);
    expect((await stat(join(agentDir, "AGENTS.md"))).uid).toBe(
      process.getuid?.(),
    );
    await expect(lstat(join(agentDir, "skills", "stale"))).rejects.toThrow();
    await expect(lstat(join(agentDir, "RULES.md"))).rejects.toThrow();
    expect(await readFile(join(agentDir, "agent.db"), "utf8")).toBe("db");
    expect(await readFile(join(agentDir, "sessions", "s.jsonl"), "utf8")).toBe(
      "{}",
    );

    // Reseeding on resume replaces managed entries in place.
    await seedWorkerProfileFiles(workspace, root, manifest);
    expect(await readFile(join(agentDir, "AGENTS.md"), "utf8")).toBe(
      "# Worker instructions\n",
    );
  });

  it("removes a planted symlink instead of writing through it", async () => {
    const root = await makeProfile();
    const workspace = path("workspace");
    const outside = path("outside");
    await mkdir(join(workspace, ".omp-gateway"), { recursive: true });
    await symlink(outside, join(workspace, ".omp-gateway", "skills"));
    await seedWorkerProfileFiles(
      workspace,
      root,
      await validateWorkerProfile(root, policy()),
    );
    expect(
      (await lstat(join(workspace, ".omp-gateway", "skills"))).isDirectory(),
    ).toBe(true);
    await expect(lstat(join(outside, "effect"))).rejects.toThrow();
  });

  it("refuses a symlinked agent directory", async () => {
    const root = await makeProfile();
    const workspace = path("workspace");
    await symlink(path("shared"), join(workspace, ".omp-gateway"));
    await expect(
      seedWorkerProfileFiles(
        workspace,
        root,
        await validateWorkerProfile(root, policy()),
      ),
    ).rejects.toThrow(/real directory/u);
  });

  it("refuses files that changed or were swapped after validation", async () => {
    const root = await makeProfile();
    const manifest = await validateWorkerProfile(root, policy());
    await write(join(root, "AGENTS.md"), "# Grown after validation\n");
    await expect(
      seedWorkerProfileFiles(path("workspace"), root, manifest),
    ).rejects.toThrow(/changed after validation/u);

    const swapped = await makeProfile();
    const swappedManifest = await validateWorkerProfile(swapped, policy());
    await symlink("/etc/hostname", join(swapped, "rules", "linked.md"));
    await expect(
      seedWorkerProfileFiles(path("workspace2"), swapped, {
        ...swappedManifest,
        files: [
          ...swappedManifest.files,
          { path: "rules/linked.md", size: 0, executable: false },
        ],
      }),
    ).rejects.toThrow();
  });

  it("rejects manifest paths that escape the agent dir", async () => {
    const root = await makeProfile();
    await expect(
      seedWorkerProfileFiles(path("workspace"), root, {
        directories: [],
        files: [{ path: "../escape.md", size: 0, executable: false }],
      }),
    ).rejects.toThrow(/invalid/u);
    await expect(
      seedWorkerProfileFiles(path("workspace"), root, {
        directories: ["sessions"],
        files: [],
      }),
    ).rejects.toThrow(/invalid/u);
  });
});

describe("worker model environment", () => {
  it("passes the default model only to fresh sessions", () => {
    expect(workerModelArguments("cliproxyapi/gpt-6-sol", false)).toEqual([
      "--model",
      "cliproxyapi/gpt-6-sol",
    ]);
    expect(workerModelArguments("cliproxyapi/gpt-6-sol", true)).toEqual([]);
    expect(workerModelArguments(undefined, false)).toEqual([]);
  });

  it("is empty without CLIProxyAPI", () => {
    expect(
      workerModelEnvironment({
        cliproxyApiBaseUrl: undefined,
        cliproxyApiKey: undefined,
        claudeExecutable: "claude-cliproxy",
      }),
    ).toEqual({});
  });

  it("passes only the CLIProxyAPI key and Claude routing to workers", () => {
    const gatewayEnvironment = {
      PATH: "/bin",
      HOME: "/root",
      ANTHROPIC_API_KEY: "direct",
      ANTHROPIC_BASE_URL: "https://api.anthropic.com",
      OPENAI_API_KEY: "direct",
      CLIPROXYAPI_API_KEY_FILE: "/run/secrets/CLIPROXYAPI_API_KEY",
      TOKEN_ENCRYPTION_KEY: "gateway-only",
    };
    const env = {
      ...workerEnvironment(
        sessionWorkerEnvironment("/work/a", gatewayEnvironment),
      ),
      ...workerModelEnvironment({
        cliproxyApiBaseUrl: BASE,
        cliproxyApiKey: "symphony-client-key",
        claudeExecutable: "claude-cliproxy",
      }),
    };
    expect(Object.keys(env).sort()).toEqual(
      [
        "BUN_INSTALL_CACHE_DIR",
        "BUN_RUNTIME_TRANSPILER_CACHE_PATH",
        "CLIPROXYAPI_API_KEY",
        "CLIPROXYAPI_BASE_URL",
        "HOME",
        "PATH",
        "PI_CLAUDE_AUTH",
        "PI_CLAUDE_DRIVER",
        "PI_CLAUDE_EXECUTABLE",
        "TMPDIR",
        "XDG_CACHE_HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
      ].sort(),
    );
    expect(env).toMatchObject({
      CLIPROXYAPI_API_KEY: "symphony-client-key",
      PI_CLAUDE_AUTH: "claude-login",
      PI_CLAUDE_EXECUTABLE: "claude-cliproxy",
    });
    expect(Object.keys(env).some((name) => name.startsWith("ANTHROPIC_"))).toBe(
      false,
    );
  });
});
