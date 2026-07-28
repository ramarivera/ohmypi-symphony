import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadConfig } from "../src/config";

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((path) => rm(path, { recursive: true, force: true })),
  );
});

describe("loadConfig", () => {
  test("loads required values from Docker secret files", async () => {
    const directory = await mkdtemp(join(tmpdir(), "gateway-secrets-"));
    temporaryDirectories.push(directory);
    const values = {
      LINEAR_CLIENT_ID: "client",
      LINEAR_CLIENT_SECRET: "secret",
      LINEAR_WEBHOOK_SECRET: "webhook",
      TOKEN_ENCRYPTION_KEY: Buffer.from(new Uint8Array(32).fill(4)).toString(
        "base64",
      ),
    };
    const env: Record<string, string> = {
      PUBLIC_URL: "https://ohmypi-symphony.ai.roxasroot.net",
    };

    for (const [name, value] of Object.entries(values)) {
      const path = join(directory, name.toLowerCase());
      await writeFile(path, `${value}\n`, { mode: 0o600 });
      env[`${name}_FILE`] = path;
    }

    const config = loadConfig(env);
    expect(config.linearClientId).toBe("client");
    expect(config.linearClientSecret).toBe("secret");
    expect(config.linearWebhookSecret).toBe("webhook");
    expect(config.tokenEncryptionKey).toEqual(new Uint8Array(32).fill(4));
    expect(config.publicUrl.toString()).toBe(
      "https://ohmypi-symphony.ai.roxasroot.net/",
    );
  });

  test("prefers a direct value over its file fallback", async () => {
    const directory = await mkdtemp(join(tmpdir(), "gateway-secrets-"));
    temporaryDirectories.push(directory);
    const clientIdFile = join(directory, "linear-client-id");
    await writeFile(clientIdFile, "file-client\n", { mode: 0o600 });

    const config = loadConfig({
      PUBLIC_URL: "http://localhost:3000",
      LINEAR_CLIENT_ID: "direct-client",
      LINEAR_CLIENT_ID_FILE: clientIdFile,
      LINEAR_CLIENT_SECRET: "secret",
      LINEAR_WEBHOOK_SECRET: "webhook",
      TOKEN_ENCRYPTION_KEY: Buffer.from(new Uint8Array(32).fill(5)).toString(
        "base64",
      ),
    });

    expect(config.linearClientId).toBe("direct-client");
  });
});
