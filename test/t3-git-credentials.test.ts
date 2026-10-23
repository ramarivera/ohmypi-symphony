import { mkdir, symlink, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Result } from "effect";
import { assertT3GitCredentials } from "../src/services/workspace.js";
import { testPaths } from "./fixtures/paths.js";

const pathFor = testPaths();
const credential = Buffer.from("x-access-token:scoped-token").toString(
  "base64",
);
const gatewayConfig = `[http "https://github.com/"]\nextraheader = AUTHORIZATION: basic ${credential}\n`;
const homeConfigs = [".omp-home/.gitconfig", ".omp-home/.config/git/config"];
const workspace = () =>
  Effect.gen(function* () {
    const cwd = pathFor("workspace");
    yield* Effect.promise(() => mkdir(join(cwd, ".git"), { recursive: true }));
    yield* Effect.promise(() =>
      writeFile(join(cwd, ".git/config"), gatewayConfig),
    );
    return cwd;
  });

describe("T3 worker-home Git credentials", () => {
  it.live("accepts missing and safe worker-home configs", () =>
    Effect.gen(function* () {
      const cwd = yield* workspace();
      yield* assertT3GitCredentials(cwd, "credentials-test", 1000, true);
      for (const file of homeConfigs) {
        yield* Effect.promise(() =>
          mkdir(dirname(join(cwd, file)), { recursive: true }),
        );
        yield* Effect.promise(() =>
          writeFile(join(cwd, file), "[user]\nname = Agent\n"),
        );
      }
      yield* assertT3GitCredentials(cwd, "credentials-test", 1000, true);
    }),
  );

  it.live("propagates worker-home inspection errors other than ENOENT", () =>
    Effect.gen(function* () {
      const cwd = yield* workspace();
      yield* Effect.promise(() =>
        writeFile(join(cwd, ".omp-home"), "not a directory"),
      );
      const result = yield* assertT3GitCredentials(
        cwd,
        "credentials-test",
        1000,
        true,
      ).pipe(Effect.result);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(result.failure.message).toContain("lstat failed");
    }),
  );

  for (const file of homeConfigs) {
    for (const config of [
      "[credential]\nhelper = store\n",
      "[core]\naskpass = retained-secret\n",
      "[include]\npath = /retained-secret\n",
      '[http "https://github.com/"]\nextraheader = AUTHORIZATION: bearer retained-secret\n',
      '[remote "origin"]\nurl = https://user:retained-secret@github.com/org/repo\n',
    ]) {
      it.live(
        `rejects unsafe settings in ${file}: ${config.split("\n")[0]}`,
        () =>
          Effect.gen(function* () {
            const cwd = yield* workspace();
            yield* Effect.promise(() =>
              mkdir(dirname(join(cwd, file)), { recursive: true }),
            );
            yield* Effect.promise(() => writeFile(join(cwd, file), config));
            const result = yield* assertT3GitCredentials(
              cwd,
              "credentials-test",
              1000,
              true,
            ).pipe(Effect.result);
            expect(Result.isFailure(result)).toBe(true);
            if (Result.isFailure(result)) {
              expect(result.failure.message).toContain(
                "T3 refused non-gateway",
              );
              expect(result.failure.message).not.toContain("retained-secret");
            }
          }),
      );
    }
    it.live(`propagates malformed config errors from ${file}`, () =>
      Effect.gen(function* () {
        const cwd = yield* workspace();
        yield* Effect.promise(() =>
          mkdir(dirname(join(cwd, file)), { recursive: true }),
        );
        yield* Effect.promise(() => writeFile(join(cwd, file), "[invalid\n"));
        const result = yield* assertT3GitCredentials(
          cwd,
          "credentials-test",
          1000,
          true,
        ).pipe(Effect.result);
        expect(Result.isFailure(result)).toBe(true);
      }),
    );
    it.live(`does not treat an unreadable ${file} as missing`, () =>
      Effect.gen(function* () {
        const cwd = yield* workspace();
        yield* Effect.promise(() =>
          mkdir(dirname(join(cwd, file)), { recursive: true }),
        );
        yield* Effect.promise(() => symlink(join(cwd, file), join(cwd, file)));
        const result = yield* assertT3GitCredentials(
          cwd,
          "credentials-test",
          1000,
          true,
        ).pipe(Effect.result);
        expect(Result.isFailure(result)).toBe(true);
      }),
    );
  }
});
