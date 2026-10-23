import { realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname } from "node:path";
import { describe, expect, it } from "vitest";
import { isWithin } from "../src/util/paths.js";
import { suitePaths, testPaths } from "./fixtures/paths.js";

const first = testPaths();
const second = testPaths();
const suite = suitePaths();

describe("fixture paths", () => {
  it("uses unique roots inside the configured temporary directory", () => {
    const roots = [first, second, suite].map((path) =>
      realpathSync(dirname(path("file.json"))),
    );
    expect(new Set(roots).size).toBe(3);
    for (const root of roots)
      expect(isWithin(realpathSync(tmpdir()), root)).toBe(true);
  });
});
