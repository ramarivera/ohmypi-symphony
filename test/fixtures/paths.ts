import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { afterAll, afterEach, beforeEach } from "vitest";

const createRoot = () => {
  const scratch = join(
    homedir(),
    "dev/agentic-scratchpads/ohmypi-symphony/hygiene/tests",
  );
  mkdirSync(scratch, { recursive: true });
  return mkdtempSync(join(scratch, "case-"));
};
const pathIn = (root: string, name: string) => {
  const path = join(root, name);
  mkdirSync(name.includes(".") ? dirname(path) : path, { recursive: true });
  return path;
};

// Per-test files are removed even when a case fails.
export const testPaths = () => {
  let root: string;
  beforeEach(() => {
    root = createRoot();
  });
  afterEach(() => rmSync(root, { recursive: true, force: true }));
  return (name: string) => pathIn(root, name);
};

// Static mock service layers capture paths at module evaluation time.
export const suitePaths = () => {
  const root = createRoot();
  afterAll(() => rmSync(root, { recursive: true, force: true }));
  return (name: string) => pathIn(root, name);
};
