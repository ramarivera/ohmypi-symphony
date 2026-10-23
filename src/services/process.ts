import { kill, platform } from "node:process";
import type { Subprocess } from "bun";

export const detachedProcessGroup = platform !== "win32";

// Children inherit the worker's group, so terminating OMP also stops its tools.
export const signalProcessTree = (
  child: Pick<Subprocess, "pid" | "kill">,
  signal: NodeJS.Signals = "SIGTERM",
): void => {
  try {
    if (detachedProcessGroup) kill(-child.pid, signal);
    else child.kill(signal);
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "ESRCH"))
      throw error;
  }
};
