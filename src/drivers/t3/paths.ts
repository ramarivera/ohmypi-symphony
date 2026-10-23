import { join } from "node:path";

/** A launched daemon's state, under the session's git-excluded worker home. */
export const t3SessionPaths = (cwd: string) => {
  const home = join(cwd, ".omp-home");
  const t3Home = join(home, "t3");
  return {
    home,
    t3Home,
    codexHome: join(t3Home, "codex-cliproxy"),
    claudeHome: join(t3Home, "claude-cliproxy"),
  };
};
