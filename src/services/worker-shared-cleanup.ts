import { chmodSync, lstatSync, readdirSync, rmSync } from "node:fs";
import { join } from "node:path";

/** Never run this traversal with gateway privileges or follow directory symlinks. */
export const cleanupWorkerSharedDirectoriesSync = (
  roots: readonly string[] = ["/tmp", "/var/tmp", "/dev/shm"],
): void => {
  const uid = process.getuid?.();
  if (uid === undefined || uid === 0)
    throw new Error("Shared-directory cleanup requires a non-root worker uid");
  const visit = (path: string, isRoot = false): void => {
    try {
      const stat = lstatSync(path);
      if (!isRoot && stat.uid === uid) {
        // Restore access to our own mode-000 directories before removing them.
        if (stat.isDirectory()) {
          chmodSync(path, 0o700);
          for (const name of readdirSync(path)) visit(join(path, name));
        }
        rmSync(path, { recursive: true, force: true });
      } else if (stat.isDirectory() && !stat.isSymbolicLink()) {
        let names: string[];
        try {
          names = readdirSync(path);
        } catch (error) {
          // Other sessions' private directories are outside this uid's reach.
          if (
            error instanceof Error &&
            "code" in error &&
            error.code === "EACCES"
          )
            return;
          throw error;
        }
        for (const name of names) visit(join(path, name));
      }
    } catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ENOENT")
        return;
      throw error;
    }
  };
  for (const root of roots) visit(root, true);
};
