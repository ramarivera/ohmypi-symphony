import { isAbsolute, relative, sep } from "node:path";

export const isWithin = (root: string, candidate: string): boolean => {
  const path = relative(root, candidate);
  return (
    path === "" ||
    (!isAbsolute(path) && !path.startsWith(`..${sep}`) && path !== "..")
  );
};
