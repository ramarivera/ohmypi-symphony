import { Effect } from "effect";
import { DriverError, type DriverKind } from "./driver.js";

/** A contradictory override fails closed; array order never selects a driver. */
export const selectDriver = (
  defaultDriver: DriverKind,
  labels: ReadonlyArray<string>,
): Effect.Effect<DriverKind, DriverError> => {
  const overrides = new Set(
    labels
      .map((label) => label.trim().toLowerCase())
      .filter((label) => label === "driver:omp" || label === "driver:t3"),
  );
  if (overrides.size > 1)
    return Effect.fail(
      new DriverError({
        driver: defaultDriver,
        operation: "select",
        reason: "rejected",
        submission: "not-submitted",
        message:
          "Conflicting driver:omp and driver:t3 labels; keep only one driver override",
      }),
    );
  return Effect.succeed(
    overrides.has("driver:t3")
      ? ("t3" as const)
      : overrides.has("driver:omp")
        ? ("omp" as const)
        : defaultDriver,
  );
};
