import { redact } from "../admin-ui/run-detail.js";
import type { ActivityType } from "../domain/models.js";

export const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const isString = (value: unknown): value is string =>
  typeof value === "string";

export const isNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);

const SIGNALS: Record<string, true> = {
  auth: true,
  continue: true,
  select: true,
  stop: true,
};
export const isActivitySignal = (
  value: unknown,
): value is "auth" | "continue" | "select" | "stop" =>
  isString(value) && SIGNALS[value] === true;

const ACTIVITY_TYPES: Record<ActivityType, true> = {
  thought: true,
  action: true,
  elicitation: true,
  response: true,
  error: true,
};
export const isActivityType = (value: unknown): value is ActivityType =>
  isString(value) && value in ACTIVITY_TYPES;

export const redactStringValues = (value: unknown): unknown => {
  if (isString(value)) return redact(value);
  if (Array.isArray(value)) return value.map(redactStringValues);
  if (isRecord(value)) {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        key,
        redactStringValues(entry),
      ]),
    );
  }
  return value;
};
