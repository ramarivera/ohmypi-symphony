import { BunRuntime } from "@effect/platform-bun";
import { Effect } from "effect";
import { main } from "./conductor.js";

BunRuntime.runMain(Effect.scoped(main));
