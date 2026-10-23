import { BunRuntime } from "@effect/platform-bun";
import { Cause, Effect, Exit } from "effect";
import { main } from "./conductor.js";

BunRuntime.runMain(Effect.scoped(main), {
  // Preserve v3's successful exit after signal-driven, interruption-only shutdown.
  teardown: (exit, onExit) =>
    onExit(
      Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause) ? 1 : 0,
    ),
});
