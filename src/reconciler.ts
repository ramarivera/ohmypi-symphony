import type { Logger } from "./logger";
import { createLogger } from "./logger";
import type { SessionAuthority } from "./session-authority";

export interface ReconcilerStatus {
  readonly running: boolean;
  readonly lastStartedAt: number | null;
  readonly lastCompletedAt: number | null;
  readonly lastError: string | null;
}

export class Reconciler {
  readonly #authority: SessionAuthority;
  readonly #intervalMs: number;
  readonly #logger: Logger;
  #timer: Timer | null = null;
  #inFlight: Promise<void> | null = null;
  #lastStartedAt: number | null = null;
  #lastCompletedAt: number | null = null;
  #lastError: string | null = null;

  constructor(
    authority: SessionAuthority,
    intervalMs = 1_000,
    logger?: Logger,
  ) {
    this.#authority = authority;
    this.#intervalMs = intervalMs;
    this.#logger =
      logger ??
      createLogger({ name: "reconciler" }).child({ component: "reconciler" });
  }
  get status(): ReconcilerStatus {
    return {
      running: this.#timer !== null,
      lastStartedAt: this.#lastStartedAt,
      lastCompletedAt: this.#lastCompletedAt,
      lastError: this.#lastError,
    };
  }

  start(): void {
    if (this.#timer) return;
    this.#logger.info({ event: "reconciler.started" });
    this.#timer = setInterval(() => {
      void this.tick();
    }, this.#intervalMs);
    void this.tick();
  }

  async tick(): Promise<void> {
    if (this.#inFlight) return this.#inFlight;
    this.#lastStartedAt = Date.now();
    this.#logger.debug({ event: "reconciler.tick" });
    this.#inFlight = this.#authority
      .processRunnable()
      .then(() => {
        this.#lastCompletedAt = Date.now();
        this.#lastError = null;
      })
      .catch((error: unknown) => {
        this.#lastError =
          error instanceof Error ? error.message : String(error);
        this.#logger.error({
          event: "reconciler.tick.error",
          error: this.#lastError,
        });
      })
      .finally(() => {
        this.#inFlight = null;
      });
    return this.#inFlight;
  }

  async stop(): Promise<void> {
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = null;
    this.#logger.info({ event: "reconciler.stopped" });
    if (this.#inFlight) await this.#inFlight;
    await this.#authority.shutdown();
  }
}
