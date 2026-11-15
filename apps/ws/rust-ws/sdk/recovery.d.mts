import type { Novu } from '@novu/js';

export interface SocketRecoveryOptions {
  /** Fetch and apply all application-specific state. Reject on failure and honor cancellation. */
  refresh: (context: { signal: AbortSignal }) => void | Promise<void>;
  onError?: (error: unknown) => void;
  retryDelayMs?: number;
  connectTimeoutMs?: number;
  /** Retry multiplier; defaults to 1 (fixed delay). */
  backoffFactor?: number;
  maxRetryDelayMs?: number;
  /** Symmetric randomization from 0 to 1, capped at maxRetryDelayMs. */
  jitterRatio?: number;
}

/** Start optional recovery; return an idempotent stop function. */
export declare function startSocketRecovery(novu: Novu, options: SocketRecoveryOptions): () => void;
