import type { Novu } from '@novu/js';

export interface SocketRecoveryOptions {
  /** Fetch and apply all application-specific state. Reject on failure and honor cancellation. */
  refresh: (context: { signal: AbortSignal }) => void | Promise<void>;
  onError?: (error: unknown) => void;
  retryDelayMs?: number;
  connectTimeoutMs?: number;
}

/** Start optional recovery; return an idempotent stop function. */
export declare function startSocketRecovery(novu: Novu, options: SocketRecoveryOptions): () => void;
