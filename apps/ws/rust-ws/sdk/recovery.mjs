/**
 * Optional recovery for @novu/js Socket.IO clients. Install before the first
 * connection; stop before intentionally disconnecting or changing sessions.
 * Refresh must fetch authoritative application state and honor the signal.
 */
export function startSocketRecovery(novu, {
  refresh,
  onError = () => {},
  retryDelayMs = 1000,
  connectTimeoutMs = 10000,
  backoffFactor = 1,
  maxRetryDelayMs = Math.max(retryDelayMs, 30000),
  jitterRatio = 0,
} = {}) {
  if (typeof refresh !== 'function') throw new TypeError('refresh is required');
  if (typeof onError !== 'function') throw new TypeError('onError must be a function');
  const maxTimerMs = 2147483647;
  if (!Number.isFinite(retryDelayMs) || retryDelayMs < 1 || retryDelayMs > maxTimerMs
    || !Number.isFinite(connectTimeoutMs) || connectTimeoutMs < 1 || connectTimeoutMs > maxTimerMs
    || !Number.isFinite(backoffFactor) || backoffFactor < 1
    || !Number.isFinite(maxRetryDelayMs) || maxRetryDelayMs < retryDelayMs || maxRetryDelayMs > maxTimerMs
    || !Number.isFinite(jitterRatio) || jitterRatio < 0 || jitterRatio > 1) {
    throw new RangeError('invalid retry, timeout, backoff or jitter configuration');
  }
  let stopped = false;
  let connected = false;
  let resetting = false;
  let recovering = false;
  let epoch = 0;
  let activeRefresh;
  let timer;
  let wake;

  function cancelWait() {
    clearTimeout(timer);
    timer = undefined;
    const resolve = wake;
    wake = undefined;
    resolve?.();
  }
  function wait(ms) {
    return new Promise(resolve => {
      wake = resolve;
      timer = setTimeout(cancelWait, ms);
    });
  }
  function report(error) {
    try { onError(error); } catch { /* reporting must not disable recovery */ }
  }
  const unsubscribeConnect = novu.on('socket.connect.resolved', ({ error } = {}) => {
    if (error) {
      if (recovering && !stopped) report(error);
      return;
    }
    connected = true;
    cancelWait();
  });
  const unsubscribeDisconnect = novu.on('socket.disconnect.resolved', () => {
    connected = false;
    if (resetting || stopped) return;
    epoch++;
    activeRefresh?.abort();
    cancelWait();
    if (!recovering) void recover();
  });

  async function recover() {
    recovering = true;
    let retryDelay = retryDelayMs;
    const advanceDelay = () => { retryDelay = Math.min(maxRetryDelayMs, retryDelay * backoffFactor); };
    const nextDelay = () => Math.max(1, Math.min(maxRetryDelayMs, retryDelay * (1 - jitterRatio + 2 * jitterRatio * Math.random())));
    while (!stopped) {
      if (!connected) {
        await wait(nextDelay());
        if (stopped) break;
        // Automatic transport reconnection may have already succeeded.
        if (!connected) {
          try {
            resetting = true;
            const disconnected = await novu.socket.disconnect();
            resetting = false;
            if (disconnected?.error) throw disconnected.error;
            if (stopped) break;
            const result = await novu.socket.connect();
            if (result?.error) throw result.error;
            if (!stopped && !connected) await wait(connectTimeoutMs);
          } catch (error) {
            resetting = false;
            report(error);
          }
        }
      }
      if (stopped) break;
      if (!connected) { advanceDelay(); continue; }
      const generation = epoch;
      const controller = new AbortController();
      activeRefresh = controller;
      try {
        // Do not let a callback that ignores cancellation stall subsequent retries.
        const aborted = new Promise(resolve => controller.signal.addEventListener('abort', resolve, { once: true }));
        await Promise.race([Promise.resolve().then(() => refresh({ signal: controller.signal })), aborted]);
        if (!stopped && connected && generation === epoch && !controller.signal.aborted) break;
      } catch (error) {
        if (!stopped && generation === epoch) {
          report(error);
          if (!stopped) {
            await wait(nextDelay());
            advanceDelay();
          }
        }
      } finally {
        if (activeRefresh === controller) activeRefresh = undefined;
      }
    }
    recovering = false;
  }

  return () => {
    if (stopped) return;
    stopped = true;
    unsubscribeConnect();
    unsubscribeDisconnect();
    activeRefresh?.abort();
    cancelWait();
  };
}
