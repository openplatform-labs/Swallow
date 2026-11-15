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
} = {}) {
  if (typeof refresh !== 'function') throw new TypeError('refresh is required');
  if (!Number.isFinite(retryDelayMs) || retryDelayMs < 1 || !Number.isFinite(connectTimeoutMs) || connectTimeoutMs < 1) {
    throw new RangeError('retry and connection timeouts must be positive');
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
    if (error) return;
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
    while (!stopped) {
      if (!connected) {
        await wait(retryDelayMs);
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
            if (!connected) await wait(connectTimeoutMs);
          } catch (error) {
            resetting = false;
            report(error);
          }
        }
      }
      if (stopped) break;
      if (!connected) continue;
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
          await wait(retryDelayMs);
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
