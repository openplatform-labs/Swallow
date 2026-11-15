import { test } from 'node:test';
import assert from 'node:assert/strict';
import { startSocketRecovery } from '../sdk/recovery.mjs';

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(predicate) {
  for (let i = 0; i < 200; i++) {
    if (predicate()) return;
    await delay(5);
  }
  throw new Error('timed out');
}
function client() {
  const listeners = new Map();
  const emit = (name, value = {}) => [...(listeners.get(name) ?? [])].forEach(fn => fn(value));
  const sdk = {
    on(name, fn) {
      if (!listeners.has(name)) listeners.set(name, new Set());
      listeners.get(name).add(fn);
      return () => listeners.get(name).delete(fn);
    },
    socket: {
      async disconnect() { emit('socket.disconnect.resolved'); return {}; },
      async connect() { emit('socket.connect.resolved'); return {}; },
    },
  };
  return { sdk, emit };
}

test('retries a failed refresh and stops without reconnecting intentionally', async () => {
  const { sdk, emit } = client();
  let calls = 0, errors = 0, connects = 0;
  const connect = sdk.socket.connect;
  sdk.socket.connect = async () => { connects++; return connect(); };
  const stop = startSocketRecovery(sdk, {
    retryDelayMs: 5, connectTimeoutMs: 10,
    refresh: async () => { if (++calls === 1) throw new Error('temporary'); },
    onError: () => errors++,
  });
  emit('socket.connect.resolved');
  emit('socket.disconnect.resolved');
  await until(() => calls === 2);
  assert.equal(errors, 1);
  assert.equal(connects, 1);
  stop();
  emit('socket.disconnect.resolved');
  await delay(30);
  assert.equal(connects, 1);
});

test('a new disconnect aborts stale refresh and starts another', async () => {
  const { sdk, emit } = client();
  let calls = 0, firstSignal;
  const stop = startSocketRecovery(sdk, {
    retryDelayMs: 5, connectTimeoutMs: 10,
    refresh: ({ signal }) => {
      calls++;
      if (calls === 1) { firstSignal = signal; return new Promise(() => {}); }
    },
  });
  emit('socket.connect.resolved');
  emit('socket.disconnect.resolved');
  await until(() => calls === 1);
  emit('socket.disconnect.resolved');
  await until(() => calls === 2);
  assert.equal(firstSignal.aborted, true);
  stop();
});

test('retries connection timeout and cancels pending retries on stop', async () => {
  const { sdk, emit } = client();
  let attempts = 0, refreshed = 0, errors = [];
  sdk.socket.connect = async () => {
    if (++attempts === 2) emit('socket.connect.resolved');
    else emit('socket.connect.resolved', {error: new Error('connection refused')});
    return {};
  };
  const stop = startSocketRecovery(sdk, {
    retryDelayMs: 5, connectTimeoutMs: 10,
    refresh: () => { refreshed++; },
    onError: error => errors.push(error.message),
  });
  emit('socket.disconnect.resolved');
  await until(() => refreshed === 1);
  assert.equal(attempts, 2);
  assert.deepEqual(errors, ['connection refused']);
  emit('socket.disconnect.resolved');
  stop();
  await delay(40);
  assert.equal(attempts, 2);
});

test('stopping from a connection error does not schedule another attempt', async () => {
  const { sdk, emit } = client();
  let attempts = 0, reports = 0, stop;
  sdk.socket.connect = async () => {
    attempts++;
    emit('socket.connect.resolved', { error: new Error('unauthorized') });
    return {};
  };
  stop = startSocketRecovery(sdk, {
    retryDelayMs: 5, connectTimeoutMs: 1000,
    refresh: () => { throw new Error('unexpected refresh'); },
    onError: () => { reports++; stop(); },
  });
  emit('socket.disconnect.resolved');
  await until(() => reports === 1);
  await delay(30);
  assert.equal(attempts, 1);
});

test('configurable backoff and jitter are capped and reset after recovery', async () => {
  const { sdk, emit } = client();
  const originalTimer = globalThis.setTimeout;
  const originalRandom = Math.random;
  const scheduled = [];
  let attempts = 0, refreshes = 0, stop;
  globalThis.setTimeout = (fn, ms, ...args) => { scheduled.push(ms); return originalTimer(fn, ms, ...args); };
  Math.random = () => 1;
  sdk.socket.connect = async () => {
    if (++attempts === 4 || attempts === 5) emit('socket.connect.resolved');
    return {};
  };
  try {
    stop = startSocketRecovery(sdk, {
      retryDelayMs: 10, connectTimeoutMs: 3, backoffFactor: 2, maxRetryDelayMs: 40, jitterRatio: 0.5,
      refresh: () => { refreshes++; },
    });
    emit('socket.disconnect.resolved');
    await until(() => refreshes === 1);
    assert.deepEqual(scheduled.filter(ms => ms !== 5 && ms !== 3), [15,30,40,40]);
    emit('socket.disconnect.resolved');
    await until(() => refreshes === 2);
    assert.equal(scheduled.filter(ms => ms !== 5 && ms !== 3).at(-1),15);
  } finally {
    stop?.();
    globalThis.setTimeout = originalTimer;
    Math.random = originalRandom;
  }
});

test('rejects invalid recovery delay settings', () => {
  const { sdk } = client();
  for (const options of [
    { backoffFactor: 0.5 }, { maxRetryDelayMs: 5, retryDelayMs: 10 },
    { jitterRatio: 1.1 }, { retryDelayMs: Infinity }, { connectTimeoutMs: 2147483648 },
  ]) assert.throws(() => startSocketRecovery(sdk, {refresh: () => {}, ...options}), RangeError);
  assert.throws(() => startSocketRecovery(sdk, {refresh: () => {}, onError: null}), TypeError);
  assert.throws(() => startSocketRecovery(sdk, {refresh: () => {}, recoverInitialConnection: 'true'}), TypeError);
  assert.throws(() => startSocketRecovery(sdk, {refresh: () => {}, initialConnectTimeoutMs: 10}), TypeError);
  assert.throws(() => startSocketRecovery(sdk, {refresh: () => {}, recoverInitialConnection: true, initialConnectTimeoutMs: 0}), RangeError);
});

test('initial socket errors are retried only when explicitly enabled', async () => {
  const disabled=client();
  let disabledAttempts=0, disabledErrors=0;
  disabled.sdk.socket.connect=async()=>{ disabledAttempts++; return {}; };
  const stopDisabled=startSocketRecovery(disabled.sdk,{
    retryDelayMs:5, refresh:()=>{}, onError:()=>disabledErrors++,
  });
  disabled.emit('socket.connect.resolved',{error:new Error('initial failure')});
  await delay(20);
  assert.equal(disabledAttempts,0);
  assert.equal(disabledErrors,0);
  stopDisabled();

  const enabled=client();
  let attempts=0, reports=0, refreshes=0;
  const originalConnect=enabled.sdk.socket.connect;
  enabled.sdk.socket.connect=async()=>{ attempts++; return originalConnect(); };
  const stopEnabled=startSocketRecovery(enabled.sdk,{
    retryDelayMs:5, recoverInitialConnection:true,
    refresh:()=>refreshes++, onError:()=>reports++,
  });
  enabled.emit('socket.connect.resolved',{error:new Error('initial failure')});
  await until(()=>refreshes===1);
  assert.equal(attempts,1);
  assert.equal(reports,1);
  stopEnabled();
});

test('initial error callback can stop before retry starts', async () => {
  const {sdk,emit}=client();
  let attempts=0, stop;
  sdk.socket.connect=async()=>{attempts++; return {};};
  stop=startSocketRecovery(sdk,{
    retryDelayMs:5,recoverInitialConnection:true,refresh:()=>{},onError:()=>stop(),
  });
  emit('socket.connect.resolved',{error:new Error('authorization failed')});
  await delay(20);
  assert.equal(attempts,0);
});

test('initial connection watchdog reports timeout and recovers', async () => {
  const {sdk,emit}=client();
  let attempts=0, refreshes=0, errors=[];
  const connect=sdk.socket.connect;
  sdk.socket.connect=async()=>{attempts++;return connect();};
  const stop=startSocketRecovery(sdk,{
    recoverInitialConnection:true,initialConnectTimeoutMs:10,retryDelayMs:5,
    refresh:()=>refreshes++,onError:error=>errors.push(error.code),
  });
  emit('socket.connect.pending');
  await until(()=>refreshes===1);
  assert.equal(attempts,1);
  assert.deepEqual(errors,['SWALLOW_SOCKET_INITIAL_TIMEOUT']);
  stop();
});

test('successful initial connect and stop cancel the watchdog', async () => {
  for (const mode of ['connect','stop']) {
    const {sdk,emit}=client();
    let errors=0;
    const stop=startSocketRecovery(sdk,{
      recoverInitialConnection:true,initialConnectTimeoutMs:10,
      refresh:()=>{},onError:()=>errors++,
    });
    emit('socket.connect.pending');
    if(mode==='connect')emit('socket.connect.resolved');
    else stop();
    await delay(25);
    assert.equal(errors,0);
    stop();
  }
});

test('initial SDK error cancels its watchdog and starts only one recovery', async () => {
  const {sdk,emit}=client();
  let reports=0, refreshes=0;
  const stop=startSocketRecovery(sdk,{
    recoverInitialConnection:true,initialConnectTimeoutMs:20,retryDelayMs:5,
    refresh:()=>refreshes++,onError:()=>reports++,
  });
  emit('socket.connect.pending');
  emit('socket.connect.resolved',{error:new Error('connection refused')});
  await until(()=>refreshes===1);
  await delay(30);
  assert.equal(reports,1);
  assert.equal(refreshes,1);
  stop();
});
