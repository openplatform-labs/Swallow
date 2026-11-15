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
  let attempts = 0, refreshed = 0;
  sdk.socket.connect = async () => {
    if (++attempts === 2) emit('socket.connect.resolved');
    return {};
  };
  const stop = startSocketRecovery(sdk, {
    retryDelayMs: 5, connectTimeoutMs: 10,
    refresh: () => { refreshed++; },
  });
  emit('socket.disconnect.resolved');
  await until(() => refreshed === 1);
  assert.equal(attempts, 2);
  emit('socket.disconnect.resolved');
  stop();
  await delay(40);
  assert.equal(attempts, 2);
});
