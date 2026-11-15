#!/usr/bin/env bash
# Disposable local PostgreSQL, real Rust API and a built Angular dashboard.
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
API="$ROOT/apps/api/rust-api"
UI="$ROOT/apps/dashboard/swallow"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-key-browser.XXXXXX")
API_PID= UI_PID=
cleanup() {
  if [[ -n "$API_PID" ]]; then kill "$API_PID" 2>/dev/null || true; wait "$API_PID" 2>/dev/null || true; fi
  if [[ -n "$UI_PID" ]]; then kill "$UI_PID" 2>/dev/null || true; wait "$UI_PID" 2>/dev/null || true; fi
  "$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/db" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$TEST_DIR"
}
trap cleanup EXIT
port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
DB_PORT=$(port); API_PORT=$(port); UI_PORT=$(port)
while [[ "$DB_PORT" == "$API_PORT" || "$DB_PORT" == "$UI_PORT" || "$API_PORT" == "$UI_PORT" ]]; do API_PORT=$(port); UI_PORT=$(port); done
"$TEST_PG_BIN/initdb" -D "$TEST_DIR/db" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/db" -o "-h 127.0.0.1 -p $DB_PORT -k $TEST_DIR" -l "$TEST_DIR/db.log" start >/dev/null
python3 - "$API/tests/fixtures/operator_jwt/test-only-public.pem" "$TEST_DIR/keys.json" <<'PY'
import json,sys
with open(sys.argv[1]) as f: public=f.read()
with open(sys.argv[2], 'w') as f: json.dump({'issuer':'https://issuer.test','audience':'swallow','keys':[{'kid':'test','pem':public}]},f)
PY
cargo build --quiet --locked --manifest-path "$API/Cargo.toml" --bin novu-rust-api
(cd "$UI" && npm run build > "$TEST_DIR/ui-build.log" 2>&1)
env -i PATH="$PATH" DATABASE_URL="postgres://postgres@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true \
LISTEN_ADDR="127.0.0.1:$API_PORT" SWALLOW_OPERATOR_JWT_CONFIG_FILE="$TEST_DIR/keys.json" \
SWALLOW_OPERATOR_JWT_RELOAD_SECONDS=1 SWALLOW_OPERATOR_JWT_RELOAD_FAILURE=reject \
SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true SWALLOW_API_OPERATOR_KEY_HEALTH_ENABLED=true \
SWALLOW_CORS_ALLOWED_ORIGINS="http://127.0.0.1:$UI_PORT" \
"$API/target/debug/novu-rust-api" >"$TEST_DIR/api.log" 2>&1 & API_PID=$!
python3 -m http.server "$UI_PORT" --bind 127.0.0.1 --directory "$UI/dist/swallow/browser" >"$TEST_DIR/ui.log" 2>&1 & UI_PID=$!
for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:$API_PORT/health/operator-keys" >/dev/null 2>&1 && curl -fsS "http://127.0.0.1:$UI_PORT/" >/dev/null 2>&1; then break; fi
  kill -0 "$API_PID" && kill -0 "$UI_PID"
  sleep 0.1
done
curl -fsS "http://127.0.0.1:$API_PORT/health/operator-keys" >/dev/null
curl -fsS "http://127.0.0.1:$UI_PORT/" >/dev/null
python3 - "$API_PORT" "$UI_PORT" "$TEST_DIR" <<'PY'
import json,sys
api,ui,root=sys.argv[1:]
script='''const fs = await import('node:fs/promises');
const task = await taskSpace('Swallow key health real API test');
await fs.writeFile(ROOT + '/space-id', String(task.spaceId));
const page = task.page('p1');
const api = 'http://127.0.0.1:' + API;
const path = ROOT + '/keys.json';
const original = await fs.readFile(path, 'utf8');
async function replace(value) { await fs.writeFile(path + '.tmp', value); await fs.rename(path + '.tmp', path); }
async function waitStatus(expected) {
  const deadline=Date.now()+15000;
  while (Date.now()<deadline) { try { if ((await fetch(api+'/health/operator-keys')).status===expected) return; } catch {} await new Promise(resolve=>setTimeout(resolve,100)); }
  throw new Error('key health did not reach '+expected);
}
await page.goto('http://127.0.0.1:' + UI + '/');
await page.fill('input[name="apiOrigin"]', api);
await page.click('loc=role:button[name="키 갱신 상태 확인"]');
await page.waitForFunction(()=>document.body.innerText.includes('최근 키 갱신 상태가 정상입니다'));
await replace('{'); await waitStatus(503);
await page.click('loc=role:button[name="키 갱신 상태 확인"]');
await page.waitForFunction(()=>document.body.innerText.includes('키 갱신이 실패했거나 지연되고 있습니다'));
await replace(original); await waitStatus(200);
await page.click('loc=role:button[name="키 갱신 상태 확인"]');
await page.waitForFunction(()=>document.body.innerText.includes('최근 키 갱신 상태가 정상입니다'));
console.log('Real API browser test: healthy, invalid configuration, recovery passed');
'''
with open(root+'/browser.mjs','w') as f: f.write('const API='+json.dumps(api)+'; const UI='+json.dumps(ui)+'; const ROOT='+json.dumps(root)+';\n'+script)
PY
ego-browser nodejs < "$TEST_DIR/browser.mjs"
SPACE_ID=$(cat "$TEST_DIR/space-id")
printf 'const task=await taskSpace(%s); await task.finish({keep:[]});\n' "$SPACE_ID" | ego-browser nodejs
