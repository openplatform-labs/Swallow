#!/usr/bin/env bash
# Docker PostgreSQL, restricted API login, synthetic JWTs and real browser.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
API="$ROOT/apps/api/rust-api"; WORKER="$ROOT/apps/worker/rust-queue"; UI="$ROOT/apps/dashboard/swallow"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-discord-endpoints-browser.XXXXXX")
API_PID= UI_PID=
CONTAINER="swallow-discord-endpoints-$$"
cleanup() {
  if [[ -n "$API_PID" ]]; then kill "$API_PID" 2>/dev/null || true; wait "$API_PID" 2>/dev/null || true; fi
  if [[ -n "$UI_PID" ]]; then kill "$UI_PID" 2>/dev/null || true; wait "$UI_PID" 2>/dev/null || true; fi
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  rm -rf "$TEST_DIR"
}
trap cleanup EXIT
port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
API_PORT=$(port); UI_PORT=$(port)
while [[ "$API_PORT" == "$UI_PORT" ]]; do UI_PORT=$(port); done
docker run -d --rm --name "$CONTAINER" -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
for _ in $(seq 1 100); do docker exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 0.1; done
DB_PORT=$(docker port "$CONTAINER" 5432/tcp | awk -F: '{print $NF}')
export DATABASE_URL="postgres://postgres@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
docker exec -i "$CONTAINER" psql -U postgres -d postgres -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE ui_fixture_api LOGIN;
GRANT USAGE ON SCHEMA public TO ui_fixture_api;
GRANT SELECT ON public.novu_rust_users,public.novu_rust_api_keys,public.novu_rust_discord_destinations TO ui_fixture_api;
GRANT SELECT,UPDATE ON public.novu_rust_subscribers TO ui_fixture_api;
GRANT SELECT,INSERT,UPDATE,DELETE ON public.novu_rust_discord_workflow_endpoints TO ui_fixture_api;
GRANT INSERT ON public.novu_rust_discord_endpoint_audit TO ui_fixture_api;
GRANT EXECUTE ON FUNCTION public.novu_rust_lock_discord_endpoint_grant(text,text,text),public.novu_rust_discord_endpoint_audit_list(text,text,uuid) TO ui_fixture_api;
INSERT INTO public.novu_rust_users VALUES('fixture-user');
INSERT INTO public.novu_rust_environments VALUES('fixture-env','fixture-org');
INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES(encode(sha256('fixture-api-key'::bytea),'hex'),'fixture-env','fixture-user');
INSERT INTO public.novu_rust_discord_endpoint_permissions VALUES(encode(sha256('fixture-api-key'::bytea),'hex'),'read'),(encode(sha256('fixture-api-key'::bytea),'hex'),'write'),(encode(sha256('fixture-api-key'::bytea),'hex'),'audit');
INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES('fixture-env','alice','alice-generation');
INSERT INTO public.novu_rust_discord_destinations VALUES('fixture-env','first','hash1'),('fixture-env','second','hash2');
SQL
python3 - "$API/tests/fixtures/operator_jwt/test-only-public.pem" "$API/tests/fixtures/operator_jwt/test-only-private.pem" "$TEST_DIR" <<'PY'
import base64,json,subprocess,sys,time
public,private,root=sys.argv[1:]
with open(public) as f: key=f.read()
with open(root+'/keys.json','w') as f: json.dump({'issuer':'https://issuer.test','audience':'swallow','keys':[{'kid':'fixture','pem':key}]},f)
def b64(v): return base64.urlsafe_b64encode(v).decode().rstrip('=')
for name in ['admin','target']:
    h=b64(json.dumps({'alg':'RS256','typ':'at+jwt','kid':'fixture'},separators=(',',':')).encode())
    p=b64(json.dumps({'iss':'https://issuer.test','aud':'swallow','sub':name,'client_id':'console','iat':int(time.time())-1,'exp':int(time.time())+900,'jti':'fixture-'+name},separators=(',',':')).encode())
    message=(h+'.'+p).encode(); sig=subprocess.run(['openssl','dgst','-sha256','-sign',private],input=message,stdout=subprocess.PIPE,check=True).stdout
    with open(root+'/'+name+'.jwt','w') as f: f.write(message.decode()+'.'+b64(sig))
PY
cargo build --quiet --locked --manifest-path "$API/Cargo.toml" --bin novu-rust-api
(cd "$UI" && npm run build > "$TEST_DIR/ui-build.log" 2>&1) || { cat "$TEST_DIR/ui-build.log"; exit 1; }
env -i PATH="$PATH" DATABASE_URL="postgres://ui_fixture_api@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true \
LISTEN_ADDR="127.0.0.1:$API_PORT" SWALLOW_OPERATOR_JWT_CONFIG_FILE="$TEST_DIR/keys.json" \
SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true SWALLOW_CORS_ALLOWED_ORIGINS="http://127.0.0.1:$UI_PORT" \
"$API/target/debug/novu-rust-api" >"$TEST_DIR/api.log" 2>&1 & API_PID=$!
python3 -m http.server "$UI_PORT" --bind 127.0.0.1 --directory "$UI/dist/swallow/browser" >"$TEST_DIR/ui.log" 2>&1 & UI_PID=$!
for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null 2>&1 && curl -fsS "http://127.0.0.1:$UI_PORT/" >/dev/null 2>&1; then break; fi
  kill -0 "$API_PID" && kill -0 "$UI_PID"; sleep 0.1
done
curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null
python3 - "$API_PORT" "$UI_PORT" "$TEST_DIR" "${TEST_EGO_SPACE_ID:-}" <<'PY'
import json,sys
api,ui,root,space=sys.argv[1:]
script='''const fs=await import('node:fs/promises');
const task=SPACE===null ? await taskSpace('Swallow Discord endpoints real API test') : await taskSpace(SPACE);
await fs.writeFile(ROOT+'/space-id',String(task.spaceId));
const page=task.page('p1'),api='http://127.0.0.1:'+API;
await page.goto('http://127.0.0.1:'+UI+'/');
async function fill(selector,value) {await page.evaluate(s=>document.querySelector(s)?.scrollIntoView({block:'center'}),selector);await page.fill(selector,value);}
await fill('input[name="apiOrigin"]',api);await page.click('nav.sidenav button >> nth=3');
await fill('input[name="discordEndpointKey"]','fixture-api-key');
await fill('input[name="discordEndpointId"]','browser-binding');await fill('input[name="discordEndpointSubscriber"]','alice');await fill('input[name="discordEndpointDestination"]','first');await fill('textarea[name="discordEndpointContext"]','{"tenant":"acme"}');
await page.click('input[name="discordEndpointConfirm"]');await page.click('app-discord-endpoints button:has-text("연결 작업 실행")');
await page.waitForFunction(()=>document.querySelector('app-discord-endpoints')?.innerText.includes('변경 응답을 확인했습니다.'));
await page.click('app-discord-endpoints button:has-text("연결 목록 조회")');await page.waitForFunction(()=>document.querySelector('app-discord-endpoints table')?.innerText.includes('tenant:acme'));
await page.selectOption('select[name="discordEndpointOperation"]','update');await page.click('app-discord-endpoints button:has-text("변경 대상 조회")');await page.waitForFunction(()=>document.querySelector('app-discord-endpoints')?.innerText.includes('조회한 대상: browser-binding'));
await fill('input[name="discordEndpointDestination"]','second');await fill('textarea[name="discordEndpointContext"]','');await page.click('input[name="discordEndpointConfirm"]');await page.click('app-discord-endpoints button:has-text("연결 작업 실행")');
await page.waitForFunction(()=>document.querySelector('app-discord-endpoints')?.innerText.includes('변경 응답을 확인했습니다.'));
await page.click('app-discord-endpoints button:has-text("연결 목록 조회")');await page.waitForFunction(()=>document.querySelector('app-discord-endpoints table')?.innerText.includes('second'));
await page.selectOption('select[name="discordEndpointOperation"]','delete');await page.click('app-discord-endpoints button:has-text("변경 대상 조회")');await page.waitForFunction(()=>document.querySelector('app-discord-endpoints')?.innerText.includes('조회한 대상: browser-binding'));
await page.click('input[name="discordEndpointConfirm"]');await page.click('app-discord-endpoints button:has-text("연결 작업 실행")');await page.waitForFunction(()=>document.querySelector('app-discord-endpoints')?.innerText.includes('변경 응답을 확인했습니다.'));
await page.click('app-discord-endpoints button:has-text("연결 목록 조회")');await page.waitForFunction(()=>document.querySelector('app-discord-endpoints')?.innerText.includes('조회된 연결이 없습니다.'));
await page.click('app-discord-endpoints button:has-text("연결 이력 조회")');
await page.waitForFunction(()=>{const tables=[...document.querySelectorAll('app-discord-endpoints table')];return tables.some(t=>t.innerText.includes('browser-binding')&&t.innerText.includes('create')&&t.innerText.includes('update')&&t.innerText.includes('delete'));});
console.log('Discord endpoint browser create, list, update, delete and audit passed');
'''
with open(root+'/browser.mjs','w') as f:f.write('const API='+json.dumps(api)+'; const UI='+json.dumps(ui)+'; const ROOT='+json.dumps(root)+'; const SPACE='+json.dumps(int(space) if space else None)+';\n'+script)
PY
ego-browser nodejs < "$TEST_DIR/browser.mjs"
SPACE_ID=$(cat "$TEST_DIR/space-id")
printf 'const task=await taskSpace(%s); await task.finish({keep:[]});\n' "$SPACE_ID" | ego-browser nodejs
[[ $(docker exec "$CONTAINER" psql -U postgres -d postgres -At -v ON_ERROR_STOP=1 -c "SELECT count(*)=3 FROM public.novu_rust_discord_endpoint_audit WHERE environment_id='fixture-env' AND identifier='browser-binding'") == t ]]
[[ $(docker exec "$CONTAINER" psql -U postgres -d postgres -At -v ON_ERROR_STOP=1 -c "SELECT count(*)=0 FROM public.novu_rust_discord_workflow_endpoints WHERE environment_id='fixture-env'") == t ]]
