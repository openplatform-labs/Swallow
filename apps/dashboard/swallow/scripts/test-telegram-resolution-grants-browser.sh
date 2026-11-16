#!/usr/bin/env bash
# Local PostgreSQL, restricted API login, synthetic JWTs and real browser.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
API="$ROOT/apps/api/rust-api"; WORKER="$ROOT/apps/worker/rust-queue"; UI="$ROOT/apps/dashboard/swallow"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-telegram-resolution-grants-browser.XXXXXX")
API_PID= UI_PID=
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
cleanup() {
  local result=$?
  if (( result != 0 )) && [[ -f "$TEST_DIR/postgres.log" ]]; then tail -n 20 "$TEST_DIR/postgres.log" >&2; fi
  if [[ -n "$API_PID" ]]; then kill "$API_PID" 2>/dev/null || true; wait "$API_PID" 2>/dev/null || true; fi
  if [[ -n "$UI_PID" ]]; then kill "$UI_PID" 2>/dev/null || true; wait "$UI_PID" 2>/dev/null || true; fi
  if [[ -f "$TEST_DIR/data/postmaster.pid" ]] && ! "$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -m immediate -w stop >/dev/null 2>&1; then echo "Database retained at $TEST_DIR" >&2; return; fi
  python3 -c 'import shutil,sys; shutil.rmtree(sys.argv[1])' "$TEST_DIR"
}
trap cleanup EXIT
port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
API_PORT=$(port); UI_PORT=$(port)
while [[ "$API_PORT" == "$UI_PORT" ]]; do UI_PORT=$(port); done
DB_PORT=$(port)
"$TEST_PG_BIN/initdb" -D "$TEST_DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -o "-h 127.0.0.1 -p $DB_PORT -k /tmp" -l "$TEST_DIR/postgres.log" -w start >/dev/null
export DATABASE_URL="postgres://postgres@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
"$TEST_PG_BIN/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE ui_fixture_api LOGIN;
GRANT USAGE ON SCHEMA public TO ui_fixture_api;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_telegram_resolution_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_telegram_resolution_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_telegram_resolution_grant_audit_read(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_telegram_resolution_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_telegram_health(text,text,text,text,bigint),public.novu_rust_operator_jwt_telegram_attention(text,text,text,text,bigint,uuid) TO ui_fixture_api;
INSERT INTO public.novu_rust_environments VALUES('fixture-env','fixture-org');
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','admin','console','fixture-env'),('https://issuer.test','target','console','fixture-env');
INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='ui_fixture_api';
INSERT INTO public.novu_rust_delivery_operator_grants SELECT 'fixture-env',rolname,'alert_view',oid FROM pg_roles WHERE rolname='ui_fixture_api';
INSERT INTO public.novu_rust_operator_jwt_telegram_resolution_grant_admins VALUES('https://issuer.test','admin','console','fixture-env',true,true);
INSERT INTO public.novu_rust_operator_jwt_telegram_resolution_grant_audit_viewers VALUES('https://issuer.test','admin','console','fixture-env');
INSERT INTO public.novu_rust_telegram_operator_grants SELECT 'fixture-env',rolname,oid,true,true,true FROM pg_roles WHERE rolname='ui_fixture_api';
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES('11111111-1111-4111-8111-111111111111','fixture-env','experimental_telegram','{}','failed');
INSERT INTO public.novu_rust_telegram_attempts(job_id,environment_id,state) VALUES('11111111-1111-4111-8111-111111111111','fixture-env','unknown');
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
const task=SPACE===null ? await taskSpace('Swallow Telegram resolution grants real API test') : await taskSpace(SPACE);
await fs.writeFile(ROOT+'/space-id',String(task.spaceId));
const page=task.page('p1'),api='http://127.0.0.1:'+API;
const admin=await fs.readFile(ROOT+'/admin.jwt','utf8'),target=await fs.readFile(ROOT+'/target.jwt','utf8');
await page.goto('http://127.0.0.1:'+UI+'/');
async function fill(selector,value) {await page.evaluate(s=>document.querySelector(s)?.scrollIntoView({block:'center'}),selector);await page.fill(selector,value);}
await fill('input[name="apiOrigin"]',api);
async function settings() {
 await page.click('nav.sidenav button >> nth=6');
 await fill('input[name="permissionEnvironment"]','fixture-env');await fill('input[name="permissionKey"]',admin);
 await fill('input[name="permissionIssuer"]','https://issuer.test');await fill('input[name="permissionSubject"]','target');await fill('input[name="permissionClient"]','console');
}
async function change(permission,previous) {
 await page.selectOption('select[name="grantKind"]','telegram-resolution:'+permission);
 await page.click('loc=role:button[name="선택 권한 조회"]');
 await page.waitForFunction(v=>document.body.innerText.includes('telegram-resolution:'+v.permission+' 현재 상태: '+v.previous),{permission,previous});
 await page.click('input[name="grantDesired"]');await fill('textarea[name="grantReason"]','Telegram resolution grants browser '+permission);
 await page.click('input[name="grantConfirmed"]');await page.click('loc=role:button[name="변경 요청 준비"]');
 await page.waitForSelector('input[name="grantSubmitConfirmed"]');await page.click('input[name="grantSubmitConfirmed"]');await page.click('loc=role:button[name="변경 요청 제출"]');
 await page.waitForFunction(()=>document.body.innerText.includes('요청이 기록됐습니다. 권한이 변경됐습니다.'));
 await page.click('loc=role:button[name="요청 정보 지우기"]');
}
async function alerts() {
 await page.click('nav.sidenav button >> nth=5');await page.selectOption('select[name="alertAuthMode"]','jwt');
 await fill('input[name="alertEnvironment"]','fixture-env');await fill('input[name="alertKey"]',target);
}
await settings();await change('list','없음');await change('retry','없음');await change('accepted','없음');
await page.selectOption('select[name="grantAuditScope"]','telegram-resolution');await page.click('section:has(select[name="grantAuditScope"]) button:has-text("위임 이력 조회")');
await page.waitForFunction(()=>[...document.querySelectorAll('table')].some(t=>t.innerText.includes('Telegram resolution grants browser accepted')));
await alerts();await page.click('app-discord-reviews button:has-text("Telegram 검토 목록 조회")');
await page.waitForFunction(()=>document.querySelector('app-discord-reviews')?.innerText.includes('11111111-1111-4111-8111-111111111111'));
await settings();await change('list','허용');
await alerts();await page.click('app-discord-reviews button:has-text("Telegram 검토 목록 조회")');
await page.waitForFunction(()=>document.querySelector('app-discord-reviews')?.innerText.includes('목록을 조회하지 못했습니다.'));
console.log('Telegram resolution grants, list access and revocation passed');
'''
with open(root+'/browser.mjs','w') as f:f.write('const API='+json.dumps(api)+'; const UI='+json.dumps(ui)+'; const ROOT='+json.dumps(root)+'; const SPACE='+json.dumps(int(space) if space else None)+';\n'+script)
PY
ego-browser nodejs < "$TEST_DIR/browser.mjs"
SPACE_ID=$(cat "$TEST_DIR/space-id")
printf 'const task=await taskSpace(%s); await task.finish({keep:[]});\n' "$SPACE_ID" | ego-browser nodejs
[[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -At -v ON_ERROR_STOP=1 -c "SELECT count(*)=4 FROM public.novu_rust_operator_jwt_telegram_resolution_grant_audit WHERE environment_id='fixture-env'") == t ]]
[[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -At -v ON_ERROR_STOP=1 -c "SELECT NOT can_list AND can_retry AND can_accept FROM public.novu_rust_operator_jwt_telegram_resolution_grants WHERE environment_id='fixture-env' AND subject='target'") == t ]]
