#!/usr/bin/env bash
# Local disposable DB, restricted API login, synthetic JWTs and real browser.
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
API="$ROOT/apps/api/rust-api"; WORKER="$ROOT/apps/worker/rust-queue"; UI="$ROOT/apps/dashboard/swallow"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-grant-browser.XXXXXX")
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
export DATABASE_URL="postgres://postgres@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
"$TEST_PG_BIN/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE ui_fixture_api LOGIN;
GRANT USAGE ON SCHEMA public TO ui_fixture_api;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_429_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_resend_429_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_resend_429_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_resend_429_grant_audit_read(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_resend_cooldown_health(text,text,text,text,bigint,integer),public.novu_rust_operator_jwt_resend_cooldown_policy_read(text,text,text,text,bigint),public.novu_rust_operator_jwt_resend_cooldown_policy_change(text,text,text,text,bigint,uuid,bigint,integer,text),public.novu_rust_operator_jwt_resend_cooldown_policy_audit_read(text,text,text,text,bigint,uuid) TO ui_fixture_api;
INSERT INTO public.novu_rust_environments VALUES('fixture-env','fixture-org');
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','admin','console','fixture-env'),('https://issuer.test','target','console','fixture-env');
INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='ui_fixture_api';
INSERT INTO public.novu_rust_operator_jwt_resend_429_grant_admins VALUES('https://issuer.test','admin','console','fixture-env',true,true);
INSERT INTO public.novu_rust_operator_jwt_resend_429_grant_audit_viewers VALUES('https://issuer.test','admin','console','fixture-env');
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'fixture-env',rolname,'alert_view',oid FROM pg_roles WHERE rolname='ui_fixture_api';
INSERT INTO public.novu_rust_resend_cooldown_policy_admins(environment_id,database_principal,principal_oid,can_read,can_write) SELECT 'fixture-env',rolname,oid,true,true FROM pg_roles WHERE rolname='ui_fixture_api';
INSERT INTO public.novu_rust_resend_cooldown_policies(environment_id,fallback_seconds,not_before) VALUES('fixture-env',30,clock_timestamp()+interval '5 minutes');
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
const task=SPACE===null ? await taskSpace('Swallow Resend cooldown grant real API test') : await taskSpace(SPACE);
await fs.writeFile(ROOT+'/space-id',String(task.spaceId));
const page=task.page('p1'), api='http://127.0.0.1:'+API;
const admin=await fs.readFile(ROOT+'/admin.jwt','utf8'), target=await fs.readFile(ROOT+'/target.jwt','utf8');
await page.goto('http://127.0.0.1:'+UI+'/');
await page.evaluate(selector=>document.querySelector(selector)?.scrollIntoView({block:'center'}),'input[name="apiOrigin"]');
await page.fill('input[name="apiOrigin"]',api);
async function settings() {
  await page.click('nav.sidenav button >> nth=6');
  await page.evaluate(selector=>document.querySelector(selector)?.scrollIntoView({block:'center'}),'input[name="permissionEnvironment"]');
  await page.fill('input[name="permissionEnvironment"]','fixture-env');
  await page.evaluate(selector=>document.querySelector(selector)?.scrollIntoView({block:'center'}),'input[name="permissionKey"]');
  await page.fill('input[name="permissionKey"]',admin);
  await page.evaluate(selector=>document.querySelector(selector)?.scrollIntoView({block:'center'}),'input[name="permissionIssuer"]');
  await page.fill('input[name="permissionIssuer"]','https://issuer.test');
  await page.evaluate(selector=>document.querySelector(selector)?.scrollIntoView({block:'center'}),'input[name="permissionSubject"]');
  await page.fill('input[name="permissionSubject"]','target');
  await page.evaluate(selector=>document.querySelector(selector)?.scrollIntoView({block:'center'}),'input[name="permissionClient"]');
  await page.fill('input[name="permissionClient"]','console');
  await page.selectOption('select[name="grantKind"]','resend-429:cooldown-health');
  await page.click('loc=role:button[name="선택 권한 조회"]');
}
async function submit(reason) {
  await page.click('input[name="grantDesired"]');
  await page.evaluate(selector=>document.querySelector(selector)?.scrollIntoView({block:'center'}),'textarea[name="grantReason"]');
  await page.fill('textarea[name="grantReason"]',reason);
  await page.click('input[name="grantConfirmed"]');
  await page.click('loc=role:button[name="변경 요청 준비"]');
  await page.waitForSelector('input[name="grantSubmitConfirmed"]');
  await page.click('input[name="grantSubmitConfirmed"]');
  await page.click('loc=role:button[name="변경 요청 제출"]');
  await page.waitForFunction(()=>document.body.innerText.includes('요청이 기록됐습니다. 권한이 변경됐습니다.'));
}
async function alerts() {
  await page.click('nav.sidenav button >> nth=5');
  await page.selectOption('select[name="alertAuthMode"]','jwt');
  await page.evaluate(selector=>document.querySelector(selector)?.scrollIntoView({block:'center'}),'input[name="alertEnvironment"]');
  await page.fill('input[name="alertEnvironment"]','fixture-env');
  await page.evaluate(selector=>document.querySelector(selector)?.scrollIntoView({block:'center'}),'input[name="alertKey"]');
  await page.fill('input[name="alertKey"]',target);
  await page.click('section[aria-labelledby="resend-cooldown-heading"] button');
}
await settings();
await page.waitForFunction(()=>document.body.innerText.includes('resend-429:cooldown-health 현재 상태: 없음'));
await submit('browser fixture grant');
await page.selectOption('select[name="grantAuditScope"]','resend-429');
await page.click('loc=role:button[name="위임 이력 조회"]');
await page.waitForFunction(()=>[...document.querySelectorAll('table')].some(t=>t.innerText.includes('browser fixture grant') && t.innerText.includes('cooldown-health')));
await alerts();
await page.waitForFunction(()=>document.querySelector('#resend-cooldown-heading')?.closest('section')?.innerText.includes('공유 대기: 진행 중'));
await settings();
await page.waitForFunction(()=>document.body.innerText.includes('resend-429:cooldown-health 현재 상태: 허용'));
await submit('browser fixture revoke');
await alerts();
await page.waitForFunction(()=>document.querySelector('#resend-cooldown-heading')?.closest('section')?.innerText.includes('조회하지 못했습니다. 권한, 정책 및 연결 상태를 확인하세요.'));
await settings();
for (const permission of ['policy-read','policy-write','policy-audit']) {
  await page.selectOption('select[name="grantKind"]','resend-429:'+permission);
  await page.click('loc=role:button[name="선택 권한 조회"]');
  await page.waitForFunction(p=>document.body.innerText.includes('resend-429:'+p+' 현재 상태: 없음'),permission);
  await submit('browser '+permission);
  await page.click('loc=role:button[name="요청 정보 지우기"]');
}
await page.evaluate(selector=>document.querySelector(selector)?.scrollIntoView({block:'center'}),'input[name="permissionKey"]');
await page.fill('input[name="permissionKey"]',target);
await page.click('app-resend-cooldown-policy section:first-child button');
await page.waitForFunction(()=>document.body.innerText.includes('기본 대기: 30초'));
await page.evaluate(selector=>document.querySelector(selector)?.scrollIntoView({block:'center'}),'input[name="resendCooldownFallback"]');
await page.fill('input[name="resendCooldownFallback"]','45');
await page.evaluate(selector=>document.querySelector(selector)?.scrollIntoView({block:'center'}),'textarea[name="resendCooldownReason"]');
await page.fill('textarea[name="resendCooldownReason"]','browser policy change');
await page.click('input[name="resendCooldownConfirm"]');
await page.click('loc=role:button[name="정책 변경 요청 준비"]');
await page.waitForSelector('input[name="resendCooldownSubmitConfirm"]');
await page.click('input[name="resendCooldownSubmitConfirm"]');
await page.click('loc=role:button[name="정책 변경 제출"]');
await page.waitForFunction(()=>document.body.innerText.includes('요청이 기록됐습니다. 현재 정책은 다시 조회하세요.'));
await page.click('app-resend-cooldown-policy section:last-child button >> nth=0');
await page.waitForFunction(()=>[...document.querySelectorAll('table')].some(t=>t.innerText.includes('browser policy change') && t.innerText.includes('45초')));
await page.click('loc=role:button[name="정책 요청 정보 지우기"]');
await page.click('app-resend-cooldown-policy section:first-child button');
await page.waitForFunction(()=>document.body.innerText.includes('기본 대기: 45초'));
// Remove only the write grant; the target must still be able to read and audit.
await page.click('nav.sidenav button >> nth=5');
await settings();
await page.selectOption('select[name="grantKind"]','resend-429:policy-write');
await page.click('loc=role:button[name="선택 권한 조회"]');
await page.waitForFunction(()=>document.body.innerText.includes('resend-429:policy-write 현재 상태: 허용'));
await submit('browser revoke policy-write');
await page.fill('input[name="permissionKey"]',target);
await page.click('app-resend-cooldown-policy section:first-child button');
await page.waitForFunction(()=>document.body.innerText.includes('기본 대기: 45초'));
await page.evaluate(()=>document.querySelector('input[name="resendCooldownFallback"]')?.scrollIntoView({block:'center'}));
await page.fill('input[name="resendCooldownFallback"]','60');
await page.evaluate(()=>document.querySelector('textarea[name="resendCooldownReason"]')?.scrollIntoView({block:'center'}));
await page.fill('textarea[name="resendCooldownReason"]','revoked write must fail');
await page.click('input[name="resendCooldownConfirm"]');
await page.click('loc=role:button[name="정책 변경 요청 준비"]');
await page.waitForSelector('input[name="resendCooldownSubmitConfirm"]');
await page.click('input[name="resendCooldownSubmitConfirm"]');
await page.click('loc=role:button[name="정책 변경 제출"]');
await page.waitForFunction(()=>document.body.innerText.includes('결과가 불명확합니다. 요청 ID로 확인하세요.'));
// Navigate away to clear the unresolved request, then revoke read separately.
await page.click('nav.sidenav button >> nth=5');
await settings();
await page.selectOption('select[name="grantKind"]','resend-429:policy-read');
await page.click('loc=role:button[name="선택 권한 조회"]');
await page.waitForFunction(()=>document.body.innerText.includes('resend-429:policy-read 현재 상태: 허용'));
await submit('browser revoke policy-read');
await page.fill('input[name="permissionKey"]',target);
await page.click('app-resend-cooldown-policy section:first-child button');
await page.waitForFunction(()=>document.body.innerText.includes('정책을 조회하지 못했습니다. 인증과 정책 조회 권한을 확인하세요.'));
await page.click('app-resend-cooldown-policy section:last-child button >> nth=0');
await page.waitForFunction(()=>[...document.querySelectorAll('table')].some(t=>t.innerText.includes('browser policy change') && t.innerText.includes('45초')));
console.log('Real API browser test: delegated policy grant, change, audit and independent revocation passed');
'''
with open(root+'/browser.mjs','w') as f:f.write('const API='+json.dumps(api)+'; const UI='+json.dumps(ui)+'; const ROOT='+json.dumps(root)+'; const SPACE='+json.dumps(int(space) if space else None)+';\n'+script)
PY
ego-browser nodejs < "$TEST_DIR/browser.mjs"
SPACE_ID=$(cat "$TEST_DIR/space-id")
printf 'const task=await taskSpace(%s); await task.finish({keep:[]});\n' "$SPACE_ID" | ego-browser nodejs
[[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -At -v ON_ERROR_STOP=1 -c "SELECT count(*)=2 AND bool_or(current) AND bool_or(NOT current) FROM public.novu_rust_operator_jwt_resend_429_grant_audit WHERE environment_id='fixture-env' AND permission='cooldown-health'") == t ]]

[[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -At -v ON_ERROR_STOP=1 -c "SELECT fallback_seconds=45 FROM public.novu_rust_resend_cooldown_policies WHERE environment_id='fixture-env'") == t ]]
