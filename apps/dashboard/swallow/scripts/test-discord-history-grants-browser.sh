#!/usr/bin/env bash
# Docker PostgreSQL, restricted API login, synthetic JWTs and real browser.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
API="$ROOT/apps/api/rust-api"; WORKER="$ROOT/apps/worker/rust-queue"; UI="$ROOT/apps/dashboard/swallow"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-discord-history-grants-browser.XXXXXX")
API_PID= UI_PID=
CONTAINER="swallow-discord-grants-$$"
cleanup() {
  if [[ -n "$API_PID" ]]; then kill "$API_PID" 2>/dev/null || true; wait "$API_PID" 2>/dev/null || true; fi
  if [[ -n "$UI_PID" ]]; then kill "$UI_PID" 2>/dev/null || true; wait "$UI_PID" 2>/dev/null || true; fi
  if [[ "${TEST_POSTGRES_MODE:-docker}" == local ]]; then
    if [[ -f "$TEST_DIR/pg/PG_VERSION" ]]; then pg_ctl -D "$TEST_DIR/pg" -m fast -w stop >/dev/null 2>&1 || { echo "Could not stop test database; retained $TEST_DIR" >&2; return; }; fi
  else docker rm -f "$CONTAINER" >/dev/null 2>&1 || true; fi
  rm -rf "$TEST_DIR"
}
trap cleanup EXIT
port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
API_PORT=$(port); UI_PORT=$(port)
while [[ "$API_PORT" == "$UI_PORT" ]]; do UI_PORT=$(port); done
if [[ "${TEST_POSTGRES_MODE:-docker}" == local ]]; then
  DB_PORT=$(port)
  initdb -D "$TEST_DIR/pg" -U postgres --auth=trust --no-locale >"$TEST_DIR/init.log" 2>&1
  pg_ctl -D "$TEST_DIR/pg" -l "$TEST_DIR/pg.log" -o "-h 127.0.0.1 -p $DB_PORT" -w start >/dev/null
  db_sql() { psql "postgres://postgres@127.0.0.1:$DB_PORT/postgres" "$@"; }
else
  docker run -d --rm --name "$CONTAINER" -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
  for _ in $(seq 1 100); do docker exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 0.1; done
  DB_PORT=$(docker port "$CONTAINER" 5432/tcp | awk -F: '{print $NF}')
  db_sql() { docker exec -i "$CONTAINER" psql -U postgres -d postgres "$@"; }
fi
export DATABASE_URL="postgres://postgres@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
db_sql -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE ui_fixture_api LOGIN;
GRANT USAGE ON SCHEMA public TO ui_fixture_api;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_discord_resolution_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_discord_resolution_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_discord_resolution_grant_audit_read(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_discord_attention(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_discord_review(text,text,text,text,bigint,uuid,uuid,uuid,bigint,text),public.novu_rust_operator_jwt_discord_review_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_discord_resolve(text,text,text,text,bigint,uuid,uuid,uuid,bigint,text,text),public.novu_rust_operator_jwt_discord_resolution_request(text,text,text,text,bigint,uuid,text),public.novu_rust_operator_jwt_discord_resolution_history(text,text,text,text,bigint,uuid,oid,uuid) TO ui_fixture_api;
INSERT INTO public.novu_rust_environments VALUES('fixture-env','fixture-org');
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','admin','console','fixture-env'),('https://issuer.test','target','console','fixture-env');
INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='ui_fixture_api';
INSERT INTO public.novu_rust_operator_jwt_discord_resolution_grant_admins VALUES('https://issuer.test','admin','console','fixture-env',true,true);
INSERT INTO public.novu_rust_operator_jwt_discord_resolution_grant_audit_viewers VALUES('https://issuer.test','admin','console','fixture-env');
INSERT INTO public.novu_rust_operator_jwt_discord_review_grants VALUES('https://issuer.test','target','console','fixture-env',true,false);
INSERT INTO public.novu_rust_discord_resolution_audit_viewers SELECT 'fixture-env',rolname,oid FROM pg_roles WHERE rolname='ui_fixture_api';
INSERT INTO public.novu_rust_discord_resolvers SELECT 'fixture-env',rolname,oid,true,true FROM pg_roles WHERE rolname='ui_fixture_api';
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'fixture-env',rolname,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','queue_ack']) p WHERE rolname='ui_fixture_api';
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES('11111111-1111-4111-8111-000000000001','fixture-env','experimental_discord_webhook','{"secret":"fixture"}','failed');
INSERT INTO public.novu_rust_discord_attempts(job_id,environment_id,state) VALUES('11111111-1111-4111-8111-000000000001','fixture-env','unknown');
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
const task=SPACE===null ? await taskSpace('Swallow Discord grant real API test') : await taskSpace(SPACE);
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
 await page.selectOption('select[name="grantKind"]','discord-resolution:'+permission);
 await page.click('loc=role:button[name="선택 권한 조회"]');
 await page.waitForFunction(v=>document.body.innerText.includes('discord-resolution:'+v.permission+' 현재 상태: '+v.previous),{permission,previous});
 await page.click('input[name="grantDesired"]');await fill('textarea[name="grantReason"]','Discord grants browser '+permission);
 await page.click('input[name="grantConfirmed"]');await page.click('loc=role:button[name="변경 요청 준비"]');
 await page.waitForSelector('input[name="grantSubmitConfirmed"]');await page.click('input[name="grantSubmitConfirmed"]');await page.click('loc=role:button[name="변경 요청 제출"]');
 await page.waitForFunction(()=>document.body.innerText.includes('요청이 기록됐습니다. 권한이 변경됐습니다.'));
 await page.click('loc=role:button[name="요청 정보 지우기"]');
}
async function alerts() {
 await page.click('nav.sidenav button >> nth=5');await page.selectOption('select[name="alertAuthMode"]','jwt');
 await fill('input[name="alertEnvironment"]','fixture-env');await fill('input[name="alertKey"]',target);
}
await settings();await change('accepted','없음');await change('retry','없음');await change('history','없음');
await page.selectOption('select[name="grantAuditScope"]','discord-resolution');await page.click('section:has(select[name="grantAuditScope"]) button:has-text("위임 이력 조회")');
await page.waitForFunction(()=>[...document.querySelectorAll('table')].some(t=>t.innerText.includes('Discord grants browser retry')));
await alerts();await page.click('app-discord-reviews button:has-text("미확인 작업 조회")');
await page.waitForFunction(()=>document.querySelector('app-discord-reviews table')?.innerText.includes('11111111-1111-4111-8111-000000000001'));
await fill('input[name="discordReviewReason"]','browser resolution evidence');await page.click('app-discord-reviews button:has-text("접수 판정")');
await page.click('input[name="discordReviewConfirm"]');await page.click('app-discord-reviews button:has-text("판정 요청")');
await page.waitForFunction(()=>document.querySelector('app-discord-reviews')?.innerText.includes('판정이 기록되었습니다.'));
await fill('input[name="discordHistoryJob"]','11111111-1111-4111-8111-000000000001');await page.click('app-discord-reviews button:has-text("판정 이력 조회")');await page.waitForFunction(()=>[...document.querySelectorAll('app-discord-reviews table')].some(t=>t.innerText.includes('https://issuer.test / target / console')));
await settings();await change('history','허용');await alerts();
await fill('input[name="discordHistoryJob"]','11111111-1111-4111-8111-000000000001');await page.click('app-discord-reviews button:has-text("판정 이력 조회")');await page.waitForFunction(()=>document.querySelector('app-discord-reviews')?.innerText.includes('판정 이력을 조회하지 못했습니다.'));
console.log('Discord history grants, real browser history and revocation passed');
'''
with open(root+'/browser.mjs','w') as f:f.write('const API='+json.dumps(api)+'; const UI='+json.dumps(ui)+'; const ROOT='+json.dumps(root)+'; const SPACE='+json.dumps(int(space) if space else None)+';\n'+script)
PY
ego-browser nodejs < "$TEST_DIR/browser.mjs"
SPACE_ID=$(cat "$TEST_DIR/space-id")
printf 'const task=await taskSpace(%s); await task.finish({keep:[]});\n' "$SPACE_ID" | ego-browser nodejs
[[ $(db_sql -At -v ON_ERROR_STOP=1 -c "SELECT count(*)=4 FROM public.novu_rust_operator_jwt_discord_resolution_grant_audit WHERE environment_id='fixture-env'") == t ]]
[[ $(db_sql -At -v ON_ERROR_STOP=1 -c "SELECT count(*)=1 FROM public.novu_rust_discord_resolutions WHERE environment_id='fixture-env'") == t ]]
[[ $(db_sql -At -v ON_ERROR_STOP=1 -c "SELECT can_accept AND can_retry FROM public.novu_rust_operator_jwt_discord_resolution_grants WHERE environment_id='fixture-env' AND subject='target'") == t ]]
