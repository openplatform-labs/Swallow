#!/usr/bin/env bash
# Docker PostgreSQL, restricted API login, synthetic JWTs and real browser.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
API="$ROOT/apps/api/rust-api"; WORKER="$ROOT/apps/worker/rust-queue"; UI="$ROOT/apps/dashboard/swallow"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-discord-resolution-browser.XXXXXX")
API_PID= UI_PID=
CONTAINER="swallow-discord-resolution-$$"
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
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_discord_attention(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_discord_review(text,text,text,text,bigint,uuid,uuid,uuid,bigint,text),public.novu_rust_operator_jwt_discord_review_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_discord_resolve(text,text,text,text,bigint,uuid,uuid,uuid,bigint,text,text),public.novu_rust_operator_jwt_discord_resolution_request(text,text,text,text,bigint,uuid,text) TO ui_fixture_api;
INSERT INTO public.novu_rust_environments VALUES('fixture-env','fixture-org');
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','admin','console','fixture-env');
INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='ui_fixture_api';
INSERT INTO public.novu_rust_operator_jwt_discord_review_grants VALUES('https://issuer.test','admin','console','fixture-env',true,true);
INSERT INTO public.novu_rust_operator_jwt_discord_resolution_grants VALUES('https://issuer.test','admin','console','fixture-env',true,true);
INSERT INTO public.novu_rust_discord_resolvers SELECT 'fixture-env',rolname,oid,true,true FROM pg_roles WHERE rolname='ui_fixture_api';
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'fixture-env',rolname,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','queue_ack']) p WHERE rolname='ui_fixture_api';
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES('11111111-1111-4111-8111-000000000001','fixture-env','experimental_discord_webhook','{"secret":"fixture"}','failed');
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES('11111111-1111-4111-8111-000000000002','fixture-env','experimental_discord_webhook','{}','failed');
INSERT INTO public.novu_rust_discord_attempts(job_id,environment_id,state) VALUES('11111111-1111-4111-8111-000000000001','fixture-env','unknown'),('11111111-1111-4111-8111-000000000002','fixture-env','rejected');
UPDATE public.novu_rust_discord_attempts SET http_status=429,retry_after_seconds=60 WHERE job_id='11111111-1111-4111-8111-000000000002';
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
const task=SPACE===null ? await taskSpace('Swallow Discord review real API test') : await taskSpace(SPACE);
await fs.writeFile(ROOT+'/space-id',String(task.spaceId));const page=task.page('p1'),api='http://127.0.0.1:'+API,admin=await fs.readFile(ROOT+'/admin.jwt','utf8');
await page.goto('http://127.0.0.1:'+UI+'/');
async function fill(selector,value) {await page.fill(selector,value);}
await fill('input[name="apiOrigin"]',api);await page.click('nav.sidenav button >> nth=5');await page.selectOption('select[name="alertAuthMode"]','jwt');
await fill('input[name="alertEnvironment"]','fixture-env');await fill('input[name="alertKey"]',admin);
await page.click('app-discord-reviews button:has-text("미확인 작업 조회")');
await page.waitForFunction(()=>document.querySelector('app-discord-reviews table')?.innerText.includes('11111111-1111-4111-8111-000000000001'));
await fill('input[name="discordReviewReason"]','relay accepted evidence');await page.click('app-discord-reviews tbody tr:nth-child(1) button:has-text("접수 판정")');
await page.click('input[name="discordReviewConfirm"]');await page.click('app-discord-reviews button:has-text("판정 요청")');
await page.waitForFunction(()=>document.querySelector('app-discord-reviews')?.innerText.includes('판정이 기록되었습니다.'));
await page.click('app-discord-reviews button:has-text("요청 결과 조회")');await page.waitForFunction(()=>document.querySelector('app-discord-reviews')?.innerText.includes('확인된 요청:'));
await page.click('app-discord-reviews button:has-text("새 검토 준비")');await page.click('app-discord-reviews button:has-text("미확인 작업 조회")');
await page.waitForFunction(()=>document.querySelector('app-discord-reviews table')?.innerText.includes('11111111-1111-4111-8111-000000000002'));
await page.waitForFunction(()=>document.querySelector('app-discord-reviews table')?.innerText.includes('60초'));
await fill('input[name="discordReviewReason"]','relay did not accept');await page.click('app-discord-reviews tbody tr:nth-child(1) button:has-text("즉시 재발송 준비")');
await page.click('input[name="discordReviewConfirm"]');await page.click('app-discord-reviews button:has-text("판정 요청")');
await page.waitForFunction(()=>document.querySelector('app-discord-reviews')?.innerText.includes('판정이 기록되었습니다.'));
console.log('Discord browser resolution and rate guidance passed');
'''
with open(root+'/browser.mjs','w') as f:f.write('const API='+json.dumps(api)+'; const UI='+json.dumps(ui)+'; const ROOT='+json.dumps(root)+'; const SPACE='+json.dumps(int(space) if space else None)+';\n'+script)
PY
ego-browser nodejs < "$TEST_DIR/browser.mjs"
SPACE_ID=$(cat "$TEST_DIR/space-id")
printf 'const task=await taskSpace(%s); await task.finish({keep:[]});\n' "$SPACE_ID" | ego-browser nodejs
[[ $(db_sql -At -v ON_ERROR_STOP=1 -c "SELECT count(*)=2 FROM public.novu_rust_discord_resolutions WHERE environment_id='fixture-env'") == t ]]
[[ $(db_sql -At -v ON_ERROR_STOP=1 -c "SELECT status='completed' FROM public.novu_queue_jobs WHERE id='11111111-1111-4111-8111-000000000001'") == t ]]
[[ $(db_sql -At -v ON_ERROR_STOP=1 -c "SELECT status='pending' FROM public.novu_queue_jobs WHERE id='11111111-1111-4111-8111-000000000002'") == t ]]
