#!/usr/bin/env bash
# Local PostgreSQL, restricted API login, synthetic JWTs and real browser.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
API="$ROOT/apps/api/rust-api"; WORKER="$ROOT/apps/worker/rust-queue"; UI="$ROOT/apps/dashboard/swallow"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-cancel-retention-browser.XXXXXX")
API_PID= UI_PID=
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
cleanup() {
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
CREATE ROLE ui_retention_api LOGIN;
GRANT USAGE ON SCHEMA public TO ui_retention_api;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_cancel_cred_retention_submit(text,text,text,text,bigint,uuid,integer,integer,text),public.novu_rust_operator_jwt_cancel_cred_retention_get(text,text,text,text,bigint,uuid) TO ui_retention_api;
INSERT INTO public.novu_rust_environments VALUES('fixture-env','fixture-org');
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','admin','console','fixture-env'),('https://issuer.test','target','console','fixture-env');
INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='ui_retention_api';
INSERT INTO public.novu_rust_operator_jwt_cancel_cred_retention_grants VALUES('https://issuer.test','admin','console','fixture-env');
INSERT INTO public.novu_rust_resend_cancel_cred_retention_operators SELECT 'fixture-env',rolname,oid FROM pg_roles WHERE rolname='ui_retention_api';
INSERT INTO public.novu_rust_resend_cancel_credential_health VALUES('fixture-env','11111111-1111-4111-8111-111111111111',session_user,-1,clock_timestamp()-interval '4 days','unhealthy',clock_timestamp()-interval '3 days'),('fixture-env','22222222-2222-4222-8222-222222222222',session_user,-1,clock_timestamp()-interval '4 days','unhealthy',clock_timestamp());
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
env -i PATH="$PATH" DATABASE_URL="postgres://ui_retention_api@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true \
LISTEN_ADDR="127.0.0.1:$API_PORT" SWALLOW_OPERATOR_JWT_CONFIG_FILE="$TEST_DIR/keys.json" \
SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true SWALLOW_CORS_ALLOWED_ORIGINS="http://127.0.0.1:$UI_PORT" \
"$API/target/debug/novu-rust-api" >"$TEST_DIR/api.log" 2>&1 & API_PID=$!
python3 -m http.server "$UI_PORT" --bind 127.0.0.1 --directory "$UI/dist/swallow/browser" >"$TEST_DIR/ui.log" 2>&1 & UI_PID=$!
for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null 2>&1 && curl -fsS "http://127.0.0.1:$UI_PORT/" >/dev/null 2>&1; then break; fi
  kill -0 "$API_PID" && kill -0 "$UI_PID"; sleep 0.1
done
curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null
python3 - "$API_PORT" "$UI_PORT" "$DB_PORT" "$TEST_DIR" "$TEST_PG_BIN" "${TEST_EGO_SPACE_ID:-}" <<'PY'
import json,sys
api,ui,db,root,pg,space=sys.argv[1:]
script='''const fs=await import('node:fs/promises');const cp=await import('node:child_process');
const task=SPACE===null?await taskSpace('Swallow retention real API test'):await taskSpace(SPACE);
await fs.writeFile(ROOT+'/space-id',String(task.spaceId));const page=task.page('p1');
const admin=await fs.readFile(ROOT+'/admin.jwt','utf8'),target=await fs.readFile(ROOT+'/target.jwt','utf8');
function count(){return cp.execFileSync(PG+'/psql',['postgres://postgres@127.0.0.1:'+DB+'/postgres','-Atc',"SELECT count(*) FROM public.novu_rust_resend_cancel_credential_health WHERE environment_id='fixture-env'"],{encoding:'utf8'}).trim();}
await page.goto('http://127.0.0.1:'+UI+'/');
async function fill(selector,value){await page.evaluate(s=>document.querySelector(s)?.scrollIntoView({block:'center'}),selector);await page.fill(selector,value);}
await fill('input[name="apiOrigin"]','http://127.0.0.1:'+API);await page.click('nav.sidenav button >> nth=5');await page.selectOption('select[name="alertAuthMode"]','jwt');
await fill('input[name="alertEnvironment"]','fixture-env');await fill('input[name="alertKey"]',admin);
await fill('input[name="cancelRetentionLimit"]','1');await fill('input[name="cancelRetentionReason"]','browser ticket');await page.click('input[name="cancelRetentionConfirm"]');
await page.click('app-resend-cancel-credential-retention button:has-text("정리 요청 준비")');await page.waitForSelector('app-resend-cancel-credential-retention button:has-text("정리 실행")');
const req=await page.evaluate(()=>{const t=document.querySelector('app-resend-cancel-credential-retention')?.innerText||'';const m=t.match(/요청 ([0-9a-f-]{36})/);if(!m)throw Error('request ID missing');return m[1]});
if(count()!=='2')throw Error('preparation changed database');
await page.evaluate(()=>document.querySelector('app-resend-cancel-credential-retention button.btn-danger')?.scrollIntoView({block:'center'}));
await page.click('app-resend-cancel-credential-retention input[type="checkbox"]');await page.click('app-resend-cancel-credential-retention button:has-text("정리 실행")');
await page.waitForFunction(()=>document.querySelector('app-resend-cancel-credential-retention')?.innerText.includes('정리 완료: 1건'));if(count()!=='1')throw Error('incorrect deletion');
await fill('input[name="cancelRetentionRequest"]',req);await page.click('app-resend-cancel-credential-retention button:has-text("요청 조회")');
await page.waitForFunction(()=>document.querySelector('app-resend-cancel-credential-retention')?.innerText.includes('정리 1건'));
await fill('input[name="alertKey"]',target);await fill('input[name="cancelRetentionRequest"]',req);await page.click('app-resend-cancel-credential-retention button:has-text("요청 조회")');
await page.waitForFunction(()=>document.querySelector('app-resend-cancel-credential-retention')?.innerText.includes('조회하지 못했습니다.'));if(count()!=='1')throw Error('unauthorized request changed data');
console.log('retention browser prepare, submit, lookup, and authorization passed');
'''
with open(root+'/browser.mjs','w') as f:f.write('const API='+json.dumps(api)+';const UI='+json.dumps(ui)+';const DB='+json.dumps(db)+';const ROOT='+json.dumps(root)+';const PG='+json.dumps(pg)+';const SPACE='+json.dumps(int(space) if space else None)+';\n'+script)
PY
ego-browser nodejs < "$TEST_DIR/browser.mjs"
SPACE_ID=$(cat "$TEST_DIR/space-id")
printf 'const task=await taskSpace(%s); await task.finish({keep:[]});\n' "$SPACE_ID" | ego-browser nodejs
