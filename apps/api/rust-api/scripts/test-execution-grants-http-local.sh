#!/usr/bin/env bash
# Disposable local PostgreSQL, real API and in-app worker; no external provider.
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
API="$ROOT/apps/api/rust-api"; WORKER="$ROOT/apps/worker/rust-queue"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-execution-http.XXXXXX")
API_PID=; WORKER_PID=
cleanup() {
  if [[ -n "$WORKER_PID" ]]; then kill "$WORKER_PID" 2>/dev/null || true; wait "$WORKER_PID" 2>/dev/null || true; fi
  if [[ -n "$API_PID" ]]; then kill "$API_PID" 2>/dev/null || true; wait "$API_PID" 2>/dev/null || true; fi
  "$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/pg" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$TEST_DIR"
}
trap cleanup EXIT
port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
DB_PORT=$(port); API_PORT=$(port)
while [[ "$API_PORT" == "$DB_PORT" ]]; do API_PORT=$(port); done
"$TEST_PG_BIN/initdb" -D "$TEST_DIR/pg" -U postgres -A trust --no-locale >"$TEST_DIR/init.log" 2>&1
"$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/pg" -l "$TEST_DIR/pg.log" -o "-h 127.0.0.1 -p $DB_PORT" -w start >/dev/null
export DATABASE_URL="postgres://postgres@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
"$TEST_PG_BIN/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE execution_http_api LOGIN;
GRANT USAGE ON SCHEMA public TO execution_http_api;
GRANT SELECT ON public.novu_rust_workflows,public.novu_rust_environments TO execution_http_api;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_execution_grant_read(text,text,text,text,bigint,text,text,text,text),public.novu_rust_operator_jwt_execution_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,jsonb,jsonb,text),public.novu_rust_operator_jwt_execution_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_execution_grant_history(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_trigger_authorize(text,text,text,text,bigint,text),public.novu_rust_operator_jwt_trigger_enqueue(text,text,text,text,bigint,text,text,uuid,text,jsonb,jsonb),public.novu_rust_operator_jwt_execution_audit_read(text,text,text,text,bigint,uuid) TO execution_http_api;
INSERT INTO public.novu_rust_environments VALUES('fixture-env','fixture-org');
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','admin','console','fixture-env'),('https://issuer.test','target','console','fixture-env');
INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='execution_http_api';
INSERT INTO public.novu_rust_operator_jwt_execution_grant_admins VALUES('https://issuer.test','admin','console','fixture-env',true,true);
INSERT INTO public.novu_rust_operator_jwt_execution_grant_history_viewers VALUES('https://issuer.test','admin','console','fixture-env');
INSERT INTO public.novu_rust_operator_jwt_execution_audit_viewers VALUES('https://issuer.test','admin','console','fixture-env');
INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,inbox_metadata) VALUES('fixture-env','welcome',true,true,'Hello {{payload.name}}','{"id":"generation-1"}');
SQL
[[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -At -c "SELECT NOT has_table_privilege('execution_http_api','public.novu_rust_operator_jwt_trigger_execute_grants','SELECT') AND NOT has_table_privilege('execution_http_api','public.novu_queue_jobs','SELECT')") == t ]]
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
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin in-app-worker
env -i PATH="$PATH" DATABASE_URL="postgres://execution_http_api@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true LISTEN_ADDR="127.0.0.1:$API_PORT" SWALLOW_OPERATOR_JWT_CONFIG_FILE="$TEST_DIR/keys.json" SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true "$API/target/debug/novu-rust-api" >"$TEST_DIR/api.log" 2>&1 & API_PID=$!
for _ in $(seq 1 100); do curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null 2>&1 && break; kill -0 "$API_PID"; sleep 0.1; done
curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null
python3 "$API/scripts/test-execution-grants-http.py" "$API_PORT" "$TEST_DIR" first
"$WORKER/target/debug/in-app-worker" >"$TEST_DIR/worker.log" 2>&1 & WORKER_PID=$!
for _ in $(seq 1 100); do [[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -At -c "SELECT count(*) FROM public.novu_queue_jobs WHERE payload->>'transactionId'='http-first' AND status='completed'") == 1 ]] && break; kill -0 "$WORKER_PID"; sleep 0.1; done
[[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -At -c "SELECT count(*) FROM public.novu_queue_jobs WHERE payload->>'transactionId'='http-first' AND status='completed'") == 1 ]]
kill "$WORKER_PID"; wait "$WORKER_PID"; WORKER_PID=
python3 "$API/scripts/test-execution-grants-http.py" "$API_PORT" "$TEST_DIR" second
"$TEST_PG_BIN/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 -c "UPDATE public.novu_rust_workflows SET inbox_metadata='{\"id\":\"generation-2\"}' WHERE environment_id='fixture-env' AND identifier='welcome'"
"$WORKER/target/debug/in-app-worker" >"$TEST_DIR/worker2.log" 2>&1 & WORKER_PID=$!
for _ in $(seq 1 100); do [[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -At -c "SELECT count(*) FROM public.novu_queue_jobs WHERE payload->>'transactionId'='http-second' AND status='failed' AND last_error='workflow generation changed'") == 1 ]] && break; kill -0 "$WORKER_PID"; sleep 0.1; done
[[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -At -c "SELECT count(*) FROM public.novu_queue_jobs WHERE payload->>'transactionId'='http-second' AND status='failed' AND last_error='workflow generation changed'") == 1 ]]
[[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -At -c "SELECT count(*) FROM public.novu_rust_notifications WHERE environment_id='fixture-env' AND transaction_id='http-first'") == 1 ]]
[[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -At -c "SELECT count(*) FROM public.novu_rust_notifications WHERE environment_id='fixture-env' AND transaction_id='http-second'") == 0 ]]
echo 'Execution grant HTTP and in-app worker fixture passed.'
