#!/usr/bin/env bash
# Disposable PostgreSQL, restricted API LOGIN, fixture JWT and Angular jsdom component.
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
API="$ROOT/apps/api/rust-api"; WORKER="$ROOT/apps/worker/rust-queue"; UI="$ROOT/apps/dashboard/swallow"
DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-smtp-ui.XXXXXX"); API_PID=
cleanup() {
  if [[ -n "$API_PID" ]]; then kill "$API_PID" 2>/dev/null || true; wait "$API_PID" 2>/dev/null || true; fi
  if [[ -f "$DIR/pg/postmaster.pid" ]] && ! "$TEST_PG_BIN/pg_ctl" -D "$DIR/pg" -m immediate -w stop >/dev/null 2>&1; then echo "Could not stop test database; retained $DIR" >&2; return; fi
  rm -rf "$DIR"
}
trap cleanup EXIT
port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
DB_PORT=$(port); API_PORT=$(port)
"$TEST_PG_BIN/initdb" -D "$DIR/pg" -U postgres -A trust --no-locale >"$DIR/init.log" 2>&1
"$TEST_PG_BIN/pg_ctl" -D "$DIR/pg" -l "$DIR/pg.log" -o "-h 127.0.0.1 -p $DB_PORT" -w start >/dev/null
export DATABASE_URL="postgres://postgres@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
"$TEST_PG_BIN/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE smtp_ui_fixture LOGIN;
GRANT USAGE ON SCHEMA public TO smtp_ui_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_smtp_transport_policy_read(text,text,text,text,bigint),public.novu_rust_operator_jwt_smtp_transport_policy_change(text,text,text,text,bigint,uuid,bigint,text,text),public.novu_rust_operator_jwt_smtp_transport_policy_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_smtp_transport_policy_audit_read(text,text,text,text,bigint,uuid) TO smtp_ui_fixture;
INSERT INTO public.novu_rust_environments VALUES('fixture-env','org');
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','admin','console','fixture-env');
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_smtp_transport_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_smtp_transport_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_smtp_transport_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_smtp_transport_grant_audit_read(text,text,text,text,bigint,uuid) TO smtp_ui_fixture;
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','target','console','fixture-env');
INSERT INTO public.novu_rust_operator_jwt_smtp_transport_grant_admins VALUES('https://issuer.test','admin','console','fixture-env',true,true);
INSERT INTO public.novu_rust_operator_jwt_smtp_transport_grant_audit_viewers VALUES('https://issuer.test','admin','console','fixture-env');
INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='smtp_ui_fixture';
INSERT INTO public.novu_rust_operator_jwt_smtp_transport_policy_grants VALUES('https://issuer.test','admin','console','fixture-env',true,true,true);
INSERT INTO public.novu_rust_smtp_transport_admins SELECT 'fixture-env',rolname,oid,true,true,false FROM pg_roles WHERE rolname='smtp_ui_fixture';
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_smtp_auto_policy_read(text,text,text,text,bigint),public.novu_rust_operator_jwt_smtp_auto_policy_change(text,text,text,text,bigint,uuid,bigint,boolean,integer,integer,text,text),public.novu_rust_operator_jwt_smtp_auto_policy_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_smtp_auto_policy_audit_read(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_smtp_auto_history(text,text,text,text,bigint,bigint) TO smtp_ui_fixture;
INSERT INTO public.novu_rust_operator_jwt_smtp_auto_policy_grants VALUES('https://issuer.test','admin','console','fixture-env',true,true,true,true);
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_smtp_auto_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_smtp_auto_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_smtp_auto_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_smtp_auto_grant_audit_read(text,text,text,text,bigint,uuid) TO smtp_ui_fixture;
INSERT INTO public.novu_rust_operator_jwt_smtp_auto_grant_admins VALUES('https://issuer.test','admin','console','fixture-env',true,true);
INSERT INTO public.novu_rust_operator_jwt_smtp_auto_grant_audit_viewers VALUES('https://issuer.test','admin','console','fixture-env');
INSERT INTO public.novu_rust_smtp_auto_admins SELECT 'fixture-env',rolname,oid,true,true,true FROM pg_roles WHERE rolname='smtp_ui_fixture';
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES('11111111-1111-4111-8111-111111111111','fixture-env','experimental_smtp_email','{"secret":"fixture-secret"}');
INSERT INTO public.novu_rust_smtp_auto_retries(environment_id,job_id,attempt_revision,failure_revision,database_principal,principal_oid,policy_max,policy_delay_seconds) SELECT 'fixture-env','11111111-1111-4111-8111-111111111111','22222222-2222-4222-8222-222222222222',1,rolname,oid,2,60 FROM pg_roles WHERE rolname='smtp_ui_fixture';
SQL
python3 - "$API/tests/fixtures/operator_jwt/test-only-public.pem" "$API/tests/fixtures/operator_jwt/test-only-private.pem" "$DIR" <<'PY'
import base64,json,subprocess,sys,time
public,private,root=sys.argv[1:]
with open(public) as f: key=f.read()
with open(root+'/keys.json','w') as f: json.dump({'issuer':'https://issuer.test','audience':'swallow','keys':[{'kid':'fixture','pem':key}]},f)
def b64(v): return base64.urlsafe_b64encode(v).decode().rstrip('=')
h=b64(json.dumps({'alg':'RS256','typ':'at+jwt','kid':'fixture'},separators=(',',':')).encode())
p=b64(json.dumps({'iss':'https://issuer.test','aud':'swallow','sub':'admin','client_id':'console','iat':int(time.time())-1,'exp':int(time.time())+900,'jti':'smtp-ui-fixture'},separators=(',',':')).encode())
message=(h+'.'+p).encode();sig=subprocess.run(['openssl','dgst','-sha256','-sign',private],input=message,stdout=subprocess.PIPE,check=True).stdout
with open(root+'/jwt','w') as f:f.write(message.decode()+'.'+b64(sig))
PY
cargo build --quiet --locked --manifest-path "$API/Cargo.toml" --bin novu-rust-api
env -i PATH="$PATH" DATABASE_URL="postgres://smtp_ui_fixture@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true LISTEN_ADDR="127.0.0.1:$API_PORT" SWALLOW_OPERATOR_JWT_CONFIG_FILE="$DIR/keys.json" SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true "$API/target/debug/novu-rust-api" >"$DIR/api.log" 2>&1 & API_PID=$!
for _ in $(seq 1 100); do curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null 2>&1 && break; kill -0 "$API_PID"; sleep 0.1; done
curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null
(cd "$UI" && SWALLOW_UI_TEST_API="http://127.0.0.1:$API_PORT" SWALLOW_UI_TEST_JWT="$(cat "$DIR/jwt")" npm test -- --watch=false --include src/app/smtp-transport-integration.spec.ts --include src/app/smtp-transport-grants-integration.spec.ts --include src/app/smtp-auto-integration.spec.ts --include src/app/smtp-auto-grants-integration.spec.ts)
[[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -qAtc "SELECT count(*) FROM public.novu_rust_operator_jwt_smtp_transport_policy_audit WHERE environment_id='fixture-env' AND actor_subject='admin'") == 1 ]]
[[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -qAtc "SELECT count(*) FROM public.novu_rust_operator_jwt_smtp_transport_grant_audit WHERE environment_id='fixture-env' AND target_subject='target'") == 2 ]]
[[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -qAtc "SELECT count(*) FROM public.novu_rust_operator_jwt_smtp_auto_policy_audit WHERE environment_id='fixture-env' AND actor_subject='admin'") == 1 ]]
[[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -qAtc "SELECT count(*) FROM public.novu_rust_operator_jwt_smtp_auto_grant_audit WHERE environment_id='fixture-env' AND target_subject='target' AND permission='history'") == 2 ]]
echo 'Angular components and restricted local API SMTP policy, retry and grants tests passed.'
