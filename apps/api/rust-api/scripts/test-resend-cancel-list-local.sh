#!/usr/bin/env bash
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd); API="$ROOT/apps/api/rust-api"; WORKER="$ROOT/apps/worker/rust-queue"
DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-cancel-api.XXXXXX"); API_PID=
cleanup() { if [[ -n "$API_PID" ]]; then kill "$API_PID" 2>/dev/null || true; wait "$API_PID" 2>/dev/null || true; fi; if [[ -f "$DIR/pg/postmaster.pid" ]] && ! "$TEST_PG_BIN/pg_ctl" -D "$DIR/pg" -m immediate -w stop >/dev/null 2>&1; then echo "Database retained at $DIR" >&2; return; fi; rm -rf "$DIR"; }
trap cleanup EXIT
port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
DB_PORT=$(port); API_PORT=$(port)
"$TEST_PG_BIN/initdb" -D "$DIR/pg" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$DIR/pg" -l "$DIR/pg.log" -o "-h 127.0.0.1 -p $DB_PORT" -w start >/dev/null
export DATABASE_URL="postgres://postgres@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
"$TEST_PG_BIN/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE cancel_api_fixture LOGIN; CREATE ROLE cancel_api_worker LOGIN;
GRANT USAGE ON SCHEMA public TO cancel_api_fixture;
GRANT SELECT ON public.novu_rust_environments TO cancel_api_fixture;
GRANT SELECT,INSERT,UPDATE ON public.novu_rust_resend_delivery_events TO cancel_api_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_resend_cancel_evidence_health(text),public.novu_rust_reserve_alert_v3(text,text,text,integer,bigint,bigint,bigint,bigint),public.novu_rust_observe_alert_conditions_v3(text,text,text,bigint,bigint,bigint,bigint) TO cancel_api_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_list(text,text,text,text,bigint,integer,uuid) TO cancel_api_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_detail(text,text,text,text,bigint,uuid) TO cancel_api_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_resolve(text,text,text,text,bigint,uuid,uuid,text,text) TO cancel_api_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_submit(text,text,text,text,bigint,uuid,uuid,text),public.novu_rust_operator_jwt_resend_cancel_command_get(text,text,text,text,bigint,uuid) TO cancel_api_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_evidence_monitor(text,text,text,text,bigint,boolean,uuid) TO cancel_api_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_command_monitor(text,text,text,text,bigint,integer,boolean,uuid) TO cancel_api_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_command_review(text,text,text,text,bigint,uuid,uuid,text,text),public.novu_rust_operator_jwt_resend_cancel_command_review_get(text,text,text,text,bigint,uuid) TO cancel_api_fixture;
GRANT USAGE ON SCHEMA public TO cancel_api_worker;
GRANT EXECUTE ON FUNCTION public.novu_rust_resend_cancel_claim(text),public.novu_rust_resend_cancel_finish(text,uuid,text,integer) TO cancel_api_worker;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_resend_cancel_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_resend_cancel_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_resend_cancel_grant_audit_read(text,text,text,text,bigint,uuid) TO cancel_api_fixture;
INSERT INTO public.novu_rust_environments VALUES ('cancel-api-env','org'),('cancel-api-other','org');
INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='cancel_api_fixture';
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','fixture','console','cancel-api-env'),('https://issuer.test','fixture','console','cancel-api-other'),('https://issuer.test','target','console','cancel-api-env');
INSERT INTO public.novu_rust_operator_jwt_resend_cancel_grant_admins VALUES ('https://issuer.test','fixture','console','cancel-api-env',true,true);
INSERT INTO public.novu_rust_operator_jwt_resend_cancel_grant_audit_viewers VALUES ('https://issuer.test','fixture','console','cancel-api-env');
INSERT INTO public.novu_rust_operator_jwt_resend_cancel_list_grants VALUES ('https://issuer.test','fixture','console','cancel-api-env');
INSERT INTO public.novu_rust_operator_jwt_resend_cancel_detail_grants VALUES ('https://issuer.test','fixture','console','cancel-api-env');
INSERT INTO public.novu_rust_operator_jwt_resend_cancel_resolution_grants VALUES ('https://issuer.test','fixture','console','cancel-api-env','confirmed');
INSERT INTO public.novu_rust_operator_jwt_resend_cancel_submit_grants VALUES ('https://issuer.test','fixture','console','cancel-api-env');
INSERT INTO public.novu_rust_operator_jwt_resend_cancel_evidence_health_grants VALUES ('https://issuer.test','fixture','console','cancel-api-env');
INSERT INTO public.novu_rust_operator_jwt_resend_cancel_evidence_list_grants VALUES ('https://issuer.test','fixture','console','cancel-api-env');
INSERT INTO public.novu_rust_operator_jwt_resend_cancel_command_health_grants VALUES ('https://issuer.test','fixture','console','cancel-api-env');
INSERT INTO public.novu_rust_operator_jwt_resend_cancel_command_failed_list_grants VALUES ('https://issuer.test','fixture','console','cancel-api-env');
INSERT INTO public.novu_rust_operator_jwt_resend_cancel_command_review_grants VALUES ('https://issuer.test','fixture','console','cancel-api-env');
INSERT INTO public.novu_rust_resend_cancel_command_reviewers SELECT 'cancel-api-env',rolname,oid FROM pg_roles WHERE rolname='cancel_api_fixture';
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'cancel-api-env',rolname,'alert_view',oid FROM pg_roles WHERE rolname='cancel_api_fixture';
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'cancel-api-env',rolname,'alert_send',oid FROM pg_roles WHERE rolname='cancel_api_fixture';
INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES('cancel-api-env','slack','blocked-evidence','rejected');
INSERT INTO public.novu_rust_alert_v2_targets(environment_id,channel,target_id) VALUES('cancel-api-env','slack','blocked-evidence');
INSERT INTO public.novu_rust_resend_cancel_operators(environment_id,database_principal,principal_oid,can_read,can_resolve) SELECT 'cancel-api-env',rolname,oid,true,true FROM pg_roles WHERE rolname='cancel_api_fixture';
INSERT INTO public.novu_rust_resend_cancel_operators(environment_id,database_principal,principal_oid,can_cancel) SELECT 'cancel-api-env',rolname,oid,true FROM pg_roles WHERE rolname='cancel_api_worker';
INSERT INTO public.novu_rust_resend_cancel_submitters SELECT 'cancel-api-env',rolname,oid FROM pg_roles WHERE rolname='cancel_api_fixture';
INSERT INTO public.novu_rust_resend_cancel_command_workers SELECT 'cancel-api-env',rolname,oid FROM pg_roles WHERE rolname='cancel_api_worker';
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,resend_scheduled_at) VALUES('44444444-4444-4444-8444-444444444444','cancel-api-env','experimental_resend_email','{}','completed',clock_timestamp()+interval '1 hour'),('55555555-5555-4555-8555-555555555555','cancel-api-env','experimental_resend_email','{}','completed',clock_timestamp()+interval '1 hour');
INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES('44444444-4444-4444-8444-444444444444','cancel-api-env','accepted','fixture-cancel'),('55555555-5555-4555-8555-555555555555','cancel-api-env','accepted','fixture-ui');
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES('99999999-9999-4999-8999-999999999999','cancel-api-env','experimental_resend_email','{}','completed');
INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES('99999999-9999-4999-8999-999999999999','cancel-api-env','accepted','evidence-private-id');
INSERT INTO public.novu_rust_resend_cancel_requests(environment_id,request_id,job_id,provider_id,database_principal,principal_oid,reason,state) SELECT 'cancel-api-env','99999999-9999-4999-8999-999999999998','99999999-9999-4999-8999-999999999999','evidence-private-id',rolname,oid,'private reason','confirmed' FROM pg_roles WHERE rolname='cancel_api_fixture';
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES('66666666-6666-4666-8666-666666666666','cancel-api-env','experimental_resend_email','{}','completed'),('77777777-7777-4777-8777-777777777777','cancel-api-env','experimental_resend_email','{}','completed');
INSERT INTO public.novu_rust_resend_cancel_commands(environment_id,request_id,job_id,reason,submitter,submitter_oid,state) SELECT 'cancel-api-env',x.request_id,x.job_id,'failure fixture',r.rolname,r.oid,'failed' FROM pg_roles r CROSS JOIN (VALUES ('ffffffff-ffff-4fff-8fff-ffffffffffff'::uuid,'66666666-6666-4666-8666-666666666666'::uuid),('eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee'::uuid,'77777777-7777-4777-8777-777777777777'::uuid)) x(request_id,job_id) WHERE r.rolname='cancel_api_fixture';
WITH jobs AS (INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) SELECT gen_random_uuid(),'cancel-api-env','experimental_resend_email','{}','completed' FROM generate_series(1,56) RETURNING id)
INSERT INTO public.novu_rust_resend_cancel_requests(environment_id,request_id,job_id,provider_id,database_principal,principal_oid,reason,state,created_at)
SELECT 'cancel-api-env',gen_random_uuid(),jobs.id,'private-provider',rolname,oid,'private-reason','unknown',clock_timestamp()-interval '5 minutes' FROM jobs CROSS JOIN pg_roles WHERE rolname='cancel_api_fixture';
SQL
python3 - "$API/tests/fixtures/operator_jwt/test-only-public.pem" "$API/tests/fixtures/operator_jwt/test-only-private.pem" "$DIR" <<'PY'
import base64,json,subprocess,sys,time
public,private,root=sys.argv[1:]
with open(public) as f:key=f.read()
with open(root+'/keys.json','w') as f:json.dump({'issuer':'https://issuer.test','audience':'swallow','keys':[{'kid':'fixture','pem':key}]},f)
def b64(v):return base64.urlsafe_b64encode(v).decode().rstrip('=')
h=b64(json.dumps({'alg':'RS256','typ':'at+jwt','kid':'fixture'},separators=(',',':')).encode())
p=b64(json.dumps({'iss':'https://issuer.test','aud':'swallow','sub':'fixture','client_id':'console','iat':int(time.time())-1,'exp':int(time.time())+900,'jti':'cancel-api-fixture'},separators=(',',':')).encode())
message=(h+'.'+p).encode();sig=subprocess.run(['openssl','dgst','-sha256','-sign',private],input=message,stdout=subprocess.PIPE,check=True).stdout
with open(root+'/jwt','w') as f:f.write(message.decode()+'.'+b64(sig))
p=b64(json.dumps({'iss':'https://issuer.test','aud':'swallow','sub':'target','client_id':'console','iat':int(time.time())-1,'exp':int(time.time())+900,'jti':'cancel-api-target'},separators=(',',':')).encode());message=(h+'.'+p).encode();sig=subprocess.run(['openssl','dgst','-sha256','-sign',private],input=message,stdout=subprocess.PIPE,check=True).stdout
with open(root+'/target-jwt','w') as f:f.write(message.decode()+'.'+b64(sig))
PY
cargo build --quiet --locked --manifest-path "$API/Cargo.toml" --bin novu-rust-api
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin send-delivery-alert
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --features test-mocks --bin resend-cancel-worker
cp "$WORKER/target/debug/resend-cancel-worker" "$DIR/cancel-worker-fixture"
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin resend-cancel-worker
env -i PATH="$PATH" DATABASE_URL="postgres://cancel_api_fixture@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true LISTEN_ADDR="127.0.0.1:$API_PORT" SWALLOW_OPERATOR_JWT_CONFIG_FILE="$DIR/keys.json" SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID=cancel-api-env NOVU_RESEND_WEBHOOK_SECRET=whsec_dGVzdC1zZWNyZXQtaXMtMzItYnl0ZXMtbG9uZyEh "$API/target/debug/novu-rust-api" >"$DIR/api.log" 2>&1 & API_PID=$!
for _ in $(seq 1 100); do curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null 2>&1 && break; kill -0 "$API_PID"; sleep 0.1; done
curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null
TEST_API="http://127.0.0.1:$API_PORT" TEST_JWT="$(cat "$DIR/jwt")" TEST_DB_PORT="$DB_PORT" TEST_ALERT="$WORKER/target/debug/send-delivery-alert" python3 "$API/tests/resend_cancel_signed_evidence_http.py"
(cd "$ROOT/apps/dashboard/swallow" && SWALLOW_UI_TEST_API="http://127.0.0.1:$API_PORT" SWALLOW_UI_TEST_JWT="$(cat "$DIR/jwt")" npm test -- --watch=false --include src/app/resend-cancel-list-integration.spec.ts --include src/app/resend-cancel-grants-integration.spec.ts --include src/app/resend-cancel-command-health-integration.spec.ts --include src/app/resend-cancel-evidence-integration.spec.ts)
TEST_API="http://127.0.0.1:$API_PORT" TEST_JWT="$(cat "$DIR/jwt")" TEST_TARGET_JWT="$(cat "$DIR/target-jwt")" TEST_PSQL="$TEST_PG_BIN/psql" TEST_CANCEL_WORKER="$DIR/cancel-worker-fixture" TEST_DB_PORT="$DB_PORT" python3 "$API/tests/resend_cancel_list_http.py"
echo 'Restricted JWT Resend cancellation list HTTP test passed.'
