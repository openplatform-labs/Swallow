#!/usr/bin/env bash
# Disposable local PostgreSQL + restricted API login + test JWT for channel result UI verification.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(cd ../../.. && pwd)
api="$root/apps/api/rust-api"; worker="$root/apps/worker/rust-queue"
pgbin=${SWALLOW_TEST_POSTGRES_BIN:-/opt/homebrew/opt/postgresql@17/bin}
testdir=$(mktemp -d /private/tmp/swallow-channel-results.XXXXXX)
api_pid=''
cleanup() {
 if [[ -n "$api_pid" ]]; then kill "$api_pid" 2>/dev/null || true; wait "$api_pid" 2>/dev/null || true; fi
 "$pgbin/pg_ctl" -D "$testdir/data" -m immediate stop >/dev/null 2>&1 || true
 python3 -c 'import shutil,sys; shutil.rmtree(sys.argv[1])' "$testdir"
}
trap cleanup EXIT
"$pgbin/initdb" -D "$testdir/data" -U postgres -A trust --no-locale >/dev/null
pg_port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$pgbin/pg_ctl" -D "$testdir/data" -o "-h 127.0.0.1 -p $pg_port -k $testdir" -l "$testdir/db.log" start >/dev/null
"$pgbin/createdb" -h 127.0.0.1 -p "$pg_port" -U postgres ui_test
export DATABASE_URL="postgres://postgres@127.0.0.1:$pg_port/ui_test" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$worker/Cargo.toml" --bin migrate-worker -- apply api "$api/migrations" --through 17
cargo run --quiet --locked --manifest-path "$worker/Cargo.toml" --bin migrate-worker -- apply worker "$worker/migrations"
cargo run --quiet --locked --manifest-path "$worker/Cargo.toml" --bin migrate-worker -- apply api "$api/migrations"
"$pgbin/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE ui_channel_results_fixture LOGIN;
GRANT USAGE ON SCHEMA public TO ui_channel_results_fixture;
INSERT INTO public.novu_rust_environments VALUES ('ui-fixture-env','org');
INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_catalog.pg_roles WHERE rolname='ui_channel_results_fixture';
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_twilio_result(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_twilio_jobs(text,text,text,text,bigint,uuid) TO ui_channel_results_fixture;
INSERT INTO public.novu_rust_operator_jwt_twilio_result_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_rust_operator_jwt_twilio_jobs_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) SELECT gen_random_uuid(),'ui-fixture-env','experimental_twilio_sms','{}' FROM generate_series(1,21);
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_fcm_result(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_fcm_jobs(text,text,text,text,bigint,uuid) TO ui_channel_results_fixture;
INSERT INTO public.novu_rust_operator_jwt_fcm_result_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_rust_operator_jwt_fcm_jobs_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) SELECT gen_random_uuid(),'ui-fixture-env','experimental_fcm_push','{}' FROM generate_series(1,21);
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_slack_result(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_slack_jobs(text,text,text,text,bigint,uuid) TO ui_channel_results_fixture;
INSERT INTO public.novu_rust_operator_jwt_slack_result_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_rust_operator_jwt_slack_jobs_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) SELECT ('11111111-1111-4111-8111-'||lpad(n::text,12,'0'))::uuid,'ui-fixture-env','experimental_slack_chat','{}' FROM generate_series(1,21) n;
UPDATE public.novu_queue_jobs SET status='failed' WHERE id='11111111-1111-4111-8111-000000000001';
INSERT INTO public.novu_rust_slack_attempts(job_id,environment_id,state,attempt_id) VALUES ('11111111-1111-4111-8111-000000000001','ui-fixture-env','unknown','22222222-2222-4222-8222-222222222222');
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_sendgrid_result(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_sendgrid_jobs(text,text,text,text,bigint,uuid) TO ui_channel_results_fixture;
INSERT INTO public.novu_rust_operator_jwt_sendgrid_result_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_rust_operator_jwt_sendgrid_jobs_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) SELECT gen_random_uuid(),'ui-fixture-env','experimental_sendgrid_email','{}' FROM generate_series(1,21);
SQL
if [[ "${SWALLOW_UI_SLACK_429_FIXTURE:-false}" == true ]]; then
 "$pgbin/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
UPDATE public.novu_rust_slack_attempts SET state='rejected',http_status=429,retry_after_seconds=120,retry_not_before=clock_timestamp()+interval '120 seconds' WHERE job_id='11111111-1111-4111-8111-000000000001';
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_slack_retry_after(text,text,text,text,bigint,uuid,uuid,uuid,text,text),public.novu_rust_operator_jwt_slack_decision_request(text,text,text,text,bigint,uuid) TO ui_channel_results_fixture;
INSERT INTO public.novu_rust_operator_jwt_slack_decision_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env','retry') ON CONFLICT DO NOTHING;
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'ui-fixture-env',rolname,'slack_retry',oid FROM pg_roles WHERE rolname='ui_channel_results_fixture' ON CONFLICT DO NOTHING;
SQL
fi
if [[ "${SWALLOW_UI_RESEND_AUTO_HEALTH_FIXTURE:-false}" == true ]]; then
 "$pgbin/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_auto_health(text,text,text,text,bigint,integer),public.novu_rust_operator_jwt_resend_auto_health_jobs(text,text,text,text,bigint,integer,text,uuid),public.novu_rust_operator_jwt_resend_result(text,text,text,text,bigint,uuid) TO ui_channel_results_fixture;
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'ui-fixture-env',rolname,'alert_view',oid FROM pg_roles WHERE rolname='ui_channel_results_fixture';
INSERT INTO public.novu_rust_operator_jwt_resend_auto_health_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_rust_operator_jwt_resend_auto_health_job_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_rust_operator_jwt_resend_result_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error) VALUES ('44444444-4444-4444-8444-444444444444','ui-fixture-env','experimental_resend_email','{}','pending','Resend automatic retry requires evidence guards');
SQL
fi
if [[ "${SWALLOW_UI_RESEND_AUTO_POLICY_FIXTURE:-false}" == true ]]; then
 "$pgbin/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_auto_policy_read(text,text,text,text,bigint),public.novu_rust_operator_jwt_resend_auto_policy_change(text,text,text,text,bigint,uuid,bigint,boolean,integer,text,text),public.novu_rust_operator_jwt_resend_auto_policy_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_resend_auto_policy_audit_read(text,text,text,text,bigint,uuid) TO ui_channel_results_fixture;
INSERT INTO public.novu_rust_resend_auto_policy_admins(environment_id,database_principal,principal_oid,can_read,can_write) SELECT 'ui-fixture-env',rolname,oid,true,true FROM pg_roles WHERE rolname='ui_channel_results_fixture';
INSERT INTO public.novu_rust_operator_jwt_resend_auto_policy_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env',true,true,true);
SQL
fi
if [[ "${SWALLOW_UI_SLACK_DECISION_FIXTURE:-false}" == true ]]; then
 "$pgbin/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_slack_resolve(text,text,text,text,bigint,uuid,uuid,uuid,text,text),public.novu_rust_operator_jwt_slack_decision_request(text,text,text,text,bigint,uuid) TO ui_channel_results_fixture;
INSERT INTO public.novu_rust_operator_jwt_slack_decision_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env','retry') ON CONFLICT DO NOTHING;
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'ui-fixture-env',rolname,'slack_retry',oid FROM pg_roles WHERE rolname='ui_channel_results_fixture' ON CONFLICT DO NOTHING;
SQL
fi
if [[ "${SWALLOW_UI_RESEND_429_AUDIT_FIXTURE:-false}" == true ]]; then
 "$pgbin/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_429_audit_read(text,text,text,text,bigint,uuid) TO ui_channel_results_fixture;
INSERT INTO public.novu_rust_operator_jwt_resend_429_audit_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_rust_operator_jwt_resend_429_audit(id,environment_id,actor_issuer,actor_subject,actor_client,database_principal,request_id,job_id,attempt_id,mode,reason,available_at)
SELECT ('aaaaaaaa-aaaa-4aaa-8aaa-'||lpad(n::text,12,'0'))::uuid,'ui-fixture-env','https://issuer.test','fixture-operator','console','ui_channel_results_fixture',gen_random_uuid(),gen_random_uuid(),gen_random_uuid(),CASE WHEN n%2=0 THEN 'after' ELSE 'immediate' END,'audit fixture '||n,clock_timestamp() FROM generate_series(1,51) n;
SQL
fi
if [[ "${SWALLOW_UI_RESEND_429_GRANT_FIXTURE:-false}" == true ]]; then
 "$pgbin/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_429_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_resend_429_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_resend_429_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_resend_429_grant_audit_read(text,text,text,text,bigint,uuid) TO ui_channel_results_fixture;
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','target-fixture','console','ui-fixture-env');
INSERT INTO public.novu_rust_operator_jwt_resend_429_grant_admins VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env',true,true);
INSERT INTO public.novu_rust_operator_jwt_resend_429_grant_audit_viewers VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
SQL
fi
if [[ "${SWALLOW_UI_SLACK_GRANT_FIXTURE:-false}" == true ]]; then
 "$pgbin/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_slack_decision_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_slack_decision_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_slack_decision_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_slack_decision_grant_audit_read(text,text,text,text,bigint,uuid) TO ui_channel_results_fixture;
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','target-fixture','console','ui-fixture-env');
INSERT INTO public.novu_rust_operator_jwt_slack_decision_grant_admins VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env',true,true);
INSERT INTO public.novu_rust_operator_jwt_slack_decision_grant_audit_viewers VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
SQL
fi
if [[ "${SWALLOW_UI_SLACK_COOLDOWN_HEALTH_FIXTURE:-false}" == true ]]; then
 "$pgbin/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_slack_cooldown_health(text,text,text,text,bigint,integer) TO ui_channel_results_fixture;
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'ui-fixture-env',rolname,'alert_view',oid FROM pg_roles WHERE rolname='ui_channel_results_fixture' ON CONFLICT DO NOTHING;
INSERT INTO public.novu_rust_operator_jwt_slack_cooldown_health_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_rust_slack_cooldown_policies(environment_id,fallback_seconds,not_before) VALUES ('ui-fixture-env',30,clock_timestamp()+interval '5 minutes');
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error,available_at) VALUES (gen_random_uuid(),'ui-fixture-env','experimental_slack_chat','{}','pending','Slack shared cooldown active',clock_timestamp()-interval '10 minutes'),(gen_random_uuid(),'ui-fixture-env','experimental_slack_webhook','{}','pending','Slack shared cooldown active',clock_timestamp());
SQL
fi
if [[ "${SWALLOW_UI_TWILIO_RETRY_FIXTURE:-false}" == true ]]; then
 "$pgbin/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_twilio_retry_after(text,text,text,text,bigint,uuid,uuid,uuid,text,text),public.novu_rust_operator_jwt_twilio_retry_request(text,text,text,text,bigint,uuid) TO ui_channel_results_fixture;
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'ui-fixture-env',rolname,'retry',oid FROM pg_roles WHERE rolname='ui_channel_results_fixture' ON CONFLICT DO NOTHING;
INSERT INTO public.novu_rust_operator_jwt_twilio_retry_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa','ui-fixture-env','experimental_twilio_sms','{}','failed');
INSERT INTO public.novu_rust_twilio_dispatch_ids(id,job_id,environment_id,account_sid) VALUES ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb','aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa','ui-fixture-env','AC11111111111111111111111111111111');
INSERT INTO public.novu_rust_twilio_attempts(job_id,environment_id,state,http_status,dispatch_id,retry_after_seconds,retry_not_before) VALUES ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa','ui-fixture-env','rejected',429,'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',120,clock_timestamp()+interval '2 minutes');
SQL
fi
if [[ "${SWALLOW_UI_TWILIO_DELAYED_HEALTH_FIXTURE:-false}" == true ]]; then
 "$pgbin/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_twilio_delayed_health(text,text,text,text,bigint,integer) TO ui_channel_results_fixture;
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'ui-fixture-env',rolname,'alert_view',oid FROM pg_roles WHERE rolname='ui_channel_results_fixture' ON CONFLICT DO NOTHING;
INSERT INTO public.novu_rust_operator_jwt_twilio_delayed_health_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,twilio_delayed_retry,available_at,last_error) VALUES (gen_random_uuid(),'ui-fixture-env','experimental_twilio_sms','{}','pending',true,clock_timestamp()-interval '10 minutes',NULL),(gen_random_uuid(),'ui-fixture-env','experimental_twilio_sms','{}','failed',true,clock_timestamp(),'sms retry blocked by provider callback');
SQL
fi
node --input-type=module - "$root" "$testdir" <<'JS'
import {readFile,writeFile} from 'node:fs/promises';
import {SignJWT,importPKCS8} from './node_modules/jose/dist/webapi/index.js';
const root=process.argv[2], dir=process.argv[3], keyDir=`${root}/apps/api/rust-api/tests/fixtures/operator_jwt`;
const [privatePem,publicPem]=await Promise.all([readFile(`${keyDir}/test-only-private.pem`,'utf8'),readFile(`${keyDir}/test-only-public.pem`,'utf8')]);
const now=Math.floor(Date.now()/1000);
const token=await new SignJWT({client_id:'console',jti:'ui-fixture'}).setProtectedHeader({alg:'RS256',typ:'at+jwt',kid:'key'}).setIssuer('https://issuer.test').setSubject('ui-fixture').setAudience('swallow').setIssuedAt(now).setExpirationTime(now+600).sign(await importPKCS8(privatePem,'RS256'));
await writeFile(`${dir}/token`,token,{mode:0o600});
await writeFile(`${dir}/jwt-config`,JSON.stringify({issuer:'https://issuer.test',audience:'swallow',keys:[{kid:'key',pem:publicPem}]}),{mode:0o600});
JS
api_port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
cargo build --quiet --locked --manifest-path "$api/Cargo.toml" --bin novu-rust-api
env -i PATH="$PATH" DATABASE_URL="postgres://ui_channel_results_fixture@127.0.0.1:$pg_port/ui_test" NOVU_POSTGRES_ALLOW_INSECURE=true LISTEN_ADDR="127.0.0.1:$api_port" SWALLOW_OPERATOR_JWT_CONFIG_FILE="$testdir/jwt-config" SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true SWALLOW_CORS_ALLOWED_ORIGINS="http://127.0.0.1:4249" "$api/target/debug/novu-rust-api" >"$testdir/api.log" 2>&1 &
api_pid=$!
for ((i=0;i<60;i++)); do
 if [[ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$api_port/health/live")" == 200 ]]; then break; fi
 if ! kill -0 "$api_pid" 2>/dev/null; then echo 'API fixture failed to start' >&2; exit 1; fi
 ((i<59)) || { echo 'API fixture not ready' >&2; exit 1; }
 sleep 1
done
printf 'FIXTURE_DIR=%s\nAPI_URL=http://127.0.0.1:%s\nPG_PORT=%s\n' "$testdir" "$api_port" "$pg_port"
wait "$api_pid"
