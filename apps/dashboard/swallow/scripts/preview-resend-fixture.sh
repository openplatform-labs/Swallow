#!/usr/bin/env bash
# Disposable local UI/API fixture. Uses only the repository's test JWT key.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(cd ../../.. && pwd)
api="$root/apps/api/rust-api"
worker="$root/apps/worker/rust-queue"
name="swallow-ui-resend-$$"
fixture_dir=$(mktemp -d /private/tmp/swallow-ui-resend.XXXXXX)
api_pid=''
cleanup() {
  if [[ -n "$api_pid" ]]; then kill "$api_pid" 2>/dev/null || true; wait "$api_pid" 2>/dev/null || true; fi
  docker rm -f "$name" >/dev/null 2>&1 || true
  rm -rf "$fixture_dir"
}
trap cleanup EXIT
docker run --rm -d --name "$name" -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=ui_test -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
for ((i=0;i<60;i++)); do
  if docker exec "$name" pg_isready -q -U postgres -d ui_test; then break; fi
  if ((i==59)); then echo 'PostgreSQL did not become ready' >&2; exit 1; fi
  sleep 1
done
pg_port=$(docker port "$name" 5432/tcp); pg_port=${pg_port##*:}
export DATABASE_URL="postgres://postgres@127.0.0.1:${pg_port}/ui_test" NOVU_POSTGRES_ALLOW_INSECURE=true
cargo run --quiet --locked --manifest-path "$worker/Cargo.toml" --bin migrate-worker -- apply api "$api/migrations" --through 17
cargo run --quiet --locked --manifest-path "$worker/Cargo.toml" --bin migrate-worker -- apply worker "$worker/migrations"
cargo run --quiet --locked --manifest-path "$worker/Cargo.toml" --bin migrate-worker -- apply api "$api/migrations"
docker exec -i "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d ui_test <<'SQL'
CREATE ROLE ui_resend_fixture LOGIN;
GRANT USAGE ON SCHEMA public TO ui_resend_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_result(text,text,text,text,bigint,uuid) TO ui_resend_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_jobs(text,text,text,text,bigint,uuid) TO ui_resend_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_workflow_read(text,text,text,text,bigint,text,text),public.novu_rust_operator_jwt_workflow_write(text,text,text,text,bigint,text,text,jsonb,jsonb,text) TO ui_resend_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_workflow_audit_read(text,text,text,text,bigint,text,text,uuid) TO ui_resend_fixture;
GRANT SELECT ON public.novu_rust_users,public.novu_rust_api_keys,public.novu_rust_workflows TO ui_resend_fixture;
GRANT UPDATE(email_subject,email_html,email_step_active,sms_content,sms_step_active,push_title,push_content,push_step_active,chat_content,chat_step_active) ON public.novu_rust_workflows TO ui_resend_fixture;
GRANT INSERT ON public.novu_rust_channel_workflow_audit,public.novu_rust_slack_workflow_audit TO ui_resend_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_lock_channel_workflow_grant(text,text,text,text),public.novu_rust_lock_slack_workflow_grant(text,text,text) TO ui_resend_fixture;
GRANT SELECT ON public.novu_rust_workflow_provider_permissions TO ui_resend_fixture;
GRANT UPDATE(email_provider) ON public.novu_rust_workflows TO ui_resend_fixture;
GRANT INSERT ON public.novu_rust_workflow_provider_audit TO ui_resend_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_lock_workflow_provider_grant(text,text,text) TO ui_resend_fixture;
INSERT INTO public.novu_rust_environments VALUES ('ui-fixture-env','org');
INSERT INTO public.novu_rust_users VALUES ('ui-fixture-user');
INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES (encode(sha256(convert_to('ui-fixture-api-key','UTF8')),'hex'),'ui-fixture-env','ui-fixture-user');
INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_step_active,email_subject,email_html,chat_content) VALUES ('ui-fixture-env','ui-fixture-workflow',true,true,false,'Fixture subject','Fixture body','Fixture Slack');
INSERT INTO public.novu_rust_channel_workflow_permissions SELECT encode(sha256(convert_to('ui-fixture-api-key','UTF8')),'hex'),channel,permission FROM unnest(ARRAY['email','sms','push']) channel CROSS JOIN unnest(ARRAY['read','write']) permission;
INSERT INTO public.novu_rust_slack_workflow_permissions SELECT encode(sha256(convert_to('ui-fixture-api-key','UTF8')),'hex'),permission FROM unnest(ARRAY['read','write']) permission;
INSERT INTO public.novu_rust_workflow_provider_permissions SELECT encode(sha256(convert_to('ui-fixture-api-key','UTF8')),'hex'),permission FROM unnest(ARRAY['read','write']) permission;
INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='ui_resend_fixture';
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_rust_operator_jwt_workflow_grants SELECT 'https://issuer.test','ui-fixture','console','ui-fixture-env',resource,permission FROM unnest(ARRAY['email','sms','push','slack','email-provider']) resource CROSS JOIN unnest(ARRAY['read','write']) permission;
INSERT INTO public.novu_rust_operator_jwt_workflow_audit_grants VALUES('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_rust_operator_jwt_resend_result_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_rust_operator_jwt_resend_jobs_grants VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env');
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES ('1889f88e-c0a3-4ab5-b861-2fdc1f26f254','ui-fixture-env','experimental_resend_email','{}','completed');
INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id,correlation_id) VALUES ('1889f88e-c0a3-4ab5-b861-2fdc1f26f254','ui-fixture-env','accepted','fixture-provider','96df49d1-0f4d-4bce-9290-666dd8fc908c');
INSERT INTO public.novu_rust_resend_recipients VALUES ('1889f88e-c0a3-4ab5-b861-2fdc1f26f254','ui-fixture-env','to',0,'fixture-hash');
INSERT INTO public.novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,recipient_hash,correlation_id) VALUES ('ui-fixture-env','fixture-event','fixture-provider','email.delivered','fixture-hash','96df49d1-0f4d-4bce-9290-666dd8fc908c');
INSERT INTO public.novu_rust_resend_auto_correlations(job_id,environment_id,correlation_id,provider_message_id,evidence_count) VALUES ('1889f88e-c0a3-4ab5-b861-2fdc1f26f254','ui-fixture-env','96df49d1-0f4d-4bce-9290-666dd8fc908c','fixture-provider',1);
UPDATE public.novu_queue_jobs SET resend_delivery_result='delivered' WHERE id='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
SQL
node --input-type=module - "$root" "$fixture_dir" <<'JS'
import {readFile,writeFile} from 'node:fs/promises';
import {SignJWT,importPKCS8} from './node_modules/jose/dist/webapi/index.js';
const root=process.argv[2], dir=process.argv[3];
const keyDir=`${root}/apps/api/rust-api/tests/fixtures/operator_jwt`;
const [privatePem,publicPem]=await Promise.all([readFile(`${keyDir}/test-only-private.pem`,'utf8'),readFile(`${keyDir}/test-only-public.pem`,'utf8')]);
const now=Math.floor(Date.now()/1000);
const token=await new SignJWT({client_id:'console',jti:'ui-fixture'}).setProtectedHeader({alg:'RS256',typ:'at+jwt',kid:'key'}).setIssuer('https://issuer.test').setSubject('ui-fixture').setAudience('swallow').setIssuedAt(now).setExpirationTime(now+600).sign(await importPKCS8(privatePem,'RS256'));
await writeFile(`${dir}/token`,token,{mode:0o600});
await writeFile(`${dir}/jwt-config`,JSON.stringify({issuer:'https://issuer.test',audience:'swallow',keys:[{kid:'key',pem:publicPem}]}),{mode:0o600});
JS
api_port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
cargo build --quiet --locked --manifest-path "$api/Cargo.toml" --bin novu-rust-api
env -i PATH="$PATH" DATABASE_URL="postgres://ui_resend_fixture@127.0.0.1:${pg_port}/ui_test" \
  NOVU_POSTGRES_ALLOW_INSECURE=true LISTEN_ADDR="127.0.0.1:${api_port}" \
  SWALLOW_OPERATOR_JWT_CONFIG_FILE="$fixture_dir/jwt-config" SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true \
  SWALLOW_CORS_ALLOWED_ORIGINS="http://127.0.0.1:4249" \
  "$api/target/debug/novu-rust-api" >"$fixture_dir/api.log" 2>&1 &
api_pid=$!
for ((i=0;i<60;i++)); do
  if [[ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${api_port}/health/live")" == 200 ]]; then break; fi
  if ! kill -0 "$api_pid" 2>/dev/null; then echo 'API fixture failed to start' >&2; exit 1; fi
  if ((i==59)); then echo 'API fixture not ready' >&2; exit 1; fi
  sleep 1
done
printf 'FIXTURE_DIR=%s\nAPI_URL=http://127.0.0.1:%s\nCONTAINER=%s\n' "$fixture_dir" "$api_port" "$name"
wait "$api_pid"
