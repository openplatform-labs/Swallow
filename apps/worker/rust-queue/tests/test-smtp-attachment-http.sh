#!/usr/bin/env bash
# Docker PostgreSQL, restricted API login, synthetic JWTs and real browser.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
API="$ROOT/apps/api/rust-api"; WORKER="$ROOT/apps/worker/rust-queue"; UI="$ROOT/apps/dashboard/swallow"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-smtp-attachment-http-browser.XXXXXX")
API_PID= UI_PID= INAPP_PID= SMTP_PID= FIXTURE_PID= WRONG_PID=
CONTAINER="swallow-smtp-attachment-http-$$"
cleanup() {
  if [[ -n "$API_PID" ]]; then kill "$API_PID" 2>/dev/null || true; wait "$API_PID" 2>/dev/null || true; fi
  for pid in "$INAPP_PID" "$SMTP_PID" "$FIXTURE_PID" "$WRONG_PID"; do if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi; done
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
CREATE ROLE smtp_fixture_worker LOGIN;
CREATE ROLE smtp_fixture_switch NOLOGIN;
CREATE ROLE smtp_fixture_plain LOGIN;
GRANT smtp_fixture_switch TO smtp_fixture_worker;
GRANT SELECT ON public.novu_queue_jobs TO smtp_fixture_switch,smtp_fixture_plain;
GRANT EXECUTE ON FUNCTION public.novu_rust_smtp_scope_audit_read(text,bigint) TO smtp_fixture_plain;
GRANT USAGE ON SCHEMA public TO smtp_fixture_worker;
GRANT SELECT,UPDATE ON public.novu_queue_jobs TO smtp_fixture_worker;
GRANT SELECT,INSERT,UPDATE ON public.novu_rust_smtp_attempts,public.novu_rust_email_workflow_worker_config TO smtp_fixture_worker;
GRANT SELECT,UPDATE ON public.novu_rust_subscribers TO smtp_fixture_worker;
GRANT UPDATE(email_subject) ON public.novu_rust_workflows TO smtp_fixture_worker;
GRANT SELECT ON public.novu_rust_workflow_preferences,public.novu_rust_subscriber_preferences,public.novu_rust_subscriber_workflow_preferences,public.novu_rust_workflows,public.novu_rust_contexts TO smtp_fixture_worker;
GRANT SELECT,INSERT,UPDATE,DELETE ON public.novu_rust_email_messages TO smtp_fixture_worker;
GRANT SELECT,UPDATE ON public.novu_rust_email_step_outcomes TO smtp_fixture_worker;
INSERT INTO public.novu_rust_users VALUES('attachment-user');
INSERT INTO public.novu_rust_environments VALUES('attachment-env','fixture-org'),('other-env','fixture-org');
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES('11111111-1111-4111-8111-000000000099','other-env','experimental_smtp_email','{"to":["other@example.test"],"subject":"other","html":"other"}');
INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,email_subject,email_html,email_provider,inbox_metadata,email_step_id) VALUES('attachment-env','smtp-attachment',true,true,'Hello','SMTP attachments','<p>hello</p>','smtp','{"id":"fixture-workflow"}','email-step');
INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES(encode(sha256(convert_to('fixture-api-key','UTF8')),'hex'),'attachment-env','attachment-user');
REVOKE INSERT ON public.novu_rust_smtp_attempts FROM smtp_fixture_worker;
SQL
if [[ "${TEST_SMTP_RLS:-false}" == true ]]; then
  db_sql -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE smtp_fixture_admin LOGIN;
CREATE ROLE smtp_fixture_target LOGIN;
GRANT USAGE ON SCHEMA public TO smtp_fixture_admin;
GRANT EXECUTE ON FUNCTION public.novu_rust_smtp_scope_admin_read(text,text),public.novu_rust_smtp_scope_admin_set(text,uuid,text,text,boolean,text),public.novu_rust_smtp_scope_admin_request(text,uuid) TO smtp_fixture_admin;
INSERT INTO public.novu_rust_smtp_scope_admins(environment_id,database_principal,principal_oid,can_read,can_write) SELECT 'attachment-env',rolname,oid,true,true FROM pg_roles WHERE rolname='smtp_fixture_admin';
INSERT INTO public.novu_rust_smtp_scoped_logins(principal_oid,database_principal,environment_id) SELECT oid,rolname,'attachment-env' FROM pg_roles WHERE rolname='smtp_fixture_worker';
INSERT INTO public.novu_rust_smtp_scope_audit_viewers(environment_id,database_principal,principal_oid) SELECT 'attachment-env',rolname,oid FROM pg_roles WHERE rolname='smtp_fixture_plain';
SQL
  if db_sql -q -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_worker; SELECT public.novu_rust_assert_smtp_scoped_login('attachment-env')" >/dev/null 2>&1; then echo 'uninstalled RLS was accepted' >&2; exit 1; fi
  if db_sql -q -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_admin; SELECT public.novu_rust_smtp_scope_admin_set('attachment-env','22222222-2222-4222-8222-000000000000','smtp_fixture_target','absent',false,'uninstalled')" >/dev/null 2>&1; then echo 'scope disabled without RLS installation' >&2; exit 1; fi
  db_sql -q -v ON_ERROR_STOP=1 -c "SELECT public.novu_rust_install_smtp_scoped_rls()"
  if db_sql -q -v ON_ERROR_STOP=1 -c "SELECT public.novu_rust_assert_smtp_scoped_login('attachment-env')" >/dev/null 2>&1; then echo 'superuser was accepted' >&2; exit 1; fi
  db_sql -q -v ON_ERROR_STOP=1 -c "CREATE ROLE smtp_fixture_bypass NOLOGIN BYPASSRLS; GRANT smtp_fixture_bypass TO smtp_fixture_worker"
  if db_sql -q -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_worker; SELECT public.novu_rust_assert_smtp_scoped_login('attachment-env')" >/dev/null 2>&1; then echo 'BYPASSRLS membership was accepted' >&2; exit 1; fi
  db_sql -q -v ON_ERROR_STOP=1 -c "REVOKE smtp_fixture_bypass FROM smtp_fixture_worker"
  db_sql -q -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_worker; SELECT public.novu_rust_assert_smtp_scoped_login('attachment-env')" >/dev/null

fi
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost -addext subjectAltName=DNS:localhost -addext basicConstraints=critical,CA:FALSE -keyout "$TEST_DIR/key" -out "$TEST_DIR/cert" >"$TEST_DIR/openssl.log" 2>&1
printf '%s' fixture-password > "$TEST_DIR/password"
cargo build --quiet --locked --manifest-path "$API/Cargo.toml" --bin novu-rust-api
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin in-app-worker --bin smtp-worker --bin delivery-reconcile --bin attachment-retention
python3 "$WORKER/tests/fixtures/smtp_starttls_fixture.py" "$TEST_DIR/cert" "$TEST_DIR/key" "$TEST_DIR/message" "$TEST_DIR/connected" > "$TEST_DIR/smtp-port" 2>"$TEST_DIR/smtp-fixture.log" & FIXTURE_PID=$!
for _ in $(seq 1 100); do [[ -s "$TEST_DIR/smtp-port" ]] && break; kill -0 "$FIXTURE_PID"; sleep 0.1; done
SMTP_PORT=$(cat "$TEST_DIR/smtp-port")
if [[ "${TEST_SMTP_RLS:-false}" == true ]]; then
  env -i PATH="$PATH" DATABASE_URL="postgres://smtp_fixture_worker@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true NOVU_SMTP_ENVIRONMENT_ID=other-env SWALLOW_SMTP_SCOPED_RLS=true SWALLOW_SMTP_HOST=localhost SWALLOW_SMTP_PORT="$SMTP_PORT" SWALLOW_SMTP_FROM=sender@example.test SWALLOW_SMTP_CA_FILE="$TEST_DIR/cert" "$WORKER/target/debug/smtp-worker" >"$TEST_DIR/wrong-scope.log" 2>&1 & WRONG_PID=$!
  for _ in $(seq 1 100); do
    rg -q 'SMTP scoped isolation unavailable; reconnecting' "$TEST_DIR/wrong-scope.log" && break
    kill -0 "$WRONG_PID"; sleep 0.1
  done
  rg -q 'SMTP scoped isolation unavailable; reconnecting' "$TEST_DIR/wrong-scope.log"
  [[ ! -e "$TEST_DIR/connected" ]]
  kill "$WRONG_PID"; wait "$WRONG_PID" || true; WRONG_PID=
  db_sql -q -v ON_ERROR_STOP=1 -c "GRANT smtp_fixture_bypass TO smtp_fixture_worker"
  env -i PATH="$PATH" DATABASE_URL="postgres://smtp_fixture_worker@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true NOVU_SMTP_ENVIRONMENT_ID=attachment-env SWALLOW_SMTP_SCOPED_RLS=true SWALLOW_SMTP_HOST=localhost SWALLOW_SMTP_PORT="$SMTP_PORT" SWALLOW_SMTP_FROM=sender@example.test SWALLOW_SMTP_CA_FILE="$TEST_DIR/cert" "$WORKER/target/debug/smtp-worker" >"$TEST_DIR/bypass-scope.log" 2>&1 & WRONG_PID=$!
  for _ in $(seq 1 100); do
    rg -q 'SMTP scoped isolation unavailable; reconnecting' "$TEST_DIR/bypass-scope.log" && break
    kill -0 "$WRONG_PID"; sleep 0.1
  done
  rg -q 'SMTP scoped isolation unavailable; reconnecting' "$TEST_DIR/bypass-scope.log"
  [[ ! -e "$TEST_DIR/connected" ]]
  kill "$WRONG_PID"; wait "$WRONG_PID" || true; WRONG_PID=
  db_sql -q -v ON_ERROR_STOP=1 -c "REVOKE smtp_fixture_bypass FROM smtp_fixture_worker"
fi
env -i PATH="$PATH" DATABASE_URL="$DATABASE_URL" NOVU_POSTGRES_ALLOW_INSECURE=true LISTEN_ADDR="127.0.0.1:$API_PORT" SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true "$API/target/debug/novu-rust-api" >"$TEST_DIR/api.log" 2>&1 & API_PID=$!
env -i PATH="$PATH" DATABASE_URL="$DATABASE_URL" NOVU_POSTGRES_ALLOW_INSECURE=true "$WORKER/target/debug/in-app-worker" >"$TEST_DIR/in-app.log" 2>&1 & INAPP_PID=$!
env -i PATH="$PATH" DATABASE_URL="postgres://smtp_fixture_worker@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true NOVU_SMTP_ENVIRONMENT_ID=attachment-env SWALLOW_SMTP_SCOPED_RLS="${TEST_SMTP_RLS:-false}" SWALLOW_EMAIL_WORKFLOW_STEP_POLICY="${TEST_EMAIL_POLICY:-snapshot}" SWALLOW_SMTP_HOST=localhost SWALLOW_SMTP_PORT="$SMTP_PORT" SWALLOW_SMTP_FROM=sender@example.test SWALLOW_SMTP_TLS=starttls SWALLOW_SMTP_USERNAME=fixture-user SWALLOW_SMTP_PASSWORD_FILE="$TEST_DIR/password" SWALLOW_SMTP_CA_FILE="$TEST_DIR/cert" "$WORKER/target/debug/smtp-worker" >"$TEST_DIR/smtp.log" 2>&1 & SMTP_PID=$!
for _ in $(seq 1 100); do curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null 2>&1 && break; kill -0 "$API_PID"; sleep 0.1; done
cat > "$TEST_DIR/request.json" <<'JSON'
{"name":"smtp-attachment","to":{"subscriberId":"recipient","email":"recipient@example.test"},"transactionId":"smtp-attachment-http","overrides":{"email":{"alternatives":[{"contentType":"text/calendar; method=REQUEST; charset=utf-8","content":"BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n"}],"headers":{"X-Swallow-Test":"fixture-value","List-Unsubscribe":"<https://example.test/unsubscribe>"}},"providers":{"smtp":{"alternatives":[{"contentType":"text/calendar","content":"PROVIDER"}]},"sendgrid":{"alternatives":[{"contentType":"text/calendar","content":"OTHER-PROVIDER"}]}},"steps":{"email-step":{"providers":{"smtp":{"alternatives":[{"contentType":"text/calendar; method=REQUEST; charset=utf-8","content":"BEGIN:VCALENDAR\r\nX-SOURCE:STEP\r\nEND:VCALENDAR\r\n"}]}}}}},"payload":{"attachments":[{"file":"aGVsbG8=","name":"hello.txt","mime":"text/plain"},{"file":{"type":"Buffer","data":[0,1,255]},"name":"logo.png","mime":"image/png","cid":"logo","channels":["email"]}]}}
JSON
python3 - "$TEST_DIR/request.json" "$TEST_DIR/invalid.json" <<'PY2'
import json,sys
v=json.load(open(sys.argv[1]));v['transactionId']='invalid-alternatives';v['overrides']['email']['alternatives']=[{'contentType':'text/calendar','content':'ok','unexpected':True}]
json.dump(v,open(sys.argv[2],'w'))
PY2
INVALID=$(curl -sS -o "$TEST_DIR/invalid-response" -w '%{http_code}' -H 'Authorization: ApiKey fixture-api-key' -H 'Content-Type: application/json' --data-binary @"$TEST_DIR/invalid.json" "http://127.0.0.1:$API_PORT/v1/events/trigger")
[[ "$INVALID" == 501 ]] || { echo "invalid alternative returned $INVALID" >&2; exit 1; }
STATUS=$(curl -sS -o "$TEST_DIR/response" -w '%{http_code}' -H 'Authorization: ApiKey fixture-api-key' -H 'Content-Type: application/json' --data-binary @"$TEST_DIR/request.json" "http://127.0.0.1:$API_PORT/v1/events/trigger")
[[ "$STATUS" == 201 ]] || { echo "trigger returned $STATUS" >&2; cat "$TEST_DIR/response" >&2; exit 1; }
# The worker may claim a job, but without attempt INSERT it must not contact the relay.
for _ in $(seq 1 100); do
  CLAIMED=$(db_sql -At -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='attachment-env' AND kind='experimental_smtp_email' AND status='running'")
  [[ "$CLAIMED" == 1 ]] && break
  kill -0 "$SMTP_PID"; sleep 0.1
done
[[ "$CLAIMED" == 1 ]] || { echo 'SMTP worker did not claim the fixture job' >&2; exit 1; }
for _ in $(seq 1 100); do
  rg -q 'SMTP worker unavailable; reconnecting' "$TEST_DIR/smtp.log" && break
  kill -0 "$SMTP_PID"; sleep 0.1
done
rg -q 'SMTP worker unavailable; reconnecting' "$TEST_DIR/smtp.log"
[[ $(db_sql -At -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM public.novu_rust_smtp_attempts WHERE environment_id='attachment-env'") == 0 ]]
[[ ! -e "$TEST_DIR/connected" && ! -s "$TEST_DIR/message" ]]
db_sql -q -v ON_ERROR_STOP=1 -c "GRANT INSERT ON public.novu_rust_smtp_attempts TO smtp_fixture_worker"
db_sql -q -v ON_ERROR_STOP=1 -c "UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE environment_id='attachment-env' AND kind='experimental_smtp_email' AND status='running'"
for _ in $(seq 1 200); do
  DONE=$(db_sql -At -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id WHERE j.environment_id='attachment-env' AND j.kind='experimental_smtp_email' AND j.status='completed' AND a.state='accepted'")
  [[ "$DONE" == 1 && -s "$TEST_DIR/message" ]] && break
  kill -0 "$INAPP_PID"; kill -0 "$SMTP_PID"; sleep 0.1
done
[[ "$DONE" == 1 && -s "$TEST_DIR/message" ]] || { cat "$TEST_DIR/api.log" "$TEST_DIR/in-app.log" "$TEST_DIR/smtp.log" "$TEST_DIR/smtp-fixture.log" >&2; exit 1; }
wait "$FIXTURE_PID"; FIXTURE_PID=
python3 - "$TEST_DIR/message" <<'PY2'
import email,sys
m=email.message_from_binary_file(open(sys.argv[1],'rb'))
assert m['X-Swallow-Test']=='fixture-value'
assert m['List-Unsubscribe']=='<https://example.test/unsubscribe>'
c=[x for x in m.walk() if x.get_content_type()=='text/calendar']; assert len(c)==1 and c[0].get_param('method')=='REQUEST' and b'X-SOURCE:STEP' in c[0].get_payload(decode=True)
p={x.get_filename():x for x in m.walk() if x.get_filename()}
assert set(p)=={'hello.txt','logo.png'}
assert p['hello.txt'].get_payload(decode=True)==b'hello'
assert p['logo.png'].get_payload(decode=True)==bytes([0,1,255])
assert p['logo.png'].get('Content-ID')=='<logo>'
PY2
[[ $(db_sql -At -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='attachment-env' AND status='completed' AND attachment_completed_at IS NOT NULL AND ((kind='experimental_static_in_app_attachments_v1' AND payload ? 'emailAttachments') OR (kind='experimental_smtp_email' AND payload ? 'attachments'))") == 2 ]]
[[ $(db_sql -At -v ON_ERROR_STOP=1 -c "SELECT status='pending' AND NOT EXISTS(SELECT 1 FROM public.novu_rust_smtp_attempts WHERE job_id='11111111-1111-4111-8111-000000000099') FROM public.novu_queue_jobs WHERE id='11111111-1111-4111-8111-000000000099'") == t ]]
if [[ "${TEST_SMTP_RLS:-false}" == true ]]; then
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_worker; SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='other-env'") == 0 ]]
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_worker; SELECT public.novu_rust_smtp_rls_allows(NULL)") == f ]]
  if db_sql -q -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_worker; UPDATE public.novu_queue_jobs SET environment_id='other-env' WHERE environment_id='attachment-env'" >/dev/null 2>&1; then echo 'cross-environment move was allowed' >&2; exit 1; fi
  if db_sql -q -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_worker; SELECT public.novu_rust_assert_smtp_scoped_login('other-env')" >/dev/null 2>&1; then echo 'wrong SMTP scope was accepted' >&2; exit 1; fi
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_worker; SET ROLE smtp_fixture_switch; SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='other-env'") == 0 ]]
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_plain; SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='other-env'") == 1 ]]
  if db_sql -q -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_worker; SELECT public.novu_rust_install_smtp_scoped_rls()" >/dev/null 2>&1; then echo 'worker could install RLS' >&2; exit 1; fi
  if db_sql -q -v ON_ERROR_STOP=1 -c "SELECT public.novu_rust_install_smtp_scoped_rls()" >/dev/null 2>&1; then echo 'existing policies unexpectedly replaced' >&2; exit 1; fi
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_worker; WITH changed AS (UPDATE public.novu_queue_jobs SET last_error='forbidden' WHERE environment_id='other-env' RETURNING 1) SELECT count(*) FROM changed") == 0 ]]
  if db_sql -q -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_worker; INSERT INTO public.novu_rust_smtp_attempts(job_id,environment_id,state) VALUES('11111111-1111-4111-8111-000000000099','other-env','unknown')" >/dev/null 2>&1; then echo 'cross-environment insert was allowed' >&2; exit 1; fi
  if db_sql -q -v ON_ERROR_STOP=1 -c "DELETE FROM public.novu_rust_smtp_scoped_logins WHERE database_principal='smtp_fixture_worker'" >/dev/null 2>&1; then echo 'scope registration was deleted' >&2; exit 1; fi
  if db_sql -q -v ON_ERROR_STOP=1 -c "UPDATE public.novu_rust_smtp_scoped_logins SET database_principal='smtp_fixture_plain' WHERE database_principal='smtp_fixture_worker'" >/dev/null 2>&1; then echo 'scope identity was changed' >&2; exit 1; fi
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_plain; SELECT count(*) FROM public.novu_rust_smtp_scope_audit_read('attachment-env',0)") == 1 ]]
  if db_sql -q -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_plain; SELECT * FROM public.novu_rust_smtp_scope_audit_read('other-env',0)" >/dev/null 2>&1; then echo 'cross-environment scope audit allowed' >&2; exit 1; fi
  if db_sql -q -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_plain; SELECT * FROM public.novu_rust_smtp_scope_audit" >/dev/null 2>&1; then echo 'direct scope audit access allowed' >&2; exit 1; fi
  db_sql -q -v ON_ERROR_STOP=1 -c "UPDATE public.novu_rust_smtp_scoped_logins SET enabled=true WHERE database_principal='smtp_fixture_worker'"
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_plain; SELECT count(*) FROM public.novu_rust_smtp_scope_audit_read('attachment-env',0)") == 1 ]]
  db_sql -q -v ON_ERROR_STOP=1 -c "CREATE FUNCTION public.fixture_audit_fail() RETURNS trigger LANGUAGE plpgsql AS \$\$ BEGIN RAISE EXCEPTION 'fixture'; END \$\$; CREATE TRIGGER fixture_audit_fail BEFORE INSERT ON public.novu_rust_smtp_scope_audit FOR EACH ROW EXECUTE FUNCTION public.fixture_audit_fail()"
  if db_sql -q -v ON_ERROR_STOP=1 -c "UPDATE public.novu_rust_smtp_scoped_logins SET enabled=false WHERE database_principal='smtp_fixture_worker'" >/dev/null 2>&1; then echo 'scope change succeeded without audit' >&2; exit 1; fi
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SELECT enabled FROM public.novu_rust_smtp_scoped_logins WHERE database_principal='smtp_fixture_worker'") == t ]]
  db_sql -q -v ON_ERROR_STOP=1 -c "DROP TRIGGER fixture_audit_fail ON public.novu_rust_smtp_scope_audit; DROP FUNCTION public.fixture_audit_fail()"
  db_sql -q -v ON_ERROR_STOP=1 -c "UPDATE public.novu_rust_smtp_scoped_logins SET enabled=false WHERE database_principal='smtp_fixture_worker'"
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_plain; SELECT count(*) FROM public.novu_rust_smtp_scope_audit_read('attachment-env',0)") == 2 ]]
  env -i PATH="$PATH" DATABASE_URL="postgres://smtp_fixture_plain@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true "$WORKER/target/debug/delivery-reconcile" smtp-scope-audit attachment-env > "$TEST_DIR/audit.json"
  python3 - "$TEST_DIR/audit.json" <<'PY2'
import json,sys
rows=json.load(open(sys.argv[1]))['data']
assert len(rows)==2 and rows[0]['previousEnabled'] is None and rows[1]['enabled'] is False
assert rows[1]['databasePrincipal']=='smtp_fixture_worker'
PY2
FIRST_AUDIT_ID=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["data"][0]["id"])' "$TEST_DIR/audit.json")
  env -i PATH="$PATH" DATABASE_URL="postgres://smtp_fixture_plain@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true "$WORKER/target/debug/delivery-reconcile" smtp-scope-audit attachment-env "$FIRST_AUDIT_ID" > "$TEST_DIR/audit-next.json"
  python3 - "$TEST_DIR/audit-next.json" <<'PY2'
import json,sys
rows=json.load(open(sys.argv[1]))['data']
assert len(rows)==1 and rows[0]['enabled'] is False
PY2
  db_sql -q -v ON_ERROR_STOP=1 -c "CREATE ROLE smtp_fixture_move LOGIN; INSERT INTO public.novu_rust_smtp_scoped_logins(principal_oid,database_principal,environment_id) SELECT oid,rolname,'attachment-env' FROM pg_roles WHERE rolname='smtp_fixture_move'; UPDATE public.novu_rust_smtp_scoped_logins SET environment_id='other-env' WHERE database_principal='smtp_fixture_move'"
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_plain; SELECT count(*) FROM public.novu_rust_smtp_scope_audit_read('attachment-env',0) WHERE database_principal='smtp_fixture_move' AND previous_environment='attachment-env' AND environment_id IS NULL") == 1 ]]
  db_sql -q -v ON_ERROR_STOP=1 -c "DELETE FROM public.novu_rust_smtp_scope_audit_viewers WHERE environment_id='attachment-env' AND database_principal='smtp_fixture_plain'"
  if db_sql -q -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_plain; SELECT * FROM public.novu_rust_smtp_scope_audit_read('attachment-env',0)" >/dev/null 2>&1; then echo 'revoked audit access allowed' >&2; exit 1; fi
  if db_sql -q -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_worker; SELECT public.novu_rust_assert_smtp_scoped_login('attachment-env')" >/dev/null 2>&1; then echo 'revoked SMTP scope was accepted' >&2; exit 1; fi
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_worker; SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='attachment-env'") == 0 ]]
else
  # Table grants are intentionally broad without the optional RLS installation.
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_worker; SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='other-env'") == 1 ]]
fi
[[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SELECT has_table_privilege('smtp_fixture_worker','public.novu_rust_subscribers','DELETE')") == f ]]
if [[ "${TEST_SMTP_RLS:-false}" == true ]]; then
  db_sql -q -v ON_ERROR_STOP=1 <<'SQL'
SQL
  admin_cli() { env -i PATH="$PATH" DATABASE_URL="postgres://smtp_fixture_admin@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true "$WORKER/target/debug/delivery-reconcile" "$@"; }
  if db_sql -q -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_admin; SELECT * FROM public.novu_rust_smtp_scoped_logins" >/dev/null 2>&1; then echo 'direct scoped login access allowed' >&2; exit 1; fi
  admin_cli smtp-scope-read attachment-env smtp_fixture_target > "$TEST_DIR/scope-read.json"
  python3 - "$TEST_DIR/scope-read.json" <<'PY2'
import json,sys
assert json.load(open(sys.argv[1])) == {'available':True,'state':'absent'}
PY2
  if admin_cli smtp-scope-read other-env smtp_fixture_target >/dev/null 2>&1; then echo 'cross-environment admin allowed' >&2; exit 1; fi
  if admin_cli smtp-scope-set attachment-env 22222222-2222-4222-8222-000000000001 smtp_fixture_admin absent enabled self >/dev/null 2>&1; then echo 'self administration allowed' >&2; exit 1; fi
  db_sql -q -v ON_ERROR_STOP=1 -c "CREATE FUNCTION public.fixture_scope_admin_fail() RETURNS trigger LANGUAGE plpgsql AS \$\$ BEGIN RAISE EXCEPTION 'fixture'; END \$\$; CREATE TRIGGER fixture_scope_admin_fail BEFORE INSERT ON public.novu_rust_smtp_scope_audit FOR EACH ROW EXECUTE FUNCTION public.fixture_scope_admin_fail()"
  if admin_cli smtp-scope-set attachment-env 22222222-2222-4222-8222-000000000002 smtp_fixture_target absent enabled rollback >/dev/null 2>&1; then echo 'admin change survived audit failure' >&2; exit 1; fi
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM public.novu_rust_smtp_scoped_logins WHERE database_principal='smtp_fixture_target'") == 0 ]]
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM public.novu_rust_smtp_scope_admin_requests WHERE request_id='22222222-2222-4222-8222-000000000002'") == 0 ]]
  db_sql -q -v ON_ERROR_STOP=1 -c "DROP TRIGGER fixture_scope_admin_fail ON public.novu_rust_smtp_scope_audit; DROP FUNCTION public.fixture_scope_admin_fail()"
  db_sql -q -v ON_ERROR_STOP=1 -c "GRANT smtp_fixture_bypass TO smtp_fixture_target"
  if admin_cli smtp-scope-set attachment-env 22222222-2222-4222-8222-000000000003 smtp_fixture_target absent enabled bypass >/dev/null 2>&1; then echo 'privileged target was accepted' >&2; exit 1; fi
  db_sql -q -v ON_ERROR_STOP=1 -c "REVOKE smtp_fixture_bypass FROM smtp_fixture_target"
  admin_cli smtp-scope-set attachment-env 22222222-2222-4222-8222-000000000004 smtp_fixture_target absent enabled ticket > "$TEST_DIR/scope-a.json" & p1=$!
  admin_cli smtp-scope-set attachment-env 22222222-2222-4222-8222-000000000004 smtp_fixture_target absent enabled ticket > "$TEST_DIR/scope-b.json" & p2=$!
  wait "$p1"; wait "$p2"
  python3 - "$TEST_DIR/scope-a.json" "$TEST_DIR/scope-b.json" <<'PY2'
import json,sys
values=[json.load(open(path)) for path in sys.argv[1:]]
assert sorted(v['replayed'] for v in values)==[False,True]
assert all(v['result']=={'previous':'absent','state':'enabled','changed':True} for v in values)
PY2
  if admin_cli smtp-scope-set attachment-env 22222222-2222-4222-8222-000000000004 smtp_fixture_target absent enabled different >/dev/null 2>&1; then echo 'conflicting replay was allowed' >&2; exit 1; fi
  if admin_cli smtp-scope-set attachment-env 22222222-2222-4222-8222-000000000005 smtp_fixture_target absent disabled stale >/dev/null 2>&1; then echo 'stale scope state was allowed' >&2; exit 1; fi
  admin_cli smtp-scope-request attachment-env 22222222-2222-4222-8222-000000000004 > "$TEST_DIR/scope-request.json"
  python3 - "$TEST_DIR/scope-request.json" <<'PY2'
import json,sys
v=json.load(open(sys.argv[1])); assert v['found'] is True and v['reason']=='ticket' and v['result']['state']=='enabled'
PY2
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM public.novu_rust_smtp_scope_audit WHERE database_principal='smtp_fixture_target'") == 1 ]]
  db_sql -q -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_target; SELECT public.novu_rust_assert_smtp_scoped_login('attachment-env')" >/dev/null
  admin_cli smtp-scope-set attachment-env 22222222-2222-4222-8222-000000000006 smtp_fixture_target enabled disabled completed > /dev/null
  [[ $(db_sql -qAt -v ON_ERROR_STOP=1 -c "SELECT enabled FROM public.novu_rust_smtp_scoped_logins WHERE database_principal='smtp_fixture_target'") == f ]]
  if db_sql -q -v ON_ERROR_STOP=1 -c "SET SESSION AUTHORIZATION smtp_fixture_target; SELECT public.novu_rust_assert_smtp_scoped_login('attachment-env')" >/dev/null 2>&1; then echo 'disabled target startup was accepted' >&2; exit 1; fi
  if admin_cli smtp-scope-set attachment-env 22222222-2222-4222-8222-000000000007 smtp_fixture_move absent enabled wrong >/dev/null 2>&1; then echo 'other environment target was changed' >&2; exit 1; fi
  db_sql -q -v ON_ERROR_STOP=1 -c "UPDATE public.novu_rust_smtp_scope_admins SET can_write=false WHERE database_principal='smtp_fixture_admin'"
  if admin_cli smtp-scope-request attachment-env 22222222-2222-4222-8222-000000000004 >/dev/null 2>&1; then echo 'revoked admin access allowed' >&2; exit 1; fi
  admin_cli smtp-scope-read attachment-env smtp_fixture_target > /dev/null
  db_sql -q -v ON_ERROR_STOP=1 -c "UPDATE public.novu_rust_smtp_scope_admins SET can_read=false WHERE database_principal='smtp_fixture_admin'"
  if admin_cli smtp-scope-read attachment-env smtp_fixture_target >/dev/null 2>&1; then echo 'revoked read access allowed' >&2; exit 1; fi
fi
db_sql -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE smtp_alt_retention LOGIN;
GRANT USAGE ON SCHEMA public TO smtp_alt_retention;
GRANT EXECUTE ON FUNCTION public.novu_rust_scrub_alternative_retention(text,integer,integer,text) TO smtp_alt_retention;
INSERT INTO public.novu_rust_attachment_retention_grants SELECT 'attachment-env',rolname,oid,false,true FROM pg_roles WHERE rolname='smtp_alt_retention';
SQL
env -i PATH="$PATH" DATABASE_URL="postgres://smtp_alt_retention@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true "$WORKER/target/debug/attachment-retention" scrub-alternatives attachment-env 0 100 fixture > "$TEST_DIR/alternative-retention.json"
python3 - "$TEST_DIR/alternative-retention.json" <<'PY2'
import json,sys
assert len(json.load(open(sys.argv[1])))==2
PY2
[[ $(db_sql -At -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='attachment-env' AND alternative_scrubbed_at IS NOT NULL AND ((kind='experimental_static_in_app_attachments_v1' AND NOT (payload->'emailOverride' ? 'alternatives') AND payload ? 'emailAttachments' AND NOT payload::text LIKE '%OTHER-PROVIDER%' AND NOT payload::text LIKE '%X-SOURCE:STEP%') OR (kind='experimental_smtp_email' AND NOT (payload ? 'alternatives') AND NOT (payload#>'{workflow_context,overrides}' ? 'alternatives') AND payload ? 'attachments'))") == 2 ]]
if db_sql -q -v ON_ERROR_STOP=1 -c "UPDATE public.novu_queue_jobs SET status='pending' WHERE environment_id='attachment-env' AND alternative_scrubbed_at IS NOT NULL" >/dev/null 2>&1; then echo 'scrubbed alternatives were requeued' >&2; exit 1; fi
echo 'HTTP trigger, restricted SMTP worker, revoked attempt permission, recovery and environment filtering passed'
