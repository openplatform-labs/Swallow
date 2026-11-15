#!/usr/bin/env bash
# Docker PostgreSQL, restricted API login, synthetic JWTs and real browser.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
API="$ROOT/apps/api/rust-api"; WORKER="$ROOT/apps/worker/rust-queue"; UI="$ROOT/apps/dashboard/swallow"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-smtp-attachment-http-browser.XXXXXX")
API_PID= UI_PID= INAPP_PID= SMTP_PID= FIXTURE_PID=
CONTAINER="swallow-smtp-attachment-http-$$"
cleanup() {
  if [[ -n "$API_PID" ]]; then kill "$API_PID" 2>/dev/null || true; wait "$API_PID" 2>/dev/null || true; fi
  for pid in "$INAPP_PID" "$SMTP_PID" "$FIXTURE_PID"; do if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi; done
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
INSERT INTO public.novu_rust_users VALUES('attachment-user');
INSERT INTO public.novu_rust_environments VALUES('attachment-env','fixture-org');
INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,email_subject,email_html,email_provider,inbox_metadata) VALUES('attachment-env','smtp-attachment',true,true,'Hello','SMTP attachments','<p>hello</p>','smtp','{"id":"fixture-workflow"}');
INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES(encode(sha256(convert_to('fixture-api-key','UTF8')),'hex'),'attachment-env','attachment-user');
SQL
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost -addext subjectAltName=DNS:localhost -addext basicConstraints=critical,CA:FALSE -keyout "$TEST_DIR/key" -out "$TEST_DIR/cert" >"$TEST_DIR/openssl.log" 2>&1
printf '%s' fixture-password > "$TEST_DIR/password"
cargo build --quiet --locked --manifest-path "$API/Cargo.toml" --bin novu-rust-api
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin in-app-worker --bin smtp-worker
python3 "$WORKER/tests/fixtures/smtp_starttls_fixture.py" "$TEST_DIR/cert" "$TEST_DIR/key" "$TEST_DIR/message" > "$TEST_DIR/smtp-port" 2>"$TEST_DIR/smtp-fixture.log" & FIXTURE_PID=$!
for _ in $(seq 1 100); do [[ -s "$TEST_DIR/smtp-port" ]] && break; kill -0 "$FIXTURE_PID"; sleep 0.1; done
SMTP_PORT=$(cat "$TEST_DIR/smtp-port")
env -i PATH="$PATH" DATABASE_URL="$DATABASE_URL" NOVU_POSTGRES_ALLOW_INSECURE=true LISTEN_ADDR="127.0.0.1:$API_PORT" SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true "$API/target/debug/novu-rust-api" >"$TEST_DIR/api.log" 2>&1 & API_PID=$!
env -i PATH="$PATH" DATABASE_URL="$DATABASE_URL" NOVU_POSTGRES_ALLOW_INSECURE=true "$WORKER/target/debug/in-app-worker" >"$TEST_DIR/in-app.log" 2>&1 & INAPP_PID=$!
env -i PATH="$PATH" DATABASE_URL="$DATABASE_URL" NOVU_POSTGRES_ALLOW_INSECURE=true NOVU_SMTP_ENVIRONMENT_ID=attachment-env SWALLOW_SMTP_HOST=localhost SWALLOW_SMTP_PORT="$SMTP_PORT" SWALLOW_SMTP_FROM=sender@example.test SWALLOW_SMTP_TLS=starttls SWALLOW_SMTP_USERNAME=fixture-user SWALLOW_SMTP_PASSWORD_FILE="$TEST_DIR/password" SWALLOW_SMTP_CA_FILE="$TEST_DIR/cert" "$WORKER/target/debug/smtp-worker" >"$TEST_DIR/smtp.log" 2>&1 & SMTP_PID=$!
for _ in $(seq 1 100); do curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null 2>&1 && break; kill -0 "$API_PID"; sleep 0.1; done
cat > "$TEST_DIR/request.json" <<'JSON'
{"name":"smtp-attachment","to":{"subscriberId":"recipient","email":"recipient@example.test"},"transactionId":"smtp-attachment-http","payload":{"attachments":[{"file":"aGVsbG8=","name":"hello.txt","mime":"text/plain"},{"file":{"type":"Buffer","data":[0,1,255]},"name":"logo.png","mime":"image/png","cid":"logo","channels":["email"]}]}}
JSON
STATUS=$(curl -sS -o "$TEST_DIR/response" -w '%{http_code}' -H 'Authorization: ApiKey fixture-api-key' -H 'Content-Type: application/json' --data-binary @"$TEST_DIR/request.json" "http://127.0.0.1:$API_PORT/v1/events/trigger")
[[ "$STATUS" == 201 ]] || { echo "trigger returned $STATUS" >&2; cat "$TEST_DIR/response" >&2; exit 1; }
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
p={x.get_filename():x for x in m.walk() if x.get_filename()}
assert set(p)=={'hello.txt','logo.png'}
assert p['hello.txt'].get_payload(decode=True)==b'hello'
assert p['logo.png'].get_payload(decode=True)==bytes([0,1,255])
assert p['logo.png'].get('Content-ID')=='<logo>'
PY2
[[ $(db_sql -At -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='attachment-env' AND status='completed' AND attachment_completed_at IS NOT NULL AND ((kind='experimental_static_in_app_attachments_v1' AND payload ? 'emailAttachments') OR (kind='experimental_smtp_email' AND payload ? 'attachments'))") == 2 ]]
echo 'HTTP trigger, actual workers, SMTP attachment MIME and retention timestamps passed'
