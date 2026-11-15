#!/usr/bin/env bash
# Disposable local PostgreSQL, Angular jsdom, Rust HTTP API and workflow worker.
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
API="$ROOT/apps/api/rust-api"; WORKER="$ROOT/apps/worker/rust-queue"; UI="$ROOT/apps/dashboard/swallow"
DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-schedule-ui.XXXXXX"); API_PID=; WORKER_PID=
cleanup() {
  for pid in "$API_PID" "$WORKER_PID"; do if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi; done
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
INSERT INTO public.novu_rust_users VALUES ('schedule-user');
INSERT INTO public.novu_rust_environments VALUES ('schedule-env','org');
INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES (encode(sha256('schedule-fixture-key'::bytea),'hex'),'schedule-env','schedule-user');
INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('schedule-env','fixture-subscriber','fixture-internal','{"email":"recipient@example.test"}');
INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,email_subject,email_html,email_provider,email_step_id,inbox_metadata) VALUES ('schedule-env','schedule-fixture',true,true,'Hi','template','<p>template</p>','resend','mail-step','{"id":"schedule-fixture-id"}');
SQL
cargo build --quiet --locked --manifest-path "$API/Cargo.toml" --bin novu-rust-api
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin in-app-worker
env -i PATH="$PATH" DATABASE_URL="$DATABASE_URL" NOVU_POSTGRES_ALLOW_INSECURE=true LISTEN_ADDR="127.0.0.1:$API_PORT" SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true "$API/target/debug/novu-rust-api" >"$DIR/api.log" 2>&1 & API_PID=$!
for _ in $(seq 1 100); do curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null 2>&1 && break; kill -0 "$API_PID"; sleep 0.1; done
curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null
(cd "$UI" && SWALLOW_UI_TEST_API="http://127.0.0.1:$API_PORT" npm test -- --watch=false --include src/app/workflow-trigger-integration.spec.ts)
"$WORKER/target/debug/in-app-worker" >"$DIR/worker.log" 2>&1 & WORKER_PID=$!
for _ in $(seq 1 100); do
  count=$("$TEST_PG_BIN/psql" "$DATABASE_URL" -qAtc "SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='schedule-env' AND kind='experimental_resend_email'")
  [[ "$count" == 1 ]] && break
  kill -0 "$WORKER_PID"; sleep 0.1
done
[[ "$count" == 1 ]]
"$TEST_PG_BIN/psql" "$DATABASE_URL" -qAtc "SELECT payload FROM public.novu_queue_jobs WHERE environment_id='schedule-env' AND kind='experimental_resend_email'" | python3 -c 'import json,sys; p=json.load(sys.stdin); assert p["workflow_context"]["overrides"]["__resend_passthrough"]["scheduledAt"]=="2030-01-01T09:00:00+09:00",p'
echo 'Angular form, local API and workflow worker schedule test passed.'
