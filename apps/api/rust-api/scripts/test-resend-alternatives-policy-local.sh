#!/usr/bin/env bash
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-resend-alternatives.XXXXXX")
API_PID=
cleanup() { if [[ -n "$API_PID" ]]; then kill "$API_PID" 2>/dev/null || true; wait "$API_PID" 2>/dev/null || true; fi; "$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf "$TEST_DIR"; }
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$TEST_DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -o "-h 127.0.0.1 -p $PORT -k $TEST_DIR" -l "$TEST_DIR/postgres.log" start >/dev/null
export TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$ROOT/apps/worker/rust-queue/Cargo.toml" --bin migrate-worker -- apply api "$ROOT/apps/api/rust-api/migrations" --through 17
cargo run --quiet --locked --manifest-path "$ROOT/apps/worker/rust-queue/Cargo.toml" --bin migrate-worker -- apply worker "$ROOT/apps/worker/rust-queue/migrations"
cargo run --quiet --locked --manifest-path "$ROOT/apps/worker/rust-queue/Cargo.toml" --bin migrate-worker -- apply api "$ROOT/apps/api/rust-api/migrations"
cargo test --locked --manifest-path "$ROOT/apps/api/rust-api/Cargo.toml" --test workflow_email_attachments resend_alternatives_policy_checks_effective_override_without_partial_trigger -- --ignored

"$TEST_PG_BIN/psql" "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "UPDATE public.novu_rust_workflows SET email_step_active=true WHERE environment_id='alternative-policy-env' AND identifier='resend-workflow'" >/dev/null
cargo build --quiet --locked --manifest-path "$ROOT/apps/api/rust-api/Cargo.toml" --bin novu-rust-api
if env -i PATH="$PATH" DATABASE_URL="$DATABASE_URL" NOVU_POSTGRES_ALLOW_INSECURE=true SWALLOW_RESEND_ALTERNATIVES_POLICY=invalid "$ROOT/apps/api/rust-api/target/debug/novu-rust-api" >"$TEST_DIR/invalid.log" 2>&1; then echo 'invalid policy unexpectedly accepted' >&2; exit 1; fi
API_PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
start_api() {
 env -i PATH="$PATH" DATABASE_URL="$DATABASE_URL" NOVU_POSTGRES_ALLOW_INSECURE=true LISTEN_ADDR="127.0.0.1:$API_PORT" SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true SWALLOW_RESEND_ALTERNATIVES_POLICY="$1" "$ROOT/apps/api/rust-api/target/debug/novu-rust-api" >"$TEST_DIR/api.log" 2>&1 & API_PID=$!
 for _ in $(seq 1 100); do curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null 2>&1 && return; kill -0 "$API_PID"; sleep 0.1; done
 return 1
}
request() { curl -sS -o "$TEST_DIR/response" -w '%{http_code}' -H 'Authorization: ApiKey alternative-policy-key' -H 'Content-Type: application/json' --data-binary "{\"name\":\"resend-workflow\",\"to\":{\"subscriberId\":\"live\",\"email\":\"recipient@example.test\"},\"transactionId\":\"$1\",\"payload\":{},\"overrides\":{\"email\":{\"alternatives\":[{\"contentType\":\"text/calendar\",\"content\":\"meeting\"}]}}}" "http://127.0.0.1:$API_PORT/v1/events/trigger"; }
start_api reject
[[ $(request live-reject) == 422 ]]
kill "$API_PID"; wait "$API_PID" || true; API_PID=
start_api defer
[[ $(request live-defer) == 201 ]]
[[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -Atc "SELECT count(*) FROM public.novu_rust_trigger_transactions WHERE transaction_id='live-reject'") == 0 ]]
echo 'real API process rejected or deferred according to configuration'
