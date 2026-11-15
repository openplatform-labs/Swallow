#!/usr/bin/env bash
# Trigger API, in-app fanout and both email adapters with mock providers.
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
API="$ROOT/apps/api/rust-api"; WORKER="$ROOT/apps/worker/rust-queue"
API_PID=
DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-email-attachments.XXXXXX")
cleanup() {
  if [[ -n "$API_PID" ]]; then kill "$API_PID" 2>/dev/null || true; wait "$API_PID" 2>/dev/null || true; fi
  if [[ -f "$DIR/data/postmaster.pid" ]] && ! "$TEST_PG_BIN/pg_ctl" -D "$DIR/data" -m immediate -w stop >/dev/null 2>&1; then
    echo "Could not stop test PostgreSQL; retained $DIR" >&2; return
  fi
  rm -rf "$DIR"
}
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$DIR/data" -o "-h 127.0.0.1 -p $PORT" -l "$DIR/postgres.log" -w start >/dev/null
export TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
cargo test --locked --manifest-path "$API/Cargo.toml" --test workflow_email_attachments trigger_preserves_bounded_attachments_for_both_email_providers -- --ignored
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --lib workflow_email_attachments_reach_both_mock_providers -- --ignored
cargo test --locked --manifest-path "$API/Cargo.toml" --test workflow_email_attachments resend_attachment_policy_rejects_unrepresentable_dispositions_before_trigger -- --ignored
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --lib resend::tests::attachments_and_headers_are_encoded
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --test reference_resend_source original_resend_options_match_selected_cases -- --ignored

"$TEST_PG_BIN/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 -c "UPDATE public.novu_rust_workflows SET email_step_active=true WHERE environment_id='attachment-policy-env' AND identifier='resend-workflow'"
cargo build --quiet --locked --manifest-path "$API/Cargo.toml" --bin novu-rust-api
if env -i PATH="$PATH" DATABASE_URL="$DATABASE_URL" NOVU_POSTGRES_ALLOW_INSECURE=true SWALLOW_RESEND_ATTACHMENT_POLICY=invalid "$API/target/debug/novu-rust-api" >"$DIR/invalid.log" 2>&1; then echo 'invalid attachment policy accepted' >&2; exit 1; fi
API_PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
start_api() {
  env -i PATH="$PATH" DATABASE_URL="$DATABASE_URL" NOVU_POSTGRES_ALLOW_INSECURE=true LISTEN_ADDR="127.0.0.1:$API_PORT" SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true SWALLOW_RESEND_ATTACHMENT_POLICY="$1" "$API/target/debug/novu-rust-api" >"$DIR/api.log" 2>&1 & API_PID=$!
  for _ in $(seq 1 100); do curl -fsS "http://127.0.0.1:$API_PORT/health/ready" >/dev/null 2>&1 && return; kill -0 "$API_PID"; sleep 0.1; done
  return 1
}
request() { curl -sS -o "$DIR/response" -w '%{http_code}' -H 'Authorization: ApiKey attachment-policy-key' -H 'Content-Type: application/json' --data-binary "{\"name\":\"resend-workflow\",\"to\":{\"subscriberId\":\"live\",\"email\":\"recipient@example.test\"},\"transactionId\":\"$1\",\"payload\":{\"attachments\":[{\"name\":\"img.png\",\"mime\":\"image/png\",\"file\":\"YQ==\",\"disposition\":\"inline\"}]}}" "http://127.0.0.1:$API_PORT/v1/events/trigger"; }
start_api reject
[[ $(request live-attachment-reject) == 422 ]]
kill "$API_PID"; wait "$API_PID" || true; API_PID=
start_api defer
[[ $(request live-attachment-defer) == 201 ]]
[[ $("$TEST_PG_BIN/psql" "$DATABASE_URL" -Atc "SELECT count(*) FROM public.novu_rust_trigger_transactions WHERE transaction_id='live-attachment-reject'") == 0 ]]
echo 'Real API process enforces the configured attachment policy.'
