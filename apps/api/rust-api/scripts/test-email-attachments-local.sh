#!/usr/bin/env bash
# Trigger API, in-app fanout and both email adapters with mock providers.
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
API="$ROOT/apps/api/rust-api"; WORKER="$ROOT/apps/worker/rust-queue"
DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-email-attachments.XXXXXX")
cleanup() {
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
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --lib resend::tests::attachments_and_headers_are_encoded
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --test reference_resend_source original_resend_options_match_selected_cases -- --ignored
