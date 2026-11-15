#!/usr/bin/env bash
# Creates and destroys a loopback-only disposable PostgreSQL cluster.
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
WORKER="$ROOT/apps/worker/rust-queue"
API="$ROOT/apps/api/rust-api"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-resend-bundle.XXXXXX")
cleanup() {
  "$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$TEST_DIR"
}
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$TEST_DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -o "-h 127.0.0.1 -p $PORT -k $TEST_DIR" -l "$TEST_DIR/postgres.log" start >/dev/null
export DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
TEST_DATABASE_URL="$DATABASE_URL" cargo test --locked --manifest-path "$API/Cargo.toml" --test resend_webhook_bundle -- --ignored
TEST_DATABASE_URL="$DATABASE_URL" cargo test --locked --manifest-path "$WORKER/Cargo.toml" --test resend_webhook_key_probe_alert resend_bundle_probe_triggers_coordinated_alert_and_recovery -- --ignored
cargo build --quiet --locked --manifest-path "$API/Cargo.toml" --bin novu-rust-api
TEST_DATABASE_URL="$DATABASE_URL" TEST_API_BINARY="$API/target/debug/novu-rust-api" cargo test --locked --manifest-path "$WORKER/Cargo.toml" --test resend_webhook_key_probe_alert real_resend_bundle_failure_triggers_alert_and_recovers -- --ignored
TEST_DATABASE_URL="$DATABASE_URL" cargo test --locked --manifest-path "$WORKER/Cargo.toml" --test operator_key_probe_alert key_probe_triggers_coordinated_alert_and_recovery -- --ignored
