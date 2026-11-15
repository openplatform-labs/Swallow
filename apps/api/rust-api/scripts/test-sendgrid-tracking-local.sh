#!/usr/bin/env bash
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
API="$ROOT/apps/api/rust-api"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-sendgrid-tracking.XXXXXX")
cleanup() { "$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf "$TEST_DIR"; }
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$TEST_DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -o "-h 127.0.0.1 -p $PORT -k $TEST_DIR" -l "$TEST_DIR/postgres.log" start >/dev/null
export TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" CARGO_INCREMENTAL=0
WORKER="$ROOT/apps/worker/rust-queue"
export DATABASE_URL="$TEST_DATABASE_URL" NOVU_POSTGRES_ALLOW_INSECURE=true
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
cargo test --locked --manifest-path "$API/Cargo.toml" --test workflow_email_attachments sendgrid_tracking_reaches_mock_provider_with_layer_precedence -- --ignored
cargo test --locked --manifest-path "$API/Cargo.toml" --test workflow_email_attachments sendgrid_sandbox_workflow_records_validation_without_delivery -- --ignored
cargo test --locked --manifest-path "$API/Cargo.toml" --test workflow_email_attachments trigger_lookup_is_environment_scoped_and_distinguishes_missing_jobs -- --ignored
cargo test --locked --manifest-path "$API/Cargo.toml" --test operator_trigger_lookup -- --ignored
cargo test --locked --manifest-path "$API/Cargo.toml" --test trigger_key_guard -- --ignored
"$TEST_PG_BIN/createdb" -h 127.0.0.1 -p "$PORT" -U postgres trigger_contract
TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/trigger_contract" NOVU_ALLOW_RECREATED_SUBSCRIBER=true cargo test --locked --manifest-path "$API/Cargo.toml" --test api_postgres trigger_api_contract -- --ignored
