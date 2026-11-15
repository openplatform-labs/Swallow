#!/usr/bin/env bash
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-sendgrid-preparation.XXXXXX")
cleanup() { "$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf "$TEST_DIR"; }
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$TEST_DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -o "-h 127.0.0.1 -p $PORT -k $TEST_DIR" -l "$TEST_DIR/postgres.log" start >/dev/null
export TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$ROOT/apps/worker/rust-queue/Cargo.toml" --bin migrate-worker -- apply api "$ROOT/apps/api/rust-api/migrations" --through 17
cargo run --quiet --locked --manifest-path "$ROOT/apps/worker/rust-queue/Cargo.toml" --bin migrate-worker -- apply worker "$ROOT/apps/worker/rust-queue/migrations"
cargo test --locked --manifest-path "$ROOT/apps/worker/rust-queue/Cargo.toml" --lib invalid_request_is_rejected_before_dispatch_reservation -- --ignored
cargo test --locked --manifest-path "$ROOT/apps/worker/rust-queue/Cargo.toml" --lib sendgrid::tests --
