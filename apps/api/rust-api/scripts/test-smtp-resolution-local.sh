#!/usr/bin/env bash
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-smtp-api-resolution.XXXXXX")
cleanup() { "$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf "$TEST_DIR"; }
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$TEST_DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -o "-h 127.0.0.1 -p $PORT -k $TEST_DIR" -l "$TEST_DIR/postgres.log" start >/dev/null
export TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" CARGO_INCREMENTAL=0
cargo test --locked --manifest-path "$ROOT/apps/api/rust-api/Cargo.toml" --test operator_smtp_resolution -- --ignored

"$TEST_PG_BIN/createdb" -h 127.0.0.1 -p "$PORT" -U postgres smtp_grants_test
export TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/smtp_grants_test"
cargo test --locked --manifest-path "$ROOT/apps/api/rust-api/Cargo.toml" --test operator_smtp_resolution_grants -- --ignored

"$TEST_PG_BIN/createdb" -h 127.0.0.1 -p "$PORT" -U postgres smtp_history_test
export TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/smtp_history_test"
cargo test --locked --manifest-path "$ROOT/apps/api/rust-api/Cargo.toml" --test operator_smtp_resolution_history -- --ignored

"$TEST_PG_BIN/createdb" -h 127.0.0.1 -p "$PORT" -U postgres smtp_review_test
export TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/smtp_review_test"
cargo test --locked --manifest-path "$ROOT/apps/api/rust-api/Cargo.toml" --test operator_smtp_review -- --ignored
