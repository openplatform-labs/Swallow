#!/usr/bin/env bash
# Creates and destroys a loopback-only disposable PostgreSQL cluster.
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
WORKER="$ROOT/apps/worker/rust-queue"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-smtp-resolution.XXXXXX")
cleanup() {
  "$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$TEST_DIR"
}
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$TEST_DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -o "-h 127.0.0.1 -p $PORT -k $TEST_DIR" -l "$TEST_DIR/postgres.log" start >/dev/null
export TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" CARGO_INCREMENTAL=0
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --test smtp_resolution -- --ignored

"$TEST_PG_BIN/createdb" -h 127.0.0.1 -p "$PORT" -U postgres smtp_history_test
export TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/smtp_history_test"
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --test smtp_resolution_history -- --ignored

# The process test needs the complete schema in a separate disposable database.
"$TEST_PG_BIN/createdb" -h 127.0.0.1 -p "$PORT" -U postgres smtp_worker_test
export DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/smtp_worker_test" TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/smtp_worker_test" NOVU_POSTGRES_ALLOW_INSECURE=true
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$ROOT/apps/api/rust-api/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$ROOT/apps/api/rust-api/migrations"
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --test smtp_resolution_worker -- --ignored
