#!/usr/bin/env bash
# Disposable local PostgreSQL; no Docker or external provider.
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
API=$(cd "$(dirname "$0")/.." && pwd)
WORKER="$API/../../worker/rust-queue"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-operator-resend.XXXXXX")
cleanup() {
  "$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/pg" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$TEST_DIR"
}
trap cleanup EXIT
DB_PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$TEST_DIR/pg" -U postgres -A trust --no-locale >"$TEST_DIR/init.log" 2>&1
"$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/pg" -l "$TEST_DIR/pg.log" -o "-h 127.0.0.1 -p $DB_PORT" -w start >/dev/null
export DATABASE_URL="postgres://postgres@127.0.0.1:$DB_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
TEST_DATABASE_URL="$DATABASE_URL" cargo test --locked --manifest-path "$API/Cargo.toml" --test operator_smtp_transport_grants -- --ignored
