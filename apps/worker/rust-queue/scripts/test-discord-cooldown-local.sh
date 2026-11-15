#!/usr/bin/env bash
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
WORKER="$ROOT/apps/worker/rust-queue"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-discord-cooldown.XXXXXX")
cleanup() { "$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf "$TEST_DIR"; }
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$TEST_DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -o "-h 127.0.0.1 -p $PORT -k $TEST_DIR" -l "$TEST_DIR/postgres.log" start >/dev/null
export CARGO_INCREMENTAL=0
for test in discord_shared_cooldown discord_rate_limit discord_retry_policy discord_queue; do
  "$TEST_PG_BIN/createdb" -h 127.0.0.1 -p "$PORT" -U postgres "$test"
  export TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/$test"
  cargo test --locked --features test-mocks --manifest-path "$WORKER/Cargo.toml" --test "$test" -- --ignored
done
