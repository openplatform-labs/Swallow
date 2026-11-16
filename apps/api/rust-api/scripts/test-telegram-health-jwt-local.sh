#!/usr/bin/env bash
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-telegram-health-jwt.XXXXXX")
cleanup() {
  if [[ -f "$DIR/data/postmaster.pid" ]] && ! "$TEST_PG_BIN/pg_ctl" -D "$DIR/data" -m immediate -w stop >/dev/null 2>&1; then
    echo "Database retained at $DIR" >&2; return
  fi
  python3 -c 'import shutil,sys; shutil.rmtree(sys.argv[1])' "$DIR"
}
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$DIR/data" -o "-h 127.0.0.1 -p $PORT -k $DIR" -l "$DIR/postgres.log" -w start >/dev/null
TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" CARGO_INCREMENTAL=0 cargo test --locked --manifest-path "$ROOT/apps/api/rust-api/Cargo.toml" --test operator_telegram_health -- --ignored --nocapture
