#!/usr/bin/env bash
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd); WORKER="$ROOT/apps/worker/rust-queue"; API="$ROOT/apps/api/rust-api"
DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-fcm-topic.XXXXXX")
cleanup() {
 if [[ -f "$DIR/data/postmaster.pid" ]] && ! "$TEST_PG_BIN/pg_ctl" -D "$DIR/data" -m immediate -w stop >/dev/null 2>&1; then echo "Database retained at $DIR" >&2; return; fi
 python3 -c 'import shutil,sys; shutil.rmtree(sys.argv[1])' "$DIR"
}
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$DIR/data" -o "-h 127.0.0.1 -p $PORT -k $DIR" -l "$DIR/postgres.log" -w start >/dev/null
export DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0 SWALLOW_WORKER_TEST_TARGET_DIR="$WORKER/target"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
cargo test --locked --manifest-path "$API/Cargo.toml" --test fcm_event_time_workflow -- --ignored
