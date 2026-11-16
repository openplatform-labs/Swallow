#!/usr/bin/env bash
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd); WORKER="$ROOT/apps/worker/rust-queue"; API="$ROOT/apps/api/rust-api"
DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-telegram.XXXXXX")
cleanup(){ if [[ -f "$DIR/data/postmaster.pid" ]] && ! "$TEST_PG_BIN/pg_ctl" -D "$DIR/data" -m immediate -w stop >/dev/null 2>&1; then echo "Database retained at $DIR" >&2; return; fi; python3 -c 'import shutil,sys; shutil.rmtree(sys.argv[1])' "$DIR"; }
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$DIR/data" -o "-h 127.0.0.1 -p $PORT -k $DIR" -l "$DIR/postgres.log" -w start >/dev/null
export DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --features test-mocks --bin telegram-worker
cp "$WORKER/target/debug/telegram-worker" "$DIR/telegram-worker-fixture"
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin telegram-worker --bin telegram-enqueue --bin delivery-reconcile
TEST_TELEGRAM_ENQUEUE="$WORKER/target/debug/telegram-enqueue" TEST_PSQL="$TEST_PG_BIN/psql" TEST_TELEGRAM_WORKER="$DIR/telegram-worker-fixture" python3 "$WORKER/tests/telegram_worker_process.py"
TEST_PSQL="$TEST_PG_BIN/psql" TEST_TELEGRAM_WORKER="$DIR/telegram-worker-fixture" TEST_DELIVERY_CLI="$WORKER/target/debug/delivery-reconcile" python3 "$WORKER/tests/telegram_operator_process.py"
TEST_PSQL="$TEST_PG_BIN/psql" TEST_TELEGRAM_WORKER="$DIR/telegram-worker-fixture" python3 "$WORKER/tests/telegram_scoped_worker_process.py"
