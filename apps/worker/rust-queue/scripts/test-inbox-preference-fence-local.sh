#!/usr/bin/env bash
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-inbox-preference-fence.XXXXXX")
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
export TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$ROOT/apps/worker/rust-queue/Cargo.toml" --bin migrate-worker -- apply api "$ROOT/apps/api/rust-api/migrations" --through 17
cargo run --quiet --locked --manifest-path "$ROOT/apps/worker/rust-queue/Cargo.toml" --bin migrate-worker -- apply worker "$ROOT/apps/worker/rust-queue/migrations"
cargo run --quiet --locked --manifest-path "$ROOT/apps/worker/rust-queue/Cargo.toml" --bin migrate-worker -- apply api "$ROOT/apps/api/rust-api/migrations"
cargo test --locked --manifest-path "$ROOT/apps/worker/rust-queue/Cargo.toml" --features test-mocks --lib in_app::preference_concurrency_tests::inbox_preference_commit_before_generation_is_observed -- --ignored --nocapture
cargo test --locked --manifest-path "$ROOT/apps/worker/rust-queue/Cargo.toml" --features test-mocks --lib in_app::preference_concurrency_tests::multiple_recipients_serialize_default_and_subscriber_preferences -- --ignored --nocapture
