#!/usr/bin/env bash
# Creates and destroys a loopback-only disposable PostgreSQL cluster.
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
WORKER="$ROOT/apps/worker/rust-queue"
API="$ROOT/apps/api/rust-api"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-fcm-exponential.XXXXXX")
cleanup() {
  "$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$TEST_DIR"
}
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$TEST_DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -o "-h 127.0.0.1 -p $PORT -k $TEST_DIR" -l "$TEST_DIR/postgres.log" start >/dev/null
export DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
"$TEST_PG_BIN/createdb" -h 127.0.0.1 -p "$PORT" -U postgres queue_test
"$TEST_PG_BIN/createdb" -h 127.0.0.1 -p "$PORT" -U postgres guard_test
TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/queue_test" cargo test --locked --manifest-path "$WORKER/Cargo.toml" --lib multicast_attempts_recovery_and_tenant_isolation -- --ignored
TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/guard_test" cargo test --locked --manifest-path "$WORKER/Cargo.toml" --test fcm_routing_guard -- --ignored
"$TEST_PG_BIN/createdb" -h 127.0.0.1 -p "$PORT" -U postgres fence_test
export DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/fence_test" TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/fence_test"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --lib exponential_policy_fences_legacy_attempts_before_dispatch -- --ignored
TEST_DISPOSABLE_POSTGRES_LOG="$TEST_DIR/postgres.log" "$WORKER/scripts/test-fcm-exponential-legacy.sh"
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --test fcm_unrecorded_claim_health -- --ignored
