#!/usr/bin/env bash
# Creates and destroys a loopback-only disposable PostgreSQL cluster.
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
WORKER="$ROOT/apps/worker/rust-queue"
API="$ROOT/apps/api/rust-api"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-resend-cooldown.XXXXXX")
cleanup() {
  "$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$TEST_DIR"
}
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$TEST_DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/data" -o "-h 127.0.0.1 -p $PORT -k $TEST_DIR" -l "$TEST_DIR/postgres.log" start >/dev/null
export DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --test resend_idempotency_policy workers_reject_conflicting_scopes_and_isolate_environments -- --ignored
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --lib shared_resend_cooldown_defers_other_jobs_and_fences_policy -- --ignored
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --test resend_idempotency_policy workers_reject_conflicting_shared_cooldown_settings -- --ignored
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --lib resend_cooldown_has_distinct_v2_conditions_and_transports
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --test resend_cooldown_health -- --ignored
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --test resend_cooldown_policy_admin -- --ignored
cargo test --locked --manifest-path "$WORKER/Cargo.toml" --lib rate_limit_header_is_recorded_on_the_attempt -- --ignored
