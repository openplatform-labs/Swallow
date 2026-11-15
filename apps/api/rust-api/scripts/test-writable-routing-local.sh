#!/usr/bin/env bash
# Creates and destroys two loopback-only PostgreSQL clusters; never use production data.
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL 17 bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
WORKER="$ROOT/apps/worker/rust-queue"
API="$ROOT/apps/api/rust-api"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-route.XXXXXX")
port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
PRIMARY_PORT=$(port)
STANDBY_PORT=$(port)
while [[ "$STANDBY_PORT" == "$PRIMARY_PORT" ]]; do STANDBY_PORT=$(port); done
cleanup() {
  "$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/standby" -m immediate stop >/dev/null 2>&1 || true
  "$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/primary" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$TEST_DIR"
}
trap cleanup EXIT
"$TEST_PG_BIN/initdb" -D "$TEST_DIR/primary" -U postgres -A trust >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/primary" -o "-h 127.0.0.1 -p $PRIMARY_PORT" -l "$TEST_DIR/primary.log" start >/dev/null
export DATABASE_URL="postgresql://postgres@127.0.0.1:$PRIMARY_PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
"$TEST_PG_BIN/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO public.novu_rust_users VALUES('routing-user');
INSERT INTO public.novu_rust_environments VALUES('routing-test','routing-org');
INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id)
VALUES(encode(sha256(convert_to('fixture-key','UTF8')),'hex'),'routing-test','routing-user');
SQL
"$TEST_PG_BIN/pg_basebackup" -h 127.0.0.1 -p "$PRIMARY_PORT" -U postgres -D "$TEST_DIR/standby" -R -X stream --checkpoint=fast
"$TEST_PG_BIN/pg_ctl" -D "$TEST_DIR/standby" -o "-h 127.0.0.1 -p $STANDBY_PORT" -l "$TEST_DIR/standby.log" start >/dev/null
TEST_PRIMARY_ADDR="127.0.0.1:$PRIMARY_PORT" TEST_STANDBY_ADDR="127.0.0.1:$STANDBY_PORT" \
 cargo test --locked --manifest-path "$API/Cargo.toml" --test api_readiness_writable writable_routing_handles_mixed_and_read_only_connections -- --ignored --nocapture
