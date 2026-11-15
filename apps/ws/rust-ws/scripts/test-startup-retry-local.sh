#!/usr/bin/env bash
# Disposable local PostgreSQL, real WS processes and controlled startup failures.
set -euo pipefail
cd "$(dirname "$0")/.."
pgbin=${SWALLOW_TEST_POSTGRES_BIN:-/opt/homebrew/opt/postgresql@17/bin}
[[ -x "$pgbin/pg_ctl" && -x "$pgbin/initdb" ]] || { echo 'PostgreSQL server binaries required' >&2; exit 1; }
dir=$(mktemp -d "${TMPDIR:-/tmp}/swallow-ws-startup.XXXXXX"); ws_pid=
cleanup() {
  if [[ -n "$ws_pid" ]]; then kill "$ws_pid" 2>/dev/null || true; wait "$ws_pid" 2>/dev/null || true; fi
  if [[ -f "$dir/data/postmaster.pid" ]] && ! "$pgbin/pg_ctl" -D "$dir/data" -m immediate -w stop >/dev/null 2>&1; then echo "Could not stop test PostgreSQL; retained $dir" >&2; return; fi
  rm -rf "$dir"
}
trap cleanup EXIT
port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
pg_port=$(port); ws_port=$(port); while [[ "$pg_port" == "$ws_port" ]]; do ws_port=$(port); done
"$pgbin/initdb" -D "$dir/data" -U postgres -A trust --no-locale >/dev/null
"$pgbin/pg_ctl" -D "$dir/data" -o "-h 127.0.0.1 -p $pg_port" -l "$dir/postgres.log" -w start >/dev/null
for file in ../../worker/rust-queue/migrations/0002_experimental_in_app.sql ../../worker/rust-queue/migrations/0014_experimental_subscriber_objectid.sql ../../worker/rust-queue/migrations/0019_experimental_subscriber_online.sql migrations/0001_connections.sql migrations/0002_subscriber_revocation.sql migrations/0003_connection_leases.sql ../../worker/rust-queue/migrations/0030_experimental_ws_events.sql; do "$pgbin/psql" "postgres://postgres@127.0.0.1:$pg_port/postgres" -q -1 -v ON_ERROR_STOP=1 < "$file" >/dev/null; done
"$pgbin/pg_ctl" -D "$dir/data" -m fast -w stop >/dev/null
export CARGO_INCREMENTAL=0 NOVU_POSTGRES_ALLOW_INSECURE=true DATABASE_URL="postgres://postgres@127.0.0.1:$pg_port/postgres?connect_timeout=1" JWT_SECRET=startup-fixture-secret LISTEN_ADDR="127.0.0.1:$ws_port" NOVU_WS_ENABLE_HEALTH_ENDPOINTS=true
cargo build --quiet --locked --offline --bin novu-rust-ws
binary=target/debug/novu-rust-ws
if env -u SWALLOW_WS_STARTUP_RETRY_SECONDS "$binary" >"$dir/default.log" 2>&1; then echo 'default startup should fail when DB is down' >&2; exit 1; fi
if SWALLOW_WS_STARTUP_RETRY_SECONDS=301 "$binary" >"$dir/invalid.log" 2>&1; then echo 'invalid retry duration accepted' >&2; exit 1; fi
if NOVU_WS_LISTENER_RECONNECT_TIMEOUT_SECONDS=0 "$binary" >"$dir/invalid-listener.log" 2>&1; then echo 'invalid listener timeout accepted' >&2; exit 1; fi
rg -q 'invalid WS listener reconnect timeout' "$dir/invalid-listener.log"
if SWALLOW_WS_STARTUP_RETRY_SECONDS=2 "$binary" >"$dir/deadline.log" 2>&1; then echo 'retry deadline should fail' >&2; exit 1; fi
rg -q 'retry deadline exceeded' "$dir/deadline.log"
SWALLOW_WS_STARTUP_RETRY_SECONDS=30 "$binary" >"$dir/terminate.log" 2>&1 & ws_pid=$!
sleep 0.3; kill -0 "$ws_pid"; kill -TERM "$ws_pid"
for ((i=0;i<40;i++)); do kill -0 "$ws_pid" 2>/dev/null || break; ((i<39)) || { echo 'termination did not interrupt startup' >&2; exit 1; }; sleep 0.1; done
wait "$ws_pid" || true; ws_pid=; rg -q 'startup interrupted' "$dir/terminate.log"
SWALLOW_WS_STARTUP_RETRY_SECONDS=20 "$binary" >"$dir/recovery.log" 2>&1 & ws_pid=$!
sleep 1; kill -0 "$ws_pid"
if curl -fsS --max-time 1 "http://127.0.0.1:$ws_port/health/ready" >/dev/null 2>&1; then echo 'ready before database startup' >&2; exit 1; fi
"$pgbin/pg_ctl" -D "$dir/data" -o "-h 127.0.0.1 -p $pg_port" -l "$dir/postgres.log" -w start >/dev/null
for ((i=0;i<100;i++)); do curl -fsS --max-time 1 "http://127.0.0.1:$ws_port/health/ready" >/dev/null 2>&1 && break; kill -0 "$ws_pid"; ((i<99)) || { echo 'WS did not become ready' >&2; exit 1; }; sleep 0.1; done
kill -TERM "$ws_pid"; wait "$ws_pid"; ws_pid=
TEST_PSQL="$pgbin/psql" TEST_PG_URL="postgres://postgres@127.0.0.1:$pg_port/postgres" TEST_PG_PORT="$pg_port" TEST_WS_BINARY="$(pwd)/$binary" node tests/startup_listener_retry.cjs
TEST_PSQL="$pgbin/psql" TEST_PG_URL="postgres://postgres@127.0.0.1:$pg_port/postgres" TEST_PG_PORT="$pg_port" TEST_WS_BINARY="$(pwd)/$binary" node tests/runtime_listener_retry.cjs
echo 'WS startup retry, deadline, termination and database recovery passed.'
