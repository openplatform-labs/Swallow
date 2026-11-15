#!/usr/bin/env bash
# Single-host synchronous PostgreSQL promotion with two Rust WS processes and SDK clients.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${NOVU_SDK_MODULE:?Set NOVU_SDK_MODULE to an absolute path to @novu/js}"
pgbin=${SWALLOW_TEST_POSTGRES_BIN:-/opt/homebrew/opt/postgresql@17/bin}
[[ -x "$pgbin/pg_ctl" && -x "$pgbin/pg_basebackup" ]] || { echo 'PostgreSQL server binaries required' >&2; exit 1; }
testdir=$(mktemp -d "${TMPDIR:-/tmp}/swallow-ws-promotion.XXXXXX")
api_pid=; worker_pid=; ws1_pid=; ws2_pid=
cleanup() {
  for pid in "$ws1_pid" "$ws2_pid"; do [[ -z "$pid" ]] || kill -CONT "$pid" 2>/dev/null || true; done
  for pid in "$api_pid" "$worker_pid" "$ws1_pid" "$ws2_pid"; do [[ -z "$pid" ]] || kill "$pid" 2>/dev/null || true; done
  for pid in "$api_pid" "$worker_pid" "$ws1_pid" "$ws2_pid"; do [[ -z "$pid" ]] || wait "$pid" 2>/dev/null || true; done
  local running=0
  for data in "$testdir/standby" "$testdir/primary"; do if [[ -f "$data/postmaster.pid" ]]; then "$pgbin/pg_ctl" -D "$data" -m immediate -w stop >/dev/null 2>&1 || running=1; fi; done
  if ((running)); then echo "Could not stop test PostgreSQL; retained $testdir" >&2; return; fi
  rm -rf "$testdir"
}
trap cleanup EXIT
port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
primary_port=$(port); standby_port=$(port); api_port=$(port); ws1_port=$(port); ws2_port=$(port)
while [[ $(printf '%s\n' "$primary_port" "$standby_port" "$api_port" "$ws1_port" "$ws2_port" | sort -u | wc -l | tr -d ' ') != 5 ]]; do standby_port=$(port); api_port=$(port); ws1_port=$(port); ws2_port=$(port); done
"$pgbin/initdb" -D "$testdir/primary" -U postgres -A trust --no-locale >/dev/null
"$pgbin/pg_ctl" -D "$testdir/primary" -o "-h 127.0.0.1 -p $primary_port -k $testdir -c wal_level=replica -c max_wal_senders=10" -l "$testdir/primary.log" -w start >/dev/null
export DATABASE_URL="postgres://postgres@127.0.0.1:$primary_port/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --offline --manifest-path ../../worker/rust-queue/Cargo.toml --bin migrate-worker -- apply api ../../api/rust-api/migrations --through 17
cargo run --quiet --locked --offline --manifest-path ../../worker/rust-queue/Cargo.toml --bin migrate-worker -- apply worker ../../worker/rust-queue/migrations
cargo run --quiet --locked --offline --manifest-path ../../worker/rust-queue/Cargo.toml --bin migrate-worker -- apply api ../../api/rust-api/migrations
for file in migrations/*.sql; do "$pgbin/psql" "$DATABASE_URL" -q -1 -v ON_ERROR_STOP=1 < "$file" >/dev/null; done
"$pgbin/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO novu_rust_users VALUES('ha-user'); INSERT INTO novu_rust_environments VALUES('ha-env','org');
INSERT INTO novu_rust_api_keys(key_hash,environment_id,user_id) VALUES('4a453798dcb87778c40d2f5daceb0f28cf67e559b7bfbe865878995c64e575fe','ha-env','ha-user');
INSERT INTO novu_rust_inbox_environments(environment_id,application_identifier,in_app_active,hmac_required,is_development,remove_novu_branding,max_snooze_duration_hours) VALUES('ha-env','ha-app',true,false,true,false,24);
INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,inbox_metadata) VALUES('ha-env','greeting',true,true,'hello HA','{"id":"64a000000000000000000004","identifier":"greeting","name":"Greeting","critical":false,"tags":[],"severity":"none","payloadAsData":false}');
SQL
"$pgbin/pg_basebackup" -h 127.0.0.1 -p "$primary_port" -U postgres -D "$testdir/standby" -R -X stream -c fast
printf "primary_conninfo = 'host=127.0.0.1 port=%s user=postgres application_name=standby1'\n" "$primary_port" >> "$testdir/standby/postgresql.auto.conf"
"$pgbin/pg_ctl" -D "$testdir/standby" -o "-h 127.0.0.1 -p $standby_port -k $testdir" -l "$testdir/standby.log" -w start >/dev/null
for ((i=0;i<60;i++)); do [[ $("$pgbin/psql" "$DATABASE_URL" -Atc "SELECT count(*) FROM pg_stat_replication WHERE application_name='standby1' AND state='streaming'") == 1 ]] && break; ((i<59)) || exit 1; sleep 1; done
"$pgbin/psql" "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (standby1)'" >/dev/null
"$pgbin/psql" "$DATABASE_URL" -v ON_ERROR_STOP=1 -c 'SELECT pg_reload_conf()' >/dev/null
for ((i=0;i<60;i++)); do [[ $("$pgbin/psql" "$DATABASE_URL" -Atc "SELECT count(*) FROM pg_stat_replication WHERE application_name='standby1' AND sync_state='sync'") == 1 ]] && break; ((i<59)) || exit 1; sleep 1; done
cargo build --quiet --locked --offline --manifest-path ../../api/rust-api/Cargo.toml --bin novu-rust-api
cargo build --quiet --locked --offline --manifest-path ../../worker/rust-queue/Cargo.toml --bin in-app-worker
cargo build --quiet --locked --offline --bin novu-rust-ws
export DATABASE_URL="postgres://postgres@127.0.0.1:$standby_port,127.0.0.1:$primary_port/postgres?target_session_attrs=read-write&connect_timeout=2" JWT_SECRET=local-ha-fixture-secret
LISTEN_ADDR="127.0.0.1:$api_port" SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true SWALLOW_API_DB_HEALTH_INTERVAL_SECONDS=1 SWALLOW_API_DB_HEALTH_TIMEOUT_SECONDS=2 ../../api/rust-api/target/debug/novu-rust-api >"$testdir/api.log" 2>&1 & api_pid=$!
../../worker/rust-queue/target/debug/in-app-worker >"$testdir/worker.log" 2>&1 & worker_pid=$!
LISTEN_ADDR="127.0.0.1:$ws1_port" NOVU_WS_DISCONNECT_ON_EVENT_GAP=true NOVU_WS_EVENT_LOOKUP_RECOVERY=probe NOVU_WS_ENABLE_HEALTH_ENDPOINTS=true target/debug/novu-rust-ws >"$testdir/ws1.log" 2>&1 & ws1_pid=$!
LISTEN_ADDR="127.0.0.1:$ws2_port" NOVU_WS_DISCONNECT_ON_EVENT_GAP=true NOVU_WS_EVENT_LOOKUP_RECOVERY=probe NOVU_WS_ENABLE_HEALTH_ENDPOINTS=true target/debug/novu-rust-ws >"$testdir/ws2.log" 2>&1 & ws2_pid=$!
for ((i=0;i<100;i++)); do if curl -fsS "http://127.0.0.1:$api_port/health/ready" >/dev/null 2>&1 && curl -fsS "http://127.0.0.1:$ws1_port/health/ready" >/dev/null 2>&1 && curl -fsS "http://127.0.0.1:$ws2_port/health/ready" >/dev/null 2>&1; then break; fi; for pid in "$api_pid" "$worker_pid" "$ws1_pid" "$ws2_pid"; do kill -0 "$pid"; done; ((i<99)) || exit 1; sleep 0.1; done
NOVU_RUST_URL="http://127.0.0.1:$api_port" RUST_WS_ONE="http://127.0.0.1:$ws1_port" RUST_WS_TWO="http://127.0.0.1:$ws2_port" TEST_PG_CTL="$pgbin/pg_ctl" TEST_PRIMARY_DATA="$testdir/primary" TEST_STANDBY_DATA="$testdir/standby" TEST_PSQL="$pgbin/psql" TEST_STANDBY_URL="postgres://postgres@127.0.0.1:$standby_port/postgres" TEST_WS1_PID="$ws1_pid" TEST_WS2_PID="$ws2_pid" node tests/sdk_promotion_e2e.cjs
