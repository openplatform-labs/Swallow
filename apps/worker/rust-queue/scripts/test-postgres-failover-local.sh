#!/usr/bin/env bash
# Disposable loopback-only PostgreSQL pair; stops only its own temporary primary.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(cd ../../.. && pwd)
scenario=${SWALLOW_LOCAL_FAILOVER_SCENARIO:-alert-binding}
case "$scenario" in alert-binding|alert-scheduler|alert-v2|resend|twilio|fcm|fcm-delay) ;; *) echo 'invalid local failover scenario' >&2; exit 2;; esac
pgbin=${SWALLOW_TEST_POSTGRES_BIN:-/opt/homebrew/opt/postgresql@17/bin}
[[ -x "$pgbin/pg_ctl" && -x "$pgbin/pg_basebackup" ]] || { echo 'PostgreSQL 17 server binaries are required' >&2; exit 1; }
testdir=$(mktemp -d /private/tmp/swallow-alert-ha.XXXXXX)
cleanup() {
  "$pgbin/pg_ctl" -D "$testdir/standby" -m immediate stop >/dev/null 2>&1 || true
  "$pgbin/pg_ctl" -D "$testdir/primary" -m immediate stop >/dev/null 2>&1 || true
  python3 -c 'import shutil,sys; shutil.rmtree(sys.argv[1])' "$testdir"
}
trap cleanup EXIT
port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
primary_port=$(port); standby_port=$(port)
[[ "$primary_port" != "$standby_port" ]] || standby_port=$(port)
"$pgbin/initdb" -D "$testdir/primary" -U postgres -A trust --no-locale >/dev/null
"$pgbin/pg_ctl" -D "$testdir/primary" -o "-h 127.0.0.1 -p $primary_port -k $testdir -c wal_level=replica -c max_wal_senders=10" -l "$testdir/primary.log" start >/dev/null
export DATABASE_URL="postgres://postgres@127.0.0.1:$primary_port/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --bin migrate-worker -- apply api "$root/apps/api/rust-api/migrations" --through 17
cargo run --quiet --locked --bin migrate-worker -- apply worker migrations
cargo run --quiet --locked --bin migrate-worker -- apply api "$root/apps/api/rust-api/migrations"
"$pgbin/pg_basebackup" -h 127.0.0.1 -p "$primary_port" -U postgres -D "$testdir/standby" -R -X stream -c fast
printf "primary_conninfo = 'host=127.0.0.1 port=%s user=postgres application_name=standby1'\n" "$primary_port" >> "$testdir/standby/postgresql.auto.conf"
"$pgbin/pg_ctl" -D "$testdir/standby" -o "-h 127.0.0.1 -p $standby_port -k $testdir" -l "$testdir/standby.log" start >/dev/null
for ((i=0;i<60;i++)); do
  if [[ "$("$pgbin/psql" "$DATABASE_URL" -Atc "SELECT count(*) FROM pg_stat_replication WHERE application_name='standby1' AND state='streaming'")" == 1 ]]; then break; fi
  ((i<59)) || { echo 'standby did not start streaming' >&2; exit 1; }
  sleep 1
done
"$pgbin/psql" "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (standby1)'" >/dev/null
"$pgbin/psql" "$DATABASE_URL" -v ON_ERROR_STOP=1 -c 'SELECT pg_reload_conf()' >/dev/null
for ((i=0;i<60;i++)); do
  if [[ "$("$pgbin/psql" "$DATABASE_URL" -Atc "SELECT count(*) FROM pg_stat_replication WHERE application_name='standby1' AND sync_state='sync'")" == 1 ]]; then break; fi
  ((i<59)) || { echo 'standby did not become synchronous' >&2; exit 1; }
  sleep 1
done
export TEST_PRIMARY_URL="$DATABASE_URL" TEST_STANDBY_URL="postgres://postgres@127.0.0.1:$standby_port/postgres"
export TEST_FAILOVER_URL="postgres://postgres@127.0.0.1:$standby_port,127.0.0.1:$primary_port/postgres?target_session_attrs=read-write&connect_timeout=2"
export TEST_LOCAL_PRIMARY_DATA="$testdir/primary" TEST_LOCAL_STANDBY_DATA="$testdir/standby" TEST_LOCAL_PG_CTL="$pgbin/pg_ctl"
if [[ "$scenario" == resend ]]; then
  cargo build --quiet --locked --bin resend-worker
  target_dir=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
  export TEST_RESEND_WORKER_BINARY="$target_dir/debug/resend-worker"
  cargo test --locked --lib resend_failover_test::synchronous_failover_during_resend_dispatch_preserves_unknown -- --ignored
elif [[ "$scenario" == fcm || "$scenario" == fcm-delay ]]; then
  cargo build --quiet --locked --bin fcm-worker
  target_dir=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
  export TEST_FCM_WORKER_BINARY="$target_dir/debug/fcm-worker"
  if [[ "$scenario" == fcm-delay ]]; then
    cargo test --locked --lib fcm_failover_test::synchronous_failover_preserves_fcm_delayed_continuation -- --ignored
  else
    cargo test --locked --lib fcm_failover_test::synchronous_failover_during_fcm_dispatch_preserves_unknown -- --ignored
  fi
elif [[ "$scenario" == twilio ]]; then
  cargo build --quiet --locked --bin twilio-worker
  cargo build --quiet --locked --manifest-path "$root/apps/api/rust-api/Cargo.toml" --bin novu-rust-api
  worker_target=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
  api_target=$(cargo metadata --format-version 1 --no-deps --manifest-path "$root/apps/api/rust-api/Cargo.toml" | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
  export TEST_TWILIO_WORKER_BINARY="$worker_target/debug/twilio-worker" TEST_TWILIO_WEBHOOK_API_BINARY="$api_target/debug/novu-rust-api"
  cargo test --locked --lib twilio_failover_test::synchronous_failover_during_twilio_dispatch_preserves_unknown -- --ignored
elif [[ "$scenario" == alert-v2 ]]; then
  cargo test --locked --test alert_v2_failover -- --ignored
elif [[ "$scenario" == alert-scheduler ]]; then
  cargo test --locked --test alert_scheduler_failover -- --ignored
else
  cargo test --locked --test alert_binding_failover -- --ignored
fi
