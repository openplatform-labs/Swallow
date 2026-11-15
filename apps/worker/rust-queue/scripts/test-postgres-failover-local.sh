#!/usr/bin/env bash
# Disposable loopback-only PostgreSQL pair; stops only its own temporary primary.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(cd ../../.. && pwd)
scenario=${SWALLOW_LOCAL_FAILOVER_SCENARIO:-alert-binding}
case "$scenario" in alert-binding|alert-scheduler|alert-v2|alert-v3|alert-timing|trigger-key|trigger-inflight|execution-grant-inflight|sendgrid-inflight|sendgrid-sandbox-inflight|smtp|smtp-rls|smtp-plain|smtp-plain-rls|smtp-plain-fenced|smtp-policy|smtp-auto-policy|smtp-auto-inflight|smtp-resolution|discord-manual-retry|discord-cooldown|discord-inflight|in-app|resend|resend-cooldown|resend-invalid|resend-replay-inflight|resend-reconciliation|twilio|fcm|fcm-delay|fcm-exp|fcm-exp-repeat|fcm-review) ;; *) echo 'invalid local failover scenario' >&2; exit 2;; esac
pgbin=${SWALLOW_TEST_POSTGRES_BIN:-/opt/homebrew/opt/postgresql@17/bin}
[[ -x "$pgbin/pg_ctl" && -x "$pgbin/pg_basebackup" ]] || { echo 'PostgreSQL 17 server binaries are required' >&2; exit 1; }
testdir=$(mktemp -d /private/tmp/swallow-alert-ha.XXXXXX)
cleanup() {
  local running=0
  for data in "$testdir/standby" "$testdir/primary"; do
    if [[ -f "$data/postmaster.pid" ]]; then
      "$pgbin/pg_ctl" -D "$data" -m immediate -w stop >/dev/null 2>&1 || running=1
    fi
  done
  if ((running)); then echo "Could not stop a test database; retained $testdir" >&2; return; fi
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
if [[ "$scenario" == alert-timing ]]; then
  "$root/apps/worker/rust-queue/scripts/test-alert-timing-failover.sh" "$root" "$testdir" "$pgbin"
elif [[ "$scenario" == trigger-key || "$scenario" == trigger-inflight ]]; then
  export TEST_TRIGGER_INFLIGHT=$([[ "$scenario" == trigger-inflight ]] && printf true || printf false)
  cargo test --locked --manifest-path "$root/apps/api/rust-api/Cargo.toml" --test trigger_key_failover -- --ignored --nocapture
elif [[ "$scenario" == execution-grant-inflight ]]; then
  cargo test --locked --manifest-path "$root/apps/api/rust-api/Cargo.toml" --test execution_grant_failover -- --ignored --nocapture
elif [[ "$scenario" == sendgrid-inflight || "$scenario" == sendgrid-sandbox-inflight ]]; then
  export TEST_SENDGRID_SANDBOX=$([[ "$scenario" == sendgrid-sandbox-inflight ]] && printf true || printf false)
  cargo test --locked --features test-mocks --test sendgrid_inflight_failover -- --ignored --nocapture
elif [[ "$scenario" == discord-inflight ]]; then
  cargo test --locked --features test-mocks --test discord_inflight_failover -- --ignored --nocapture
elif [[ "$scenario" == discord-cooldown ]]; then
  cargo test --locked --features test-mocks --test discord_cooldown_failover -- --ignored --nocapture
elif [[ "$scenario" == discord-manual-retry ]]; then
  cargo test --locked --features test-mocks --test discord_manual_retry_failover -- --ignored --nocapture
elif [[ "$scenario" == smtp-auto-policy || "$scenario" == smtp-auto-inflight ]]; then
  cargo build --quiet --locked --manifest-path "$root/apps/api/rust-api/Cargo.toml" --bin novu-rust-api
  cargo build --quiet --locked --bin smtp-auto-retry-worker
  TEST_SMTP_AUTO_INFLIGHT=$([[ "$scenario" == smtp-auto-inflight ]] && printf true || printf false) TEST_API_BINARY="$root/apps/api/rust-api/target/debug/novu-rust-api" TEST_AUTO_WORKER="$root/apps/worker/rust-queue/target/debug/smtp-auto-retry-worker" TEST_PSQL="$pgbin/psql" TEST_FIXTURE_DIR="$testdir" TEST_ROOT="$root" python3 tests/smtp_auto_policy_failover.py
elif [[ "$scenario" == smtp-policy ]]; then
  cargo build --quiet --locked --manifest-path "$root/apps/api/rust-api/Cargo.toml" --bin novu-rust-api
  TEST_API_BINARY="$root/apps/api/rust-api/target/debug/novu-rust-api" TEST_PSQL="$pgbin/psql" TEST_FIXTURE_DIR="$testdir" TEST_ROOT="$root" python3 tests/smtp_transport_policy_failover.py
elif [[ "$scenario" == smtp-resolution ]]; then
  cargo test --locked --test smtp_resolution_failover -- --ignored --nocapture
elif [[ "$scenario" == smtp || "$scenario" == smtp-rls || "$scenario" == smtp-plain || "$scenario" == smtp-plain-rls || "$scenario" == smtp-plain-fenced ]]; then
  TEST_SMTP_FAILOVER_RLS=$([[ "$scenario" == smtp-rls || "$scenario" == smtp-plain-rls || "$scenario" == smtp-plain-fenced ]] && printf true || printf false) TEST_SMTP_FAILOVER_PLAIN=$([[ "$scenario" == smtp-plain || "$scenario" == smtp-plain-rls || "$scenario" == smtp-plain-fenced ]] && printf true || printf false) TEST_SMTP_FAILOVER_FENCE=$([[ "$scenario" == smtp-plain-fenced ]] && printf true || printf false) cargo test --locked --test smtp_failover -- --ignored --nocapture
elif [[ "$scenario" == in-app ]]; then
  cargo test --locked --test postgres_failover -- --ignored
elif [[ "$scenario" == resend-replay-inflight ]]; then
  cargo build --quiet --locked --features test-mocks --bin resend-worker
  cp target/debug/resend-worker "$testdir/resend-worker-mock"
  cargo build --quiet --locked --bin resend-worker
  TEST_RESEND_WORKER="$testdir/resend-worker-mock" TEST_PSQL="$pgbin/psql" TEST_FIXTURE_DIR="$testdir" python3 tests/resend_replay_failover.py
elif [[ "$scenario" == resend-reconciliation ]]; then
  cargo build --quiet --locked --manifest-path "$root/apps/api/rust-api/Cargo.toml" --bin novu-rust-api
  TEST_API_BINARY="$root/apps/api/rust-api/target/debug/novu-rust-api" TEST_PSQL="$pgbin/psql" TEST_FIXTURE_DIR="$testdir" TEST_ROOT="$root" python3 tests/resend_reconciliation_failover.py
elif [[ "$scenario" == resend-invalid ]]; then
  cargo build --quiet --locked --bin resend-worker
  target_dir=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
  export TEST_RESEND_WORKER_BINARY="$target_dir/debug/resend-worker"
  cargo test --locked --lib resend_failover_test::synchronous_failover_during_invalid_resend_preparation_rolls_back_attempt -- --ignored
elif [[ "$scenario" == resend || "$scenario" == resend-cooldown ]]; then
  cargo build --quiet --locked --bin resend-worker
  target_dir=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
  export TEST_RESEND_WORKER_BINARY="$target_dir/debug/resend-worker"
  if [[ "$scenario" == resend-cooldown ]]; then
    cargo test --locked --lib resend_failover_test::synchronous_failover_preserves_resend_shared_cooldown -- --ignored
  else
    cargo build --quiet --locked --bin resend-projection-worker
    cargo build --quiet --locked --manifest-path "$root/apps/api/rust-api/Cargo.toml" --bin novu-rust-api
    api_target=$(cargo metadata --format-version 1 --no-deps --manifest-path "$root/apps/api/rust-api/Cargo.toml" | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
    export TEST_RESEND_FAILOVER_API_BINARY="$api_target/debug/novu-rust-api" TEST_RESEND_FAILOVER_PROJECTION_BINARY="$target_dir/debug/resend-projection-worker"
    cargo test --locked --lib resend_failover_test::synchronous_failover_during_resend_dispatch_preserves_unknown -- --ignored
  fi
elif [[ "$scenario" == fcm || "$scenario" == fcm-delay || "$scenario" == fcm-exp || "$scenario" == fcm-exp-repeat ]]; then
  cargo build --quiet --locked --bin fcm-worker
  target_dir=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
  export TEST_FCM_WORKER_BINARY="$target_dir/debug/fcm-worker"
  if [[ "$scenario" == fcm-exp-repeat ]]; then
    TEST_FCM_EXP_REPEATED=true cargo test --locked --lib fcm_failover_test::synchronous_failover_preserves_fcm_exponential_503_continuation -- --ignored
  elif [[ "$scenario" == fcm-exp ]]; then
    cargo test --locked --lib fcm_failover_test::synchronous_failover_preserves_fcm_exponential_503_continuation -- --ignored
  elif [[ "$scenario" == fcm-delay ]]; then
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
elif [[ "$scenario" == fcm-review ]]; then
  cargo test --locked --test fcm_review_failover -- --ignored
elif [[ "$scenario" == alert-v3 ]]; then
  cargo test --locked --test alert_v3_failover -- --ignored
elif [[ "$scenario" == alert-v2 ]]; then
  cargo test --locked --test alert_v2_failover -- --ignored
elif [[ "$scenario" == alert-scheduler ]]; then
  cargo test --locked --test alert_scheduler_failover -- --ignored
else
  cargo test --locked --test alert_binding_failover -- --ignored
fi
