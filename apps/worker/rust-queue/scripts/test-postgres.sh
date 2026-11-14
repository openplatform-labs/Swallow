#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Legacy synthetic job fixtures do not have recipient identity snapshots.
export NOVU_ALLOW_RECREATED_SUBSCRIBER=true
export NOVU_POSTGRES_ALLOW_INSECURE=true
name="novu-rust-queue-test-$$"
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
# The container is disposable; access is limited to a random loopback port.
docker run --rm -d --name "$name" -e POSTGRES_HOST_AUTH_METHOD=trust \
  -e POSTGRES_DB=novu_queue_test -p "127.0.0.1:${port}:5432" postgres:17-alpine >/dev/null
for ((i=0; i<30; i++)); do
  if docker exec "$name" pg_isready -q -U postgres -d novu_queue_test; then
    break
  fi
  if (( i == 29 )); then
    echo 'PostgreSQL did not become ready' >&2
    exit 1
  fi
  sleep 1
done
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_queue_test" \
  cargo test --locked --test postgres --test in_app_recovery -- --ignored
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_queue_test" \
  cargo test --locked --lib sendgrid_queue::tests::email_attempt_recovery_and_tenant_isolation -- --ignored
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_queue_test" \
  cargo test --locked --lib twilio_queue::tests::sms_attempt_recovery_and_tenant_isolation -- --ignored
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_queue_test" \
  cargo test --locked --lib fcm_queue::tests::multicast_attempts_recovery_and_tenant_isolation -- --ignored
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_queue_test" \
  cargo test --locked --test reconciliation -- --ignored
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_queue_test" \
  cargo test --locked --lib resend_queue::tests::standalone_dispatch_and_recovery -- --ignored

# Exercise strict actor fencing in a fresh database, independent of legacy fixtures.
docker exec "$name" createdb -U postgres novu_actor_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_actor_test" \
  NOVU_ALLOW_RECREATED_SUBSCRIBER=false cargo test --locked --lib actor_delivery_tests::changed_actor_is_rejected_by_all_channels -- --ignored

# Resend workflow path in strict identity mode on a fresh fixture database.
docker exec "$name" createdb -U postgres novu_resend_workflow_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_resend_workflow_test" \
  NOVU_ALLOW_RECREATED_SUBSCRIBER=false cargo test --locked --lib resend_queue::tests::workflow_delivery_revalidates_subscriber -- --ignored

docker exec "$name" createdb -U postgres novu_resend_alert_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_resend_alert_test" \
  cargo test --locked --test resend_alert -- --ignored

docker exec "$name" createdb -U postgres novu_twilio_secret_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_twilio_secret_test" \
  cargo test --locked --test twilio_secret_files -- --ignored

docker exec "$name" createdb -U postgres novu_alert_delivery_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_alert_delivery_test" \
  cargo test --locked --test alerts_postgres -- --ignored

docker exec "$name" createdb -U postgres novu_alert_transport_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_alert_transport_test" \
  cargo test --locked --lib alerts::tests::unhealthy_environment_dispatches_both_destinations_with_cooldown -- --ignored

docker exec "$name" createdb -U postgres novu_grant_lock_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_grant_lock_test" \
  cargo test --locked --test operator_grant_lock -- --ignored

docker exec "$name" createdb -U postgres novu_twilio_projection_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_twilio_projection_test" \
  cargo test --locked --test twilio_projection -- --ignored

# These tests restart the disposable database container sequentially.
docker exec "$name" createdb -U postgres novu_twilio_reconnect_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_twilio_reconnect_test" TEST_PG_CONTAINER="$name" \
  cargo test --locked --test twilio_reconnect -- --ignored

# This final test restarts the disposable database container.
docker exec "$name" createdb -U postgres novu_resend_reconnect_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_resend_reconnect_test" TEST_PG_CONTAINER="$name" \
  cargo test --locked --test resend_reconnect -- --ignored
