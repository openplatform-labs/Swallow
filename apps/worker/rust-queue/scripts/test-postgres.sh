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

docker exec "$name" createdb -U postgres novu_sendgrid_secret_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_sendgrid_secret_test" \
  cargo test --locked --test sendgrid_secret_files -- --ignored

docker exec "$name" createdb -U postgres novu_fcm_secret_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_fcm_secret_test" \
  cargo test --locked --test fcm_secret_files -- --ignored

docker exec "$name" createdb -U postgres novu_alert_delivery_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_alert_delivery_test" \
  cargo test --locked --test alerts_postgres -- --ignored

docker exec "$name" createdb -U postgres novu_alert_transport_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_alert_transport_test" \
  cargo test --locked --lib alerts::tests::unhealthy_environment_dispatches_both_destinations_with_cooldown -- --ignored

docker exec "$name" createdb -U postgres novu_grant_lock_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_grant_lock_test" \
  cargo test --locked --test operator_grant_lock -- --ignored

docker exec "$name" createdb -U postgres novu_grant_management_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_grant_management_test" \
  cargo test --locked --test operator_grant_management -- --ignored

docker exec "$name" createdb -U postgres novu_operator_identity_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_operator_identity_test" \
  cargo test --locked --test operator_role_identity -- --ignored

docker exec "$name" createdb -U postgres novu_slack_cli_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_slack_cli_test" \
  cargo test --locked --test slack_cli -- --ignored

docker exec "$name" createdb -U postgres novu_slack_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_slack_test" \
  cargo test --locked --lib slack_queue::tests::queue_fences_concurrency_and_recovers_uncertain_attempts -- --ignored

docker exec "$name" createdb -U postgres novu_slack_webhook_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_slack_webhook_test" \
  cargo test --locked --lib slack_webhook_queue::tests::destinations_are_isolated_and_uncertain_attempts_are_not_retried -- --ignored

docker exec "$name" createdb -U postgres novu_slack_webhook_cli_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_slack_webhook_cli_test" \
  cargo test --locked --test slack_webhook_cli -- --ignored

docker exec "$name" createdb -U postgres novu_slack_endpoint_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_slack_endpoint_test" \
  cargo test --locked --test slack_channel_endpoints -- --ignored

docker exec "$name" createdb -U postgres novu_slack_workflow_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_slack_workflow_test" \
  cargo test --locked --lib slack_workflow::tests::resolves_endpoints_and_revalidates_each_delivery -- --ignored

docker exec "$name" createdb -U postgres novu_slack_trigger_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_slack_trigger_test" NOVU_ALLOW_RECREATED_SUBSCRIBER=false \
  cargo test --locked --lib slack_workflow::tests::trigger_fanout_and_delivery_render_subscriber -- --ignored

docker exec "$name" createdb -U postgres novu_slack_workflow_cli_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_slack_workflow_cli_test" \
  cargo test --locked --test slack_workflow_cli -- --ignored

docker exec "$name" createdb -U postgres novu_worker_migrations_real_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_worker_migrations_real_test" \
  cargo test --locked --test worker_migrations_real -- --ignored
docker exec "$name" createdb -U postgres novu_worker_migrations_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_worker_migrations_test" \
  cargo test --locked --test worker_migrations -- --ignored
docker exec "$name" createdb -U postgres novu_alert_resolution_permission_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_alert_resolution_permission_test" \
  cargo test --locked --test alert_resolution_permission -- --ignored
docker exec "$name" createdb -U postgres novu_slack_workflow_attention_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_slack_workflow_attention_test" \
  cargo test --locked --test slack_workflow_attention -- --ignored

docker exec "$name" createdb -U postgres novu_slack_reconnect_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_slack_reconnect_test" TEST_PG_CONTAINER="$name" \
  cargo test --locked --test slack_reconnect -- --ignored

docker exec "$name" createdb -U postgres novu_twilio_projection_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_twilio_projection_test" \
  cargo test --locked --test twilio_projection -- --ignored

# These tests restart the disposable database container sequentially.
docker exec "$name" createdb -U postgres novu_twilio_reconnect_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_twilio_reconnect_test" TEST_PG_CONTAINER="$name" \
  cargo test --locked --test twilio_reconnect -- --ignored

docker exec "$name" createdb -U postgres novu_twilio_delivery_reconnect_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_twilio_delivery_reconnect_test" TEST_PG_CONTAINER="$name" \
  cargo test --locked --test twilio_delivery_reconnect -- --ignored

docker exec "$name" createdb -U postgres novu_fcm_delivery_reconnect_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_fcm_delivery_reconnect_test" TEST_PG_CONTAINER="$name" \
  cargo test --locked --test fcm_delivery_reconnect -- --ignored

docker exec "$name" createdb -U postgres novu_sendgrid_delivery_reconnect_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_sendgrid_delivery_reconnect_test" TEST_PG_CONTAINER="$name" \
  cargo test --locked --test sendgrid_delivery_reconnect -- --ignored

docker exec "$name" createdb -U postgres novu_in_app_reconnect_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_in_app_reconnect_test" TEST_PG_CONTAINER="$name" \
  cargo test --locked --test in_app_reconnect -- --ignored

# This final test restarts the disposable database container.
docker exec "$name" createdb -U postgres novu_resend_reconnect_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_resend_reconnect_test" TEST_PG_CONTAINER="$name" \
  cargo test --locked --test resend_reconnect -- --ignored

docker exec "$name" createdb -U postgres novu_resend_delivery_reconnect_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_resend_delivery_reconnect_test" TEST_PG_CONTAINER="$name" \
  cargo test --locked --test resend_delivery_reconnect -- --ignored
