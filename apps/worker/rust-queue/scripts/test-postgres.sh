#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Legacy synthetic job fixtures do not have recipient identity snapshots.
export NOVU_ALLOW_RECREATED_SUBSCRIBER=true
export NOVU_POSTGRES_ALLOW_INSECURE=true
name="novu-rust-queue-test-$$"
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
# The container is disposable; access is limited to a random loopback port.
docker run --rm -d --name "$name" -e POSTGRES_HOST_AUTH_METHOD=trust \
  -e POSTGRES_DB=novu_queue_test -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
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
port=$(docker port "$name" 5432/tcp)
port=${port##*:}
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_queue_test" \
  cargo test --locked --test postgres --test in_app_recovery -- --ignored
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_queue_test" \
  cargo test --locked --lib sendgrid_queue::tests::email_attempt_recovery_and_tenant_isolation -- --ignored
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_queue_test" \
  cargo test --locked --lib twilio_queue::tests::sms_attempt_recovery_and_tenant_isolation -- --ignored
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_queue_test" \
  cargo test --locked --lib fcm_queue::tests::multicast_attempts_recovery_and_tenant_isolation -- --ignored
