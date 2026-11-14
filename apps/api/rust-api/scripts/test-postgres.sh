#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Legacy synthetic jobs and tokens exercise the original compatibility mode.
export NOVU_ALLOW_RECREATED_SUBSCRIBER=true
export NOVU_POSTGRES_ALLOW_INSECURE=true
name="novu-rust-api-test-$$"
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
secret_file=$(mktemp)
hmac_file=$(mktemp)
chmod 600 "$secret_file"
chmod 600 "$hmac_file"
printf %s test-secret > "$secret_file"
printf %s '{"env-a":["old-key","new-key"],"env-b":["other-key"]}' > "$hmac_file"
cleanup() { rm -f "$secret_file" "$hmac_file"; docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker run --rm -d --name "$name" -e POSTGRES_HOST_AUTH_METHOD=trust \
  -e POSTGRES_DB=novu_api_test -p "127.0.0.1:${port}:5432" postgres:17-alpine >/dev/null
for ((i=0; i<30; i++)); do
  if docker exec "$name" pg_isready -q -U postgres -d novu_api_test; then break; fi
  if (( i == 29 )); then echo 'PostgreSQL did not become ready' >&2; exit 1; fi
  sleep 1
done
env -u INBOX_HMAC_KEYS_JSON -u JWT_SECRET_FILE \
  -u NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID -u NOVU_RESEND_WEBHOOK_SECRET -u NOVU_RESEND_WEBHOOK_SECRET_FILE \
  -u NOVU_TWILIO_WEBHOOK_ENVIRONMENT_ID -u NOVU_TWILIO_WEBHOOK_ACCOUNT_SID -u NOVU_TWILIO_WEBHOOK_URL -u NOVU_TWILIO_WEBHOOK_AUTH_TOKEN -u NOVU_TWILIO_WEBHOOK_AUTH_TOKEN_FILE \
  SWALLOW_API_STRICT_SECURITY_CONFIG=true SWALLOW_API_REQUIRED_SECURITY=inbox-jwt TEST_INBOX_HMAC_FILE_CONFIG=1 INBOX_HMAC_KEYS_JSON_FILE="$hmac_file" JWT_SECRET=isolated-subscriber-secret \
  TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_api_test" \
  cargo test --locked --test api_postgres -- --ignored
docker exec "$name" createdb -U postgres novu_provider_auth_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_provider_auth_test" \
  cargo test --locked --test provider_auth_race -- --ignored

docker exec "$name" createdb -U postgres novu_slack_endpoint_api_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_slack_endpoint_api_test" \
  cargo test --locked --test slack_endpoints -- --ignored

TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_api_test" \
  cargo test --locked --test sendgrid_webhook -- --ignored

env -u NOVU_TWILIO_WEBHOOK_AUTH_TOKEN TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_api_test" TEST_TWILIO_FILE_CONFIG=1 \
  SWALLOW_API_STRICT_SECURITY_CONFIG=true SWALLOW_API_REQUIRED_SECURITY=twilio-webhook \
  NOVU_TWILIO_WEBHOOK_ENVIRONMENT_ID=env-a NOVU_TWILIO_WEBHOOK_ACCOUNT_SID=AC11111111111111111111111111111111 \
  NOVU_TWILIO_WEBHOOK_URL=https://events.example.test/webhooks/organizations/org-a/environments/env-a/sms/twilio \
  NOVU_TWILIO_WEBHOOK_AUTH_TOKEN_FILE="$secret_file" \
  cargo test --locked --test twilio_webhook signed_ -- --ignored --test-threads=1
NOVU_TWILIO_WEBHOOK_ENVIRONMENT_ID=env-a NOVU_TWILIO_WEBHOOK_ACCOUNT_SID=AC11111111111111111111111111111111 \
  NOVU_TWILIO_WEBHOOK_URL=https://events.example.test/webhooks/organizations/org-a/environments/env-a/sms/twilio \
  NOVU_TWILIO_WEBHOOK_AUTH_TOKEN=test-secret NOVU_TWILIO_WEBHOOK_AUTH_TOKEN_FILE="$secret_file" \
  cargo test --locked --test twilio_webhook conflicting_webhook_secret_sources -- --ignored

env -u NOVU_RESEND_WEBHOOK_SECRET_FILE TEST_RESEND_STRICT_CONFIG=1 SWALLOW_API_STRICT_SECURITY_CONFIG=true SWALLOW_API_REQUIRED_SECURITY=resend-webhook \
  NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID=env-a NOVU_RESEND_WEBHOOK_SECRET=whsec_dGVzdC1zZWNyZXQtaXMtMzItYnl0ZXMtbG9uZyEh \
  TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_api_test" \
  cargo test --locked --test resend_webhook -- --ignored

# The final case pauses and restarts this disposable container.
docker exec "$name" createdb -U postgres novu_api_reconnect_test
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/novu_api_reconnect_test" TEST_PG_CONTAINER="$name" \
  cargo test --locked --test api_reconnect -- --ignored
