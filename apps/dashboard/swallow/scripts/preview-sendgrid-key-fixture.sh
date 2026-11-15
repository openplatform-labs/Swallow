#!/usr/bin/env bash
# Disposable local SendGrid key-management UI fixture; no external provider calls.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(cd ../../.. && pwd)
api="$root/apps/api/rust-api"
name="swallow-ui-sendgrid-$$"
fixture_dir=$(mktemp -d /private/tmp/swallow-ui-sendgrid.XXXXXX)
api_pid=''
cleanup() {
  if [[ -n "$api_pid" ]]; then kill "$api_pid" 2>/dev/null || true; wait "$api_pid" 2>/dev/null || true; fi
  docker rm -f "$name" >/dev/null 2>&1 || true
  rm -rf "$fixture_dir"
}
trap cleanup EXIT
docker run --rm -d --name "$name" -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=ui_test -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
for ((i=0;i<60;i++)); do
  if docker exec "$name" pg_isready -q -U postgres -d ui_test; then break; fi
  if ((i==59)); then echo 'PostgreSQL did not become ready' >&2; exit 1; fi
  sleep 1
done
pg_port=$(docker port "$name" 5432/tcp); pg_port=${pg_port##*:}
for migration in 0001_experimental.sql 0004_sendgrid_webhooks.sql 0064_sendgrid_key_management.sql 0065_sendgrid_key_audit_access.sql 0066_sendgrid_key_activation.sql; do
  docker exec -i "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d ui_test < "$api/migrations/$migration"
done
docker exec -i "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d ui_test <<'SQL'
CREATE ROLE ui_sendgrid_fixture LOGIN;
GRANT USAGE ON SCHEMA public TO ui_sendgrid_fixture;
GRANT SELECT ON public.novu_rust_users,public.novu_rust_api_keys TO ui_sendgrid_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_sendgrid_keys_list(text,text,text),public.novu_rust_sendgrid_key_change(text,text,text,text,text,uuid),public.novu_rust_sendgrid_key_set_active(text,text,text,boolean,text,uuid),public.novu_rust_sendgrid_key_audit_list(text,text,uuid) TO ui_sendgrid_fixture;
INSERT INTO public.novu_rust_environments VALUES ('ui-fixture-env','org');
INSERT INTO public.novu_rust_users VALUES ('ui-fixture-user');
INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES (encode(sha256(convert_to('ui-sendgrid-test-key','UTF8')),'hex'),'ui-fixture-env','ui-fixture-user');
INSERT INTO public.novu_rust_sendgrid_key_permissions SELECT encode(sha256(convert_to('ui-sendgrid-test-key','UTF8')),'hex'),p FROM unnest(ARRAY['read','write','audit']) p;
SQL
openssl ecparam -name prime256v1 -genkey -noout -out "$fixture_dir/private.pem" 2>/dev/null
openssl pkey -in "$fixture_dir/private.pem" -pubout -outform DER -out "$fixture_dir/public.der" 2>/dev/null
base64 < "$fixture_dir/public.der" | tr -d '\n' > "$fixture_dir/public-key"
api_port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
cargo build --quiet --locked --manifest-path "$api/Cargo.toml" --bin novu-rust-api
env -i PATH="$PATH" DATABASE_URL="postgres://ui_sendgrid_fixture@127.0.0.1:${pg_port}/ui_test" \
  NOVU_POSTGRES_ALLOW_INSECURE=true LISTEN_ADDR="127.0.0.1:${api_port}" \
  SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true SWALLOW_CORS_ALLOWED_ORIGINS="http://127.0.0.1:4259" \
  "$api/target/debug/novu-rust-api" >"$fixture_dir/api.log" 2>&1 &
api_pid=$!
for ((i=0;i<60;i++)); do
  if [[ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${api_port}/health/live")" == 200 ]]; then break; fi
  if ! kill -0 "$api_pid" 2>/dev/null; then echo 'API fixture failed to start' >&2; exit 1; fi
  if ((i==59)); then echo 'API fixture not ready' >&2; exit 1; fi
  sleep 1
done
printf 'FIXTURE_DIR=%s\nAPI_URL=http://127.0.0.1:%s\nCONTAINER=%s\nAPI_KEY=ui-sendgrid-test-key\n' "$fixture_dir" "$api_port" "$name"
wait "$api_pid"
