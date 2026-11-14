#!/usr/bin/env bash
# Disposable database; compares the pinned gateway code, not the full WS app.
set -euo pipefail
export NOVU_POSTGRES_ALLOW_INSECURE=true
cd "$(dirname "$0")/.."
reference=${NOVU_REFERENCE_SOURCE:?Set NOVU_REFERENCE_SOURCE to the built reference checkout}
case "$reference" in /*) ;; *) echo 'NOVU_REFERENCE_SOURCE must be absolute' >&2; exit 2;; esac
name="novu-rust-ws-test-$$"
ref_pid=''; rust_pid=''; rust2_pid=''; strict_pid=''; strict2_pid=''; secret_file=''
cleanup() {
  if [[ -n "$ref_pid" ]]; then kill "$ref_pid" >/dev/null 2>&1 || true; wait "$ref_pid" 2>/dev/null || true; fi
  if [[ -n "$rust_pid" ]]; then kill "$rust_pid" >/dev/null 2>&1 || true; wait "$rust_pid" 2>/dev/null || true; fi
  if [[ -n "$strict2_pid" ]]; then kill "$strict2_pid" >/dev/null 2>&1 || true; wait "$strict2_pid" 2>/dev/null || true; fi
  if [[ -n "$strict_pid" ]]; then kill "$strict_pid" >/dev/null 2>&1 || true; wait "$strict_pid" 2>/dev/null || true; fi
  if [[ -n "$rust2_pid" ]]; then kill "$rust2_pid" >/dev/null 2>&1 || true; wait "$rust2_pid" 2>/dev/null || true; fi
  if [[ -n "$secret_file" ]]; then rm -f "$secret_file"; fi
  docker rm -f "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT
port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'; }
pg_port=$(port)
docker run -d --name "$name" -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=novu_ws_test -p "127.0.0.1:$pg_port:5432" postgres:17-alpine >/dev/null
for ((i=0;i<30;i++)); do
  if docker logs "$name" 2>&1 | rg -q 'PostgreSQL init process complete' && docker exec "$name" pg_isready -q -U postgres -d novu_ws_test; then break; fi
  if (( i == 29 )); then echo 'PostgreSQL did not become ready' >&2; exit 1; fi
  sleep 1
done
for file in ../../worker/rust-queue/migrations/0002_experimental_in_app.sql ../../worker/rust-queue/migrations/0014_experimental_subscriber_objectid.sql ../../worker/rust-queue/migrations/0019_experimental_subscriber_online.sql migrations/0001_connections.sql migrations/0002_subscriber_revocation.sql migrations/0003_connection_leases.sql ../../worker/rust-queue/migrations/0030_experimental_ws_events.sql ../../worker/rust-queue/migrations/0032_experimental_message_severity.sql ../../worker/rust-queue/migrations/0034_experimental_notifications.sql ../../worker/rust-queue/migrations/0050_in_app_subscriber_identity.sql; do
  docker exec -i "$name" psql -v ON_ERROR_STOP=1 -U postgres -d novu_ws_test < "$file" >/dev/null
done
docker exec "$name" psql -v ON_ERROR_STOP=1 -U postgres -d novu_ws_test -c "INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('64a000000000000000000002','alice','64a000000000000000000010')" >/dev/null
ref_port=$(port); rust_port=$(port); rust2_port=$(port); strict_port=$(port); strict2_port=$(port)
export NOVU_REFERENCE_SOURCE="$reference" JWT_SECRET=local-compat-test-secret
secret_file=$(mktemp)
chmod 600 "$secret_file"
printf %s "$JWT_SECRET" > "$secret_file"
export REF_WS_PORT="$ref_port" REF_WS_URL="http://127.0.0.1:$ref_port" RUST_WS_URL="http://127.0.0.1:$rust_port" RUST_WS_URL2="http://127.0.0.1:$rust2_port" PG_CONTAINER="$name" PG_PORT="$pg_port"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-/private/tmp/swallow-create-target} CARGO_INCREMENTAL=0
export STRICT_WS_URL="http://127.0.0.1:$strict_port" STRICT_WS_URL2="http://127.0.0.1:$strict2_port"
cargo build --locked --offline >/dev/null
python3 - "$CARGO_TARGET_DIR/debug/novu-rust-ws" "$secret_file" "$pg_port" <<'PY'
import os, subprocess, sys
env = os.environ.copy()
env.update(JWT_SECRET_FILE=sys.argv[2], DATABASE_URL=f'postgresql://postgres@127.0.0.1:{sys.argv[3]}/novu_ws_test')
try:
    result = subprocess.run([sys.argv[1]], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
except subprocess.TimeoutExpired:
    raise SystemExit('WS accepted two JWT secret sources')
if result.returncode == 0:
    raise SystemExit('WS accepted two JWT secret sources')
PY
node tests/reference_gateway.cjs >/dev/null 2>&1 & ref_pid=$!
env -u JWT_SECRET JWT_SECRET_FILE="$secret_file" NOVU_ALLOW_RECREATED_SUBSCRIBER=true DATABASE_URL="postgresql://postgres@127.0.0.1:$pg_port/novu_ws_test" LISTEN_ADDR="127.0.0.1:$rust_port" "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1 & rust_pid=$!
env -u JWT_SECRET JWT_SECRET_FILE="$secret_file" NOVU_ALLOW_RECREATED_SUBSCRIBER=true DATABASE_URL="postgresql://postgres@127.0.0.1:$pg_port/novu_ws_test" LISTEN_ADDR="127.0.0.1:$rust2_port" "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1 & rust2_pid=$!
for ((i=0;i<30;i++)); do
  if curl -fsS "$REF_WS_URL/status" >/dev/null 2>&1 && curl -fsS "$RUST_WS_URL/socket.io/?EIO=4&transport=polling" >/dev/null 2>&1 && curl -fsS "$RUST_WS_URL2/socket.io/?EIO=4&transport=polling" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$ref_pid" 2>/dev/null || ! kill -0 "$rust_pid" 2>/dev/null || ! kill -0 "$rust2_pid" 2>/dev/null || (( i == 29 )); then echo 'WS test server startup failed' >&2; exit 1; fi
  sleep 1
done
node tests/reference_gateway_compare.cjs
node tests/commit_ack_loss.cjs
env -u JWT_SECRET JWT_SECRET_FILE="$secret_file" NOVU_ALLOW_RECREATED_SUBSCRIBER=false DATABASE_URL="postgresql://postgres@127.0.0.1:$pg_port/novu_ws_test" LISTEN_ADDR="127.0.0.1:$strict_port" "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1 & strict_pid=$!
env -u JWT_SECRET JWT_SECRET_FILE="$secret_file" NOVU_ALLOW_RECREATED_SUBSCRIBER=false DATABASE_URL="postgresql://postgres@127.0.0.1:$pg_port/novu_ws_test" LISTEN_ADDR="127.0.0.1:$strict2_port" "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1 & strict2_pid=$!
for ((i=0;i<30;i++)); do
  if curl -fsS "$STRICT_WS_URL/socket.io/?EIO=4&transport=polling" >/dev/null 2>&1 && curl -fsS "$STRICT_WS_URL2/socket.io/?EIO=4&transport=polling" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$strict_pid" 2>/dev/null || ! kill -0 "$strict2_pid" 2>/dev/null || (( i == 29 )); then echo 'strict WS startup failed' >&2; exit 1; fi
  sleep 1
done
node tests/strict_identity.cjs
export STRICT_WS_PID="$strict_pid"
node tests/crash_recovery.cjs
