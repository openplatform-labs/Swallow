#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
name="swallow-ws-health-$$"
pids=()
cleanup() { for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; done; docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'; }
pg_port=$(port); health_port=$(port); default_port=$(port)
docker run -d --name "$name" -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=novu_ws_health -p "127.0.0.1:$pg_port:5432" postgres:17-alpine >/dev/null
for ((i=0;i<60;i++)); do docker exec "$name" pg_isready -q -U postgres -d novu_ws_health && break; sleep 1; done
for file in ../../worker/rust-queue/migrations/0002_experimental_in_app.sql ../../worker/rust-queue/migrations/0014_experimental_subscriber_objectid.sql ../../worker/rust-queue/migrations/0019_experimental_subscriber_online.sql migrations/0001_connections.sql migrations/0002_subscriber_revocation.sql migrations/0003_connection_leases.sql ../../worker/rust-queue/migrations/0030_experimental_ws_events.sql; do docker exec -i "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d novu_ws_health < "$file" >/dev/null; done
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-/private/tmp/swallow-create-target} CARGO_INCREMENTAL=0 NOVU_POSTGRES_ALLOW_INSECURE=true NOVU_ALLOW_RECREATED_SUBSCRIBER=false DATABASE_URL="postgresql://postgres@127.0.0.1:$pg_port/novu_ws_health" JWT_SECRET=health-fixture-secret
cargo build --locked --offline >/dev/null
if NOVU_WS_ENABLE_HEALTH_ENDPOINTS=invalid "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1; then echo 'invalid health configuration accepted' >&2; exit 1; fi
LISTEN_ADDR="127.0.0.1:$health_port" NOVU_WS_ENABLE_HEALTH_ENDPOINTS=true "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1 & pids+=("$!")
env -u NOVU_WS_ENABLE_HEALTH_ENDPOINTS LISTEN_ADDR="127.0.0.1:$default_port" "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1 & pids+=("$!")
code() { curl -s -o /dev/null -w '%{http_code}' --max-time 4 "$1" || true; }
for p in "$health_port" "$default_port"; do for ((i=0;i<100;i++)); do if curl -fsS "http://127.0.0.1:$p/socket.io/?EIO=4&transport=polling" >/dev/null 2>&1; then break; fi; if (( i == 99 )); then echo 'WS did not start' >&2; exit 1; fi; sleep 0.1; done; done
[[ $(code "http://127.0.0.1:$health_port/health/live") == 204 ]]
[[ $(code "http://127.0.0.1:$health_port/health/ready") == 204 ]]
[[ $(code "http://127.0.0.1:$default_port/health/live") == 404 ]]
# Interrupt only LISTEN, leaving the query connection and PostgreSQL running.
docker exec "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d novu_ws_health -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'" >/dev/null
for ((i=0;i<50;i++)); do [[ $(code "http://127.0.0.1:$health_port/health/ready") == 503 ]] && break; if (( i == 49 )); then echo 'readiness missed listener failure' >&2; exit 1; fi; sleep 0.02; done
for ((i=0;i<80;i++)); do [[ $(code "http://127.0.0.1:$health_port/health/ready") == 204 ]] && break; if (( i == 79 )); then echo 'listener readiness did not recover' >&2; exit 1; fi; sleep 0.1; done
docker stop "$name" >/dev/null
for ((i=0;i<80;i++)); do [[ $(code "http://127.0.0.1:$health_port/health/ready") == 503 ]] && break; if (( i == 79 )); then echo 'readiness did not fail during database outage' >&2; exit 1; fi; sleep 0.1; done
[[ $(code "http://127.0.0.1:$health_port/health/live") == 204 ]]
docker start "$name" >/dev/null
for ((i=0;i<120;i++)); do [[ $(code "http://127.0.0.1:$health_port/health/ready") == 204 ]] && break; if (( i == 119 )); then echo 'readiness did not recover' >&2; exit 1; fi; sleep 0.25; done
echo 'optional WS health endpoints detect database outage and recover; default routes absent'
