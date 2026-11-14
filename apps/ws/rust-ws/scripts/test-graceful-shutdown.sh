#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
name="swallow-ws-drain-$$"
pids=()
cleanup() { for pid in ${pids[@]+"${pids[@]}"}; do kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; done; docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'; }
pg_port=$(port); export WS_PORT=$(port) WS_PEER_PORT=$(port) WS_TIMEOUT_PORT=$(port) WS_RACE_PORT=$(port) JWT_SECRET=drain-fixture-secret PG_CONTAINER="$name"
docker run -d --name "$name" -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=novu_ws_shutdown -p "127.0.0.1:$pg_port:5432" postgres:17-alpine >/dev/null
for ((i=0;i<60;i++)); do docker exec "$name" pg_isready -q -h 127.0.0.1 -U postgres -d novu_ws_shutdown && break; sleep 1; done
for file in ../../worker/rust-queue/migrations/0002_experimental_in_app.sql ../../worker/rust-queue/migrations/0014_experimental_subscriber_objectid.sql ../../worker/rust-queue/migrations/0019_experimental_subscriber_online.sql migrations/0001_connections.sql migrations/0002_subscriber_revocation.sql migrations/0003_connection_leases.sql ../../worker/rust-queue/migrations/0030_experimental_ws_events.sql; do docker exec -i "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d novu_ws_shutdown < "$file" >/dev/null; done
docker exec "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d novu_ws_shutdown -c "INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('env-a','test-user','subscriber-id')"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-/private/tmp/swallow-ws-target} CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_BUILD_JOBS=4 NOVU_POSTGRES_ALLOW_INSECURE=true NOVU_ALLOW_RECREATED_SUBSCRIBER=false DATABASE_URL="postgresql://postgres@127.0.0.1:$pg_port/novu_ws_shutdown"
cargo build --locked --offline >/dev/null
for value in 0 301 invalid; do if SWALLOW_WS_SHUTDOWN_GRACE_SECONDS="$value" "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1; then echo 'invalid shutdown grace accepted' >&2; exit 1; fi; done
LISTEN_ADDR="127.0.0.1:$WS_PORT" NOVU_WS_PRESENCE_POLICY=active NOVU_WS_ENABLE_HEALTH_ENDPOINTS=true SWALLOW_WS_DRAIN_SECONDS=2 SWALLOW_WS_SHUTDOWN_GRACE_SECONDS=5 "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1 & export WS_PID=$!; pids+=("$WS_PID")
LISTEN_ADDR="127.0.0.1:$WS_PEER_PORT" NOVU_WS_PRESENCE_POLICY=active NOVU_WS_ENABLE_HEALTH_ENDPOINTS=true "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1 & pids+=("$!")
LISTEN_ADDR="127.0.0.1:$WS_TIMEOUT_PORT" NOVU_WS_PRESENCE_POLICY=active NOVU_WS_ENABLE_HEALTH_ENDPOINTS=true SWALLOW_WS_DRAIN_SECONDS=0 SWALLOW_WS_SHUTDOWN_GRACE_SECONDS=1 "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1 & export WS_TIMEOUT_PID=$!; pids+=("$WS_TIMEOUT_PID")
LISTEN_ADDR="127.0.0.1:$WS_RACE_PORT" NOVU_WS_PRESENCE_POLICY=active NOVU_WS_ENABLE_HEALTH_ENDPOINTS=true SWALLOW_WS_DRAIN_SECONDS=0 SWALLOW_WS_SHUTDOWN_GRACE_SECONDS=5 "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1 & export WS_RACE_PID=$!; pids+=("$WS_RACE_PID")
for p in "$WS_PORT" "$WS_PEER_PORT" "$WS_TIMEOUT_PORT" "$WS_RACE_PORT"; do for ((i=0;i<100;i++)); do if curl -fsS "http://127.0.0.1:$p/health/ready" >/dev/null 2>&1; then break; fi; if (( i == 99 )); then echo 'WS did not start' >&2; exit 1; fi; sleep 0.1; done; done
node tests/graceful_shutdown.cjs
wait "$WS_PID"
wait "$WS_RACE_PID"
if wait "$WS_TIMEOUT_PID"; then echo 'locked cleanup unexpectedly succeeded' >&2; exit 1; fi
