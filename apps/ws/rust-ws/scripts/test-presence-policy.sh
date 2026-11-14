#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
name="swallow-ws-presence-$$"
pids=()
cleanup() { for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; done; docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'; }
pg_port=$(port); export ACTIVE_PORT=$(port) ACTIVE_PORT2=$(port) REFERENCE_PORT=$(port) JWT_SECRET=presence-fixture-secret PG_CONTAINER="$name"
docker run -d --name "$name" -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=novu_ws_presence -p "127.0.0.1:$pg_port:5432" postgres:17-alpine >/dev/null
for ((i=0;i<60;i++)); do docker exec "$name" pg_isready -q -U postgres -d novu_ws_presence && break; sleep 1; done
for file in ../../worker/rust-queue/migrations/0002_experimental_in_app.sql ../../worker/rust-queue/migrations/0014_experimental_subscriber_objectid.sql ../../worker/rust-queue/migrations/0019_experimental_subscriber_online.sql migrations/0001_connections.sql migrations/0002_subscriber_revocation.sql migrations/0003_connection_leases.sql ../../worker/rust-queue/migrations/0030_experimental_ws_events.sql; do docker exec -i "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d novu_ws_presence < "$file" >/dev/null; done
docker exec "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d novu_ws_presence -c "INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('env-a','active','active-id'),('env-a','reference','reference-id')"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-/private/tmp/swallow-create-target} CARGO_INCREMENTAL=0 NOVU_POSTGRES_ALLOW_INSECURE=true NOVU_ALLOW_RECREATED_SUBSCRIBER=false DATABASE_URL="postgresql://postgres@127.0.0.1:$pg_port/novu_ws_presence"
cargo build --locked --offline >/dev/null
if NOVU_WS_PRESENCE_POLICY=invalid "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1; then echo 'invalid presence policy accepted' >&2; exit 1; fi
for p in "$ACTIVE_PORT" "$ACTIVE_PORT2"; do LISTEN_ADDR="127.0.0.1:$p" NOVU_WS_PRESENCE_POLICY=active "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1 & pids+=("$!"); done
env -u NOVU_WS_PRESENCE_POLICY LISTEN_ADDR="127.0.0.1:$REFERENCE_PORT" "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1 & pids+=("$!")
for p in "$ACTIVE_PORT" "$ACTIVE_PORT2" "$REFERENCE_PORT"; do for ((i=0;i<100;i++)); do if curl -fsS "http://127.0.0.1:$p/socket.io/?EIO=4&transport=polling" >/dev/null 2>&1; then break; fi; if (( i == 99 )); then echo 'WS did not start' >&2; exit 1; fi; sleep 0.1; done; done
node tests/presence_policy.cjs
