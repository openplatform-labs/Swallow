#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
name="swallow-ws-gap-$$"
guarded_pid=''; legacy_pid=''
cleanup() { [[ -z "$guarded_pid" ]] || kill "$guarded_pid" 2>/dev/null || true; [[ -z "$legacy_pid" ]] || kill "$legacy_pid" 2>/dev/null || true; wait "$guarded_pid" 2>/dev/null || true; wait "$legacy_pid" 2>/dev/null || true; docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'; }
pg_port=$(port); export GUARDED_PORT=$(port) LEGACY_PORT=$(port) JWT_SECRET=gap-fixture-secret PG_CONTAINER="$name"
docker run -d --name "$name" -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=novu_ws_gap -p "127.0.0.1:$pg_port:5432" postgres:17-alpine >/dev/null
for ((i=0;i<60;i++)); do docker exec "$name" pg_isready -q -U postgres -d novu_ws_gap && break; sleep 1; done
for file in ../../worker/rust-queue/migrations/0002_experimental_in_app.sql ../../worker/rust-queue/migrations/0014_experimental_subscriber_objectid.sql ../../worker/rust-queue/migrations/0019_experimental_subscriber_online.sql migrations/0001_connections.sql migrations/0002_subscriber_revocation.sql migrations/0003_connection_leases.sql ../../worker/rust-queue/migrations/0030_experimental_ws_events.sql; do docker exec -i "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d novu_ws_gap < "$file" >/dev/null; done
docker exec "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d novu_ws_gap -c "INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('env-a','subscriber','subscriber-id')"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-/private/tmp/swallow-create-target} CARGO_INCREMENTAL=0 NOVU_POSTGRES_ALLOW_INSECURE=true
cargo build --locked --offline >/dev/null
DATABASE_URL="postgresql://postgres@127.0.0.1:$pg_port/novu_ws_gap" LISTEN_ADDR="127.0.0.1:$GUARDED_PORT" NOVU_WS_DISCONNECT_ON_EVENT_GAP=true "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1 & guarded_pid=$!
DATABASE_URL="postgresql://postgres@127.0.0.1:$pg_port/novu_ws_gap" LISTEN_ADDR="127.0.0.1:$LEGACY_PORT" NOVU_WS_DISCONNECT_ON_EVENT_GAP=false "$CARGO_TARGET_DIR/debug/novu-rust-ws" >/dev/null 2>&1 & legacy_pid=$!
for ((i=0;i<60;i++)); do if curl -fsS "http://127.0.0.1:$GUARDED_PORT/socket.io/?EIO=4&transport=polling" >/dev/null 2>&1 && curl -fsS "http://127.0.0.1:$LEGACY_PORT/socket.io/?EIO=4&transport=polling" >/dev/null 2>&1; then break; fi; sleep 0.2; done
node tests/listener_gap.cjs
