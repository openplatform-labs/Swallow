#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
monitor="swallow-db-alert-monitor-$$"; target="swallow-db-alert-target-$$"; backup="swallow-db-alert-backup-$$"
cleanup(){ docker rm -f "$monitor" "$target" "$backup" >/dev/null 2>&1 || true; }
trap cleanup EXIT
port(){ python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
monitor_port=$(port);target_port=$(port);backup_port=$(port)
docker run -d --name "$monitor" -e POSTGRES_HOST_AUTH_METHOD=trust -p "127.0.0.1:$monitor_port:5432" postgres:17-alpine >/dev/null
docker run -d --name "$target" -e POSTGRES_HOST_AUTH_METHOD=trust -p "127.0.0.1:$target_port:5432" postgres:17-alpine >/dev/null
docker run -d --name "$backup" -e POSTGRES_HOST_AUTH_METHOD=trust -p "127.0.0.1:$backup_port:5432" postgres:17-alpine >/dev/null
for name in "$monitor" "$target" "$backup"; do for ((i=0;i<60;i++)); do docker exec "$name" pg_isready -q -U postgres && break; sleep 1; done; done
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-/private/tmp/swallow-create-target} CARGO_INCREMENTAL=0
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:$monitor_port/postgres" TEST_PROBE_URL="postgresql://postgres@127.0.0.1:$target_port/postgres" TEST_PROBE_CONTAINER="$target" TEST_BACKUP_URL="postgresql://postgres@127.0.0.1:$backup_port/postgres" cargo test --locked --test database_alert -- --ignored
