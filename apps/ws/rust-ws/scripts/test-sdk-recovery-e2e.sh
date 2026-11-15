#!/usr/bin/env bash
# Disposable PostgreSQL + Rust API, worker and WS; SDK is installed separately.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${NOVU_SDK_MODULE:?Set NOVU_SDK_MODULE to the absolute path of an installed @novu/js module}"
name="swallow-sdk-recovery-$$"
api_pid=''; ws_pid=''; worker_pid=''
cleanup() {
  [[ -z "$ws_pid" ]] || kill -CONT "$ws_pid" 2>/dev/null || true
  for pid in "$api_pid" "$ws_pid" "$worker_pid"; do [[ -z "$pid" ]] || kill "$pid" 2>/dev/null || true; done
  for pid in "$api_pid" "$ws_pid" "$worker_pid"; do [[ -z "$pid" ]] || wait "$pid" 2>/dev/null || true; done
  docker rm -f "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT
port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'; }
pg_port=$(port); api_port=$(port); ws_port=$(port)
docker run -d --name "$name" -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=novu_recovery -p "127.0.0.1:$pg_port:5432" postgres:17-alpine >/dev/null
for ((i=0;i<60;i++)); do docker exec "$name" pg_isready -q -U postgres && break; sleep 1; done
docker exec "$name" pg_isready -q -U postgres
for file in ../../api/rust-api/migrations/*.sql; do
  [[ "$file" == ../../api/rust-api/migrations/0018_fcm_integration_management.sql || ! "${file##*/}" < 0056 ]] && continue
  docker exec -i "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d novu_recovery < "$file" >/dev/null || { echo "migration failed: $file" >&2; exit 1; }
done
for file in ../../worker/rust-queue/migrations/*.sql ../../api/rust-api/migrations/0018_fcm_integration_management.sql; do
  docker exec -i "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d novu_recovery < "$file" >/dev/null || { echo "migration failed: $file" >&2; exit 1; }
done
for file in ../../api/rust-api/migrations/*.sql; do
  [[ "${file##*/}" < 0056 ]] && continue
  docker exec -i "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d novu_recovery < "$file" >/dev/null || { echo "migration failed: $file" >&2; exit 1; }
done
for file in migrations/*.sql; do
  docker exec -i "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d novu_recovery < "$file" >/dev/null || { echo "migration failed: $file" >&2; exit 1; }
done
docker exec "$name" psql -q -v ON_ERROR_STOP=1 -U postgres -d novu_recovery -c "INSERT INTO novu_rust_users VALUES ('user-a'); INSERT INTO novu_rust_environments VALUES ('env-a','org-a'); INSERT INTO novu_rust_api_keys(key_hash,environment_id,user_id) VALUES ('4a453798dcb87778c40d2f5daceb0f28cf67e559b7bfbe865878995c64e575fe','env-a','user-a'); INSERT INTO novu_rust_inbox_environments(environment_id,application_identifier,in_app_active,hmac_required,is_development,remove_novu_branding,max_snooze_duration_hours) VALUES ('env-a','recovery-app',true,false,true,false,24); INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content) VALUES ('env-a','greeting',true,true,'hello recovery'); UPDATE novu_rust_workflows SET inbox_metadata=jsonb_build_object('id','64a000000000000000000004','identifier','greeting','name','Greeting','critical',false,'tags',jsonb_build_array(),'severity','none','payloadAsData',false) WHERE environment_id='env-a' AND identifier='greeting';"
CARGO_INCREMENTAL=0 cargo build --locked --offline --manifest-path ../../api/rust-api/Cargo.toml --target-dir ../../api/rust-api/target --bin novu-rust-api >/dev/null
CARGO_INCREMENTAL=0 cargo build --locked --offline --manifest-path ../../worker/rust-queue/Cargo.toml --target-dir ../../worker/rust-queue/target --bin in-app-worker >/dev/null
CARGO_INCREMENTAL=0 cargo build --locked --offline --target-dir target >/dev/null
export DATABASE_URL="postgresql://postgres@127.0.0.1:$pg_port/novu_recovery" NOVU_POSTGRES_ALLOW_INSECURE=true JWT_SECRET=local-recovery-fixture-secret
LISTEN_ADDR="127.0.0.1:$api_port" ../../api/rust-api/target/debug/novu-rust-api >/dev/null 2>&1 & api_pid=$!
LISTEN_ADDR="127.0.0.1:$ws_port" NOVU_WS_DISCONNECT_ON_EVENT_GAP=true NOVU_WS_EVENT_LOOKUP_RECOVERY=probe NOVU_WS_ENABLE_HEALTH_ENDPOINTS=true target/debug/novu-rust-ws >/dev/null 2>&1 & ws_pid=$!
../../worker/rust-queue/target/debug/in-app-worker >/dev/null 2>&1 & worker_pid=$!
for ((i=0;i<60;i++)); do
  if curl -fsS "http://127.0.0.1:$ws_port/socket.io/?EIO=4&transport=polling" >/dev/null 2>&1 && curl -s -o /dev/null "http://127.0.0.1:$api_port/v1/inbox/session"; then break; fi
  if ! kill -0 "$api_pid" 2>/dev/null || ! kill -0 "$ws_pid" 2>/dev/null || ! kill -0 "$worker_pid" 2>/dev/null || (( i == 59 )); then echo 'service startup failed' >&2; exit 1; fi
  sleep 0.2
done
NOVU_RUST_URL="http://127.0.0.1:$api_port" RUST_WS_URL="http://127.0.0.1:$ws_port" PG_CONTAINER="$name" WS_PID="$ws_pid" node tests/sdk_recovery_e2e.cjs
