#!/usr/bin/env bash
# Disposable two-node synchronous replication test; kills its own primary.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(cd ../../.. && pwd)
suffix="$$-$RANDOM"
network="swallow-ha-net-$suffix"
primary="swallow-ha-primary-$suffix"
standby="swallow-ha-standby-$suffix"
volume="swallow-ha-standby-data-$suffix"
cleanup() { docker rm -f "$standby" "$primary" >/dev/null 2>&1 || true; docker volume rm "$volume" >/dev/null 2>&1 || true; docker network rm "$network" >/dev/null 2>&1 || true; }
trap cleanup EXIT
export NOVU_POSTGRES_ALLOW_INSECURE=true
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/private/tmp/swallow-ha-target}"
docker network create "$network" >/dev/null
docker volume create "$volume" >/dev/null
docker run -d --name "$primary" --network "$network" -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1::5432 postgres:17-alpine -c wal_level=replica -c max_wal_senders=10 >/dev/null
for ((i=0;i<90;i++)); do
  if docker exec "$primary" pg_isready -q -U postgres; then break; fi
  if ((i==89)); then echo 'primary not ready' >&2; exit 1; fi
  sleep 1
done
primary_port=$(docker port "$primary" 5432/tcp | awk -F: '{print $NF}')
export DATABASE_URL="postgresql://postgres@127.0.0.1:$primary_port/postgres"
cargo build --locked --bin migrate-worker >/dev/null
"$CARGO_TARGET_DIR/debug/migrate-worker" apply api "$root/apps/api/rust-api/migrations" --through 17
"$CARGO_TARGET_DIR/debug/migrate-worker" apply worker "$root/apps/worker/rust-queue/migrations"
"$CARGO_TARGET_DIR/debug/migrate-worker" apply api "$root/apps/api/rust-api/migrations"
docker exec "$primary" sh -c "printf '\nhost replication all 0.0.0.0/0 trust\n' >> /var/lib/postgresql/data/pg_hba.conf"
docker exec "$primary" psql -v ON_ERROR_STOP=1 -U postgres -c 'SELECT pg_reload_conf()' >/dev/null
docker run -d --name "$standby" --network "$network" -v "$volume:/var/lib/postgresql/data" -e PRIMARY_HOST="$primary" -p 127.0.0.1::5432 postgres:17-alpine sh -c 'set -e; chown postgres:postgres /var/lib/postgresql/data; chmod 700 /var/lib/postgresql/data; gosu postgres pg_basebackup -h "$PRIMARY_HOST" -U postgres -D /var/lib/postgresql/data -R -X stream -c fast; exec gosu postgres postgres -D /var/lib/postgresql/data -c "primary_conninfo=host=$PRIMARY_HOST user=postgres application_name=standby1"' >/dev/null
for ((i=0;i<120;i++)); do
  if [[ "$(docker exec "$primary" psql -U postgres -Atc "SELECT count(*) FROM pg_stat_replication WHERE application_name='standby1' AND state='streaming'" 2>/dev/null || true)" == 1 ]]; then break; fi
  if [[ "$(docker inspect -f '{{.State.Running}}' "$standby")" != true ]]; then echo 'standby stopped' >&2; docker logs "$standby" >&2; exit 1; fi
  if ((i==119)); then echo 'standby not streaming' >&2; docker logs "$standby" >&2; exit 1; fi
  sleep 1
done
docker exec "$primary" psql -v ON_ERROR_STOP=1 -U postgres -c "ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (standby1)'" >/dev/null
docker exec "$primary" psql -v ON_ERROR_STOP=1 -U postgres -c 'SELECT pg_reload_conf()' >/dev/null
for ((i=0;i<60;i++)); do
  if [[ "$(docker exec "$primary" psql -U postgres -Atc "SELECT count(*) FROM pg_stat_replication WHERE application_name='standby1' AND sync_state='sync'" 2>/dev/null || true)" == 1 ]]; then break; fi
  if ((i==59)); then echo 'synchronous standby not ready' >&2; exit 1; fi
  sleep 1
done
standby_port=$(docker port "$standby" 5432/tcp | awk -F: '{print $NF}')
export TEST_PRIMARY_CONTAINER="$primary" TEST_STANDBY_CONTAINER="$standby"
export TEST_PRIMARY_URL="postgresql://postgres@127.0.0.1:$primary_port/postgres" TEST_STANDBY_URL="postgresql://postgres@127.0.0.1:$standby_port/postgres"
export TEST_FAILOVER_URL="postgresql://postgres@127.0.0.1:$standby_port,127.0.0.1:$primary_port/postgres?target_session_attrs=read-write&connect_timeout=2"
cargo test --locked --test postgres_failover -- --ignored
