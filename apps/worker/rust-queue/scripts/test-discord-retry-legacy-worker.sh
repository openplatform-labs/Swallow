#!/usr/bin/env bash
# Historical Discord worker, local proxy, and disposable Docker PostgreSQL.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(cd ../../.. && pwd)
testdir=$(mktemp -d /private/tmp/swallow-discord-retry-legacy.XXXXXX)
container="swallow-discord-retry-legacy-$$-$RANDOM"
cleanup() { docker rm -f "$container" >/dev/null 2>&1 || true; python3 -c 'import shutil,sys;shutil.rmtree(sys.argv[1])' "$testdir"; }
trap cleanup EXIT
git -C "$root" archive 6821aa6d apps/worker/rust-queue apps/api/rust-contracts | tar -x -C "$testdir"
target=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys;print(json.load(sys.stdin)["target_directory"])')
CARGO_TARGET_DIR="$target" cargo build --quiet --locked --manifest-path "$testdir/apps/worker/rust-queue/Cargo.toml" --bin discord-webhook-worker
cp "$target/debug/discord-webhook-worker" "$testdir/old-discord-worker"
docker run -d --rm --name "$container" -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
for ((i=0;i<100;i++)); do if docker exec "$container" pg_isready -q -U postgres; then break; fi; if ((i==99)); then echo 'PostgreSQL not ready' >&2; exit 1; fi; sleep 0.2; done
port=$(docker port "$container" 5432/tcp | awk -F: '{print $NF}')
export DATABASE_URL="postgresql://postgres@127.0.0.1:$port/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:$port/postgres"
cargo run --quiet --locked --bin migrate-worker -- apply api "$root/apps/api/rust-api/migrations" --through 17
cargo run --quiet --locked --bin migrate-worker -- apply worker "$root/apps/worker/rust-queue/migrations"
cargo run --quiet --locked --bin migrate-worker -- apply api "$root/apps/api/rust-api/migrations"
TEST_OLD_DISCORD_BINARY="$testdir/old-discord-worker" TEST_PG_CONTAINER="$container" cargo test --locked --test discord_retry_legacy_worker -- --ignored
