#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
worker="../../worker/rust-queue"
name="swallow-subscriber-conditional-$$"
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker run --rm -d --name "$name" -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=conditional_test -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
for ((i=0;i<60;i++)); do
  if docker exec "$name" pg_isready -q -U postgres -d conditional_test; then break; fi
  if ((i==59)); then echo 'PostgreSQL did not become ready' >&2; exit 1; fi
  sleep 1
done
port=$(docker port "$name" 5432/tcp); port=${port##*:}
export DATABASE_URL="postgres://postgres@127.0.0.1:${port}/conditional_test" NOVU_POSTGRES_ALLOW_INSECURE=true
cargo run --locked --manifest-path "$worker/Cargo.toml" --bin migrate-worker -- apply api migrations --through 17
cargo run --locked --manifest-path "$worker/Cargo.toml" --bin migrate-worker -- apply worker "$worker/migrations"
cargo run --locked --manifest-path "$worker/Cargo.toml" --bin migrate-worker -- apply api migrations
TEST_DATABASE_URL="$DATABASE_URL" cargo test --locked --test subscriber_conditional -- --ignored
