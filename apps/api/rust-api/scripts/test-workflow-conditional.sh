#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
name="swallow-workflow-cas-$$"
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker run --rm -d --name "$name" -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=workflow_cas -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
for ((i=0;i<60;i++)); do
  if docker exec "$name" pg_isready -q -U postgres -d workflow_cas; then break; fi
  if ((i==59)); then echo 'PostgreSQL did not become ready' >&2; exit 1; fi
  sleep 1
done
port=$(docker port "$name" 5432/tcp); port=${port##*:}
TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/workflow_cas" cargo test --locked --test workflow_conditional -- --ignored
for test_name in channel_workflow_management slack_workflow_management provider_auth_race; do
  docker exec "$name" createdb -U postgres "$test_name"
  TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:${port}/${test_name}" cargo test --locked --test "$test_name" -- --ignored
done
