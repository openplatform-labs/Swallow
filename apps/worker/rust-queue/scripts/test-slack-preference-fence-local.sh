#!/usr/bin/env bash
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-slack-preference-fence.XXXXXX")
cleanup() {
  if [[ -f "$DIR/data/postmaster.pid" ]] && ! "$TEST_PG_BIN/pg_ctl" -D "$DIR/data" -m immediate -w stop >/dev/null 2>&1; then
    echo "Database retained at $DIR" >&2; return
  fi
  python3 -c 'import shutil,sys; shutil.rmtree(sys.argv[1])' "$DIR"
}
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$DIR/data" -o "-h 127.0.0.1 -p $PORT -k $DIR" -l "$DIR/postgres.log" -w start >/dev/null
TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" CARGO_INCREMENTAL=0 cargo test --locked --manifest-path "$ROOT/apps/worker/rust-queue/Cargo.toml" --features test-mocks --lib slack_workflow::tests::resolves_endpoints_and_revalidates_each_delivery -- --ignored --nocapture
"$TEST_PG_BIN/createdb" -h 127.0.0.1 -p "$PORT" -U postgres slack_step_policy
TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/slack_step_policy" CARGO_INCREMENTAL=0 cargo test --locked --manifest-path "$ROOT/apps/worker/rust-queue/Cargo.toml" --features test-mocks --lib slack_workflow::tests::step_policies_recheck_and_fence_workflow_updates -- --ignored --nocapture
