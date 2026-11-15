#!/usr/bin/env bash
# Requires a fully migrated, disposable PostgreSQL database.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${TEST_DATABASE_URL:?Set TEST_DATABASE_URL to a disposable fully migrated PostgreSQL database}"
: "${TEST_DISPOSABLE_POSTGRES_LOG:?Set TEST_DISPOSABLE_POSTGRES_LOG to that disposable database server log}"
root=$(cd ../../.. && pwd)
testdir=$(mktemp -d /private/tmp/swallow-twilio-legacy.XXXXXX)
cleanup() { python3 -c 'import shutil,sys; shutil.rmtree(sys.argv[1])' "$testdir"; }
trap cleanup EXIT
# Archive only project sources needed by the old binary, never an existing checkout.
git -C "$root" archive d6a4bef4 apps/worker/rust-queue apps/api/rust-contracts | tar -x -C "$testdir"
target=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys;print(json.load(sys.stdin)["target_directory"])')
CARGO_TARGET_DIR="$target" cargo build --quiet --locked --manifest-path "$testdir/apps/worker/rust-queue/Cargo.toml" --bin twilio-worker
cp "$target/debug/twilio-worker" "$testdir/old-twilio-worker"
TEST_OLD_TWILIO_WORKER_BINARY="$testdir/old-twilio-worker" cargo test --locked --test twilio_legacy_fence_process -- --ignored
