#!/usr/bin/env bash
# Requires a fully migrated disposable database and its log.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${TEST_DATABASE_URL:?Set TEST_DATABASE_URL to a disposable fully migrated PostgreSQL database}"
: "${TEST_DISPOSABLE_POSTGRES_LOG:?Set the disposable PostgreSQL server log}"
root=$(cd ../../.. && pwd)
testdir=$(mktemp -d /private/tmp/swallow-fcm-legacy.XXXXXX)
cleanup() { python3 -c 'import shutil,sys; shutil.rmtree(sys.argv[1])' "$testdir"; }
trap cleanup EXIT
git -C "$root" archive 2fd94b90 apps/worker/rust-queue apps/api/rust-contracts | tar -x -C "$testdir"
target=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys;print(json.load(sys.stdin)["target_directory"])')
CARGO_TARGET_DIR="$target" cargo build --quiet --locked --manifest-path "$testdir/apps/worker/rust-queue/Cargo.toml" --bin fcm-worker
cp "$target/debug/fcm-worker" "$testdir/old-fcm-worker"
TEST_OLD_FCM_WORKER_BINARY="$testdir/old-fcm-worker" cargo test --locked --test fcm_exponential_legacy_process -- --ignored
