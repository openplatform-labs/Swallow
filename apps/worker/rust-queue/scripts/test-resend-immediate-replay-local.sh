#!/usr/bin/env bash
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
WORKER="$ROOT/apps/worker/rust-queue"; API="$ROOT/apps/api/rust-api"
DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-resend-replay.XXXXXX")
cleanup() { if [[ -f "$DIR/data/postmaster.pid" ]] && ! "$TEST_PG_BIN/pg_ctl" -D "$DIR/data" -m immediate -w stop >/dev/null 2>&1; then echo "Could not stop test PostgreSQL; retained $DIR" >&2; return; fi; rm -rf "$DIR"; }
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$DIR/data" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$DIR/data" -o "-h 127.0.0.1 -p $PORT" -l "$DIR/postgres.log" -w start >/dev/null
export TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --offline --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --offline --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --offline --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
cargo test --locked --offline --manifest-path "$WORKER/Cargo.toml" --lib resend_queue::tests::immediate_replay_keeps_body_and_key_and_preserves_uncertainty -- --ignored
cargo build --quiet --locked --offline --manifest-path "$WORKER/Cargo.toml" --bin resend-worker
"$TEST_PG_BIN/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE resend_replay_fixture LOGIN;
GRANT USAGE ON SCHEMA public TO resend_replay_fixture;
GRANT SELECT,INSERT,UPDATE ON public.novu_rust_email_workflow_worker_config,public.novu_rust_resend_idempotency_policy TO resend_replay_fixture;
INSERT INTO public.novu_rust_environments VALUES('replay-worker-fixture','org');
SQL
worker_pid=
stop_worker() { if [[ -n "$worker_pid" ]]; then kill "$worker_pid" 2>/dev/null || true; wait "$worker_pid" 2>/dev/null || true; worker_pid=; fi; }
trap 'stop_worker; cleanup' EXIT
DATABASE_URL="postgres://resend_replay_fixture@127.0.0.1:$PORT/postgres" NOVU_RESEND_ENVIRONMENT_ID=replay-worker-fixture NOVU_RESEND_API_KEY=fixture NOVU_RESEND_FROM=sender@example.test NOVU_RESEND_REPLAY_UNCERTAIN_ONCE=true "$WORKER/target/debug/resend-worker" >"$DIR/worker.log" 2>&1 & worker_pid=$!
for _ in $(seq 1 100); do rg -q 'immediate replay migration or privilege unavailable' "$DIR/worker.log" && break; kill -0 "$worker_pid"; sleep 0.1; done
rg -q 'immediate replay migration or privilege unavailable' "$DIR/worker.log"
stop_worker
"$TEST_PG_BIN/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 -c 'GRANT SELECT (same_request_replays), UPDATE (same_request_replays) ON public.novu_rust_resend_attempts TO resend_replay_fixture'
DATABASE_URL="postgres://resend_replay_fixture@127.0.0.1:$PORT/postgres" NOVU_RESEND_ENVIRONMENT_ID=replay-worker-fixture NOVU_RESEND_API_KEY=fixture NOVU_RESEND_FROM=sender@example.test NOVU_RESEND_REPLAY_UNCERTAIN_ONCE=true "$WORKER/target/debug/resend-worker" >"$DIR/worker-reconciliation.log" 2>&1 & worker_pid=$!
for _ in $(seq 1 100); do rg -q 'immediate replay migration or privilege unavailable' "$DIR/worker-reconciliation.log" && break; kill -0 "$worker_pid"; sleep 0.1; done
rg -q 'immediate replay migration or privilege unavailable' "$DIR/worker-reconciliation.log"
stop_worker
if NOVU_RESEND_ENVIRONMENT_ID=replay-worker-fixture NOVU_RESEND_API_KEY=fixture NOVU_RESEND_FROM=sender@example.test NOVU_RESEND_REPLAY_UNCERTAIN_ONCE=invalid "$WORKER/target/debug/resend-worker" >"$DIR/invalid.log" 2>&1; then echo 'invalid replay setting accepted' >&2; exit 1; fi
rg -q 'invalid Resend immediate replay setting' "$DIR/invalid.log"
echo 'Resend replay fixture and worker configuration checks passed.'
