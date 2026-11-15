#!/usr/bin/env bash
set -euo pipefail
: "${TEST_PG_BIN:?Set TEST_PG_BIN to the PostgreSQL bin directory}"
ROOT=$(cd "$(dirname "$0")/../../../.." && pwd); WORKER="$ROOT/apps/worker/rust-queue"; API="$ROOT/apps/api/rust-api"
DIR=$(mktemp -d "${TMPDIR:-/tmp}/swallow-resend-cancel.XXXXXX")
cleanup() { if [[ -f "$DIR/pg/postmaster.pid" ]] && ! "$TEST_PG_BIN/pg_ctl" -D "$DIR/pg" -m immediate -w stop >/dev/null 2>&1; then echo "Database retained at $DIR" >&2; return; fi; rm -rf "$DIR"; }
trap cleanup EXIT
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$TEST_PG_BIN/initdb" -D "$DIR/pg" -U postgres -A trust --no-locale >/dev/null
"$TEST_PG_BIN/pg_ctl" -D "$DIR/pg" -l "$DIR/pg.log" -o "-h 127.0.0.1 -p $PORT" -w start >/dev/null
export DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations" --through 17
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply worker "$WORKER/migrations"
cargo run --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin migrate-worker -- apply api "$API/migrations"
"$TEST_PG_BIN/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE cancel_fixture LOGIN; CREATE ROLE cancel_unprivileged LOGIN;
GRANT USAGE ON SCHEMA public TO cancel_fixture,cancel_unprivileged;
GRANT EXECUTE ON FUNCTION public.novu_rust_resend_cancel_reserve(text,uuid,uuid,text),public.novu_rust_resend_cancel_finish(text,uuid,text,integer),public.novu_rust_resend_cancel_get(text,uuid) TO cancel_fixture,cancel_unprivileged;
INSERT INTO public.novu_rust_environments VALUES ('cancel-env','org'),('cancel-other','org');
INSERT INTO public.novu_rust_resend_cancel_operators SELECT 'cancel-env',rolname,oid,true,true FROM pg_roles WHERE rolname='cancel_fixture';
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,resend_scheduled_at) VALUES
 ('11111111-1111-4111-8111-111111111111','cancel-env','experimental_resend_email','{}','completed',clock_timestamp()+interval '1 hour'),
 ('22222222-2222-4222-8222-222222222222','cancel-env','experimental_resend_email','{}','completed',clock_timestamp()+interval '1 hour'),
 ('33333333-3333-4333-8333-333333333333','cancel-env','experimental_resend_email','{}','completed',clock_timestamp()+interval '1 hour');
INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES
 ('11111111-1111-4111-8111-111111111111','cancel-env','accepted','fixture-one'),
 ('22222222-2222-4222-8222-222222222222','cancel-env','accepted','fixture-two'),
 ('33333333-3333-4333-8333-333333333333','cancel-env','accepted','fixture-three');
SQL
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --features test-mocks --bin resend-cancel
cp "$WORKER/target/debug/resend-cancel" "$DIR/cancel-fixture"
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin resend-cancel
TEST_CANCEL="$DIR/cancel-fixture" TEST_PSQL="$TEST_PG_BIN/psql" TEST_PORT="$PORT" python3 "$WORKER/tests/resend_cancel_process.py"
echo 'Resend cancellation restricted CLI and mock provider tests passed.'
