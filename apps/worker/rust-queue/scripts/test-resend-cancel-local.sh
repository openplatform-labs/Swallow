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
CREATE ROLE cancel_fixture LOGIN; CREATE ROLE cancel_unprivileged LOGIN; CREATE ROLE cancel_resolver LOGIN; CREATE ROLE cancel_submitter LOGIN; CREATE ROLE cancel_worker LOGIN;
GRANT USAGE ON SCHEMA public TO cancel_fixture,cancel_unprivileged,cancel_resolver,cancel_submitter,cancel_worker;
GRANT EXECUTE ON FUNCTION public.novu_rust_resend_cancel_reserve(text,uuid,uuid,text),public.novu_rust_resend_cancel_finish(text,uuid,text,integer),public.novu_rust_resend_cancel_get(text,uuid) TO cancel_fixture,cancel_unprivileged;
GRANT EXECUTE ON FUNCTION public.novu_rust_resend_cancel_resolve(text,uuid,uuid,text,text),public.novu_rust_resend_cancel_resolution_get(text,uuid),public.novu_rust_resend_cancel_get(text,uuid) TO cancel_resolver,cancel_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_resend_cancel_list(text,integer,uuid) TO cancel_resolver,cancel_fixture,cancel_unprivileged;
GRANT EXECUTE ON FUNCTION public.novu_rust_resend_cancel_enqueue(text,uuid,uuid,text),public.novu_rust_resend_cancel_command_get(text,uuid) TO cancel_submitter,cancel_unprivileged;
GRANT EXECUTE ON FUNCTION public.novu_rust_resend_cancel_claim(text),public.novu_rust_resend_cancel_finish(text,uuid,text,integer) TO cancel_worker;
GRANT EXECUTE ON FUNCTION public.novu_rust_resend_cancel_health(text,integer),public.novu_rust_reserve_alert_v3(text,text,text,integer,bigint,bigint,bigint,bigint),public.novu_rust_observe_alert_conditions_v3(text,text,text,bigint,bigint,bigint,bigint) TO cancel_fixture;
INSERT INTO public.novu_rust_environments VALUES ('cancel-env','org'),('cancel-other','org');
INSERT INTO public.novu_rust_resend_cancel_operators(environment_id,database_principal,principal_oid,can_cancel,can_read,can_resolve) SELECT 'cancel-env',rolname,oid,rolname='cancel_fixture',true,rolname='cancel_resolver' FROM pg_roles WHERE rolname IN ('cancel_fixture','cancel_resolver');
INSERT INTO public.novu_rust_resend_cancel_operators(environment_id,database_principal,principal_oid,can_cancel) SELECT 'cancel-env',rolname,oid,true FROM pg_roles WHERE rolname='cancel_worker';
INSERT INTO public.novu_rust_resend_cancel_submitters SELECT 'cancel-env',rolname,oid FROM pg_roles WHERE rolname='cancel_submitter';
INSERT INTO public.novu_rust_resend_cancel_command_workers SELECT 'cancel-env',rolname,oid FROM pg_roles WHERE rolname='cancel_worker';
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'cancel-env',rolname,decision,oid FROM pg_roles CROSS JOIN (VALUES ('alert_view'),('alert_send')) AS x(decision) WHERE rolname='cancel_fixture';
INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES('cancel-env','slack','blocked','rejected');
INSERT INTO public.novu_rust_alert_v2_targets(environment_id,channel,target_id) VALUES('cancel-env','slack','blocked');
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,resend_scheduled_at) VALUES
 ('11111111-1111-4111-8111-111111111111','cancel-env','experimental_resend_email','{}','completed',clock_timestamp()+interval '1 hour'),
 ('22222222-2222-4222-8222-222222222222','cancel-env','experimental_resend_email','{}','completed',clock_timestamp()+interval '1 hour'),
 ('33333333-3333-4333-8333-333333333333','cancel-env','experimental_resend_email','{}','completed',clock_timestamp()+interval '1 hour'),
 ('44444444-4444-4444-8444-444444444444','cancel-env','experimental_resend_email','{}','completed',clock_timestamp()+interval '1 hour'),
 ('55555555-5555-4555-8555-555555555555','cancel-env','experimental_resend_email','{}','completed',clock_timestamp()+interval '1 hour');
INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES
 ('11111111-1111-4111-8111-111111111111','cancel-env','accepted','fixture-one'),
 ('22222222-2222-4222-8222-222222222222','cancel-env','accepted','fixture-two'),
 ('33333333-3333-4333-8333-333333333333','cancel-env','accepted','fixture-three'),
 ('44444444-4444-4444-8444-444444444444','cancel-env','accepted','fixture-four'),
 ('55555555-5555-4555-8555-555555555555','cancel-env','accepted','fixture-five');
SQL
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --features test-mocks --bin resend-cancel
cp "$WORKER/target/debug/resend-cancel" "$DIR/cancel-fixture"
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin resend-cancel
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin delivery-reconcile --bin send-delivery-alert
TEST_CANCEL="$DIR/cancel-fixture" TEST_WORKER_DIR="$WORKER/target/debug" TEST_PSQL="$TEST_PG_BIN/psql" TEST_PORT="$PORT" python3 "$WORKER/tests/resend_cancel_process.py"
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --features test-mocks --bin resend-cancel-worker
cp "$WORKER/target/debug/resend-cancel-worker" "$DIR/cancel-worker-fixture"
cargo build --quiet --locked --manifest-path "$WORKER/Cargo.toml" --bin resend-cancel-worker
TEST_CANCEL_WORKER="$DIR/cancel-worker-fixture" TEST_PSQL="$TEST_PG_BIN/psql" TEST_PORT="$PORT" python3 "$WORKER/tests/resend_cancel_worker_process.py"
echo 'Resend cancellation restricted CLI and mock provider tests passed.'
