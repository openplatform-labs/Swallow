#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
pgbin=${SWALLOW_TEST_POSTGRES_BIN:-/opt/homebrew/opt/postgresql@17/bin}
testdir=$(mktemp -d "${TMPDIR:-/tmp}/swallow-ws-retention.XXXXXX")
cleanup() { "$pgbin/pg_ctl" -D "$testdir/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf "$testdir"; }
trap cleanup EXIT
"$pgbin/initdb" -D "$testdir/data" -U postgres -A trust --no-locale >/dev/null
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
"$pgbin/pg_ctl" -D "$testdir/data" -o "-h 127.0.0.1 -p $port -k $testdir" -l "$testdir/db.log" start >/dev/null
"$pgbin/createdb" -h 127.0.0.1 -p "$port" -U postgres retention_test
admin="postgres://postgres@127.0.0.1:$port/retention_test"
role="postgres://ws_retention_test@127.0.0.1:$port/retention_test"
psql=("$pgbin/psql" "$admin" -q -v ON_ERROR_STOP=1)
"${psql[@]}" -f ../../worker/rust-queue/migrations/0030_experimental_ws_events.sql
"${psql[@]}" -f ../../worker/rust-queue/migrations/0168_ws_event_retention.sql
"${psql[@]}" <<'SQL'
CREATE ROLE ws_retention_test LOGIN;
GRANT USAGE ON SCHEMA public TO ws_retention_test;
GRANT EXECUTE ON FUNCTION public.novu_rust_ws_retention_batch(text,timestamptz,integer,boolean,text) TO ws_retention_test;
INSERT INTO public.novu_rust_ws_retention_grants SELECT 'tenant-a',rolname,oid,true,false FROM pg_roles WHERE rolname='ws_retention_test';
INSERT INTO public.novu_rust_ws_events(environment_id,subscriber_internal_id,event,data,created_at)
 SELECT 'tenant-a','subscriber','fixture','{}','2023-01-01'::timestamptz FROM generate_series(1,3);
INSERT INTO public.novu_rust_ws_events(environment_id,subscriber_internal_id,event,data,created_at) VALUES ('tenant-a','subscriber','recent','{}','2025-01-01'),('tenant-b','subscriber','private','{}','2023-01-01');
SQL
export CARGO_INCREMENTAL=0 NOVU_POSTGRES_ALLOW_INSECURE=true
cargo build --quiet --locked --bin ws-event-retention
binary="${CARGO_TARGET_DIR:-target}/debug/ws-event-retention"
export DATABASE_URL="$role"
preview=$("$binary" preview tenant-a 2024-01-01T00:00:00Z 2)
python3 -c 'import json,sys; r=json.loads(sys.argv[1]); assert r["count"]==2 and r["deleted"] is False' "$preview"
if "$binary" preview tenant-b 2024-01-01T00:00:00Z 2 >/dev/null 2>&1; then echo 'cross environment access allowed' >&2; exit 1; fi
if "$binary" delete tenant-a 2024-01-01T00:00:00Z 2 test >/dev/null 2>&1; then echo 'delete without permission allowed' >&2; exit 1; fi
if "$binary" preview tenant-a 2024-01-01 2 >/dev/null 2>&1; then echo 'timezone-free cutoff accepted' >&2; exit 1; fi
if "$pgbin/psql" "$role" -q -c 'SELECT * FROM public.novu_rust_ws_events' >/dev/null 2>&1; then echo 'direct event read allowed' >&2; exit 1; fi
"${psql[@]}" -c "UPDATE public.novu_rust_ws_retention_grants SET can_delete=true WHERE database_principal='ws_retention_test'"
"${psql[@]}" -c "ALTER TABLE public.novu_rust_ws_retention_audit ADD CONSTRAINT test_reject CHECK (reason<>'rollback')"
if "$binary" delete tenant-a 2024-01-01T00:00:00Z 2 rollback >/dev/null 2>&1; then echo 'audit failure accepted' >&2; exit 1; fi
[[ $("${psql[@]}" -Atc "SELECT count(*) FROM public.novu_rust_ws_events WHERE environment_id='tenant-a' AND created_at<'2024-01-01'") == 3 ]]
"${psql[@]}" -c 'ALTER TABLE public.novu_rust_ws_retention_audit DROP CONSTRAINT test_reject'
deleted=$("$binary" delete tenant-a 2024-01-01T00:00:00Z 2 test)
python3 -c 'import json,sys; r=json.loads(sys.argv[1]); assert r["count"]==2 and r["deleted"] is True' "$deleted"
[[ $("${psql[@]}" -Atc "SELECT count(*) FROM public.novu_rust_ws_events WHERE environment_id='tenant-a'") == 2 ]]
[[ $("${psql[@]}" -Atc "SELECT count(*) FROM public.novu_rust_ws_events WHERE environment_id='tenant-b'") == 1 ]]
[[ $("${psql[@]}" -Atc "SELECT count(*) FROM public.novu_rust_ws_retention_audit WHERE environment_id='tenant-a' AND deleted_count=2 AND reason='test'") == 1 ]]
# A locked event is skipped, not deleted or blocked; a later batch removes it.
"${psql[@]}" -c "INSERT INTO public.novu_rust_ws_events(environment_id,subscriber_internal_id,event,data,created_at) SELECT 'tenant-a','subscriber','locked-fixture','{}','2023-01-01'::timestamptz FROM generate_series(1,2)"
"${psql[@]}" -c "SET application_name='retention-test-lock'; BEGIN; SELECT id FROM public.novu_rust_ws_events WHERE environment_id='tenant-a' AND created_at<'2024-01-01' ORDER BY created_at,id LIMIT 1 FOR UPDATE; SELECT pg_sleep(5); COMMIT" >"$testdir/lock.log" 2>&1 &
lock_pid=$!
for ((i=0;i<100;i++)); do
 if [[ $("${psql[@]}" -Atc "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='retention-test-lock' AND wait_event='PgSleep')") == t ]]; then break; fi
 ((i<99)) || { echo 'event lock not acquired' >&2; exit 1; }
 sleep 0.05
done
skipped=$("$binary" delete tenant-a 2024-01-01T00:00:00Z 3 concurrent)
python3 -c 'import json,sys; assert json.loads(sys.argv[1])["count"]==2' "$skipped"
wait "$lock_pid"
remaining=$("$binary" delete tenant-a 2024-01-01T00:00:00Z 3 after-lock)
python3 -c 'import json,sys; assert json.loads(sys.argv[1])["count"]==1' "$remaining"
"${psql[@]}" -c "DELETE FROM public.novu_rust_ws_retention_grants WHERE database_principal='ws_retention_test'"
if "$binary" preview tenant-a 2024-01-01T00:00:00Z 2 >/dev/null 2>&1; then echo 'revoked grant accepted' >&2; exit 1; fi
printf '%s\n' 'WS retention role isolation, cutoff, batch, locked-row concurrency, audit rollback and revocation verified'
