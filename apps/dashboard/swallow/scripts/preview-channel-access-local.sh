#!/usr/bin/env bash
# Disposable local PostgreSQL + restricted API login + test JWT for UI verification.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(cd ../../.. && pwd)
api="$root/apps/api/rust-api"; worker="$root/apps/worker/rust-queue"
pgbin=${SWALLOW_TEST_POSTGRES_BIN:-/opt/homebrew/opt/postgresql@17/bin}
testdir=$(mktemp -d /private/tmp/swallow-channel-ui.XXXXXX)
api_pid=''
cleanup() {
 if [[ -n "$api_pid" ]]; then kill "$api_pid" 2>/dev/null || true; wait "$api_pid" 2>/dev/null || true; fi
 "$pgbin/pg_ctl" -D "$testdir/data" -m immediate stop >/dev/null 2>&1 || true
 python3 -c 'import shutil,sys; shutil.rmtree(sys.argv[1])' "$testdir"
}
trap cleanup EXIT
"$pgbin/initdb" -D "$testdir/data" -U postgres -A trust --no-locale >/dev/null
pg_port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
"$pgbin/pg_ctl" -D "$testdir/data" -o "-h 127.0.0.1 -p $pg_port -k $testdir" -l "$testdir/db.log" start >/dev/null
"$pgbin/createdb" -h 127.0.0.1 -p "$pg_port" -U postgres ui_test
export DATABASE_URL="postgres://postgres@127.0.0.1:$pg_port/ui_test" NOVU_POSTGRES_ALLOW_INSECURE=true CARGO_INCREMENTAL=0
cargo run --quiet --locked --manifest-path "$worker/Cargo.toml" --bin migrate-worker -- apply api "$api/migrations" --through 17
cargo run --quiet --locked --manifest-path "$worker/Cargo.toml" --bin migrate-worker -- apply worker "$worker/migrations"
cargo run --quiet --locked --manifest-path "$worker/Cargo.toml" --bin migrate-worker -- apply api "$api/migrations"
"$pgbin/psql" "$DATABASE_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE ui_channel_fixture LOGIN;
GRANT USAGE ON SCHEMA public TO ui_channel_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_channel_access_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_channel_access_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_channel_access_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_channel_access_grant_audit_read(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_channel_access_grant_audit_filtered(text,text,text,text,bigint,text,uuid) TO ui_channel_fixture;
INSERT INTO public.novu_rust_environments VALUES ('ui-fixture-env','org');
INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_catalog.pg_roles WHERE rolname='ui_channel_fixture';
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env'),('https://issuer.test','target-fixture','console','ui-fixture-env');
INSERT INTO public.novu_rust_operator_jwt_channel_access_grant_scopes VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env','sendgrid',true,true);
INSERT INTO public.novu_rust_operator_jwt_channel_access_grant_audit_scopes VALUES ('https://issuer.test','ui-fixture','console','ui-fixture-env','sendgrid');
SQL
node --input-type=module - "$root" "$testdir" <<'JS'
import {readFile,writeFile} from 'node:fs/promises';
import {SignJWT,importPKCS8} from './node_modules/jose/dist/webapi/index.js';
const root=process.argv[2], dir=process.argv[3], keyDir=`${root}/apps/api/rust-api/tests/fixtures/operator_jwt`;
const [privatePem,publicPem]=await Promise.all([readFile(`${keyDir}/test-only-private.pem`,'utf8'),readFile(`${keyDir}/test-only-public.pem`,'utf8')]);
const now=Math.floor(Date.now()/1000);
const token=await new SignJWT({client_id:'console',jti:'ui-fixture'}).setProtectedHeader({alg:'RS256',typ:'at+jwt',kid:'key'}).setIssuer('https://issuer.test').setSubject('ui-fixture').setAudience('swallow').setIssuedAt(now).setExpirationTime(now+600).sign(await importPKCS8(privatePem,'RS256'));
await writeFile(`${dir}/token`,token,{mode:0o600});
await writeFile(`${dir}/jwt-config`,JSON.stringify({issuer:'https://issuer.test',audience:'swallow',keys:[{kid:'key',pem:publicPem}]}),{mode:0o600});
JS
api_port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
cargo build --quiet --locked --manifest-path "$api/Cargo.toml" --bin novu-rust-api
env -i PATH="$PATH" DATABASE_URL="postgres://ui_channel_fixture@127.0.0.1:$pg_port/ui_test" NOVU_POSTGRES_ALLOW_INSECURE=true LISTEN_ADDR="127.0.0.1:$api_port" SWALLOW_OPERATOR_JWT_CONFIG_FILE="$testdir/jwt-config" SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true SWALLOW_CORS_ALLOWED_ORIGINS="http://127.0.0.1:4249" "$api/target/debug/novu-rust-api" >"$testdir/api.log" 2>&1 &
api_pid=$!
for ((i=0;i<60;i++)); do
 if [[ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$api_port/health/live")" == 200 ]]; then break; fi
 if ! kill -0 "$api_pid" 2>/dev/null; then echo 'API fixture failed to start' >&2; exit 1; fi
 ((i<59)) || { echo 'API fixture not ready' >&2; exit 1; }
 sleep 1
done
printf 'FIXTURE_DIR=%s\nAPI_URL=http://127.0.0.1:%s\nPG_PORT=%s\n' "$testdir" "$api_port" "$pg_port"
wait "$api_pid"
