#!/usr/bin/env bash
# Invoked only by test-postgres-failover-local.sh after its synchronous standby is ready.
set -euo pipefail
root=$1; testdir=$2; pgbin=$3
api="$root/apps/api/rust-api"; worker="$root/apps/worker/rust-queue"; api_pid=
cleanup_api() { if [[ -n "$api_pid" ]]; then kill "$api_pid" 2>/dev/null || true; wait "$api_pid" 2>/dev/null || true; fi; }
trap cleanup_api EXIT
"$pgbin/psql" "$TEST_PRIMARY_URL" -v ON_ERROR_STOP=1 -q <<'SQL'
CREATE ROLE timing_failover_api LOGIN;
GRANT USAGE ON SCHEMA public TO timing_failover_api;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_alert_timing_read(text,text,text,text,bigint,text,text),public.novu_rust_operator_jwt_alert_timing_change(text,text,text,text,bigint,text,text,uuid,bigint,integer,integer,integer,text),public.novu_rust_operator_jwt_alert_timing_request(text,text,text,text,bigint,text,text,uuid) TO timing_failover_api;
GRANT EXECUTE ON FUNCTION public.novu_rust_jwt_alert_timing_grant_change(text,text,text,text,bigint,text,text,uuid,text,text,text,text,boolean,boolean,text) TO timing_failover_api;
INSERT INTO public.novu_rust_environments VALUES('timing-ha','org');
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','admin','console','timing-ha'),('https://issuer.test','target','console','timing-ha');
INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='timing_failover_api';
INSERT INTO public.novu_rust_alert_timing_grants SELECT 'timing-ha',rolname,oid,true,true FROM pg_roles WHERE rolname IN ('timing_failover_api',session_user);
INSERT INTO public.novu_rust_jwt_alert_timing_grant_admins VALUES('https://issuer.test','admin','console','timing-ha','slack','ops',true,true,true);
INSERT INTO public.novu_rust_operator_jwt_alert_timing_grants VALUES('https://issuer.test','admin','console','timing-ha','slack','ops',true,true,true);
INSERT INTO public.novu_rust_delivery_operator_grants SELECT 'timing-ha',rolname,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_send','alert_view']) p WHERE rolname=session_user;
INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES('timing-ha','slack','ops','rejected');
INSERT INTO public.novu_rust_resend_projection_heartbeat(environment_id,last_completed_at) VALUES('timing-ha',clock_timestamp());
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,available_at) VALUES(gen_random_uuid(),'timing-ha','experimental_resend_email','{}',clock_timestamp()-interval '120 seconds');
SQL
python3 - "$api/tests/fixtures/operator_jwt/test-only-public.pem" "$api/tests/fixtures/operator_jwt/test-only-private.pem" "$testdir" <<'PY'
import base64,json,subprocess,sys,time,uuid
pub,priv,root=sys.argv[1:]
with open(pub) as f:key=f.read()
with open(root+'/keys.json','w') as f:json.dump({'issuer':'https://issuer.test','audience':'swallow','keys':[{'kid':'fixture','pem':key}]},f)
def b64(v):return base64.urlsafe_b64encode(v).decode().rstrip('=')
for who in ['admin','target']:
 h=b64(json.dumps({'alg':'RS256','typ':'at+jwt','kid':'fixture'},separators=(',',':')).encode());p=b64(json.dumps({'iss':'https://issuer.test','aud':'swallow','sub':who,'client_id':'console','iat':int(time.time())-1,'exp':int(time.time())+900,'jti':'test-'+who},separators=(',',':')).encode());m=(h+'.'+p).encode();sig=subprocess.run(['openssl','dgst','-sha256','-sign',priv],input=m,stdout=subprocess.PIPE,check=True).stdout
 with open(root+'/'+who+'.jwt','w') as f:f.write(m.decode()+'.'+b64(sig))
with open(root+'/timing.json','w') as f:json.dump({'requestId':str(uuid.uuid4()),'expectedRevision':'0','grace':60,'heartbeat':300,'cooldown':90,'reason':'failover timing'},f)
with open(root+'/grant.json','w') as f:json.dump({'requestId':str(uuid.uuid4()),'targetIssuer':'https://issuer.test','targetSubject':'target','targetClientId':'console','permission':'read','expected':False,'desired':True,'reason':'failover grant'},f)
with open(root+'/slack','w') as f:f.write('https://hooks.slack.com/services/A/B/C')
with open(root+'/targets.json','w') as f:json.dump({'targets':[{'channel':'slack','id':'ops','webhook_file':root+'/slack'}]},f)
PY
cargo build --quiet --locked --manifest-path "$api/Cargo.toml" --bin novu-rust-api
cargo build --quiet --locked --manifest-path "$worker/Cargo.toml" --bin alert-fanout --bin send-delivery-alert
api_port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
api_db=${TEST_FAILOVER_URL/postgres:\/\/postgres@/postgres:\/\/timing_failover_api@}
start_api() {
 env -i PATH="$PATH" DATABASE_URL="$api_db" NOVU_POSTGRES_ALLOW_INSECURE=true LISTEN_ADDR="127.0.0.1:$api_port" SWALLOW_OPERATOR_JWT_CONFIG_FILE="$testdir/keys.json" SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true "$api/target/debug/novu-rust-api" >"$testdir/timing-api.log" 2>&1 & api_pid=$!
 for _ in $(seq 1 100); do curl -fsS "http://127.0.0.1:$api_port/health/ready" >/dev/null 2>&1 && return; kill -0 "$api_pid"; sleep 0.1; done
 echo 'API did not become ready' >&2; return 1
}
stop_api() { kill "$api_pid" 2>/dev/null || true; wait "$api_pid" 2>/dev/null || true; api_pid=; }
start_api
admin=$(cat "$testdir/admin.jwt"); target=$(cat "$testdir/target.jwt")
timing_url="http://127.0.0.1:$api_port/v1/experimental/operator/alert-timing?environmentId=timing-ha&channel=slack&targetId=ops"
grant_url="http://127.0.0.1:$api_port/v1/experimental/operator/alert-timing-grants?environmentId=timing-ha&channel=slack&targetId=ops"
curl -fsS -X PUT -H "Authorization: Bearer $admin" -H 'Content-Type: application/json' --data-binary @"$testdir/timing.json" "$timing_url" >"$testdir/timing-response"
curl -fsS -X PUT -H "Authorization: Bearer $admin" -H 'Content-Type: application/json' --data-binary @"$testdir/grant.json" "$grant_url" >"$testdir/grant-response"
python3 - "$testdir/timing-response" "$testdir/grant-response" <<'PY'
import json,sys
with open(sys.argv[1]) as f:t=json.load(f)
with open(sys.argv[2]) as f:g=json.load(f)
assert t['settings']['revision']=='1' and t['replayed'] is False
assert g['changed'] is True and g['replayed'] is False
PY
for _ in $(seq 1 100); do
 count=$("$pgbin/psql" "$TEST_STANDBY_URL" -Atc "SELECT count(*) FROM public.novu_rust_alert_timing_settings WHERE environment_id='timing-ha' AND revision=1 AND grace=60" 2>/dev/null || true)
 grant=$("$pgbin/psql" "$TEST_STANDBY_URL" -Atc "SELECT count(*) FROM public.novu_rust_operator_jwt_alert_timing_grants WHERE environment_id='timing-ha' AND subject='target' AND can_read" 2>/dev/null || true)
 [[ "$count" == 1 && "$grant" == 1 ]] && break
 sleep 0.1
done
[[ "$count" == 1 && "$grant" == 1 ]]
"$pgbin/pg_ctl" -D "$TEST_LOCAL_PRIMARY_DATA" -m immediate -w stop >/dev/null
"$pgbin/pg_ctl" -D "$TEST_LOCAL_STANDBY_DATA" -w promote >/dev/null
stop_api
start_api
curl -fsS -H "Authorization: Bearer $target" "$timing_url" >"$testdir/read-after"
curl -fsS -X PUT -H "Authorization: Bearer $admin" -H 'Content-Type: application/json' --data-binary @"$testdir/timing.json" "$timing_url" >"$testdir/replay-after"
curl -fsS -X PUT -H "Authorization: Bearer $admin" -H 'Content-Type: application/json' --data-binary @"$testdir/grant.json" "$grant_url" >"$testdir/grant-replay-after"
python3 - "$testdir/read-after" "$testdir/replay-after" "$testdir/grant-replay-after" <<'PY'
import json,sys
with open(sys.argv[1]) as f:r=json.load(f)
with open(sys.argv[2]) as f:t=json.load(f)
with open(sys.argv[3]) as f:g=json.load(f)
assert r=={'revision':'1','grace':60,'heartbeat':300,'cooldown':90}
assert t['replayed'] is True and g['replayed'] is True
PY
[[ $("$pgbin/psql" "$TEST_STANDBY_URL" -Atc "SELECT (SELECT count(*) FROM public.novu_rust_operator_jwt_alert_timing_audit WHERE environment_id='timing-ha')=1 AND (SELECT count(*) FROM public.novu_rust_jwt_alert_timing_grant_audit WHERE environment_id='timing-ha')=1") == t ]]
set +e
env -i PATH="$PATH" DATABASE_URL="$TEST_FAILOVER_URL" NOVU_POSTGRES_ALLOW_INSECURE=true SWALLOW_ALERT_TIMING_SOURCE=postgres "$worker/target/debug/alert-fanout" delivery timing-ha 300 300 60 "$testdir/targets.json" >"$testdir/fanout.out" 2>"$testdir/fanout.err"
status=$?
set -e
[[ "$status" == 2 ]]
rg -q 'slack ops: attention' "$testdir/fanout.out"
python3 - "$testdir/grant.json" "$testdir/revoke.json" <<'PY'
import json,sys,uuid
with open(sys.argv[1]) as f:v=json.load(f)
v.update(requestId=str(uuid.uuid4()),expected=True,desired=False,reason='revoke after promotion')
with open(sys.argv[2],'w') as f:json.dump(v,f)
PY
curl -fsS -X PUT -H "Authorization: Bearer $admin" -H 'Content-Type: application/json' --data-binary @"$testdir/revoke.json" "$grant_url" >"$testdir/revoke-response"
[[ $(curl -sS -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $target" "$timing_url") == 401 ]]
[[ $("$pgbin/psql" "$TEST_STANDBY_URL" -Atc "SELECT count(*)=2 FROM public.novu_rust_jwt_alert_timing_grant_audit WHERE environment_id='timing-ha'") == t ]]
printf '%s\n' 'Alert timing and JWT grant survived synchronous failover; promoted fanout used stored timing.'
