#!/usr/bin/env python3
"""Check committed Resend reconciliation and JWT access across synchronous promotion."""
import base64
import json
import os
import pathlib
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid

root=pathlib.Path(os.environ['TEST_ROOT']); fixture=pathlib.Path(os.environ['TEST_FIXTURE_DIR'])
primary=os.environ['TEST_PRIMARY_URL']; standby=os.environ['TEST_STANDBY_URL']; psql=os.environ['TEST_PSQL']; pgctl=os.environ['TEST_LOCAL_PG_CTL']
job=str(uuid.uuid4()); attempt=str(uuid.uuid4()); inflight_job=str(uuid.uuid4()); inflight_attempt=str(uuid.uuid4()); api=None; inflight=None

def sql(url, statement):
    p=subprocess.run([psql,url,'-q','-At','-v','ON_ERROR_STOP=1','-c',statement],capture_output=True,text=True,timeout=15)
    if p.returncode: raise RuntimeError('fixture SQL failed: '+p.stderr)
    return p.stdout.strip()
def control(data,*args):
    p=subprocess.run([pgctl,'-D',data,*args,'-w'],capture_output=True,text=True,timeout=25)
    if p.returncode: raise RuntimeError('fixture PostgreSQL control failed: '+p.stderr)
def b64(value): return base64.urlsafe_b64encode(value).decode().rstrip('=')
def token():
    header=b64(json.dumps({'alg':'RS256','typ':'at+jwt','kid':'fixture'},separators=(',',':')).encode())
    claims=b64(json.dumps({'iss':'https://issuer.test','aud':'swallow','sub':'viewer','client_id':'console','iat':int(time.time())-1,'exp':int(time.time())+900,'jti':'failover-fixture'},separators=(',',':')).encode())
    message=(header+'.'+claims).encode(); key=root/'apps/api/rust-api/tests/fixtures/operator_jwt/test-only-private.pem'
    signature=subprocess.run(['openssl','dgst','-sha256','-sign',str(key)],input=message,stdout=subprocess.PIPE,check=True).stdout
    return message.decode()+'.'+b64(signature)
def get(url, jwt=None):
    req=urllib.request.Request(url,headers={'Authorization':'Bearer '+jwt} if jwt else {})
    try:
        with urllib.request.urlopen(req,timeout=3) as res: return res.status,json.load(res)
    except urllib.error.HTTPError as e: return e.code,None
    except (urllib.error.URLError,TimeoutError,ConnectionError): return 0,None
def until(f, message):
    end=time.monotonic()+35
    while time.monotonic()<end:
        if api is not None and api.poll() is not None: raise RuntimeError('API exited unexpectedly')
        try:
            if f(): return
        except (urllib.error.URLError,TimeoutError,ConnectionError): pass
        time.sleep(.2)
    raise RuntimeError(message)
try:
    sql(primary,"""CREATE ROLE resend_reconcile_ha_api LOGIN;
GRANT USAGE ON SCHEMA public TO resend_reconcile_ha_api;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_result(text,text,text,text,bigint,uuid) TO resend_reconcile_ha_api;
INSERT INTO public.novu_rust_environments VALUES('resend-reconcile-ha','org'),('resend-other-ha','org');
INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='resend_reconcile_ha_api';
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','viewer','console','resend-reconcile-ha');
INSERT INTO public.novu_rust_operator_jwt_resend_result_grants VALUES('https://issuer.test','viewer','console','resend-reconcile-ha');
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'resend-reconcile-ha',rolname,'retry',oid FROM pg_roles WHERE rolname='postgres';
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES ('%s','resend-reconcile-ha','experimental_resend_email','{}','failed');
INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,attempt_id,same_request_replays) VALUES ('%s','resend-reconcile-ha','unknown','%s',1);
SELECT public.novu_rust_resolve_email_sms('resend-reconcile-ha','%s','retry','fixture','reviewed');""" % (job,job,attempt,job))
    until(lambda:sql(standby,"SELECT count(*) FROM public.novu_rust_delivery_reconciliations WHERE job_id='%s' AND resend_attempt_id='%s' AND resend_replay_reserved"%(job,attempt))=='1','reconciliation did not replicate')
    # Block after the audit insert and attempt delete, before the job update.
    # The single function call is one uncommitted PostgreSQL transaction.
    sql(primary,"""INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES ('%s','resend-reconcile-ha','experimental_resend_email','{}','failed');
INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,attempt_id,same_request_replays) VALUES ('%s','resend-reconcile-ha','unknown','%s',1);
CREATE FUNCTION public.fixture_pause_reconciliation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(120); RETURN NEW; END $$;
CREATE TRIGGER fixture_pause_reconciliation BEFORE UPDATE ON public.novu_queue_jobs FOR EACH ROW WHEN (OLD.id='%s'::uuid) EXECUTE FUNCTION public.fixture_pause_reconciliation();"""%(inflight_job,inflight_job,inflight_attempt,inflight_job))
    inflight=subprocess.Popen([psql,primary,'-q','-v','ON_ERROR_STOP=1','-c',"SELECT public.novu_rust_resolve_email_sms('resend-reconcile-ha','%s','retry','fixture','inflight')"%inflight_job],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    until(lambda:sql(primary,"SELECT count(*) FROM pg_stat_activity WHERE wait_event='PgSleep' AND query LIKE '%%"+inflight_job+"%%'")=='1','reconciliation did not reach the blocked update')
    assert inflight.poll() is None
    assert sql(standby,"SELECT count(*) FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id='%s' AND j.status='failed' AND a.state='unknown' AND a.attempt_id='%s'"%(inflight_job,inflight_attempt))=='1'
    assert sql(standby,"SELECT count(*) FROM public.novu_rust_delivery_reconciliations WHERE job_id='%s'"%inflight_job)=='0'

    pub=(root/'apps/api/rust-api/tests/fixtures/operator_jwt/test-only-public.pem').read_text()
    keys=fixture/'operator-keys.json';keys.write_text(json.dumps({'issuer':'https://issuer.test','audience':'swallow','keys':[{'kid':'fixture','pem':pub}]}))
    sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close()
    cluster=os.environ['TEST_FAILOVER_URL'].replace('postgres://postgres@','postgres://resend_reconcile_ha_api@',1)
    cfg={**os.environ,'DATABASE_URL':cluster,'NOVU_POSTGRES_ALLOW_INSECURE':'true','LISTEN_ADDR':f'127.0.0.1:{port}','SWALLOW_OPERATOR_JWT_CONFIG_FILE':str(keys),'SWALLOW_API_HEALTH_ENDPOINTS_ENABLED':'true'}
    api=subprocess.Popen([os.environ['TEST_API_BINARY']],env=cfg,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    base=f'http://127.0.0.1:{port}'; jwt=token();path=f'/v1/experimental/operator/resend-result?environmentId=resend-reconcile-ha&jobId={job}'
    def good():
        status,value=get(base+path,jwt)
        return status==200 and value['lastResolvedAttemptId']==attempt and value['lastResolvedReplayReserved'] is True and value['attemptId'] is None
    until(good,'pre-promotion authorized result unavailable')
    assert get(base+path,None)[0]==401
    assert get(base+path.replace('resend-reconcile-ha','resend-other-ha'),jwt)[0]==401
    control(os.environ['TEST_LOCAL_PRIMARY_DATA'],'-m','immediate','stop');control(os.environ['TEST_LOCAL_STANDBY_DATA'],'promote')
    until(good,'API did not reconnect with preserved reconciliation')
    inflight.wait(timeout=10)
    assert inflight.returncode != 0
    assert sql(standby,"SELECT count(*) FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id='%s' AND j.status='failed' AND a.state='unknown' AND a.attempt_id='%s'"%(inflight_job,inflight_attempt))=='1'
    assert sql(standby,"SELECT count(*) FROM public.novu_rust_delivery_reconciliations WHERE job_id='%s'"%inflight_job)=='0'
    sql(standby,"DROP TRIGGER fixture_pause_reconciliation ON public.novu_queue_jobs; DROP FUNCTION public.fixture_pause_reconciliation(); SELECT public.novu_rust_resolve_email_sms('resend-reconcile-ha','%s','retry','fixture','after promotion')"%inflight_job)
    other_path=path.replace(job,inflight_job)
    status,value=get(base+other_path,jwt)
    assert status==200 and value['lastResolvedAttemptId']==inflight_attempt and value['lastResolvedReplayReserved'] is True and value['attemptId'] is None
    assert get(base+path.replace('resend-reconcile-ha','resend-other-ha'),jwt)[0]==401
    assert sql(standby,"SELECT count(*) FROM public.novu_queue_jobs WHERE id='%s' AND status='pending'"%job)=='1'
    print('Committed evidence survived promotion; in-flight reconciliation rolled back and could be retried after API reconnection.')
finally:
    if inflight is not None and inflight.poll() is None:
        inflight.terminate()
        try:inflight.wait(timeout=10)
        except subprocess.TimeoutExpired:inflight.kill();inflight.wait()
    if api is not None:
        api.terminate()
        try:api.wait(timeout=10)
        except subprocess.TimeoutExpired:api.kill();api.wait()
