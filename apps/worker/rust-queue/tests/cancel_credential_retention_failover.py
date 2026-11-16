"""Single-host synchronous PostgreSQL promotion with a real restricted JWT API."""
import base64,json,os,socket,subprocess,threading,time,urllib.error,urllib.request,uuid
from pathlib import Path
root=Path(os.environ['TEST_ROOT']);directory=Path(os.environ['TEST_FIXTURE_DIR'])
psql=os.environ['TEST_PSQL'];primary=os.environ['TEST_PRIMARY_URL'];standby=os.environ['TEST_STANDBY_URL']
cluster=os.environ['TEST_FAILOVER_URL'].replace('postgres://postgres@','postgres://retention_ha_api@',1)
pgctl=os.environ['TEST_LOCAL_PG_CTL'];api=os.environ['TEST_API_BINARY'];process=None;blocker=None

def sql(url,statement):
    return subprocess.run([psql,url,'-v','ON_ERROR_STOP=1','-Atc',statement],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True).stdout.strip()
def b64(v):return base64.urlsafe_b64encode(v).decode().rstrip('=')
private=root/'apps/api/rust-api/tests/fixtures/operator_jwt/test-only-private.pem'
public=root/'apps/api/rust-api/tests/fixtures/operator_jwt/test-only-public.pem'
(directory/'retention-keys.json').write_text(json.dumps({'issuer':'https://issuer.test','audience':'swallow','keys':[{'kid':'fixture','pem':public.read_text()}]}))
h=b64(json.dumps({'alg':'RS256','typ':'at+jwt','kid':'fixture'},separators=(',',':')).encode())
p=b64(json.dumps({'iss':'https://issuer.test','aud':'swallow','sub':'admin','client_id':'console','iat':int(time.time())-1,'exp':int(time.time())+900,'jti':'fixture'},separators=(',',':')).encode())
message=(h+'.'+p).encode();signature=subprocess.run(['openssl','dgst','-sha256','-sign',str(private)],input=message,stdout=subprocess.PIPE,check=True).stdout
token=message.decode()+'.'+b64(signature)
with socket.socket() as s:s.bind(('127.0.0.1',0));port=s.getsockname()[1]
origin=f'http://127.0.0.1:{port}';path='/v1/experimental/operator/resend-cancel-credential-retention?environmentId=ha-retention'

def start():
    env={'PATH':os.environ['PATH'],'DATABASE_URL':cluster,'NOVU_POSTGRES_ALLOW_INSECURE':'true','LISTEN_ADDR':f'127.0.0.1:{port}','SWALLOW_OPERATOR_JWT_CONFIG_FILE':str(directory/'retention-keys.json'),'SWALLOW_API_HEALTH_ENDPOINTS_ENABLED':'true'}
    proc=subprocess.Popen([api],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    for _ in range(150):
        if proc.poll() is not None:raise RuntimeError('API exited')
        try:
            with urllib.request.urlopen(origin+'/health/ready',timeout=1) as r:
                if r.status==200:return proc
        except Exception:time.sleep(.1)
    raise RuntimeError('API readiness timeout')
def request(method,suffix='',body=None):
    req=urllib.request.Request(origin+path+suffix,method=method,data=None if body is None else json.dumps(body).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
    try:r=urllib.request.urlopen(req,timeout=8)
    except urllib.error.HTTPError as e:r=e
    with r:return r.status,json.loads(r.read())

try:
    sql(primary,"""CREATE ROLE retention_ha_api LOGIN; GRANT USAGE ON SCHEMA public TO retention_ha_api;
    GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_cancel_cred_retention_submit(text,text,text,text,bigint,uuid,integer,integer,text),public.novu_rust_operator_jwt_cancel_cred_retention_get(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_cancel_cred_retention_audit(text,text,text,text,bigint,uuid) TO retention_ha_api;
    INSERT INTO public.novu_rust_environments VALUES('ha-retention','org');
    INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','admin','console','ha-retention');
    INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='retention_ha_api';
    INSERT INTO public.novu_rust_resend_cancel_cred_retention_operators SELECT 'ha-retention',rolname,oid FROM pg_roles WHERE rolname='retention_ha_api';
    INSERT INTO public.novu_rust_cancel_cred_retention_audit_db_viewers SELECT 'ha-retention',rolname,oid FROM pg_roles WHERE rolname='retention_ha_api';
    INSERT INTO public.novu_rust_operator_jwt_cancel_cred_retention_grants VALUES('https://issuer.test','admin','console','ha-retention');
    INSERT INTO public.novu_rust_operator_jwt_cancel_cred_retention_audit_viewers VALUES('https://issuer.test','admin','console','ha-retention');
    INSERT INTO public.novu_rust_resend_cancel_credential_health VALUES('ha-retention','11111111-1111-4111-8111-111111111111',session_user,-1,clock_timestamp()-interval '5 days','unhealthy',clock_timestamp()-interval '3 days'),('ha-retention','22222222-2222-4222-8222-222222222222',session_user,-1,clock_timestamp()-interval '5 days','unhealthy',clock_timestamp()-interval '3 days');""")
    process=start();rid=str(uuid.uuid4());body={'requestId':rid,'olderThanSeconds':86401,'batchLimit':1,'reason':'ha test'}
    # Forward the request to the real API but intentionally drop its successful
    # response, reproducing an operator whose request outcome is unknown.
    proxy=socket.socket();proxy.bind(('127.0.0.1',0));proxy.listen(1);proxy_port=proxy.getsockname()[1];observed=[]
    def drop_response():
        with proxy:
            downstream,_=proxy.accept()
            with downstream, socket.create_connection(('127.0.0.1',port),timeout=10) as upstream:
                data=b''
                while b'\r\n\r\n' not in data:data+=downstream.recv(65536)
                headers,rest=data.split(b'\r\n\r\n',1)
                length=int(next(line.split(b':',1)[1].strip() for line in headers.split(b'\r\n') if line.lower().startswith(b'content-length:')))
                while len(rest)<length:rest+=downstream.recv(65536)
                upstream.sendall(headers+b'\r\n\r\n'+rest)
                observed.append(upstream.recv(4096).split(b'\r\n',1)[0])
    thread=threading.Thread(target=drop_response,daemon=True);thread.start()
    lost=urllib.request.Request(f'http://127.0.0.1:{proxy_port}'+path,method='POST',data=json.dumps(body).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json','Connection':'close'})
    try:urllib.request.urlopen(lost,timeout=10);raise AssertionError('response was not dropped')
    except (urllib.error.URLError,ConnectionError,OSError):pass
    thread.join(timeout=10);assert not thread.is_alive() and observed and b' 200 ' in observed[0],observed
    for _ in range(100):
        if sql(standby,"SELECT (SELECT count(*)=1 FROM public.novu_rust_operator_jwt_cancel_cred_retention_requests WHERE environment_id='ha-retention') AND (SELECT count(*)=1 FROM public.novu_rust_resend_cancel_credential_health WHERE environment_id='ha-retention')")=='t':break
        time.sleep(.1)
    else:raise RuntimeError('standby did not replay committed retention')
    # Pause a second request after deleting a row but before its audit insert.
    # Terminating the primary here must roll back the whole transaction.
    sql(primary,"CREATE FUNCTION public.test_retention_ha_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(9911001); RETURN NEW; END $$; CREATE TRIGGER test_retention_ha_pause BEFORE INSERT ON public.novu_rust_resend_cancel_cred_retention_audit FOR EACH ROW EXECUTE FUNCTION public.test_retention_ha_pause()")
    blocker=subprocess.Popen([psql,primary,'-v','ON_ERROR_STOP=1','-c','SELECT pg_advisory_lock(9911001); SELECT pg_sleep(60)'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    for _ in range(100):
        if sql(primary,"SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=9911001 AND granted)")=='t':break
        if blocker.poll() is not None:raise RuntimeError('barrier exited')
        time.sleep(.1)
    else:raise RuntimeError('barrier did not lock')
    pending_id=str(uuid.uuid4());pending_body={'requestId':pending_id,'olderThanSeconds':86401,'batchLimit':1,'reason':'inflight'};pending_result=[]
    def submit_pending():
        try:pending_result.append(request('POST',body=pending_body)[0])
        except Exception:pending_result.append('connection-lost')
    pending=threading.Thread(target=submit_pending,daemon=True);pending.start()
    for _ in range(100):
        if sql(primary,"SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE '%operator_jwt_cancel_cred_retention_submit%')")=='t':break
        if not pending.is_alive():raise RuntimeError('inflight request finished before barrier')
        time.sleep(.1)
    else:raise RuntimeError('inflight request did not reach audit barrier')
    subprocess.run([pgctl,'-D',os.environ['TEST_LOCAL_PRIMARY_DATA'],'-m','immediate','stop','-w'],check=True,stdout=subprocess.DEVNULL)
    pending.join(timeout=10);assert not pending.is_alive(), 'inflight API call did not end after primary stop'
    blocker.wait(timeout=10);blocker=None
    process.terminate();process.wait(timeout=10);process=None
    subprocess.run([pgctl,'-D',os.environ['TEST_LOCAL_STANDBY_DATA'],'promote','-w'],check=True,stdout=subprocess.DEVNULL)
    process=start();status,result=request('POST',body=body);assert status==200 and result['deleted']==1 and result['replayed'] is True,(status,result)
    status,record=request('GET','&requestId='+rid);assert status==200 and record['found'] and record['deleted']==1,(status,record)
    audit_req=urllib.request.Request(origin+'/v1/experimental/operator/resend-cancel-credential-retention/audit?environmentId=ha-retention',headers={'Authorization':'Bearer '+token})
    with urllib.request.urlopen(audit_req,timeout=8) as response:audit=json.loads(response.read())
    assert len(audit['data'])==1 and audit['data'][0]['requestId']==rid
    assert sql(standby,"SELECT count(*) FROM public.novu_rust_resend_cancel_credential_health WHERE environment_id='ha-retention'")=='1'
    assert sql(standby,"SELECT count(*) FROM public.novu_rust_resend_cancel_cred_retention_audit WHERE environment_id='ha-retention'")=='1'
    status,missing=request('GET','&requestId='+pending_id);assert status==200 and missing['found'] is False,(status,missing)
    sql(standby,"DROP TRIGGER test_retention_ha_pause ON public.novu_rust_resend_cancel_cred_retention_audit; DROP FUNCTION public.test_retention_ha_pause()")
    status,result=request('POST',body=pending_body);assert status==200 and result['deleted']==1 and result['replayed'] is False,(status,result)
    status,result=request('POST',body=pending_body);assert status==200 and result['deleted']==1 and result['replayed'] is True,(status,result)
    assert sql(standby,"SELECT count(*) FROM public.novu_rust_resend_cancel_credential_health WHERE environment_id='ha-retention'")=='0'
    assert sql(standby,"SELECT count(*) FROM public.novu_rust_resend_cancel_cred_retention_audit WHERE environment_id='ha-retention'")=='2'
    sql(standby,"DELETE FROM public.novu_rust_operator_jwt_cancel_cred_retention_audit_viewers WHERE environment_id='ha-retention'")
    try:urllib.request.urlopen(audit_req,timeout=8);raise AssertionError('revoked audit access succeeded')
    except urllib.error.HTTPError as e:assert e.code==401,e.code
    print('synchronous promotion preserved replay, rolled back in-flight retention, and enforced audit revocation')
finally:
    if blocker is not None:
        blocker.terminate()
        try:blocker.wait(timeout=10)
        except subprocess.TimeoutExpired:blocker.kill();blocker.wait()
    if process is not None:
        process.terminate()
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:process.kill();process.wait()
