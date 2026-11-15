#!/usr/bin/env python3
"""Exercise JWT automatic retry policy and grant changes across a local synchronous promotion."""
import base64,json,os,pathlib,socket,subprocess,time,urllib.error,urllib.request,uuid
root=pathlib.Path(os.environ['TEST_ROOT']);directory=pathlib.Path(os.environ['TEST_FIXTURE_DIR'])
primary=os.environ['TEST_PRIMARY_URL'];standby=os.environ['TEST_STANDBY_URL'];psql=os.environ['TEST_PSQL'];pgctl=os.environ['TEST_LOCAL_PG_CTL'];api=None;worker=None

def sql(url,query):
    p=subprocess.run([psql,url,'-q','-At','-v','ON_ERROR_STOP=1','-c',query],capture_output=True,text=True,timeout=15)
    if p.returncode:raise RuntimeError('fixture SQL failed: '+p.stderr)
    return p.stdout.strip()
def control(data,*args):
    p=subprocess.run([pgctl,'-D',data,*args,'-w'],capture_output=True,text=True,timeout=25)
    if p.returncode:raise RuntimeError('fixture database control failed: '+p.stderr)
def b64(value):return base64.urlsafe_b64encode(value).decode().rstrip('=')
def token(subject):
    h=b64(json.dumps({'alg':'RS256','typ':'at+jwt','kid':'fixture'},separators=(',',':')).encode())
    p=b64(json.dumps({'iss':'https://issuer.test','aud':'swallow','sub':subject,'client_id':'console','iat':int(time.time())-1,'exp':int(time.time())+900,'jti':'smtp-policy-ha-'+subject},separators=(',',':')).encode())
    msg=(h+'.'+p).encode();key=root/'apps/api/rust-api/tests/fixtures/operator_jwt/test-only-private.pem'
    sig=subprocess.run(['openssl','dgst','-sha256','-sign',str(key)],input=msg,stdout=subprocess.PIPE,check=True).stdout
    return msg.decode()+'.'+b64(sig)
def call(path,jwt,method='GET',body=None):
    req=urllib.request.Request(base+path,data=None if body is None else json.dumps(body).encode(),method=method,headers={'Authorization':'Bearer '+jwt,'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=3) as r:return r.status,json.load(r)
    except urllib.error.HTTPError as e:return e.code,None
    except (urllib.error.URLError,TimeoutError,ConnectionError):return 0,None
def until(predicate,message):
    deadline=time.monotonic()+35
    while time.monotonic()<deadline:
        if api is not None and api.poll() is not None:raise RuntimeError('API exited unexpectedly')
        if predicate():return
        time.sleep(.2)
    raise RuntimeError(message)
try:
    sql(primary,"""CREATE ROLE smtp_policy_ha_api LOGIN; GRANT USAGE ON SCHEMA public TO smtp_policy_ha_api;
GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_smtp_auto_policy_read(text,text,text,text,bigint),public.novu_rust_operator_jwt_smtp_auto_policy_change(text,text,text,text,bigint,uuid,bigint,boolean,integer,integer,text,text),public.novu_rust_operator_jwt_smtp_auto_policy_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_smtp_auto_policy_audit_read(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_smtp_auto_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_smtp_auto_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_smtp_auto_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_smtp_auto_grant_audit_read(text,text,text,text,bigint,uuid) TO smtp_policy_ha_api;
INSERT INTO public.novu_rust_environments VALUES('smtp-policy-ha','org'),('smtp-other-ha','org');
INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','admin','console','smtp-policy-ha'),('https://issuer.test','target','console','smtp-policy-ha');
INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='smtp_policy_ha_api';
INSERT INTO public.novu_rust_operator_jwt_smtp_auto_policy_grants VALUES('https://issuer.test','admin','console','smtp-policy-ha',true,true,true,true);
INSERT INTO public.novu_rust_operator_jwt_smtp_auto_grant_admins VALUES('https://issuer.test','admin','console','smtp-policy-ha',true,true);
INSERT INTO public.novu_rust_operator_jwt_smtp_auto_grant_audit_viewers VALUES('https://issuer.test','admin','console','smtp-policy-ha');
INSERT INTO public.novu_rust_smtp_auto_admins SELECT 'smtp-policy-ha',rolname,oid,true,true,true FROM pg_roles WHERE rolname='smtp_policy_ha_api';
CREATE ROLE smtp_auto_ha_worker LOGIN; GRANT USAGE ON SCHEMA public TO smtp_auto_ha_worker;
GRANT EXECUTE ON FUNCTION public.novu_rust_smtp_auto_retry_batch(text,integer) TO smtp_auto_ha_worker;
INSERT INTO public.novu_rust_smtp_auto_retry_workers SELECT 'smtp-policy-ha',rolname,oid FROM pg_roles WHERE rolname='smtp_auto_ha_worker';""")
    pub=(root/'apps/api/rust-api/tests/fixtures/operator_jwt/test-only-public.pem').read_text();keys=directory/'smtp-policy-keys.json';keys.write_text(json.dumps({'issuer':'https://issuer.test','audience':'swallow','keys':[{'kid':'fixture','pem':pub}]}))
    sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close();base=f'http://127.0.0.1:{port}'
    cfg={**os.environ,'DATABASE_URL':os.environ['TEST_FAILOVER_URL'].replace('postgres://postgres@','postgres://smtp_policy_ha_api@',1),'NOVU_POSTGRES_ALLOW_INSECURE':'true','LISTEN_ADDR':f'127.0.0.1:{port}','SWALLOW_OPERATOR_JWT_CONFIG_FILE':str(keys),'SWALLOW_API_HEALTH_ENDPOINTS_ENABLED':'true'}
    api=subprocess.Popen([os.environ['TEST_API_BINARY']],env=cfg,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    admin,target=token('admin'),token('target');policy='/v1/experimental/operator/smtp-auto-policy?environmentId=smtp-policy-ha';grants='/v1/experimental/operator/smtp-auto-grants?environmentId=smtp-policy-ha'
    until(lambda:call(policy,admin)[0]==200,'API failed to become ready')
    assert call(policy,target)[0]==401
    policy_id=str(uuid.uuid4());grant_id=str(uuid.uuid4());change={'requestId':policy_id,'expectedRevision':'0','enabled':True,'maxRetries':2,'delaySeconds':10,'cutoff':'all','reason':'HA fixture'}
    grant={'requestId':grant_id,'targetIssuer':'https://issuer.test','targetSubject':'target','targetClientId':'console','resource':'smtp-auto','permission':'read','expected':False,'desired':True,'reason':'HA fixture'}
    status,result=call(policy,admin,'PUT',change);assert status==200 and result['policy']=={'enabled':True,'maxRetries':2,'delaySeconds':10,'eligibleSince':'-infinity','revision':'1'}
    status,result=call(grants,admin,'PUT',grant);assert status==200 and result['changed'] is True
    assert call(policy,target)[0]==200
    until(lambda:sql(standby,"SELECT count(*) FROM public.novu_rust_operator_jwt_smtp_auto_policy_audit WHERE environment_id='smtp-policy-ha'")=='1' and sql(standby,"SELECT count(*) FROM public.novu_rust_operator_jwt_smtp_auto_grant_audit WHERE environment_id='smtp-policy-ha'")=='1','changes did not replicate')
    worker_cfg={**os.environ,'DATABASE_URL':os.environ['TEST_FAILOVER_URL'].replace('postgres://postgres@','postgres://smtp_auto_ha_worker@',1),'NOVU_POSTGRES_ALLOW_INSECURE':'true','NOVU_SMTP_ENVIRONMENT_ID':'smtp-policy-ha','SWALLOW_SMTP_AUTO_RETRY_INTERVAL_SECONDS':'1'}
    worker=subprocess.Popen([os.environ['TEST_AUTO_WORKER']],env=worker_cfg,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    time.sleep(2);assert worker.poll() is None
    control(os.environ['TEST_LOCAL_PRIMARY_DATA'],'-m','immediate','stop');control(os.environ['TEST_LOCAL_STANDBY_DATA'],'promote')
    until(lambda:call(policy,target)[0]==200,'API did not reconnect with target grant')
    status,value=call(policy,admin);assert status==200 and value=={'enabled':True,'maxRetries':2,'delaySeconds':10,'eligibleSince':'-infinity','revision':'1'}
    status,value=call(policy,admin,'PUT',change);assert status==200 and value['replayed'] is True
    status,value=call(grants,admin,'PUT',grant);assert status==200 and value['replayed'] is True
    status,value=call(policy.replace('smtp-policy-ha','smtp-other-ha'),admin);assert status==401
    revoke={**grant,'requestId':str(uuid.uuid4()),'expected':True,'desired':False,'reason':'revoke fixture'}
    status,value=call(grants,admin,'PUT',revoke);assert status==200 and value['changed'] is True
    assert call(policy,target)[0]==401
    assert sql(standby,"SELECT count(*) FROM public.novu_rust_operator_jwt_smtp_auto_grant_audit WHERE environment_id='smtp-policy-ha'")=='2'
    sql(standby,"""INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES('11111111-1111-4111-8111-111111111111','smtp-policy-ha','experimental_smtp_email','{}','failed');
INSERT INTO public.novu_rust_smtp_attempts(job_id,environment_id,state,updated_at) VALUES('11111111-1111-4111-8111-111111111111','smtp-policy-ha','unknown',clock_timestamp()-interval '1 hour');""")
    until(lambda:sql(standby,"SELECT count(*) FROM public.novu_rust_smtp_auto_retries WHERE environment_id='smtp-policy-ha'")=='1','retry did not run after promotion')
    assert worker.poll() is None
    assert sql(standby,"SELECT status FROM public.novu_queue_jobs WHERE id='11111111-1111-4111-8111-111111111111'")=='pending'
    assert sql(standby,"SELECT count(*) FROM public.novu_rust_smtp_attempts WHERE job_id='11111111-1111-4111-8111-111111111111'")=='0'
    print('SMTP automatic retry policy, delegated rights and recovery survived synchronous promotion.')
finally:
    if worker is not None:
        worker.terminate()
        try:worker.wait(timeout=10)
        except subprocess.TimeoutExpired:worker.kill();worker.wait()
    if api is not None:
        api.terminate()
        try:api.wait(timeout=10)
        except subprocess.TimeoutExpired:api.kill();api.wait()
