#!/usr/bin/env python3
"""Real API process, restricted DB login and synthetic JWT on disposable PostgreSQL."""
import base64,json,os,pathlib,socket,subprocess,time,urllib.error,urllib.request,uuid
api=pathlib.Path(os.environ['TEST_API_DIR']);root=pathlib.Path(os.environ['TEST_FIXTURE_DIR']);url=os.environ['DATABASE_URL'];psql=os.environ['TEST_PSQL'];process=None

def sql(command):
    p=subprocess.run([psql,url,'-q','-At','-v','ON_ERROR_STOP=1','-c',command],capture_output=True,text=True,timeout=10)
    if p.returncode: raise RuntimeError('database fixture failed: '+p.stderr)
    return p.stdout.strip()
def b64(value):return base64.urlsafe_b64encode(value).decode().rstrip('=')
def token():
    head=b64(json.dumps({'alg':'RS256','typ':'at+jwt','kid':'fixture'},separators=(',',':')).encode())
    claims=b64(json.dumps({'iss':'https://issuer.test','aud':'swallow','sub':'alice','client_id':'console','iat':int(time.time())-1,'exp':int(time.time())+300,'jti':'smtp-http-fixture'},separators=(',',':')).encode())
    msg=(head+'.'+claims).encode();sig=subprocess.run(['openssl','dgst','-sha256','-sign',str(api/'tests/fixtures/operator_jwt/test-only-private.pem')],input=msg,stdout=subprocess.PIPE,check=True).stdout
    return msg.decode()+'.'+b64(sig)
def call(path,jwt,method='GET',body=None):
    request=urllib.request.Request(base+path,data=None if body is None else json.dumps(body).encode(),method=method,headers={'Authorization':'Bearer '+jwt,'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(request,timeout=3) as r:return r.status,json.load(r),r.headers.get('Cache-Control')
    except urllib.error.HTTPError as e:return e.code,None,e.headers.get('Cache-Control')
    except (urllib.error.URLError,TimeoutError):return 0,None,None
try:
    env,role=sql("SELECT environment_id||'|'||database_principal FROM public.novu_rust_smtp_auto_admins LIMIT 1").split('|')
    sql("UPDATE public.novu_rust_smtp_auto_admins SET can_read=true,can_write=true,can_audit=true WHERE environment_id='%s'; UPDATE public.novu_rust_operator_jwt_smtp_auto_policy_grants SET can_read=true,can_write=true,can_audit=true,can_history=true WHERE environment_id='%s'"%(env,env))
    public=(api/'tests/fixtures/operator_jwt/test-only-public.pem').read_text();config=root/'smtp-jwt.json';config.write_text(json.dumps({'issuer':'https://issuer.test','audience':'swallow','keys':[{'kid':'fixture','pem':public}]}))
    sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close();base=f'http://127.0.0.1:{port}'
    config_env={**os.environ,'DATABASE_URL':url.replace('postgres://postgres@','postgres://'+role+'@',1),'NOVU_POSTGRES_ALLOW_INSECURE':'true','LISTEN_ADDR':f'127.0.0.1:{port}','SWALLOW_OPERATOR_JWT_CONFIG_FILE':str(config),'SWALLOW_API_HEALTH_ENDPOINTS_ENABLED':'true'}
    process=subprocess.Popen([str(api/'target/debug/novu-rust-api')],env=config_env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    path='/v1/experimental/operator/smtp-auto-policy?environmentId='+env;jwt=token();deadline=time.monotonic()+20
    while True:
        status,policy,cache=call(path,jwt)
        if status==200:break
        if process.poll() is not None or time.monotonic()>deadline:raise RuntimeError('API not ready')
        time.sleep(.1)
    assert cache=='no-store' and policy['enabled'] is False and policy['revision']=='2'
    request_id=str(uuid.uuid4());body={'requestId':request_id,'expectedRevision':'2','enabled':True,'maxRetries':1,'delaySeconds':60,'cutoff':'now','reason':'http fixture'}
    status,result,cache=call(path,jwt,'PUT',body);assert status==200 and cache=='no-store' and result['policy']['revision']=='3'
    status,replayed,_=call(path,jwt,'PUT',body);assert status==200 and replayed['replayed'] is True
    audit='/v1/experimental/operator/smtp-auto-policy/audit?environmentId='+env
    status,events,_=call(audit,jwt);assert status==200 and any(event['requestId']==request_id and event['actorSubject']=='alice' for event in events['data'])
    status,history,_=call('/v1/experimental/operator/smtp-auto-history?environmentId='+env,jwt)
    assert status==200 and len(history['data'])==1 and 'secret' not in json.dumps(history)
    sql("UPDATE public.novu_rust_operator_jwt_smtp_auto_policy_grants SET can_read=false WHERE environment_id='%s'"%env)
    assert call(path,jwt)[0]==401
    print('Restricted real API enforced SMTP automatic retry policy JWT permissions and request replay.')
finally:
    if process is not None:
        process.terminate()
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:process.kill();process.wait()
