import concurrent.futures,json,os,subprocess,threading,time,urllib.error,urllib.request,urllib.parse
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
origin=os.environ['TEST_API']; token=os.environ['TEST_JWT']; admin=os.environ['DATABASE_URL']; psql=os.environ['TEST_PSQL']
def get(query,credential=token,endpoint='resend-cancel-list'):
    headers={'Authorization':'Bearer '+credential} if credential else {}
    try:
        with urllib.request.urlopen(urllib.request.Request(origin+'/v1/experimental/operator/'+endpoint+'?'+query,headers=headers),timeout=5) as r:return r.status,json.loads(r.read())
    except urllib.error.HTTPError as e:return e.code,json.loads(e.read())
def sql(q):subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',q],check=True,capture_output=True)
def resolve(body):
    req=urllib.request.Request(origin+'/v1/experimental/operator/resend-cancel-resolution?environmentId=cancel-api-env',data=json.dumps(body).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'},method='POST')
    try:
        with urllib.request.urlopen(req,timeout=5) as r:return r.status,json.loads(r.read())
    except urllib.error.HTTPError as e:return e.code,json.loads(e.read())
def grant(method,path,body=None):
    req=urllib.request.Request(origin+'/v1/experimental/operator/resend-cancel-grants'+path,data=json.dumps(body).encode() if body is not None else None,headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'},method=method)
    try:
        with urllib.request.urlopen(req,timeout=5) as r:return r.status,json.loads(r.read())
    except urllib.error.HTTPError as e:return e.code,json.loads(e.read())
target=os.environ['TEST_TARGET_JWT']
target_params={'environmentId':'cancel-api-env','targetIssuer':'https://issuer.test','targetSubject':'target','targetClientId':'console','resource':'resend-cancel'}
for n,permission in enumerate(['list','detail','confirmed','not_cancelled','submit','command-health','command-list','command-review','evidence-health','evidence-list','legacy-health','legacy-list'],1):
    params={**target_params,'permission':permission};status,data=grant('GET','?'+urllib.parse.urlencode(params));assert status==200 and data['targetExists'] and not data['granted']
    body={'requestId':'11111111-1111-4111-8111-'+str(n).zfill(12),'targetIssuer':'https://issuer.test','targetSubject':'target','targetClientId':'console','resource':'resend-cancel','permission':permission,'expected':False,'desired':True,'reason':'delegate'}
    if n==1:
        sql("CREATE FUNCTION public.fail_cancel_grant_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER fail_cancel_grant_audit BEFORE INSERT ON public.novu_rust_operator_jwt_resend_cancel_grant_audit FOR EACH ROW EXECUTE FUNCTION public.fail_cancel_grant_audit()")
        assert grant('PUT','?environmentId=cancel-api-env',body)[0]==500
        assert not grant('GET','?'+urllib.parse.urlencode(params))[1]['granted']
        sql('DROP TRIGGER fail_cancel_grant_audit ON public.novu_rust_operator_jwt_resend_cancel_grant_audit')
    if n==2:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool: results=list(pool.map(lambda _:grant('PUT','?environmentId=cancel-api-env',body),range(2)))
        assert all(code==200 for code,_ in results) and sorted(data['replayed'] for _,data in results)==[False,True],results
    else:
        status,data=grant('PUT','?environmentId=cancel-api-env',body);assert status==200 and data['changed'] and not data['replayed'],(status,data)
    assert grant('PUT','?environmentId=cancel-api-env',body)[1]['replayed']
    assert grant('PUT','?environmentId=cancel-api-env',{**body,'reason':'changed'})[0]==409
    assert grant('GET','?'+urllib.parse.urlencode(params))[1]['granted']
status,audit=grant('GET','/audit?environmentId=cancel-api-env');assert status==200 and len(audit['data'])==14
evidence='environmentId=cancel-api-env'
status,counts=get(evidence,endpoint='resend-cancel-evidence-health');assert status==200 and counts['conflicting']==1 and counts['ambiguous']>=1,(status,counts)
status,page=get(evidence,endpoint='resend-cancel-evidence-list');assert status==200 and any(x['evidence']=='conflicting' for x in page['data']) and 'private' not in json.dumps(page)
assert get(evidence,target,endpoint='resend-cancel-evidence-list')[0]==200
assert get('environmentId=cancel-api-other',endpoint='resend-cancel-evidence-health')[0] in (401,403)
assert get(evidence+'&after=bad',endpoint='resend-cancel-evidence-list')[0]==400
sql("DELETE FROM public.novu_rust_operator_jwt_resend_cancel_evidence_list_grants WHERE environment_id='cancel-api-env' AND subject='fixture'")
assert get(evidence,endpoint='resend-cancel-evidence-list')[0] in (401,403) and get(evidence,endpoint='resend-cancel-evidence-health')[0]==200
sql("INSERT INTO public.novu_rust_operator_jwt_resend_cancel_evidence_list_grants VALUES('https://issuer.test','fixture','console','cancel-api-env')")
query_target='environmentId=cancel-api-env&graceSeconds=60';assert get(query_target,target)[0]==200
self_change={**body,'requestId':'22222222-2222-4222-8222-222222222222','targetSubject':'fixture'};assert grant('PUT','?environmentId=cancel-api-env',self_change)[0] in (401,403)
revoke={**body,'requestId':'33333333-3333-4333-8333-333333333333','permission':'list','expected':True,'desired':False};assert grant('PUT','?environmentId=cancel-api-env',revoke)[0]==200
assert get(query_target,target)[0] in (401,403)
query='environmentId=cancel-api-env&graceSeconds=60'
status,monitor=get(query,endpoint='resend-cancel-command-health');assert status==200 and monitor['unreviewed']==1
status,failures=get(query,endpoint='resend-cancel-command-failures');assert status==200 and len(failures['data'])==1 and failures['data'][0]['requestId']=='ffffffff-ffff-4fff-8fff-ffffffffffff'
def review(body):
    req=urllib.request.Request(origin+'/v1/experimental/operator/resend-cancel-command-review?environmentId=cancel-api-env',data=json.dumps(body).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'},method='POST')
    try:
        with urllib.request.urlopen(req,timeout=5) as r:return r.status,json.loads(r.read())
    except urllib.error.HTTPError as e:return e.code,json.loads(e.read())
review_body={'reviewId':'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1','commandId':failures['data'][0]['requestId'],'failedAt':failures['data'][0]['failedAt'],'reason':'checked failure'}
assert review({**review_body,'failedAt':'2020-01-01T00:00:00Z'})[0]==409
sql("CREATE FUNCTION public.fail_review_actor() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER fail_review_actor BEFORE INSERT ON public.novu_rust_operator_jwt_resend_cancel_command_reviews FOR EACH ROW EXECUTE FUNCTION public.fail_review_actor()")
assert review(review_body)[0]==500
assert get(query,endpoint='resend-cancel-command-health')[1]['unreviewed']==1
sql('DROP TRIGGER fail_review_actor ON public.novu_rust_operator_jwt_resend_cancel_command_reviews')
status,result=review(review_body);assert status==200 and not result['replayed']
assert review(review_body)[1]['replayed']
assert review({**review_body,'reason':'different'})[0]==409
assert get('environmentId=cancel-api-env&requestId='+review_body['reviewId'],endpoint='resend-cancel-command-review')[1]['review']['reason']=='checked failure'
assert get(query,endpoint='resend-cancel-command-health')[1]['unreviewed']==0
sql("DELETE FROM public.novu_rust_operator_jwt_resend_cancel_command_review_grants WHERE environment_id='cancel-api-env' AND subject='fixture'")
assert review(review_body)[0] in (401,403)
sql("INSERT INTO public.novu_rust_operator_jwt_resend_cancel_command_review_grants VALUES('https://issuer.test','fixture','console','cancel-api-env'); DELETE FROM public.novu_rust_resend_cancel_command_reviewers WHERE environment_id='cancel-api-env' AND database_principal='cancel_api_fixture'")
assert review(review_body)[0] in (401,403)
sql("DELETE FROM public.novu_rust_operator_jwt_resend_cancel_command_health_grants WHERE environment_id='cancel-api-env' AND subject='fixture'")
assert get(query,endpoint='resend-cancel-command-health')[0] in (401,403) and get(query,endpoint='resend-cancel-command-failures')[0]==200
sql("INSERT INTO public.novu_rust_operator_jwt_resend_cancel_command_health_grants VALUES('https://issuer.test','fixture','console','cancel-api-env'); DELETE FROM public.novu_rust_operator_jwt_resend_cancel_command_failed_list_grants WHERE environment_id='cancel-api-env' AND subject='fixture'")
assert get(query,endpoint='resend-cancel-command-failures')[0] in (401,403) and get(query,endpoint='resend-cancel-command-health')[0]==200
sql("INSERT INTO public.novu_rust_operator_jwt_resend_cancel_command_failed_list_grants VALUES('https://issuer.test','fixture','console','cancel-api-env')")
sql("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id='cancel-api-env' AND database_principal='cancel_api_fixture' AND decision='alert_view'")
assert get(query,endpoint='resend-cancel-command-health')[0] in (401,403) and get(query,endpoint='resend-cancel-command-failures')[0] in (401,403)
sql("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'cancel-api-env',rolname,'alert_view',oid FROM pg_roles WHERE rolname='cancel_api_fixture'")
status,page=get(query);assert status==200 and len(page['data'])==50 and page['next'],(status,page)
assert 'private' not in json.dumps(page)
status,second=get(query+'&after='+page['next']);assert status==200 and len(second['data'])==5 and second['next'] is None
rows=page['data']+second['data'];ids=[x['requestId'] for x in rows]
assert ids==sorted(ids) and len(set(ids))==55 and all(x['overdue'] for x in rows)
assert get(query,None)[0]==401
assert get('environmentId=cancel-api-other&graceSeconds=60')[0] in (401,403)
assert get('environmentId=cancel-api-env&graceSeconds=0')[0]==400
assert get(query+'&after=invalid')[0]==400
assert get(query+'&graceSeconds=60')[0]==400
detail_query='environmentId=cancel-api-env&requestId='+ids[0]
status,detail=get(detail_query,endpoint='resend-cancel-detail');assert status==200 and detail['request']['requestId']==ids[0] and detail['resolution'] is None
sql("DELETE FROM public.novu_rust_operator_jwt_resend_cancel_detail_grants WHERE environment_id='cancel-api-env'")
assert get(detail_query,endpoint='resend-cancel-detail')[0] in (401,403)
assert get(query)[0]==200
sql("INSERT INTO public.novu_rust_operator_jwt_resend_cancel_detail_grants VALUES ('https://issuer.test','fixture','console','cancel-api-env'); INSERT INTO public.novu_rust_resend_cancel_resolutions(environment_id,resolution_id,request_id,database_principal,principal_oid,decision,reason) SELECT 'cancel-api-env','aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa','%s',rolname,oid,'confirmed','fixture evidence' FROM pg_roles WHERE rolname='cancel_api_fixture'; UPDATE public.novu_rust_resend_cancel_requests SET state='confirmed' WHERE request_id='%s'"%(ids[0],ids[0]))
status,detail=get(detail_query,endpoint='resend-cancel-detail');assert status==200 and detail['resolution']['decision']=='confirmed' and detail['resolution']['reason']=='fixture evidence'
assert get('environmentId=cancel-api-other&requestId='+ids[0],endpoint='resend-cancel-detail')[0] in (401,403)
assert get('environmentId=cancel-api-env&requestId=invalid',endpoint='resend-cancel-detail')[0]==400
def command(method,request_id,body=None,credential=token):
    query='environmentId=cancel-api-env'+('&requestId='+request_id if method=='GET' else '')
    req=urllib.request.Request(origin+'/v1/experimental/operator/resend-cancel-command?'+query,data=json.dumps(body).encode() if body is not None else None,headers={'Authorization':'Bearer '+credential,'Content-Type':'application/json'},method=method)
    try:
        with urllib.request.urlopen(req,timeout=5) as r:return r.status,json.loads(r.read())
    except urllib.error.HTTPError as e:return e.code,json.loads(e.read())
cmd={'requestId':'dddddddd-dddd-4ddd-8ddd-dddddddddddd','jobId':'44444444-4444-4444-8444-444444444444','reason':'operator request'}
sql("CREATE FUNCTION public.fail_command_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER fail_command_audit BEFORE INSERT ON public.novu_rust_operator_jwt_resend_cancel_command_audit FOR EACH ROW EXECUTE FUNCTION public.fail_command_audit()")
assert command('POST',cmd['requestId'],cmd)[0]==500
assert command('GET',cmd['requestId'])[1]['command'] is None
sql('DROP TRIGGER fail_command_audit ON public.novu_rust_operator_jwt_resend_cancel_command_audit')
status,result=command('POST',cmd['requestId'],cmd);assert status==200 and result['command']['state']=='pending' and not result['replayed'],(status,result)
assert command('POST',cmd['requestId'],cmd)[1]['replayed']
assert command('POST',cmd['requestId'],{**cmd,'reason':'changed'})[0]==409
assert command('GET',cmd['requestId'],credential=target)[1]['command'] is None
class Provider(BaseHTTPRequestHandler):
    def do_POST(self):
        assert self.path in ('/emails/fixture-cancel/cancel','/emails/fixture-ui/cancel') and self.headers.get('Authorization')=='Bearer fixture'
        self.server.calls.append(self.path);body=json.dumps({'object':'email','id':self.path.split('/')[2]}).encode();self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    def log_message(self,*args):pass
provider=ThreadingHTTPServer(('127.0.0.1',0),Provider);provider.calls=[];thread=threading.Thread(target=provider.serve_forever,daemon=True);thread.start()
worker_env={**os.environ,'DATABASE_URL':'postgres://cancel_api_worker@127.0.0.1:'+os.environ['TEST_DB_PORT']+'/postgres','NOVU_RESEND_ENVIRONMENT_ID':'cancel-api-env','NOVU_RESEND_API_KEY':'fixture','SWALLOW_TEST_RESEND_ENDPOINT':'http://127.0.0.1:%d/emails'%provider.server_port}
worker=subprocess.Popen([os.environ['TEST_CANCEL_WORKER']],env=worker_env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
try:
    end=time.monotonic()+15
    while time.monotonic()<end:
        status,result=command('GET',cmd['requestId'])
        if status==200 and result['command']['state']=='finished' and len(provider.calls)==2:break
        if worker.poll() is not None:raise AssertionError(worker.stderr.read().decode())
        time.sleep(.1)
    else:raise AssertionError('worker did not finish command')
    assert result['command']['attemptState']=='confirmed' and sorted(provider.calls)==['/emails/fixture-cancel/cancel','/emails/fixture-ui/cancel']
    end=time.monotonic()+5
    while time.monotonic()<end:
        count=subprocess.check_output([psql,admin,'-qAtc',"SELECT count(*) FROM public.novu_rust_resend_cancel_commands WHERE environment_id='cancel-api-env' AND job_id='55555555-5555-4555-8555-555555555555' AND state='finished'"],text=True).strip()
        if count=='1':break
        time.sleep(.05)
    else:raise AssertionError('UI command did not finish')
    assert command('POST',cmd['requestId'],cmd)[1]['replayed'] and len(provider.calls)==2
finally:
    worker.terminate();worker.wait(timeout=5);provider.shutdown();provider.server_close();thread.join()
sql("DELETE FROM public.novu_rust_operator_jwt_resend_cancel_submit_grants WHERE environment_id='cancel-api-env' AND subject='fixture'")
assert command('GET',cmd['requestId'])[0] in (401,403)
sql("INSERT INTO public.novu_rust_operator_jwt_resend_cancel_submit_grants VALUES('https://issuer.test','fixture','console','cancel-api-env'); DELETE FROM public.novu_rust_resend_cancel_submitters WHERE environment_id='cancel-api-env' AND database_principal='cancel_api_fixture'")
assert command('GET',cmd['requestId'])[0] in (401,403)
body={'resolutionId':'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb','requestId':ids[1],'decision':'confirmed','reason':'verified provider'}
assert resolve({**body,'decision':'not_cancelled'})[0] in (401,403)
status,answer=resolve(body);assert status==200 and answer['replayed'] is False
status,answer=resolve(body);assert status==200 and answer['replayed'] is True
assert resolve({**body,'reason':'different'})[0]==409
status,detail=get('environmentId=cancel-api-env&requestId='+ids[1],endpoint='resend-cancel-detail');assert status==200 and detail['resolution']['operator']['subject']=='fixture' and detail['resolution']['operator']['resolutionId']==body['resolutionId']
sql("INSERT INTO public.novu_rust_operator_jwt_resend_cancel_resolution_grants VALUES ('https://issuer.test','fixture','console','cancel-api-env','not_cancelled'); CREATE FUNCTION public.fail_cancel_attribution() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER fail_cancel_attribution BEFORE INSERT ON public.novu_rust_operator_jwt_resend_cancel_resolution_audit FOR EACH ROW EXECUTE FUNCTION public.fail_cancel_attribution()")
second_body={'resolutionId':'cccccccc-cccc-4ccc-8ccc-cccccccccccc','requestId':ids[2],'decision':'not_cancelled','reason':'provider checked'}
assert resolve(second_body)[0]==500
assert get('environmentId=cancel-api-env&requestId='+ids[2],endpoint='resend-cancel-detail')[1]['request']['state']=='unknown'
sql('DROP TRIGGER fail_cancel_attribution ON public.novu_rust_operator_jwt_resend_cancel_resolution_audit')
assert resolve(second_body)[0]==200
# Historical 409 can be reviewed through independently authorized JWT resolution.
legacy_id='88888888-8888-4888-8888-888888888887';legacy_job='88888888-8888-4888-8888-888888888886'
sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES('%s','cancel-api-env','experimental_resend_email','{}','completed'); INSERT INTO public.novu_rust_resend_cancel_requests(environment_id,request_id,job_id,provider_id,database_principal,principal_oid,reason,state,http_status) SELECT 'cancel-api-env','%s','%s','legacy-id',rolname,oid,'fixture','rejected',409 FROM pg_roles WHERE rolname='cancel_api_fixture'"%(legacy_job,legacy_id,legacy_job))
legacy_query='environmentId=cancel-api-env'
status,legacy_health=get(legacy_query,endpoint='resend-cancel-legacy-health');assert status==200 and legacy_health['legacyAmbiguous']==1,(status,legacy_health)
status,legacy_page=get(legacy_query,endpoint='resend-cancel-legacy-list');assert status==200 and len(legacy_page['data'])==1 and legacy_page['data'][0]['requestId']==legacy_id and legacy_page['data'][0]['httpStatus']==409
assert get(legacy_query,target,endpoint='resend-cancel-legacy-list')[0]==200
assert get('environmentId=cancel-api-other',endpoint='resend-cancel-legacy-health')[0] in (401,403)
assert get(legacy_query+'&after=invalid',endpoint='resend-cancel-legacy-list')[0]==400
sql("DELETE FROM public.novu_rust_operator_jwt_resend_cancel_legacy_list_grants WHERE environment_id='cancel-api-env' AND subject='fixture'")
assert get(legacy_query,endpoint='resend-cancel-legacy-list')[0] in (401,403) and get(legacy_query,endpoint='resend-cancel-legacy-health')[0]==200
sql("INSERT INTO public.novu_rust_operator_jwt_resend_cancel_legacy_list_grants VALUES('https://issuer.test','fixture','console','cancel-api-env'); UPDATE public.novu_rust_resend_cancel_operators SET can_read=false WHERE environment_id='cancel-api-env' AND database_principal='cancel_api_fixture'")
assert get(legacy_query,endpoint='resend-cancel-legacy-list')[0] in (401,403) and get(legacy_query,endpoint='resend-cancel-legacy-health')[0]==200
sql("UPDATE public.novu_rust_resend_cancel_operators SET can_read=true WHERE environment_id='cancel-api-env' AND database_principal='cancel_api_fixture'; DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id='cancel-api-env' AND database_principal='cancel_api_fixture' AND decision='alert_view'")
assert get(legacy_query,endpoint='resend-cancel-legacy-health')[0] in (401,403) and get(legacy_query,endpoint='resend-cancel-legacy-list')[0]==200
sql("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'cancel-api-env',rolname,'alert_view',oid FROM pg_roles WHERE rolname='cancel_api_fixture'")
legacy_body={'resolutionId':'abababab-abab-4bab-8bab-abababababab','requestId':legacy_id,'decision':'not_cancelled','reason':'reviewed provider evidence'}
assert resolve(legacy_body)[0]==200
assert get(legacy_query,endpoint='resend-cancel-legacy-health')[1]['legacyAmbiguous']==0
status,legacy_detail=get('environmentId=cancel-api-env&requestId='+legacy_id,endpoint='resend-cancel-detail')
assert status==200 and legacy_detail['request']['state']=='not_cancelled' and legacy_detail['request']['httpStatus']==409 and legacy_detail['resolution']['operator']['subject']=='fixture'
sql("DELETE FROM public.novu_rust_operator_jwt_resend_cancel_resolution_grants WHERE environment_id='cancel-api-env' AND decision='confirmed'")
assert resolve(body)[0] in (401,403)
sql("INSERT INTO public.novu_rust_operator_jwt_resend_cancel_resolution_grants VALUES ('https://issuer.test','fixture','console','cancel-api-env','confirmed'); UPDATE public.novu_rust_resend_cancel_operators SET can_resolve=false WHERE environment_id='cancel-api-env' AND database_principal='cancel_api_fixture'")
assert resolve(body)[0] in (401,403)
sql("DELETE FROM public.novu_rust_operator_jwt_resend_cancel_list_grants WHERE environment_id='cancel-api-env'")
assert get(query)[0] in (401,403)
assert get(detail_query,endpoint='resend-cancel-detail')[0]==200
sql("INSERT INTO public.novu_rust_operator_jwt_resend_cancel_list_grants VALUES ('https://issuer.test','fixture','console','cancel-api-env'); UPDATE public.novu_rust_resend_cancel_operators SET can_read=false WHERE environment_id='cancel-api-env' AND database_principal='cancel_api_fixture'")
assert get(query)[0] in (401,403)
assert get(detail_query,endpoint='resend-cancel-detail')[0] in (401,403)
