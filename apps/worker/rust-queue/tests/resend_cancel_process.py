import json, os, subprocess, threading, time
from http.server import BaseHTTPRequestHandler, HTTPServer

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        self.server.calls.append(self.path)
        assert self.headers.get('Authorization')=='Bearer fixture'
        if self.path=='/emails/fixture-two/cancel':
            self.close_connection=True
            return
        assert self.path=='/emails/fixture-one/cancel'
        result=b'{"object":"email","id":"fixture-one"}'
        self.send_response(200);self.send_header('Content-Length',str(len(result)));self.end_headers();self.wfile.write(result)
    def log_message(self,*args): pass
server=HTTPServer(('127.0.0.1',0),Handler);server.calls=[]
thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
env=os.environ.copy();env.update(DATABASE_URL='postgres://cancel_fixture@127.0.0.1:'+env['TEST_PORT']+'/postgres',NOVU_RESEND_API_KEY='fixture',SWALLOW_TEST_RESEND_ENDPOINT='http://127.0.0.1:%d/emails'%server.server_port)
bin=env['TEST_CANCEL']; req1='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'; req2='bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'; job1='11111111-1111-4111-8111-111111111111'; job2='22222222-2222-4222-8222-222222222222'
def run(*args,override=None):
    return subprocess.run([bin,*args],env=override or env,text=True,capture_output=True,timeout=20)
try:
    guarded=env.copy();guarded.update(SWALLOW_RESEND_CANCEL_REQUIRE_ACCOUNT_TAG='true',SWALLOW_RESEND_ACCOUNT_TAG='wrong-account')
    assert run('cancel','cancel-env','99999999-9999-4999-8999-999999999998','44444444-4444-4444-8444-444444444444','wrong',override=guarded).returncode!=0
    assert not server.calls
    result=run('cancel','cancel-env',req1,job1,'fixture');assert result.returncode==0,(result.stdout,result.stderr)
    assert json.loads(result.stdout)['state']=='confirmed'
    replay=run('cancel','cancel-env',req1,job1,'fixture');assert replay.returncode==0 and json.loads(replay.stdout)['fresh'] is False
    bad=run('cancel','cancel-env',req1,job1,'different');assert bad.returncode!=0
    unknown=run('cancel','cancel-env',req2,job2,'fixture');assert unknown.returncode==2 and json.loads(unknown.stdout)['state']=='unknown'
    health_bin=os.path.join(env['TEST_WORKER_DIR'],'delivery-reconcile')
    def health(environment='cancel-env',grace='60'):
        return subprocess.run([health_bin,'check-resend-cancel',environment,grace],env=env,text=True,capture_output=True,timeout=10)
    current=health();assert current.returncode==0 and json.loads(current.stdout)=={'unknown':1,'overdue':0}
    assert health('cancel-other').returncode!=0
    assert health('cancel-env','0').returncode!=0
    subprocess.run([env['TEST_PSQL'],os.environ['DATABASE_URL'],'-v','ON_ERROR_STOP=1','-c',"UPDATE public.novu_rust_resend_cancel_requests SET created_at=clock_timestamp()-interval '5 minutes' WHERE request_id='%s'"%req2],check=True,capture_output=True)
    current=health();assert current.returncode==2 and json.loads(current.stdout)=={'unknown':1,'overdue':1}
    alert_env={**env,'SWALLOW_ALERT_RESEND_ENABLED':'false','SWALLOW_ALERT_RESEND_CANCEL_GRACE_SECONDS':'60','SWALLOW_ALERT_SCOPED_DELIVERY':'true','SWALLOW_ALERT_COOLDOWN_POLICY':'new-condition','SWALLOW_ALERT_CONDITIONS_V3':'true','SWALLOW_ALERT_SLACK_WEBHOOK':'https://hooks.slack.com/services/A/B/C','SWALLOW_ALERT_SLACK_TARGET_ID':'blocked'}
    alert=subprocess.run([os.path.join(env['TEST_WORKER_DIR'],'send-delivery-alert'),'cancel-env','300','120','60'],env=alert_env,text=True,capture_output=True,timeout=15)
    assert alert.returncode==2 and 'requires operator review' in alert.stderr,(alert.stdout,alert.stderr)
    assert subprocess.run([os.path.join(env['TEST_WORKER_DIR'],'send-delivery-alert'),'cancel-env','300','120','60'],env={**alert_env,'SWALLOW_ALERT_CONDITIONS_V3':'false'},capture_output=True,timeout=15).returncode==1
    replay=run('cancel','cancel-env',req2,job2,'fixture');assert replay.returncode==2 and json.loads(replay.stdout)['fresh'] is False
    competing=run('cancel','cancel-env','cccccccc-cccc-4ccc-8ccc-cccccccccccc',job2,'fixture');assert competing.returncode!=0
    status=run('get','cancel-env',req2);assert status.returncode==0 and json.loads(status.stdout)['request']['state']=='unknown'
    denied=env.copy();denied['DATABASE_URL']=denied['DATABASE_URL'].replace('cancel_fixture@','cancel_unprivileged@')
    assert run('get','cancel-env',req1,override=denied).returncode!=0
    assert run('get','cancel-other',req1).returncode!=0
    assert server.calls==['/emails/fixture-one/cancel','/emails/fixture-two/cancel'],server.calls
    # Two callers race for the same request; only one gets dispatch authority.
    sql="SELECT public.novu_rust_resend_cancel_reserve('cancel-env','dddddddd-dddd-4ddd-8ddd-dddddddddddd','33333333-3333-4333-8333-333333333333','concurrent')"
    command=[env['TEST_PSQL'],env['DATABASE_URL'],'-qAt','-v','ON_ERROR_STOP=1','-c',sql]
    processes=[subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) for _ in range(2)]
    outputs=[p.communicate(timeout=10) for p in processes]
    assert all(p.returncode==0 for p in processes),outputs
    results=[json.loads(out) for out,_ in outputs];assert sorted(r['fresh'] for r in results)==[False,True],results
    # A crash after reservation leaves unknown; replay cannot dispatch.
    recovered=run('cancel','cancel-env','dddddddd-dddd-4ddd-8ddd-dddddddddddd','33333333-3333-4333-8333-333333333333','concurrent')
    assert recovered.returncode==2 and json.loads(recovered.stdout)['fresh'] is False
    assert len(server.calls)==2
    resolver=env.copy();resolver['DATABASE_URL']=resolver['DATABASE_URL'].replace('cancel_fixture@','cancel_resolver@')
    rid='eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee'
    assert run('resolve','cancel-env',rid,req2,'not_cancelled','checked',override=env).returncode!=0
    # A failed audit insert must leave the request unknown.
    admin=os.environ['DATABASE_URL'];psql=env['TEST_PSQL']
    failure="CREATE FUNCTION public.fail_cancel_resolution() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER fail_cancel_resolution BEFORE INSERT ON public.novu_rust_resend_cancel_resolutions FOR EACH ROW EXECUTE FUNCTION public.fail_cancel_resolution()"
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',failure],check=True,capture_output=True)
    assert run('resolve','cancel-env',rid,req2,'not_cancelled','checked',override=resolver).returncode!=0
    assert json.loads(run('get','cancel-env',req2).stdout)['request']['state']=='unknown'
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c','DROP TRIGGER fail_cancel_resolution ON public.novu_rust_resend_cancel_resolutions'],check=True,capture_output=True)
    resolved=run('resolve','cancel-env',rid,req2,'not_cancelled','checked',override=resolver)
    assert resolved.returncode==0,(resolved.stdout,resolved.stderr)
    assert json.loads(resolved.stdout)['replayed'] is False
    replay=run('resolve','cancel-env',rid,req2,'not_cancelled','checked',override=resolver)
    assert replay.returncode==0 and json.loads(replay.stdout)['replayed'] is True
    assert run('resolve','cancel-env',rid,req2,'confirmed','checked',override=resolver).returncode!=0
    audit=run('resolution-get','cancel-env',rid,override=resolver);assert json.loads(audit.stdout)['request']['decision']=='not_cancelled'
    late="SELECT public.novu_rust_resend_cancel_finish('cancel-env','%s','confirmed',NULL)"%req2
    assert subprocess.run([env['TEST_PSQL'],env['DATABASE_URL'],'-qAt','-v','ON_ERROR_STOP=1','-c',late],capture_output=True).returncode!=0
    # A fresh attempt is now an explicit operator choice, not an automatic retry.
    retry=run('cancel','cancel-env','cccccccc-cccc-4ccc-8ccc-cccccccccccc',job2,'fixture');assert retry.returncode==2
    assert server.calls==['/emails/fixture-one/cancel','/emails/fixture-two/cancel','/emails/fixture-two/cancel']
    request3='dddddddd-dddd-4ddd-8ddd-dddddddddddd';resolution3='ffffffff-ffff-4fff-8fff-ffffffffffff'
    assert run('resolve','cancel-env',resolution3,request3,'confirmed','provider evidence',override=resolver).returncode==0
    assert json.loads(run('get','cancel-env',request3).stdout)['request']['state']=='confirmed'
    assert run('cancel','cancel-env','99999999-9999-4999-8999-999999999999','33333333-3333-4333-8333-333333333333','new').returncode!=0
    assert len(server.calls)==3
    # Delivery evidence remains separate from provider and operator confirmations.
    def result(job, url=None, environment='cancel-env'):
        query="SELECT public.novu_rust_channel_result('%s','%s','resend')"%(environment,job)
        return subprocess.run([psql,url or env['DATABASE_URL'],'-qAt','-v','ON_ERROR_STOP=1','-c',query],text=True,capture_output=True)
    provider=json.loads(result(job1).stdout)
    assert provider['cancellationState']=='confirmed' and provider['cancellationConfirmationSource']=='provider_response' and provider['cancellationEvidence']=='no_conflict_observed',provider
    operator=json.loads(result('33333333-3333-4333-8333-333333333333').stdout)
    assert operator['cancellationConfirmationSource']=='operator' and operator['cancellationEvidence']=='no_conflict_observed',operator
    assert result(job1,denied['DATABASE_URL']).returncode!=0
    assert result(job1,environment='cancel-other').returncode!=0
    insert_event="INSERT INTO public.novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type) VALUES ('cancel-env','cancel-conflict','fixture-one','email.delivered')"
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',insert_event],check=True,capture_output=True)
    conflict=json.loads(result(job1).stdout)
    assert conflict['cancellationEvidence']=='conflicting' and conflict['result']==provider['result'],conflict
    def evidence_cli(command, environment='cancel-env', extra=(), selected_env=None):
        return subprocess.run([health_bin,command,environment,*extra],env=selected_env or env,text=True,capture_output=True,timeout=10)
    check=evidence_cli('check-resend-cancel-evidence');assert check.returncode==2 and json.loads(check.stdout)=={'conflicting':1,'ambiguous':0},(check.stdout,check.stderr)
    listed=evidence_cli('list-resend-cancel-evidence'); page=json.loads(listed.stdout)
    assert listed.returncode==0 and page['data']==[{'requestId':req1,'jobId':job1,'evidence':'conflicting'}] and page['next'] is None,page
    assert evidence_cli('list-resend-cancel-evidence',extra=(req1,)).stdout.strip()=='{"data":[],"next":null}'
    assert evidence_cli('check-resend-cancel-evidence','cancel-other').returncode!=0
    assert evidence_cli('check-resend-cancel-evidence',selected_env=denied).returncode!=0
    conflict_alert={**alert_env,'SWALLOW_ALERT_RESEND_CANCEL_GRACE_SECONDS':'','SWALLOW_ALERT_RESEND_CANCEL_EVIDENCE_ENABLED':'true'}
    conflict_alert.pop('SWALLOW_ALERT_RESEND_CANCEL_GRACE_SECONDS')
    sent=subprocess.run([os.path.join(env['TEST_WORKER_DIR'],'send-delivery-alert'),'cancel-env','300','120','60'],env=conflict_alert,text=True,capture_output=True,timeout=15)
    assert sent.returncode==2 and 'requires operator review' in sent.stderr,(sent.stdout,sent.stderr)
    collision="UPDATE public.novu_rust_resend_attempts SET provider_message_id='fixture-one' WHERE job_id='88888888-8888-4888-8888-888888888888'"
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',collision],check=True,capture_output=True)
    ambiguous=json.loads(result(job1).stdout)
    assert ambiguous['cancellationEvidence']=='ambiguous' and ambiguous['result']=='unknown',ambiguous
    check=evidence_cli('check-resend-cancel-evidence');assert json.loads(check.stdout)=={'conflicting':0,'ambiguous':1},check.stdout
    # Page unknown requests without exposing provider IDs or investigation reasons.
    insert="""WITH jobs AS (INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) SELECT gen_random_uuid(),'cancel-env','experimental_resend_email','{}','completed' FROM generate_series(1,55) RETURNING id)
    INSERT INTO public.novu_rust_resend_cancel_requests(environment_id,request_id,job_id,provider_id,database_principal,principal_oid,reason,state,created_at)
    SELECT 'cancel-env',gen_random_uuid(),jobs.id,'private-provider-id',r.rolname,r.oid,'private reason','unknown',clock_timestamp()-interval '5 minutes' FROM jobs CROSS JOIN pg_roles r WHERE r.rolname='cancel_fixture'"""
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',insert],check=True,capture_output=True)
    first=run('list','cancel-env','60',override=resolver);assert first.returncode==0,(first.stdout,first.stderr)
    page=json.loads(first.stdout);assert len(page['data'])==50 and page['next'];assert 'private' not in first.stdout
    second=run('list','cancel-env','60',page['next'],override=resolver);more=json.loads(second.stdout)
    assert second.returncode==0 and len(more['data'])==6 and more['next'] is None
    ids=[x['requestId'] for x in page['data']+more['data']];assert len(set(ids))==56 and ids==sorted(ids)
    assert all(set(x)=={'requestId','jobId','createdAt','overdue'} for x in page['data']+more['data'])
    assert run('list','cancel-env','0',override=resolver).returncode!=0
    assert run('list','cancel-other','60',override=resolver).returncode!=0
    assert run('list','cancel-env','60',override=denied).returncode!=0
finally:
    server.shutdown();server.server_close();thread.join()
