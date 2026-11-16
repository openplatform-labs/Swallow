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
    # A prior retry identity or a pre-existing delivery event forbids a new external cancellation.
    psql=env['TEST_PSQL'];admin=os.environ['DATABASE_URL']
    audit="INSERT INTO public.novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason,retry_provider_message_id) VALUES(gen_random_uuid(),'cancel-env','44444444-4444-4444-8444-444444444444','retry','fixture','fixture','fixture-four')"
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',audit],check=True,capture_output=True)
    assert run('cancel','cancel-env','99999999-9999-4999-8999-999999999997','44444444-4444-4444-8444-444444444444','fixture').returncode!=0
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',"DELETE FROM public.novu_rust_delivery_reconciliations WHERE retry_provider_message_id='fixture-four'"],check=True,capture_output=True)
    stale="BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT public.novu_rust_resend_cancel_reserve('cancel-env','99999999-9999-4999-8999-999999999994','44444444-4444-4444-8444-444444444444','fixture'); COMMIT"
    assert subprocess.run([psql,env['DATABASE_URL'],'-qAt','-v','ON_ERROR_STOP=1','-c',stale],capture_output=True).returncode!=0
    # The retry audit holds the shared provider lock until commit; reservation must re-read it.
    race=subprocess.Popen([psql,admin,'-qAt','-v','ON_ERROR_STOP=1','-c','BEGIN; '+audit+'; SELECT pg_sleep(2); COMMIT'],env={**os.environ,'PGAPPNAME':'cancel-retry-race'},stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    try:
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            state=subprocess.run([psql,admin,'-qAt','-c',"SELECT count(*) FROM pg_stat_activity WHERE application_name='cancel-retry-race' AND wait_event='PgSleep'"],text=True,capture_output=True)
            if state.stdout.strip()=='1':break
            assert race.poll() is None
            time.sleep(.05)
        else:raise AssertionError('retry transaction did not acquire provider lock')
        refused=run('cancel','cancel-env','99999999-9999-4999-8999-999999999995','44444444-4444-4444-8444-444444444444','fixture')
        assert refused.returncode!=0 and not server.calls,(refused.stdout,refused.stderr)
        out,err=race.communicate(timeout=5);assert race.returncode==0,(out,err)
    finally:
        if race.poll() is None:race.kill();race.wait()
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',"DELETE FROM public.novu_rust_delivery_reconciliations WHERE retry_provider_message_id='fixture-four'"],check=True,capture_output=True)
    event="INSERT INTO public.novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type) VALUES('cancel-env','prior-event','fixture-five','email.delivery_delayed')"
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',event],check=True,capture_output=True)
    assert run('cancel','cancel-env','99999999-9999-4999-8999-999999999996','55555555-5555-4555-8555-555555555555','fixture').returncode!=0
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',"DELETE FROM public.novu_rust_resend_delivery_events WHERE event_id='prior-event'; UPDATE public.novu_queue_jobs SET resend_delivery_result=NULL WHERE id='55555555-5555-4555-8555-555555555555'"],check=True,capture_output=True)
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
    # Only the provider-confirmed job is excluded; an operator assessment stays visible.
    ages="UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()-interval '2 hours',resend_scheduled_at=clock_timestamp()-interval '1 hour' WHERE id IN ('%s','33333333-3333-4333-8333-333333333333'); INSERT INTO public.novu_rust_resend_projection_heartbeat VALUES('cancel-env',clock_timestamp())"%job1
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',ages],check=True,capture_output=True)
    def policy_health(exclude=True,db_url=None,environment='cancel-env'):
        query=("SELECT overdue,eligible FROM public.novu_rust_resend_health_exclude_confirmed_cancel('%s',60,120)" if exclude else "SELECT overdue,eligible FROM public.novu_rust_provider_health('%s','resend',60,120)")%environment
        return subprocess.run([psql,db_url or env['DATABASE_URL'],'-qAt','-v','ON_ERROR_STOP=1','-c',query],text=True,capture_output=True)
    assert policy_health(False).stdout.strip()=='2|2'
    assert policy_health().stdout.strip()=='1|1'
    assert policy_health(db_url=denied['DATABASE_URL']).returncode!=0
    assert policy_health(environment='cancel-other').returncode!=0
    policy_alert={**alert_env,'SWALLOW_ALERT_RESEND_ENABLED':'true','SWALLOW_ALERT_CHECK_SCOPED_ACCESS':'true','SWALLOW_ALERT_RESEND_EXCLUDE_CONFIRMED_CANCEL':'true','SWALLOW_ALERT_RESEND_CANCEL_EVIDENCE_ENABLED':'true'}
    policy_alert.pop('SWALLOW_ALERT_RESEND_CANCEL_GRACE_SECONDS')
    sent=subprocess.run([os.path.join(env['TEST_WORKER_DIR'],'send-delivery-alert'),'cancel-env','60','120','60'],env=policy_alert,text=True,capture_output=True,timeout=15)
    assert sent.returncode==2 and 'requires operator review' in sent.stderr,(sent.stdout,sent.stderr)
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',"UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()+interval '1 hour',resend_scheduled_at=clock_timestamp()+interval '1 hour' WHERE id='33333333-3333-4333-8333-333333333333'"],check=True,capture_output=True)
    no_alert=subprocess.run([os.path.join(env['TEST_WORKER_DIR'],'send-delivery-alert'),'cancel-env','60','120','60'],env=policy_alert,text=True,capture_output=True,timeout=15)
    assert no_alert.returncode==0,(no_alert.stdout,no_alert.stderr)
    default_policy={**policy_alert,'SWALLOW_ALERT_RESEND_EXCLUDE_CONFIRMED_CANCEL':'false'}
    default_alert=subprocess.run([os.path.join(env['TEST_WORKER_DIR'],'send-delivery-alert'),'cancel-env','60','120','60'],env=default_policy,text=True,capture_output=True,timeout=15)
    assert default_alert.returncode==2 and 'requires operator review' in default_alert.stderr,(default_alert.stdout,default_alert.stderr)
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',"UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()-interval '2 hours',resend_scheduled_at=clock_timestamp()-interval '1 hour' WHERE id='33333333-3333-4333-8333-333333333333'"],check=True,capture_output=True)
    disabled={**policy_alert,'SWALLOW_ALERT_RESEND_CANCEL_EVIDENCE_ENABLED':'false'}
    sent=subprocess.run([os.path.join(env['TEST_WORKER_DIR'],'send-delivery-alert'),'cancel-env','60','120','60'],env=disabled,text=True,capture_output=True,timeout=15)
    assert sent.returncode==1 and 'requires scoped Resend health and cancellation evidence alerts' in sent.stderr,(sent.stdout,sent.stderr)
    # A later retry identity also reopens the alert and prevents exclusion.
    for audit_job in (job1,'44444444-4444-4444-8444-444444444444'):
        audit="INSERT INTO public.novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason,retry_provider_message_id) VALUES(gen_random_uuid(),'cancel-env','%s','retry','fixture','fixture','fixture-one')"%audit_job
        subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',audit],check=True,capture_output=True)
        assert json.loads(result(job1).stdout)['cancellationEvidence']=='ambiguous'
        assert policy_health().stdout.strip()=='2|2'
        subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',"DELETE FROM public.novu_rust_delivery_reconciliations WHERE retry_provider_message_id='fixture-one'"],check=True,capture_output=True)
        assert policy_health().stdout.strip()=='1|1'
    # Each stored event independently changes the live result and alert count;
    # a scheduled-only event is not a delivery conflict.
    for kind in ('email.scheduled','email.delivery_delayed','email.opened','email.clicked'):
        insert="INSERT INTO public.novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type) VALUES ('cancel-env','kind-fixture','fixture-one','%s')"%kind
        subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',insert],check=True,capture_output=True)
        # Webhook invalidation marks the projection unknown; never exclude it until recomputed.
        assert policy_health().stdout.strip()=='2|2',kind
        subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',"UPDATE public.novu_queue_jobs SET resend_delivery_result='pending' WHERE id='%s'"%job1],check=True,capture_output=True)
        observed=json.loads(result(job1).stdout)
        expected='no_conflict_observed' if kind=='email.scheduled' else 'conflicting'
        assert observed['cancellationEvidence']==expected,(kind,observed)
        assert policy_health().stdout.strip()==('1|1' if kind=='email.scheduled' else '2|2'),kind
        if kind!='email.scheduled':
            check=subprocess.run([health_bin,'check-resend-cancel-evidence','cancel-env'],env=env,text=True,capture_output=True,timeout=10)
            assert check.returncode==2 and json.loads(check.stdout)['conflicting']==1,(kind,check.stdout,check.stderr)
        subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',"DELETE FROM public.novu_rust_resend_delivery_events WHERE event_id='kind-fixture'"],check=True,capture_output=True)
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
    assert policy_health().stdout.strip()=='2|2'
    check=evidence_cli('check-resend-cancel-evidence');assert json.loads(check.stdout)=={'conflicting':0,'ambiguous':1},check.stdout
    # Historical 408/409 rows remain unchanged but can be found by scoped readers.
    legacy_job='12121212-1212-4212-8212-121212121212'
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',"INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,resend_scheduled_at) VALUES('%s','cancel-env','experimental_resend_email','{}','completed',clock_timestamp()+interval '1 hour'); INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES('%s','cancel-env','accepted','legacy-fixture')"%(legacy_job,legacy_job)],check=True,capture_output=True)
    legacy="INSERT INTO public.novu_rust_resend_cancel_requests(environment_id,request_id,job_id,provider_id,database_principal,principal_oid,reason,state,http_status) SELECT 'cancel-env','aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa0','%s','legacy-fixture',rolname,oid,'private reason','rejected',408 FROM pg_roles WHERE rolname='cancel_fixture'"%legacy_job
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',legacy],check=True,capture_output=True)
    legacy409=legacy.replace('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa0','aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1').replace("'rejected',408","'rejected',409")
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',legacy409],check=True,capture_output=True)
    history=run('list-legacy-ambiguous','cancel-env',override=resolver);assert history.returncode==0,(history.stdout,history.stderr)
    page=json.loads(history.stdout);assert len(page['data'])==2 and [x['httpStatus'] for x in page['data']]==[408,409] and 'private' not in history.stdout,page
    assert json.loads(run('list-legacy-ambiguous','cancel-env',page['data'][-1]['requestId'],override=resolver).stdout)['data']==[]
    assert run('list-legacy-ambiguous','cancel-other',override=resolver).returncode!=0
    assert run('list-legacy-ambiguous','cancel-env',override=denied).returncode!=0
    req408=page['data'][0]['requestId'];req409=page['data'][1]['requestId'];review408='abababab-abab-4bab-8bab-abababababab';review409='cdcdcdcd-cdcd-4dcd-8dcd-cdcdcdcdcdcd'
    fresh='dddddddd-aaaa-4ddd-8ddd-dddddddddddd'
    reserve="SELECT public.novu_rust_resend_cancel_reserve('cancel-env','%s','%s','fixture')"%(fresh,legacy_job)
    assert subprocess.run([psql,env['DATABASE_URL'],'-qAt','-v','ON_ERROR_STOP=1','-c',reserve],capture_output=True).returncode!=0
    assert run('resolve','cancel-env',review408,req408,'not_cancelled','checked',override=env).returncode!=0
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c','CREATE TRIGGER fail_cancel_resolution BEFORE INSERT ON public.novu_rust_resend_cancel_resolutions FOR EACH ROW EXECUTE FUNCTION public.fail_cancel_resolution()'],check=True,capture_output=True)
    assert run('resolve','cancel-env',review408,req408,'not_cancelled','checked',override=resolver).returncode!=0
    assert json.loads(run('get','cancel-env',req408).stdout)['request']['state']=='rejected'
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c','DROP TRIGGER fail_cancel_resolution ON public.novu_rust_resend_cancel_resolutions'],check=True,capture_output=True)
    assert run('resolve','cancel-env',review408,req408,'not_cancelled','checked',override=resolver).returncode==0
    assert json.loads(run('get','cancel-env',req408).stdout)['request']['httpStatus']==408
    assert json.loads(run('resolve','cancel-env',review408,req408,'not_cancelled','checked',override=resolver).stdout)['replayed'] is True
    assert subprocess.run([psql,env['DATABASE_URL'],'-qAt','-v','ON_ERROR_STOP=1','-c',reserve],capture_output=True).returncode!=0
    review_sql="SELECT public.novu_rust_resend_cancel_resolve('cancel-env','%s','%s','not_cancelled','checked')"%(review409,req409)
    race=subprocess.Popen([psql,resolver['DATABASE_URL'],'-qAt','-v','ON_ERROR_STOP=1','-c','BEGIN; '+review_sql+'; SELECT pg_sleep(2); COMMIT'],env={**os.environ,'PGAPPNAME':'cancel-legacy-review'},stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    try:
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            state=subprocess.run([psql,admin,'-qAt','-c',"SELECT count(*) FROM pg_stat_activity WHERE application_name='cancel-legacy-review' AND wait_event='PgSleep'"],text=True,capture_output=True)
            if state.stdout.strip()=='1':break
            assert race.poll() is None
            time.sleep(.05)
        else:raise AssertionError('legacy review did not hold job lock')
        reserved=subprocess.run([psql,env['DATABASE_URL'],'-qAt','-v','ON_ERROR_STOP=1','-c',reserve],capture_output=True,text=True,timeout=10)
        assert reserved.returncode==0 and json.loads(reserved.stdout)['fresh'] is True,(reserved.stdout,reserved.stderr)
        out,err=race.communicate(timeout=5);assert race.returncode==0,(out,err)
    finally:
        if race.poll() is None:race.kill();race.wait()
    # If a later unknown request already exists, marking an older one confirmed must roll back.
    third=req408[:-1]+'2';third_resolution='efefefef-efef-4fef-8fef-efefefefefef'
    third_sql=legacy.replace(req408,third)
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',third_sql],check=True,capture_output=True)
    assert run('resolve','cancel-env',third_resolution,third,'confirmed','checked',override=resolver).returncode!=0
    assert json.loads(run('get','cancel-env',third).stdout)['request']['state']=='rejected'
    assert run('resolve','cancel-env',third_resolution,third,'not_cancelled','checked',override=resolver).returncode==0
    assert json.loads(run('get','cancel-env',req409).stdout)['request']['httpStatus']==409
    assert json.loads(run('list-legacy-ambiguous','cancel-env',override=resolver).stdout)['data']==[]
    # Page unknown requests without exposing provider IDs or investigation reasons.
    insert="""WITH jobs AS (INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) SELECT gen_random_uuid(),'cancel-env','experimental_resend_email','{}','completed' FROM generate_series(1,55) RETURNING id)
    INSERT INTO public.novu_rust_resend_cancel_requests(environment_id,request_id,job_id,provider_id,database_principal,principal_oid,reason,state,created_at)
    SELECT 'cancel-env',gen_random_uuid(),jobs.id,'private-provider-id',r.rolname,r.oid,'private reason','unknown',clock_timestamp()-interval '5 minutes' FROM jobs CROSS JOIN pg_roles r WHERE r.rolname='cancel_fixture'"""
    subprocess.run([psql,admin,'-v','ON_ERROR_STOP=1','-c',insert],check=True,capture_output=True)
    first=run('list','cancel-env','60',override=resolver);assert first.returncode==0,(first.stdout,first.stderr)
    page=json.loads(first.stdout);assert len(page['data'])==50 and page['next'];assert 'private' not in first.stdout
    second=run('list','cancel-env','60',page['next'],override=resolver);more=json.loads(second.stdout)
    assert second.returncode==0 and len(more['data'])==7 and more['next'] is None
    ids=[x['requestId'] for x in page['data']+more['data']];assert len(set(ids))==57 and ids==sorted(ids)
    assert all(set(x)=={'requestId','jobId','createdAt','overdue'} for x in page['data']+more['data'])
    assert run('list','cancel-env','0',override=resolver).returncode!=0
    assert run('list','cancel-other','60',override=resolver).returncode!=0
    assert run('list','cancel-env','60',override=denied).returncode!=0
finally:
    server.shutdown();server.server_close();thread.join()
