#!/usr/bin/env python3
"""Synchronous single-host promotion during an actual cancellation worker call."""
import http.server,os,subprocess,threading,time
worker=os.environ['TEST_CANCEL_WORKER'];psql=os.environ['TEST_PSQL'];primary=os.environ['TEST_PRIMARY_URL'];standby=os.environ['TEST_STANDBY_URL'];cluster=os.environ['TEST_FAILOVER_URL'];pgctl=os.environ['TEST_LOCAL_PG_CTL']
credential_ha=os.environ.get('TEST_CANCEL_CREDENTIAL_HA')=='true'
children=[];calls=[];received=threading.Event();release=threading.Event();lock=threading.Lock()
def sql(url,query):
    p=subprocess.run([psql,url,'-qAt','-v','ON_ERROR_STOP=1','-c',query],capture_output=True,text=True,timeout=10)
    if p.returncode:raise RuntimeError('fixture SQL failed: '+p.stderr)
    return p.stdout.strip()
def until(check,message,timeout=30):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        if check():return
        time.sleep(.1)
    raise RuntimeError(message)
def control(data,*args):
    p=subprocess.run([pgctl,'-D',data,*args,'-w'],capture_output=True,timeout=30)
    if p.returncode:raise RuntimeError('fixture PostgreSQL control failed')
class Provider(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        assert self.headers.get('Authorization')=='Bearer fixture'
        with lock:calls.append(self.path)
        if self.path=='/emails/first-provider/cancel':received.set();release.wait(15);return
        assert self.path=='/emails/second-provider/cancel';body=b'{"object":"email","id":"second-provider"}';self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    def log_message(self,*args):pass
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Provider);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
env='resend-cancel-ha';job1='11111111-1111-4111-8111-111111111111';job2='22222222-2222-4222-8222-222222222222';req1='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';req2='bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
try:
    sql(primary,"""CREATE ROLE cancel_ha_submit LOGIN;CREATE ROLE cancel_ha_worker LOGIN;
GRANT USAGE ON SCHEMA public TO cancel_ha_submit,cancel_ha_worker;
GRANT EXECUTE ON FUNCTION public.novu_rust_resend_cancel_enqueue(text,uuid,uuid,text) TO cancel_ha_submit;
GRANT EXECUTE ON FUNCTION public.novu_rust_resend_cancel_claim_tagged(text,text),public.novu_rust_resend_cancel_finish(text,uuid,text,integer) TO cancel_ha_worker;
INSERT INTO public.novu_rust_environments VALUES ('resend-cancel-ha','org');
INSERT INTO public.novu_rust_resend_cancel_submitters SELECT 'resend-cancel-ha',rolname,oid FROM pg_roles WHERE rolname='cancel_ha_submit';
INSERT INTO public.novu_rust_resend_cancel_command_workers SELECT 'resend-cancel-ha',rolname,oid FROM pg_roles WHERE rolname='cancel_ha_worker';
INSERT INTO public.novu_rust_resend_cancel_operators(environment_id,database_principal,principal_oid,can_cancel) SELECT 'resend-cancel-ha',rolname,oid,true FROM pg_roles WHERE rolname='cancel_ha_worker';
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,resend_scheduled_at) VALUES ('%s','resend-cancel-ha','experimental_resend_email','{}','completed',clock_timestamp()+interval '1 hour'),('%s','resend-cancel-ha','experimental_resend_email','{}','completed',clock_timestamp()+interval '1 hour');
INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id,account_tag) VALUES ('%s','resend-cancel-ha','accepted','first-provider','account-a'),('%s','resend-cancel-ha','accepted','second-provider','account-a');"""%(job1,job2,job1,job2))
    if credential_ha:
        sql(primary,"""CREATE ROLE cancel_ha_monitor LOGIN;
GRANT USAGE ON SCHEMA public TO cancel_ha_monitor;
GRANT EXECUTE ON FUNCTION public.novu_rust_resend_cancel_credential_heartbeat(text,uuid,text) TO cancel_ha_worker;
GRANT EXECUTE ON FUNCTION public.novu_rust_resend_cancel_credential_health_check(text,integer) TO cancel_ha_monitor;
GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert_v3(text,text,text,integer,bigint,bigint,bigint,bigint),public.novu_rust_observe_alert_conditions_v3(text,text,text,bigint,bigint,bigint,bigint) TO cancel_ha_monitor;
INSERT INTO public.novu_rust_resend_cancel_credential_workers SELECT 'resend-cancel-ha',rolname,oid FROM pg_roles WHERE rolname='cancel_ha_worker';
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'resend-cancel-ha',rolname,'alert_view',oid FROM pg_roles WHERE rolname='cancel_ha_monitor';
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'resend-cancel-ha',rolname,'alert_send',oid FROM pg_roles WHERE rolname='cancel_ha_monitor';
INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES('resend-cancel-ha','slack','blocked','rejected');
INSERT INTO public.novu_rust_alert_v2_targets(environment_id,channel,target_id) VALUES('resend-cancel-ha','slack','blocked');""")
        key=os.path.join(os.environ['TEST_FIXTURE_DIR'],'cancel-key');open(key,'w').write('fixture');os.chmod(key,0o600)
    submit_primary=primary.replace('postgres://postgres@','postgres://cancel_ha_submit@',1)
    sql(submit_primary,"SELECT public.novu_rust_resend_cancel_enqueue('%s','%s','%s','fixture')"%(env,req1,job1))
    config={**os.environ,'DATABASE_URL':cluster.replace('postgres://postgres@','postgres://cancel_ha_worker@',1),'NOVU_POSTGRES_ALLOW_INSECURE':'true','NOVU_RESEND_ENVIRONMENT_ID':env,'NOVU_RESEND_API_KEY':'fixture','SWALLOW_RESEND_CANCEL_REQUIRE_ACCOUNT_TAG':'true','SWALLOW_RESEND_ACCOUNT_TAG':'account-a','SWALLOW_TEST_RESEND_ENDPOINT':'http://127.0.0.1:%d/emails'%server.server_port}
    if credential_ha:
        config.pop('NOVU_RESEND_API_KEY');config.update({'NOVU_RESEND_API_KEY_FILE':key,'NOVU_RESEND_RELOAD_API_KEY':'true','SWALLOW_RESEND_CANCEL_CREDENTIAL_HEALTH_ENABLED':'true'})
    first=subprocess.Popen([worker],env=config,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);children.append(first)
    assert received.wait(20),'provider request not observed'
    until(lambda:sql(standby,"SELECT count(*) FROM public.novu_rust_resend_cancel_commands c JOIN public.novu_rust_resend_cancel_requests r USING(environment_id,request_id) WHERE c.request_id='%s' AND c.state='dispatching' AND r.state='unknown'"%req1)=='1','unknown marker not replicated')
    if credential_ha:
        until(lambda:sql(standby,"SELECT count(*) FROM public.novu_rust_resend_cancel_credential_health WHERE environment_id='resend-cancel-ha' AND state='healthy'")=='1','credential heartbeat not replicated')
        open(key,'w').write('')
    control(os.environ['TEST_LOCAL_PRIMARY_DATA'],'-m','immediate','stop');control(os.environ['TEST_LOCAL_STANDBY_DATA'],'promote')
    if credential_ha:
        monitor=standby.replace('postgres://postgres@','postgres://cancel_ha_monitor@',1)
        assert sql(monitor,"SELECT healthy FROM public.novu_rust_resend_cancel_credential_health_check('resend-cancel-ha',60)")=='0'
        release.set()
        until(lambda:sql(monitor,"SELECT unhealthy FROM public.novu_rust_resend_cancel_credential_health_check('resend-cancel-ha',60)")=='1','worker did not reconnect and report key failure')
        alert_env={**os.environ,'DATABASE_URL':monitor,'NOVU_POSTGRES_ALLOW_INSECURE':'true','SWALLOW_ALERT_RESEND_ENABLED':'false','SWALLOW_ALERT_RESEND_CANCEL_CREDENTIAL_MAX_AGE_SECONDS':'60','SWALLOW_ALERT_RESEND_CANCEL_CREDENTIAL_MIN_HEALTHY':'1','SWALLOW_ALERT_SCOPED_DELIVERY':'true','SWALLOW_ALERT_COOLDOWN_POLICY':'new-condition','SWALLOW_ALERT_CONDITIONS_V3':'true','SWALLOW_ALERT_SLACK_WEBHOOK':'https://hooks.slack.com/services/A/B/C','SWALLOW_ALERT_SLACK_TARGET_ID':'blocked'}
        def alert():return subprocess.run([os.environ['TEST_CANCEL_ALERT'],env,'300','120','60'],env=alert_env,capture_output=True,text=True,timeout=10)
        raised=alert();assert raised.returncode==2 and 'requires operator review' in raised.stderr,(raised.stdout,raised.stderr)
    else:
        first.kill();first.wait(timeout=5);release.set()
        second=subprocess.Popen([worker],env=config,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);children.append(second)
    submit_standby=standby.replace('postgres://postgres@','postgres://cancel_ha_submit@',1)
    sql(submit_standby,"SELECT public.novu_rust_resend_cancel_enqueue('%s','%s','%s','fixture')"%(env,req2,job2))
    if credential_ha:
        time.sleep(3);assert sql(standby,"SELECT state FROM public.novu_rust_resend_cancel_commands WHERE request_id='%s'"%req2)=='pending'
        with lock:assert '/emails/second-provider/cancel' not in calls
        replacement=key+'-new';open(replacement,'w').write('fixture');os.chmod(replacement,0o600);os.replace(replacement,key)
        until(lambda:sql(monitor,"SELECT healthy FROM public.novu_rust_resend_cancel_credential_health_check('resend-cancel-ha',60)")=='1','worker did not report recovered key')
        recovered=alert();assert recovered.returncode==0,(recovered.stdout,recovered.stderr)
    until(lambda:sql(standby,"SELECT count(*) FROM public.novu_rust_resend_cancel_commands c JOIN public.novu_rust_resend_cancel_requests r USING(environment_id,request_id) WHERE c.request_id='%s' AND c.state='finished' AND r.state='confirmed'"%req2)=='1','second command did not complete')
    time.sleep(2)
    with lock:assert calls.count('/emails/first-provider/cancel')==1 and calls.count('/emails/second-provider/cancel')==1,calls
    assert sql(standby,"SELECT count(*) FROM public.novu_rust_resend_cancel_commands c JOIN public.novu_rust_resend_cancel_requests r USING(environment_id,request_id) WHERE c.request_id='%s' AND c.state='dispatching' AND r.state='unknown'"%req1)=='1'
    if credential_ha:assert first.poll() is None
    print('Synchronous PostgreSQL promotion preserved unknown cancellation and did not automatically repeat it; a later command completed.')
    if credential_ha:print('The same worker reconnected, reported invalid credentials, paused claims, and recovered after key replacement.')
finally:
    release.set()
    for child in children:
        if child.poll() is None:child.terminate()
        try:child.wait(timeout=5)
        except subprocess.TimeoutExpired:child.kill();child.wait()
    server.shutdown();server.server_close();thread.join(timeout=5)
