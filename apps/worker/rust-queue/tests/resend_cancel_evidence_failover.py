#!/usr/bin/env python3
"""Single-host synchronous promotion with signed cancellation evidence."""
import base64,hashlib,hmac,json,os,socket,subprocess,time,urllib.error,urllib.request
primary=os.environ['TEST_PRIMARY_URL'];standby=os.environ['TEST_STANDBY_URL'];psql=os.environ['TEST_PSQL'];pgctl=os.environ['TEST_LOCAL_PG_CTL'];api=None
job='11111111-1111-4111-8111-111111111111';request='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';env='cancel-evidence-ha';secret=b'test-secret-is-32-bytes-long!!'
def sql(url,query):
    p=subprocess.run([psql,url,'-qAt','-v','ON_ERROR_STOP=1','-c',query],capture_output=True,text=True,timeout=15)
    if p.returncode:raise RuntimeError('fixture SQL failed: '+p.stderr)
    return p.stdout.strip()
def until(predicate,message,timeout=30):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        if predicate():return
        time.sleep(.1)
    raise RuntimeError(message)
def control(data,*args):
    p=subprocess.run([pgctl,'-D',data,*args,'-w'],capture_output=True,timeout=30)
    if p.returncode:raise RuntimeError('fixture PostgreSQL control failed')
def stop_api():
    global api
    if api and api.poll() is None:api.terminate();api.wait(timeout=10)
    api=None
def start_api(url):
    global api
    with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    settings={**os.environ,'DATABASE_URL':url.replace('postgres://postgres@','postgres://cancel_evidence_ha_api@',1),'NOVU_POSTGRES_ALLOW_INSECURE':'true','LISTEN_ADDR':'127.0.0.1:'+str(port),'SWALLOW_API_HEALTH_ENDPOINTS_ENABLED':'true','NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID':env,'NOVU_RESEND_WEBHOOK_SECRET':'whsec_'+base64.b64encode(secret).decode()}
    api=subprocess.Popen([os.environ['TEST_API_BINARY']],env=settings,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    origin='http://127.0.0.1:'+str(port)
    def ready():
        if api.poll() is not None:raise RuntimeError('API exited')
        try:
            with urllib.request.urlopen(origin+'/health/ready',timeout=1) as res:return res.status==200
        except (OSError,urllib.error.URLError):return False
    until(ready,'API not ready');return origin
def alert(url):
    settings={**os.environ,'DATABASE_URL':url.replace('postgres://postgres@','postgres://cancel_evidence_ha_monitor@',1),'NOVU_POSTGRES_ALLOW_INSECURE':'true','SWALLOW_ALERT_RESEND_ENABLED':'true','SWALLOW_ALERT_CHECK_SCOPED_ACCESS':'true','SWALLOW_ALERT_RESEND_EXCLUDE_CONFIRMED_CANCEL':'true','SWALLOW_ALERT_RESEND_CANCEL_EVIDENCE_ENABLED':'true','SWALLOW_ALERT_SCOPED_DELIVERY':'true','SWALLOW_ALERT_COOLDOWN_POLICY':'new-condition','SWALLOW_ALERT_CONDITIONS_V3':'true','SWALLOW_ALERT_SLACK_WEBHOOK':'https://hooks.slack.com/services/A/B/C','SWALLOW_ALERT_SLACK_TARGET_ID':'blocked'}
    return subprocess.run([os.environ['TEST_ALERT_BINARY'],env,'60','120','60'],env=settings,capture_output=True,text=True,timeout=15)
def webhook(origin):
    body=json.dumps({'type':'email.delivery_delayed','data':{'email_id':'fixture-provider','to':['fixture@example.test']}},separators=(',',':')).encode();ts=str(int(time.time()));eid='after-promotion'
    signature=base64.b64encode(hmac.new(secret,(eid+'.'+ts+'.').encode()+body,hashlib.sha256).digest()).decode()
    req=urllib.request.Request(origin+'/webhooks/organizations/org/environments/'+env+'/email/resend',data=body,method='POST',headers={'svix-id':eid,'svix-timestamp':ts,'svix-signature':'v1,'+signature,'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=5) as res:return res.status
try:
    sql(primary,"""CREATE ROLE cancel_evidence_ha_api LOGIN;CREATE ROLE cancel_evidence_ha_monitor LOGIN;
GRANT USAGE ON SCHEMA public TO cancel_evidence_ha_api,cancel_evidence_ha_monitor;
GRANT SELECT ON public.novu_rust_environments TO cancel_evidence_ha_api;
GRANT SELECT,INSERT,UPDATE ON public.novu_rust_resend_delivery_events TO cancel_evidence_ha_api;
GRANT EXECUTE ON FUNCTION public.novu_rust_resend_health_exclude_confirmed_cancel(text,integer,integer),public.novu_rust_resend_cancel_evidence_health(text),public.novu_rust_reserve_alert_v3(text,text,text,integer,bigint,bigint,bigint,bigint),public.novu_rust_observe_alert_conditions_v3(text,text,text,bigint,bigint,bigint,bigint) TO cancel_evidence_ha_monitor;
INSERT INTO public.novu_rust_environments VALUES('cancel-evidence-ha','org');
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'cancel-evidence-ha',rolname,decision,oid FROM pg_roles CROSS JOIN (VALUES('alert_view'),('alert_send')) x(decision) WHERE rolname='cancel_evidence_ha_monitor';
INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES('cancel-evidence-ha','slack','blocked','rejected');
INSERT INTO public.novu_rust_alert_v2_targets(environment_id,channel,target_id) VALUES('cancel-evidence-ha','slack','blocked');
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,available_at,resend_scheduled_at) VALUES('%s','cancel-evidence-ha','experimental_resend_email','{}','completed',clock_timestamp()-interval '2 hours',clock_timestamp()-interval '1 hour');
INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES('%s','cancel-evidence-ha','accepted','fixture-provider');
INSERT INTO public.novu_rust_resend_cancel_requests(environment_id,request_id,job_id,provider_id,database_principal,principal_oid,reason,state) SELECT 'cancel-evidence-ha','%s','%s','fixture-provider',rolname,oid,'fixture','confirmed' FROM pg_roles WHERE rolname='cancel_evidence_ha_monitor';
INSERT INTO public.novu_rust_resend_projection_heartbeat VALUES('cancel-evidence-ha',clock_timestamp());"""%(job,job,request,job))
    monitor_primary=primary.replace('postgres://postgres@','postgres://cancel_evidence_ha_monitor@',1)
    query="SELECT overdue,eligible FROM public.novu_rust_resend_health_exclude_confirmed_cancel('cancel-evidence-ha',60,120)"
    assert sql(monitor_primary,query)=='0|0'
    first=alert(primary);assert first.returncode==0,(first.returncode,first.stderr)
    until(lambda:sql(standby,"SELECT count(*) FROM public.novu_rust_resend_cancel_requests WHERE request_id='%s' AND state='confirmed'"%request)=='1','confirmation not replicated')
    origin=start_api(primary);control(os.environ['TEST_LOCAL_PRIMARY_DATA'],'-m','immediate','stop');control(os.environ['TEST_LOCAL_STANDBY_DATA'],'promote');stop_api()
    origin=start_api(standby)
    monitor_standby=standby.replace('postgres://postgres@','postgres://cancel_evidence_ha_monitor@',1)
    assert sql(monitor_standby,query)=='0|0'
    assert webhook(origin)==204 and webhook(origin)==204
    assert sql(monitor_standby,"SELECT conflicting,ambiguous FROM public.novu_rust_resend_cancel_evidence_health('cancel-evidence-ha')")=='1|0'
    assert sql(monitor_standby,query)=='1|1'
    after=alert(standby);assert after.returncode==2 and 'requires operator review' in after.stderr,(after.returncode,after.stderr)
    print('Synchronous promotion preserved cancellation; signed callback after API restart reactivated health and blocked alert.')
finally:
    stop_api()
