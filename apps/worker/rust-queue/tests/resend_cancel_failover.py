#!/usr/bin/env python3
"""Synchronous single-host promotion during an actual cancellation worker call."""
import http.server,os,subprocess,threading,time
worker=os.environ['TEST_CANCEL_WORKER'];psql=os.environ['TEST_PSQL'];primary=os.environ['TEST_PRIMARY_URL'];standby=os.environ['TEST_STANDBY_URL'];cluster=os.environ['TEST_FAILOVER_URL'];pgctl=os.environ['TEST_LOCAL_PG_CTL']
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
    submit_primary=primary.replace('postgres://postgres@','postgres://cancel_ha_submit@',1)
    sql(submit_primary,"SELECT public.novu_rust_resend_cancel_enqueue('%s','%s','%s','fixture')"%(env,req1,job1))
    config={**os.environ,'DATABASE_URL':cluster.replace('postgres://postgres@','postgres://cancel_ha_worker@',1),'NOVU_POSTGRES_ALLOW_INSECURE':'true','NOVU_RESEND_ENVIRONMENT_ID':env,'NOVU_RESEND_API_KEY':'fixture','SWALLOW_RESEND_CANCEL_REQUIRE_ACCOUNT_TAG':'true','SWALLOW_RESEND_ACCOUNT_TAG':'account-a','SWALLOW_TEST_RESEND_ENDPOINT':'http://127.0.0.1:%d/emails'%server.server_port}
    first=subprocess.Popen([worker],env=config,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);children.append(first)
    assert received.wait(20),'provider request not observed'
    until(lambda:sql(standby,"SELECT count(*) FROM public.novu_rust_resend_cancel_commands c JOIN public.novu_rust_resend_cancel_requests r USING(environment_id,request_id) WHERE c.request_id='%s' AND c.state='dispatching' AND r.state='unknown'"%req1)=='1','unknown marker not replicated')
    control(os.environ['TEST_LOCAL_PRIMARY_DATA'],'-m','immediate','stop');control(os.environ['TEST_LOCAL_STANDBY_DATA'],'promote')
    first.kill();first.wait(timeout=5);release.set()
    second=subprocess.Popen([worker],env=config,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);children.append(second)
    submit_standby=standby.replace('postgres://postgres@','postgres://cancel_ha_submit@',1)
    sql(submit_standby,"SELECT public.novu_rust_resend_cancel_enqueue('%s','%s','%s','fixture')"%(env,req2,job2))
    until(lambda:sql(standby,"SELECT count(*) FROM public.novu_rust_resend_cancel_commands c JOIN public.novu_rust_resend_cancel_requests r USING(environment_id,request_id) WHERE c.request_id='%s' AND c.state='finished' AND r.state='confirmed'"%req2)=='1','second command did not complete')
    time.sleep(2)
    with lock:assert calls.count('/emails/first-provider/cancel')==1 and calls.count('/emails/second-provider/cancel')==1,calls
    assert sql(standby,"SELECT count(*) FROM public.novu_rust_resend_cancel_commands c JOIN public.novu_rust_resend_cancel_requests r USING(environment_id,request_id) WHERE c.request_id='%s' AND c.state='dispatching' AND r.state='unknown'"%req1)=='1'
    print('Synchronous PostgreSQL promotion preserved unknown cancellation and did not automatically repeat it; a later command completed.')
finally:
    release.set()
    for child in children:
        if child.poll() is None:child.terminate()
        try:child.wait(timeout=5)
        except subprocess.TimeoutExpired:child.kill();child.wait()
    server.shutdown();server.server_close();thread.join(timeout=5)
