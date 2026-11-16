import json,os,subprocess,tempfile,threading,time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
psql=os.environ['TEST_PSQL'];admin=os.environ['DATABASE_URL'];port=os.environ['TEST_PORT'];worker=os.environ['TEST_CANCEL_WORKER']
submit=admin.replace('postgres://postgres@','postgres://cancel_submitter@');worker_db=admin.replace('postgres://postgres@','postgres://cancel_worker@');resolver=admin.replace('postgres://postgres@','postgres://cancel_resolver@')
def sql(query,url=admin,ok=True):
    p=subprocess.run([psql,url,'-qAt','-v','ON_ERROR_STOP=1','-c',query],capture_output=True,text=True,timeout=10)
    if ok and p.returncode:raise RuntimeError(p.stderr)
    return p.stdout.strip() if ok else p.returncode
calls=[];auths={};received=threading.Event();release=threading.Event()
class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        calls.append(self.path);auths[self.path]=self.headers.get('Authorization')
        if self.path.startswith('/emails/rotation-'):
            data=json.dumps({'object':'email','id':self.path.split('/')[2]}).encode();self.send_response(200);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data);return
        assert self.headers.get('Authorization')=='Bearer fixture'
        if self.path=='/emails/fixture-five/cancel':received.set();release.wait(10);return
        if self.path in ('/emails/status-408/cancel','/emails/status-409/cancel'):
            self.send_response(int(self.path.split('-')[1].split('/')[0]));self.send_header('Content-Length','0');self.end_headers();return
        assert self.path=='/emails/fixture-four/cancel';data=b'{"object":"email","id":"fixture-four"}'
        self.send_response(200);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    def log_message(self,*args):pass
server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
env={**os.environ,'DATABASE_URL':worker_db,'NOVU_RESEND_ENVIRONMENT_ID':'cancel-env','NOVU_RESEND_API_KEY':'fixture','SWALLOW_RESEND_CANCEL_REQUIRE_ACCOUNT_TAG':'true','SWALLOW_RESEND_ACCOUNT_TAG':'account-a','SWALLOW_TEST_RESEND_ENDPOINT':'http://127.0.0.1:%d/emails'%server.server_port};children=[]
def start():
    p=subprocess.Popen([worker],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);children.append(p);return p
def wait(predicate):
    limit=time.monotonic()+15
    while time.monotonic()<limit:
        if predicate():return
        time.sleep(.1)
    raise AssertionError('timed out')
def enqueue(req,job,url=submit):return sql("SELECT public.novu_rust_resend_cancel_enqueue('cancel-env','%s','%s','fixture')"%(req,job),url)
r1='77777777-7777-4777-8777-777777777777';j1='44444444-4444-4444-8444-444444444444';r2='88888888-8888-4888-8888-888888888888';j2='55555555-5555-4555-8555-555555555555'
try:
    assert json.loads(enqueue(r1,j1))['replayed'] is False
    assert json.loads(enqueue(r1,j1))['replayed'] is True
    assert json.loads(sql("SELECT public.novu_rust_resend_cancel_command_get('cancel-env','%s')"%r1,submit))['state']=='pending'
    assert sql("SELECT public.novu_rust_resend_cancel_enqueue('cancel-env','99999999-9999-4999-8999-999999999999','%s','fixture')"%j1,submit,False)!=0
    assert sql("SELECT public.novu_rust_resend_cancel_enqueue('cancel-env','99999999-9999-4999-8999-999999999999','%s','fixture')"%j1,admin.replace('postgres://postgres@','postgres://cancel_unprivileged@'),False)!=0
    a=start();b=start()
    wait(lambda:sql("SELECT state FROM public.novu_rust_resend_cancel_commands WHERE request_id='%s'"%r1)=='finished')
    assert sql("SELECT state FROM public.novu_rust_resend_cancel_requests WHERE request_id='%s'"%r1)=='confirmed'
    assert json.loads(sql("SELECT public.novu_rust_resend_cancel_command_get('cancel-env','%s')"%r1,submit))['attemptState']=='confirmed'
    assert calls.count('/emails/fixture-four/cancel')==1
    for p in (a,b):p.terminate();p.wait(timeout=5)
    assert json.loads(enqueue(r2,j2))['state']=='pending'
    c=start();assert received.wait(15),'provider did not receive request'
    assert sql("SELECT state FROM public.novu_rust_resend_cancel_requests WHERE request_id='%s'"%r2)=='unknown'
    c.kill();c.wait(timeout=5);release.set();d=start()
    time.sleep(2)
    assert calls.count('/emails/fixture-five/cancel')==1
    assert sql("SELECT state FROM public.novu_rust_resend_cancel_commands WHERE request_id='%s'"%r2)=='dispatching'
    sql("SELECT public.novu_rust_resend_cancel_resolve('cancel-env','aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaab','%s','not_cancelled','verified')"%r2,resolver)
    assert sql("SELECT state FROM public.novu_rust_resend_cancel_commands WHERE request_id='%s'"%r2)=='finished'
    mismatch='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaad';job7='77777777-7777-4777-8777-777777777777';enqueue(mismatch,job7)
    wait(lambda:sql("SELECT state FROM public.novu_rust_resend_cancel_commands WHERE request_id='%s'"%mismatch)=='failed')
    assert '/emails/fixture-seven/cancel' not in calls
    missing='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaae';job8='88888888-8888-4888-8888-888888888888';enqueue(missing,job8)
    wait(lambda:sql("SELECT state FROM public.novu_rust_resend_cancel_commands WHERE request_id='%s'"%missing)=='failed')
    assert '/emails/fixture-eight/cancel' not in calls
    # An expired scheduled job becomes a visible failed command with no network call.
    fail='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaac';job6='66666666-6666-4666-8666-666666666666';enqueue(fail,job6)
    wait(lambda:sql("SELECT state FROM public.novu_rust_resend_cancel_commands WHERE request_id='%s'"%fail)=='failed')
    assert '/emails/fixture-six/cancel' not in calls
    d.terminate();d.wait(timeout=5)
    assert json.loads(enqueue('99999999-9999-4999-8999-999999999999',j2))['state']=='pending'
    sql("UPDATE public.novu_rust_resend_cancel_commands SET created_at=clock_timestamp()-interval '5 minutes' WHERE request_id='99999999-9999-4999-8999-999999999999'")
    monitor={**os.environ,'DATABASE_URL':admin.replace('postgres://postgres@','postgres://cancel_fixture@')}
    reconcile=os.path.join(os.environ['TEST_WORKER_DIR'],'delivery-reconcile')
    def cli(args,env=monitor):return subprocess.run([reconcile,*args],env=env,capture_output=True,text=True,timeout=10)
    health=cli(['check-resend-cancel-commands','cancel-env','60']);assert health.returncode==2 and json.loads(health.stdout)=={'pending':1,'overdue':1,'unreviewed':3}
    assert cli(['check-resend-cancel-commands','cancel-other','60']).returncode!=0
    listing=cli(['list-resend-cancel-failed','cancel-env']);rows=json.loads(listing.stdout)['data'];assert len(rows)==3;failed=next(r for r in rows if r['requestId']==fail)
    assert cli(['ack-resend-cancel-failed','cancel-env',fail,failed['failedAt'],'checked'],monitor).returncode!=0
    reviewer={**monitor,'DATABASE_URL':resolver};assert cli(['ack-resend-cancel-failed','cancel-env',fail,'2020-01-01T00:00:00Z','checked'],reviewer).returncode!=0
    alert_env={**monitor,'SWALLOW_ALERT_RESEND_ENABLED':'false','SWALLOW_ALERT_RESEND_CANCEL_COMMAND_GRACE_SECONDS':'60','SWALLOW_ALERT_SCOPED_DELIVERY':'true','SWALLOW_ALERT_COOLDOWN_POLICY':'new-condition','SWALLOW_ALERT_CONDITIONS_V3':'true','SWALLOW_ALERT_SLACK_WEBHOOK':'https://hooks.slack.com/services/A/B/C','SWALLOW_ALERT_SLACK_TARGET_ID':'blocked'}
    alert=subprocess.run([os.path.join(os.environ['TEST_WORKER_DIR'],'send-delivery-alert'),'cancel-env','300','120','60'],env=alert_env,capture_output=True,text=True,timeout=10)
    assert alert.returncode==2 and 'requires operator review' in alert.stderr,(alert.stdout,alert.stderr)
    sql("CREATE FUNCTION public.fail_cancel_review() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER fail_cancel_review BEFORE INSERT ON public.novu_rust_resend_cancel_command_reviews FOR EACH ROW EXECUTE FUNCTION public.fail_cancel_review()")
    assert cli(['ack-resend-cancel-failed','cancel-env',fail,failed['failedAt'],'checked'],reviewer).returncode!=0
    assert json.loads(cli(['check-resend-cancel-commands','cancel-env','60']).stdout)['unreviewed']==3
    sql('DROP TRIGGER fail_cancel_review ON public.novu_rust_resend_cancel_command_reviews')
    ack=cli(['ack-resend-cancel-failed','cancel-env',fail,failed['failedAt'],'checked'],reviewer);assert ack.returncode==0 and json.loads(ack.stdout)['recorded'] is True
    assert json.loads(cli(['check-resend-cancel-commands','cancel-env','60']).stdout)=={'pending':1,'overdue':1,'unreviewed':2}
    assert json.loads(cli(['ack-resend-cancel-failed','cancel-env',fail,failed['failedAt'],'checked'],reviewer).stdout)['recorded'] is False
    # Keep the deliberately aged fixture from dispatching during the following scenario.
    sql("UPDATE public.novu_rust_resend_cancel_commands SET state='failed' WHERE request_id='99999999-9999-4999-8999-999999999999'")
    # Timeout and conflict can be ambiguous: never release the live-request fence.
    for code in (408,409):
        job='99999999-9999-4999-8999-'+str(code).zfill(12);request='dddddddd-dddd-4ddd-8ddd-'+str(code).zfill(12)
        sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,resend_scheduled_at) VALUES('%s','cancel-env','experimental_resend_email','{}','completed',clock_timestamp()+interval '1 hour'); INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id,account_tag) VALUES('%s','cancel-env','accepted','status-%s','account-a')"%(job,job,code))
        enqueue(request,job)
    worker2=start()
    for code in (408,409):
        request='dddddddd-dddd-4ddd-8ddd-'+str(code).zfill(12);job='99999999-9999-4999-8999-'+str(code).zfill(12)
        wait(lambda:sql("SELECT count(*) FROM public.novu_rust_resend_cancel_requests WHERE request_id='%s' AND state='unknown'"%request)=='1')
        assert sql("SELECT state FROM public.novu_rust_resend_cancel_commands WHERE request_id='%s'"%request)=='dispatching'
        assert sql("SELECT public.novu_rust_resend_cancel_finish('cancel-env','%s','rejected',%s)"%(request,code),worker_db,False)!=0
        assert sql("SELECT public.novu_rust_resend_cancel_enqueue('cancel-env',gen_random_uuid(),'%s','new')"%job,submit,False)!=0
    time.sleep(2)
    for code in (408,409):assert calls.count('/emails/status-%s/cancel'%code)==1,calls
    worker2.terminate();worker2.wait(timeout=5)
    # Rotate the key between commands; invalid files must stop claims, not use stale keys.
    with tempfile.TemporaryDirectory(prefix='swallow-cancel-key-') as directory:
        key=os.path.join(directory,'key');open(key,'w').write('rotation-one');os.chmod(key,0o600)
        rotation_env={**env,'NOVU_RESEND_API_KEY_FILE':key,'NOVU_RESEND_RELOAD_API_KEY':'true'};rotation_env.pop('NOVU_RESEND_API_KEY')
        rotated=subprocess.Popen([worker],env=rotation_env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);children.append(rotated)
        def rotation_job(n):
            job='cccccccc-cccc-4ccc-8ccc-'+str(n).zfill(12);request='eeeeeeee-eeee-4eee-8eee-'+str(n).zfill(12)
            sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,resend_scheduled_at) VALUES('%s','cancel-env','experimental_resend_email','{}','completed',clock_timestamp()+interval '1 hour'); INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id,account_tag) VALUES('%s','cancel-env','accepted','rotation-%s','account-a')"%(job,job,n))
            enqueue(request,job);return request
        first=rotation_job(1);wait(lambda:sql("SELECT state FROM public.novu_rust_resend_cancel_commands WHERE request_id='%s'"%first)=='finished')
        assert auths['/emails/rotation-1/cancel']=='Bearer rotation-one'
        open(key,'w').write('');time.sleep(3);second=rotation_job(2);time.sleep(3)
        assert sql("SELECT state FROM public.novu_rust_resend_cancel_commands WHERE request_id='%s'"%second)=='pending'
        assert '/emails/rotation-2/cancel' not in calls
        replacement=os.path.join(directory,'replacement');open(replacement,'w').write('rotation-two');os.chmod(replacement,0o600);os.replace(replacement,key)
        wait(lambda:sql("SELECT state FROM public.novu_rust_resend_cancel_commands WHERE request_id='%s'"%second)=='finished')
        assert auths['/emails/rotation-2/cancel']=='Bearer rotation-two'
        rotated.terminate();rotated.wait(timeout=5)
        exit_env={**rotation_env,'SWALLOW_RESEND_CANCEL_RELOAD_FAILURE_POLICY':'exit'}
        exiting=subprocess.Popen([worker],env=exit_env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);children.append(exiting)
        time.sleep(2);open(key,'w').write('');exiting.wait(timeout=8);assert exiting.returncode!=0
        log=exiting.stderr.read().decode();assert 'reload failed' in log and 'rotation-two' not in log and key not in log
        conflict={**rotation_env,'NOVU_RESEND_API_KEY':'inline'}
        bad=subprocess.run([worker],env=conflict,capture_output=True,text=True,timeout=5);assert bad.returncode!=0 and 'inline' not in bad.stderr and key not in bad.stderr
        missing_file=dict(rotation_env);missing_file.pop('NOVU_RESEND_API_KEY_FILE')
        invalid_policy={**rotation_env,'SWALLOW_RESEND_CANCEL_RELOAD_FAILURE_POLICY':'invalid'}
        exit_without_reload={**env,'SWALLOW_RESEND_CANCEL_RELOAD_FAILURE_POLICY':'exit'}
        for settings in (missing_file,invalid_policy,exit_without_reload):
            failed=subprocess.run([worker],env=settings,capture_output=True,text=True,timeout=5);assert failed.returncode!=0 and key not in failed.stderr
finally:
    release.set()
    for p in children:
        if p.poll() is None:p.terminate()
        try:p.wait(timeout=5)
        except subprocess.TimeoutExpired:p.kill();p.wait()
    server.shutdown();server.server_close();thread.join()
