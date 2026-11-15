#!/usr/bin/env python3
"""Two real recovery workers, actual SMTP worker and an isolated mock relay."""
import json,os,pathlib,socket,socketserver,subprocess,threading,time,uuid
psql=os.environ['TEST_PSQL'];db=os.environ['DATABASE_URL'];work=os.environ['TEST_WORKER_DIR'];children=[];messages=[];lock=threading.Lock()
def sql(query):
    p=subprocess.run([psql,db,'-q','-At','-v','ON_ERROR_STOP=1','-c',query],capture_output=True,text=True,timeout=10)
    if p.returncode:raise RuntimeError('fixture SQL failed: '+p.stderr)
    return p.stdout.strip()
def until(predicate,message,timeout=25):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        if any(p.poll() is not None for p in children):raise RuntimeError('fixture worker exited unexpectedly')
        if predicate():return
        time.sleep(.1)
    raise RuntimeError(message)
class Relay(socketserver.StreamRequestHandler):
    def handle(self):
        self.request.settimeout(10);self.wfile.write(b'220 fixture ESMTP\r\n');assert self.rfile.readline().startswith(b'EHLO');self.wfile.write(b'250 localhost\r\n')
        while True:
            line=self.rfile.readline()
            if line.startswith((b'MAIL FROM',b'RCPT TO')):self.wfile.write(b'250 OK\r\n')
            elif line.startswith(b'DATA'):self.wfile.write(b'354 Continue\r\n');break
            else:raise RuntimeError('unexpected fixture SMTP command')
        body=bytearray()
        while True:
            line=self.rfile.readline()
            if not line:raise RuntimeError('truncated DATA')
            if line==b'.\r\n':break
            body.extend(line)
        subject='repeated' if b'Subject: repeated' in body else 'successful'
        with lock:
            messages.append(subject);n=messages.count(subject)
        if subject=='successful' and n==2:self.wfile.write(b'250 accepted\r\n')
        else:self.request.shutdown(socket.SHUT_RDWR)
class Server(socketserver.ThreadingTCPServer):allow_reuse_address=True;daemon_threads=True
server=Server(('127.0.0.1',0),Relay);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
try:
    env='smtp-auto-fixture';other='smtp-auto-other';first=str(uuid.uuid4());second=str(uuid.uuid4());old=str(uuid.uuid4());reviewed=str(uuid.uuid4())
    sql("""CREATE ROLE smtp_auto_fixture LOGIN; GRANT USAGE ON SCHEMA public TO smtp_auto_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_smtp_auto_retry_batch(text,integer),public.novu_rust_smtp_auto_retry_batch_observed(text,integer),public.novu_rust_smtp_auto_worker_health_check(text,integer),public.novu_rust_smtp_auto_history(text,bigint),public.novu_rust_smtp_auto_retry_health(text,integer) TO smtp_auto_fixture;
GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert_v3(text,text,text,integer,bigint,bigint,bigint,bigint),public.novu_rust_observe_alert_conditions_v3(text,text,text,bigint,bigint,bigint,bigint) TO smtp_auto_fixture;
INSERT INTO public.novu_rust_environments VALUES('smtp-auto-fixture','org'),('smtp-auto-other','org');
INSERT INTO public.novu_rust_smtp_auto_retry_workers SELECT 'smtp-auto-fixture',rolname,oid FROM pg_roles WHERE rolname='smtp_auto_fixture';
INSERT INTO public.novu_rust_smtp_auto_admins SELECT 'smtp-auto-fixture',rolname,oid,false,false,true FROM pg_roles WHERE rolname='smtp_auto_fixture';
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'smtp-auto-fixture',rolname,'alert_view',oid FROM pg_roles WHERE rolname='smtp_auto_fixture';
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'smtp-auto-fixture',rolname,'alert_send',oid FROM pg_roles WHERE rolname='smtp_auto_fixture';
INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES('smtp-auto-fixture','slack','blocked','rejected');
INSERT INTO public.novu_rust_alert_v2_targets(environment_id,channel,target_id) VALUES('smtp-auto-fixture','slack','blocked');
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES ('%s','smtp-auto-fixture','experimental_smtp_email','{"to":["first@example.test"],"subject":"successful","html":"hello"}');"""%first)
    cfg={**os.environ,'NOVU_POSTGRES_ALLOW_INSECURE':'true','NOVU_SMTP_ENVIRONMENT_ID':env,'SWALLOW_SMTP_HOST':'127.0.0.1','SWALLOW_SMTP_PORT':str(server.server_address[1]),'SWALLOW_SMTP_TLS':'none','SWALLOW_SMTP_FROM':'sender@example.test'}
    for key in ('SWALLOW_SMTP_USERNAME','SWALLOW_SMTP_PASSWORD','SWALLOW_SMTP_PASSWORD_FILE','SWALLOW_SMTP_CA_FILE','SWALLOW_SMTP_TRANSPORT_GUARD'):cfg.pop(key,None)
    children.append(subprocess.Popen([str(pathlib.Path(work)/'smtp-worker')],env=cfg,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL))
    until(lambda:sql("SELECT count(*) FROM public.novu_rust_smtp_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id WHERE a.job_id='%s' AND a.state='unknown' AND j.status='failed'"%first)=='1','first unknown attempt missing')
    # Register a historical failure that must remain outside the explicit cutoff.
    sql("""INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES('%s','smtp-auto-fixture','experimental_smtp_email','{}','failed');
INSERT INTO public.novu_rust_smtp_attempts(job_id,environment_id,state,updated_at) VALUES('%s','smtp-auto-fixture','unknown',clock_timestamp()-interval '2 hours');
UPDATE public.novu_rust_smtp_attempts SET updated_at=clock_timestamp()-interval '20 seconds' WHERE job_id='%s';"""%(old,old,first))
    sql("""INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES('%s','smtp-auto-fixture','experimental_smtp_email','{}','failed');
INSERT INTO public.novu_rust_smtp_attempts(job_id,environment_id,state,updated_at) VALUES('%s','smtp-auto-fixture','unknown',clock_timestamp()-interval '20 seconds');
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'smtp-auto-fixture',rolname,'queue_ack',oid FROM pg_roles WHERE rolname='postgres';
SELECT public.novu_rust_ack_smtp('smtp-auto-fixture','%s',a.revision,j.failure_revision,'fixture','reviewed') FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id WHERE j.id='%s';"""%(reviewed,reviewed,reviewed,reviewed))
    runner_url=db.replace('postgres://postgres@','postgres://smtp_auto_fixture@',1)
    runner={**os.environ,'DATABASE_URL':runner_url,'NOVU_POSTGRES_ALLOW_INSECURE':'true','NOVU_SMTP_ENVIRONMENT_ID':env,'SWALLOW_SMTP_AUTO_RETRY_INTERVAL_SECONDS':'1','SWALLOW_SMTP_AUTO_WORKER_HEALTH':'true'}
    def health(grace='1'):
        p=subprocess.run([str(pathlib.Path(work)/'delivery-reconcile'),'check-smtp-auto',env,grace],env=runner,capture_output=True,text=True,timeout=10)
        return p.returncode,json.loads(p.stdout) if p.stdout else None
    assert health()==(0,{'enabled':False,'eligible':0,'overdue':0,'exhausted':0})
    assert subprocess.run([str(pathlib.Path(work)/'delivery-reconcile'),'check-smtp-auto','smtp-auto-other','1'],env=runner,capture_output=True,timeout=10).returncode==1
    denied=subprocess.run([psql,runner_url,'-q','-At','-v','ON_ERROR_STOP=1','-c',"SELECT public.novu_rust_smtp_auto_retry_batch('smtp-auto-other',1)"],capture_output=True,timeout=10)
    assert denied.returncode!=0
    for _ in range(2):children.append(subprocess.Popen([str(pathlib.Path(work)/'smtp-auto-retry-worker')],env=runner,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL))
    def worker_health(minimum='2'):
        p=subprocess.run([str(pathlib.Path(work)/'delivery-reconcile'),'check-smtp-auto-workers',env,'10',minimum],env=runner,capture_output=True,text=True,timeout=10)
        return p.returncode,json.loads(p.stdout) if p.stdout else None
    until(lambda:worker_health()[1]['healthy']==2,'worker heartbeats missing')
    assert worker_health()[0]==0
    time.sleep(2);assert sql("SELECT count(*) FROM public.novu_rust_smtp_auto_retries")=='0'
    sql("CREATE FUNCTION public.fixture_smtp_retry_audit_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER fixture_smtp_retry_audit_fail BEFORE INSERT ON public.novu_rust_smtp_auto_retries FOR EACH ROW EXECUTE FUNCTION public.fixture_smtp_retry_audit_fail(); UPDATE public.novu_rust_smtp_auto_retry_policy SET enabled=true,max_retries=1,delay_seconds=10,eligible_since=clock_timestamp()-interval '1 hour' WHERE environment_id='smtp-auto-fixture'")
    time.sleep(2)
    assert sql("SELECT count(*) FROM public.novu_rust_smtp_auto_retries")=='0'
    assert sql("SELECT count(*) FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id WHERE j.id='%s' AND j.status='failed' AND a.state='unknown'"%first)=='1'
    assert health()==(2,{'enabled':True,'eligible':1,'overdue':1,'exhausted':0})
    assert health('0')[0]==1
    alert_env={**runner,'SWALLOW_ALERT_RESEND_ENABLED':'false','SWALLOW_ALERT_SMTP_AUTO_GRACE_SECONDS':'1','SWALLOW_ALERT_SCOPED_DELIVERY':'true','SWALLOW_ALERT_COOLDOWN_POLICY':'new-condition','SWALLOW_ALERT_CONDITIONS_V3':'true','SWALLOW_ALERT_SLACK_WEBHOOK':'https://hooks.slack.com/services/A/B/C','SWALLOW_ALERT_SLACK_TARGET_ID':'blocked'}
    alert=subprocess.run([str(pathlib.Path(work)/'send-delivery-alert'),env,'300','120','60'],env=alert_env,capture_output=True,text=True,timeout=15)
    assert alert.returncode==2 and 'requires operator review' in alert.stderr,alert.stderr
    bad_env={**alert_env,'SWALLOW_ALERT_CONDITIONS_V3':'false'}
    assert subprocess.run([str(pathlib.Path(work)/'send-delivery-alert'),env,'300','120','60'],env=bad_env,capture_output=True,timeout=15).returncode==1
    sql("DROP TRIGGER fixture_smtp_retry_audit_fail ON public.novu_rust_smtp_auto_retries; DROP FUNCTION public.fixture_smtp_retry_audit_fail()")
    until(lambda:sql("SELECT count(*) FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id WHERE j.id='%s' AND j.status='completed' AND a.state='accepted'"%first)=='1','automatic replay did not reach relay')
    assert sql("SELECT count(*) FROM public.novu_rust_smtp_auto_retries WHERE job_id='%s'"%first)=='1'
    assert sql("SELECT count(*) FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id WHERE j.id='%s' AND j.status='failed' AND a.state='unknown'"%old)=='1'
    assert sql("SELECT count(*) FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id WHERE j.id='%s' AND j.status='failed' AND a.state='unknown'"%reviewed)=='1'
    sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES ('%s','smtp-auto-fixture','experimental_smtp_email','{\"to\":[\"second@example.test\"],\"subject\":\"repeated\",\"html\":\"hello\"}')"%second)
    until(lambda:sql("SELECT count(*) FROM public.novu_rust_smtp_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id WHERE a.job_id='%s' AND a.state='unknown' AND j.status='failed'"%second)=='1','second unknown attempt missing')
    sql("UPDATE public.novu_rust_smtp_attempts SET updated_at=clock_timestamp()-interval '20 seconds' WHERE job_id='%s'"%second)
    until(lambda:sql("SELECT count(*) FROM public.novu_rust_smtp_auto_retries WHERE job_id='%s'"%second)=='1','second replay was not reserved')
    until(lambda:sql("SELECT count(*) FROM public.novu_rust_smtp_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id WHERE a.job_id='%s' AND a.state='unknown' AND j.status='failed'"%second)=='1','second replay did not remain unknown')
    sql("UPDATE public.novu_rust_smtp_attempts SET updated_at=clock_timestamp()-interval '20 seconds' WHERE job_id='%s'"%second)
    time.sleep(3)
    assert sql("SELECT count(*) FROM public.novu_rust_smtp_auto_retries WHERE job_id='%s'"%second)=='1'
    assert health()==(2,{'enabled':True,'eligible':0,'overdue':0,'exhausted':1})
    history=subprocess.run([psql,runner_url,'-q','-At','-v','ON_ERROR_STOP=1','-c',"SELECT public.novu_rust_smtp_auto_history('smtp-auto-fixture',NULL)"],capture_output=True,text=True,timeout=10)
    assert history.returncode==0
    rows=json.loads(history.stdout)['data'];assert len(rows)==2 and {row['jobId'] for row in rows}=={first,second}
    assert 'example.test' not in history.stdout
    with lock:assert messages.count('successful')==2 and messages.count('repeated')==2, messages
    children[-1].terminate();children[-1].wait(timeout=10)
    status,counts=worker_health();assert status==2 and counts['healthy']==1 and counts['stale']>=1
    sql("DELETE FROM public.novu_rust_smtp_auto_retry_workers WHERE environment_id='smtp-auto-fixture' AND database_principal='smtp_auto_fixture'")
    status,counts=worker_health();assert status==2 and counts['healthy']==0
    denied=subprocess.run([str(pathlib.Path(work)/'delivery-reconcile'),'check-smtp-auto-workers','smtp-auto-other','10','1'],env=runner,capture_output=True,timeout=10)
    assert denied.returncode==1
    print('Explicit SMTP auto retry duplicated a lost reply, respected cutoff and retry limit with two workers.')
finally:
    for p in children:
        if p.poll() is None:p.terminate()
    for p in children:
        try:p.wait(timeout=10)
        except subprocess.TimeoutExpired:p.kill();p.wait()
    server.shutdown();server.server_close();thread.join(timeout=5)
