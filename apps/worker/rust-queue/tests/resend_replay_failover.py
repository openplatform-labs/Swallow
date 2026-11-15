#!/usr/bin/env python3
"""Pause a real test-feature Resend worker after replay reservation and promote PostgreSQL."""
import http.server
import json
import os
import pathlib
import signal
import socket
import subprocess
import threading
import time
import uuid

worker=os.environ['TEST_RESEND_WORKER'];psql=os.environ['TEST_PSQL'];fixture=pathlib.Path(os.environ['TEST_FIXTURE_DIR'])
primary=os.environ['TEST_PRIMARY_URL'];standby=os.environ['TEST_STANDBY_URL'];cluster=os.environ['TEST_FAILOVER_URL'];pgctl=os.environ['TEST_LOCAL_PG_CTL']
children=[];requests=[];lock=threading.Lock();paused=None

def sql(url,query):
    result=subprocess.run([psql,url,'-q','-At','-v','ON_ERROR_STOP=1','-c',query],capture_output=True,text=True,timeout=10)
    if result.returncode: raise RuntimeError('fixture database operation failed')
    return result.stdout.strip()
def until(check,message,timeout=25):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        if check():return
        if any(p.poll() is not None for p in children):raise RuntimeError('fixture worker exited unexpectedly')
        time.sleep(.1)
    raise RuntimeError(message)
def control(data,*args):
    result=subprocess.run([pgctl,'-D',data,*args,'-w'],capture_output=True,timeout=20)
    if result.returncode:raise RuntimeError('fixture PostgreSQL control failed')
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self,*_):pass
    def do_POST(self):
        body=self.rfile.read(int(self.headers['Content-Length']))
        with lock:
            requests.append((self.headers.get('Idempotency-Key'),body))
            number=len(requests)
        if number==1:
            self.connection.shutdown(socket.SHUT_RDWR);self.connection.close();return
        response=b'{"id":"new-job-accepted"}'
        self.send_response(200);self.send_header('Content-Length',str(len(response)));self.send_header('Connection','close');self.end_headers();self.wfile.write(response)
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
try:
    env='resend-replay-ha';job=str(uuid.uuid4());gate=fixture/'replay-gate';gate.mkdir()
    sql(primary,"INSERT INTO public.novu_rust_environments VALUES('resend-replay-ha','org'); INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES ('"+job+"','resend-replay-ha','experimental_resend_email','{\"to\":[\"alice@example.test\"],\"subject\":\"fixture\",\"html\":\"hello\"}');")
    config={**os.environ,'DATABASE_URL':cluster,'NOVU_POSTGRES_ALLOW_INSECURE':'true','NOVU_RESEND_ENVIRONMENT_ID':env,'NOVU_RESEND_API_KEY':'fixture-key','NOVU_RESEND_FROM':'sender@example.test','NOVU_RESEND_REPLAY_UNCERTAIN_ONCE':'true','NOVU_RESEND_CORRELATION_TAG':'true','SWALLOW_TEST_RESEND_ENDPOINT':f'http://127.0.0.1:{server.server_port}/emails','SWALLOW_TEST_RESEND_REPLAY_GATE':str(gate)}
    for _ in range(2):children.append(subprocess.Popen([worker],env=config,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL))
    until(lambda:(gate/'ready').exists(),'replay reservation not reached')
    paused=int((gate/'ready').read_text());assert paused in [p.pid for p in children];os.kill(paused,signal.SIGSTOP)
    until(lambda:sql(standby,"SELECT count(*) FROM public.novu_rust_resend_attempts WHERE job_id='"+job+"' AND state='dispatching' AND same_request_replays=1")=='1','replay reservation was not replicated')
    with lock:assert len(requests)==1 and requests[0][0]=='swallow/'+job
    control(os.environ['TEST_LOCAL_PRIMARY_DATA'],'-m','immediate','stop');control(os.environ['TEST_LOCAL_STANDBY_DATA'],'promote')
    sql(standby,"UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id='"+job+"'")
    until(lambda:sql(standby,"SELECT count(*) FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id='"+job+"' AND j.status='failed' AND a.state='unknown' AND a.same_request_replays=1")=='1','promoted worker did not recover unknown attempt')
    survivor=next(p for p in children if p.pid!=paused);survivor.terminate();survivor.wait(timeout=10);children.remove(survivor)
    (gate/'release').write_text('resume');os.kill(paused,signal.SIGCONT);paused=None
    next_job=str(uuid.uuid4())
    sql(standby,"INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES ('"+next_job+"','resend-replay-ha','experimental_resend_email','{\"to\":[\"alice@example.test\"],\"subject\":\"next\",\"html\":\"hello\"}')")
    until(lambda:sql(standby,"SELECT count(*) FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id='"+next_job+"' AND j.status='completed' AND a.state='accepted'")=='1','resumed worker did not process new job')
    time.sleep(2)
    with lock:
        assert len(requests)==2,'unexpected number of requests after recovery'
        assert requests[0][0]=='swallow/'+job and requests[1][0]=='swallow/'+next_job,'stale worker replayed original job'
    assert sql(standby,"SELECT count(*) FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id='"+job+"' AND j.status='failed' AND a.state='unknown'")=='1'
    print('Promoted database recovered reserved Resend replay as unknown; resumed worker processed a new job without resending the original.')
finally:
    if paused is not None:
        try:os.kill(paused,signal.SIGCONT)
        except ProcessLookupError:pass
    for p in children:
        if p.poll() is None:p.terminate()
    for p in children:
        try:p.wait(timeout=10)
        except subprocess.TimeoutExpired:p.kill();p.wait()
    server.shutdown();server.server_close();thread.join(timeout=5)
