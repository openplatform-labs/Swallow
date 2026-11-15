#!/usr/bin/env python3
"""Actual in-app and Resend workers against a local ambiguous-response fixture."""
import http.server
import json
import os
import socket
import subprocess
import threading
import time
import uuid

worker, in_app, psql, database_url = (os.environ[k] for k in ('TEST_RESEND_WORKER','TEST_IN_APP_WORKER','TEST_PSQL','TEST_DATABASE_URL'))
children = []
requests = []
lock = threading.Lock()

def sql(query):
    result = subprocess.run([psql, database_url, '-q', '-v', 'ON_ERROR_STOP=1', '-At', '-c', query], capture_output=True, text=True, timeout=10)
    if result.returncode:
        raise RuntimeError('fixture database operation failed')
    return result.stdout.strip()

def until(check, message, timeout=20):
    end = time.monotonic()+timeout
    while time.monotonic()<end:
        if check(): return
        if any(p.poll() is not None for p in children): raise RuntimeError('fixture worker exited unexpectedly')
        time.sleep(.1)
    raise RuntimeError(message)

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_): pass
    def do_POST(self):
        data=self.rfile.read(int(self.headers['Content-Length']))
        with lock:
            requests.append((self.headers.get('Idempotency-Key'),data))
            n=len(requests)
        if n==1:
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        body=b'{"id":"fixture-accepted"}'
        self.send_response(200);self.send_header('Content-Length',str(len(body)));self.send_header('Connection','close');self.end_headers();self.wfile.write(body)

server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
try:
    env='resend-replay-process'; parent=str(uuid.uuid4())
    sql("INSERT INTO public.novu_rust_environments VALUES('resend-replay-process','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html,email_provider,inbox_metadata) VALUES('resend-replay-process','fixture',true,true,'Hello','<p>Hello</p>','resend','{\"id\":\"64a000000000000000000051\"}'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES('resend-replay-process','alice','64a000000000000000000054','{\"email\":\"alice@example.test\"}');")
    payload=json.dumps({'workflow':'fixture','transactionId':'process-replay','content':'','payload':{},'recipients':[{'subscriberId':'alice'}],'recipientIdentitySnapshot':{'alice':'64a000000000000000000054'},'severityOverride':None},separators=(',',':'))
    sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES ('"+parent+"','resend-replay-process','experimental_static_in_app','"+payload+"'::jsonb)")
    base={**os.environ,'DATABASE_URL':database_url,'NOVU_POSTGRES_ALLOW_INSECURE':'true'}
    in_app_process=subprocess.Popen([in_app],env=base,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);children.append(in_app_process)
    until(lambda:sql("SELECT count(*) FROM public.novu_rust_email_step_outcomes WHERE environment_id='resend-replay-process' AND transaction_id='process-replay' AND email_job_id IS NOT NULL")=='1','in-app worker did not create email job')
    in_app_process.terminate();in_app_process.wait(timeout=10);children.remove(in_app_process)
    job=sql("SELECT email_job_id FROM public.novu_rust_email_step_outcomes WHERE environment_id='resend-replay-process' AND transaction_id='process-replay'")
    config={**base,'NOVU_RESEND_ENVIRONMENT_ID':env,'NOVU_RESEND_API_KEY':'fixture-key','NOVU_RESEND_FROM':'sender@example.test','NOVU_RESEND_REPLAY_UNCERTAIN_ONCE':'true','NOVU_RESEND_CORRELATION_TAG':'true','SWALLOW_TEST_RESEND_ENDPOINT':f'http://127.0.0.1:{server.server_port}/emails'}
    for _ in range(2): children.append(subprocess.Popen([worker],env=config,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL))
    until(lambda:sql("SELECT count(*) FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id='"+job+"' AND j.status='completed' AND a.state='accepted' AND a.provider_message_id='fixture-accepted' AND a.same_request_replays=1")=='1','Resend worker did not complete same-request replay')
    time.sleep(2)
    with lock:
        assert len(requests)==2
        assert requests[0][0]==requests[1][0]=='swallow/'+job
        assert requests[0][1]==requests[1][1]
        assert json.loads(requests[0][1])['tags'][0]['name']=='swallow_dispatch'
    print('Two Resend worker processes handled one workflow job; one identical replay recovered the dropped response.')
finally:
    for p in children:
        if p.poll() is None: p.terminate()
    for p in children:
        try: p.wait(timeout=10)
        except subprocess.TimeoutExpired: p.kill();p.wait()
    server.shutdown();server.server_close();thread.join(timeout=5)
