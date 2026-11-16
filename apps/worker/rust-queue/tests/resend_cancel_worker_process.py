import json,os,subprocess,threading,time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
psql=os.environ['TEST_PSQL'];admin=os.environ['DATABASE_URL'];port=os.environ['TEST_PORT'];worker=os.environ['TEST_CANCEL_WORKER']
submit=admin.replace('postgres://postgres@','postgres://cancel_submitter@');worker_db=admin.replace('postgres://postgres@','postgres://cancel_worker@');resolver=admin.replace('postgres://postgres@','postgres://cancel_resolver@')
def sql(query,url=admin,ok=True):
    p=subprocess.run([psql,url,'-qAt','-v','ON_ERROR_STOP=1','-c',query],capture_output=True,text=True,timeout=10)
    if ok and p.returncode:raise RuntimeError(p.stderr)
    return p.stdout.strip() if ok else p.returncode
calls=[];received=threading.Event();release=threading.Event()
class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        calls.append(self.path);assert self.headers.get('Authorization')=='Bearer fixture'
        if self.path=='/emails/fixture-five/cancel':received.set();release.wait(10);return
        assert self.path=='/emails/fixture-four/cancel';data=b'{"object":"email","id":"fixture-four"}'
        self.send_response(200);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    def log_message(self,*args):pass
server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
env={**os.environ,'DATABASE_URL':worker_db,'NOVU_RESEND_ENVIRONMENT_ID':'cancel-env','NOVU_RESEND_API_KEY':'fixture','SWALLOW_TEST_RESEND_ENDPOINT':'http://127.0.0.1:%d/emails'%server.server_port};children=[]
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
    assert json.loads(enqueue('99999999-9999-4999-8999-999999999999',j2))['state']=='pending'
finally:
    release.set()
    for p in children:
        if p.poll() is None:p.terminate()
        try:p.wait(timeout=5)
        except subprocess.TimeoutExpired:p.kill();p.wait()
    server.shutdown();server.server_close();thread.join()
