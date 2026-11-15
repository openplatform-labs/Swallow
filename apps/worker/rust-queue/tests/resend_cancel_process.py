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
    result=run('cancel','cancel-env',req1,job1,'fixture');assert result.returncode==0,(result.stdout,result.stderr)
    assert json.loads(result.stdout)['state']=='confirmed'
    replay=run('cancel','cancel-env',req1,job1,'fixture');assert replay.returncode==0 and json.loads(replay.stdout)['fresh'] is False
    bad=run('cancel','cancel-env',req1,job1,'different');assert bad.returncode!=0
    unknown=run('cancel','cancel-env',req2,job2,'fixture');assert unknown.returncode==2 and json.loads(unknown.stdout)['state']=='unknown'
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
finally:
    server.shutdown();server.server_close();thread.join()
