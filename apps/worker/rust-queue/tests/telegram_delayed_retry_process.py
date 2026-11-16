import json, os, subprocess, threading, time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

psql=os.environ['TEST_PSQL']; admin=os.environ['DATABASE_URL']; cli=os.environ['TEST_DELIVERY_CLI']; worker=os.environ['TEST_TELEGRAM_WORKER']
restricted=admin.replace('postgres://postgres@','postgres://telegram_delay_operator@',1)
def sql(q, url=admin, ok=True):
    p=subprocess.run([psql,url,'-qAt','-v','ON_ERROR_STOP=1','-c',q],capture_output=True,text=True,timeout=10)
    if ok and p.returncode: raise RuntimeError(p.stderr)
    return p.stdout.strip() if ok else p.returncode
def run(args,url=restricted):
    return subprocess.run([cli,*args],env={**os.environ,'DATABASE_URL':url},capture_output=True,text=True,timeout=10)
def wait(check):
    deadline=time.monotonic()+15
    while time.monotonic()<deadline:
        if check(): return
        time.sleep(.1)
    raise AssertionError('timed out')
sql("CREATE ROLE telegram_delay_operator LOGIN; GRANT USAGE ON SCHEMA public TO telegram_delay_operator; GRANT EXECUTE ON FUNCTION public.novu_rust_telegram_resolve_after(text,uuid,uuid,uuid,text),public.novu_rust_telegram_resolve(text,uuid,uuid,uuid,text,text),public.novu_rust_telegram_resolution_history(text,uuid,uuid) TO telegram_delay_operator; INSERT INTO public.novu_rust_environments VALUES('telegram-delay','org'),('telegram-delay-other','org'); INSERT INTO public.novu_rust_telegram_operator_grants(environment_id,database_principal,principal_oid,can_retry) SELECT 'telegram-delay',rolname,oid,true FROM pg_roles WHERE rolname='telegram_delay_operator'; INSERT INTO public.novu_rust_telegram_history_viewers SELECT 'telegram-delay',rolname,oid FROM pg_roles WHERE rolname='telegram_delay_operator'")
job='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'; attempt='bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'; request='cccccccc-cccc-4ccc-8ccc-cccccccccccc'
sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES('%s','telegram-delay','experimental_telegram','{\"destination\":\"ops\",\"chatId\":\"123\",\"text\":\"delayed\"}','failed'); INSERT INTO public.novu_rust_telegram_attempts(job_id,environment_id,attempt_id,state,http_status,retry_after_seconds) VALUES('%s','telegram-delay','%s','rejected',429,120)"%(job,job,attempt))
args=['telegram-retry-after','telegram-delay',request,job,attempt,'investigated']
assert run(['telegram-retry-after','telegram-delay-other',request,job,attempt,'investigated']).returncode!=0
sql("ALTER TABLE public.novu_rust_telegram_resolutions ADD CONSTRAINT fixture_reject CHECK (false) NOT VALID")
assert run(args).returncode!=0
assert sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'"%job)=='failed'
assert sql("SELECT count(*) FROM public.novu_rust_telegram_attempts WHERE job_id='%s'"%job)=='1'
sql('ALTER TABLE public.novu_rust_telegram_resolutions DROP CONSTRAINT fixture_reject')
with ThreadPoolExecutor(max_workers=2) as pool:
    results=list(pool.map(lambda _:run(args),range(2)))
assert all(r.returncode==0 for r in results),[r.stderr for r in results]
values=[json.loads(r.stdout) for r in results]
assert sorted(v['replayed'] for v in values)==[False,True]
assert values[0]['retryNotBefore']==values[1]['retryNotBefore']
assert sql("SELECT available_at>clock_timestamp()+interval '60 seconds' FROM public.novu_queue_jobs WHERE id='%s'"%job)=='t'
assert run(['resolve-telegram','telegram-delay',request,job,attempt,'retry','investigated']).returncode!=0
assert run(args[:-1]+['different']).returncode!=0
history=json.loads(run(['telegram-resolution-history','telegram-delay',job]).stdout)['data']
assert len(history)==1 and history[0]['decision']=='retry_after' and history[0]['retryAfterSeconds']==120 and history[0]['retryNotBefore']
calls=[]
class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        calls.append(self.rfile.read(int(self.headers['Content-Length'])))
        body=b'{"ok":true,"result":{"message_id":42,"date":100}}';self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    def log_message(self,*args): pass
server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
env={**os.environ,'NOVU_TELEGRAM_ENVIRONMENT_ID':'telegram-delay','NOVU_TELEGRAM_DESTINATION_ID':'ops','NOVU_TELEGRAM_BOT_TOKEN':'123:test','SWALLOW_TEST_TELEGRAM_ENDPOINT':'http://127.0.0.1:%d/sendMessage'%server.server_port}
process=subprocess.Popen([worker],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
try:
    time.sleep(2);assert not calls and sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'"%job)=='pending'
    sql("UPDATE public.novu_queue_jobs SET available_at=clock_timestamp() WHERE id='%s'"%job)
    wait(lambda:sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'"%job)=='completed')
    assert len(calls)==1
    sql("UPDATE public.novu_rust_telegram_operator_grants SET can_retry=false WHERE environment_id='telegram-delay' AND database_principal='telegram_delay_operator'")
    assert run(args).returncode!=0
finally:
    process.terminate();process.wait(timeout=5);server.shutdown();server.server_close();thread.join()
print('Telegram delayed retry enforces guidance, permissions, idempotency, rollback and scheduled worker dispatch.')
