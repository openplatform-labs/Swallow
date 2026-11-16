"""Restricted operator and CLI exercise against the disposable Telegram fixture."""
import json
import os
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

psql = os.environ['TEST_PSQL']
admin = os.environ['DATABASE_URL']
cli = os.environ['TEST_DELIVERY_CLI']
reader = admin.replace('postgres://postgres@', 'postgres://telegram_predispatch_reader@', 1)


def sql(query):
    result = subprocess.run([psql, admin, '-qAt', '-v', 'ON_ERROR_STOP=1', '-c', query],
                            capture_output=True, text=True, timeout=10, check=True)
    return result.stdout.strip()


def call(env, *args, success=True):
    result = subprocess.run([cli, 'list-telegram-predispatch', env, *args],
                            env={**os.environ, 'DATABASE_URL': reader}, capture_output=True,
                            text=True, timeout=10)
    assert (result.returncode == 0) == success, result.stderr
    return json.loads(result.stdout) if success else None


sql("""
CREATE ROLE telegram_predispatch_reader LOGIN;
GRANT USAGE ON SCHEMA public TO telegram_predispatch_reader;
GRANT EXECUTE ON FUNCTION public.novu_rust_telegram_predispatch_list(text,uuid),public.novu_rust_telegram_predispatch_retry(text,uuid,uuid,bigint,text),public.novu_rust_telegram_predispatch_cancel(text,uuid,uuid,bigint,text),public.novu_rust_telegram_health(text) TO telegram_predispatch_reader;
INSERT INTO public.novu_rust_environments VALUES ('telegram-pre','org'),('telegram-pre-other','org');
INSERT INTO public.novu_rust_telegram_operator_grants
 SELECT 'telegram-pre',rolname,oid,true,false,false FROM pg_roles WHERE rolname='telegram_predispatch_reader';
INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid)
 SELECT 'telegram-pre',rolname,'alert_view',oid FROM pg_roles WHERE rolname='telegram_predispatch_reader';
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error)
 SELECT gen_random_uuid(),'telegram-pre','experimental_telegram_workflow','{"secret":"payload-secret"}','failed',
 CASE WHEN i=1 THEN 'Telegram endpoint changed or deleted' ELSE 'secret arbitrary error' END FROM generate_series(1,52) i;
INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error)
 VALUES (gen_random_uuid(),'telegram-pre-other','experimental_telegram','{}','failed','invalid Telegram job');
""")
excluded = str(uuid.uuid4())
sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error) VALUES('%s','telegram-pre','experimental_telegram','{}','failed','invalid Telegram job'); INSERT INTO public.novu_rust_telegram_attempts(job_id,environment_id,state) VALUES('%s','telegram-pre','unknown')" % (excluded, excluded))
first = call('telegram-pre')
assert len(first['data']) == 50 and first['hasMore'] and first['next']
second = call('telegram-pre', first['next'])
assert len(second['data']) == 2 and not second['hasMore'] and second['next'] is None
rows = first['data'] + second['data']
assert len({row['jobId'] for row in rows}) == 52 and excluded not in {row['jobId'] for row in rows}
assert {row['diagnostic'] for row in rows} == {'other', 'Telegram endpoint changed or deleted'}
assert 'secret' not in json.dumps(rows)
call('telegram-pre-other', success=False)
sql("UPDATE public.novu_rust_telegram_operator_grants SET can_view=false WHERE environment_id='telegram-pre' AND database_principal='telegram_predispatch_reader'")
call('telegram-pre', success=False)

def retry(request, job, revision, reason='reviewed', success=True, env='telegram-pre'):
    result = subprocess.run([cli, 'retry-telegram-predispatch', env, request, job, str(revision), reason],
                            env={**os.environ, 'DATABASE_URL': reader}, capture_output=True, text=True, timeout=10)
    assert (result.returncode == 0) == success, result.stderr
    return json.loads(result.stdout) if success else None

job = str(uuid.uuid4())
request = str(uuid.uuid4())
sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error) VALUES('%s','telegram-pre','experimental_telegram','{\"destination\":\"ops\",\"chatId\":\"123\",\"text\":\"retry\"}','failed','invalid Telegram job')" % job)
retry(request, job, 0, success=False)
sql("UPDATE public.novu_rust_telegram_operator_grants SET can_retry=true WHERE environment_id='telegram-pre' AND database_principal='telegram_predispatch_reader'")
retry(str(uuid.uuid4()), job, 1, success=False)
retry(str(uuid.uuid4()), excluded, 0, success=False)
retry(str(uuid.uuid4()), job, 0, success=False, env='telegram-pre-other')
sql("CREATE FUNCTION public.reject_telegram_pre_audit() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'test'; END$$; CREATE TRIGGER reject_telegram_pre_audit BEFORE INSERT ON public.novu_rust_telegram_predispatch_retries FOR EACH ROW EXECUTE FUNCTION public.reject_telegram_pre_audit()")
retry(request, job, 0, success=False)
assert sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'" % job) == 'failed'
sql('DROP TRIGGER reject_telegram_pre_audit ON public.novu_rust_telegram_predispatch_retries')
with ThreadPoolExecutor(max_workers=2) as pool:
    results = list(pool.map(lambda _: retry(request, job, 0), range(2)))
assert sorted(r['replayed'] for r in results) == [False, True]
retry(request, job, 0, reason='changed', success=False)
assert sql("SELECT count(*) FROM public.novu_rust_telegram_predispatch_retries WHERE job_id='%s'" % job) == '1'
sql("UPDATE public.novu_rust_telegram_operator_grants SET can_retry=false WHERE environment_id='telegram-pre' AND database_principal='telegram_predispatch_reader'")
retry(request, job, 0, success=False)

def cancel(request, job, revision, reason='not required', success=True):
    result = subprocess.run([cli, 'cancel-telegram-predispatch', 'telegram-pre', request, job, str(revision), reason],
                            env={**os.environ, 'DATABASE_URL': reader}, capture_output=True, text=True, timeout=10)
    assert (result.returncode == 0) == success, result.stderr
    return json.loads(result.stdout) if success else None

cancel_job = str(uuid.uuid4()); cancel_request = str(uuid.uuid4())
sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error) VALUES('%s','telegram-pre','experimental_telegram','{\"destination\":\"ops\",\"chatId\":\"123\",\"text\":\"cancelled\"}','failed','invalid Telegram job')" % cancel_job)
cancel(cancel_request, cancel_job, 0, success=False)
sql("INSERT INTO public.novu_rust_telegram_predispatch_cancel_grants SELECT 'telegram-pre',rolname,oid FROM pg_roles WHERE rolname='telegram_predispatch_reader'")
cancel(str(uuid.uuid4()), cancel_job, 1, success=False)
cancel(str(uuid.uuid4()), excluded, 0, success=False)
def health():
    result=subprocess.run([psql,reader,'-qAt','-v','ON_ERROR_STOP=1','-c',"SELECT pre_dispatch_failures FROM public.novu_rust_telegram_health('telegram-pre')"],capture_output=True,text=True,timeout=10,check=True)
    return int(result.stdout.strip())
before=health()
sql("CREATE FUNCTION public.reject_telegram_pre_cancel() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'test'; END$$; CREATE TRIGGER reject_telegram_pre_cancel BEFORE INSERT ON public.novu_rust_telegram_predispatch_cancellations FOR EACH ROW EXECUTE FUNCTION public.reject_telegram_pre_cancel()")
cancel(cancel_request, cancel_job, 0, success=False)
assert sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'" % cancel_job)=='failed' and health()==before
sql('DROP TRIGGER reject_telegram_pre_cancel ON public.novu_rust_telegram_predispatch_cancellations')
with ThreadPoolExecutor(max_workers=2) as pool:
    results=list(pool.map(lambda _: cancel(cancel_request,cancel_job,0),range(2)))
assert sorted(r['replayed'] for r in results)==[False,True]
assert sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'" % cancel_job)=='cancelled' and health()==before-1
cancel(cancel_request,cancel_job,0,reason='changed',success=False)
race_job=str(uuid.uuid4())
sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error) VALUES('%s','telegram-pre','experimental_telegram','{\"destination\":\"race\"}','failed','invalid Telegram job'); UPDATE public.novu_rust_telegram_operator_grants SET can_retry=true WHERE environment_id='telegram-pre' AND database_principal='telegram_predispatch_reader'" % race_job)
def race(action):
    return subprocess.run([cli,action,'telegram-pre',str(uuid.uuid4()),race_job,'0','race'],env={**os.environ,'DATABASE_URL':reader},capture_output=True,text=True,timeout=10).returncode
with ThreadPoolExecutor(max_workers=2) as pool:
    codes=list(pool.map(race,['retry-telegram-predispatch','cancel-telegram-predispatch']))
assert sum(code==0 for code in codes)==1,codes
assert sql("SELECT (SELECT count(*) FROM public.novu_rust_telegram_predispatch_cancellations WHERE job_id='%s')+(SELECT count(*) FROM public.novu_rust_telegram_predispatch_retries WHERE job_id='%s')" % (race_job,race_job))=='1'
sql("DELETE FROM public.novu_rust_telegram_predispatch_cancel_grants WHERE environment_id='telegram-pre' AND database_principal='telegram_predispatch_reader'")
cancel(cancel_request,cancel_job,0,success=False)

calls = []
class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        calls.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
        body = b'{"ok":true,"result":{"message_id":42,"date":100}}'
        self.send_response(200); self.send_header('Content-Length', str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
worker = subprocess.Popen([os.environ['TEST_TELEGRAM_WORKER']], env={**os.environ,
    'NOVU_TELEGRAM_ENVIRONMENT_ID': 'telegram-pre', 'NOVU_TELEGRAM_DESTINATION_ID': 'ops',
    'NOVU_TELEGRAM_BOT_TOKEN': '123:pre-secret',
    'SWALLOW_TEST_TELEGRAM_ENDPOINT': 'http://127.0.0.1:%d/sendMessage' % server.server_port},
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'" % job) != 'completed':
        time.sleep(.1)
    assert sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'" % job) == 'completed'
    assert [item['text'] for item in calls] == ['retry']
finally:
    worker.terminate(); worker.wait(timeout=5)
    server.shutdown(); server.server_close(); thread.join()
print('Telegram pre-dispatch CLI isolates lists and enforces retry/cancel permissions, revision, idempotency, rollback, health and worker dispatch.')
