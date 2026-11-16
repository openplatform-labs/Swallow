import concurrent.futures,json,os,subprocess,threading,time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
psql=os.environ['TEST_PSQL'];db=os.environ['DATABASE_URL'];cli=os.environ['TEST_TELEGRAM_ENQUEUE'];worker=os.environ['TEST_TELEGRAM_WORKER']
def sql(q,url=db,ok=True):
 p=subprocess.run([psql,url,'-qAt','-v','ON_ERROR_STOP=1','-c',q],capture_output=True,text=True,timeout=10)
 if ok and p.returncode:raise RuntimeError(p.stderr)
 return p.stdout.strip() if ok else p.returncode
def wait(check):
 deadline=time.monotonic()+15
 while time.monotonic()<deadline:
  if check():return
  time.sleep(.1)
 raise AssertionError('timed out')
sql("CREATE ROLE telegram_publisher LOGIN;CREATE ROLE telegram_publisher_other LOGIN;CREATE ROLE telegram_enqueue_worker LOGIN;GRANT USAGE ON SCHEMA public TO telegram_publisher,telegram_publisher_other,telegram_enqueue_worker; GRANT EXECUTE ON FUNCTION public.novu_rust_telegram_enqueue(text,uuid,jsonb) TO telegram_publisher,telegram_publisher_other; GRANT EXECUTE ON FUNCTION public.novu_rust_telegram_worker_register(text,text,text),public.novu_rust_telegram_worker_claim(text,text,uuid),public.novu_rust_telegram_worker_prepare(text,text,uuid,uuid,boolean),public.novu_rust_telegram_worker_finish(text,text,uuid,uuid,text,integer,text,text),public.novu_rust_telegram_worker_recover(text,text) TO telegram_enqueue_worker;INSERT INTO public.novu_rust_environments VALUES('telegram-enqueue-env','org'),('telegram-enqueue-other','org');INSERT INTO public.novu_rust_telegram_enqueue_grants SELECT 'telegram-enqueue-env','ops',rolname,oid FROM pg_roles WHERE rolname IN ('telegram_publisher','telegram_publisher_other');INSERT INTO public.novu_rust_telegram_worker_grants SELECT 'telegram-enqueue-env','ops',rolname,oid FROM pg_roles WHERE rolname='telegram_enqueue_worker'")
pub_db=db.replace('postgres://postgres@','postgres://telegram_publisher@',1);other_db=db.replace('postgres://postgres@','postgres://telegram_publisher_other@',1);worker_db=db.replace('postgres://postgres@','postgres://telegram_enqueue_worker@',1)
assert sql('SELECT * FROM public.novu_queue_jobs',pub_db,False)!=0
assert sql('SELECT * FROM public.novu_rust_telegram_enqueue_requests',pub_db,False)!=0
assert sql("SELECT public.novu_rust_telegram_enqueue('telegram-enqueue-env',gen_random_uuid(),'{}'::jsonb)",pub_db,False)!=0
assert sql("SELECT public.novu_rust_telegram_enqueue('telegram-enqueue-env',gen_random_uuid(),jsonb_build_object('destination','ops','chatId',chr(10),'text','x'))",pub_db,False)!=0

config={**os.environ,'DATABASE_URL':pub_db,'NOVU_TELEGRAM_ENQUEUE_SCOPED':'true'}
req='11111111-2222-4333-8444-555555555555';payload={'destination':'ops','chatId':'123','text':'scoped test','passthrough':{'parse_mode':'HTML'}}
def enqueue(body=payload,request=req,env=config,scope='telegram-enqueue-env'):
 return subprocess.run([cli,scope,request],input=json.dumps(body),text=True,capture_output=True,env=env,timeout=10)
assert enqueue(scope='telegram-enqueue-other').returncode!=0
assert enqueue(body={**payload,'destination':'other'}).returncode!=0
assert enqueue(body={**payload,'chatId':'bad\nchat'}).returncode!=0
assert enqueue(body={**payload,'unexpected':'bad'}).returncode!=0
# If the audit row fails, no queue entry or request record can commit.
sql("CREATE FUNCTION public.fail_telegram_enqueue() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER fail_telegram_enqueue BEFORE INSERT ON public.novu_rust_telegram_enqueue_requests FOR EACH ROW EXECUTE FUNCTION public.fail_telegram_enqueue()")
failed=enqueue();assert failed.returncode!=0 and 'fixture' in failed.stderr and sql("SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='telegram-enqueue-env'")=='0'
sql('DROP TRIGGER fail_telegram_enqueue ON public.novu_rust_telegram_enqueue_requests')
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(lambda _:enqueue(),range(2)))
assert all(r.returncode==0 for r in results),[(r.stdout,r.stderr) for r in results]
values=[json.loads(r.stdout) for r in results];assert sorted(v['replayed'] for v in values)==[False,True] and values[0]['jobId']==values[1]['jobId'];job=values[0]['jobId']
assert enqueue(body={**payload,'text':'changed'}).returncode!=0
assert enqueue(env={**config,'DATABASE_URL':other_db}).returncode!=0
assert sql("SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='telegram-enqueue-env'")=='1'
# A scoped worker can consume the accepted job without direct table rights.
calls=[]
class Handler(BaseHTTPRequestHandler):
 def do_POST(self):
  calls.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))));body=b'{"ok":true,"result":{"message_id":42,"date":100}}';self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def log_message(self,*args):pass
server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
worker_env={**os.environ,'DATABASE_URL':worker_db,'NOVU_TELEGRAM_SCOPED_DB':'true','NOVU_TELEGRAM_ENVIRONMENT_ID':'telegram-enqueue-env','NOVU_TELEGRAM_DESTINATION_ID':'ops','NOVU_TELEGRAM_BOT_TOKEN':'123:enqueue-test-secret','SWALLOW_TEST_TELEGRAM_ENDPOINT':'http://127.0.0.1:%d/sendMessage'%server.server_port}
p=subprocess.Popen([worker],env=worker_env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
try:
 wait(lambda:sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'"%job)=='completed')
 assert calls==[{'chat_id':'123','text':'scoped test','parse_mode':'HTML'}]
 assert json.loads(enqueue().stdout)['replayed'] is True
 sql("DELETE FROM public.novu_rust_telegram_enqueue_grants WHERE environment_id='telegram-enqueue-env' AND database_principal='telegram_publisher'")
 assert enqueue().returncode!=0
 print('Scoped Telegram enqueue authorization, replay, rollback and delivery passed.')
finally:
 p.terminate();p.wait(timeout=5);server.shutdown();server.server_close();thread.join()
