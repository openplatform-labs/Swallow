import json,os,subprocess,threading,time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
psql=os.environ['TEST_PSQL'];admin=os.environ['DATABASE_URL'];worker=os.environ['TEST_TELEGRAM_WORKER'];scoped=admin.replace('postgres://postgres@','postgres://telegram_worker_scoped@',1)
def sql(q,url=admin,ok=True):
 p=subprocess.run([psql,url,'-qAt','-v','ON_ERROR_STOP=1','-c',q],capture_output=True,text=True,timeout=10)
 if ok and p.returncode:raise RuntimeError(p.stderr)
 return p.stdout.strip() if ok else p.returncode
def wait(check):
 deadline=time.monotonic()+18
 while time.monotonic()<deadline:
  if check():return
  time.sleep(.1)
 raise AssertionError('timed out')
sql("CREATE ROLE telegram_worker_scoped LOGIN; GRANT USAGE ON SCHEMA public TO telegram_worker_scoped; GRANT EXECUTE ON FUNCTION public.novu_rust_telegram_worker_register(text,text,text),public.novu_rust_telegram_worker_claim(text,text,uuid),public.novu_rust_telegram_worker_prepare(text,text,uuid,uuid,boolean),public.novu_rust_telegram_worker_finish(text,text,uuid,uuid,text,integer,text,text),public.novu_rust_telegram_worker_recover(text,text) TO telegram_worker_scoped; INSERT INTO public.novu_rust_environments VALUES('telegram-scope','org'),('telegram-scope-other','org'); INSERT INTO public.novu_rust_telegram_worker_grants SELECT 'telegram-scope','ops',rolname,oid FROM pg_roles WHERE rolname='telegram_worker_scoped'")
assert sql('SELECT * FROM public.novu_queue_jobs',scoped,False)!=0
assert sql('SELECT * FROM public.novu_rust_telegram_attempts',scoped,False)!=0
assert sql("SELECT * FROM public.novu_rust_telegram_worker_claim('telegram-scope','other',gen_random_uuid())",scoped,False)!=0
assert sql("SELECT * FROM public.novu_rust_telegram_worker_claim('telegram-scope-other','ops',gen_random_uuid())",scoped,False)!=0
assert sql("SELECT public.novu_rust_telegram_worker_finish('telegram-scope','ops',gen_random_uuid(),gen_random_uuid(),'rejected',409,NULL,NULL)",scoped,False)!=0
calls=[];received=threading.Event();release=threading.Event()
class Handler(BaseHTTPRequestHandler):
 def do_POST(self):
  payload=json.loads(self.rfile.read(int(self.headers['Content-Length'])));calls.append(payload)
  if payload['text']=='hold':received.set();release.wait(15);return
  if payload['text']=='conflict':self.send_response(409);body=b'conflict'
  elif payload['text']=='rate':self.send_response(429);body=b'rate limit'
  else:self.send_response(200);body=b'{"ok":true,"result":{"message_id":42,"date":100}}'
  self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def log_message(self,*args):pass
server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start();children=[]
env={**os.environ,'DATABASE_URL':scoped,'NOVU_TELEGRAM_SCOPED_DB':'true','NOVU_TELEGRAM_ENVIRONMENT_ID':'telegram-scope','NOVU_TELEGRAM_DESTINATION_ID':'ops','NOVU_TELEGRAM_BOT_TOKEN':'123:scoped-secret','SWALLOW_TEST_TELEGRAM_ENDPOINT':'http://127.0.0.1:%d/sendMessage'%server.server_port}
def start(settings=env):
 p=subprocess.Popen([worker],env=settings,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);children.append(p);return p
def add(job,text,dest='ops',env_id='telegram-scope'):
 payload=json.dumps({'destination':dest,'chatId':'123','text':text}).replace("'","''")
 sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES('%s','%s','experimental_telegram','%s','pending')"%(job,env_id,payload))
try:
 one='11111111-1111-4111-8111-111111111112';other='11111111-1111-4111-8111-111111111113';add(one,'first');add(other,'other','other')
 a=start();b=start();wait(lambda:sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'"%one)=='completed')
 assert len(calls)==1 and calls[0]['text']=='first' and sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'"%other)=='pending'
 conflict='55555555-5555-4555-8555-555555555556';rate='66666666-6666-4666-8666-666666666667';add(conflict,'conflict');add(rate,'rate')
 wait(lambda:sql("SELECT count(*) FROM public.novu_rust_telegram_attempts WHERE job_id IN ('%s','%s') AND state IN ('unknown','rejected')"%(conflict,rate))=='2')
 assert sql("SELECT state FROM public.novu_rust_telegram_attempts WHERE job_id='%s'"%conflict)=='unknown' and sql("SELECT state FROM public.novu_rust_telegram_attempts WHERE job_id='%s'"%rate)=='rejected'

 bad=start({**env,'NOVU_TELEGRAM_BOT_TOKEN':'123:another-secret'});bad.wait(timeout=8);assert bad.returncode!=0 and b'another-secret' not in bad.stderr.read()
 sql("DELETE FROM public.novu_rust_telegram_worker_grants WHERE environment_id='telegram-scope' AND destination_id='ops' AND database_principal='telegram_worker_scoped'")
 two='22222222-2222-4222-8222-222222222223';add(two,'after-revoke');time.sleep(3)
 assert sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'"%two)=='pending' and len(calls)==3
 sql("INSERT INTO public.novu_rust_telegram_worker_grants SELECT 'telegram-scope','ops',rolname,oid FROM pg_roles WHERE rolname='telegram_worker_scoped'")
 wait(lambda:sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'"%two)=='completed');assert len(calls)==4
 invalid='44444444-4444-4444-8444-444444444445';sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES('%s','telegram-scope','experimental_telegram','{\"destination\":\"ops\"}','pending')"%invalid)
 wait(lambda:sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'"%invalid)=='failed');assert sql("SELECT count(*) FROM public.novu_rust_telegram_attempts WHERE job_id='%s'"%invalid)=='0' and len(calls)==4
 assert sql("SELECT public.novu_rust_telegram_worker_prepare('telegram-scope','ops','%s',gen_random_uuid(),true)"%one,scoped)=='f'
 for p in (a,b):p.terminate();p.wait(timeout=5)
 third='33333333-3333-4333-8333-333333333334';add(third,'hold');c=start();assert received.wait(15);c.kill();c.wait(timeout=5);release.set()
 sql("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id='%s'"%third);d=start()
 wait(lambda:sql("SELECT state FROM public.novu_rust_telegram_attempts WHERE job_id='%s'"%third)=='unknown');time.sleep(2)
 assert sum(1 for x in calls if x['text']=='hold')==1
 print('Scoped Telegram workers enforced grants, revocation, binding and crash recovery.')
finally:
 release.set()
 for p in children:
  if p.poll() is None:p.terminate()
  try:p.wait(timeout=5)
  except subprocess.TimeoutExpired:p.kill();p.wait()
 server.shutdown();server.server_close();thread.join()
