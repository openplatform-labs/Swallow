import base64,hashlib,json,os,subprocess,threading,time,uuid
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
hash_value=base64.b64encode(hashlib.sha256(b'123:scoped-secret').digest()).decode()
sql("GRANT EXECUTE ON FUNCTION public.novu_rust_telegram_workflow_claim(text,text,uuid),public.novu_rust_telegram_workflow_snapshot(text,text,uuid,uuid,boolean),public.novu_rust_telegram_workflow_prepare(text,text,uuid,uuid,text,boolean),public.novu_rust_telegram_workflow_recover(text,text) TO telegram_worker_scoped; INSERT INTO public.novu_rust_telegram_worker_grants SELECT 'telegram-scope','workflow',rolname,oid FROM pg_roles WHERE rolname='telegram_worker_scoped'; INSERT INTO public.novu_rust_telegram_destinations VALUES('telegram-scope','workflow','%s'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_step_active,chat_content,inbox_metadata) VALUES('telegram-scope','flow',true,true,false,'{{name}}','{\"id\":\"wf\"}'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES('telegram-scope','alice','identity','{}'); INSERT INTO public.novu_rust_telegram_workflow_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,destination_id,chat_id) VALUES('telegram-scope','ep','alice','identity','workflow','-123')"%hash_value)
endpoint,revision=sql("SELECT internal_id,revision FROM public.novu_rust_telegram_workflow_endpoints WHERE environment_id='telegram-scope' AND identifier='ep'").split('|')
def add(text):
 job=str(uuid.uuid4());payload={'destination':'workflow','workflowContext':{'subscriber_id':'alice','subscriber_internal_id':'identity','workflow_identifier':'flow','workflow_internal_id':'wf','content_template':'{{name}}','payload':{'name':text},'context_keys':[],'actor':None,'actor_internal_id':None,'endpoint_id':endpoint,'endpoint_revision':revision}}
 sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES('%s','telegram-scope','experimental_telegram_workflow','%s')"%(job,json.dumps(payload).replace("'","''")));return job
calls=[];received=threading.Event();release=threading.Event()
class Handler(BaseHTTPRequestHandler):
 def do_POST(self):
  payload=json.loads(self.rfile.read(int(self.headers['Content-Length'])));calls.append(payload)
  if payload['text']=='hold':received.set();release.wait(15);return
  body=b'{"ok":true,"result":{"message_id":42,"date":100}}';self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def log_message(self,*args):pass
server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start();children=[]
env={**os.environ,'DATABASE_URL':scoped,'NOVU_TELEGRAM_SCOPED_DB':'true','NOVU_TELEGRAM_WORKFLOW_ENABLED':'true','NOVU_TELEGRAM_ENVIRONMENT_ID':'telegram-scope','NOVU_TELEGRAM_DESTINATION_ID':'workflow','NOVU_TELEGRAM_BOT_TOKEN':'123:scoped-secret','SWALLOW_TEST_TELEGRAM_ENDPOINT':'http://127.0.0.1:%d/sendMessage'%server.server_port}
def start():
 p=subprocess.Popen([worker],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);children.append(p);return p
try:
 assert sql('SELECT * FROM public.novu_rust_telegram_workflow_endpoints',scoped,False)!=0
 job=add('normal');a=start();b=start();wait(lambda:sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'"%job)=='completed');assert len(calls)==1 and calls[0]['chat_id']=='-123'
 for p in (a,b):p.terminate();p.wait(timeout=5)
 job=add('hold');c=start();assert received.wait(15);c.kill();c.wait(timeout=5);release.set();sql("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id='%s'"%job);d=start();wait(lambda:sql("SELECT state FROM public.novu_rust_telegram_attempts WHERE job_id='%s'"%job)=='unknown');time.sleep(2);assert sum(x['text']=='hold' for x in calls)==1
 sql("DELETE FROM public.novu_rust_telegram_worker_grants WHERE environment_id='telegram-scope' AND destination_id='workflow' AND database_principal='telegram_worker_scoped'");assert sql("SELECT * FROM public.novu_rust_telegram_workflow_claim('telegram-scope','workflow',gen_random_uuid())",scoped,False)!=0
 print('Scoped Telegram workflow workers enforced isolation and recovered an interrupted dispatch without resending.')
finally:
 release.set()
 for p in children:
  if p.poll() is None:p.terminate()
  try:p.wait(timeout=5)
  except subprocess.TimeoutExpired:p.kill();p.wait()
 server.shutdown();server.server_close();thread.join()
