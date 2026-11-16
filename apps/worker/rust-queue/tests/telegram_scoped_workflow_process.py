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
def start(settings=env):
 p=subprocess.Popen([worker],env=settings,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);children.append(p);return p
try:
 assert sql('SELECT * FROM public.novu_rust_telegram_workflow_endpoints',scoped,False)!=0
 job=add('normal');a=start();b=start();wait(lambda:sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'"%job)=='completed');assert len(calls)==1 and calls[0]['chat_id']=='-123'
 for p in (a,b):p.terminate();p.wait(timeout=5)
 job=add('hold');c=start();assert received.wait(15);c.kill();c.wait(timeout=5);release.set();sql("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id='%s'"%job);d=start();wait(lambda:sql("SELECT state FROM public.novu_rust_telegram_attempts WHERE job_id='%s'"%job)=='unknown');time.sleep(2);assert sum(x['text']=='hold' for x in calls)==1
 d.terminate();d.wait(timeout=5)
 # A preference change begun before reservation must commit before the worker reads it.
 # Run both restricted and direct worker paths, starting with no preference row.
 for scoped_mode in (True,False):
  sql("DELETE FROM public.novu_rust_workflow_preferences WHERE environment_id='telegram-scope' AND workflow_identifier='flow'")
  marker='blocked-scoped' if scoped_mode else 'blocked-direct';job=add(marker)
  writer=subprocess.Popen([psql,admin,'-qAt','-v','ON_ERROR_STOP=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
  try:
   writer.stdin.write("BEGIN; INSERT INTO public.novu_rust_workflow_preferences(environment_id,workflow_identifier,chat_enabled) VALUES('telegram-scope','flow',false); SELECT 'ready';\n");writer.stdin.flush()
   assert writer.stdout.readline().strip()=='ready'
   settings={**env,'DATABASE_URL':(scoped if scoped_mode else admin)+'?application_name=telegram_pref_fence','NOVU_TELEGRAM_SCOPED_DB':'true' if scoped_mode else 'false'}
   fenced=start(settings)
   wait(lambda:sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='telegram_pref_fence' AND wait_event_type='Lock'")=='1')
   assert not any(x['text']==marker for x in calls)
   writer.stdin.write('COMMIT;\n\\q\n');writer.stdin.flush();writer.wait(timeout=10);assert writer.returncode==0
   wait(lambda:sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'"%job)=='cancelled')
   assert sql("SELECT count(*) FROM public.novu_rust_telegram_attempts WHERE job_id='%s'"%job)=='0'
   assert not any(x['text']==marker for x in calls)
   fenced.terminate();fenced.wait(timeout=5)
  finally:
   if writer.poll() is None:writer.terminate();writer.wait(timeout=5)
 # Reservation first: preference writes wait until the transaction has committed the attempt.
 sql("UPDATE public.novu_rust_workflow_preferences SET chat_enabled=true WHERE environment_id='telegram-scope' AND workflow_identifier='flow'")
 job=add('reservation-first');lease=str(uuid.uuid4())
 holder=subprocess.Popen([psql,scoped,'-qAt','-v','ON_ERROR_STOP=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
 changer=None
 try:
  holder.stdin.write("BEGIN; SELECT count(*) FROM public.novu_rust_telegram_workflow_claim('telegram-scope','workflow','%s'); SELECT public.novu_rust_telegram_workflow_snapshot('telegram-scope','workflow','%s','%s',false); SELECT 'ready';\n"%(lease,job,lease));holder.stdin.flush()
  while True:
   line=holder.stdout.readline()
   assert line,'snapshot transaction failed'
   if line.strip()=='ready':break
  changer=subprocess.Popen([psql,admin+'?application_name=telegram_pref_writer','-qAt','-v','ON_ERROR_STOP=1','-c',"UPDATE public.novu_rust_workflow_preferences SET chat_enabled=false WHERE environment_id='telegram-scope' AND workflow_identifier='flow'"],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True)
  wait(lambda:sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='telegram_pref_writer' AND wait_event_type='Lock'")=='1')
  holder.stdin.write("SELECT public.novu_rust_telegram_workflow_prepare('telegram-scope','workflow','%s','%s',NULL,false); COMMIT;\n\\q\n"%(job,lease));holder.stdin.flush();holder.wait(timeout=10);assert holder.returncode==0
  changer.wait(timeout=10);assert changer.returncode==0
  assert sql("SELECT count(*) FROM public.novu_rust_telegram_attempts WHERE job_id='%s' AND state='dispatching'"%job)=='1'
 finally:
  for process in (holder,changer):
   if process is not None and process.poll() is None:process.terminate();process.wait(timeout=5)
 # The trigger owner supplies the parent lock without granting table access to writers.
 sql("CREATE ROLE telegram_pref_writer LOGIN; GRANT USAGE ON SCHEMA public TO telegram_pref_writer; GRANT SELECT,INSERT,UPDATE,DELETE ON public.novu_rust_workflow_preferences TO telegram_pref_writer")
 limited=admin.replace('postgres://postgres@','postgres://telegram_pref_writer@',1)
 assert sql('SELECT * FROM public.novu_rust_workflows',limited,False)!=0
 sql("UPDATE public.novu_rust_workflow_preferences SET chat_enabled=true WHERE environment_id='telegram-scope' AND workflow_identifier='flow'",limited)
 sql("BEGIN; DELETE FROM public.novu_rust_workflows WHERE environment_id='telegram-scope' AND identifier='flow'; ROLLBACK")
 sql("DELETE FROM public.novu_rust_telegram_worker_grants WHERE environment_id='telegram-scope' AND destination_id='workflow' AND database_principal='telegram_worker_scoped'");assert sql("SELECT * FROM public.novu_rust_telegram_workflow_claim('telegram-scope','workflow',gen_random_uuid())",scoped,False)!=0
 print('Telegram workflow workers enforced isolation, recovered interrupted dispatch and fenced concurrent workflow preference changes.')
finally:
 release.set()
 for p in children:
  if p.poll() is None:p.terminate()
  try:p.wait(timeout=5)
  except subprocess.TimeoutExpired:p.kill();p.wait()
 server.shutdown();server.server_close();thread.join()
