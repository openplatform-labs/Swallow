import json,os,subprocess,threading,time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
psql=os.environ['TEST_PSQL'];db=os.environ['DATABASE_URL'];worker=os.environ['TEST_TELEGRAM_WORKER']
def sql(q):
 p=subprocess.run([psql,db,'-qAt','-v','ON_ERROR_STOP=1','-c',q],text=True,capture_output=True,timeout=10)
 if p.returncode:raise RuntimeError(p.stderr)
 return p.stdout.strip()
def wait(check):
 deadline=time.monotonic()+15
 while time.monotonic()<deadline:
  if check():return
  time.sleep(.1)
 raise AssertionError('timed out')
calls=[];received=threading.Event();release=threading.Event()
class Handler(BaseHTTPRequestHandler):
 def do_POST(self):
  data=json.loads(self.rfile.read(int(self.headers['Content-Length'])));calls.append(data)
  if data['text']=='hold':received.set();release.wait(15);return
  if data['text']=='conflict':self.send_response(409);body=b'conflict'
  elif data['text']=='rate':self.send_response(429);body=b'{"ok":false,"error_code":429,"parameters":{"retry_after":17}}'
  else:self.send_response(200);body=b'{"ok":true,"result":{"message_id":42,"date":100}}'
  self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def log_message(self,*args):pass
server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start();children=[]
env={**os.environ,'NOVU_TELEGRAM_ENVIRONMENT_ID':'telegram-test','NOVU_TELEGRAM_DESTINATION_ID':'ops','NOVU_TELEGRAM_BOT_TOKEN':'123:fixture-secret','SWALLOW_TEST_TELEGRAM_ENDPOINT':'http://127.0.0.1:%d/sendMessage'%server.server_port}
def start(settings=env):
 p=subprocess.Popen([worker],env=settings,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);children.append(p);return p
try:
 sql("INSERT INTO public.novu_rust_environments VALUES('telegram-test','org'),('telegram-other','org')")
 sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES ('11111111-1111-4111-8111-111111111111','telegram-test','experimental_telegram','{\"destination\":\"ops\",\"chatId\":\"123\",\"text\":\"ok\",\"passthrough\":{\"parse_mode\":\"HTML\"}}','pending'),('22222222-2222-4222-8222-222222222222','telegram-test','experimental_telegram','{\"destination\":\"ops\",\"chatId\":\"123\",\"text\":\"conflict\"}','pending'),('33333333-3333-4333-8333-333333333333','telegram-test','experimental_telegram','{\"destination\":\"other\",\"chatId\":\"123\",\"text\":\"other\"}','pending'),('44444444-4444-4444-8444-444444444444','telegram-other','experimental_telegram','{\"destination\":\"ops\",\"chatId\":\"123\",\"text\":\"other-env\"}','pending')")
 sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES('66666666-6666-4666-8666-666666666666','telegram-test','experimental_telegram','{\"destination\":\"ops\",\"chatId\":\"123\",\"text\":\"rate\"}','pending')")
 a=start();b=start();wait(lambda:sql("SELECT count(*) FROM public.novu_rust_telegram_attempts WHERE state IN ('accepted','unknown','rejected')")=='3')
 assert sql("SELECT state FROM public.novu_rust_telegram_attempts WHERE job_id='11111111-1111-4111-8111-111111111111'")=='accepted'
 assert sql("SELECT state FROM public.novu_rust_telegram_attempts WHERE job_id='22222222-2222-4222-8222-222222222222'")=='unknown'
 assert sql("SELECT state FROM public.novu_rust_telegram_attempts WHERE job_id='66666666-6666-4666-8666-666666666666'")=='rejected'
 assert sql("SELECT retry_after_seconds FROM public.novu_rust_telegram_attempts WHERE job_id='66666666-6666-4666-8666-666666666666'")=='17'
 assert sorted(x['text'] for x in calls)==['conflict','ok','rate'];assert next(x for x in calls if x['text']=='ok')=={'chat_id':'123','text':'ok','parse_mode':'HTML'}
 assert sql("SELECT count(*) FROM public.novu_queue_jobs WHERE id IN ('33333333-3333-4333-8333-333333333333','44444444-4444-4444-8444-444444444444') AND status='pending'")=='2'
 bad=start({**env,'NOVU_TELEGRAM_BOT_TOKEN':'123:different-secret'});bad.wait(timeout=8);assert bad.returncode!=0;assert b'different-secret' not in bad.stderr.read()
 # An expired dispatch attempt is made unknown and never automatically resent.
 sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,lease_token,lease_until) VALUES('55555555-5555-4555-8555-555555555555','telegram-test','experimental_telegram','{\"destination\":\"ops\",\"chatId\":\"123\",\"text\":\"expired\"}','running',gen_random_uuid(),clock_timestamp()-interval '1 second'); INSERT INTO public.novu_rust_telegram_attempts(job_id,environment_id,state) VALUES('55555555-5555-4555-8555-555555555555','telegram-test','dispatching')")
 wait(lambda:sql("SELECT state FROM public.novu_rust_telegram_attempts WHERE job_id='55555555-5555-4555-8555-555555555555'")=='unknown');time.sleep(2)
 assert len(calls)==3 and sql("SELECT status FROM public.novu_queue_jobs WHERE id='55555555-5555-4555-8555-555555555555'")=='failed'
 # Real enqueue CLI accepts JSON on stdin, and the worker consumes the committed job.
 enqueue=subprocess.run([os.environ['TEST_TELEGRAM_ENQUEUE'],'telegram-test'],input=json.dumps({'destination':'ops','chatId':'123','text':'cli'}),capture_output=True,text=True,timeout=8)
 assert enqueue.returncode==0,(enqueue.stdout,enqueue.stderr);job=json.loads(enqueue.stdout)['jobId']
 wait(lambda:sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'"%job)=='completed')
 assert sum(1 for item in calls if item['text']=='cli')==1
 invalid=subprocess.run([os.environ['TEST_TELEGRAM_ENQUEUE'],'telegram-test'],input='{}',capture_output=True,text=True,timeout=8);assert invalid.returncode!=0 and 'invalid Telegram job' in invalid.stderr
 # Kill a real worker after the provider has received the request but before its response.
 for p in (a,b):p.terminate();p.wait(timeout=5)
 sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES('77777777-7777-4777-8777-777777777777','telegram-test','experimental_telegram','{\"destination\":\"ops\",\"chatId\":\"123\",\"text\":\"hold\"}','pending')")
 c=start();assert received.wait(15),'provider did not receive request';c.kill();c.wait(timeout=5);release.set()
 sql("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id='77777777-7777-4777-8777-777777777777'")
 d=start();wait(lambda:sql("SELECT state FROM public.novu_rust_telegram_attempts WHERE job_id='77777777-7777-4777-8777-777777777777'")=='unknown');time.sleep(2)
 assert sum(1 for item in calls if item['text']=='hold')==1


 print('Telegram worker mock delivery, ambiguity, recovery and destination fencing passed.')
finally:
 release.set()
 for p in children:
  if p.poll() is None:p.terminate()
  try:p.wait(timeout=5)
  except subprocess.TimeoutExpired:p.kill();p.wait()
 server.shutdown();server.server_close();thread.join()
