"""A scoped workflow dispatch across a disposable synchronous PostgreSQL promotion."""
import base64
import hashlib
import json
import os
import subprocess
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

psql = os.environ['TEST_PSQL']
primary = os.environ['TEST_PRIMARY_URL']
standby = os.environ['TEST_STANDBY_URL']
cluster = os.environ['TEST_FAILOVER_URL'].replace('postgres://postgres@', 'postgres://telegram_ha_worker@', 1)
worker = os.environ['TEST_TELEGRAM_WORKER']


def sql(query, url=primary):
    result = subprocess.run([psql, url, '-qAt', '-v', 'ON_ERROR_STOP=1', '-c', query],
                            capture_output=True, text=True, timeout=10)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout.strip()


def wait(check, seconds=25):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(.1)
    raise AssertionError('timed out waiting for failover condition')


token_hash = base64.b64encode(hashlib.sha256(b'123:ha-secret').digest()).decode()
sql("""
CREATE ROLE telegram_ha_worker LOGIN;
GRANT USAGE ON SCHEMA public TO telegram_ha_worker;
GRANT EXECUTE ON FUNCTION public.novu_rust_telegram_worker_register(text,text,text),
 public.novu_rust_telegram_worker_finish(text,text,uuid,uuid,text,integer,text,text),
 public.novu_rust_telegram_workflow_claim(text,text,uuid),
 public.novu_rust_telegram_workflow_snapshot(text,text,uuid,uuid,boolean),
 public.novu_rust_telegram_workflow_prepare(text,text,uuid,uuid,text,boolean),
 public.novu_rust_telegram_workflow_recover(text,text) TO telegram_ha_worker;
INSERT INTO public.novu_rust_environments VALUES('telegram-ha','org');
INSERT INTO public.novu_rust_telegram_worker_grants
 SELECT 'telegram-ha','workflow',rolname,oid FROM pg_roles WHERE rolname='telegram_ha_worker';
INSERT INTO public.novu_rust_telegram_destinations VALUES('telegram-ha','workflow','%s');
INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_step_active,chat_content,inbox_metadata)
 VALUES('telegram-ha','flow',true,true,false,'{{name}}','{"id":"wf"}');
INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile)
 VALUES('telegram-ha','alice','identity','{}');
INSERT INTO public.novu_rust_telegram_workflow_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,destination_id,chat_id)
 VALUES('telegram-ha','ep','alice','identity','workflow','-123');
""" % token_hash)
endpoint, revision = sql("SELECT internal_id,revision FROM public.novu_rust_telegram_workflow_endpoints WHERE environment_id='telegram-ha' AND identifier='ep'").split('|')


def enqueue(text, url):
    job = str(uuid.uuid4())
    payload = {'destination': 'workflow', 'workflowContext': {
        'subscriber_id': 'alice', 'subscriber_internal_id': 'identity',
        'workflow_identifier': 'flow', 'workflow_internal_id': 'wf',
        'content_template': '{{name}}', 'payload': {'name': text}, 'context_keys': [],
        'actor': None, 'actor_internal_id': None, 'endpoint_id': endpoint, 'endpoint_revision': revision}}
    sql("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES('%s','telegram-ha','experimental_telegram_workflow','%s')" % (job, json.dumps(payload).replace("'", "''")), url)
    return job


calls = []
received = threading.Event()
release = threading.Event()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        calls.append(payload)
        if payload['text'] == 'inflight':
            received.set()
            release.wait(30)
        body = b'{"ok":true,"result":{"message_id":42,"date":100}}'
        try:
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *args):
        pass


server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
children = []
try:
    job = enqueue('inflight', primary)
    for name in ('telegram_ha_1', 'telegram_ha_2'):
        env = {**os.environ, 'DATABASE_URL': cluster + '&application_name=' + name,
               'NOVU_TELEGRAM_SCOPED_DB': 'true', 'NOVU_TELEGRAM_WORKFLOW_ENABLED': 'true',
               'NOVU_TELEGRAM_ENVIRONMENT_ID': 'telegram-ha', 'NOVU_TELEGRAM_DESTINATION_ID': 'workflow',
               'NOVU_TELEGRAM_BOT_TOKEN': '123:ha-secret',
               'SWALLOW_TEST_TELEGRAM_ENDPOINT': 'http://127.0.0.1:%d/sendMessage' % server.server_port}
        children.append(subprocess.Popen([worker], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    assert received.wait(20), 'worker did not dispatch'
    state = "SELECT count(*) FROM public.novu_queue_jobs j JOIN public.novu_rust_telegram_attempts a ON a.job_id=j.id WHERE j.id='%s' AND j.status='running' AND a.state='dispatching'" % job
    wait(lambda: sql(state, standby) == '1')
    pgctl = os.environ['TEST_LOCAL_PG_CTL']
    subprocess.run([pgctl, '-D', os.environ['TEST_LOCAL_PRIMARY_DATA'], '-m', 'immediate', 'stop', '-w'], check=True, stdout=subprocess.DEVNULL)
    subprocess.run([pgctl, '-D', os.environ['TEST_LOCAL_STANDBY_DATA'], 'promote', '-w'], check=True, stdout=subprocess.DEVNULL)
    release.set()
    sql("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id='%s'" % job, standby)
    wait(lambda: sql("SELECT count(*) FROM public.novu_queue_jobs j JOIN public.novu_rust_telegram_attempts a ON a.job_id=j.id WHERE j.id='%s' AND j.status='failed' AND a.state='unknown'" % job, standby) == '1')
    wait(lambda: sql("SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('telegram_ha_1','telegram_ha_2')", standby) == '2')
    next_job = enqueue('after-promotion', standby)
    wait(lambda: sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'" % next_job, standby) == 'completed')
    time.sleep(2)
    assert [c['text'] for c in calls].count('inflight') == 1, calls
    assert [c['text'] for c in calls].count('after-promotion') == 1, calls
    assert all(p.poll() is None for p in children), 'worker exited during failover'
    print('Telegram scoped workflow workers recovered an unknown in-flight dispatch without resend and processed a new job after synchronous promotion.')
finally:
    release.set()
    for child in children:
        if child.poll() is None:
            child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
    server.shutdown()
    server.server_close()
    thread.join()
