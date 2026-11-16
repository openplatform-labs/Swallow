"""Committed and in-flight retry/cancel decisions across synchronous promotion."""
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

psql = os.environ['TEST_PSQL']
primary = os.environ['TEST_PRIMARY_URL']
standby = os.environ['TEST_STANDBY_URL']
cluster = os.environ['TEST_FAILOVER_URL']
cli = os.environ['TEST_DELIVERY_CLI']
worker = os.environ['TEST_TELEGRAM_WORKER']


def sql(query, url):
    result = subprocess.run([psql, url, '-qAt', '-v', 'ON_ERROR_STOP=1', '-c', query],
                            capture_output=True, text=True, timeout=10, check=True)
    return result.stdout.strip()


def wait(predicate):
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.1)
    raise AssertionError('failover condition timed out')


def as_role(url, role):
    return url.replace('postgres://postgres@', 'postgres://' + role + '@', 1)


def decide(url, action, request, job):
    result = subprocess.run([cli, action, 'telegram-decision-ha', request, job, '0', 'reviewed'],
                            env={**os.environ, 'DATABASE_URL': as_role(url, 'telegram_ha_operator')},
                            capture_output=True, text=True, timeout=15, check=True)
    return json.loads(result.stdout)


sql("""
CREATE ROLE telegram_ha_operator LOGIN;
CREATE ROLE telegram_ha_delivery LOGIN;
GRANT USAGE ON SCHEMA public TO telegram_ha_operator,telegram_ha_delivery;
GRANT EXECUTE ON FUNCTION public.novu_rust_telegram_predispatch_retry(text,uuid,uuid,bigint,text),
 public.novu_rust_telegram_predispatch_cancel(text,uuid,uuid,bigint,text) TO telegram_ha_operator;
GRANT EXECUTE ON FUNCTION public.novu_rust_telegram_worker_register(text,text,text),
 public.novu_rust_telegram_worker_claim(text,text,uuid),public.novu_rust_telegram_worker_prepare(text,text,uuid,uuid,boolean),
 public.novu_rust_telegram_worker_finish(text,text,uuid,uuid,text,integer,text,text),public.novu_rust_telegram_worker_recover(text,text) TO telegram_ha_delivery;
INSERT INTO public.novu_rust_environments VALUES('telegram-decision-ha','org');
INSERT INTO public.novu_rust_telegram_operator_grants SELECT 'telegram-decision-ha',rolname,oid,false,true,false FROM pg_roles WHERE rolname='telegram_ha_operator';
INSERT INTO public.novu_rust_telegram_predispatch_cancel_grants SELECT 'telegram-decision-ha',rolname,oid FROM pg_roles WHERE rolname='telegram_ha_operator';
INSERT INTO public.novu_rust_telegram_worker_grants SELECT 'telegram-decision-ha','ops',rolname,oid FROM pg_roles WHERE rolname='telegram_ha_delivery';
""", primary)
retry_job, cancel_job = str(uuid.uuid4()), str(uuid.uuid4())
retry_request, cancel_request = str(uuid.uuid4()), str(uuid.uuid4())
inflight_retry, inflight_cancel = str(uuid.uuid4()), str(uuid.uuid4())
inflight_retry_request, inflight_cancel_request = str(uuid.uuid4()), str(uuid.uuid4())
sql("""INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error) VALUES
 ('%s','telegram-decision-ha','experimental_telegram','{"destination":"ops","chatId":"123","text":"retry"}','failed','invalid Telegram job'),
 ('%s','telegram-decision-ha','experimental_telegram','{"destination":"ops","chatId":"123","text":"cancel"}','failed','invalid Telegram job'),
 ('%s','telegram-decision-ha','experimental_telegram','{"destination":"ops","chatId":"123","text":"retry-inflight"}','failed','invalid Telegram job'),
 ('%s','telegram-decision-ha','experimental_telegram','{"destination":"ops","chatId":"123","text":"cancel-inflight"}','failed','invalid Telegram job')""" % (retry_job, cancel_job, inflight_retry, inflight_cancel), primary)
assert not decide(primary, 'retry-telegram-predispatch', retry_request, retry_job)['replayed']
assert not decide(primary, 'cancel-telegram-predispatch', cancel_request, cancel_job)['replayed']
evidence = """SELECT (SELECT count(*) FROM public.novu_queue_jobs WHERE id='%s' AND status='pending')=1 AND
 (SELECT count(*) FROM public.novu_queue_jobs WHERE id='%s' AND status='cancelled')=1 AND
 (SELECT count(*) FROM public.novu_rust_telegram_predispatch_retries WHERE request_id='%s')=1 AND
 (SELECT count(*) FROM public.novu_rust_telegram_predispatch_cancellations WHERE request_id='%s')=1""" % (retry_job, cancel_job, retry_request, cancel_request)
wait(lambda: sql(evidence, standby) == 't')
pending_evidence = """SELECT (SELECT count(*) FROM public.novu_queue_jobs WHERE id IN ('%s','%s') AND status='failed')=2 AND
 (SELECT count(*) FROM public.novu_rust_telegram_predispatch_retries WHERE request_id='%s')=0 AND
 (SELECT count(*) FROM public.novu_rust_telegram_predispatch_cancellations WHERE request_id='%s')=0""" % (inflight_retry, inflight_cancel, inflight_retry_request, inflight_cancel_request)
transaction = """BEGIN;
 SELECT public.novu_rust_telegram_predispatch_retry('telegram-decision-ha','%s','%s',0,'reviewed');
 SELECT public.novu_rust_telegram_predispatch_cancel('telegram-decision-ha','%s','%s',0,'reviewed');
 SELECT pg_sleep(90); COMMIT""" % (inflight_retry_request, inflight_retry, inflight_cancel_request, inflight_cancel)
inflight = subprocess.Popen([psql, as_role(primary, 'telegram_ha_operator') + '?application_name=telegram_inflight_decision',
                             '-qAt', '-v', 'ON_ERROR_STOP=1', '-c', transaction],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
pgctl = os.environ['TEST_LOCAL_PG_CTL']
try:
    wait(lambda: sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='telegram_inflight_decision' AND wait_event='PgSleep'", primary) == '1')
    assert inflight.poll() is None and sql(pending_evidence, primary) == 't' and sql(pending_evidence, standby) == 't'
    subprocess.run([pgctl, '-D', os.environ['TEST_LOCAL_PRIMARY_DATA'], '-m', 'immediate', 'stop', '-w'], check=True, stdout=subprocess.DEVNULL)
    try:
        subprocess.run([pgctl, '-D', os.environ['TEST_LOCAL_STANDBY_DATA'], 'promote', '-w'], check=True, stdout=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        print(Path(os.environ['TEST_LOCAL_STANDBY_DATA']).parent.joinpath('standby.log').read_text()[-4000:])
        raise
    assert inflight.wait(timeout=10) != 0
finally:
    if inflight.poll() is None:
        inflight.terminate()
        inflight.wait(timeout=5)
assert sql(evidence, standby) == 't'
assert sql(pending_evidence, standby) == 't'
assert decide(cluster, 'retry-telegram-predispatch', retry_request, retry_job)['replayed']
assert decide(cluster, 'cancel-telegram-predispatch', cancel_request, cancel_job)['replayed']
assert not decide(cluster, 'retry-telegram-predispatch', inflight_retry_request, inflight_retry)['replayed']
assert not decide(cluster, 'cancel-telegram-predispatch', inflight_cancel_request, inflight_cancel)['replayed']
assert decide(cluster, 'retry-telegram-predispatch', inflight_retry_request, inflight_retry)['replayed']
assert decide(cluster, 'cancel-telegram-predispatch', inflight_cancel_request, inflight_cancel)['replayed']

calls = []
class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        calls.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
        body = b'{"ok":true,"result":{"message_id":42,"date":100}}'
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
children = []
try:
    for name in ('telegram_decision_ha_1', 'telegram_decision_ha_2'):
        env = {**os.environ, 'DATABASE_URL': as_role(cluster, 'telegram_ha_delivery') + '&application_name=' + name,
               'NOVU_TELEGRAM_SCOPED_DB': 'true', 'NOVU_TELEGRAM_ENVIRONMENT_ID': 'telegram-decision-ha',
               'NOVU_TELEGRAM_DESTINATION_ID': 'ops', 'NOVU_TELEGRAM_BOT_TOKEN': '123:ha-secret',
               'SWALLOW_TEST_TELEGRAM_ENDPOINT': 'http://127.0.0.1:%d/sendMessage' % server.server_port}
        children.append(subprocess.Popen([worker], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    wait(lambda: sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'" % retry_job, standby) == 'completed')
    wait(lambda: sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'" % inflight_retry, standby) == 'completed')
    wait(lambda: sql("SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('telegram_decision_ha_1','telegram_decision_ha_2')", standby) == '2')
    time.sleep(2)
    assert sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'" % cancel_job, standby) == 'cancelled'
    assert sql("SELECT status FROM public.novu_queue_jobs WHERE id='%s'" % inflight_cancel, standby) == 'cancelled'
    assert sorted(item['text'] for item in calls) == ['retry', 'retry-inflight'] and all(item['chat_id'] == '123' for item in calls), calls
    assert sql("SELECT count(*) FROM public.novu_rust_telegram_predispatch_retries WHERE request_id='%s'" % retry_request, standby) == '1'
    assert sql("SELECT count(*) FROM public.novu_rust_telegram_predispatch_cancellations WHERE request_id='%s'" % cancel_request, standby) == '1'
    assert sql("SELECT count(*) FROM public.novu_rust_telegram_predispatch_retries WHERE request_id='%s'" % inflight_retry_request, standby) == '1'
    assert sql("SELECT count(*) FROM public.novu_rust_telegram_predispatch_cancellations WHERE request_id='%s'" % inflight_cancel_request, standby) == '1'
    assert all(child.poll() is None for child in children)
    print('Committed decisions survived synchronous promotion; uncommitted decisions rolled back and replayed, and only retried jobs were sent.')
finally:
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
