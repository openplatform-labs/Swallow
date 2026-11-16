"""Live API reconnect and ambiguous pre-dispatch decision during synchronous promotion."""
import base64
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

root = Path(os.environ['TEST_ROOT'])
directory = Path(os.environ['TEST_FIXTURE_DIR'])
primary, standby = os.environ['TEST_PRIMARY_URL'], os.environ['TEST_STANDBY_URL']
psql, pgctl = os.environ['TEST_PSQL'], os.environ['TEST_LOCAL_PG_CTL']
api = blocker = None


def sql(url, query):
    result = subprocess.run([psql, url, '-qAt', '-v', 'ON_ERROR_STOP=1', '-c', query],
                            capture_output=True, text=True, timeout=10, check=True)
    return result.stdout.strip()


def wait(predicate):
    deadline = time.monotonic() + 35
    while time.monotonic() < deadline:
        if api is not None and api.poll() is not None:
            raise RuntimeError('API exited unexpectedly')
        if predicate():
            return
        time.sleep(.2)
    raise AssertionError('failover condition timed out')


def b64(value):
    return base64.urlsafe_b64encode(value).decode().rstrip('=')


def token(subject):
    header = b64(json.dumps({'alg': 'RS256', 'typ': 'at+jwt', 'kid': 'fixture'}, separators=(',', ':')).encode())
    payload = b64(json.dumps({'iss': 'https://issuer.test', 'aud': 'swallow', 'sub': subject,
                              'client_id': 'console', 'iat': int(time.time()) - 1,
                              'exp': int(time.time()) + 900, 'jti': 'telegram-ha-' + subject}, separators=(',', ':')).encode())
    message = (header + '.' + payload).encode()
    key = root / 'apps/api/rust-api/tests/fixtures/operator_jwt/test-only-private.pem'
    signature = subprocess.run(['openssl', 'dgst', '-sha256', '-sign', str(key)], input=message, stdout=subprocess.PIPE, check=True).stdout
    return message.decode() + '.' + b64(signature)


def call(path, jwt, method='GET', body=None, timeout=8):
    request = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(), method=method,
                                     headers={'Authorization': 'Bearer ' + jwt, 'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, None
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return 0, None


def commit_then_drop_response(path, jwt, body):
    """Forward one request to the real API, then drop its successful response."""
    upstream = []

    class Proxy(BaseHTTPRequestHandler):
        def do_POST(self):
            request_body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            upstream.append(call(self.path, jwt, 'POST', request_body))
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Proxy)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    connection = http.client.HTTPConnection('127.0.0.1', server.server_port, timeout=10)
    try:
        connection.request('POST', path, json.dumps(body), {'Content-Type': 'application/json'})
        try:
            connection.getresponse()
        except http.client.RemoteDisconnected:
            pass
        else:
            raise AssertionError('the client unexpectedly received an HTTP response')
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        thread.join()
    assert len(upstream) == 1 and upstream[0][0] == 200 and upstream[0][1]['replayed'] is False, upstream


try:
    sql(primary, """CREATE ROLE telegram_api_ha LOGIN; GRANT USAGE ON SCHEMA public TO telegram_api_ha;
 GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_telegram_predispatch_retry(text,text,text,text,bigint,uuid,uuid,bigint,text),
 public.novu_rust_operator_jwt_telegram_predispatch_request(text,text,text,text,bigint,uuid),
 public.novu_rust_operator_jwt_telegram_predispatch_cancel(text,text,text,text,bigint,uuid,uuid,bigint,text),
 public.novu_rust_operator_jwt_telegram_predispatch_cancel_request(text,text,text,text,bigint,uuid) TO telegram_api_ha;
 INSERT INTO public.novu_rust_environments VALUES('telegram-api-ha','org'),('telegram-api-other','org');
 INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES
 ('https://issuer.test','admin','console','telegram-api-ha'),('https://issuer.test','target','console','telegram-api-ha'),('https://issuer.test','admin','console','telegram-api-other');
 INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='telegram_api_ha';
 INSERT INTO public.novu_rust_operator_jwt_telegram_resolution_grants VALUES('https://issuer.test','admin','console','telegram-api-ha',false,true,false);
 INSERT INTO public.novu_rust_operator_jwt_telegram_predispatch_cancellers VALUES('https://issuer.test','admin','console','telegram-api-ha');
 INSERT INTO public.novu_rust_telegram_operator_grants SELECT 'telegram-api-ha',rolname,oid,false,true,false FROM pg_roles WHERE rolname='telegram_api_ha';
 INSERT INTO public.novu_rust_telegram_predispatch_cancel_grants SELECT 'telegram-api-ha',rolname,oid FROM pg_roles WHERE rolname='telegram_api_ha';""")
    pub = (root / 'apps/api/rust-api/tests/fixtures/operator_jwt/test-only-public.pem').read_text()
    keys = directory / 'telegram-api-keys.json'
    keys.write_text(json.dumps({'issuer': 'https://issuer.test', 'audience': 'swallow', 'keys': [{'kid': 'fixture', 'pem': pub}]}))
    sock = socket.socket(); sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]; sock.close()
    base = 'http://127.0.0.1:%d' % port
    db_url = os.environ['TEST_FAILOVER_URL'].replace('postgres://postgres@', 'postgres://telegram_api_ha@', 1) + '&application_name=telegram_api_ha'
    api = subprocess.Popen([os.environ['TEST_API_BINARY']], env={**os.environ, 'DATABASE_URL': db_url,
        'NOVU_POSTGRES_ALLOW_INSECURE': 'true', 'LISTEN_ADDR': '127.0.0.1:%d' % port,
        'SWALLOW_OPERATOR_JWT_CONFIG_FILE': str(keys), 'SWALLOW_API_HEALTH_ENDPOINTS_ENABLED': 'true'},
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    admin, target = token('admin'), token('target')
    retry_path = '/v1/experimental/operator/telegram-predispatch-retry?environmentId=telegram-api-ha'
    cancel_path = '/v1/experimental/operator/telegram-predispatch-cancel?environmentId=telegram-api-ha'
    retry_id, cancel_id, retry_job, cancel_job, lost_cancel_id, lost_cancel_job = (str(uuid.uuid4()) for _ in range(6))
    sql(primary, "INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error) VALUES ('%s','telegram-api-ha','experimental_telegram','{}','failed','invalid Telegram job'),('%s','telegram-api-ha','experimental_telegram','{}','failed','invalid Telegram job'),('%s','telegram-api-ha','experimental_telegram','{}','failed','invalid Telegram job')" % (retry_job, cancel_job, lost_cancel_job))
    retry = {'requestId': retry_id, 'jobId': retry_job, 'failureRevision': '0', 'reason': 'reviewed'}
    cancel = {'requestId': cancel_id, 'jobId': cancel_job, 'failureRevision': '0', 'reason': 'reviewed'}
    lost_cancel = {'requestId': lost_cancel_id, 'jobId': lost_cancel_job, 'failureRevision': '0', 'reason': 'reviewed'}
    lookup = '/v1/experimental/operator/telegram-predispatch-cancel-request?environmentId=telegram-api-ha&requestId=' + cancel_id
    wait(lambda: call(lookup, admin)[0] == 200)
    assert call(cancel_path, target, 'POST', cancel)[0] == 401
    commit_then_drop_response(retry_path, admin, retry)
    commit_then_drop_response(cancel_path, admin, lost_cancel)
    wait(lambda: sql(standby, "SELECT count(*) FROM public.novu_rust_operator_jwt_telegram_predispatch_requests WHERE request_id='%s'" % retry_id) == '1')
    wait(lambda: sql(standby, "SELECT count(*) FROM public.novu_rust_operator_jwt_telegram_predispatch_cancellations WHERE request_id='%s'" % lost_cancel_id) == '1')
    lock_sql = "BEGIN; SELECT id FROM public.novu_queue_jobs WHERE id='%s' FOR UPDATE; SELECT pg_sleep(90); COMMIT" % cancel_job
    blocker = subprocess.Popen([psql, primary + '?application_name=telegram_api_lock', '-qAt', '-v', 'ON_ERROR_STOP=1', '-c', lock_sql],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait(lambda: sql(primary, "SELECT count(*) FROM pg_stat_activity WHERE application_name='telegram_api_lock' AND wait_event='PgSleep'") == '1')
    result = []
    inflight = threading.Thread(target=lambda: result.append(call(cancel_path, admin, 'POST', cancel, timeout=20)), daemon=True)
    inflight.start()
    wait(lambda: sql(primary, "SELECT count(*) FROM pg_stat_activity WHERE application_name='telegram_api_ha' AND wait_event_type='Lock'") == '1')
    subprocess.run([pgctl, '-D', os.environ['TEST_LOCAL_PRIMARY_DATA'], '-m', 'immediate', 'stop', '-w'], check=True, stdout=subprocess.DEVNULL)
    subprocess.run([pgctl, '-D', os.environ['TEST_LOCAL_STANDBY_DATA'], 'promote', '-w'], check=True, stdout=subprocess.DEVNULL)
    inflight.join(timeout=25)
    assert not inflight.is_alive() and result and result[0][0] != 200, result
    assert blocker.wait(timeout=10) != 0
    assert sql(standby, "SELECT status FROM public.novu_queue_jobs WHERE id='%s'" % cancel_job) == 'failed'
    assert sql(standby, "SELECT count(*) FROM public.novu_rust_operator_jwt_telegram_predispatch_cancellations WHERE request_id='%s'" % cancel_id) == '0'
    wait(lambda: call(lookup, admin)[0] == 200)
    assert call(lookup, admin)[1] == {'found': False}
    status, value = call(retry_path, admin, 'POST', retry)
    assert status == 200 and value['replayed'] is True
    assert sql(standby, "SELECT count(*) FROM public.novu_rust_operator_jwt_telegram_predispatch_requests WHERE request_id='%s'" % retry_id) == '1'
    assert sql(standby, "SELECT count(*) FROM public.novu_rust_telegram_predispatch_retries WHERE job_id='%s'" % retry_job) == '1'
    status, value = call(cancel_path, admin, 'POST', lost_cancel)
    assert status == 200 and value['replayed'] is True
    assert sql(standby, "SELECT status FROM public.novu_queue_jobs WHERE id='%s'" % lost_cancel_job) == 'cancelled'
    assert sql(standby, "SELECT count(*) FROM public.novu_rust_telegram_predispatch_cancellations WHERE job_id='%s'" % lost_cancel_job) == '1'
    status, value = call(cancel_path, admin, 'POST', cancel)
    assert status == 200 and value['replayed'] is False
    status, value = call(cancel_path, admin, 'POST', cancel)
    assert status == 200 and value['replayed'] is True
    assert call(lookup, admin)[1]['jobId'] == cancel_job
    assert call(lookup, target)[0] == 401
    assert sql(standby, "SELECT status FROM public.novu_queue_jobs WHERE id='%s'" % cancel_job) == 'cancelled'
    assert sql(standby, "SELECT count(*) FROM public.novu_rust_operator_jwt_telegram_predispatch_cancellations WHERE request_id='%s'" % cancel_id) == '1'
    assert sql(standby, "SELECT count(*) FROM public.novu_rust_telegram_predispatch_cancellations WHERE job_id='%s'" % cancel_job) == '1'
    print('Telegram API recovered lost responses after committed retry/cancellation and an interrupted cancellation across synchronous promotion.')
finally:
    for child in (blocker, api):
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
