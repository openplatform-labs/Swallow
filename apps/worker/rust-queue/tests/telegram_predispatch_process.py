"""Restricted operator and CLI exercise against the disposable Telegram fixture."""
import json
import os
import subprocess
import uuid

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
GRANT EXECUTE ON FUNCTION public.novu_rust_telegram_predispatch_list(text,uuid) TO telegram_predispatch_reader;
INSERT INTO public.novu_rust_environments VALUES ('telegram-pre','org'),('telegram-pre-other','org');
INSERT INTO public.novu_rust_telegram_operator_grants
 SELECT 'telegram-pre',rolname,oid,true,false,false FROM pg_roles WHERE rolname='telegram_predispatch_reader';
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
print('Telegram pre-dispatch CLI enforces grants, isolates environments, sanitizes diagnostics and paginates.')
