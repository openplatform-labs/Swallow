#!/usr/bin/env python3
"""Compare Novu's Mongo context predicates to PostgreSQL on disposable containers.

This checks database query semantics, not the original HTTP server, worker,
side effects or SDK. Run locally with Docker and mongo:8/postgres:17-alpine.
"""
import json
import subprocess
import time
import uuid

suffix = uuid.uuid4().hex[:12]
mongo, postgres = 'swallow-context-mongo-' + suffix, 'swallow-context-pg-' + suffix

def docker(*args):
    return subprocess.check_output(['docker', *args], text=True, stderr=subprocess.DEVNULL).strip()

def wait(*args):
    for _ in range(60):
        try:
            return docker(*args)
        except subprocess.CalledProcessError:
            time.sleep(1)
    raise RuntimeError('container did not become ready')

def mongosh(script):
    return json.loads(docker('exec', mongo, 'mongosh', '--quiet', '--eval', script))

def sql(script):
    return json.loads(docker('exec', postgres, 'psql', '-U', 'postgres', '-d', 'postgres', '-At', '-v', 'ON_ERROR_STOP=1', '-c', script).splitlines()[-1])

try:
    docker('run', '--rm', '-d', '--name', mongo, 'mongo:8', '--quiet')
    docker('run', '--rm', '-d', '--name', postgres, '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', 'postgres:17-alpine')
    wait('exec', mongo, 'mongosh', '--quiet', '--eval', 'db.adminCommand({ping:1})')
    wait('exec', postgres, 'pg_isready', '-q', '-U', 'postgres')
    mongosh('''const c=db.getSiblingDB('context_compare').messages; c.insertMany([
      {name:'legacy',env:'a'}, {name:'empty',env:'a',contextKeys:[]},
      {name:'acme',env:'a',contextKeys:['tenant:acme']},
      {name:'multi',env:'a',contextKeys:['app:jira','tenant:acme']},
      {name:'other',env:'a',contextKeys:['tenant:other']},
      {name:'foreign',env:'b',contextKeys:['tenant:acme']}]); print(JSON.stringify(true));''')
    docker('exec', postgres, 'psql', '-U', 'postgres', '-v', 'ON_ERROR_STOP=1', '-c', """CREATE TABLE messages(name text, env text, context_keys text[] NOT NULL); INSERT INTO messages VALUES
      ('legacy','a',ARRAY[]::text[]), ('empty','a',ARRAY[]::text[]),
      ('acme','a',ARRAY['tenant:acme']), ('multi','a',ARRAY['app:jira','tenant:acme']),
      ('other','a',ARRAY['tenant:other']), ('foreign','b',ARRAY['tenant:acme']);""")
    for keys in ([], ['tenant:acme'], ['app:jira', 'tenant:acme'], ['tenant:missing']):
        encoded = json.dumps(keys)
        array = "ARRAY[" + ','.join("'" + k + "'" for k in keys) + "]::text[]"
        for operation in ('exact', 'bulk'):
            # Matches BaseRepository.buildContextExactMatchQuery and the
            # MessageRepository bulk $in conditional, respectively.
            predicate = "(keys.length ? {contextKeys:{$all:keys,$size:keys.length}} : {$or:[{contextKeys:{$exists:false}},{contextKeys:[]}]})" if operation == 'exact' else "(keys.length ? {contextKeys:{$in:keys}} : {})"
            js = f"const keys={encoded}; const q=Object.assign({{env:'a'}},{predicate}); print(JSON.stringify(db.getSiblingDB('context_compare').messages.find(q).sort({{name:1}}).toArray().map(x=>x.name)));"
            pg = f"cardinality(context_keys)=cardinality({array}) AND context_keys @> {array}" if operation == 'exact' else f"cardinality({array})=0 OR context_keys && {array}"
            actual = sql(f"SELECT COALESCE(json_agg(name ORDER BY name),'[]'::json) FROM messages WHERE env='a' AND ({pg});")
            expected = mongosh(js)
            assert expected == actual, (operation, keys, expected, actual)
            print(f'{operation} {encoded}: {actual}')
finally:
    for name in (mongo, postgres):
        subprocess.run(['docker', 'rm', '-f', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
