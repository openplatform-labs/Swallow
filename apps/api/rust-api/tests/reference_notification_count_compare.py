#!/usr/bin/env python3
"""Compare bounded subscriber notification counts on disposable servers.

Requires NOVU_REFERENCE_URL, NOVU_RUST_URL, NOVU_TEST_API_KEY,
NOVU_REFERENCE_MONGO_CONTAINER, NOVU_REFERENCE_REDIS_CONTAINER,
NOVU_RUST_PG_CONTAINER and NOVU_TEST_ENVIRONMENT_ID. Both servers must have
an 'alice' subscriber with equivalent in-app messages in three states:
(read,seen,archived) = (false,false,false), (true,true,false), (true,true,true).
There must be no 'missing' subscriber. This test inserts 110 synthetic messages
on both sides and flushes the dedicated reference Redis database: NEVER use it
against shared or production services.
"""
import datetime
import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
import uuid

reference = os.environ['NOVU_REFERENCE_URL'].rstrip('/')
rust = os.environ['NOVU_RUST_URL'].rstrip('/')
key = os.environ['NOVU_TEST_API_KEY']
mongo = os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
redis = os.environ['NOVU_REFERENCE_REDIS_CONTAINER']
postgres = os.environ['NOVU_RUST_PG_CONTAINER']
environment = os.environ['NOVU_TEST_ENVIRONMENT_ID']
assert len(environment) == 24 and all(c in '0123456789abcdef' for c in environment)

def request(base, subscriber, raw_filters, auth='valid'):
    headers = {}
    if auth != 'missing':
        headers['Authorization'] = 'ApiKey ' + (key if auth == 'valid' else 'invalid')
    query = '' if raw_filters is None else '?' + urllib.parse.urlencode({'filters': raw_filters})
    req = urllib.request.Request(base + '/v2/subscribers/' + subscriber + '/notifications/count' + query, headers=headers)
    try:
        response = urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        status, result = response.status, json.load(response)
    if status >= 400:
        stamp = result.pop('timestamp')
        assert stamp.endswith('Z')
        datetime.datetime.fromisoformat(stamp.replace('Z', '+00:00'))
    return status, result

cases = [
    ('states', 'alice', '[{},{"read":false},{"read":true},{"seen":false},{"archived":true},{"read":null}]', 'valid'),
    ('empty_filters', 'alice', '[]', 'valid'),
    ('unread_archived', 'alice', '[{"read":false,"archived":true}]', 'valid'),
    ('missing_subscriber', 'missing', '[{}]', 'valid'),
    ('missing_filters', 'alice', None, 'valid'),
    ('invalid_json', 'alice', 'not-json', 'valid'),
    ('missing_auth', 'alice', '[{}]', 'missing'),
    ('invalid_key', 'alice', '[{}]', 'invalid'),
]
for name, subscriber, filters, auth in cases:
    left, right = (request(base, subscriber, filters, auth) for base in (reference, rust))
    if left != right:
        raise SystemExit(f'{name}: mismatch\n  reference={left!r}\n  rust={right!r}')
    print(f'{name}: match')

fixture = uuid.uuid4().hex
# Identifiers below are generated hex or validated environment IDs.
js = f'''const d=db.getSiblingDB("novu-db"),e=ObjectId("{environment}"),s=d.subscribers.findOne({{_environmentId:e,subscriberId:"alice"}}); d.messages.insertMany(Array.from({{length:110}},(_,i)=>({{_environmentId:e,_subscriberId:s._id,channel:"in_app",read:true,seen:true,archived:false,transactionId:"{fixture}-"+i}})));'''
subprocess.check_call(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], stdout=subprocess.DEVNULL)
sql = f"INSERT INTO novu_rust_in_app_messages (id, environment_id, subscriber_id, workflow_identifier, transaction_id, content, payload, is_read, is_seen) SELECT md5('{fixture}' || n)::uuid, '{environment}', 'alice', 'fixture', '{fixture}-' || n, 'x', '{{}}'::jsonb, true, true FROM generate_series(1,110) AS n"
subprocess.check_call(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-v', 'ON_ERROR_STOP=1', '-c', sql], stdout=subprocess.DEVNULL)
subprocess.check_call(['docker', 'exec', redis, 'redis-cli', 'FLUSHALL'], stdout=subprocess.DEVNULL)
left, right = (request(base, 'alice', '[{}, {"read":true}]') for base in (reference, rust))
expected = (200, {'data': [{'count': 100, 'filter': {}}, {'count': 100, 'filter': {'read': True}}]})
assert left == right == expected, (left, right)
print('maximum count: match')
