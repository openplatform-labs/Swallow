#!/usr/bin/env python3
"""Compare selected/all notification seen transitions on disposable servers.

Requires NOVU_REFERENCE_URL, NOVU_RUST_URL, NOVU_TEST_API_KEY,
NOVU_REFERENCE_MONGO_CONTAINER and NOVU_RUST_PG_CONTAINER. Both servers need
an 'alice' subscriber with unseen messages having internal IDs
64a000000000000000000101 and 64a000000000000000000102, and a 'bob'
subscriber with unseen message 64a000000000000000000103. No 'missing'
subscriber may exist. Mutates these fixtures; use only disposable databases.
WebSocket events, webhooks and interaction traces are not compared.
"""
import datetime
import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request

reference = os.environ['NOVU_REFERENCE_URL'].rstrip('/')
rust = os.environ['NOVU_RUST_URL'].rstrip('/')
key = os.environ['NOVU_TEST_API_KEY']
mongo = os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
postgres = os.environ['NOVU_RUST_PG_CONTAINER']
first = '64a000000000000000000101'
second = '64a000000000000000000102'

def request(base, method, path, body=None, auth='valid'):
    headers = {'Content-Type': 'application/json'}
    if auth != 'missing':
        headers['Authorization'] = 'ApiKey ' + (key if auth == 'valid' else 'invalid')
    req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(), headers=headers, method=method)
    try:
        response = urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        status, raw = response.status, response.read()
    result = json.loads(raw) if raw else None
    if status >= 400:
        stamp = result.pop('timestamp')
        assert stamp.endswith('Z')
        datetime.datetime.fromisoformat(stamp.replace('Z', '+00:00'))
    return status, result

cases = [
    ('empty_missing', 'missing', {'notificationIds': []}, 'valid'),
    ('bad_id', 'alice', {'notificationIds': ['bad']}, 'valid'),
    ('not_array', 'alice', {'notificationIds': 'bad'}, 'valid'),
    ('valid_scalar', 'alice', {'notificationIds': first}, 'valid'),
    ('missing_subscriber', 'missing', {}, 'valid'),
    ('missing_auth', 'alice', {}, 'missing'),
    ('invalid_key', 'alice', {}, 'invalid'),
    ('unknown_id', 'alice', {'notificationIds': ['64a000000000000000000199']}, 'valid'),
    ('other_subscriber_id', 'alice', {'notificationIds': ['64a000000000000000000103']}, 'valid'),
    ('selected', 'alice', {'notificationIds': [first.upper()]}, 'valid'),
]
for name, subscriber, body, auth in cases:
    left, right = (request(base, 'POST', '/v2/subscribers/' + subscriber + '/notifications/seen', body, auth) for base in (reference, rust))
    if left != right:
        raise SystemExit(f'{name}: mismatch\n  reference={left!r}\n  rust={right!r}')
    print(f'{name}: match')

def states():
    js = f'''const d=db.getSiblingDB("novu-db"); print(JSON.stringify(d.messages.find({{_id:{{$in:[ObjectId("{first}"),ObjectId("{second}")]}}}}).sort({{_id:1}}).toArray().map(m=>({{seen:m.seen,read:m.read,archived:m.archived,first:!!m.firstSeenDate,last:!!m.lastSeenDate}}))));'''
    sql = f"SELECT json_agg(json_build_object('seen',is_seen,'read',is_read,'archived',is_archived,'first',first_seen_at IS NOT NULL,'last',last_seen_at IS NOT NULL) ORDER BY internal_id)::text FROM novu_rust_in_app_messages WHERE internal_id IN ('{first}','{second}')"
    return (json.loads(subprocess.check_output(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], text=True)),
            json.loads(subprocess.check_output(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-At', '-c', sql], text=True)))

expected = [{'seen': True, 'read': False, 'archived': False, 'first': True, 'last': True},
            {'seen': False, 'read': False, 'archived': False, 'first': False, 'last': False}]
left, right = states()
assert left == right == expected, (left, right)
print('selected state: match')
query = urllib.parse.urlencode({'filters': '[{"seen":false}]'})
left, right = (request(base, 'GET', '/v2/subscribers/bob/notifications/count?' + query) for base in (reference, rust))
assert left == right == (200, {'data': [{'count': 1, 'filter': {'seen': False}}]})
print('other subscriber isolation: match')

query = urllib.parse.urlencode({'filters': '[{"seen":true},{"seen":false}]'})
left, right = (request(base, 'GET', '/v2/subscribers/alice/notifications/count?' + query) for base in (reference, rust))
assert left == right == (200, {'data': [{'count': 1, 'filter': {'seen': True}}, {'count': 1, 'filter': {'seen': False}}]})
for base in (reference, rust):
    assert request(base, 'POST', '/v2/subscribers/alice/notifications/seen', {}) == (204, None)
left, right = states()
assert left == right and all(state['seen'] and not state['read'] for state in left), (left, right)
print('mark all seen: match')
