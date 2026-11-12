#!/usr/bin/env python3
"""Compare one disposable, preconfigured in-app workflow end-to-end.

Requires NOVU_REFERENCE_URL, NOVU_RUST_URL, NOVU_TEST_API_KEY,
NOVU_REFERENCE_MONGO_CONTAINER and NOVU_RUST_PG_CONTAINER. Both environments
must have an active 'greeting' workflow rendering 'Hello {{subscriber.firstName}}',
and both worker processes must run. Uses synthetic data and creates one message
on each side; run only against disposable isolated services.
"""
import json
import os
import re
import subprocess
import time
import urllib.parse
import urllib.request
import uuid

reference = os.environ['NOVU_REFERENCE_URL'].rstrip('/')
rust = os.environ['NOVU_RUST_URL'].rstrip('/')
key = os.environ['NOVU_TEST_API_KEY']
mongo = os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
postgres = os.environ['NOVU_RUST_PG_CONTAINER']
transaction = 'compat-' + uuid.uuid4().hex
subscriber = 'compat-' + uuid.uuid4().hex
body = {'name': 'greeting', 'to': {'subscriberId': subscriber, 'firstName': 'Jane'}, 'transactionId': transaction, 'payload': {}}

def trigger(base):
    req = urllib.request.Request(base + '/v1/events/trigger', data=json.dumps(body).encode(), headers={'Content-Type': 'application/json', 'Authorization': 'ApiKey ' + key})
    with urllib.request.urlopen(req, timeout=10) as response:
        return response.status, json.load(response)

expected = (201, {'data': {'acknowledged': True, 'status': 'processed', 'transactionId': transaction}})
assert trigger(reference) == expected, 'reference trigger response differs'
assert trigger(rust) == expected, 'Rust trigger response differs'

# Both identifiers are generated here from UUID hex, so they cannot inject script or SQL.
js = f'''const d=db.getSiblingDB("novu-db"); const rows=d.messages.find({{transactionId:"{transaction}"}}).toArray(); print(JSON.stringify(rows.map(m=>({{id:m._id.toString(),content:m.content,channel:m.channel,transactionId:m.transactionId,subscriberId:d.subscribers.findOne({{_id:m._subscriberId}})?.subscriberId}}))));'''
sql = f'''SELECT COALESCE(json_agg(json_build_object('id',internal_id,'content',content,'channel','in_app','transactionId',transaction_id,'subscriberId',subscriber_id)), '[]'::json)::text FROM novu_rust_in_app_messages WHERE transaction_id='{transaction}';'''
for _ in range(30):
    ref = json.loads(subprocess.check_output(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], text=True))
    actual = json.loads(subprocess.check_output(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-At', '-c', sql], text=True))
    if ref and actual:
        break
    time.sleep(1)
else:
    raise SystemExit(f'timed out waiting for delivery: reference_count={len(ref)} rust_count={len(actual)}')
assert len(ref) == len(actual) == 1, (ref, actual)
reference_id, rust_id = ref[0].pop('id'), actual[0].pop('id')
assert re.fullmatch('[0-9a-f]{24}', reference_id) and re.fullmatch('[0-9a-f]{24}', rust_id)
expected_messages = [{'content': 'Hello Jane', 'channel': 'in_app', 'transactionId': transaction, 'subscriberId': subscriber}]
if ref != expected_messages or actual != expected_messages:
    raise SystemExit(f'message mismatch: reference={ref!r} rust={actual!r}')
filters = [{}, {'read': False}, {'seen': False}, {'archived': False}]
query = urllib.parse.urlencode({'filters': json.dumps(filters, separators=(',', ':'))})
expected_counts = (200, {'data': [{'count': 1, 'filter': item} for item in filters]})
for base in (reference, rust):
    req = urllib.request.Request(base + '/v2/subscribers/' + subscriber + '/notifications/count?' + query,
                                 headers={'Authorization': 'ApiKey ' + key})
    with urllib.request.urlopen(req, timeout=10) as response:
        result = response.status, json.load(response)
    assert result == expected_counts, result
print('Trigger, selected in-app message fields, and notification counts match (one recipient, one workflow).')

def list_notifications(base):
    req = urllib.request.Request(base + '/v2/subscribers/' + subscriber + '/notifications', headers={'Authorization': 'ApiKey ' + key})
    with urllib.request.urlopen(req, timeout=10) as response:
        return response.status, json.load(response)

lists = [list_notifications(base) for base in (reference, rust)]
for result, expected_id in zip(lists, (reference_id, rust_id)):
    assert result[0] == 200 and result[1]['hasMore'] is False and result[1]['filter'] == {}, result
    assert len(result[1]['data']) == 1
    item = result[1]['data'][0]
    assert item.pop('id') == expected_id
    assert re.fullmatch('[0-9a-f]{24}', item['to'].pop('id'))
    assert item.pop('createdAt').endswith('Z')
    assert len(item['deliveredAt']) == 1 and item.pop('deliveredAt')[0].endswith('Z')
assert lists[0] == lists[1], lists
print('Inbox list and generated message ID match, excluding independently generated IDs and timestamps')

for base, message_id in ((reference, reference_id), (rust, rust_id)):
    req = urllib.request.Request(base + '/v2/subscribers/' + subscriber + '/notifications/seen',
                                 data=json.dumps({'notificationIds': [message_id]}).encode(),
                                 headers={'Authorization': 'ApiKey ' + key, 'Content-Type': 'application/json'}, method='POST')
    with urllib.request.urlopen(req, timeout=10) as response:
        assert response.status == 204 and response.read() == b''
    filters = [{'seen': True}, {'read': False}]
    query = urllib.parse.urlencode({'filters': json.dumps(filters, separators=(',', ':'))})
    req = urllib.request.Request(base + '/v2/subscribers/' + subscriber + '/notifications/count?' + query,
                                 headers={'Authorization': 'ApiKey ' + key})
    with urllib.request.urlopen(req, timeout=10) as response:
        assert (response.status, json.load(response)) == (200, {'data': [{'count': 1, 'filter': item} for item in filters]})
print('Selected message marked seen: response and updated counts match')

for action, filters in [
    ('read', [{'read': True}, {'seen': True}, {'archived': False}]),
    ('read-archive', [{'read': True}, {'seen': True}, {'archived': True}]),
    ('archive', [{'read': True}, {'seen': True}, {'archived': True}]),
]:
    for base in (reference, rust):
        req = urllib.request.Request(base + '/v2/subscribers/' + subscriber + '/notifications/' + action,
                                     data=b'{}', headers={'Authorization': 'ApiKey ' + key, 'Content-Type': 'application/json'}, method='POST')
        with urllib.request.urlopen(req, timeout=10) as response:
            assert response.status == 204 and response.read() == b''
        query = urllib.parse.urlencode({'filters': json.dumps(filters, separators=(',', ':'))})
        req = urllib.request.Request(base + '/v2/subscribers/' + subscriber + '/notifications/count?' + query,
                                     headers={'Authorization': 'ApiKey ' + key})
        with urllib.request.urlopen(req, timeout=10) as response:
            assert (response.status, json.load(response)) == (200, {'data': [{'count': 1, 'filter': item} for item in filters]})
    print(f'{action}: response and updated counts match')
