#!/usr/bin/env python3
"""Compare an Inbox listing from real worker deliveries on disposable servers.

Requires NOVU_REFERENCE_URL, NOVU_RUST_URL, NOVU_TEST_API_KEY,
NOVU_REFERENCE_MONGO_CONTAINER and NOVU_RUST_PG_CONTAINER. Both systems need
running workers and equivalent active 'greeting' in-app workflows. Rust must
have the matching inbox_metadata set. Creates a synthetic subscriber and three
messages in each database. It does not validate SDK sessions or Socket.IO.
"""
import json
import base64
import hashlib
import hmac
import os
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

reference = os.environ['NOVU_REFERENCE_URL'].rstrip('/')
rust = os.environ['NOVU_RUST_URL'].rstrip('/')
key = os.environ['NOVU_TEST_API_KEY']
mongo = os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
postgres = os.environ['NOVU_RUST_PG_CONTAINER']
subscriber = 'compat-' + uuid.uuid4().hex
prefix = 'compat-' + uuid.uuid4().hex

def request(base, method, path, body=None, auth=True, auth_value=None):
    headers = {'Content-Type': 'application/json'}
    if auth:
        headers['Authorization'] = auth_value or 'ApiKey ' + key
    req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(), headers=headers, method=method)
    try:
        response = urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        status, raw = response.status, response.read()
    result = json.loads(raw) if raw else None
    if status >= 400:
        assert result.pop('timestamp').endswith('Z')
    return status, result

for i in range(3):
    transaction = f'{prefix}-{i}'
    body = {'name': 'greeting', 'to': {'subscriberId': subscriber, 'firstName': 'Jane'}, 'transactionId': transaction, 'payload': {'sequence': i}}
    expected = (201, {'data': {'acknowledged': True, 'status': 'processed', 'transactionId': transaction}})
    assert request(reference, 'POST', '/v1/events/trigger', body) == expected
    assert request(rust, 'POST', '/v1/events/trigger', body) == expected
    # The generated transaction is hex, hyphens and digits only.
    js = f'print(db.getSiblingDB("novu-db").messages.countDocuments({{transactionId:"{transaction}"}}));'
    sql = f"SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='{transaction}'"
    for _ in range(30):
        counts = (int(subprocess.check_output(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], text=True)),
                  int(subprocess.check_output(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-At', '-c', sql], text=True)))
        if counts == (1, 1):
            break
        time.sleep(1)
    else:
        raise SystemExit(f'timed out waiting for {transaction}: {counts}')

def normalized(result):
    status, body = result
    result = json.loads(json.dumps(body))
    for item in result.get('data', []):
        assert re.fullmatch('[0-9a-f]{24}', item.pop('id'))
        assert re.fullmatch('[0-9a-f]{24}', item['to'].pop('id'))
        for field in ('createdAt', 'readAt', 'firstSeenAt', 'archivedAt'):
            if field in item:
                value = item[field]
                assert value is None or value.endswith('Z')
                if value is not None:
                    item[field] = '<timestamp>'
        if 'deliveredAt' in item:
            assert all(value.endswith('Z') for value in item.pop('deliveredAt'))
    return status, result

def compare(name, query):
    path = '/v2/subscribers/' + subscriber + '/notifications' + query
    left, right = (request(base, 'GET', path) for base in (reference, rust))
    if normalized(left) != normalized(right):
        raise SystemExit(f'{name}: mismatch\n  reference={left!r}\n  rust={right!r}')
    print(f'{name}: match')
    return left, right

first_ref, first_rust = compare('first_page', '?limit=1')
assert first_ref[1]['hasMore'] and first_rust[1]['hasMore']
ref_cursor = first_ref[1]['data'][0]['id']
rust_cursor = first_rust[1]['data'][0]['id']
left = request(reference, 'GET', '/v2/subscribers/' + subscriber + '/notifications?' + urllib.parse.urlencode({'limit': 1, 'after': ref_cursor, 'offset': 99}))
right = request(rust, 'GET', '/v2/subscribers/' + subscriber + '/notifications?' + urllib.parse.urlencode({'limit': 1, 'after': rust_cursor, 'offset': 99}))
assert normalized(left) == normalized(right) and left[1]['hasMore']
print('cursor_page: match')
compare('offset_page', '?limit=2&offset=1')
compare('unread', '?read=false&seen=false&archived=false')

if os.environ.get('NOVU_TEST_JWT_SECRET'):
    secret = os.environ['NOVU_TEST_JWT_SECRET'].encode()
    def token(payload, signing_secret=secret):
        b64 = lambda value: base64.urlsafe_b64encode(value).rstrip(b'=')
        header = b64(json.dumps({'alg': 'HS256', 'typ': 'JWT'}, separators=(',', ':')).encode())
        encoded = b64(json.dumps(payload, separators=(',', ':')).encode())
        signed = header + b'.' + encoded
        return (signed + b'.' + b64(hmac.new(signing_secret, signed, hashlib.sha256).digest())).decode()
    claims = {'_id': '64a000000000000000000001', 'environmentId': '64a000000000000000000002', 'organizationId': '64a000000000000000000003', 'subscriberId': subscriber, 'contextKeys': [], 'aud': 'widget_user', 'iss': 'novu_api', 'exp': 4102444800}
    bearer = 'Bearer ' + token(claims)
    if os.environ.get('NOVU_TEST_APP_IDENTIFIER'):
        status, session = request(reference, 'POST', '/v1/inbox/session', {'applicationIdentifier': os.environ['NOVU_TEST_APP_IDENTIFIER'], 'subscriberId': subscriber}, auth=False)
        assert status == 201 and isinstance(session['data']['token'], str), (status, session)
        bearer = 'Bearer ' + session['data']['token']
        print('reference_session_token: issued')
    for name, query in [('inbox_jwt_list', ''), ('inbox_jwt_snoozed', '?snoozed=false')]:
        left, right = (request(base, 'GET', '/v1/inbox/notifications' + query, auth_value=bearer) for base in (reference, rust))
        assert normalized(left) == normalized(right), (name, left, right)
        print(name + ': match')
    filters = [{'read': False, 'snoozed': False}, {'snoozed': True}]
    query = urllib.parse.urlencode({'filters': json.dumps(filters, separators=(',', ':'))})
    left, right = (request(base, 'GET', '/v1/inbox/notifications/count?' + query, auth_value=bearer) for base in (reference, rust))
    assert left == right, (left, right)
    print('inbox_jwt_count: match')
    for name, credential in [('bad_signature', 'Bearer ' + token(claims, b'wrong')), ('expired', 'Bearer ' + token({**claims, 'exp': 1})), ('wrong_audience', 'Bearer ' + token({**claims, 'aud': 'user'})), ('missing_subscriber', 'Bearer ' + token({**claims, 'subscriberId': 'missing'})), ('api_key', 'ApiKey ' + key)]:
        left, right = (request(base, 'GET', '/v1/inbox/notifications', auth_value=credential) for base in (reference, rust))
        assert left == right, (name, left, right)
        print('inbox_jwt_' + name + ': match')

# Assign matching tags to the worker-produced messages, then compare listing and
# count filters. All interpolated transaction IDs are generated UUID hex.
for i, (tags, data) in enumerate(zip((['news', 'red'], ['news', 'blue'], ['other']), ({'type': 'order', 'details': {'state': 'new'}, 'items': [{'status': 'open'}, {}], 'labels': ['vip', 'red'], 'priority': 2}, {'type': 'order', 'details': {'state': 'done'}, 'items': [{'status': 'done'}], 'priority': 1}, {'type': 'comment', 'items': [], 'priority': 3}))):
    transaction = f'{prefix}-{i}'
    js = f'db.getSiblingDB("novu-db").messages.updateOne({{transactionId:"{transaction}"}},{{$set:{{tags:{json.dumps(tags)},data:{json.dumps(data)}}}}});'
    sql = f"UPDATE novu_rust_in_app_messages SET tags='{json.dumps(tags)}'::jsonb, data='{json.dumps(data)}'::jsonb WHERE transaction_id='{transaction}'"
    subprocess.check_call(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], stdout=subprocess.DEVNULL)
    subprocess.check_call(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-c', sql], stdout=subprocess.DEVNULL)
for name, query, count in [('single_tag', '?tags=red', 1), ('or_tags', '?tags=red&tags=blue', 2), ('no_tag_match', '?tags=missing', 0)]:
    left, right = compare(name, query)
    assert len(left[1]['data']) == len(right[1]['data']) == count
filters = [{'tags': []}, {'tags': ['red', 'blue']}, {'tags': {'or': ['news']}}, {'tags': {'and': [{'or': ['news']}, {'or': ['red', 'other']}]}}, {'tags': {'and': []}}, {'tags': ['missing']}, {'tags': ['red'], 'seen': False}]
query = urllib.parse.urlencode({'filters': json.dumps(filters, separators=(',', ':'))})
path = f'/v2/subscribers/{subscriber}/notifications/count?{query}'
left, right = (request(base, 'GET', path) for base in (reference, rust))
assert left == right, (left, right)
assert [entry['count'] for entry in left[1]['data']] == [3, 2, 2, 1, 3, 0, 1]
print('tag_counts: match')
for name, data, count in [('data_scalar', {'type': 'order'}, 2), ('data_nested_or', {'type': ['order', 'none'], 'details': {'state': 'new'}}, 1), ('data_and', {'priority': {'and': [{'or': [1, 2]}, {'or': [2, 3]}]}}, 1), ('data_array_element', {'labels': 'vip'}, 1), ('data_array_path', {'items': {'status': 'open'}}, 1), ('data_array_path_missing', {'items': {'status': None}}, 1), ('data_null_missing', {'missing': None}, 3), ('data_empty', {}, 3), ('data_not_payload', {'sequence': 0}, 0)]:
    query = '?' + urllib.parse.urlencode({'data': json.dumps(data)})
    left, right = compare(name, query)
    assert len(left[1]['data']) == len(right[1]['data']) == count

def bulk_tagged(action, tags):
    for base in (reference, rust):
        assert request(base, 'POST', f'/v2/subscribers/{subscriber}/notifications/{action}', {'tags': tags}) == (204, None)
    return compare('tagged_' + action, '')[0][1]['data']

items = bulk_tagged('seen', ['red'])
assert [(item['isSeen'], item['isRead']) for item in items if 'red' in item['tags']] == [(True, False)]
assert all(not item['isSeen'] for item in items if 'red' not in item['tags'])
items = bulk_tagged('read', {'and': [{'or': ['news']}, {'or': ['red']}]})
assert [(item['isRead'], item['isArchived']) for item in items if 'red' in item['tags']] == [(True, False)]
assert all(not item['isRead'] for item in items if 'red' not in item['tags'])
items = bulk_tagged('archive', {'or': ['blue']})
assert [(item['isRead'], item['isArchived']) for item in items if 'blue' in item['tags']] == [(True, True)]
assert all(not item['isArchived'] for item in items if 'blue' not in item['tags'])
items = bulk_tagged('read-archive', ['red', 'other'])
assert [(item['isRead'], item['isArchived']) for item in items if 'red' in item['tags']] == [(True, True)]
assert [(item['isRead'], item['isArchived']) for item in items if 'other' in item['tags']] == [(False, False)]

for action, data in [('seen', {'type': 'comment'}), ('read', {'type': 'comment'}), ('archive', {'type': 'comment'})]:
    for base in (reference, rust):
        assert request(base, 'POST', f'/v2/subscribers/{subscriber}/notifications/{action}', {'data': json.dumps(data)}) == (204, None)
    items = compare('data_' + action, '')[0][1]['data']
    selected = next(item for item in items if item['data']['type'] == 'comment')
    assert selected['isSeen'] and (action == 'seen' or selected['isRead'])
    if action == 'archive': assert selected['isArchived']

if os.environ.get('NOVU_TEST_JWT_SECRET'):
    for action in ('read', 'unread', 'archive', 'unarchive'):
        results = []
        for base, message_id in ((reference, ref_cursor), (rust, rust_cursor)):
            status, body = request(base, 'PATCH', f'/v1/inbox/notifications/{message_id}/{action}', {}, auth_value=bearer)
            assert status == 200, (action, status, body)
            results.append(normalized((status, {'data': [body['data']]})))
        assert results[0] == results[1], (action, results)
        compare('inbox_jwt_' + action, '')
    for action in ('seen', 'read', 'archive', 'read-archive'):
        for base in (reference, rust):
            assert request(base, 'POST', '/v1/inbox/notifications/' + action, {'tags': ['blue']}, auth_value=bearer) == (204, None)
        compare('inbox_jwt_bulk_' + action, '')
    for base in (reference, rust):
        status, _ = request(base, 'PATCH', '/v1/inbox/notifications/bad/read', {}, auth_value=bearer)
        assert status == 422
        status, _ = request(base, 'POST', '/v1/inbox/notifications/read', {}, auth_value='ApiKey ' + key)
        assert status == 401

for base, message_id in ((reference, ref_cursor), (rust, rust_cursor)):
    assert request(base, 'POST', '/v2/subscribers/' + subscriber + '/notifications/seen', {'notificationIds': [message_id], 'tags': ['nonmatching'], 'data': 'not-json'}) == (204, None)
compare('seen', '?seen=true')
compare('unseen', '?seen=false')

# Each implementation generates its own message ID. Compare the returned DTOs
# after validating and removing only independent identifiers and timestamps.
for action, read, archived in [('archive', True, True), ('unread', False, False), ('read', True, False), ('unarchive', True, False)]:
    results = []
    for base, message_id in ((reference, ref_cursor), (rust, rust_cursor)):
        status, body = request(base, 'PATCH', f'/v2/subscribers/{subscriber}/notifications/{message_id}/{action}')
        assert status == 200, (action, status, body)
        item = body['data']
        assert (item['isRead'], item['isArchived'], item['isSeen']) == (read, archived, True)
        results.append(normalized((status, {'data': [item]})))
    assert results[0] == results[1], (action, results)
    compare('list_after_' + action, '')
    print(action + ': match')

for name, sub, msg, auth in [('invalid_id', subscriber, 'bad', True), ('missing_subscriber', 'missing', '64a000000000000000000199', True), ('missing_message', subscriber, '64a000000000000000000199', True), ('missing_auth', subscriber, ref_cursor, False)]:
    path = f'/v2/subscribers/{sub}/notifications/{msg}/read'
    left, right = (request(base, 'PATCH', path, auth=auth) for base in (reference, rust))
    assert left == right, (name, left, right)
    print('mark_' + name + ': match')

for base, message_id in ((reference, ref_cursor), (rust, rust_cursor)):
    if os.environ.get('NOVU_TEST_JWT_SECRET'):
        assert request(base, 'DELETE', '/v1/inbox/notifications/' + message_id + '/delete', auth_value=bearer) == (204, None)
    else:
        assert request(base, 'DELETE', '/v2/subscribers/' + subscriber + '/notifications/' + message_id) == (204, None)
left, right = compare('list_after_delete', '')
assert len(left[1]['data']) == len(right[1]['data']) == 2
for base in (reference, rust):
    assert request(base, 'POST', f'/v2/subscribers/{subscriber}/notifications/delete', {'tags': ['news'], 'data': json.dumps({'items': {'status': 'open'}})}) == (204, None)
items = compare('data_and_tags_delete', '')[0][1]['data']
assert len(items) == 1 and items[0]['tags'] == ['news', 'blue']
for base in (reference, rust):
    if os.environ.get('NOVU_TEST_JWT_SECRET'):
        assert request(base, 'POST', '/v1/inbox/notifications/delete', {}, auth_value=bearer) == (204, None)
    else:
        assert request(base, 'POST', '/v2/subscribers/' + subscriber + '/notifications/delete', {}) == (204, None)
left, right = compare('empty_after_delete_all', '')
assert left[1]['data'] == right[1]['data'] == []

for name, sub, auth in [('missing_subscriber', 'missing', True), ('missing_auth', subscriber, False)]:
    left, right = (request(base, 'GET', '/v2/subscribers/' + sub + '/notifications', auth=auth) for base in (reference, rust))
    assert left == right, (name, left, right)
    print(f'{name}: match')
