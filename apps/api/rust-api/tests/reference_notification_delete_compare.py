#!/usr/bin/env python3
"""Compare notification deletion on equivalent disposable API fixtures.

Requires NOVU_REFERENCE_URL, NOVU_RUST_URL and NOVU_TEST_API_KEY. Each server
must have 'alice' with messages 64a000000000000000000101 and ...102, and 'bob'
with message ...103, all unread in-app messages. 'missing' must not exist.
The test deletes Alice's messages. It does not compare Socket.IO, webhooks,
interaction traces, filtering, concurrent delivery or non-in-app channels.
"""
import datetime
import json
import os
import urllib.error
import urllib.parse
import urllib.request

reference = os.environ['NOVU_REFERENCE_URL'].rstrip('/')
rust = os.environ['NOVU_RUST_URL'].rstrip('/')
key = os.environ['NOVU_TEST_API_KEY']

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
    ('missing_auth', 'alice', '64a000000000000000000101', 'missing'),
    ('invalid_key', 'alice', '64a000000000000000000101', 'invalid'),
    ('invalid_id', 'alice', 'bad', 'valid'),
    ('missing_subscriber', 'missing', '64a000000000000000000101', 'valid'),
    ('missing_message', 'alice', '64a000000000000000000199', 'valid'),
    ('other_subscriber', 'alice', '64a000000000000000000103', 'valid'),
    ('delete_one', 'alice', '64A000000000000000000101', 'valid'),
    ('delete_again', 'alice', '64a000000000000000000101', 'valid'),
]
for name, subscriber, notification, auth in cases:
    path = f'/v2/subscribers/{subscriber}/notifications/{notification}'
    left, right = (request(base, 'DELETE', path, auth=auth) for base in (reference, rust))
    if left != right:
        raise SystemExit(f'{name}: mismatch\n  reference={left!r}\n  rust={right!r}')
    print(f'{name}: match')

query = urllib.parse.urlencode({'filters': '[{}]'})
for subscriber, count in [('alice', 1), ('bob', 1)]:
    left, right = (request(base, 'GET', f'/v2/subscribers/{subscriber}/notifications/count?{query}') for base in (reference, rust))
    assert left == right == (200, {'data': [{'count': count, 'filter': {}}]}), (left, right)
print('counts and subscriber isolation: match')

for base in (reference, rust):
    assert request(base, 'POST', '/v2/subscribers/alice/notifications/delete', {}) == (204, None)
    assert request(base, 'POST', '/v2/subscribers/alice/notifications/delete', {}) == (204, None)
    assert request(base, 'GET', '/v2/subscribers/alice/notifications/count?' + query) == (200, {'data': [{'count': 0, 'filter': {}}]})
    assert request(base, 'GET', '/v2/subscribers/bob/notifications/count?' + query) == (200, {'data': [{'count': 1, 'filter': {}}]})
print('delete all, repeat and counts: match')
