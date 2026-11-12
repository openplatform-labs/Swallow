#!/usr/bin/env python3
"""Compare a bounded PATCH/GET sequence on two disposable servers.

Requires NOVU_REFERENCE_URL, NOVU_RUST_URL, NOVU_TEST_API_KEY and equivalent
subscriber 'alice' fixtures with the same _id, organization/environment IDs,
createdAt, updatedAt, firstName='Jane', phone='+123', avatar='old', and
data={'old': True}. Neither side may contain subscriber 'missing'.
Only selected profile fields are supported; this mutates the fixture.
"""
import datetime
import json
import os
import urllib.error
import urllib.request

reference = os.environ['NOVU_REFERENCE_URL'].rstrip('/')
rust = os.environ['NOVU_RUST_URL'].rstrip('/')
key = os.environ['NOVU_TEST_API_KEY']

def request(base, method, subscriber, body=None, auth=True):
    headers = {'Content-Type': 'application/json'}
    if auth:
        headers['Authorization'] = 'ApiKey ' + key
    req = urllib.request.Request(base + '/v2/subscribers/' + subscriber,
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers=headers, method=method)
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

def comparable(result):
    status, body = result
    body = json.loads(json.dumps(body))
    if status < 400 and 'data' in body:
        # Separate servers assign update times independently.
        body['data'].pop('updatedAt', None)
    return status, body

cases = [
    ('update', 'alice', {'firstName': 'Kim', 'phone': None, 'data': {'new': True}}, True),
    ('preserve', 'alice', {'lastName': 'Doe'}, True),
    ('email', 'alice', {'email': 'Jane+alerts@example.test'}, True),
    ('locale', 'alice', {'locale': 'zh-Hant-TW'}, True),
    ('invalid_locale', 'alice', {'locale': 'en--US'}, True),
    ('clear_locale', 'alice', {'locale': None}, True),
    ('timezone', 'alice', {'timezone': 'US/Eastern'}, True),
    ('invalid_timezone', 'alice', {'timezone': '+24:00'}, True),
    ('clear_timezone', 'alice', {'timezone': None}, True),
    ('timezone_offset', 'alice', {'timezone': '+05:30'}, True),
    ('timezone_lowercase', 'alice', {'timezone': 'asia/seoul'}, True),
    ('timezone_type', 'alice', {'timezone': 7}, True),
    ('locale_type', 'alice', {'locale': 7}, True),
    ('invalid_email', 'alice', {'email': 'bad..name@example.test'}, True),
    ('clear_email', 'alice', {'email': None}, True),
    ('empty_data', 'alice', {'data': {}}, True),
    ('preserve_empty_data', 'alice', {'firstName': 'Lee'}, True),
    ('clear', 'alice', {'data': None, 'avatar': None}, True),
    ('no_change', 'alice', {}, True),
    ('invalid_fields', 'alice', {'phone': 123, 'firstName': False, 'avatar': [], 'data': []}, True),
    ('missing', 'missing', {'firstName': 'Nobody'}, True),
    ('missing_auth', 'alice', {'phone': 'bad'}, False),
]
for name, subscriber, body, auth in cases:
    previous = None
    if subscriber == 'alice' and auth:
        previous = [request(base, 'GET', subscriber)[1]['data']['updatedAt'] for base in (reference, rust)]
    expected = request(reference, 'PATCH', subscriber, body, auth)
    actual = request(rust, 'PATCH', subscriber, body, auth)
    if expected[0] == actual[0] == 200 and previous:
        for result, stamp in zip((expected, actual), previous):
            assert result[1]['data']['updatedAt'] == stamp, f'{name}: PATCH changed response timestamp'
    if comparable(expected) != comparable(actual):
        raise SystemExit(f'{name}: PATCH mismatch\n  reference={expected!r}\n  rust={actual!r}')
    if expected[0] == 200:
        left = request(reference, 'GET', subscriber)
        right = request(rust, 'GET', subscriber)
        if comparable(left) != comparable(right):
            raise SystemExit(f'{name}: GET mismatch\n  reference={left!r}\n  rust={right!r}')
    print(f'{name}: match')
