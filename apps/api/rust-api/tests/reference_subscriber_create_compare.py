#!/usr/bin/env python3
"""Compare a bounded POST/GET subscriber sequence on disposable servers.

Requires NOVU_REFERENCE_URL, NOVU_RUST_URL, NOVU_TEST_API_KEY. Both isolated
servers must use API keys scoped to equivalent environment and organization IDs.
The test creates three synthetic subscribers on each system. Generated IDs and
timestamps are checked for format but excluded from cross-server equality.
"""
import datetime
import json
import os
import re
import urllib.error
import urllib.request
import uuid

reference = os.environ['NOVU_REFERENCE_URL'].rstrip('/')
rust = os.environ['NOVU_RUST_URL'].rstrip('/')
key = os.environ['NOVU_TEST_API_KEY']
subscriber = 'compat-' + uuid.uuid4().hex
other = 'compat-' + uuid.uuid4().hex
empty = 'compat-' + uuid.uuid4().hex

def request(base, method, path, body=None, auth=True):
    headers = {'Content-Type': 'application/json'}
    if auth:
        headers['Authorization'] = 'ApiKey ' + key
    req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(), headers=headers, method=method)
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
    else:
        data = result['data']
        assert re.fullmatch(r'[0-9a-f]{24}', data.pop('_id'))
        for field in ('createdAt', 'updatedAt'):
            stamp = data.pop(field)
            assert stamp.endswith('Z')
            datetime.datetime.fromisoformat(stamp.replace('Z', '+00:00'))
    return status, result

cases = [
    ('create', '', {'subscriberId': subscriber, 'firstName': 'Jane', 'email': 'Jane+news@example.test', 'phone': '+123', 'data': {'plan': 'gold'}}, True),
    ('upsert', '', {'subscriberId': subscriber, 'lastName': 'Doe'}, True),
    ('explicit_false', '?failIfExists=false', {'subscriberId': subscriber, 'data': {'x': 1}}, True),
    ('null_data', '', {'subscriberId': subscriber, 'phone': None, 'data': None}, True),
    ('conflict', '?failIfExists=true', {'subscriberId': subscriber, 'firstName': 'No'}, True),
    ('trim', '', {'subscriberId': ' ' + other + ' ', 'firstName': 'Kim', 'data': {'x': 1}}, True),
    ('default_data', '', {'subscriberId': empty}, True),
    ('invalid_fields', '', {'subscriberId': subscriber, 'firstName': False, 'phone': 123}, True),
    ('locale', '', {'subscriberId': subscriber, 'locale': 'ko-KR'}, True),
    ('invalid_locale', '', {'subscriberId': subscriber, 'locale': 'en--US'}, True),
    ('clear_locale', '', {'subscriberId': subscriber, 'locale': None}, True),
    ('timezone', '', {'subscriberId': subscriber, 'timezone': 'Asia/Seoul'}, True),
    ('invalid_timezone', '', {'subscriberId': subscriber, 'timezone': '+24:00'}, True),
    ('clear_timezone', '', {'subscriberId': subscriber, 'timezone': None}, True),
    ('timezone_offset', '', {'subscriberId': subscriber, 'timezone': '+05:30'}, True),
    ('timezone_alias', '', {'subscriberId': subscriber, 'timezone': 'US/Eastern'}, True),
    ('timezone_type', '', {'subscriberId': subscriber, 'timezone': 7}, True),
    ('locale_type', '', {'subscriberId': subscriber, 'locale': 7}, True),
    ('invalid_email', '', {'subscriberId': subscriber, 'email': 'bad..name@example.test'}, True),
    ('clear_email', '', {'subscriberId': subscriber, 'email': None}, True),
    ('missing_id', '', {'phone': 123}, True),
    ('null_id', '', {'subscriberId': None}, True),
    ('number_id', '', {'subscriberId': 4}, True),
    ('empty_id', '', {'subscriberId': '   '}, True),
    ('missing_auth', '', {'subscriberId': subscriber}, False),
]
for name, query, body, auth in cases:
    expected = request(reference, 'POST', '/v2/subscribers' + query, body, auth)
    actual = request(rust, 'POST', '/v2/subscribers' + query, body, auth)
    if expected != actual:
        raise SystemExit(f'{name}: POST mismatch\n  reference={expected!r}\n  rust={actual!r}')
    if expected[0] == 201:
        target = body['subscriberId'].strip()
        left = request(reference, 'GET', '/v2/subscribers/' + target)
        right = request(rust, 'GET', '/v2/subscribers/' + target)
        if left != right:
            raise SystemExit(f'{name}: GET mismatch\n  reference={left!r}\n  rust={right!r}')
    print(f'{name}: match')
