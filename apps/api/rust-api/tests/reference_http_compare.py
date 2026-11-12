#!/usr/bin/env python3
"""Compare a bounded trigger API subset against a separately running reference.

Requires disposable instances with equivalent fixture API keys, no workflow named
'missing', and 'inactive'/'empty' workflows matching their names:
NOVU_REFERENCE_URL, NOVU_RUST_URL, NOVU_TEST_API_KEY. The reference
server must be built from the documented baseline revision. Does not test delivery.
"""
import datetime
import json
import os
import urllib.error
import urllib.request

reference = os.environ['NOVU_REFERENCE_URL'].rstrip('/')
rust = os.environ['NOVU_RUST_URL'].rstrip('/')
key = os.environ['NOVU_TEST_API_KEY']
cases = [
    ('missing_auth', None, {'name': 'missing', 'to': 'alice'}),
    ('invalid_key', 'ApiKey invalid', {'name': 'missing', 'to': 'alice'}),
    ('unknown_workflow', f'ApiKey {key}', {'name': 'missing', 'to': 'alice'}),
    ('bad_scheme', 'Invalid invalid_value', {'name': 'missing', 'to': 'alice'}),
    ('invalid_bearer', 'Bearer invalid', {'name': 'missing', 'to': 'alice'}),
    ('empty_body', f'ApiKey {key}', {}),
    ('bad_name', f'ApiKey {key}', {'name': 1, 'to': 'x'}),
    ('null_recipient', f'ApiKey {key}', {'name': 'x', 'to': None}),
    ('bad_payload_txn', f'ApiKey {key}', {'name': 'x', 'to': 'a', 'payload': [], 'transactionId': 3}),
    ('bad_payload_recipient', f'ApiKey {key}', {'name': 'x', 'to': 1, 'payload': 'bad'}),
    ('null_name_recipient', f'ApiKey {key}', {'name': None, 'to': None}),
    ('inactive_workflow', f'ApiKey {key}', {'name': 'inactive', 'to': 'alice', 'transactionId': 'compat-inactive'}),
    ('empty_workflow', f'ApiKey {key}', {'name': 'empty', 'to': 'alice', 'transactionId': 'compat-empty'}),
]

def call(base, auth, body):
    headers = {'Content-Type': 'application/json'}
    if auth is not None:
        headers['Authorization'] = auth
    request = urllib.request.Request(base + '/v1/events/trigger', data=json.dumps(body).encode(), headers=headers)
    try:
        response = urllib.request.urlopen(request, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        status = response.status
        result = json.load(response)
    # Request times naturally differ. Validate ISO-8601 UTC while comparing all other fields.
    if status >= 400:
        stamp = result.pop('timestamp')
        assert stamp.endswith('Z')
        datetime.datetime.fromisoformat(stamp.replace('Z', '+00:00'))
    return status, result

failures = []
for name, auth, body in cases:
    expected = call(reference, auth, body)
    actual = call(rust, auth, body)
    if expected != actual:
        failures.append(name)
        print(f'{name}: mismatch\n  reference={expected!r}\n  rust={actual!r}')
    else:
        print(f'{name}: match')
if failures:
    raise SystemExit(f'{len(failures)} case(s) differ')
