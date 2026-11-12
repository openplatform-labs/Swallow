#!/usr/bin/env python3
"""Compare bounded v2 subscriber GET cases against a running original server.

Requires NOVU_REFERENCE_URL, NOVU_RUST_URL, NOVU_TEST_API_KEY. Both disposable
servers must contain equivalent subscriber 'alice' (including IDs and timestamps)
and no subscriber named 'missing'. Does not test other subscriber fields/routes.
"""
import datetime
import json
import os
import urllib.error
import urllib.request

reference = os.environ['NOVU_REFERENCE_URL'].rstrip('/')
rust = os.environ['NOVU_RUST_URL'].rstrip('/')
key = os.environ['NOVU_TEST_API_KEY']
cases = [('existing', 'alice', 'ApiKey ' + key), ('not_found', 'missing', 'ApiKey ' + key), ('missing_auth', 'alice', None), ('invalid_key', 'alice', 'ApiKey invalid')]

def call(base, subscriber, auth):
    headers = {}
    if auth is not None:
        headers['Authorization'] = auth
    request = urllib.request.Request(f'{base}/v2/subscribers/{subscriber}', headers=headers)
    try:
        response = urllib.request.urlopen(request, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        status = response.status
        result = json.load(response)
    if status >= 400:
        stamp = result.pop('timestamp')
        assert stamp.endswith('Z')
        datetime.datetime.fromisoformat(stamp.replace('Z', '+00:00'))
    return status, result

failures = []
for name, subscriber, auth in cases:
    expected = call(reference, subscriber, auth)
    actual = call(rust, subscriber, auth)
    if expected != actual:
        failures.append(name)
        print(f'{name}: mismatch\n  reference={expected!r}\n  rust={actual!r}')
    else:
        print(f'{name}: match')
if failures:
    raise SystemExit(f'{len(failures)} case(s) differ')
