#!/usr/bin/env python3
"""Differential test for trigger-driven subscriber profile creation and updates.

Requires NOVU_REFERENCE_URL, NOVU_RUST_URL, NOVU_TEST_API_KEY and the two
container variables accepted by reference_in_app_compare.py. Both disposable
servers need an active `greeting` workflow rendering `Hello {{subscriber.firstName}} {{subscriber.email}}`
and their workers running; the reference fixture also needs an active Novu
in-app integration and enabled workflow preferences. Creates a synthetic subscriber and three in-app
messages on each system. IDs and timestamps in subscriber responses are excluded
from equality because the systems generate them independently.
"""
import json
import os
import subprocess
import time
import urllib.request
import uuid

reference = os.environ['NOVU_REFERENCE_URL'].rstrip('/')
rust = os.environ['NOVU_RUST_URL'].rstrip('/')
key = os.environ['NOVU_TEST_API_KEY']
mongo = os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
postgres = os.environ['NOVU_RUST_PG_CONTAINER']
subscriber = 'compat-' + uuid.uuid4().hex
headers = {'Content-Type': 'application/json', 'Authorization': 'ApiKey ' + key}

def request(base, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=10) as response:
        return response.status, json.load(response)

def delivered(transaction):
    # The transaction identifier is generated from UUID hex, never user input.
    js = f'''const d=db.getSiblingDB("novu-db"); const rows=d.messages.find({{transactionId:"{transaction}"}}).toArray(); print(JSON.stringify(rows.map(m=>({{content:m.content,channel:m.channel,transactionId:m.transactionId,subscriberId:d.subscribers.findOne({{_id:m._subscriberId}})?.subscriberId}}))));'''
    sql = f"SELECT COALESCE(json_agg(json_build_object('content',content,'channel','in_app','transactionId',transaction_id,'subscriberId',subscriber_id)), '[]'::json)::text FROM novu_rust_in_app_messages WHERE transaction_id='{transaction}'"
    return (json.loads(subprocess.check_output(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], text=True)),
            json.loads(subprocess.check_output(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-At', '-c', sql], text=True)))

cases = [
    ('create', {'subscriberId': subscriber, 'firstName': 'Jane', 'email': 'Jane+alerts@example.test', 'phone': '+12025550123', 'avatar': 'https://example.test/avatar', 'data': {'plan': 'gold', 'old': True}}, 'Hello Jane Jane+alerts@example.test'),
    ('replace', {'subscriberId': subscriber, 'email': 'jane2@example.test', 'phone': None, 'data': {'plan': 'silver'}}, 'Hello Jane jane2@example.test'),
    ('clear', {'subscriberId': subscriber, 'email': None, 'data': None}, 'Hello Jane '),
]
for name, recipient, expected_content in cases:
    transaction = 'compat-' + uuid.uuid4().hex
    body = {'name': 'greeting', 'to': recipient, 'transactionId': transaction, 'payload': {}}
    expected = (201, {'data': {'acknowledged': True, 'status': 'processed', 'transactionId': transaction}})
    assert request(reference, '/v1/events/trigger', body) == expected
    assert request(rust, '/v1/events/trigger', body) == expected
    for _ in range(30):
        reference_messages, rust_messages = delivered(transaction)
        if reference_messages and rust_messages:
            break
        time.sleep(1)
    else:
        raise SystemExit(f'{name}: timed out waiting for both deliveries')
    expected_messages = [{'content': expected_content, 'channel': 'in_app', 'transactionId': transaction, 'subscriberId': subscriber}]
    if reference_messages != expected_messages or rust_messages != expected_messages:
        raise SystemExit(f'{name}: message mismatch: reference={reference_messages!r} rust={rust_messages!r}')
    left = request(reference, '/v2/subscribers/' + subscriber)
    right = request(rust, '/v2/subscribers/' + subscriber)
    # Systems generate distinct IDs/timestamps; all other HTTP fields must match.
    for result in (left, right):
        for field in ('_id', 'createdAt', 'updatedAt'):
            result[1]['data'].pop(field)
    if left != right:
        raise SystemExit(f'{name}: subscriber mismatch\n  reference={left!r}\n  rust={right!r}')
    print(f'{name}: trigger, subscriber response, and selected message fields match')
