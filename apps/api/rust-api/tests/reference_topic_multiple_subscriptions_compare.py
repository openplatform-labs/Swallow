#!/usr/bin/env python3
"""Compare legacy views/removal of distinct v2 subscription identities."""
import json
import os
import urllib.error
import urllib.request
import uuid

reference = os.environ['NOVU_REFERENCE_URL']
rust = os.environ['NOVU_RUST_URL']
key = os.environ['NOVU_TEST_API_KEY']
suffix = uuid.uuid4().hex
subscriber = 'multi-user-' + suffix
topic = 'multi-topic-' + suffix
identifier = 'custom-' + suffix

def req(base, method, path, body=None):
    request = urllib.request.Request(base + path, method=method,
        headers={'Authorization': 'ApiKey ' + key, 'Content-Type': 'application/json'},
        data=json.dumps(body).encode() if body is not None else None)
    try:
        response = urllib.request.urlopen(request, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read()
        return response.status, json.loads(raw) if raw else None

def clean(result):
    status, body = result
    data = body.get('data', {}) if isinstance(body, dict) else {}
    for field in ('_id', '_topicId', '_subscriberId', 'createdAt', 'updatedAt'):
        data.pop(field, None)
    if 'subscriberId' in data:
        data.pop('id', None)
    for field in ('subscribers', 'topics'):
        if field in data:
            data[field].sort()
    return status, body

def compare(label, method, path, body=None):
    results = [req(base, method, path, body) for base in (reference, rust)]
    assert clean(results[0]) == clean(results[1]), (label, results)
    print(label + ': match')
    return results[0]

for base in (reference, rust):
    assert req(base, 'POST', '/v2/subscribers', {'subscriberId': subscriber})[0] == 201
member = '/v1/topics/' + topic + '/subscribers'
compare('multiple_default_create', 'POST', member, {'subscribers': [subscriber]})
results = [req(base, 'POST', '/v2/topics/' + topic + '/subscriptions',
               {'subscriptions': [{'subscriberId': subscriber, 'identifier': identifier, 'name': 'Custom'}]}) for base in (reference, rust)]
for status, result in results:
    assert status == 201 and result['data'][0]['identifier'] == identifier, (status, result)
    assert result['data'][0]['createdAt'] == '' and result['data'][0]['updatedAt'] == ''
for ident in (identifier, identifier + ':ctx_project:one', 'missing'):
    results = [req(base, 'GET', '/v2/topics/' + topic + '/subscriptions/' + ident) for base in (reference, rust)]
    for status, body in results:
        if status == 200:
            assert len(body['data']['id']) == 24
            body['data'].pop('id')
    assert results[0] == results[1], ('v2_subscription_get', ident, results)
    print('v2_subscription_get: match')
_, result = compare('multiple_topic_get', 'GET', '/v1/topics/' + topic)
assert result['data']['subscribers'] == [subscriber, subscriber], result
compare('multiple_member_get', 'GET', member + '/' + subscriber)
_, result = compare('multiple_subscriber_topics', 'GET', '/v1/subscribers/' + subscriber + '?includeTopics=true')
assert result['data']['topics'] == [topic], result
compare('multiple_default_idempotent', 'POST', member, {'subscribers': [subscriber]})
compare('multiple_remove', 'POST', member + '/removal', {'subscribers': [subscriber]})
_, result = compare('multiple_topic_after_remove', 'GET', '/v1/topics/' + topic)
assert result['data']['subscribers'] == [], result
