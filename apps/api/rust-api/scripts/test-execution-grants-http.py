#!/usr/bin/env python3
"""Synthetic JWT HTTP client for the disposable execution-grant fixture."""
import json
import pathlib
import sys
import urllib.error
import urllib.parse
import urllib.request

port, root, phase = sys.argv[1:]
root = pathlib.Path(root)
tokens = {name: (root / (name + '.jwt')).read_text() for name in ('admin', 'target')}
base = 'http://127.0.0.1:' + port

def request(actor, path, method='GET', body=None):
    data = json.dumps(body).encode() if body is not None else None
    headers = {'Authorization': 'Bearer ' + tokens[actor]}
    if data is not None:
        headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)

env = '?environmentId=fixture-env'
grant = '/v1/experimental/operator/execution-grants' + env
trigger = '/v1/experimental/operator/events/trigger' + env
absent = {'granted': False, 'workflowInternalId': None}
bound = {'granted': True, 'workflowInternalId': 'generation-1'}
target = {'targetIssuer': 'https://issuer.test', 'targetSubject': 'target', 'targetClientId': 'console', 'workflow': 'welcome'}
query = urllib.parse.urlencode(target)

def change(request_id, expected, desired):
    return request('admin', grant, 'PUT', {'requestId': request_id, **target, 'expected': expected, 'desired': desired, 'reason': 'HTTP fixture'})

def invoke(transaction):
    return request('target', trigger, 'POST', {'name': 'welcome', 'to': 'alice', 'payload': {'name': 'Alice'}, 'transactionId': transaction})

if phase == 'first':
    assert invoke('http-denied')[0] == 401
    status, body = request('admin', grant + '&' + query)
    assert status == 200 and body['targetExists'] and body['workflowExists'] and body['state'] == absent
    request_id = '11111111-1111-4111-8111-111111111111'
    status, body = change(request_id, absent, bound)
    assert status == 200 and body['changed'] and not body['replayed']
    status, body = change(request_id, absent, bound)
    assert status == 200 and body['replayed']
    status, body = request('admin', '/v1/experimental/operator/execution-grants/request' + env + '&requestId=' + request_id)
    assert status == 200 and body['found'] and body['desired'] == bound
    status, body = invoke('http-first')
    assert status == 201 and body['data']['status'] == 'processed'
elif phase == 'second':
    status, body = invoke('http-second')
    assert status == 201 and body['data']['status'] == 'processed'
    status, body = change('22222222-2222-4222-8222-222222222222', bound, absent)
    assert status == 200 and body['changed']
    assert invoke('http-denied-after')[0] == 401
    status, body = request('admin', '/v1/experimental/operator/execution-grants/history' + env)
    assert status == 200 and len(body['data']) == 2
    status, body = request('admin', '/v1/experimental/operator/execution-audit' + env)
    assert status == 200 and {r['transactionId'] for r in body['data']} == {'http-first', 'http-second'}
else:
    raise ValueError('unknown phase')
