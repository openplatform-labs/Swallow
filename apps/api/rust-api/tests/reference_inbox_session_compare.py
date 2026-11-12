#!/usr/bin/env python3
"""Compare Inbox sessions against disposable reference and Rust servers.

Requires the two URLs, NOVU_TEST_API_KEY, NOVU_TEST_APP_IDENTIFIER and the
MongoDB, Redis and PostgreSQL container names in the environment variables
below. Both servers must share the test JWT secret and equivalent active
in-app settings. This creates synthetic subscribers, changes the disposable
integration's HMAC policy and flushes its dedicated Redis; never use it on
shared or production services. Context, keyless, schedules and realtime are
outside this test.
"""
import hashlib
import hmac
import json
import os
import subprocess
import urllib.error
import urllib.request
import uuid

reference = os.environ['NOVU_REFERENCE_URL'].rstrip('/')
rust = os.environ['NOVU_RUST_URL'].rstrip('/')
key = os.environ['NOVU_TEST_API_KEY']
app = os.environ['NOVU_TEST_APP_IDENTIFIER']
mongo = os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
redis = os.environ['NOVU_REFERENCE_REDIS_CONTAINER']
postgres = os.environ['NOVU_RUST_PG_CONTAINER']
subscriber = 'compat-session-' + uuid.uuid4().hex
environment = '64a000000000000000000002'

def request(base, method, path, body=None, auth=None):
    headers = {'Content-Type': 'application/json'}
    if auth: headers['Authorization'] = auth
    req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(), headers=headers, method=method)
    try: response = urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as error: response = error
    with response: status, raw = response.status, response.read()
    value = json.loads(raw) if raw else None
    if status >= 400: assert value.pop('timestamp').endswith('Z')
    return status, value

def compare(name, body, expected_status=201):
    results = [request(base, 'POST', '/v1/inbox/session', body) for base in (reference, rust)]
    assert results[0][0] == results[1][0] == expected_status, (name, results)
    tokens = []
    if expected_status == 201:
        for _, value in results: tokens.append(value['data'].pop('token'))
    assert results[0] == results[1], (name, results)
    if tokens:
        for base in (reference, rust):
            for token in tokens:
                status, _ = request(base, 'GET', '/v1/inbox/notifications/count?filters=%5B%7B%7D%5D', auth='Bearer ' + token)
                assert status == 200, (name, status)
    print(name + ': match')

def profile():
    results = [request(base, 'GET', '/v2/subscribers/' + subscriber, auth='ApiKey ' + key) for base in (reference, rust)]
    for _, body in results:
        for field in ('_id', 'createdAt', 'updatedAt'): body['data'].pop(field)
    assert results[0] == results[1], results
    return results[0][1]['data']

compare('missing_identifier', {'subscriberId': subscriber}, 422)
compare('missing_subscriber', {'applicationIdentifier': app}, 422)
compare('unknown_identifier', {'applicationIdentifier': 'does-not-exist', 'subscriberId': subscriber}, 400)
compare('create_without_hmac', {'applicationIdentifier': app, 'subscriber': {'subscriberId': subscriber, 'firstName': 'Jane'}})
assert profile()['firstName'] == 'Jane'
marker = uuid.uuid4().hex
js = f'''const d=db.getSiblingDB("novu-db"), e=ObjectId("{environment}"), s=d.subscribers.findOne({{_environmentId:e,subscriberId:"{subscriber}"}}); d.messages.insertMany([{{_environmentId:e,_subscriberId:s._id,channel:"in_app",read:false,seen:false,archived:false,transactionId:"{marker}-1"}},{{_environmentId:e,_subscriberId:s._id,channel:"in_app",read:true,seen:true,archived:false,transactionId:"{marker}-2"}}]);'''
subprocess.check_call(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], stdout=subprocess.DEVNULL)
sql = f"INSERT INTO novu_rust_in_app_messages (id,environment_id,subscriber_id,workflow_identifier,transaction_id,content,payload,is_read,is_seen) VALUES (md5('{marker}-1')::uuid,'{environment}','{subscriber}','fixture','{marker}-1','x','{{}}',false,false),(md5('{marker}-2')::uuid,'{environment}','{subscriber}','fixture','{marker}-2','x','{{}}',true,true)"
subprocess.check_call(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-v', 'ON_ERROR_STOP=1', '-c', sql], stdout=subprocess.DEVNULL)
subprocess.check_call(['docker', 'exec', redis, 'redis-cli', 'FLUSHALL'], stdout=subprocess.DEVNULL)
compare('session_with_unread', {'applicationIdentifier': app, 'subscriberId': subscriber})
compare('untrusted_update_ignored', {'applicationIdentifier': app, 'subscriber': {'subscriberId': subscriber, 'firstName': 'Changed'}})
assert profile()['firstName'] == 'Jane'
digest = hmac.new(key.encode(), subscriber.encode(), hashlib.sha256).hexdigest()
compare('authenticated_update', {'applicationIdentifier': app, 'subscriber': {'subscriberId': subscriber, 'firstName': 'Valid'}, 'subscriberHash': digest})
assert profile()['firstName'] == 'Valid'

js = f'db.getSiblingDB("novu-db").integrations.updateMany({{_environmentId:ObjectId("{environment}"),providerId:"novu"}},{{$set:{{"credentials.hmac":true}}}})'
subprocess.check_call(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], stdout=subprocess.DEVNULL)
subprocess.check_call(['docker', 'exec', redis, 'redis-cli', 'FLUSHALL'], stdout=subprocess.DEVNULL)
subprocess.check_call(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-v', 'ON_ERROR_STOP=1', '-c', f"UPDATE novu_rust_inbox_environments SET hmac_required=true WHERE environment_id='{environment}'"], stdout=subprocess.DEVNULL)
compare('required_missing', {'applicationIdentifier': app, 'subscriberId': subscriber}, 400)
compare('required_invalid', {'applicationIdentifier': app, 'subscriberId': subscriber, 'subscriberHash': '0' * 64}, 400)
compare('required_valid', {'applicationIdentifier': app, 'subscriberId': subscriber, 'subscriberHash': digest})
assert profile()['firstName'] == 'Valid'
print('subscriber profile after HMAC: match')
