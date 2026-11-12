#!/usr/bin/env python3
"""Compare subscriber deletion and message cleanup on disposable servers.

Requires NOVU_REFERENCE_URL, NOVU_RUST_URL, NOVU_TEST_API_KEY, NOVU_SECOND_TEST_API_KEY,
NOVU_REFERENCE_MONGO_CONTAINER, NOVU_RUST_PG_CONTAINER and
NOVU_TEST_ENVIRONMENT_ID. The original MongoDB must support transactions
(e.g. a single-node replica set). Creates two synthetic subscribers and one
message for each in both systems, plus a subscriber in a second environment;
the second key must resolve to a different environment in both databases.
Run only against disposable databases.
"""
import datetime
import json
import os
import subprocess
import urllib.error
import urllib.request
import uuid

reference = os.environ['NOVU_REFERENCE_URL'].rstrip('/')
rust = os.environ['NOVU_RUST_URL'].rstrip('/')
key = os.environ['NOVU_TEST_API_KEY']
second_key = os.environ['NOVU_SECOND_TEST_API_KEY']
mongo = os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
postgres = os.environ['NOVU_RUST_PG_CONTAINER']
environment = os.environ['NOVU_TEST_ENVIRONMENT_ID']
assert len(environment) == 24 and all(c in '0123456789abcdef' for c in environment)
target = 'compat-' + uuid.uuid4().hex
survivor = 'compat-' + uuid.uuid4().hex

def request(base, method, path, body=None, auth='valid'):
    headers = {'Content-Type': 'application/json'}
    if auth != 'missing':
        headers['Authorization'] = 'ApiKey ' + {'valid': key, 'second': second_key}.get(auth, 'invalid')
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
    return status, result

subscriber_ids = {}
for subscriber in (target, survivor):
    for base in (reference, rust):
        status, result = request(base, 'POST', '/v2/subscribers', {'subscriberId': subscriber, 'firstName': 'Jane'})
        assert status == 201, result
        subscriber_ids[(base, subscriber)] = result['data']['_id']
for base in (reference, rust):
    status, result = request(base, 'POST', '/v2/subscribers', {'subscriberId': target, 'firstName': 'Other environment'}, auth='second')
    assert status == 201 and result['data']['_environmentId'] != environment, result
    subscriber_ids[(base, 'second')] = result['data']['_id']

# The second preference deliberately points to a nonexistent subscription:
# v1 removals can leave such records behind, but subscriber deletion removes them.
topic = 'delete-' + uuid.uuid4().hex
ids = []
for base in (reference, rust):
    status, result = request(base, 'POST', '/v2/topics/' + topic + '/subscriptions',
                             {'subscriptions': [{'subscriberId': target, 'identifier': topic}]})
    assert status == 201, result
    ids.append((result['data'][0]['_id'], result['data'][0]['subscriber']['_id']))
orphan = '64a000000000000000000099'
workflow = '64a000000000000000000098'
for subid, internal in (ids[0],):
    js = f'''const d=db.getSiblingDB("novu-db"),e=ObjectId("{environment}"),o=ObjectId("64a000000000000000000003"),s=ObjectId("{internal}"),w=ObjectId("{workflow}"); for(const sub of ["{subid}","{orphan}"]) d.preferences.insertOne({{_environmentId:e,_organizationId:o,_subscriberId:s,_topicSubscriptionId:ObjectId(sub),_templateId:w,type:"SUBSCRIPTION_SUBSCRIBER_WORKFLOW",preferences:{{all:{{enabled:false}}}},deleted:false}});'''
    subprocess.check_call(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], stdout=subprocess.DEVNULL)
subid, internal = ids[1]
sql = f"INSERT INTO novu_rust_subscription_preferences(environment_id,subscriber_internal_id,subscription_internal_id,workflow_internal_id,preferences) VALUES ('{environment}','{internal}','{subid}','{workflow}','{{}}'),('{environment}','{internal}','{orphan}','{workflow}','{{}}')"
subprocess.check_call(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-v', 'ON_ERROR_STOP=1', '-c', sql], stdout=subprocess.DEVNULL)
second_env = '64a000000000000000000012'
for base, env, organization, subscriber in ((reference, environment, '64a000000000000000000003', survivor), (reference, second_env, '64a000000000000000000013', 'second')):
    internal = subscriber_ids[(base, subscriber)]
    js = f'''const d=db.getSiblingDB("novu-db"); d.preferences.insertOne({{_environmentId:ObjectId("{env}"),_organizationId:ObjectId("{organization}"),_subscriberId:ObjectId("{internal}"),_topicSubscriptionId:ObjectId("{orphan}"),_templateId:ObjectId("{workflow}"),type:"SUBSCRIPTION_SUBSCRIBER_WORKFLOW",preferences:{{all:{{enabled:false}}}},deleted:false}});'''
    subprocess.check_call(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], stdout=subprocess.DEVNULL)
for env, subscriber in ((environment, survivor), (second_env, 'second')):
    internal = subscriber_ids[(rust, subscriber)]
    sql = f"INSERT INTO novu_rust_subscription_preferences(environment_id,subscriber_internal_id,subscription_internal_id,workflow_internal_id,preferences) VALUES ('{env}','{internal}','{orphan}','{workflow}','{{}}')"
    subprocess.check_call(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-v', 'ON_ERROR_STOP=1', '-c', sql], stdout=subprocess.DEVNULL)
js = f'''const d=db.getSiblingDB("novu-db"),e=ObjectId("{environment}"),s=ObjectId("{ids[0][1]}"); print(JSON.stringify({{preferences:d.preferences.countDocuments({{_environmentId:e,_subscriberId:s}}),subscriptions:d.topicsubscribers.countDocuments({{_environmentId:e,_subscriberId:s}})}}));'''
before = json.loads(subprocess.check_output(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], text=True))
assert before == {'preferences': 2, 'subscriptions': 1}, before

# All interpolated identifiers are generated UUID hex or validated environment IDs.
js = f'''const d=db.getSiblingDB("novu-db"), e=ObjectId("{environment}"); for(const id of ["{target}","{survivor}"]) {{ const s=d.subscribers.findOne({{_environmentId:e,subscriberId:id}}); d.messages.insertOne({{_environmentId:e,_subscriberId:s._id,transactionId:id,channel:"in_app",content:"fixture",deleted:false}}); }}'''
subprocess.check_call(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], stdout=subprocess.DEVNULL)
for subscriber in (target, survivor):
    sql = f"INSERT INTO novu_rust_in_app_messages (id,environment_id,subscriber_id,workflow_identifier,transaction_id,content,payload) VALUES ('{uuid.uuid4()}','{environment}','{subscriber}','fixture','{subscriber}','fixture','{{}}')"
    subprocess.check_call(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-v', 'ON_ERROR_STOP=1', '-c', sql], stdout=subprocess.DEVNULL)

for name, subscriber, auth in [('missing_auth', target, 'missing'), ('invalid_key', target, 'invalid'), ('not_found', 'missing', 'valid'), ('delete', target, 'valid'), ('delete_again', target, 'valid')]:
    expected = request(reference, 'DELETE', '/v2/subscribers/' + subscriber, auth=auth)
    actual = request(rust, 'DELETE', '/v2/subscribers/' + subscriber, auth=auth)
    if expected != actual:
        raise SystemExit(f'{name}: mismatch\n  reference={expected!r}\n  rust={actual!r}')
    print(f'{name}: match')

left = request(reference, 'GET', '/v2/subscribers/' + target)
right = request(rust, 'GET', '/v2/subscribers/' + target)
assert left == right and left[0] == 404, (left, right)
for base in (reference, rust):
    status, result = request(base, 'GET', '/v2/subscribers/' + survivor)
    assert status == 200 and result['data']['subscriberId'] == survivor
    status, result = request(base, 'GET', '/v2/subscribers/' + target, auth='second')
    assert status == 200 and result['data']['firstName'] == 'Other environment'

js = f'''const d=db.getSiblingDB("novu-db"),e=ObjectId("{environment}"),s=d.subscribers.findOne({{_environmentId:e,subscriberId:"{survivor}"}}); print(JSON.stringify({{target:d.subscribers.countDocuments({{_environmentId:e,subscriberId:"{target}"}}),survivor:d.subscribers.countDocuments({{_environmentId:e,subscriberId:"{survivor}"}}),messages:d.messages.countDocuments({{_environmentId:e,transactionId:{{$in:["{target}","{survivor}"]}}}}),survivorMessages:d.messages.countDocuments({{_environmentId:e,_subscriberId:s._id,transactionId:"{survivor}"}})}}));'''
ref_counts = json.loads(subprocess.check_output(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], text=True))
sql = f"SELECT json_build_object('target',(SELECT count(*) FROM novu_rust_subscribers WHERE environment_id='{environment}' AND subscriber_id='{target}'),'survivor',(SELECT count(*) FROM novu_rust_subscribers WHERE environment_id='{environment}' AND subscriber_id='{survivor}'),'messages',(SELECT count(*) FROM novu_rust_in_app_messages WHERE environment_id='{environment}' AND transaction_id IN ('{target}','{survivor}')),'survivorMessages',(SELECT count(*) FROM novu_rust_in_app_messages WHERE environment_id='{environment}' AND subscriber_id='{survivor}' AND transaction_id='{survivor}'))::text"
rust_counts = json.loads(subprocess.check_output(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-At', '-c', sql], text=True))
expected_counts = {'target': 0, 'survivor': 1, 'messages': 1, 'survivorMessages': 1}
assert ref_counts == rust_counts == expected_counts, (ref_counts, rust_counts)
print('subscriber lookup, environment isolation and related message cleanup: match')

js = f'''const d=db.getSiblingDB("novu-db"),e=ObjectId("{environment}"); print(JSON.stringify({{preferences:d.preferences.countDocuments({{_environmentId:e,_subscriberId:ObjectId("{ids[0][1]}")}}),subscriptions:d.topicsubscribers.countDocuments({{_environmentId:e,_subscriberId:ObjectId("{ids[0][1]}")}})}}));'''
ref_counts = json.loads(subprocess.check_output(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], text=True))
sql = f"SELECT json_build_object('preferences',(SELECT count(*) FROM novu_rust_subscription_preferences WHERE environment_id='{environment}' AND subscriber_internal_id='{ids[1][1]}'),'subscriptions',(SELECT count(*) FROM novu_rust_topic_subscribers WHERE environment_id='{environment}' AND subscriber_id='{target}'))::text"
rust_counts = json.loads(subprocess.check_output(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-At', '-c', sql], text=True))
assert ref_counts == rust_counts == {'preferences': 0, 'subscriptions': 0}, (ref_counts, rust_counts)
print('subscriber subscriptions and orphan preferences cleanup: match')
for env, subscriber in ((environment, survivor), (second_env, 'second')):
    js = f'''print(db.getSiblingDB("novu-db").preferences.countDocuments({{_environmentId:ObjectId("{env}"),_subscriberId:ObjectId("{subscriber_ids[(reference, subscriber)]}")}}));'''
    ref_count = int(subprocess.check_output(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], text=True))
    sql = f"SELECT count(*) FROM novu_rust_subscription_preferences WHERE environment_id='{env}' AND subscriber_internal_id='{subscriber_ids[(rust, subscriber)]}'"
    rust_count = int(subprocess.check_output(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-At', '-c', sql], text=True))
    assert ref_count == rust_count == 1, (env, subscriber, ref_count, rust_count)
print('other subscriber and environment preferences preserved: match')
# The following reference tests use global preference counts, so remove only
# the preservation fixtures created by this test after verifying them.
for env, subscriber in ((environment, survivor), (second_env, 'second')):
    js = f'''db.getSiblingDB("novu-db").preferences.deleteMany({{_environmentId:ObjectId("{env}"),_subscriberId:ObjectId("{subscriber_ids[(reference, subscriber)]}"),_topicSubscriptionId:ObjectId("{orphan}")}});'''
    subprocess.check_call(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], stdout=subprocess.DEVNULL)
    sql = f"DELETE FROM novu_rust_subscription_preferences WHERE environment_id='{env}' AND subscriber_internal_id='{subscriber_ids[(rust, subscriber)]}' AND subscription_internal_id='{orphan}'"
    subprocess.check_call(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-v', 'ON_ERROR_STOP=1', '-c', sql], stdout=subprocess.DEVNULL)

for base in (reference, rust):
    status, result = request(base, 'POST', '/v2/subscribers', {'subscriberId': target, 'firstName': 'Recreated'})
    assert status == 201 and result['data']['firstName'] == 'Recreated', result
    assert result['data']['_id'] != ids[0 if base == reference else 1][1], result
    status, result = request(base, 'GET', '/v2/subscribers/' + target)
    assert status == 200 and result['data']['subscriberId'] == target
    status, result = request(base, 'GET', '/v2/subscribers/' + target + '/subscriptions')
    assert status == 200 and result['data'] == [], result
print('recreate after deletion: match')
