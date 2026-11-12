#!/usr/bin/env python3
"""Compare context sessions with disposable reference/Rust API and databases.

Requires the same environment variables as reference_inbox_session_compare.py.
Changes the isolated integration HMAC policy and flushes its dedicated Redis.
Never run on shared services; this is not a complete Inbox/worker comparison.
"""
import hashlib
import hmac
import json
import os
import subprocess
import urllib.error
import urllib.request
import uuid

bases = [os.environ['NOVU_REFERENCE_URL'].rstrip('/'), os.environ['NOVU_RUST_URL'].rstrip('/')]
key = os.environ['NOVU_TEST_API_KEY']
app = os.environ['NOVU_TEST_APP_IDENTIFIER']
mongo = os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
redis = os.environ['NOVU_REFERENCE_REDIS_CONTAINER']
postgres = os.environ['NOVU_RUST_PG_CONTAINER']
environment = '64a000000000000000000002'
subscriber = 'ctx-' + uuid.uuid4().hex
context_id = 'c-' + uuid.uuid4().hex

def call(base, method, path, body=None, auth=None):
    headers = {'Content-Type':'application/json'}
    if auth: headers['Authorization'] = auth
    request = urllib.request.Request(base+path, data=json.dumps(body).encode() if body is not None else None, headers=headers, method=method)
    try: response = urllib.request.urlopen(request, timeout=15)
    except urllib.error.HTTPError as error: response = error
    with response: status, raw = response.status, response.read()
    data = json.loads(raw) if raw else None
    if status >= 400: data.pop('timestamp', None)
    return status, data

def session(name, body, expected=201, unread=0):
    results = [call(base,'POST','/v1/inbox/session',body) for base in bases]
    tokens = []
    for status, value in results:
        assert status == expected, (name, results)
        if status == 201:
            assert value['data']['totalUnreadCount'] == unread, (name, value)
            tokens.append(value['data'].pop('token'))
    assert results[0] == results[1], (name, results)
    if tokens:
        for base in bases:
            for token in tokens:
                status, value = call(base, 'GET', '/v1/inbox/notifications/count?filters=%5B%7B%7D%5D', auth='Bearer '+token)
                assert status == 200 and value['data'][0]['count'] == unread, (name, status, value)
    print(name + ': match')

js = f'db.getSiblingDB("novu-db").integrations.updateMany({{_environmentId:ObjectId("{environment}"),providerId:"novu"}},{{$set:{{"credentials.hmac":false}}}})'
subprocess.check_call(['docker','exec',mongo,'mongosh','--quiet','--eval',js], stdout=subprocess.DEVNULL)
subprocess.check_call(['docker','exec',redis,'redis-cli','FLUSHALL'],stdout=subprocess.DEVNULL)
subprocess.check_call(['docker','exec',postgres,'psql','-U','postgres','-d','novu_compat','-v','ON_ERROR_STOP=1','-c',f"UPDATE novu_rust_inbox_environments SET hmac_required=false WHERE environment_id='{environment}'"],stdout=subprocess.DEVNULL)

context = {'tenant':{'id':context_id,'data':{'plan':'basic','nested':{'count':2}}},'app':'jira'}
body = {'applicationIdentifier':app,'subscriberId':subscriber,'context':context}
session('rich_context',body)
changed = {'tenant':{'id':context_id,'data':{'plan':'changed'}},'app':'jira'}
session('session_does_not_update',dict(body,context=changed))
for base in bases:
    status, value = call(base,'GET','/v2/contexts/tenant/'+context_id,auth='ApiKey '+key)
    assert status == 200 and value['data']['data'] == context['tenant']['data'], (base,status,value)
print('context_data_preserved: match')
# Three synthetic unread messages with exact, partial and empty context sets.
js = f'''const d=db.getSiblingDB("novu-db"), e=ObjectId("{environment}"), s=d.subscribers.findOne({{_environmentId:e,subscriberId:"{subscriber}"}}); d.messages.insertMany([{{_environmentId:e,_subscriberId:s._id,channel:"in_app",read:false,seen:false,archived:false,contextKeys:["app:jira","tenant:{context_id}"],transactionId:"{subscriber}-exact"}},{{_environmentId:e,_subscriberId:s._id,channel:"in_app",read:false,seen:false,archived:false,contextKeys:["tenant:{context_id}"],transactionId:"{subscriber}-partial"}},{{_environmentId:e,_subscriberId:s._id,channel:"in_app",read:false,seen:false,archived:false,contextKeys:[],transactionId:"{subscriber}-empty"}}]);'''
subprocess.check_call(['docker','exec',mongo,'mongosh','--quiet','--eval',js],stdout=subprocess.DEVNULL)
sql = f"INSERT INTO novu_rust_in_app_messages (id,environment_id,subscriber_id,workflow_identifier,transaction_id,content,payload,context_keys) VALUES (md5('{subscriber}-exact')::uuid,'{environment}','{subscriber}','fixture','{subscriber}-exact','x','{{}}',ARRAY['app:jira','tenant:{context_id}']),(md5('{subscriber}-partial')::uuid,'{environment}','{subscriber}','fixture','{subscriber}-partial','x','{{}}',ARRAY['tenant:{context_id}']),(md5('{subscriber}-empty')::uuid,'{environment}','{subscriber}','fixture','{subscriber}-empty','x','{{}}',ARRAY[]::text[])"
subprocess.check_call(['docker','exec',postgres,'psql','-U','postgres','-d','novu_compat','-v','ON_ERROR_STOP=1','-c',sql],stdout=subprocess.DEVNULL)
subprocess.check_call(['docker','exec',redis,'redis-cli','FLUSHALL'],stdout=subprocess.DEVNULL)
session('exact_context_unread',body,unread=1)
session('partial_context_unread',dict(body,context={'tenant':context_id}),unread=1)
session('empty_context_unread',{'applicationIdentifier':app,'subscriberId':subscriber},unread=1)

js = f'db.getSiblingDB("novu-db").integrations.updateMany({{_environmentId:ObjectId("{environment}"),providerId:"novu"}},{{$set:{{"credentials.hmac":true}}}})'
subprocess.check_call(['docker','exec',mongo,'mongosh','--quiet','--eval',js], stdout=subprocess.DEVNULL)
subprocess.check_call(['docker','exec',redis,'redis-cli','FLUSHALL'],stdout=subprocess.DEVNULL)
subprocess.check_call(['docker','exec',postgres,'psql','-U','postgres','-d','novu_compat','-v','ON_ERROR_STOP=1','-c',f"UPDATE novu_rust_inbox_environments SET hmac_required=true WHERE environment_id='{environment}'"],stdout=subprocess.DEVNULL)
digest=lambda value: hmac.new(key.encode(),value.encode(),hashlib.sha256).hexdigest()
subscriber_hash=digest(subscriber)
canonical=json.dumps(context,sort_keys=True,separators=(',',':'),ensure_ascii=False)
session('hmac_missing_context_hash',dict(body,subscriberHash=subscriber_hash),400)
session('hmac_invalid_context_hash',dict(body,subscriberHash=subscriber_hash,contextHash='0'*64),400)
session('hmac_valid_context',dict(body,subscriberHash=subscriber_hash,contextHash=digest(canonical)),unread=1)
large_context = {'tenant': {'id': context_id, 'data': {'large': 1e21, 'rounded': 9007199254740992}}}
large_canonical = json.dumps(large_context, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
session('hmac_large_integer', dict(body, context=large_context, subscriberHash=subscriber_hash, contextHash=digest(large_canonical)), unread=1)
