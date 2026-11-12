#!/usr/bin/env python3
"""Compare unfiltered bulk state transitions on disposable Novu and Rust APIs.

Requires NOVU_REFERENCE_URL, NOVU_RUST_URL, NOVU_TEST_API_KEY,
NOVU_REFERENCE_MONGO_CONTAINER, NOVU_RUST_PG_CONTAINER and
NOVU_TEST_ENVIRONMENT_ID. Creates synthetic subscribers and messages in both
databases. This does not compare webhook, Socket.IO, analytics or traces.
"""
import datetime
import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
import uuid

reference = os.environ['NOVU_REFERENCE_URL'].rstrip('/')
rust = os.environ['NOVU_RUST_URL'].rstrip('/')
key = os.environ['NOVU_TEST_API_KEY']
mongo = os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
postgres = os.environ['NOVU_RUST_PG_CONTAINER']
environment = os.environ['NOVU_TEST_ENVIRONMENT_ID']
assert len(environment) == 24 and all(c in '0123456789abcdef' for c in environment)
subscriber = 'compat-' + uuid.uuid4().hex
marker = uuid.uuid4().hex

def request(base, method, path, body=None, auth='valid'):
    headers = {'Content-Type': 'application/json'}
    if auth != 'missing':
        headers['Authorization'] = 'ApiKey ' + (key if auth == 'valid' else 'invalid')
    req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(), headers=headers, method=method)
    try:
        response = urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        status, raw = response.status, response.read()
    result = json.loads(raw) if raw else None
    if status >= 400:
        stamp = result.pop('timestamp')
        assert stamp.endswith('Z')
        datetime.datetime.fromisoformat(stamp.replace('Z', '+00:00'))
    return status, result

for base in (reference, rust):
    status, _ = request(base, 'POST', '/v2/subscribers', {'subscriberId': subscriber})
    assert status == 201

# All interpolated identifiers are generated UUID hex or validated environment IDs.
js = f'''const d=db.getSiblingDB("novu-db"),e=ObjectId("{environment}"),s=d.subscribers.findOne({{_environmentId:e,subscriberId:"{subscriber}"}}),old=ISODate("2024-01-02T03:04:05.123Z"); d.messages.insertMany([{{_environmentId:e,_subscriberId:s._id,channel:"in_app",transactionId:"{marker}-a",read:false,seen:false,archived:false}},{{_environmentId:e,_subscriberId:s._id,channel:"in_app",transactionId:"{marker}-b",read:true,seen:true,archived:false,firstSeenDate:old}},{{_environmentId:e,_subscriberId:s._id,channel:"in_app",transactionId:"{marker}-c",read:true,seen:true,archived:true,firstSeenDate:old}}]);'''
subprocess.check_call(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], stdout=subprocess.DEVNULL)
sql = f"INSERT INTO novu_rust_in_app_messages (id,environment_id,subscriber_id,workflow_identifier,transaction_id,content,payload,is_read,is_seen,is_archived,first_seen_at) VALUES ('{uuid.uuid4()}','{environment}','{subscriber}','fixture','{marker}-a','x','{{}}',false,false,false,NULL),('{uuid.uuid4()}','{environment}','{subscriber}','fixture','{marker}-b','x','{{}}',true,true,false,'2024-01-02T03:04:05.123Z'),('{uuid.uuid4()}','{environment}','{subscriber}','fixture','{marker}-c','x','{{}}',true,true,true,'2024-01-02T03:04:05.123Z')"
subprocess.check_call(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-v', 'ON_ERROR_STOP=1', '-c', sql], stdout=subprocess.DEVNULL)

def state():
    js = f'''const d=db.getSiblingDB("novu-db"),rows=d.messages.find({{transactionId:{{$in:["{marker}-a","{marker}-b","{marker}-c"]}}}}).sort({{transactionId:1}}).toArray(); print(JSON.stringify(rows.map(m=>({{read:m.read,seen:m.seen,archived:m.archived,first:!!m.firstSeenDate,lastSeen:!!m.lastSeenDate,lastRead:!!m.lastReadDate,archivedAt:!!m.archivedAt,oldFirst:!!(m.firstSeenDate && m.firstSeenDate.toISOString()==="2024-01-02T03:04:05.123Z")}}))));'''
    sql = f"SELECT json_agg(json_build_object('read',is_read,'seen',is_seen,'archived',is_archived,'first',first_seen_at IS NOT NULL,'lastSeen',last_seen_at IS NOT NULL,'lastRead',last_read_at IS NOT NULL,'archivedAt',archived_at IS NOT NULL,'oldFirst',COALESCE(first_seen_at = '2024-01-02T03:04:05.123Z'::timestamptz,false)) ORDER BY transaction_id)::text FROM novu_rust_in_app_messages WHERE transaction_id IN ('{marker}-a','{marker}-b','{marker}-c')"
    left = json.loads(subprocess.check_output(['docker', 'exec', mongo, 'mongosh', '--quiet', '--eval', js], text=True))
    right = json.loads(subprocess.check_output(['docker', 'exec', postgres, 'psql', '-U', 'postgres', '-d', 'novu_compat', '-At', '-c', sql], text=True))
    return left, right

for action, expected_archived in [('read-archive', 2), ('read', 2), ('archive', 3)]:
    for base in (reference, rust):
        assert request(base, 'POST', f'/v2/subscribers/{subscriber}/notifications/{action}', {}) == (204, None)
    filters = [{'read': False}, {'read': True}, {'seen': True}, {'archived': True}]
    query = urllib.parse.urlencode({'filters': json.dumps(filters, separators=(',', ':'))})
    left, right = (request(base, 'GET', f'/v2/subscribers/{subscriber}/notifications/count?{query}') for base in (reference, rust))
    assert left == right, (action, left, right)
    assert left[1]['data'][3]['count'] == expected_archived
    left_state, right_state = state()
    assert left_state == right_state, (action, left_state, right_state)
    assert left_state[1]['oldFirst'] and left_state[2]['oldFirst']
    print(f'{action}: response, counts and stored state match')

for name, sub, auth in [('missing_subscriber', 'missing', 'valid'), ('missing_auth', subscriber, 'missing'), ('invalid_key', subscriber, 'invalid')]:
    left, right = (request(base, 'POST', f'/v2/subscribers/{sub}/notifications/read', {}, auth) for base in (reference, rust))
    assert left == right, (name, left, right)
    print(f'{name}: match')

# Context filters use overlap for management bulk operations, not exact match.
def reset_context_fixture():
    js=f'''const d=db.getSiblingDB("novu-db");for (const [s,keys] of [["a",["tenant:a"]],["b",["tenant:a","team:b"]],["c",["team:b"]]]) d.messages.updateOne({{transactionId:"{marker}-"+s}},{{$set:{{read:true,seen:false,archived:false,contextKeys:keys}}}});'''
    sql=f"UPDATE novu_rust_in_app_messages SET is_read=true,is_seen=false,is_archived=false,context_keys=CASE right(transaction_id,1) WHEN 'a' THEN ARRAY['tenant:a'] WHEN 'b' THEN ARRAY['tenant:a','team:b'] ELSE ARRAY['team:b'] END WHERE transaction_id LIKE '{marker}-%'"
    subprocess.check_call(['docker','exec',mongo,'mongosh','--quiet','--eval',js],stdout=subprocess.DEVNULL)
    subprocess.check_call(['docker','exec',postgres,'psql','-U','postgres','-d','novu_compat','-v','ON_ERROR_STOP=1','-c',sql],stdout=subprocess.DEVNULL)

def context_state():
    js=f'''const d=db.getSiblingDB("novu-db");print(JSON.stringify(d.messages.find({{transactionId:{{$regex:"^{marker}-"}}}}).sort({{transactionId:1}}).toArray().map(x=>({{transactionId:x.transactionId,read:x.read,seen:x.seen,archived:x.archived}}))));'''
    sql=f"SELECT coalesce(json_agg(json_build_object('transactionId',transaction_id,'read',is_read,'seen',is_seen,'archived',is_archived) ORDER BY transaction_id),'[]'::json)::text FROM novu_rust_in_app_messages WHERE transaction_id LIKE '{marker}-%'"
    return (json.loads(subprocess.check_output(['docker','exec',mongo,'mongosh','--quiet','--eval',js],text=True)),json.loads(subprocess.check_output(['docker','exec',postgres,'psql','-U','postgres','-d','novu_compat','-At','-c',sql],text=True)))

for action,keys,expected in [('read',['tenant:a'],[True,True,False]),('seen',['tenant:a'],[True,True,False]),('archive',['team:b'],[False,True,True]),('read-archive',['tenant:a'],[True,True,False]),('seen',[],[True,True,True])]:
    reset_context_fixture()
    for base in (reference,rust): assert request(base,'POST',f'/v2/subscribers/{subscriber}/notifications/{action}',{'contextKeys':keys})==(204,None)
    left,right=context_state()
    assert left==right,(action,keys,left,right)
    field='archived' if action in ('archive','read-archive') else 'seen'
    assert [row[field] for row in left]==expected,(action,keys,left)
    print(f'bulk {action} context overlap: match')
reset_context_fixture()
js=f'''const d=db.getSiblingDB("novu-db");print(JSON.stringify(d.messages.find({{transactionId:{{$in:["{marker}-b","{marker}-c"]}}}}).sort({{transactionId:1}}).toArray().map(m=>String(m._id))));'''
sql=f"SELECT json_agg(internal_id ORDER BY transaction_id)::text FROM novu_rust_in_app_messages WHERE transaction_id IN ('{marker}-b','{marker}-c')"
ids=[json.loads(subprocess.check_output(['docker','exec',mongo,'mongosh','--quiet','--eval',js],text=True)),json.loads(subprocess.check_output(['docker','exec',postgres,'psql','-U','postgres','-d','novu_compat','-At','-c',sql],text=True))]
for base,selected in zip((reference,rust),ids): assert request(base,'POST',f'/v2/subscribers/{subscriber}/notifications/seen',{'contextKeys':['tenant:a'],'notificationIds':selected})==(204,None)
left,right=context_state()
assert left==right and [row['seen'] for row in left]==[False,True,False],(left,right)
print('bulk seen IDs and context intersection: match')
reset_context_fixture()
for base in (reference,rust): assert request(base,'POST',f'/v2/subscribers/{subscriber}/notifications/delete',{'contextKeys':['tenant:a']})==(204,None)
left,right=context_state()
assert left==right and [row['transactionId'] for row in left]==[f'{marker}-c'],(left,right)
print('bulk delete context overlap: match')
