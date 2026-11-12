#!/usr/bin/env python3
"""Compare severity count filters against real Novu in disposable databases."""
import json
import os
import subprocess
import urllib.parse
import urllib.request

mongo = os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
postgres = os.environ['NOVU_RUST_PG_CONTAINER']
redis = os.environ['NOVU_REFERENCE_REDIS_CONTAINER']
js = '''const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),s=ObjectId("64a000000000000000000010");
const w=ObjectId("64a000000000000000000004");
d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:ObjectId("64a000000000000000000003"),name:"Severity",critical:false,type:"REGULAR",tags:[],severity:"high",triggers:[{type:"event",identifier:"fixture"}]});
const base={_environmentId:e,_subscriberId:s,_templateId:w,channel:"in_app",content:"hello",read:false,seen:false,archived:false,tags:["news"],cta:{action:{buttons:[]}},createdAt:new Date("2024-01-01T00:00:00Z")};
d.messages.insertMany([{...base},{...base,severity:null},{...base,severity:"none"},{...base,severity:"low"},{...base,severity:"medium",read:true},{...base,severity:"high"}].map((m,i)=>({...m,_id:ObjectId((i+1).toString().padStart(24,"0")),transactionId:"severity-"+(i+1)})));'''
subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval',js],check=True,stdout=subprocess.DEVNULL)
sql = """INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,inbox_metadata) VALUES ('64a000000000000000000002','fixture',true,true,'{"id":"64a000000000000000000004","identifier":"fixture","name":"Severity","critical":false,"tags":[],"severity":"high","payloadAsData":true}'::jsonb);
INSERT INTO novu_rust_in_app_messages (id,internal_id,environment_id,subscriber_id,workflow_identifier,transaction_id,content,payload,tags,is_read,severity,severity_present,created_at)
SELECT gen_random_uuid(),lpad(n::text,24,'0'),'64a000000000000000000002','alice','fixture','severity-'||n,'hello','{}','["news"]'::jsonb,n=5,
CASE n WHEN 3 THEN 'none' WHEN 4 THEN 'low' WHEN 5 THEN 'medium' WHEN 6 THEN 'high' END,n<>1,'2024-01-01T00:00:00Z' FROM generate_series(1,6) n"""
subprocess.run(['docker','exec',postgres,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',sql],check=True,stdout=subprocess.DEVNULL)
subprocess.run(['docker','exec',redis,'redis-cli','FLUSHALL'],check=True,stdout=subprocess.DEVNULL)
filters=[{}, {'severity':'none'}, {'severity':'low'}, {'severity':'medium'}, {'severity':'high'}, {'severity':['none','high']}, {'severity':[]}, {'severity':None}, {'severity':['low','medium'], 'read':False}, {'severity':['none','low'], 'tags':['news']}, {'contextKeys':['tenant:missing']}, {'contextKeys':[]}, {'contextKeys':None}, {'contextKeys':['tenant:missing'], 'severity':'high'}]
query=urllib.parse.urlencode({'filters':json.dumps(filters,separators=(',',':'))})
def request(base):
    req=urllib.request.Request(base+'/v2/subscribers/alice/notifications/count?'+query,headers={'Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']})
    with urllib.request.urlopen(req,timeout=10) as response:
        return response.status,json.load(response)
left,right=(request(os.environ[name]) for name in ('NOVU_REFERENCE_URL','NOVU_RUST_URL'))
assert left==right,(left,right)
assert [item['count'] for item in left[1]['data']]==[6,2,1,1,1,3,6,6,1,3,6,6,6,1],left
print('severity counts and filter contextKeys: match')

# The list DTO must return message severity separately from workflow severity.
for params in [[], [('severity','none')], [('severity','high')], [('severity','low'),('severity','medium')], [('severity','none'),('severity','high'),('read','false')]]:
    path='/v2/subscribers/alice/notifications?'+urllib.parse.urlencode(params)
    def get_list(base):
        req=urllib.request.Request(base+path,headers={'Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']})
        with urllib.request.urlopen(req,timeout=10) as response:
            return json.load(response)
    left,right=(get_list(os.environ[name]) for name in ('NOVU_REFERENCE_URL','NOVU_RUST_URL'))
    assert left==right,(params,left,right)
    assert all(item['workflow']['severity']=='high' for item in left['data'])
    assert all(item['severity'] in ('none','low','medium','high') for item in left['data'])
print('severity list filters and message/workflow DTOs: match')

# Individual mutation responses use the same DTO and may have dynamic times.
path='/v2/subscribers/alice/notifications/000000000000000000000006/read'
def read_one(base):
    req=urllib.request.Request(base+path,data=b'{}',method='PATCH',headers={'Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY'],'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=10) as response:
        return response.status,json.load(response)
left,right=(read_one(os.environ[name]) for name in ('NOVU_REFERENCE_URL','NOVU_RUST_URL'))
for response in (left,right):
    for field in ('readAt','firstSeenAt'):
        if response[1]['data'].get(field) is not None:
            assert response[1]['data'][field].endswith('Z')
            response[1]['data'][field]='<timestamp>'
assert left==right,(left,right)
assert left[1]['data']['severity']=='high' and left[1]['data']['workflow']['severity']=='high'
print('severity individual read response: match')

# Management list uses exact, order-independent context-key matching. The
# response filter intentionally omits contextKeys, as in the original API.
subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval', '''const d=db.getSiblingDB("novu-db"); d.messages.updateOne({transactionId:"severity-4"},{$set:{contextKeys:["tenant:a"]}}); d.messages.updateOne({transactionId:"severity-6"},{$set:{contextKeys:["tenant:a","team:b"]}});'''],check=True,stdout=subprocess.DEVNULL)
subprocess.run(['docker','exec',postgres,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c', "UPDATE novu_rust_in_app_messages SET context_keys=CASE transaction_id WHEN 'severity-4' THEN ARRAY['tenant:a'] WHEN 'severity-6' THEN ARRAY['tenant:a','team:b'] ELSE context_keys END WHERE transaction_id IN ('severity-4','severity-6')"],check=True,stdout=subprocess.DEVNULL)
for params, expected in [([('contextKeys','tenant:a')], ['severity-4']), ([('contextKeys','team:b'),('contextKeys','tenant:a')], ['severity-6']), ([('contextKeys','tenant:a'),('contextKeys','tenant:a')], ['severity-6']), ([('contextKeys','tenant:missing')], []), ([('contextKeys','tenant:a'),('severity','low')], ['severity-4'])]:
    path='/v2/subscribers/alice/notifications?'+urllib.parse.urlencode(params)
    def get_context_list(base):
        req=urllib.request.Request(base+path,headers={'Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']})
        with urllib.request.urlopen(req,timeout=10) as response: return json.load(response)
    left,right=(get_context_list(os.environ[name]) for name in ('NOVU_REFERENCE_URL','NOVU_RUST_URL'))
    for response in (left,right):
        for item in response['data']:
            for field in ('readAt','firstSeenAt'):
                if item.get(field) is not None:
                    assert item[field].endswith('Z')
                    item[field]='<timestamp>'
    assert left==right,(params,left,right)
    assert [item['transactionId'] for item in left['data']]==expected,(params,left)
    assert 'contextKeys' not in left['filter'],left
print('notification list exact context-key filters: match')
