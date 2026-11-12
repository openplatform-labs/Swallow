#!/usr/bin/env python3
"""Compare workflow severity changed after trigger, before either worker runs."""
import json
import os
import signal
import subprocess
import time
import urllib.request
import uuid

mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
pg=os.environ['NOVU_RUST_PG_CONTAINER']
worker=os.environ['NOVU_REFERENCE_WORKER_CONTAINER']
pid=int(os.environ['NOVU_RUST_WORKER_PID'])
base_js='''const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),w=ObjectId("64a000000000000000000008"),m=ObjectId("64a000000000000000000013");
d.messagetemplates.insertOne({_id:m,_environmentId:e,_organizationId:o,type:"in_app",active:true,content:"before",deleted:false});
d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:o,name:"Queued Severity",active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:["before"],severity:"low",triggers:[{type:"event",identifier:"queued-severity"}],steps:[{active:true,_templateId:m}],createdAt:new Date(),updatedAt:new Date()});
d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:w,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{in_app:{enabled:true}}},deleted:false});'''
def mongo_exec(js):
    return subprocess.check_output(['docker','exec',mongo,'mongosh','--quiet','--eval',js],text=True).strip()
def pg_exec(sql):
    return subprocess.check_output(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-At','-c',sql],text=True).strip()
mongo_exec(base_js)
pg_exec('''INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,inbox_metadata) VALUES ('64a000000000000000000002','queued-severity',true,true,'before','{"id":"64a000000000000000000008","identifier":"queued-severity","name":"Queued Severity","critical":false,"tags":["before"],"severity":"low","payloadAsData":true}'::jsonb); INSERT INTO novu_rust_workflow_preferences(environment_id,workflow_identifier,all_enabled,read_only,in_app_enabled) VALUES ('64a000000000000000000002','queued-severity',true,false,true);''')
ids=['queued-severity-'+uuid.uuid4().hex for _ in range(2)]
def trigger(base,txn,override,name='queued-severity'):
    body={'name':name,'to':{'subscriberId':'queue-severity-user'},'transactionId':txn,'payload':{}}
    if override is not None: body['overrides']={'severity':override}
    req=urllib.request.Request(base+'/v1/events/trigger',data=json.dumps(body).encode(),headers={'Content-Type':'application/json','Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']},method='POST')
    with urllib.request.urlopen(req,timeout=10) as r: assert r.status==201,(r.status,r.read())
paused=False
try:
    subprocess.run(['docker','pause',worker],check=True,stdout=subprocess.DEVNULL)
    paused=True
    os.kill(pid,signal.SIGSTOP)
    for base in (os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']):
        trigger(base,ids[0],None)
        trigger(base,ids[1],'medium')
    assert pg_exec("SELECT count(*) FROM novu_queue_jobs WHERE status='pending' AND payload->>'workflow'='queued-severity'")=='2'
    mongo_exec('db.getSiblingDB("novu-db").notificationtemplates.updateOne({_id:ObjectId("64a000000000000000000008")},{$set:{severity:"high",tags:["after","news"]}})')
    mongo_exec('db.getSiblingDB("novu-db").messagetemplates.updateOne({_id:ObjectId("64a000000000000000000013")},{$set:{content:"after"}})')
    pg_exec("UPDATE novu_rust_workflows SET inbox_metadata=jsonb_set(inbox_metadata,'{tags}', '[\"after\",\"news\"]'::jsonb) WHERE environment_id='64a000000000000000000002' AND identifier='queued-severity'")
    pg_exec("UPDATE novu_rust_workflows SET in_app_content='after' WHERE environment_id='64a000000000000000000002' AND identifier='queued-severity'")
    pg_exec("UPDATE novu_rust_workflows SET inbox_metadata=jsonb_set(inbox_metadata,'{severity}', '\"high\"'::jsonb) WHERE environment_id='64a000000000000000000002' AND identifier='queued-severity'")
finally:
    os.kill(pid,signal.SIGCONT)
    if paused: subprocess.run(['docker','unpause',worker],check=True,stdout=subprocess.DEVNULL)
for txn,expected in zip(ids,['high','medium']):
    for _ in range(60):
        left=mongo_exec(f'const m=db.getSiblingDB("novu-db").messages.findOne({{transactionId:"{txn}"}}); if(m) print(JSON.stringify({{severity:m.severity,content:m.content,tags:m.tags}}));')
        right=pg_exec(f"SELECT json_build_object('severity',severity,'content',content,'tags',tags)::text FROM novu_rust_in_app_messages WHERE environment_id='64a000000000000000000002' AND transaction_id='{txn}'")
        if left and right: break
        time.sleep(.25)
    assert json.loads(left)==json.loads(right)=={'severity':expected,'content':'after','tags':['after','news']},(txn,left,right,expected)
print('queued workflow severity, template, tags and explicit override: match')
for tag,expected_count in [('after',2),('before',0)]:
    def tagged_list(base):
        request=urllib.request.Request(base+'/v2/subscribers/queue-severity-user/notifications?tags='+tag,headers={'Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']})
        with urllib.request.urlopen(request,timeout=10) as response:
            return json.load(response)
    left,right=(tagged_list(os.environ[name]) for name in ('NOVU_REFERENCE_URL','NOVU_RUST_URL'))
    def selected(result):
        return sorted((item['transactionId'],item['body'],item['tags'],item['severity']) for item in result['data'])
    assert len(left['data'])==len(right['data'])==expected_count,(tag,left,right)
    assert selected(left)==selected(right),(tag,left,right)
print('queued workflow tags affect Inbox filters: match')

# Delete one workflow and replace another with the same identifier while the
# subscriber jobs are queued. The original worker binds by template ID.
fixture_js='''const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),m=ObjectId("64a000000000000000000005");
for(const [id,name] of [["64a000000000000000000009","queued-delete"],["64a000000000000000000011","queued-replace"]]) {const w=ObjectId(id);d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:o,name,active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],severity:"low",triggers:[{type:"event",identifier:name}],steps:[{active:true,_templateId:m}],createdAt:new Date(),updatedAt:new Date()});d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:w,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{in_app:{enabled:true}}},deleted:false});}'''
mongo_exec(fixture_js)
pg_exec('''INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,inbox_metadata) VALUES ('64a000000000000000000002','queued-delete',true,true,'hello','{"id":"64a000000000000000000009","severity":"low"}'::jsonb),('64a000000000000000000002','queued-replace',true,true,'hello','{"id":"64a000000000000000000011","severity":"low"}'::jsonb);''')
deleted_tx='queued-delete-'+uuid.uuid4().hex
replaced_tx='queued-replace-'+uuid.uuid4().hex
paused=False
try:
    subprocess.run(['docker','pause',worker],check=True,stdout=subprocess.DEVNULL)
    paused=True
    os.kill(pid,signal.SIGSTOP)
    for base in (os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']):
        trigger(base,deleted_tx,None,'queued-delete')
        trigger(base,replaced_tx,'high','queued-replace')
    # Retain only the reference trigger job on failure so its actual result can
    # be inspected; the disposable Redis job's execution behavior is unchanged.
    lua="local keys=redis.call('KEYS','bull:trigger-handler:*'); for _,k in ipairs(keys) do if redis.call('TYPE',k).ok=='hash' then local d=redis.call('HGET',k,'data'); if d and string.find(d,ARGV[1],1,true) then local opts=cjson.decode(redis.call('HGET',k,'opts')); opts.removeOnFail=false; redis.call('HSET',k,'opts',cjson.encode(opts)); return k end end end return ''"
    reference_job=subprocess.check_output(['docker','exec',os.environ['NOVU_REFERENCE_REDIS_CONTAINER'],'redis-cli','--raw','EVAL',lua,'0',deleted_tx],text=True).strip()
    assert reference_job.startswith('bull:trigger-handler:'),reference_job
    replacement_job=subprocess.check_output(['docker','exec',os.environ['NOVU_REFERENCE_REDIS_CONTAINER'],'redis-cli','--raw','EVAL',lua,'0',replaced_tx],text=True).strip()
    assert replacement_job.startswith('bull:trigger-handler:'),replacement_job
    mongo_exec('''const d=db.getSiblingDB("novu-db"),old=ObjectId("64a000000000000000000011");const replacement=d.notificationtemplates.findOne({_id:old});d.notificationtemplates.deleteMany({_id:{$in:[ObjectId("64a000000000000000000009"),old]}});replacement._id=ObjectId("64a000000000000000000012");replacement.steps[0]._templateId=ObjectId("64a000000000000000000014");d.messagetemplates.insertOne({_id:replacement.steps[0]._templateId,_environmentId:replacement._environmentId,_organizationId:replacement._organizationId,type:"in_app",active:true,content:"replacement",deleted:false});d.notificationtemplates.insertOne(replacement);d.preferences.insertOne({_environmentId:replacement._environmentId,_organizationId:replacement._organizationId,_templateId:replacement._id,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{in_app:{enabled:true}}},deleted:false});''')
    pg_exec('''DELETE FROM novu_rust_workflows WHERE environment_id='64a000000000000000000002' AND identifier IN ('queued-delete','queued-replace'); INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,inbox_metadata) VALUES ('64a000000000000000000002','queued-replace',true,true,'replacement','{"id":"64a000000000000000000012","severity":"low"}'::jsonb);''')
finally:
    os.kill(pid,signal.SIGCONT)
    if paused: subprocess.run(['docker','unpause',worker],check=True,stdout=subprocess.DEVNULL)
for _ in range(80):
    state=pg_exec(f"SELECT status FROM novu_queue_jobs WHERE environment_id='64a000000000000000000002' AND payload->>'transactionId'='{deleted_tx}'")
    reason=subprocess.check_output(['docker','exec',os.environ['NOVU_REFERENCE_REDIS_CONTAINER'],'redis-cli','--raw','HGET',reference_job,'failedReason'],text=True).strip()
    if state=='failed' and reason: break
    time.sleep(.25)
assert state=='failed' and 'Notification template could not be found' in reason,(state,reason)
assert mongo_exec(f'print(db.getSiblingDB("novu-db").messages.countDocuments({{transactionId:"{deleted_tx}"}}))')=='0'
assert pg_exec(f"SELECT count(*) FROM novu_rust_in_app_messages WHERE environment_id='64a000000000000000000002' AND transaction_id='{deleted_tx}'")=='0'
for _ in range(80):
    left=mongo_exec(f'const m=db.getSiblingDB("novu-db").messages.findOne({{transactionId:"{replaced_tx}"}}); if(m) print(JSON.stringify({{severity:m.severity,content:m.content}}));')
    right=pg_exec(f"SELECT json_build_object('severity',severity,'content',content)::text FROM novu_rust_in_app_messages WHERE environment_id='64a000000000000000000002' AND transaction_id='{replaced_tx}'")
    if left and right: break
    time.sleep(.25)
replacement_reason=subprocess.check_output(['docker','exec',os.environ['NOVU_REFERENCE_REDIS_CONTAINER'],'redis-cli','--raw','HGET',replacement_job,'failedReason'],text=True).strip()
assert json.loads(left)==json.loads(right)=={'severity':'high','content':'replacement'},(left,right,replacement_reason)
print('queued deleted workflow fails; replacement content delivers on both workers')

# Changes after trigger: workflow-level active/draft is an API gate at
# acceptance; step activation and step existence are evaluated by the worker.
state_js='''const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),m=ObjectId("64a000000000000000000005");
for(const [id,name] of [["64a000000000000000000015","queued-inactive-workflow"],["64a000000000000000000016","queued-inactive-step"],["64a000000000000000000017","queued-no-steps"]]) {const w=ObjectId(id); d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:o,name,active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],triggers:[{type:"event",identifier:name}],steps:[{active:true,_templateId:m}],createdAt:new Date(),updatedAt:new Date()}); d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:w,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{in_app:{enabled:true}}},deleted:false});}'''
mongo_exec(state_js)
pg_exec('''INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,inbox_metadata) VALUES ('64a000000000000000000002','queued-inactive-workflow',true,true,'hello','{"id":"64a000000000000000000015"}'::jsonb),('64a000000000000000000002','queued-inactive-step',true,true,'hello','{"id":"64a000000000000000000016"}'::jsonb),('64a000000000000000000002','queued-no-steps',true,true,'hello','{"id":"64a000000000000000000017"}'::jsonb);''')
scenarios=[(name,name+'-'+uuid.uuid4().hex,expected) for name,expected in [('queued-inactive-workflow',1),('queued-inactive-step',0),('queued-no-steps',0)]]
paused=False
try:
    subprocess.run(['docker','pause',worker],check=True,stdout=subprocess.DEVNULL)
    paused=True
    os.kill(pid,signal.SIGSTOP)
    for base in (os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']):
        for name,txn,_ in scenarios: trigger(base,txn,None,name)
    mongo_exec('''const d=db.getSiblingDB("novu-db"); d.notificationtemplates.updateOne({_id:ObjectId("64a000000000000000000015")},{$set:{active:false,draft:true}}); d.notificationtemplates.updateOne({_id:ObjectId("64a000000000000000000016")},{$set:{"steps.0.active":false}}); d.notificationtemplates.updateOne({_id:ObjectId("64a000000000000000000017")},{$set:{steps:[]}});''')
    pg_exec('''UPDATE novu_rust_workflows SET active=false WHERE environment_id='64a000000000000000000002' AND identifier='queued-inactive-workflow'; UPDATE novu_rust_workflows SET in_app_step_active=false WHERE environment_id='64a000000000000000000002' AND identifier='queued-inactive-step'; UPDATE novu_rust_workflows SET has_steps=false WHERE environment_id='64a000000000000000000002' AND identifier='queued-no-steps';''')
finally:
    os.kill(pid,signal.SIGCONT)
    if paused: subprocess.run(['docker','unpause',worker],check=True,stdout=subprocess.DEVNULL)
for name,txn,expected in scenarios:
    for _ in range(80):
        original_notification=mongo_exec(f'print(db.getSiblingDB("novu-db").notifications.countDocuments({{transactionId:"{txn}"}}))')
        state=pg_exec(f"SELECT status FROM novu_queue_jobs WHERE environment_id='64a000000000000000000002' AND payload->>'transactionId'='{txn}'")
        if original_notification=='1' and state=='completed': break
        time.sleep(.25)
    assert original_notification=='1' and state=='completed',(name,original_notification,state)
    for _ in range(40):
        original_count=int(mongo_exec(f'print(db.getSiblingDB("novu-db").messages.countDocuments({{transactionId:"{txn}"}}))'))
        rust_count=int(pg_exec(f"SELECT count(*) FROM novu_rust_in_app_messages WHERE environment_id='64a000000000000000000002' AND transaction_id='{txn}'"))
        if original_count==expected and rust_count==expected: break
        time.sleep(.25)
    assert original_count==rust_count==expected,(name,original_count,rust_count,expected)
    source=mongo_exec(f'const n=db.getSiblingDB("novu-db").notifications.findOne({{transactionId:"{txn}"}}); print(JSON.stringify({{id:n._id,organization:n._organizationId,workflow:n._templateId,subscriber:n._subscriberId,to:n.to,payload:n.payload??{{}},channels:n.channels,tags:n.tags,severity:n.severity,critical:n.critical,contextKeys:n.contextKeys??[]}}));')
    stored=pg_exec(f"SELECT json_build_object('id',internal_id,'organization',organization_id,'workflow',workflow_internal_id,'subscriber',subscriber_internal_id,'to',recipient,'payload',payload,'channels',channels,'tags',tags,'severity',severity,'critical',critical,'contextKeys',context_keys)::text FROM novu_rust_notifications WHERE environment_id='64a000000000000000000002' AND transaction_id='{txn}'")
    original,actual=json.loads(source),json.loads(stored)
    for item in (original,actual):
        assert len(item.pop('id'))==24
        assert len(item.pop('subscriber'))==24
    assert original==actual,(name,original,actual)
print('queued workflow active/draft, inactive step and empty steps: match')
print('per-recipient notification fields and no-step records: match')
# Verify message-to-notification identity, then the reference's historical
# notification retention after subscriber deletion.
active_tx=scenarios[0][1]
source_link=mongo_exec(f'''const d=db.getSiblingDB("novu-db"),n=d.notifications.findOne({{transactionId:"{active_tx}"}}),m=d.messages.findOne({{transactionId:"{active_tx}"}});print(!!n && !!m && m._notificationId.equals(n._id));''')
rust_link=pg_exec(f"SELECT count(*) FROM novu_rust_in_app_messages m JOIN novu_rust_notifications n ON n.internal_id=m.notification_internal_id AND n.environment_id=m.environment_id WHERE m.environment_id='64a000000000000000000002' AND m.transaction_id='{active_tx}' AND n.transaction_id=m.transaction_id")
assert source_link=='true' and rust_link=='1',(source_link,rust_link)
before_source=int(mongo_exec('print(db.getSiblingDB("novu-db").notifications.countDocuments({"to.subscriberId":"queue-severity-user"}))'))
before_rust=int(pg_exec("SELECT count(*) FROM novu_rust_notifications WHERE environment_id='64a000000000000000000002' AND subscriber_id='queue-severity-user'"))
assert before_source==before_rust and before_source>0,(before_source,before_rust)
for base in (os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']):
    req=urllib.request.Request(base+'/v2/subscribers/queue-severity-user',method='DELETE',headers={'Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']})
    with urllib.request.urlopen(req,timeout=10) as response: assert response.status in (200,204),response.status
after_source=int(mongo_exec('print(db.getSiblingDB("novu-db").notifications.countDocuments({"to.subscriberId":"queue-severity-user"}))'))
after_rust=int(pg_exec("SELECT count(*) FROM novu_rust_notifications WHERE environment_id='64a000000000000000000002' AND subscriber_id='queue-severity-user'"))
assert after_source==before_source and after_rust==before_rust,(before_source,after_source,before_rust,after_rust)
assert mongo_exec(f'print(db.getSiblingDB("novu-db").messages.countDocuments({{transactionId:"{active_tx}"}}))')=='0'
assert pg_exec(f"SELECT count(*) FROM novu_rust_in_app_messages WHERE environment_id='64a000000000000000000002' AND transaction_id='{active_tx}'")=='0'
print('message notification linkage and subscriber deletion retention: match')
