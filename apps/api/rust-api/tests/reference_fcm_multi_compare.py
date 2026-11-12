#!/usr/bin/env python3
"""Compare per-integration FCM subscriber channels on disposable servers.

The reference sends to each active integration. Rust requires explicit distinct
provider credentials for multi-integration delivery.
"""
import json
import os
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

mongo,pg,worker=(os.environ[k] for k in ('NOVU_REFERENCE_MONGO_CONTAINER','NOVU_RUST_PG_CONTAINER','NOVU_REFERENCE_WORKER_CONTAINER'))
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
env='64a000000000000000000002'
first='64a000000000000000000071';second='64a000000000000000000072'
subscriber='multi-'+uuid.uuid4().hex
def run(*args):return subprocess.check_output(args,text=True).strip()
def request(base,method,path,body=None):
    req=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers={'Content-Type':'application/json','Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']},method=method)
    try:response=urllib.request.urlopen(req,timeout=10)
    except urllib.error.HTTPError as error:response=error
    with response:
        raw=response.read();return response.status,json.loads(raw) if raw else None
run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db"),e=ObjectId("{env}"),o=ObjectId("64a000000000000000000003");for(const [id,identifier,date] of [["{first}","fcm-first","2024-01-01T00:00:00Z"],["{second}","fcm-second","2024-01-02T00:00:00Z"]])d.integrations.insertOne({{_id:ObjectId(id),_environmentId:e,_organizationId:o,identifier,providerId:"fcm",channel:"push",active:true,primary:true,credentials:{{serviceAccount:JSON.stringify({{project_id:"demo",client_email:"fixture@example.test",private_key:"fixture"}})}},createdAt:new Date(date),deleted:false}});const w=ObjectId("64a000000000000000000073"),m=ObjectId("64a000000000000000000074");d.messagetemplates.insertOne({{_id:m,_environmentId:e,_organizationId:o,type:"push",active:true,title:"Hi",content:"Hello",deleted:false}});d.notificationtemplates.insertOne({{_id:w,_environmentId:e,_organizationId:o,name:"Multi",active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],triggers:[{{type:"event",identifier:"push-multi"}}],steps:[{{active:true,_templateId:m}}],createdAt:new Date(),updatedAt:new Date()}});d.preferences.insertOne({{_environmentId:e,_organizationId:o,_templateId:w,type:"WORKFLOW_RESOURCE",preferences:{{all:{{enabled:true,readOnly:false}},channels:{{push:{{enabled:true}}}}}},deleted:false}});''')
run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',f'''INSERT INTO novu_rust_fcm_integrations(environment_id,internal_id,identifier,active,created_at) VALUES ('{env}','{first}','fcm-first',true,'2024-01-01T00:00:00Z'),('{env}','{second}','fcm-second',true,'2024-01-02T00:00:00Z');INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,push_title,push_content,inbox_metadata) VALUES ('{env}','push-multi',true,true,'Hi','Hello','{{"id":"64a000000000000000000073"}}');''')
for base in bases:assert request(base,'POST','/v2/subscribers',{'subscriberId':subscriber})[0]==201
def apply(method,identifier,devices,expected):
    body={'providerId':'fcm','credentials':{'deviceTokens':devices}}
    if identifier is not None:body['integrationIdentifier']=identifier
    outputs=[]
    for base in bases:
        status,response=request(base,method,'/v1/subscribers/'+subscriber+'/credentials',body)
        assert status==200,(base,status,response)
        outputs.append(response['data']['channels'])
    assert outputs[0]==outputs[1]==expected,(method,identifier,outputs,expected)
def channel(identity,devices):return {'providerId':'fcm','_integrationId':identity,'credentials':{'deviceTokens':devices}}
apply('PUT','fcm-first',['first-a'],[channel(first,['first-a'])])
apply('PUT','fcm-second',['second-a'],[channel(first,['first-a']),channel(second,['second-a'])])
apply('PATCH','fcm-first',['first-b','first-a'],[channel(first,['first-a','first-b']),channel(second,['second-a'])])
apply('PUT',None,['second-b'],[channel(first,['first-a','first-b']),channel(second,['second-b'])])
expected=[channel(first,['first-a','first-b']),channel(second,['second-b'])]
for base in bases:
    for path in ['/v1/subscribers/'+subscriber,'/v2/subscribers/'+subscriber,'/v2/subscribers?subscriberId='+subscriber]:
        status,response=request(base,'GET',path)
        rows=response['data'] if isinstance(response['data'],list) else [response['data']]
        assert status==200 and len(rows)==1 and rows[0]['channels']==expected,(base,path,response)
print('multiple FCM credentials, selection and reads matched')

# The 100-device limit applies independently to each integration channel.
limit_user='multi-limit-'+uuid.uuid4().hex
limit_tokens=['device-'+str(i) for i in range(100)]
for base in bases:
    assert request(base,'POST','/v2/subscribers',{'subscriberId':limit_user})[0]==201
    for identifier in ('fcm-first','fcm-second'):
        status,body=request(base,'PUT','/v1/subscribers/'+limit_user+'/credentials',{'providerId':'fcm','integrationIdentifier':identifier,'credentials':{'deviceTokens':limit_tokens}})
        assert status==200,(base,identifier,status,body)
limits=[]
for base in bases:
    status,body=request(base,'PATCH','/v1/subscribers/'+limit_user+'/credentials',{'providerId':'fcm','integrationIdentifier':'fcm-first','credentials':{'deviceTokens':['device-100']}})
    assert status==400,(base,status,body)
    body.pop('timestamp',None);limits.append(body)
    status,body=request(base,'GET','/v2/subscribers/'+limit_user)
    assert status==200 and body['data']['channels']==[channel(first,limit_tokens),channel(second,limit_tokens)],(base,body)
assert limits[0]==limits[1] and limits[0]['currentCount']==101,limits
print('FCM device limit applies independently per integration: match')

# Exercise the missing delivery behavior explicitly: the source sends to both
# devices, whereas the Rust worker refuses to use one provider for both IDs.
for base in bases:
    status,response=request(base,'POST','/v1/events/trigger',{'name':'push-multi','to':subscriber,'transactionId':subscriber})
    assert status==201,(base,status,response)
for _ in range(90):
    job=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT push_job_id FROM novu_rust_push_step_outcomes WHERE transaction_id='{subscriber}' AND status='queued'")
    log=subprocess.run(['docker','exec',worker,'cat','/tmp/novu-push-capture.jsonl'],capture_output=True,text=True).stdout.splitlines()
    sent=[json.loads(line)['tokens'][0] for line in log if json.loads(line).get('tokens',[None])[0] in ('first-a','first-b','second-b')]
    if job and len(sent)==3:break
    time.sleep(.5)
else:raise AssertionError((job,sent))
with tempfile.TemporaryDirectory(prefix='novu-multi-fcm-') as directory:
    capture=Path(directory)/'capture.json'
    subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_push','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_PUSH_CAPTURE_PATH=str(capture),TEST_PUSH_EXPECTED_COUNT='0'),check=True)
    assert json.loads(capture.read_text())==[]
result=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT status FROM novu_queue_jobs WHERE id='{job}'")
assert sorted(sent)==['first-a','first-b','second-b'] and result=='failed',(sent,result)
print('multi-integration delivery: source sent three targets; Rust safely rejected the unsupported routing')

transaction=subscriber+'-mapped'
for base in bases:
    status,response=request(base,'POST','/v1/events/trigger',{'name':'push-multi','to':subscriber,'transactionId':transaction})
    assert status==201,(base,status,response)
for _ in range(90):
    job=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT push_job_id FROM novu_rust_push_step_outcomes WHERE transaction_id='{transaction}' AND status='queued'")
    log=subprocess.run(['docker','exec',worker,'cat','/tmp/novu-push-capture.jsonl'],capture_output=True,text=True).stdout.splitlines()
    requests=[json.loads(line) for line in log if json.loads(line).get('tokens',[None])[0] in ('first-a','first-b','second-b')]
    if job and len(requests)>=6:break
    time.sleep(.5)
else:raise AssertionError((job,len(requests)))
reference=requests[-3:]
with tempfile.TemporaryDirectory(prefix='novu-multi-fcm-') as directory:
    config={first:'first-fixture-secret',second:'second-fixture-secret'}
    actual=[];credentials=[]
    # Restart the worker between target one and the remaining two targets to
    # verify persisted integration routing and channel-token snapshots.
    for stage,count in enumerate((1,2)):
        capture=Path(directory)/f'capture-{stage}.json';auth=Path(directory)/f'auth-{stage}.json'
        subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_push','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_PUSH_CAPTURE_PATH=str(capture),TEST_PUSH_AUTH_CAPTURE_PATH=str(auth),TEST_PUSH_EXPECTED_COUNT=str(count),TEST_PUSH_INTEGRATIONS=json.dumps(config)),check=True)
        actual.extend(json.loads(capture.read_text()));credentials.extend(json.loads(auth.read_text()))
assert [item['message']['token'] for item in actual]==['first-a','first-b','second-b'],actual
assert credentials==['Bearer first-fixture-secret','Bearer first-fixture-secret','Bearer second-fixture-secret'],credentials
for original,rust in zip(reference,actual):
    original['token']=original.pop('tokens')[0]
    original['data'].pop('__nvMessageId');rust['message']['data'].pop('__nvMessageId')
    assert original==rust['message'],(original,rust)
result=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT status FROM novu_queue_jobs WHERE id='{job}'")
assert result=='completed',result
source_history=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db");print(JSON.stringify(d.messages.find({{transactionId:"{transaction}"}}).toArray().map(m=>({{deviceTokens:m.deviceTokens,status:m.status}}))));'''))
rust_history=json.loads(run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT COALESCE(json_agg(json_build_object('deviceTokens',device_tokens,'status',status)),'[]'::json)::text FROM novu_rust_push_messages WHERE transaction_id='{transaction}'"))
sort=lambda items:sorted(items,key=lambda item:json.dumps(item,sort_keys=True))
assert sort(source_history)==sort(rust_history)==sort([{'deviceTokens':['first-a','first-b'],'status':'sent'},{'deviceTokens':['first-a','first-b'],'status':'sent'},{'deviceTokens':['second-b'],'status':'sent'}]),(source_history,rust_history)
print('multi-integration requests match; Rust uses the configured test credential for each integration')

# A provider-wide token is sent once per populated channel, while a topic or
# a subscriber with no channels uses the most recently created integration.
for label,target_user,route_key,count,expected_auth in [
    ('token-channels',subscriber,'token',2,['first-fixture-secret','second-fixture-secret']),
    ('topic-broadcast',subscriber,'topic',1,['second-fixture-secret']),
    ('token-no-channel','route-empty-'+uuid.uuid4().hex,'token',1,['second-fixture-secret']),
]:
    if label=='token-no-channel':
        for base in bases:assert request(base,'POST','/v2/subscribers',{'subscriberId':target_user})[0]==201
    destination='multi-route-'+uuid.uuid4().hex
    transaction='route-'+uuid.uuid4().hex
    event={'name':'push-multi','to':target_user,'transactionId':transaction,'overrides':{'providers':{'fcm':{route_key:destination}}}}
    for base in bases:
        status,response=request(base,'POST','/v1/events/trigger',event)
        assert status==201,(base,status,response)
    for _ in range(90):
        job=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT push_job_id FROM novu_rust_push_step_outcomes WHERE transaction_id='{transaction}' AND status='queued'")
        log=subprocess.run(['docker','exec',worker,'cat','/tmp/novu-push-capture.jsonl'],capture_output=True,text=True).stdout.splitlines()
        reference=[json.loads(line) for line in log if json.loads(line).get(route_key)==destination]
        if job and len(reference)==count:break
        time.sleep(.5)
    else:raise AssertionError((label,job,reference))
    with tempfile.TemporaryDirectory(prefix='novu-multi-route-') as directory:
        capture=Path(directory)/'capture.json';auth=Path(directory)/'auth.json'
        config={first:'first-fixture-secret',second:'second-fixture-secret'}
        subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_push','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_PUSH_CAPTURE_PATH=str(capture),TEST_PUSH_AUTH_CAPTURE_PATH=str(auth),TEST_PUSH_EXPECTED_COUNT=str(count),TEST_PUSH_INTEGRATIONS=json.dumps(config)),check=True)
        actual=[item['message'] for item in json.loads(capture.read_text())]
        assert json.loads(auth.read_text())==['Bearer '+secret for secret in expected_auth],(label,auth.read_text())
    for original,rust in zip(reference,actual):
        original['data'].pop('__nvMessageId');rust['data'].pop('__nvMessageId')
        assert original==rust,(label,original,rust)
    source_history=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db");print(JSON.stringify(d.messages.find({{transactionId:"{transaction}"}}).toArray().map(m=>({{deviceTokens:m.deviceTokens,status:m.status}}))));'''))
    rust_history=json.loads(run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT COALESCE(json_agg(json_build_object('deviceTokens',device_tokens,'status',status)),'[]'::json)::text FROM novu_rust_push_messages WHERE transaction_id='{transaction}'"))
    assert sort(source_history)==sort(rust_history)==[{'deviceTokens':[],'status':'sent'}]*count,(label,source_history,rust_history)
    print(label+': multiple integration routing and history matched')

# A tokens override is one logical multicast per subscriber integration, even
# though the Rust HTTP v1 adapter dispatches each target individually.
transaction='multi-multicast-'+uuid.uuid4().hex
tokens=['multi-cast-a-'+uuid.uuid4().hex,'multi-cast-b-'+uuid.uuid4().hex]
for base in bases:
    status,response=request(base,'POST','/v1/events/trigger',{'name':'push-multi','to':subscriber,'transactionId':transaction,'overrides':{'providers':{'fcm':{'tokens':tokens}}}})
    assert status==201,(base,status,response)
for _ in range(90):
    job=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT push_job_id FROM novu_rust_push_step_outcomes WHERE transaction_id='{transaction}' AND status='queued'")
    log=subprocess.run(['docker','exec',worker,'cat','/tmp/novu-push-capture.jsonl'],capture_output=True,text=True).stdout.splitlines()
    reference=[json.loads(line) for line in log if json.loads(line).get('tokens')==tokens]
    if job and len(reference)==2:break
    time.sleep(.5)
else:raise AssertionError((job,reference))
with tempfile.TemporaryDirectory(prefix='novu-multi-multicast-') as directory:
    config={first:'first-fixture-secret',second:'second-fixture-secret'}
    capture=Path(directory)/'capture.json';auth=Path(directory)/'auth.json'
    subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_push','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_PUSH_CAPTURE_PATH=str(capture),TEST_PUSH_AUTH_CAPTURE_PATH=str(auth),TEST_PUSH_EXPECTED_COUNT='4',TEST_PUSH_INTEGRATIONS=json.dumps(config)),check=True)
    actual=[entry['message'] for entry in json.loads(capture.read_text())]
    assert json.loads(auth.read_text())==['Bearer first-fixture-secret']*2+['Bearer second-fixture-secret']*2
reference_ids=[];rust_ids=[]
for idx,original in enumerate(reference):
    reference_ids.append(original['data'].pop('__nvMessageId'));original.pop('tokens')
    ids=set()
    for token,message in zip(tokens,actual[2*idx:2*idx+2]):
        ids.add(message['data'].pop('__nvMessageId'));assert message.pop('token')==token
        assert original==message,(original,message)
    assert len(ids)==1
    rust_ids.append(next(iter(ids)))
source_history=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db");print(JSON.stringify(d.messages.find({{transactionId:"{transaction}"}}).toArray().map(m=>({{id:String(m._id),deviceTokens:m.deviceTokens,status:m.status}}))));'''))
rust_history=json.loads(run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT COALESCE(json_agg(json_build_object('id',internal_id,'deviceTokens',device_tokens,'status',status)),'[]'::json)::text FROM novu_rust_push_messages WHERE transaction_id='{transaction}'"))
assert sort(source_history)==sort([{'id':identity,'deviceTokens':tokens,'status':'sent'} for identity in reference_ids]),source_history
assert sort(rust_history)==sort([{'id':identity,'deviceTokens':tokens,'status':'sent'} for identity in rust_ids]),rust_history
print('multicast uses separate shared history for each integration: match')

# An absent credential must fail before the first provider request, even when
# the first target has valid credentials.
missing_tx=subscriber+'-missing-config'
assert request(bases[1],'POST','/v1/events/trigger',{'name':'push-multi','to':subscriber,'transactionId':missing_tx})[0]==201
for _ in range(90):
    missing_job=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT push_job_id FROM novu_rust_push_step_outcomes WHERE transaction_id='{missing_tx}' AND status='queued'")
    if missing_job:break
    time.sleep(.5)
else:raise AssertionError('missing-config job not queued')
with tempfile.TemporaryDirectory(prefix='novu-multi-fcm-') as directory:
    capture=Path(directory)/'capture.json'
    subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_push','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_PUSH_CAPTURE_PATH=str(capture),TEST_PUSH_EXPECTED_COUNT='0',TEST_PUSH_INTEGRATIONS=json.dumps({first:'first-fixture-secret'})),check=True)
    assert json.loads(capture.read_text())==[]
assert run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT status FROM novu_queue_jobs WHERE id='{missing_job}'")=='failed'
print('missing integration credentials fail before any send')

if os.environ.get('IS_EXPIRED_TOKENS_REMOVAL_ENABLED')=='true':
    apply('PUT','fcm-first',['device-invalid-multi','first-a'],[channel(first,['device-invalid-multi','first-a']),channel(second,['second-b'])])
    invalid_tx=subscriber+'-invalid'
    for base in bases:assert request(base,'POST','/v1/events/trigger',{'name':'push-multi','to':subscriber,'transactionId':invalid_tx})[0]==201
    for _ in range(90):
        invalid_job=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT push_job_id FROM novu_rust_push_step_outcomes WHERE transaction_id='{invalid_tx}' AND status='queued'")
        status=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''print(JSON.stringify(db.getSiblingDB("novu-db").jobs.find({{transactionId:"{invalid_tx}",type:"push"}}).toArray().map(j=>j.status)))'''))
        if invalid_job and status and all(v in ('completed','failed','canceled','skipped') for v in status):break
        time.sleep(.5)
    else:raise AssertionError((invalid_job,status))
    with tempfile.TemporaryDirectory(prefix='novu-multi-fcm-') as directory:
        capture=Path(directory)/'capture.json';auth=Path(directory)/'auth.json'
        subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_push','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_PUSH_CAPTURE_PATH=str(capture),TEST_PUSH_AUTH_CAPTURE_PATH=str(auth),TEST_PUSH_EXPECTED_COUNT='3',TEST_PUSH_INVALID_TARGET='device-invalid-multi',TEST_PUSH_INTEGRATIONS=json.dumps({first:'first-fixture-secret',second:'second-fixture-secret'})),check=True)
        assert json.loads(auth.read_text())==['Bearer first-fixture-secret','Bearer first-fixture-secret','Bearer second-fixture-secret']
    for base in bases:
        status,body=request(base,'GET','/v2/subscribers/'+subscriber)
        assert status==200 and body['data']['channels']==[channel(first,['first-a']),channel(second,['second-b'])],(base,body)
    assert run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT status FROM novu_queue_jobs WHERE id='{invalid_job}'")=='completed'
    print('invalid token cleanup respects integration identity: match')

apply('PUT','fcm-first',['first-only'],[channel(first,['first-only']),channel(second,['second-b'])])
run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''db.getSiblingDB("novu-db").integrations.updateOne({{_id:ObjectId("{second}")}},{{$set:{{active:false}}}})''')
run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',f"UPDATE novu_rust_fcm_integrations SET active=false WHERE environment_id='{env}' AND internal_id='{second}'")
apply('PATCH',None,['fallback'],[channel(first,['first-only','fallback']),channel(second,['second-b'])])
inactive_tx=subscriber+'-inactive'
for base in bases:assert request(base,'POST','/v1/events/trigger',{'name':'push-multi','to':subscriber,'transactionId':inactive_tx})[0]==201
for _ in range(90):
    inactive_job=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT push_job_id FROM novu_rust_push_step_outcomes WHERE transaction_id='{inactive_tx}' AND status='queued'")
    log=subprocess.run(['docker','exec',worker,'cat','/tmp/novu-push-capture.jsonl'],capture_output=True,text=True).stdout.splitlines()
    sent=[json.loads(line)['tokens'][0] for line in log if json.loads(line).get('tokens',[None])[0] in ('first-only','fallback')]
    if inactive_job and len(sent)==2:break
    time.sleep(.5)
else:raise AssertionError((inactive_job,sent))
with tempfile.TemporaryDirectory(prefix='novu-multi-fcm-') as directory:
    capture=Path(directory)/'capture.json';auth=Path(directory)/'auth.json'
    subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_push','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_PUSH_CAPTURE_PATH=str(capture),TEST_PUSH_AUTH_CAPTURE_PATH=str(auth),TEST_PUSH_EXPECTED_COUNT='2',TEST_PUSH_INTEGRATIONS=json.dumps({first:'first-fixture-secret'})),check=True)
    assert [item['message']['token'] for item in json.loads(capture.read_text())]==sent==['first-only','fallback']
    assert json.loads(auth.read_text())==['Bearer first-fixture-secret','Bearer first-fixture-secret']
print('inactive integration skipped and default selection moved to active integration: match')

for base in bases:
    status,body=request(base,'DELETE','/v1/subscribers/'+subscriber+'/credentials/fcm',{})
    assert status==204 and body is None
    status,body=request(base,'GET','/v2/subscribers/'+subscriber)
    assert status==200 and body['data']['channels']==[],(base,body)
print('delete removes all FCM channels: match')
