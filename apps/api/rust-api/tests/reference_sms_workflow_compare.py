#!/usr/bin/env python3
"""Compare selected SMS workflow cases against original Novu using mock Twilio."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.request
import uuid

mongo, pg, worker = (os.environ[k] for k in ('NOVU_REFERENCE_MONGO_CONTAINER','NOVU_RUST_PG_CONTAINER','NOVU_REFERENCE_WORKER_CONTAINER'))
def run(*args):
    return subprocess.check_output(args,text=True).strip()
def request(base, method, path, payload):
    req=urllib.request.Request(base+path,data=json.dumps(payload).encode(),headers={'Content-Type':'application/json','Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']},method=method)
    with urllib.request.urlopen(req,timeout=10) as response:
        return response.status,json.load(response)

run('docker','exec',mongo,'mongosh','--quiet','--eval','''
const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),w=ObjectId("64a000000000000000000031"),m=ObjectId("64a000000000000000000032");
d.integrations.insertOne({_environmentId:e,_organizationId:o,providerId:"twilio",channel:"sms",active:true,primary:true,credentials:{accountSid:"AC00000000000000000000000000000001",token:"fixture",from:"+15550000001"},deleted:false});
d.messagetemplates.insertOne({_id:m,_environmentId:e,_organizationId:o,type:"sms",active:true,content:"Hello {{subscriber.firstName}} {{name}}",deleted:false});
d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:o,name:"SMS Compare",active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],triggers:[{type:"event",identifier:"sms-compare"}],steps:[{active:true,_templateId:m,stepId:"sms-step"}],createdAt:new Date(),updatedAt:new Date()});
d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:w,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{sms:{enabled:true}}},deleted:false});
''')
run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c','''
INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,sms_content,sms_step_id,inbox_metadata) VALUES ('64a000000000000000000002','sms-compare',true,true,NULL,'sms-step','{"id":"64a000000000000000000031","name":"SMS Compare","critical":false,"tags":[]}'::jsonb);
INSERT INTO novu_rust_channel_workflow_permissions SELECT key_hash,'sms','write' FROM novu_rust_api_keys WHERE environment_id='64a000000000000000000002';
INSERT INTO novu_rust_workflow_preferences(environment_id,workflow_identifier,all_enabled,read_only,sms_enabled) VALUES ('64a000000000000000000002','sms-compare',true,false,true);
''')
status,_=request(os.environ['NOVU_RUST_URL'],'PUT','/v1/experimental/workflows/sms-compare/channel-steps/sms',{'title':None,'content':'Hello {{subscriber.firstName}} {{name}}','active':True})
assert status==200,status
cases=[
 ('subscriber',{'subscriberId':'sms-a','phone':'+15550000002','firstName':'Jane'},{'name':'Alice'},None,{'To':'+15550000002','From':'+15550000001','Body':'Hello Jane Alice'}),
 ('payload',{'subscriberId':'sms-b','phone':'+15550000002','firstName':'Jane'},{'name':'Alice','phone':'+15550000003'},None,{'To':'+15550000003','From':'+15550000001','Body':'Hello Jane Alice'}),
 ('override',{'subscriberId':'sms-c','phone':'+15550000002','firstName':'Jane'},{'name':'Alice','phone':'+15550000003'},{'to':'+15550000004','from':'+15550000005','content':'Override'}, {'To':'+15550000004','From':'+15550000005','Body':'Override'}),
 ('provider-flat',{'subscriberId':'sms-flat','phone':'+15550000002','firstName':'Jane'},{'name':'Alice'},{'twilio':{'to':'+15550000006','from':'+15550000007','content':'Flat'}},{'To':'+15550000006','From':'+15550000007','Body':'Flat'}),
 ('provider-step',{'subscriberId':'sms-layer','phone':'+15550000002','firstName':'Jane'},{'name':'Alice'},{'sms':{'to':'+15550000003','content':'channel'},'twilio':{'to':'+15550000004','content':'flat'},'providers':{'twilio':{'to':'+15550000005','body':'global','applicationSid':'AP123','_passthrough':{'body':{'to':'+15550000008','body':'passthrough'}}}},'steps':{'sms-step':{'providers':{'twilio':{'to':'+15550000006','from':'+15550000007','body':'step','statusCallback':'https://example.test/status'}}}}},{'To':'+15550000008','From':'+15550000007','Body':'passthrough','ApplicationSid':'AP123','StatusCallback':'https://example.test/status'}),
 ('provider-blank',{'subscriberId':'sms-blank','phone':'+15550000002','firstName':'Jane'},{'name':'Alice','phone':'+15550000003'},{'providers':{'twilio':{'to':'+15550000005','_passthrough':{'body':{'to':'  '}}}},'steps':{'sms-step':{'providers':{'twilio':{'to':' '}}}}},{'To':'+15550000003','From':'+15550000001','Body':'Hello Jane Alice'}),
 ('provider-options',{'subscriberId':'sms-options','phone':'+15550000002','firstName':'Jane'},{'name':'Alice'},{'providers':{'twilio':{'mediaUrl':['https://example.test/old'], 'persistentAction':['geo:1,2','geo:3,4'], 'maxPrice':0.25, 'attempt':2, 'validityPeriod':120, 'provideFeedback':True, 'forceDelivery':False, 'contentRetention':'retain', 'addressRetention':'obfuscate', 'smartEncoded':True, 'shortenUrls':False, 'scheduleType':'fixed', 'sendAt':'2030-01-02T03:04:05Z', 'sendAsMms':True, 'contentVariables':'{"x":"y"}', 'riskCheck':'enable', 'contentSid':'HX123'}},'steps':{'sms-step':{'providers':{'twilio':{'mediaUrl':['https://example.test/a','https://example.test/b']}}}}},{'To':'+15550000002','From':'+15550000001','Body':'Hello Jane Alice','MediaUrl':['https://example.test/a','https://example.test/b'],'PersistentAction':['geo:1,2','geo:3,4'],'MaxPrice':'0.25','Attempt':'2','ValidityPeriod':'120','ProvideFeedback':'true','ForceDelivery':'false','ContentRetention':'retain','AddressRetention':'obfuscate','SmartEncoded':'true','ShortenUrls':'false','ScheduleType':'fixed','SendAt':'2030-01-02T03:04:05Z','SendAsMms':'true','ContentVariables':'{"x":"y"}','RiskCheck':'enable','ContentSid':'HX123'}),
 ('rejected',{'subscriberId':'sms-rejected','phone':'+15550000999','firstName':'Jane'},{'name':'Alice'},None,{'To':'+15550000999','From':'+15550000001','Body':'Hello Jane Alice'}),
 ('missing',{'subscriberId':'sms-missing','firstName':'Jane'},{'name':'Alice'},None,None),
 ('disabled', 'sms-disabled', {'name':'Alice'}, None, None),
]
for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
    base=os.environ[key]
    assert request(base,'POST','/v2/subscribers',{'subscriberId':'sms-disabled','phone':'+15550000002'})[0]==201
    assert request(base,'PATCH','/v2/subscribers/sms-disabled/preferences',{'channels':{'sms':False}})[0]==200
for index,(label,recipient,payload,override,expected) in enumerate(cases):
    txn='sms-'+label+'-'+uuid.uuid4().hex
    event={'name':'sms-compare','to':recipient,'payload':payload,'transactionId':txn}
    if override: event['overrides']=override if label.startswith('provider-') else {'sms':override}
    for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
        status,result=request(os.environ[key],'POST','/v1/events/trigger',event)
        assert status==201 and result['data']['status']=='processed',(key,result)
    for _ in range(90):
        result=subprocess.run(['docker','exec',worker,'cat','/tmp/novu-sms-capture.jsonl'],text=True,capture_output=True)
        lines=result.stdout.strip().splitlines()
        queued=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT count(*) FROM novu_rust_sms_step_outcomes WHERE transaction_id='{txn}' AND status='queued'")
        source=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db"); print(JSON.stringify({{jobs:d.jobs.find({{transactionId:"{txn}"}}).toArray().map(j=>({{status:j.status,detail:j.deliveryLifecycleState?.detail}})),messages:d.messages.find({{transactionId:"{txn}"}}).toArray().map(m=>({{status:m.status,errorId:m.errorId ?? null}}))}}));'''))
        terminal = any(j['status'] in ('canceled','skipped','failed') for j in source['jobs'])
        if queued=='1' and ((expected is not None and len(lines)>=index+1) or (expected is None and terminal)): break
        time.sleep(.5)
    else: raise AssertionError((label,queued,source,result.stderr))
    with tempfile.TemporaryDirectory(prefix='novu-sms-compare-') as directory:
        capture=Path(directory)/'capture.json'
        env=dict(os.environ,TEST_SMS_CAPTURE_PATH=str(capture))
        if expected is None: env['TEST_SMS_ALLOW_NO_REQUEST']='1'
        if label=='rejected': env['TEST_SMS_HTTP_STATUS']='429'
        subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_sms','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=env,check=True)
        rust=json.loads(capture.read_text())
    if expected is not None:
        original=json.loads(lines[-1]); assert original==rust==expected,(label,original,rust,expected)
    else:
        assert rust is None
        outcome=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT status FROM novu_rust_sms_step_outcomes WHERE transaction_id='{txn}'")
        assert outcome==('missing_phone' if label=='missing' else 'canceled'),outcome
        if label=='missing':
            assert source['messages']==[{'status':'warning','errorId':'no_subscriber_phone'}], source
        else:
            assert not source['messages'] and any(j.get('detail')=='preference' for j in source['jobs']),source
    for _ in range(30):
        source_messages=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db"); print(JSON.stringify(d.messages.find({{transactionId:"{txn}"}}).toArray().map(m=>({{status:m.status,phone:m.phone ?? null,content:m.content ?? null,errorId:m.errorId ?? null,errorText:m.errorText ?? null,identifier:m.identifier ?? null}}))));'''))
        if label=='disabled' or (source_messages and (label=='missing' and source_messages[0]['status']=='warning' or label=='rejected' and source_messages[0]['status']=='error' or label not in ('missing','rejected') and source_messages[0]['identifier']=='SM123')): break
        time.sleep(.25)
    rust_messages=json.loads(run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT COALESCE(json_agg(json_build_object('status',status,'phone',phone,'content',content,'errorId',error_id,'errorText',error_text,'identifier',identifier)),'[]'::json)::text FROM novu_rust_sms_messages WHERE transaction_id='{txn}'"))
    if label=='rejected':
        assert source_messages[0]['errorText'] and rust_messages[0]['errorText']=='sms provider rejected request', (source_messages,rust_messages)
        source_messages[0]['errorText']=rust_messages[0]['errorText']
    assert source_messages==rust_messages,(label,source_messages,rust_messages)
    print(label+': selected SMS behavior matched')
