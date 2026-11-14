#!/usr/bin/env python3
"""Compare one real Novu workflow with Rust, both using fake SendGrid transports."""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
import urllib.request
import uuid

mongo = os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
pg = os.environ['NOVU_RUST_PG_CONTAINER']
original_worker = os.environ['NOVU_REFERENCE_WORKER_CONTAINER']
env = '64a000000000000000000002'
workflow = 'email-compare'
transaction = 'email-compare-' + uuid.uuid4().hex

def run(*args):
    return subprocess.check_output(args, text=True).strip()

run('docker','exec',mongo,'mongosh','--quiet','--eval', '''
const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),w=ObjectId("64a000000000000000000021"),m=ObjectId("64a000000000000000000022");
d.integrations.insertOne({_environmentId:e,_organizationId:o,providerId:"sendgrid",channel:"email",active:true,primary:true,credentials:{apiKey:"SG.fixture",from:"sender@example.test",senderName:"Sender"},deleted:false});
d.messagetemplates.insertOne({_id:m,_environmentId:e,_organizationId:o,type:"email",active:true,subject:" Hello {{subscriber.firstName}} ",content:"<p>{{name}}</p>",contentType:"customHtml",deleted:false});
d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:o,name:"Email Compare",active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],triggers:[{type:"event",identifier:"email-compare"}],steps:[{active:true,_templateId:m,stepId:"email-step"}],createdAt:new Date(),updatedAt:new Date()});
d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:w,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{email:{enabled:true}}},deleted:false});
''')
run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c', '''
INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html,inbox_metadata,email_step_id)
VALUES ('64a000000000000000000002','email-compare',true,true,NULL,NULL,'{"id":"64a000000000000000000021","name":"Email Compare","critical":false,"tags":[]}'::jsonb,'email-step');
INSERT INTO novu_rust_workflow_preferences(environment_id,workflow_identifier,all_enabled,read_only,email_enabled) VALUES ('64a000000000000000000002','email-compare',true,false,true);
INSERT INTO novu_rust_channel_workflow_permissions SELECT key_hash,'email','write' FROM novu_rust_api_keys WHERE environment_id='64a000000000000000000002';
''')
req=urllib.request.Request(os.environ['NOVU_RUST_URL']+'/v1/experimental/workflows/email-compare/channel-steps/email',data=json.dumps({'title':' Hello {{subscriber.firstName}} ','content':'<p>{{name}}</p>','active':True}).encode(),headers={'Content-Type':'application/json','Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']},method='PUT')
with urllib.request.urlopen(req,timeout=10) as response: assert response.status==200
body = {'name':workflow,'to':{'subscriberId':'email-reference-user','email':'recipient@example.test','firstName':'Jane'},'payload':{'name':'<Alice>'},'transactionId':transaction}
for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
    req = urllib.request.Request(os.environ[key]+'/v1/events/trigger', data=json.dumps(body).encode(), headers={'Content-Type':'application/json','Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']}, method='POST')
    with urllib.request.urlopen(req,timeout=10) as response:
        result=json.load(response)
        assert response.status == 201 and result['data']['status']=='processed', result

original=''
queued=''
for _ in range(90):
    result=subprocess.run(['docker','exec',original_worker,'cat','/tmp/novu-email-capture.jsonl'],text=True,capture_output=True)
    original=result.stdout.strip()
    queued=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT count(*) FROM novu_rust_email_step_outcomes WHERE transaction_id='{transaction}' AND status='queued'")
    if original and queued=='1': break
    time.sleep(.5)
if not original or queued!='1':
    logs=subprocess.run(['docker','logs','--tail','80',original_worker],text=True,capture_output=True)
    raise AssertionError(f'email job missing: original={bool(original)} rust={queued}; worker log={logs.stderr[-6000:]}')

with tempfile.TemporaryDirectory(prefix='novu-email-compare-') as directory:
    capture=Path(directory)/'capture.json'
    envvars=dict(os.environ,TEST_EMAIL_CAPTURE_PATH=str(capture),TEST_EMAIL_STEP_POLICY='unchanged')
    subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_email','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=envvars,check=True)
    rust=json.loads(capture.read_text())
original=json.loads(original.splitlines()[0])
original_message_id=original['custom_args']['id']
rust_message_id=rust['custom_args']['id']
for message in (original,rust):
    custom=message['custom_args']
    assert re.fullmatch('[0-9a-f]{24}',custom['id']), custom
    assert custom.pop('novuMessageId')==custom.pop('id')
assert original==rust, (original,rust)
assert rust['personalizations'][0]['to']==[{'email':'recipient@example.test'}]
assert rust['subject']=='Hello Jane' and rust['content']==[{'value':'<html><head></head><body><p>&lt;Alice&gt;</p></body></html>','type':'text/html'}]
for _ in range(30):
    source_id=run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const m=db.getSiblingDB("novu-db").messages.findOne({{transactionId:"{transaction}"}}); if (m?.identifier) print(m.identifier);''')
    if source_id: break
    time.sleep(.25)
rust_id=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT identifier FROM novu_rust_email_messages WHERE environment_id='{env}' AND transaction_id='{transaction}'")
assert source_id==original_message_id and rust_id==rust_message_id,(source_id,rust_id)
print('original and Rust email trigger -> workflow -> mock SendGrid request: match')

# A separate workflow avoids cache races when checking inlining of a style rule.
run('docker','exec',mongo,'mongosh','--quiet','--eval', '''
const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),w=ObjectId("64a000000000000000000023"),m=ObjectId("64a000000000000000000024");
d.messagetemplates.insertOne({_id:m,_environmentId:e,_organizationId:o,type:"email",active:true,subject:"Styled",content:"<style>p { color: red; }</style><p>Hello</p>",contentType:"customHtml",deleted:false});
d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:o,name:"Email CSS",active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],triggers:[{type:"event",identifier:"email-css"}],steps:[{active:true,_templateId:m}],createdAt:new Date(),updatedAt:new Date()});
d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:w,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{email:{enabled:true}}},deleted:false});
''')
run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c', '''
INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html) VALUES ('64a000000000000000000002','email-css',true,true,'Styled','<style>p { color: red; }</style><p>Hello</p>');
''')
transaction = 'email-css-' + uuid.uuid4().hex
body={'name':'email-css','to':{'subscriberId':'email-reference-user','email':'recipient@example.test'},'payload':{},'transactionId':transaction}
for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
    req=urllib.request.Request(os.environ[key]+'/v1/events/trigger',data=json.dumps(body).encode(),headers={'Content-Type':'application/json','Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']},method='POST')
    with urllib.request.urlopen(req,timeout=10) as response:
        assert response.status==201 and json.load(response)['data']['status']=='processed'
original=''
for _ in range(90):
    result=subprocess.run(['docker','exec',original_worker,'cat','/tmp/novu-email-capture.jsonl'],text=True,capture_output=True)
    lines=result.stdout.strip().splitlines()
    if len(lines)>=2:
        original=lines[-1]
    queued=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT count(*) FROM novu_rust_email_step_outcomes WHERE transaction_id='{transaction}' AND status='queued'")
    if original and queued=='1': break
    time.sleep(.5)
assert original and queued=='1', (original,queued)
with tempfile.TemporaryDirectory(prefix='novu-email-css-') as directory:
    capture=Path(directory)/'capture.json'
    subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_email','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_EMAIL_CAPTURE_PATH=str(capture)),check=True)
    rust=json.loads(capture.read_text())
original=json.loads(original)
for message in (original,rust):
    custom=message['custom_args']
    assert re.fullmatch('[0-9a-f]{24}',custom['id'])
    assert custom.pop('novuMessageId')==custom.pop('id')
assert original==rust,(original,rust)
print('original and Rust CSS inlining through email workflow: match')

# Compare cancellation and missing-address behavior without sending anything.
def request(base, method, path, payload):
    req=urllib.request.Request(base+path,data=json.dumps(payload).encode(),headers={'Content-Type':'application/json','Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']},method=method)
    with urllib.request.urlopen(req,timeout=10) as response:
        return response.status, json.load(response)

disabled='email-disabled-'+uuid.uuid4().hex
missing='email-missing-'+uuid.uuid4().hex
for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
    base=os.environ[key]
    assert request(base,'POST','/v2/subscribers',{'subscriberId':disabled,'email':'disabled@example.test'})[0]==201
    assert request(base,'PATCH','/v2/subscribers/'+disabled+'/preferences',{'channels':{'email':False}})[0]==200
    assert request(base,'POST','/v2/subscribers',{'subscriberId':missing})[0]==201

for label,subscriber,want_status in [('preference',disabled,'canceled'),('missing_email',missing,'missing_email')]:
    txn='email-'+label+'-'+uuid.uuid4().hex
    for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
        status,result=request(os.environ[key],'POST','/v1/events/trigger',{'name':'email-compare','to':subscriber,'payload':{},'transactionId':txn})
        assert status==201 and result['data']['status']=='processed',result
    for _ in range(90):
        ref=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db"); print(JSON.stringify({{jobs:d.jobs.find({{transactionId:"{txn}"}}).toArray().map(j=>({{status:j.status,detail:j.deliveryLifecycleState?.detail}})),messages:d.messages.find({{transactionId:"{txn}"}}).toArray().map(m=>({{channel:m.channel,status:m.status,email:m.email ?? null,subject:m.subject,errorId:m.errorId ?? null,errorText:m.errorText ?? null}}))}}));'''))
        sql=f"SELECT json_build_object('outcomes',COALESCE((SELECT json_agg(status) FROM novu_rust_email_step_outcomes WHERE transaction_id='{txn}'),'[]'::json),'jobs',(SELECT count(*) FROM novu_queue_jobs q JOIN novu_rust_sendgrid_attempts a ON a.job_id=q.id WHERE q.kind='experimental_sendgrid_email' AND q.payload->>'transaction_id'='{txn}'),'messages',COALESCE((SELECT json_agg(json_build_object('channel','email','status',status,'email',email,'subject',subject,'errorId',error_id,'errorText',error_text)) FROM novu_rust_email_messages WHERE transaction_id='{txn}'),'[]'::json))::text"
        rust=json.loads(run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql))
        if rust['outcomes'] and ref['jobs'] and all(j['status'] in ('completed','skipped','canceled','failed') for j in ref['jobs']): break
        time.sleep(.5)
    with tempfile.TemporaryDirectory(prefix='novu-email-skipped-') as directory:
        capture=Path(directory)/'capture.json'
        subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_email','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_EMAIL_CAPTURE_PATH=str(capture),TEST_EMAIL_ALLOW_NO_REQUEST='1'),check=True)
        assert json.loads(capture.read_text()) is None
    rust=json.loads(run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql))
    assert rust['outcomes']==[want_status] and rust['jobs']==0,(label,ref,rust)
    if label=='preference':
        assert any(j['status']=='canceled' and j.get('detail')=='preference' for j in ref['jobs']) and not ref['messages'],(ref,rust)
    else:
        assert any(j['status']=='canceled' and j.get('detail')=='missing_email' for j in ref['jobs']), (ref,rust)
        assert len(ref['messages'])==1 and ref['messages'][0]['channel']=='email', (ref,rust)
    assert ref['messages']==rust['messages'],(label,ref,rust)
    print(label+': no provider request; selected source outcome and message fields match')
capture=run('docker','exec',original_worker,'cat','/tmp/novu-email-capture.jsonl')
assert len(capture.splitlines())==2, capture

# Provider rejection: compare the source worker's message and job outcome with
# the Rust consumer's recorded result. Response details are intentionally
# redacted by Rust and therefore are not compared byte for byte.
txn='email-reject-'+uuid.uuid4().hex
for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
    status,result=request(os.environ[key],'POST','/v1/events/trigger',{'name':'email-compare','to':{'subscriberId':'email-rejected','email':'reject@example.test'},'transactionId':txn,'payload':{}})
    assert status==201 and result['data']['status']=='processed',result
for _ in range(90):
    original=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db"); print(JSON.stringify({{jobs:d.jobs.find({{transactionId:"{txn}"}}).toArray().map(j=>({{status:j.status,detail:j.deliveryLifecycleState?.detail}})),message:d.messages.findOne({{transactionId:"{txn}"}},{{status:1,errorId:1,errorText:1}})}}));'''))
    queued=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT count(*) FROM novu_rust_email_step_outcomes WHERE transaction_id='{txn}' AND status='queued'")
    if original.get('message') and original['message'].get('status')=='error' and queued=='1': break
    time.sleep(.5)
else: raise AssertionError((original,queued))
with tempfile.TemporaryDirectory(prefix='novu-email-reject-') as directory:
    capture=Path(directory)/'capture.json'
    subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_email','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_EMAIL_CAPTURE_PATH=str(capture),TEST_EMAIL_HTTP_STATUS='429'),check=True)
    rust_request=json.loads(capture.read_text())
ref_request=json.loads(run('docker','exec',original_worker,'cat','/tmp/novu-email-capture.jsonl').splitlines()[-1])
for message in (ref_request,rust_request):
    custom=message['custom_args']
    assert re.fullmatch('[0-9a-f]{24}',custom['id'])
    assert custom.pop('novuMessageId')==custom.pop('id')
assert ref_request==rust_request,(ref_request,rust_request)
sql=f"SELECT json_build_object('status',m.status,'errorId',m.error_id,'errorText',m.error_text,'identifier',m.identifier,'job',q.status,'attempt',a.state,'httpStatus',a.http_status)::text FROM novu_rust_email_messages m JOIN novu_queue_jobs q ON q.id=m.email_job_id JOIN novu_rust_sendgrid_attempts a ON a.job_id=q.id WHERE m.environment_id='{env}' AND m.transaction_id='{txn}'"
rust=json.loads(run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql))
assert original['message']['status']==rust['status']=='error',(original,rust)
assert original['message']['errorId']==rust['errorId']=='mail_unexpected_error',(original,rust)
assert rust['errorText']=='email provider rejected request' and rust['identifier'] is None,(original,rust)
assert rust['job']=='failed' and rust['attempt']=='rejected' and rust['httpStatus']==429,(original,rust)
print('provider 429: request and selected error message fields match; Rust diagnostic redacted')

for name,overrides,subscriber in [
    ('append',{'toRecipient':'chosen@example.test','to':['chosen@example.test','extra@example.test'],'cc':['cc@example.test'],'bcc':['bcc@example.test'],'from':'from@example.test','senderName':'Other Sender','replyTo':'reply@example.test','subject':'Override Subject','html':'<b>Override</b>','headers':{'X-Example':'yes'},'customData':{'source':'fixture'}},{'subscriberId':'email-override','email':'stored@example.test'}),
    ('replace',{'replaceToRecipient':True,'to':['target@example.test'],'cc':['cc@example.test']},{'subscriberId':'email-override-no-email'}),
]:
    txn='email-override-'+name+'-'+uuid.uuid4().hex
    for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
        status,result=request(os.environ[key],'POST','/v1/events/trigger',{'name':'email-compare','to':subscriber,'overrides':{'email':overrides},'transactionId':txn,'payload':{}})
        assert status==201 and result['data']['status']=='processed',(key,status,result)
    found=None
    for _ in range(90):
        lines=run('docker','exec',original_worker,'cat','/tmp/novu-email-capture.jsonl').splitlines()
        found=next((v for v in map(json.loads,lines) if v.get('custom_args',{}).get('novuTransactionId')==txn),None)
        queued=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT count(*) FROM novu_rust_email_step_outcomes WHERE transaction_id='{txn}' AND status='queued'")
        if found and queued=='1': break
        time.sleep(.5)
    assert found and queued=='1',(name,found,queued)
    with tempfile.TemporaryDirectory(prefix='novu-email-overrides-') as directory:
        capture=Path(directory)/'capture.json'
        subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_email','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_EMAIL_CAPTURE_PATH=str(capture)),check=True)
        rust=json.loads(capture.read_text())
    for message in (found,rust):
        custom=message['custom_args']
        assert re.fullmatch('[0-9a-f]{24}',custom['id'])
        assert custom.pop('novuMessageId')==custom.pop('id')
    assert found==rust,(name,found,rust)
    print('email override '+name+': original and Rust provider requests match')

txn='email-layers-'+uuid.uuid4().hex
overrides={'email':{'subject':'channel','cc':['channel@example.test']},'sendgrid':{'subject':'flat'},'providers':{'sendgrid':{'subject':'global','cc':['global@example.test'],'to':['provider@example.test'],'from':'provider-from@example.test','senderName':'Provider','html':'<p>provider</p>','headers':{'X-Global':'yes'}}},'steps':{'email-step':{'providers':{'sendgrid':{'subject':'step','cc':['step@example.test'],'html':'<b>step</b>','headers':{'X-Step':'ok'}}}}}}
for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
    status,result=request(os.environ[key],'POST','/v1/events/trigger',{'name':'email-compare','to':{'subscriberId':'email-layers','email':'recipient@example.test'},'overrides':overrides,'transactionId':txn,'payload':{}})
    assert status==201 and result['data']['status']=='processed',(key,status,result)
found=None
for _ in range(90):
    lines=run('docker','exec',original_worker,'cat','/tmp/novu-email-capture.jsonl').splitlines()
    found=next((v for v in map(json.loads,lines) if v.get('custom_args',{}).get('novuTransactionId')==txn),None)
    queued=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT count(*) FROM novu_rust_email_step_outcomes WHERE transaction_id='{txn}' AND status='queued'")
    if found and queued=='1': break
    time.sleep(.5)
assert found and queued=='1',(found,queued)
with tempfile.TemporaryDirectory(prefix='novu-email-layers-') as directory:
    capture=Path(directory)/'capture.json'
    subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_email','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_EMAIL_CAPTURE_PATH=str(capture)),check=True)
    rust=json.loads(capture.read_text())
for message in (found,rust):
    custom=message['custom_args']
    assert re.fullmatch('[0-9a-f]{24}',custom['id'])
    assert custom.pop('novuMessageId')==custom.pop('id')
assert found==rust,(found,rust)
assert rust['subject']=='step' and rust['personalizations'][0]['cc']==[{'email':'step@example.test'}],rust
print('email override layer precedence: original and Rust requests match')
