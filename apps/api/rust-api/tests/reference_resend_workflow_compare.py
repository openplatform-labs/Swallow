#!/usr/bin/env python3
"""Compare one original/Rust workflow with mocked Resend transports, not real delivery."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.request
import uuid

mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
pg=os.environ['NOVU_RUST_PG_CONTAINER']
original_worker=os.environ['NOVU_REFERENCE_WORKER_CONTAINER']
transaction='resend-compare-'+uuid.uuid4().hex

def run(*args):
    return subprocess.check_output(args,text=True).strip()

run('docker','exec',mongo,'mongosh','--quiet','--eval','''
const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),w=ObjectId("64a000000000000000000031"),m=ObjectId("64a000000000000000000032");
d.integrations.insertOne({_environmentId:e,_organizationId:o,providerId:"resend",channel:"email",active:true,primary:true,credentials:{apiKey:"re_fixture",from:"sender@example.test",senderName:"Sender"},deleted:false});
d.messagetemplates.insertOne({_id:m,_environmentId:e,_organizationId:o,type:"email",active:true,subject:" Hello {{subscriber.firstName}} ",content:"<p>{{name}}</p>",contentType:"customHtml",deleted:false});
d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:o,name:"Resend Compare",active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],triggers:[{type:"event",identifier:"resend-compare"}],steps:[{active:true,_templateId:m,stepId:"email-step"}],createdAt:new Date(),updatedAt:new Date()});
d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:w,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{email:{enabled:true}}},deleted:false});
''')
run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c','''
INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html,email_provider,inbox_metadata,email_step_id)
VALUES ('64a000000000000000000002','resend-compare',true,true,' Hello {{subscriber.firstName}} ','<p>{{name}}</p>','resend','{"id":"64a000000000000000000031","name":"Resend Compare","critical":false,"tags":[]}'::jsonb,'email-step');
INSERT INTO novu_rust_workflow_preferences(environment_id,workflow_identifier,all_enabled,read_only,email_enabled) VALUES ('64a000000000000000000002','resend-compare',true,false,true);
''')
body={'name':'resend-compare','to':{'subscriberId':'resend-reference-user','email':'recipient@example.test','firstName':'Jane'},'payload':{'name':'<Alice>'},'transactionId':transaction}
for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
    request=urllib.request.Request(os.environ[key]+'/v1/events/trigger',data=json.dumps(body).encode(),headers={'Content-Type':'application/json','Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']},method='POST')
    with urllib.request.urlopen(request,timeout=10) as response:
        assert response.status==201 and json.load(response)['data']['status']=='processed'
original='';queued=''
for _ in range(90):
    result=subprocess.run(['docker','exec',original_worker,'cat','/tmp/novu-resend-capture.jsonl'],text=True,capture_output=True)
    original=result.stdout.strip()
    queued=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT count(*) FROM novu_queue_jobs WHERE kind='experimental_resend_email' AND payload->>'transaction_id'='{transaction}'")
    if original and queued=='1': break
    time.sleep(.5)
if not original or queued!='1':
    logs=subprocess.run(['docker','logs',original_worker],text=True,capture_output=True)
    raise AssertionError(f'worker capture missing original={bool(original)} queued={queued}; logs={logs.stderr[-4000:]}')
with tempfile.TemporaryDirectory(prefix='swallow-resend-compare-') as directory:
    capture=Path(directory)/'capture.json'
    envvars=dict(os.environ,TEST_EMAIL_CAPTURE_PATH=str(capture))
    subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_resend_workflow','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=envvars,check=True)
    rust=json.loads(capture.read_text())
original=json.loads(original.splitlines()[0])
assert original==rust,(original,rust)
assert rust['to']==['recipient@example.test'] and rust['subject']=='Hello Jane'
status=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT status||':'||delivery_state||':'||identifier FROM novu_rust_email_messages WHERE transaction_id='{transaction}'")
assert status=='sent:accepted:mock-resend-id',status
original_status=''
for _ in range(30):
    original_status=run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const m=db.getSiblingDB("novu-db").messages.findOne({{transactionId:"{transaction}"}}); if (m?.identifier) print(m.status+":"+m.identifier);''')
    if original_status: break
    time.sleep(.25)
assert original_status=='sent:mock-resend-id',original_status
print('original and Rust trigger -> workflow -> mock Resend request and selected history: match')
