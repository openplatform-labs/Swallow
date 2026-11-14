#!/usr/bin/env python3
"""Compare topic preference fan-out across four mocked delivery channels."""
import json, os, re, subprocess, tempfile, time, urllib.error, urllib.request, uuid
from pathlib import Path
mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER']; pg=os.environ['NOVU_RUST_PG_CONTAINER']; worker=os.environ['NOVU_REFERENCE_WORKER_CONTAINER']
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]; key=os.environ['NOVU_TEST_API_KEY']
env='64a000000000000000000002'; wf='64a000000000000000000004'; suffix=uuid.uuid4().hex
users=['channels-blocked-'+suffix,'channels-allowed-'+suffix]; topic='channels-topic-'+suffix

def run(*args):return subprocess.check_output(args,text=True).strip()
def req(base,method,path,body):
    request=urllib.request.Request(base+path,data=json.dumps(body).encode(),headers={'Content-Type':'application/json','Authorization':'ApiKey '+key},method=method)
    try:r=urllib.request.urlopen(request,timeout=15)
    except urllib.error.HTTPError as e:r=e
    with r:
        raw=r.read();return r.status,json.loads(raw) if raw else None

run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db"),e=ObjectId("{env}"),o=ObjectId("64a000000000000000000003"),w=ObjectId("{wf}");
const steps=[['email','64a0000000000000000000a1'],['sms','64a0000000000000000000a2'],['push','64a0000000000000000000a3']];
for(const [type,id] of steps) d.messagetemplates.insertOne({{_id:ObjectId(id),_environmentId:e,_organizationId:o,type,active:true,subject:'Topic email',title:'Topic push',content:'Hello',contentType:'customHtml',deleted:false}});
d.notificationtemplates.updateOne({{_id:w}},{{$push:{{steps:{{$each:steps.map(([type,id])=>({{active:true,_templateId:ObjectId(id),stepId:'topic-'+type}}))}}}}}});
d.integrations.insertMany([
{{_environmentId:e,_organizationId:o,providerId:'sendgrid',channel:'email',active:true,primary:true,credentials:{{apiKey:'SG.fixture',from:'sender@example.test',senderName:'Sender'}},deleted:false}},
{{_environmentId:e,_organizationId:o,providerId:'twilio',channel:'sms',active:true,primary:true,credentials:{{accountSid:'AC00000000000000000000000000000001',token:'fixture',from:'+15550000001'}},deleted:false}},
{{_id:ObjectId('64a0000000000000000000a4'),_environmentId:e,_organizationId:o,identifier:'fixture-fcm',providerId:'fcm',channel:'push',active:true,primary:true,credentials:{{serviceAccount:JSON.stringify({{project_id:'demo',client_email:'fixture@example.test',private_key:'fixture'}})}},deleted:false}}]);
d.preferences.updateOne({{_environmentId:e,_templateId:w,type:'WORKFLOW_RESOURCE'}},{{$set:{{'preferences.channels.email.enabled':true,'preferences.channels.sms.enabled':true,'preferences.channels.push.enabled':true}}}});''')
run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',f'''UPDATE novu_rust_workflows SET email_subject='Topic email',email_html='Hello',sms_content='Hello',push_title='Topic push',push_content='Hello' WHERE environment_id='{env}' AND identifier='greeting';
UPDATE novu_rust_workflow_preferences SET email_enabled=true,sms_enabled=true,push_enabled=true WHERE environment_id='{env}' AND workflow_identifier='greeting';
INSERT INTO novu_rust_fcm_integrations(environment_id,internal_id,identifier,active) VALUES ('{env}','64a0000000000000000000a4','fixture-fcm',true);''')
for base in bases:
    for i,user in enumerate(users):
        status,_=req(base,'POST','/v2/subscribers',{'subscriberId':user,'firstName':'Jane','email':f'user{i}@example.test','phone':f'+1555000000{i+2}'})
        assert status==201,(base,status)
        status,_=req(base,'PUT','/v1/subscribers/'+user+'/credentials',{'providerId':'fcm','credentials':{'deviceTokens':[f'topic-device-{i}']}})
        assert status==200,(base,status)
        status,result=req(base,'POST','/v2/topics/'+topic+'/subscriptions',{'subscriberIds':[user],'preferences':[{'workflowId':'greeting','enabled':bool(i)}]})
        assert status==201,(base,status,result)

def capture_lines(channel):
    output=subprocess.run(['docker','exec',worker,'cat',f'/tmp/novu-{channel}-capture.jsonl'],capture_output=True,text=True)
    return [json.loads(line) for line in output.stdout.splitlines()]

for label,direct,expected in [('topic-filter',False,[users[1]]),('direct-bypass',True,users)]:
    before={channel:len(capture_lines(channel)) for channel in ['email','sms','push']}
    txn='topic-channels-'+uuid.uuid4().hex
    to=[{'type':'Topic','topicKey':topic}]+([users[0]] if direct else [])
    for base in bases:
        status,result=req(base,'POST','/v1/events/trigger',{'name':'greeting','to':to,'payload':{},'transactionId':txn})
        assert status==201,(base,status,result)
    expected_pairs=sorted((user,channel) for user in expected for channel in ['in_app','email','sms','push'])
    js=f'''const d=db.getSiblingDB("novu-db");let jobs=d.jobs.find({{transactionId:"{txn}"}}).toArray();let msgs=d.messages.find({{transactionId:"{txn}"}}).toArray().map(m=>[d.subscribers.findOne({{_id:m._subscriberId}})?.subscriberId,m.channel]);print(JSON.stringify({{jobs:jobs.map(j=>j.status),msgs}}));'''
    sql=f'''SELECT json_build_object('trigger',(SELECT status FROM novu_queue_jobs WHERE payload->>'transactionId'='{txn}'),'pairs',COALESCE((SELECT json_agg(json_build_array(subscriber_id,channel)) FROM (SELECT subscriber_id,'in_app' AS channel FROM novu_rust_in_app_messages WHERE transaction_id='{txn}' UNION ALL SELECT subscriber_id,'email' FROM novu_rust_email_step_outcomes WHERE transaction_id='{txn}' AND status='queued' UNION ALL SELECT subscriber_id,'sms' FROM novu_rust_sms_step_outcomes WHERE transaction_id='{txn}' AND status='queued' UNION ALL SELECT subscriber_id,'push' FROM novu_rust_push_step_outcomes WHERE transaction_id='{txn}' AND status='queued') x),'[]'::json))::text'''
    for _ in range(90):
        original=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',js))
        rust=json.loads(run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql))
        if original['jobs'] and all(x in ('completed','canceled','skipped','failed') for x in original['jobs']) and rust['trigger']=='completed' and sorted(map(tuple,original['msgs']))==expected_pairs and sorted(map(tuple,rust['pairs']))==expected_pairs:break
        time.sleep(1)
    else:raise AssertionError((label,original,rust,expected_pairs))
    assert 'failed' not in original['jobs'],(label,original)
    # Drain real Rust channel workers through local mock providers; no external send.
    captures={'email':[],'sms':[],'push':[]}
    for _ in expected:
        with tempfile.TemporaryDirectory(prefix='swallow-topic-channels-') as directory:
            for channel,env_name,test,count in [('email','TEST_EMAIL_CAPTURE_PATH','capture_reference_workflow_email',None),('sms','TEST_SMS_CAPTURE_PATH','capture_reference_workflow_sms',None),('push','TEST_PUSH_CAPTURE_PATH','capture_reference_workflow_push','1')]:
                path=Path(directory)/f'{channel}.json'; child=dict(os.environ,**{env_name:str(path)})
                if count:child['TEST_PUSH_EXPECTED_COUNT']=count
                subprocess.run(['cargo','test','--locked','--offline','--lib',test,'--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=child,check=True)
                received=json.loads(path.read_text()); assert received is not None,(label,channel)
                captures[channel].extend(received if channel=='push' else [received])
    for channel in ['email','sms','push']:
        original=capture_lines(channel)[before[channel]:]
        actual=captures[channel]
        def normalize(message):
            message=json.loads(json.dumps(message))
            if channel=='email':
                args=message.get('custom_args',{});identifier=args.pop('id');assert re.fullmatch('[0-9a-f]{24}',identifier) and args.pop('novuMessageId')==identifier
            if channel=='push':
                if 'message' in message:message=message['message']
                if 'tokens' in message:message['token']=message.pop('tokens')[0]
                identifier=message['data'].pop('__nvMessageId');assert re.fullmatch('[0-9a-f]{24}',identifier)
            return json.dumps(message,sort_keys=True)
        assert sorted(map(normalize,original))==sorted(map(normalize,actual)),(label,channel,original,actual)
    print(label+': recipient/channel membership and mocked provider requests matched',flush=True)
