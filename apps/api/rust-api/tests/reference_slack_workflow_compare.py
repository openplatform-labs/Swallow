#!/usr/bin/env python3
"""Compare API -> workflow -> mocked Slack provider with a disposable original server."""
import json, os, subprocess, tempfile, time, urllib.request, urllib.error, uuid
from pathlib import Path

mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
pg=os.environ['NOVU_RUST_PG_CONTAINER']
worker=os.environ['NOVU_REFERENCE_WORKER_CONTAINER']
transaction='slack-compare-'+uuid.uuid4().hex
def run(*args):
    return subprocess.check_output(args,text=True).strip()
def request(base,method,path,body):
    req=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers={'Content-Type':'application/json','Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']},method=method)
    try: response=urllib.request.urlopen(req,timeout=15)
    except urllib.error.HTTPError as error: response=error
    with response: return response.status,json.load(response)

run('docker','exec',mongo,'mongosh','--quiet','--eval','''
const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),w=ObjectId("64a000000000000000000071"),m=ObjectId("64a000000000000000000072");
d.integrations.insertOne({_environmentId:e,_organizationId:o,identifier:"slack-main",providerId:"slack",channel:"chat",active:true,primary:true,credentials:{},deleted:false});
d.channelconnections.insertOne({_environmentId:e,_organizationId:o,identifier:"slack-connection",integrationIdentifier:"slack-main",providerId:"slack",channel:"chat",contextKeys:[],workspace:{id:"T123"},auth:{accessToken:"fixture-token"}});
d.messagetemplates.insertOne({_id:m,_environmentId:e,_organizationId:o,type:"chat",active:true,content:"Hello {{subscriber.firstName}}",deleted:false});
d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:o,name:"Slack Compare",active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],triggers:[{type:"event",identifier:"slack-compare"}],steps:[{active:true,_templateId:m,stepId:"chat-step"}],createdAt:new Date(),updatedAt:new Date()});
d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:w,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{chat:{enabled:true}}},deleted:false});
''')
run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c','''
INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,chat_content,inbox_metadata) VALUES ('64a000000000000000000002','slack-compare',true,true,NULL,'{"id":"64a000000000000000000071","name":"Slack Compare","critical":false,"tags":[]}');
INSERT INTO novu_rust_workflow_preferences(environment_id,workflow_identifier,all_enabled,read_only,chat_enabled) VALUES ('64a000000000000000000002','slack-compare',true,false,true);
INSERT INTO novu_rust_slack_endpoint_permissions(key_hash,permission) VALUES ('4a453798dcb87778c40d2f5daceb0f28cf67e559b7bfbe865878995c64e575fe','read'),('4a453798dcb87778c40d2f5daceb0f28cf67e559b7bfbe865878995c64e575fe','write');
INSERT INTO novu_rust_slack_workflow_permissions(key_hash,permission) VALUES ('4a453798dcb87778c40d2f5daceb0f28cf67e559b7bfbe865878995c64e575fe','read'),('4a453798dcb87778c40d2f5daceb0f28cf67e559b7bfbe865878995c64e575fe','write');
INSERT INTO novu_rust_slack_integration_permissions(key_hash,permission) VALUES ('4a453798dcb87778c40d2f5daceb0f28cf67e559b7bfbe865878995c64e575fe','read'),('4a453798dcb87778c40d2f5daceb0f28cf67e559b7bfbe865878995c64e575fe','write');
''')
# The original workflow is seeded directly; Swallow's separate extension
# configures the equivalent step through its authorized management API.
metadata='/v1/experimental/slack-integrations'
status,body=request(os.environ['NOVU_RUST_URL'],'POST',metadata,{'identifier':'slack-main'})
assert status==201,(status,body)
status,body=request(os.environ['NOVU_RUST_URL'],'POST',metadata+'/slack-main/connections',{'identifier':'slack-connection'})
assert status==201,(status,body)
status,body=request(os.environ['NOVU_RUST_URL'],'GET',metadata+'/slack-main/connections',None)
assert status==200 and body['data']==['slack-connection'],(status,body)
step_path='/v1/experimental/workflows/slack-compare/slack-step'
status,body=request(os.environ['NOVU_RUST_URL'],'PUT',step_path,{'content':'Hello {{subscriber.firstName}}','active':True})
assert status==200 and body['data']['content']=='Hello {{subscriber.firstName}}' and body['data']['active'] is True,(status,body)
status,body=request(os.environ['NOVU_RUST_URL'],'GET',step_path,None)
assert status==200 and body['data']['content']=='Hello {{subscriber.firstName}}',(status,body)
assert run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',"SELECT count(*) FROM novu_rust_slack_workflow_audit WHERE environment_id='64a000000000000000000002' AND workflow_identifier='slack-compare'")=='1'
endpoints=[{'identifier':'slack-endpoint','subscriberId':'alice','integrationIdentifier':'slack-main','connectionIdentifier':'slack-connection','type':'slack_channel','endpoint':{'channelId':'C123'}},{'identifier':'slack-user-endpoint','subscriberId':'alice','integrationIdentifier':'slack-main','connectionIdentifier':'slack-connection','type':'slack_user','endpoint':{'userId':'U456'}}]
for base in [os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]:
    for endpoint in endpoints:
        status,body=request(base,'POST','/v1/channel-endpoints',endpoint)
        assert status==201,(base,status,body)
        data=body['data'];assert all(data[k]==v for k,v in endpoint.items()),(base,data)
def normalized(data):
    return {key:value for key,value in data.items() if key not in ('createdAt','updatedAt')}
for suffix,method,body in [('/slack-endpoint','GET',None),('/slack-endpoint','PATCH',{'endpoint':{'channelId':'C777'}}),('/slack-endpoint','PATCH',{'endpoint':{'channelId':'C123'}}),('/slack-user-endpoint','GET',None)]:
    replies=[request(os.environ[key],method,'/v1/channel-endpoints'+suffix,body) for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']]
    assert [item[0] for item in replies]==[200,200],(method,suffix,replies)
    assert normalized(replies[0][1]['data'])==normalized(replies[1][1]['data']),(method,suffix,replies)
lists=[request(os.environ[key],'GET','/v1/channel-endpoints?subscriberId=alice',None) for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']]
assert [item[0] for item in lists]==[200,200],lists
reference_list=sorted((normalized(item) for item in lists[0][1]['data']),key=lambda item:item['identifier'])
rust_list=sorted((normalized(item) for item in lists[1][1]['data']),key=lambda item:item['identifier'])
assert reference_list==rust_list and len(rust_list)==2,(reference_list,rust_list)
trigger={'name':'slack-compare','to':{'subscriberId':'alice'},'payload':{},'transactionId':transaction}
for base in [os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]:
    status,body=request(base,'POST','/v1/events/trigger',trigger)
    assert status==201 and body['data']['status']=='processed',(base,status,body)
original='';queued=''
for _ in range(90):
    original=subprocess.run(['docker','exec',worker,'cat','/tmp/novu-slack-capture.jsonl'],text=True,capture_output=True).stdout.strip()
    queued=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',"SELECT count(*) FROM novu_queue_jobs WHERE kind='experimental_slack_workflow_fanout'")
    if len(original.splitlines())>=2 and queued=='1':break
    time.sleep(.5)
if len(original.splitlines())<2 or queued!='1':
    logs=subprocess.run(['docker','logs',worker],text=True,capture_output=True)
    raise AssertionError(f'missing capture: original={bool(original)} fanout={queued}; logs={logs.stderr[-5000:]}')
with tempfile.TemporaryDirectory(prefix='swallow-slack-compare-') as directory:
    capture=Path(directory)/'capture.json'
    subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_slack_workflow','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_SLACK_CAPTURE_PATH=str(capture),NOVU_SLACK_WORKFLOW_STEP_POLICY="unchanged"),check=True)
    rust=json.loads(capture.read_text())
reference=sorted((json.loads(line) for line in original.splitlines()),key=lambda item:item['channel'])
rust=sorted(rust,key=lambda item:item['channel'])
assert reference==rust==[{'channel':'C123','text':'Hello Jane'},{'channel':'U456','text':'Hello Jane'}],(reference,rust)
print('original and Rust HTTP trigger -> workflow -> mock Slack request: match; Rust integration, connection and step set via management APIs with unchanged guard')
