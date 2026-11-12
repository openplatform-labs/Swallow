#!/usr/bin/env python3
"""Compare selected trigger-to-mocked-FCM workflow behavior with original Novu."""
import json,os,re,subprocess,tempfile,time,urllib.request,urllib.error,uuid
from pathlib import Path
mongo,pg,worker=(os.environ[k] for k in ('NOVU_REFERENCE_MONGO_CONTAINER','NOVU_RUST_PG_CONTAINER','NOVU_REFERENCE_WORKER_CONTAINER'))
def run(*args):return subprocess.check_output(args,text=True).strip()
def request(base,method,path,body,key=None):
    req=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers={'Content-Type':'application/json','Authorization':'ApiKey '+(key or os.environ['NOVU_TEST_API_KEY'])},method=method)
    try: response=urllib.request.urlopen(req,timeout=10)
    except urllib.error.HTTPError as error: response=error
    with response:
        raw=response.read()
        return response.status,json.loads(raw) if raw else None
run('docker','exec',mongo,'mongosh','--quiet','--eval','''
const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),w=ObjectId("64a000000000000000000051"),m=ObjectId("64a000000000000000000052"),i=ObjectId("64a000000000000000000053");
d.integrations.insertOne({_id:i,_environmentId:e,_organizationId:o,providerId:"fcm",channel:"push",active:true,primary:true,credentials:{serviceAccount:JSON.stringify({project_id:"demo",client_email:"fixture@example.test",private_key:"fixture"})},deleted:false});
d.messagetemplates.insertOne({_id:m,_environmentId:e,_organizationId:o,type:"push",active:true,title:"Hi {{subscriber.firstName}}",content:"Hello {{name}}",deleted:false});
d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:o,name:"Push Compare",active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],triggers:[{type:"event",identifier:"push-compare"}],steps:[{active:true,_templateId:m,stepId:"push-step"}],createdAt:new Date(),updatedAt:new Date()});
d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:w,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{push:{enabled:true}}},deleted:false});
''')
run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c','''
INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,push_title,push_content,inbox_metadata) VALUES ('64a000000000000000000002','push-compare',true,true,'Hi {{subscriber.firstName}}','Hello {{name}}','{"id":"64a000000000000000000051","name":"Push Compare","critical":false,"tags":[]}'::jsonb);
INSERT INTO novu_rust_workflow_preferences(environment_id,workflow_identifier,all_enabled,read_only,push_enabled) VALUES ('64a000000000000000000002','push-compare',true,false,true);
INSERT INTO novu_rust_fcm_integrations(environment_id,internal_id,identifier,active) VALUES ('64a000000000000000000002','64a000000000000000000053','fixture-fcm',true);
''')
credential_user='push-credentials-'+uuid.uuid4().hex
for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
    assert request(os.environ[key],'POST','/v2/subscribers',{'subscriberId':credential_user})[0]==201
# A PATCH containing only an existing subset is not the same input as the
# stored credentials and advances updatedAt, even if the token union is equal.
clock_user='push-clock-'+uuid.uuid4().hex
for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
    assert request(os.environ[key],'POST','/v2/subscribers',{'subscriberId':clock_user})[0]==201
for method,tokens,changes in [('PUT',['a','b'],True),('PUT',['a','a','b'],False),('PATCH',['a','b'],False),('PATCH',['a'],True),('PATCH',['b','a'],True),('PUT',[],True),('PATCH',[],False)]:
    for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
        base=os.environ[key];path='/v1/subscribers/'+clock_user
        status,before=request(base,'GET',path,None);assert status==200,(key,before)
        time.sleep(.025)
        status,after=request(base,method,path+'/credentials',{'providerId':'fcm','credentials':{'deviceTokens':tokens}})
        assert status==200 and (after['data']['updatedAt']!=before['data']['updatedAt'])==changes,(key,method,tokens,before,after)
for method,tokens in [('PUT',['limit-'+str(i) for i in range(101)]),('PATCH',['limit-'+str(i) for i in range(101)])]:
    results=[]
    for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
        status,body=request(os.environ[key],method,'/v1/subscribers/'+clock_user+'/credentials',{'providerId':'fcm','credentials':{'deviceTokens':tokens}})
        assert status==400,(key,status,body)
        body.pop('timestamp',None)
        results.append(body)
    assert results[0]==results[1],(method,results)
for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
    status,body=request(os.environ[key],'PUT','/v1/subscribers/'+clock_user+'/credentials',{'providerId':'fcm','credentials':{'deviceTokens':['limit-'+str(i) for i in range(99)]}})
    assert status==200,(key,status,body)
results=[]
for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
    base=os.environ[key];path='/v1/subscribers/'+clock_user
    status,before=request(base,'GET',path,None);assert status==200
    status,body=request(base,'PATCH',path+'/credentials',{'providerId':'fcm','credentials':{'deviceTokens':['limit-0','limit-99','limit-100','limit-100']}})
    assert status==400,(key,status,body)
    body.pop('timestamp',None);results.append(body)
    status,after=request(base,'GET',path,None);assert status==200
    assert after['data']['channels']==before['data']['channels'] and after['data']['updatedAt']==before['data']['updatedAt'],(key,before,after)
assert results[0]==results[1] and results[0]['currentCount']==101,results
for iteration in range(2):
    changes=[]
    for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
        base=os.environ[key];path='/v1/subscribers/'+clock_user
        status,before=request(base,'GET',path,None);assert status==200
        time.sleep(.025)
        status,body=request(base,'DELETE',path+'/credentials/fcm',{})
        assert status==204 and body is None,(key,status,body)
        status,after=request(base,'GET',path,None);assert status==200
        changes.append(after['data']['updatedAt']!=before['data']['updatedAt'])
    assert changes[0]==changes[1],('delete_timestamp',iteration,changes)
for method,tokens,want in [('PUT',['a','a','b'],['a','b']),('PATCH',['b','c','c'],['a','b','c']),('PUT',['z','z'],['z']),('PATCH',[],[])]:
    results=[]
    for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
        status,body=request(os.environ[key],method,'/v1/subscribers/'+credential_user+'/credentials',{'providerId':'fcm','credentials':{'deviceTokens':tokens}})
        assert status==200,(key,status,body)
        results.append(body['data']['channels'])
    assert results[0]==results[1]==[{'providerId':'fcm','_integrationId':'64a000000000000000000053','credentials':{'deviceTokens':want}}],(method,results)
    for version in ['v1','v2']:
        retrieved=[]
        for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
            status,body=request(os.environ[key],'GET','/'+version+'/subscribers/'+credential_user,None)
            assert status==200,(key,version,status,body)
            retrieved.append(body['data']['channels'])
        assert retrieved[0]==retrieved[1]==results[0],(version,retrieved,results)
    listed=[]
    for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
        status,body=request(os.environ[key],'GET','/v2/subscribers?subscriberId='+credential_user,None)
        assert status==200 and len(body['data'])==1,(key,status,body)
        listed.append(body['data'][0]['channels'])
        status,other=request(os.environ[key],'GET','/v2/subscribers?subscriberId='+credential_user,None,'compat-second-key')
        assert status==200 and other['data']==[],(key,status,other)
    assert listed[0]==listed[1]==results[0],(method,listed,results)
page_prefix='push-list-'+uuid.uuid4().hex
for n in range(3):
    for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
        base=os.environ[key]
        assert request(base,'POST','/v2/subscribers',{'subscriberId':page_prefix+str(n),'firstName':page_prefix})[0]==201
        if n<2:
            assert request(base,'PUT','/v1/subscribers/'+page_prefix+str(n)+'/credentials',{'providerId':'fcm','credentials':{'deviceTokens':['page-token-'+str(n)]}})[0]==200
pages=[]
for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
    base=os.environ[key];cursor=None;items=[]
    for _ in range(2):
        path='/v2/subscribers?name='+page_prefix+'&limit=2&orderDirection=ASC'+('&after='+cursor if cursor else '')
        status,body=request(base,'GET',path,None)
        assert status==200,(key,status,body)
        items.extend((row['subscriberId'],row['channels']) for row in body['data'])
        cursor=body['next']
    pages.append(items)
expected=[(page_prefix+str(n),[{'providerId':'fcm','_integrationId':'64a000000000000000000053','credentials':{'deviceTokens':['page-token-'+str(n)]}}] if n<2 else []) for n in range(3)]
assert pages[0]==pages[1]==expected,pages
# Same external ID in two environments must never mix channel credentials.
run('docker','exec',mongo,'mongosh','--quiet','--eval','''const d=db.getSiblingDB("novu-db");d.integrations.insertOne({_id:ObjectId("64a000000000000000000055"),_environmentId:ObjectId("64a000000000000000000012"),_organizationId:ObjectId("64a000000000000000000013"),providerId:"fcm",channel:"push",active:true,primary:true,credentials:{serviceAccount:JSON.stringify({project_id:"other",client_email:"other@example.test",private_key:"fixture"})},deleted:false});''')
run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',"INSERT INTO novu_rust_fcm_integrations(environment_id,internal_id,identifier,active) VALUES ('64a000000000000000000012','64a000000000000000000055','other-fcm',true)")
for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
    base=os.environ[key];other_key='compat-second-key'
    assert request(base,'POST','/v2/subscribers',{'subscriberId':page_prefix+'0'},other_key)[0]==201
    assert request(base,'PUT','/v1/subscribers/'+page_prefix+'0/credentials',{'providerId':'fcm','credentials':{'deviceTokens':['other-environment-token']}},other_key)[0]==200
    for api_key,want_id,want_token in [(os.environ['NOVU_TEST_API_KEY'],'64a000000000000000000053','page-token-0'),(other_key,'64a000000000000000000055','other-environment-token')]:
        status,body=request(base,'GET','/v2/subscribers?subscriberId='+page_prefix+'0',None,api_key)
        assert status==200 and len(body['data'])==1,(key,status,body)
        assert body['data'][0]['channels']==[{'providerId':'fcm','_integrationId':want_id,'credentials':{'deviceTokens':[want_token]}}],(key,body)
for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
    status,body=request(os.environ[key],'DELETE','/v1/subscribers/'+credential_user+'/credentials/fcm',{})
    assert status==204 and body is None
    status,body=request(os.environ[key],'GET','/v1/subscribers/'+credential_user,None)
    assert status==200 and body['data']['channels']==[],(key,body)
    status,body=request(os.environ[key],'GET','/v2/subscribers?subscriberId='+credential_user,None)
    assert status==200 and len(body['data'])==1 and body['data'][0]['channels']==[],(key,body)
    status,_=request(os.environ[key],'PUT','/v1/subscribers/'+credential_user+'/credentials',{'providerId':'fcm','credentials':{'deviceTokens':['private']}},'compat-second-key')
    assert status==400,(key,status)
print('FCM credentials replace, append, clear, delete and subscriber list matched')
for label,tokens,disabled in [('one',['device-one'],False),('two',['device-a','device-b'],False),('failed',['device-fail'],False),('invalid',['device-invalid'],False),('invalid_mixed',['device-invalid-mixed','device-good'],False),('invalid_raw',['device-invalid-raw'],False),('known_meta',['device-known-meta'],False),('missing',[],False),('disabled',['device-disabled'],True)]:
    subscriber='push-'+label+'-'+uuid.uuid4().hex
    for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
        base=os.environ[key]
        assert request(base,'POST','/v2/subscribers',{'subscriberId':subscriber,'firstName':'Jane'})[0]==201
        if disabled:assert request(base,'PATCH','/v2/subscribers/'+subscriber+'/preferences',{'channels':{'push':False}})[0]==200
    if tokens:
        for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
            status,_=request(os.environ[key],'PUT','/v1/subscribers/'+subscriber+'/credentials',{'providerId':'fcm','credentials':{'deviceTokens':tokens}})
            assert status==200,(key,status)
    txn='push-'+label+'-'+uuid.uuid4().hex
    event={'name':'push-compare','to':subscriber,'payload':{'name':'Alice'},'transactionId':txn}
    for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
        status,result=request(os.environ[key],'POST','/v1/events/trigger',event)
        assert status==201 and result['data']['status']=='processed',(key,result)
    count=0 if disabled else len(tokens)
    for _ in range(90):
        logs=subprocess.run(['docker','exec',worker,'cat','/tmp/novu-push-capture.jsonl'],text=True,capture_output=True).stdout.splitlines()
        messages=[json.loads(line) for line in logs if any(token in json.loads(line).get('tokens',[]) for token in tokens)]
        queued=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT count(*) FROM novu_rust_push_step_outcomes WHERE transaction_id='{txn}' AND status='queued'")
        source=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db");print(JSON.stringify(d.jobs.find({{transactionId:"{txn}"}}).toArray().map(j=>({{status:j.status,detail:j.deliveryLifecycleState?.detail}}))));'''))
        terminal=any(j['status'] in ('skipped','canceled','failed') for j in source)
        if queued=='1' and ((count and len(messages)==count) or (not count and terminal)):break
        time.sleep(.5)
    else:raise AssertionError((label,queued,source,messages))
    with tempfile.TemporaryDirectory(prefix='novu-push-compare-') as directory:
        capture=Path(directory)/'capture.json'
        env=dict(os.environ,TEST_PUSH_CAPTURE_PATH=str(capture),TEST_PUSH_EXPECTED_COUNT=str(count))
        if label=='failed':env['TEST_PUSH_HTTP_STATUS']='429'
        if label=='invalid':env['TEST_PUSH_HTTP_STATUS']='404'
        if label=='invalid_mixed':env['TEST_PUSH_INVALID_TARGET']='device-invalid-mixed'
        if label=='invalid_raw':env.update({'TEST_PUSH_HTTP_STATUS':'404','TEST_PUSH_ERROR_BODY':'Requested entity was not found.'})
        if label=='known_meta':env.update({'TEST_PUSH_HTTP_STATUS':'404','TEST_PUSH_ERROR_BODY':json.dumps({'error':{'status':'NOT_FOUND','message':'fixture FCM error','details':[{'reason':'Requested entity was not found'}]}})})
        subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_push','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=env,check=True)
        rust=json.loads(capture.read_text())
    if count:
        ref_by_token={m['tokens'][0]:m for m in messages}
        rust_by_token={m['message']['token']:m['message'] for m in rust}
        assert set(ref_by_token)==set(rust_by_token)==set(tokens)
        ref_ids={m['data']['__nvMessageId'] for m in ref_by_token.values()}
        rust_ids={m['data']['__nvMessageId'] for m in rust_by_token.values()}
        for token in tokens:
            original=ref_by_token[token];original['token']=original.pop('tokens')[0]
            rust_message=rust_by_token[token]
            for message in (original,rust_message):
                assert re.fullmatch('[0-9a-f]{24}',message['data'].pop('__nvMessageId'))
            assert original==rust_message,(label,original,rust_message)
    else:
        assert rust==[]
        outcome=run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT status FROM novu_rust_push_step_outcomes WHERE transaction_id='{txn}'")
        assert outcome==('canceled' if disabled else 'missing_device'),(label,outcome,source)
        assert any(j.get('detail')==('preference' if disabled else 'missing_push_token') for j in source),(label,source)
    for _ in range(30):
        original_history=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db");print(JSON.stringify(d.messages.find({{transactionId:"{txn}"}}).toArray().map(m=>({{id:String(m._id),identifier:m.identifier??null,status:m.status,title:m.title,content:m.content??null,deviceTokens:m.deviceTokens,errorId:m.errorId??null,errorText:m.errorText??null}}))));'''))
        if len(original_history)==count and all(m['identifier']==m['id'] and (label not in ('failed','invalid','invalid_raw','known_meta') or m['status']=='error') for m in original_history):break
        time.sleep(.25)
    rust_history=json.loads(run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT COALESCE(json_agg(json_build_object('id',internal_id,'identifier',identifier,'status',status,'title',title,'content',content,'deviceTokens',device_tokens,'errorId',error_id,'errorText',error_text)),'[]'::json)::text FROM novu_rust_push_messages WHERE transaction_id='{txn}'"))
    assert len(original_history)==len(rust_history)==count,(label,original_history,rust_history)
    if count:
        assert {m['id'] for m in original_history}==ref_ids,(label,original_history,ref_ids)
        assert {m['id'] for m in rust_history}==rust_ids,(label,rust_history,rust_ids)
    for entries in (original_history,rust_history):
        for item in entries:
            assert re.fullmatch('[0-9a-f]{24}',item['id']) and item['identifier']==item['id'],item
            item.pop('id');item.pop('identifier')
            if item['status']=='error':
                assert item['errorText']
                item['errorText']='redacted'
    assert sorted(original_history,key=lambda v:json.dumps(v,sort_keys=True))==sorted(rust_history,key=lambda v:json.dumps(v,sort_keys=True)),(label,original_history,rust_history)
    if label in ('invalid','invalid_mixed','invalid_raw','known_meta','failed'):
        for _ in range(60):
            source=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db");print(JSON.stringify(d.jobs.find({{transactionId:"{txn}",type:"push"}}).toArray().map(j=>j.status)));'''))
            if source and all(status in ('completed','failed','canceled','skipped') for status in source):break
            time.sleep(.25)
        else:raise AssertionError(('source push did not finish',source))
        expected=[token for token in tokens if not (label in ('invalid','invalid_mixed','invalid_raw') and os.environ.get('IS_EXPIRED_TOKENS_REMOVAL_ENABLED')=='true' and token.startswith('device-invalid'))]
        for key in ['NOVU_REFERENCE_URL','NOVU_RUST_URL']:
            status,body=request(os.environ[key],'GET','/v1/subscribers/'+subscriber,None)
            assert status==200 and body['data']['channels'][0]['credentials']['deviceTokens']==expected,(key,body,expected)
    print(label+': selected push workflow behavior matched')
