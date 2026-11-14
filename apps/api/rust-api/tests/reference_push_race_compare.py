#!/usr/bin/env python3
"""Compare token cleanup after mutations during an in-flight mocked FCM send.

Only run against the disposable reference harness; it creates fixtures and
uses barriers in the reference and Rust mock providers.
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

assert os.environ.get('IS_EXPIRED_TOKENS_REMOVAL_ENABLED') == 'true'
mongo, pg, worker = (os.environ[k] for k in ('NOVU_REFERENCE_MONGO_CONTAINER','NOVU_RUST_PG_CONTAINER','NOVU_REFERENCE_WORKER_CONTAINER'))
bases = [os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
def run(*args):
    return subprocess.check_output(args,text=True).strip()
def request(base,method,path,body=None):
    req=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers={'Content-Type':'application/json','Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']},method=method)
    try: response=urllib.request.urlopen(req,timeout=5)
    except urllib.error.HTTPError as error: response=error
    with response:
        data=response.read()
        return response.status,json.loads(data) if data else None
def wait_for(fn, seconds=75):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        result=fn()
        if result:return result
        time.sleep(.1)
    raise AssertionError('timed out waiting for test barrier')
def tokens(base,subscriber,values):
    status,body=request(base,'PUT','/v1/subscribers/'+subscriber+'/credentials',{'providerId':'fcm','credentials':{'deviceTokens':values}})
    assert status==200,(base,status,body)

run('docker','exec',mongo,'mongosh','--quiet','--eval','''
const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),w=ObjectId("64a000000000000000000061"),m=ObjectId("64a000000000000000000062"),i=ObjectId("64a000000000000000000063");
d.integrations.insertOne({_id:i,_environmentId:e,_organizationId:o,providerId:"fcm",channel:"push",active:true,primary:true,credentials:{serviceAccount:JSON.stringify({project_id:"demo",client_email:"fixture@example.test",private_key:"fixture"})},deleted:false});
d.messagetemplates.insertOne({_id:m,_environmentId:e,_organizationId:o,type:"push",active:true,title:"Hi",content:"Hello {{subscriber.subscriberId}}",deleted:false});
d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:o,name:"Push Race",active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],triggers:[{type:"event",identifier:"push-race"}],steps:[{active:true,_templateId:m}],createdAt:new Date(),updatedAt:new Date()});
d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:w,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{push:{enabled:true}}},deleted:false});
''')
run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c','''
INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,push_title,push_content,inbox_metadata) VALUES ('64a000000000000000000002','push-race',true,true,'Hi','Hello {{subscriber.subscriberId}}','{"id":"64a000000000000000000061"}');
INSERT INTO novu_rust_fcm_integrations(environment_id,internal_id,identifier,active) VALUES ('64a000000000000000000002','64a000000000000000000063','fixture-fcm',true);
''')

all_cases=('change_tokens','remove_token','readd_token','same_tokens','same_tokens_patch','recreate_subscriber','delete_subscriber')
cases=os.environ.get('TEST_PUSH_RACE_CASES',','.join(all_cases)).split(',')
assert cases and all(case in all_cases for case in cases),cases
for case in cases:
    suffix=uuid.uuid4().hex
    subscriber='push-race-'+suffix
    token='device-invalid-barrier-'+suffix
    good='valid-'+suffix
    original_ids=[]
    for base in bases:
        status,body=request(base,'POST','/v2/subscribers',{'subscriberId':subscriber})
        assert status==201,(base,status,body)
        original_ids.append(body['data']['_id'])
        tokens(base,subscriber,[token])
        status,body=request(base,'POST','/v1/events/trigger',{'name':'push-race','to':subscriber,'transactionId':subscriber})
        assert status==201,(base,status,body)
    def source_request_seen():
        result=subprocess.run(['docker','exec',worker,'cat','/tmp/novu-push-capture.jsonl'],capture_output=True,text=True)
        return any(token in json.loads(line).get('tokens',[]) for line in result.stdout.splitlines())
    wait_for(source_request_seen)
    job=wait_for(lambda:run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT push_job_id FROM novu_rust_push_step_outcomes WHERE transaction_id='{subscriber}' AND status='queued'"))
    with tempfile.TemporaryDirectory(prefix='novu-push-race-') as directory:
        barrier=Path(directory)
        capture=barrier/'capture.json'
        process=subprocess.Popen(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_push','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_PUSH_CAPTURE_PATH=str(capture),TEST_PUSH_EXPECTED_COUNT='1',TEST_PUSH_HTTP_STATUS='404',TEST_PUSH_BARRIER_DIR=directory),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
        try:
            def ready():
                if process.poll() is not None: raise AssertionError(('rust mock exited early',process.communicate()[0]))
                return (barrier/'ready').exists()
            wait_for(ready)
            if case=='delete_subscriber':
                # Seed all three channels for the target, a different internal
                # identity in the same environment, and the same identity in a
                # different environment. Only the first group may be removed.
                ref_id,rust_id=original_ids
                other='64a000000000000000000099'
                history='history-'+subscriber
                run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db");for(const [env,id] of [["64a000000000000000000002","{ref_id}"],["64a000000000000000000002","{other}"],["64a000000000000000000012","{ref_id}"]])for(const channel of ["email","sms","push"])d.messages.insertOne({{_environmentId:ObjectId(env),_subscriberId:ObjectId(id),transactionId:"{history}",channel}});''')
                statements=[]
                for env,internal in [('64a000000000000000000002',rust_id),('64a000000000000000000002',other),('64a000000000000000000012',rust_id)]:
                    for channel in ('email','sms','push'):
                        mid=uuid.uuid4().hex[:24]
                        common=f"'{mid}','{env}','{subscriber}','{internal}','notification','push-race','{history}'"
                        if channel=='email':statements.append(f"INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status) VALUES ({common},'subject','sent')")
                        elif channel=='sms':statements.append(f"INSERT INTO novu_rust_sms_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,status) VALUES ({common},'sent')")
                        else:statements.append(f"INSERT INTO novu_rust_push_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,device_tokens,title,status,push_job_id,target_index) VALUES ({common},'{{}}','title','sent','{job}',{998+len(statements)})")
                run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',';'.join(statements))
            for index,base in enumerate(bases):
                if case=='change_tokens':tokens(base,subscriber,[good,token])
                elif case=='remove_token':tokens(base,subscriber,[good])
                elif case=='same_tokens':
                    if base==bases[1] and os.environ.get('SWALLOW_FCM_REASSERT_REGISTRATION')=='true':
                        run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c','ALTER TABLE novu_rust_fcm_devices DISABLE TRIGGER novu_rust_fcm_device_cleanup_generation')
                        try:
                            status,_=request(base,'PUT','/v1/subscribers/'+subscriber+'/credentials',{'providerId':'fcm','credentials':{'deviceTokens':[token]}})
                            assert status==503,status
                        finally:
                            run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c','ALTER TABLE novu_rust_fcm_devices ENABLE TRIGGER novu_rust_fcm_device_cleanup_generation')
                        run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c','ALTER FUNCTION public.novu_rust_assert_fcm_registration_guard() RENAME TO novu_rust_assert_fcm_guard_temporarily_missing')
                        try:
                            status,_=request(base,'PUT','/v1/subscribers/'+subscriber+'/credentials',{'providerId':'fcm','credentials':{'deviceTokens':[token]}})
                            assert status==503,status
                        finally:
                            run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c','ALTER FUNCTION public.novu_rust_assert_fcm_guard_temporarily_missing() RENAME TO novu_rust_assert_fcm_registration_guard')
                        run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',"CREATE FUNCTION public.test_fcm_skip_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$; CREATE TRIGGER zz_test_fcm_skip_update BEFORE UPDATE ON novu_rust_fcm_devices FOR EACH ROW EXECUTE FUNCTION public.test_fcm_skip_update()")
                        try:
                            status,_=request(base,'PUT','/v1/subscribers/'+subscriber+'/credentials',{'providerId':'fcm','credentials':{'deviceTokens':[token]}})
                            assert status==500,status
                        finally:
                            run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c','DROP TRIGGER zz_test_fcm_skip_update ON novu_rust_fcm_devices; DROP FUNCTION public.test_fcm_skip_update()')
                    tokens(base,subscriber,[token])
                elif case=='same_tokens_patch':
                    status,_=request(base,'PATCH','/v1/subscribers/'+subscriber+'/credentials',{'providerId':'fcm','credentials':{'deviceTokens':[token]}})
                    assert status==200,status
                elif case=='readd_token':
                    tokens(base,subscriber,[])
                    tokens(base,subscriber,[token,good])
                elif case in ('recreate_subscriber','delete_subscriber'):
                    status,body=request(base,'DELETE','/v2/subscribers/'+subscriber)
                    assert status==200,(base,status,body)
                    if case=='recreate_subscriber':
                        status,body=request(base,'POST','/v2/subscribers',{'subscriberId':subscriber})
                        assert status==201 and body['data']['_id']!=original_ids[index],(base,status,body)
                        tokens(base,subscriber,[token,good])
        finally:
            (barrier/'release').touch()
            run('docker','exec',worker,'touch','/tmp/novu-release-'+token)
        output=process.communicate(timeout=30)[0]
        assert process.returncode==0,(case,output)
        sent=json.loads(capture.read_text())
        assert len(sent)==1 and sent[0]['message']['token']==token,(case,sent)
    def source_finished():
        jobs=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''print(JSON.stringify(db.getSiblingDB("novu-db").jobs.find({{transactionId:"{subscriber}",type:"push"}}).toArray().map(j=>j.status)))'''))
        return jobs and all(status in ('completed','failed','canceled','skipped') for status in jobs)
    wait_for(source_finished)
    ref_messages=int(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''print(db.getSiblingDB("novu-db").messages.countDocuments({{transactionId:"{subscriber}"}}))'''))
    rust_messages=int(run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT count(*) FROM novu_rust_push_messages WHERE transaction_id='{subscriber}'"))
    expected_messages=0 if case in ('recreate_subscriber','delete_subscriber') else 1
    assert ref_messages==rust_messages==expected_messages,(case,ref_messages,rust_messages)
    if case=='delete_subscriber':
        ref_counts=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db");print(JSON.stringify(["email","sms","push"].map(channel=>d.messages.countDocuments({{transactionId:"history-{subscriber}",channel}}))));'''))
        rust_counts=json.loads(run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT json_build_array((SELECT count(*) FROM novu_rust_email_messages WHERE transaction_id='history-{subscriber}'),(SELECT count(*) FROM novu_rust_sms_messages WHERE transaction_id='history-{subscriber}'),(SELECT count(*) FROM novu_rust_push_messages WHERE transaction_id='history-{subscriber}'))::text"))
        assert ref_counts==rust_counts==[2,2,2],(ref_counts,rust_counts)
        ref_scopes=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db");print(JSON.stringify([["64a000000000000000000002","{original_ids[0]}"],["64a000000000000000000002","64a000000000000000000099"],["64a000000000000000000012","{original_ids[0]}"]].map(([e,s])=>d.messages.countDocuments({{transactionId:"history-{subscriber}",_environmentId:ObjectId(e),_subscriberId:ObjectId(s)}}))));'''))
        queries=[]
        for env,internal in [('64a000000000000000000002',original_ids[1]),('64a000000000000000000002','64a000000000000000000099'),('64a000000000000000000012',original_ids[1])]:
            queries.append('('+'+'.join(f"(SELECT count(*) FROM novu_rust_{channel}_messages WHERE transaction_id='history-{subscriber}' AND environment_id='{env}' AND subscriber_internal_id='{internal}')" for channel in ('email','sms','push'))+')')
        rust_scopes=json.loads(run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c','SELECT json_build_array('+','.join(queries)+')::text'))
        assert ref_scopes==rust_scopes==[0,3,3],(ref_scopes,rust_scopes)
    for base in bases:
        status,body=request(base,'GET','/v1/subscribers/'+subscriber)
        if case=='delete_subscriber':
            assert status==404,(base,status,body)
        else:
            expected=[token,good] if case=='recreate_subscriber' else [good]
            if case in ('same_tokens','same_tokens_patch'): expected=[]
            if os.environ.get('TEST_PUSH_CLEANUP_GUARD')=='version' and base==bases[1]:
                if case=='change_tokens': expected=[good,token]
                if case=='readd_token': expected=[token,good]
                if case in ('same_tokens','same_tokens_patch') and os.environ.get('SWALLOW_FCM_REASSERT_REGISTRATION')=='true': expected=[token]
            assert status==200 and body['data']['channels'][0]['credentials']['deviceTokens']==expected,(case,base,status,body,expected)
    print(case+': in-flight invalid-token cleanup verified')
