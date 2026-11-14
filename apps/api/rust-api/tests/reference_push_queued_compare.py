#!/usr/bin/env python3
"""Compare push changes after channel jobs are queued and before delivery."""
import json,os,re,subprocess,tempfile,time,urllib.request,uuid
from pathlib import Path
mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER'];pg=os.environ['NOVU_RUST_PG_CONTAINER'];worker=os.environ['NOVU_REFERENCE_WORKER_CONTAINER']
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
def run(*args):return subprocess.check_output(args,text=True).strip()
def mq(js):return run('docker','exec',mongo,'mongosh','--quiet','--eval',js)
def pq(sql):return run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql)
def request(base,method,path,body=None):
    req=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers={'Content-Type':'application/json','Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']},method=method)
    with urllib.request.urlopen(req,timeout=10) as res:
        data=res.read();return res.status,json.loads(data) if data else None
def queue_action(action):
    script="const {Queue}=require('/app/libs/application-generic/node_modules/bullmq');(async()=>{let q=new Queue('standard',{connection:{host:process.env.REDIS_HOST,port:6379}});await q.%s();await q.close()})().catch(e=>{console.error(e);process.exit(1)})" % action
    run('docker','exec',worker,'node','-e',script)
mq('''const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),i=ObjectId("64a000000000000000000053");
d.integrations.insertOne({_id:i,_environmentId:e,_organizationId:o,providerId:"fcm",channel:"push",active:true,primary:true,credentials:{serviceAccount:JSON.stringify({project_id:"demo",client_email:"fixture@example.test",private_key:"fixture"})},deleted:false});
for(const [w,m,name,title,content] of [["64a000000000000000000031","64a000000000000000000032","push-queued","Hi {{subscriber.firstName}}","{{subscriber.subscriberId}} {{actor.firstName}} {{context.tenant.data.plan}}"],["64a000000000000000000041","64a000000000000000000042","push-template-snapshot","Original","Original {{subscriber.subscriberId}}"],["64a000000000000000000043","64a000000000000000000044","push-workflow-deleted","Original","Original {{subscriber.subscriberId}}"],["64a000000000000000000045","64a000000000000000000046","push-workflow-recreated","Original","Original {{subscriber.subscriberId}}"]]) {const wi=ObjectId(w),mi=ObjectId(m);d.messagetemplates.insertOne({_id:mi,_environmentId:e,_organizationId:o,type:"push",active:true,title,content,deleted:false});d.notificationtemplates.insertOne({_id:wi,_environmentId:e,_organizationId:o,name,active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],triggers:[{type:"event",identifier:name}],steps:[{active:true,_templateId:mi}],createdAt:new Date(),updatedAt:new Date()});d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:wi,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{push:{enabled:true}}},deleted:false});}''')
pq('''INSERT INTO novu_rust_fcm_integrations(environment_id,internal_id,identifier,active) VALUES ('64a000000000000000000002','64a000000000000000000053','fixture-fcm',true);
INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,push_title,push_content,inbox_metadata) VALUES
('64a000000000000000000002','push-queued',true,true,'Hi {{subscriber.firstName}}','{{subscriber.subscriberId}} {{actor.firstName}} {{context.tenant.data.plan}}','{"id":"64a000000000000000000031"}'),
('64a000000000000000000002','push-template-snapshot',true,true,'Original','Original {{subscriber.subscriberId}}','{"id":"64a000000000000000000041"}'),
('64a000000000000000000002','push-workflow-deleted',true,true,'Original','Original {{subscriber.subscriberId}}','{"id":"64a000000000000000000043"}'),
('64a000000000000000000002','push-workflow-recreated',true,true,'Original','Original {{subscriber.subscriberId}}','{"id":"64a000000000000000000045"}');''')
labels=['changed','disabled','deleted','recreated','enabled_later','device_added','device_removed','actor_context','template_changed','workflow_deleted','workflow_recreated']
cases=[(label,'push-queued-'+label+'-'+uuid.uuid4().hex) for label in labels]
# Allow the Rust monitoring tool to inspect and acknowledge only this fixture environment.
pq("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT '64a000000000000000000002',session_user,decision,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','queue_ack']) decision WHERE rolname=session_user ON CONFLICT DO NOTHING")
queue_root=Path(__file__).resolve().parents[3]/'worker'/'rust-queue'
def monitor(*args):
    return subprocess.run(['cargo','run','--quiet','--locked','--offline','--bin','delivery-reconcile','--',*args],cwd=queue_root,env=dict(os.environ,DATABASE_URL=os.environ['TEST_DATABASE_URL'],NOVU_POSTGRES_ALLOW_INSECURE='true'),capture_output=True,text=True,check=False)
actor='push-actor-'+uuid.uuid4().hex; context='push-tenant-'+uuid.uuid4().hex;jobs={}
def set_tokens(base,subscriber,tokens):
    return request(base,'PUT','/v1/subscribers/'+subscriber+'/credentials',{'providerId':'fcm','credentials':{'deviceTokens':tokens}})[0]
queue_action('pause')
try:
    for label,subscriber in cases:
        for base in bases:
            assert request(base,'POST','/v2/subscribers',{'subscriberId':subscriber,'firstName':'Before'})[0]==201
            if label!='device_added':assert set_tokens(base,subscriber,['before-'+label])==200
            if label=='enabled_later':assert request(base,'PATCH','/v2/subscribers/'+subscriber+'/preferences',{'channels':{'push':False}})[0]==200
            event={'name':{'template_changed':'push-template-snapshot','workflow_deleted':'push-workflow-deleted','workflow_recreated':'push-workflow-recreated'}.get(label,'push-queued'),'to':subscriber,'transactionId':subscriber}
            if label=='actor_context':event.update({'actor':{'subscriberId':actor,'firstName':'ActorBefore'},'context':{'tenant':{'id':context,'data':{'plan':'basic'}}}})
            assert request(base,'POST','/v1/events/trigger',event)[0]==201
    for label,subscriber in cases:
        for _ in range(90):
            ref=int(mq(f'print(db.getSiblingDB("novu-db").jobs.countDocuments({{transactionId:"{subscriber}",type:"push"}}))'))
            job=pq(f"SELECT push_job_id FROM novu_rust_push_step_outcomes WHERE transaction_id='{subscriber}' AND status='queued'")
            if ref==1 and job:break
            time.sleep(.5)
        assert ref==1 and job,(label,ref,job)
        jobs[label]=job
    for label,subscriber in cases:
        for base in bases:
            if label=='changed':
                assert request(base,'PATCH','/v2/subscribers/'+subscriber,{'firstName':'After'})[0]==200
                status=set_tokens(base,subscriber,['after-'+label])
            elif label=='disabled':status=request(base,'PATCH','/v2/subscribers/'+subscriber+'/preferences',{'channels':{'push':False}})[0]
            elif label=='deleted':status=request(base,'DELETE','/v2/subscribers/'+subscriber)[0]
            elif label=='recreated':
                assert request(base,'DELETE','/v2/subscribers/'+subscriber)[0]==200
                assert request(base,'POST','/v2/subscribers',{'subscriberId':subscriber,'firstName':'Recreated'})[0]==201
                status=set_tokens(base,subscriber,['after-'+label])
            elif label=='enabled_later':status=request(base,'PATCH','/v2/subscribers/'+subscriber+'/preferences',{'channels':{'push':True}})[0]
            elif label=='device_added':status=set_tokens(base,subscriber,['after-'+label])
            elif label=='device_removed':status=set_tokens(base,subscriber,[])
            elif label=='actor_context':
                assert request(base,'PATCH','/v2/subscribers/'+actor,{'firstName':'ActorAfter'})[0]==200
                status=request(base,'PATCH','/v2/contexts/tenant/'+context,{'data':{'plan':'premium'}})[0]
            else:continue
            assert status==200,(label,base,status)
    mq('''const d=db.getSiblingDB("novu-db");d.messagetemplates.updateOne({_id:ObjectId("64a000000000000000000042")},{$set:{title:"Changed",content:"Changed"}});d.notificationtemplates.deleteOne({_id:ObjectId("64a000000000000000000043")});const old=d.notificationtemplates.findOne({_id:ObjectId("64a000000000000000000045")});d.notificationtemplates.deleteOne({_id:old._id});old._id=ObjectId("64a000000000000000000047");d.notificationtemplates.insertOne(old);''')
    pq('''UPDATE novu_rust_workflows SET push_title='Changed',push_content='Changed' WHERE environment_id='64a000000000000000000002' AND identifier='push-template-snapshot'; DELETE FROM novu_rust_workflows WHERE environment_id='64a000000000000000000002' AND identifier IN ('push-workflow-deleted','push-workflow-recreated'); INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,push_title,push_content,inbox_metadata) VALUES ('64a000000000000000000002','push-workflow-recreated',true,true,'Replacement','Replacement','{"id":"64a000000000000000000047"}');''')
    pq("UPDATE novu_queue_jobs SET available_at=now()+interval '1 hour' WHERE kind='experimental_fcm_push' AND status='pending'")
finally:queue_action('resume')

for label,subscriber in cases:
    for _ in range(90):
        ref=json.loads(mq(f'''const d=db.getSiblingDB("novu-db");print(JSON.stringify({{jobs:d.jobs.find({{transactionId:"{subscriber}"}}).toArray().map(j=>({{status:j.status,detail:j.deliveryLifecycleState?.detail}})),messages:d.messages.find({{transactionId:"{subscriber}"}}).toArray().map(m=>({{status:m.status}}))}}));'''))
        log=subprocess.run(['docker','exec',worker,'cat','/tmp/novu-push-capture.jsonl'],capture_output=True,text=True).stdout.splitlines()
        captured=[m for m in map(json.loads,log) if subscriber in m.get('notification',{}).get('body','')]
        if ref['jobs'] and all(j['status'] in ('completed','skipped','canceled','failed') for j in ref['jobs']):break
        time.sleep(.5)
    should_send=label in ('changed','recreated','enabled_later','device_added','actor_context','template_changed')
    pq(f"UPDATE novu_queue_jobs SET available_at=now() WHERE id='{jobs[label]}'")
    with tempfile.TemporaryDirectory(prefix='novu-push-queued-') as directory:
        capture=Path(directory)/'capture.json'
        subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_push','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_PUSH_CAPTURE_PATH=str(capture),TEST_PUSH_EXPECTED_COUNT='1' if should_send else '0'),check=True)
        rust_requests=json.loads(capture.read_text())
    rust=json.loads(pq(f"SELECT json_build_object('status',q.status,'outcome',o.status,'messages',(SELECT count(*) FROM novu_rust_push_messages m WHERE m.environment_id=q.environment_id AND m.transaction_id='{subscriber}'))::text FROM novu_queue_jobs q JOIN novu_rust_push_step_outcomes o ON o.environment_id=q.environment_id AND o.transaction_id='{subscriber}' WHERE q.id='{jobs[label]}'"))
    if should_send:
        assert len(captured)==len(rust_requests)==1,(label,ref,captured,rust)
        original=captured[0];original['token']=original.pop('tokens')[0];actual=rust_requests[0]['message']
        for message in (original,actual):assert re.fullmatch('[0-9a-f]{24}',message['data'].pop('__nvMessageId'))
        assert original==actual,(label,original,actual)
        if label in ('changed','recreated','device_added'):assert actual['token']=='after-'+label,actual
        if label=='actor_context':assert 'ActorAfter premium' in actual['notification']['body'],actual
        if label=='template_changed':assert actual['notification']['title']=='Original',actual
        if label=='recreated':
            identities=json.loads(mq(f'''const d=db.getSiblingDB("novu-db"),s=d.subscribers.findOne({{subscriberId:"{subscriber}"}}),j=d.jobs.findOne({{transactionId:"{subscriber}",type:"push"}}),m=d.messages.findOne({{transactionId:"{subscriber}"}});print(JSON.stringify({{current:String(s._id),job:String(j._subscriberId),message:String(m._subscriberId)}}));'''))
            rust_ids=json.loads(pq(f"SELECT json_build_object('current',s.internal_id,'job',o.subscriber_internal_id,'message',m.subscriber_internal_id)::text FROM novu_rust_subscribers s JOIN novu_rust_push_step_outcomes o ON o.environment_id=s.environment_id AND o.subscriber_id=s.subscriber_id JOIN novu_rust_push_messages m ON m.environment_id=o.environment_id AND m.transaction_id=o.transaction_id WHERE s.environment_id='64a000000000000000000002' AND o.transaction_id='{subscriber}'"))
            for ids in (identities,rust_ids):assert ids['job']==ids['message'] and ids['current']!=ids['job'],ids
        assert rust['status']=='completed' and rust['messages']==1,rust
    elif label in ('disabled','device_removed'):
        assert not captured and not rust_requests and rust['status']=='cancelled' and rust['outcome']==('canceled' if label=='disabled' else 'missing_device') and rust['messages']==0,(label,ref,rust)
    else:
        assert not captured and not rust_requests and rust['status']=='failed' and rust['outcome']=='failed' and rust['messages']==0,(label,ref,rust)
        assert any(j['status']=='failed' for j in ref['jobs']),ref
        if label=='deleted':
            environment='64a000000000000000000002'
            check=monitor('check-failed-jobs',environment);assert check.returncode==2,(check.stdout,check.stderr)
            listed=monitor('list-failed-jobs',environment);assert listed.returncode==0,listed.stderr
            assert jobs[label] in [row['jobId'] for row in json.loads(listed.stdout)],listed.stdout
            ack=monitor('ack-failed-job',environment,jobs[label],'fixture-operator','verified test');assert ack.returncode==0,ack.stderr
            listed=monitor('list-failed-jobs',environment);assert listed.returncode==0 and jobs[label] not in [row['jobId'] for row in json.loads(listed.stdout)],listed.stdout
            assert pq(f"SELECT status FROM novu_queue_jobs WHERE id='{jobs[label]}'")=='failed'
            print('HTTP trigger through FCM failure, monitoring and acknowledgement verified on Rust')
    print(label+': selected queued push behavior matched')

# Integration selection is evaluated after the channel job has been created.
# Use direct fixture changes on both databases: integration management is not
# implemented in Rust and the reference repository reads these records at send.
for label in ('integration_inactive','integration_deleted','integration_replaced','integration_reactivated','inactive_empty_tokens'):
    subscriber='push-'+label+'-'+uuid.uuid4().hex
    queue_action('pause')
    try:
        for base in bases:
            assert request(base,'POST','/v2/subscribers',{'subscriberId':subscriber,'firstName':'Before'})[0]==201
            assert set_tokens(base,subscriber,[] if label=='inactive_empty_tokens' else ['integration-token'])==200
            assert request(base,'POST','/v1/events/trigger',{'name':'push-queued','to':subscriber,'transactionId':subscriber})[0]==201
        for _ in range(90):
            ref_count=int(mq(f'print(db.getSiblingDB("novu-db").jobs.countDocuments({{transactionId:"{subscriber}",type:"push"}}))'))
            job=pq(f"SELECT push_job_id FROM novu_rust_push_step_outcomes WHERE transaction_id='{subscriber}' AND status='queued'")
            if ref_count==1 and job:break
            time.sleep(.5)
        assert ref_count==1 and job,(label,ref_count,job)
        pq(f"UPDATE novu_queue_jobs SET available_at=now()+interval '1 hour' WHERE id='{job}'")
        if label in ('integration_inactive','inactive_empty_tokens','integration_reactivated'):
            mq('db.getSiblingDB("novu-db").integrations.updateOne({_id:ObjectId("64a000000000000000000053")},{$set:{active:false}})')
            pq("UPDATE novu_rust_fcm_integrations SET active=false WHERE environment_id='64a000000000000000000002'")
            if label=='integration_reactivated':
                mq('db.getSiblingDB("novu-db").integrations.updateOne({_id:ObjectId("64a000000000000000000053")},{$set:{active:true}})')
                pq("UPDATE novu_rust_fcm_integrations SET active=true WHERE environment_id='64a000000000000000000002'")
        else:
            mq('''const d=db.getSiblingDB("novu-db"),i=d.integrations.findOne({_id:ObjectId("64a000000000000000000053")});d.integrations.deleteOne({_id:i._id});if('''+('true' if label=='integration_replaced' else 'false')+'''){i._id=ObjectId("64a000000000000000000054");d.integrations.insertOne(i)}''')
            if label=='integration_replaced':pq("UPDATE novu_rust_fcm_integrations SET internal_id='64a000000000000000000054' WHERE environment_id='64a000000000000000000002'")
            else:pq("DELETE FROM novu_rust_fcm_integrations WHERE environment_id='64a000000000000000000002'")
    finally:queue_action('resume')
    try:
        for _ in range(90):
            ref=json.loads(mq(f'''const d=db.getSiblingDB("novu-db");print(JSON.stringify({{jobs:d.jobs.find({{transactionId:"{subscriber}",type:"push"}}).toArray().map(j=>({{status:j.status,detail:j.deliveryLifecycleState?.detail}})),messages:d.messages.countDocuments({{transactionId:"{subscriber}"}})}}));'''))
            if ref['jobs'] and all(j['status'] in ('completed','skipped','canceled','failed') for j in ref['jobs']):break
            time.sleep(.5)
        assert ref['jobs'] and all(j['status'] in ('completed','skipped','canceled','failed') for j in ref['jobs']),(label,ref)
        should_send=label=='integration_reactivated'
        pq(f"UPDATE novu_queue_jobs SET available_at=now() WHERE id='{job}'")
        with tempfile.TemporaryDirectory(prefix='novu-push-integration-') as directory:
            capture=Path(directory)/'capture.json'
            subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_push','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_PUSH_CAPTURE_PATH=str(capture),TEST_PUSH_EXPECTED_COUNT='1' if should_send else '0'),check=True)
            rust_requests=json.loads(capture.read_text())
        rust=json.loads(pq(f"SELECT json_build_object('status',q.status,'outcome',o.status,'messages',(SELECT count(*) FROM novu_rust_push_messages m WHERE m.environment_id=q.environment_id AND m.transaction_id='{subscriber}'))::text FROM novu_queue_jobs q JOIN novu_rust_push_step_outcomes o ON o.environment_id=q.environment_id AND o.transaction_id='{subscriber}' WHERE q.id='{job}'"))
        log=subprocess.run(['docker','exec',worker,'cat','/tmp/novu-push-capture.jsonl'],capture_output=True,text=True).stdout.splitlines()
        captured=[m for m in map(json.loads,log) if subscriber in m.get('notification',{}).get('body','')]
        expected='completed' if should_send else 'canceled' if label=='inactive_empty_tokens' else 'failed'
        assert ref['jobs'][0]['status']==expected,(label,ref)
        assert len(captured)==len(rust_requests)==(1 if should_send else 0),(label,ref,rust,captured,rust_requests)
        assert ref['messages']==rust['messages']==(1 if should_send else 0),(label,ref,rust)
        assert rust['status']==('cancelled' if label=='inactive_empty_tokens' else expected),(label,ref,rust)
        assert rust['outcome']==('queued' if should_send else 'missing_device' if label=='inactive_empty_tokens' else 'failed'),(label,ref,rust)
        print(label+': selected integration behavior matched')
    finally:
        mq('''const d=db.getSiblingDB("novu-db");d.integrations.deleteMany({_id:{$in:[ObjectId("64a000000000000000000053"),ObjectId("64a000000000000000000054")]}});d.integrations.insertOne({_id:ObjectId("64a000000000000000000053"),_environmentId:ObjectId("64a000000000000000000002"),_organizationId:ObjectId("64a000000000000000000003"),providerId:"fcm",channel:"push",active:true,primary:true,credentials:{serviceAccount:JSON.stringify({project_id:"demo",client_email:"fixture@example.test",private_key:"fixture"})},deleted:false});''')
        pq("DELETE FROM novu_rust_fcm_integrations WHERE environment_id='64a000000000000000000002'; INSERT INTO novu_rust_fcm_integrations(environment_id,internal_id,identifier,active) VALUES ('64a000000000000000000002','64a000000000000000000053','fixture-fcm',true)")
