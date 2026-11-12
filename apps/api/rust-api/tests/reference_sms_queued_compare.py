#!/usr/bin/env python3
"""Compare SMS recipient and workflow changes after jobs are queued."""
import json, os, subprocess, tempfile, time, urllib.request, uuid
from pathlib import Path

mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
pg=os.environ['NOVU_RUST_PG_CONTAINER']
worker=os.environ['NOVU_REFERENCE_WORKER_CONTAINER']
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
def run(*args): return subprocess.check_output(args,text=True).strip()
def request(base,method,path,body=None):
    data=json.dumps(body).encode() if body is not None else None
    req=urllib.request.Request(base+path,data=data,headers={'Content-Type':'application/json','Authorization':'ApiKey '+os.environ['NOVU_TEST_API_KEY']},method=method)
    with urllib.request.urlopen(req,timeout=10) as response:
        raw=response.read(); return response.status,json.loads(raw) if raw else None
def mq(js): return run('docker','exec',mongo,'mongosh','--quiet','--eval',js)
def pq(sql): return run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql)
def queue_action(action):
    script="const {Queue}=require('/app/libs/application-generic/node_modules/bullmq');(async()=>{let q=new Queue('standard',{connection:{host:process.env.REDIS_HOST,port:6379}});await q.%s();await q.close()})().catch(e=>{console.error(e);process.exit(1)})" % action
    run('docker','exec',worker,'node','-e',script)

mq('''const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003");
d.integrations.insertOne({_environmentId:e,_organizationId:o,providerId:"twilio",channel:"sms",active:true,primary:true,credentials:{accountSid:"AC00000000000000000000000000000001",token:"fixture",from:"+15550000001"},deleted:false});
for(const [w,m,name,content] of [["64a000000000000000000031","64a000000000000000000032","sms-queued","Hello {{subscriber.subscriberId}} {{subscriber.firstName}} {{actor.firstName}} {{context.tenant.data.plan}}"],["64a000000000000000000041","64a000000000000000000042","sms-template-snapshot","Original {{subscriber.subscriberId}}"],["64a000000000000000000043","64a000000000000000000044","sms-workflow-deleted","Original {{subscriber.subscriberId}}"],["64a000000000000000000045","64a000000000000000000046","sms-workflow-recreated","Original {{subscriber.subscriberId}}"]]) {const wi=ObjectId(w),mi=ObjectId(m);d.messagetemplates.insertOne({_id:mi,_environmentId:e,_organizationId:o,type:"sms",active:true,content,deleted:false});d.notificationtemplates.insertOne({_id:wi,_environmentId:e,_organizationId:o,name,active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],triggers:[{type:"event",identifier:name}],steps:[{active:true,_templateId:mi}],createdAt:new Date(),updatedAt:new Date()});d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:wi,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{sms:{enabled:true}}},deleted:false});}''')
pq('''INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,sms_content,inbox_metadata) VALUES
('64a000000000000000000002','sms-queued',true,true,'Hello {{subscriber.subscriberId}} {{subscriber.firstName}} {{actor.firstName}} {{context.tenant.data.plan}}','{"id":"64a000000000000000000031"}'),
('64a000000000000000000002','sms-template-snapshot',true,true,'Original {{subscriber.subscriberId}}','{"id":"64a000000000000000000041"}'),
('64a000000000000000000002','sms-workflow-deleted',true,true,'Original {{subscriber.subscriberId}}','{"id":"64a000000000000000000043"}'),
('64a000000000000000000002','sms-workflow-recreated',true,true,'Original {{subscriber.subscriberId}}','{"id":"64a000000000000000000045"}');''')
labels=['changed','disabled','deleted','recreated','enabled_later','phone_added','actor_context','template_changed','workflow_deleted','workflow_recreated']
cases=[(label,'sms-queued-'+label+'-'+uuid.uuid4().hex) for label in labels]
actor='sms-actor-'+uuid.uuid4().hex
context_id='sms-tenant-'+uuid.uuid4().hex
jobs={}
queue_action('pause')
try:
    for label,subscriber in cases:
        for base in bases:
            profile={'subscriberId':subscriber,'firstName':'Before'}
            if label!='phone_added': profile['phone']='+15550000002'
            assert request(base,'POST','/v2/subscribers',profile)[0]==201
            if label=='enabled_later': assert request(base,'PATCH','/v2/subscribers/'+subscriber+'/preferences',{'channels':{'sms':False}})[0]==200
            event={'name':{'template_changed':'sms-template-snapshot','workflow_deleted':'sms-workflow-deleted','workflow_recreated':'sms-workflow-recreated'}.get(label,'sms-queued'),'to':subscriber,'transactionId':subscriber}
            if label=='actor_context': event.update({'actor':{'subscriberId':actor,'firstName':'ActorBefore'},'context':{'tenant':{'id':context_id,'data':{'plan':'basic'}}}})
            assert request(base,'POST','/v1/events/trigger',event)[0]==201
    for label,subscriber in cases:
        for _ in range(90):
            ref=int(mq(f'print(db.getSiblingDB("novu-db").jobs.countDocuments({{transactionId:"{subscriber}",type:"sms"}}))'))
            job=pq(f"SELECT sms_job_id FROM novu_rust_sms_step_outcomes WHERE transaction_id='{subscriber}' AND status='queued'")
            if ref==1 and job: break
            time.sleep(.5)
        assert ref==1 and job,(label,ref,job)
        jobs[label]=job
    for label,subscriber in cases:
        for base in bases:
            if label=='changed': status,_=request(base,'PATCH','/v2/subscribers/'+subscriber,{'phone':'+15550000003','firstName':'After'})
            elif label=='disabled': status,_=request(base,'PATCH','/v2/subscribers/'+subscriber+'/preferences',{'channels':{'sms':False}})
            elif label=='deleted': status,_=request(base,'DELETE','/v2/subscribers/'+subscriber)
            elif label=='recreated':
                assert request(base,'DELETE','/v2/subscribers/'+subscriber)[0]==200
                assert request(base,'POST','/v2/subscribers',{'subscriberId':subscriber,'phone':'+15550000004','firstName':'Recreated'})[0]==201
                status=200
            elif label=='enabled_later': status,_=request(base,'PATCH','/v2/subscribers/'+subscriber+'/preferences',{'channels':{'sms':True}})
            elif label=='phone_added': status,_=request(base,'PATCH','/v2/subscribers/'+subscriber,{'phone':'+15550000003','firstName':'After'})
            elif label=='actor_context':
                assert request(base,'PATCH','/v2/subscribers/'+actor,{'firstName':'ActorAfter'})[0]==200
                status,_=request(base,'PATCH','/v2/contexts/tenant/'+context_id,{'data':{'plan':'premium'}})
            else: continue
            assert status==200,(label,base,status)
    mq('''const d=db.getSiblingDB("novu-db");d.messagetemplates.updateOne({_id:ObjectId("64a000000000000000000042")},{$set:{content:"Changed"}});d.notificationtemplates.deleteOne({_id:ObjectId("64a000000000000000000043")});const old=d.notificationtemplates.findOne({_id:ObjectId("64a000000000000000000045")});d.notificationtemplates.deleteOne({_id:old._id});old._id=ObjectId("64a000000000000000000047");d.notificationtemplates.insertOne(old);''')
    pq('''UPDATE novu_rust_workflows SET sms_content='Changed' WHERE environment_id='64a000000000000000000002' AND identifier='sms-template-snapshot'; DELETE FROM novu_rust_workflows WHERE environment_id='64a000000000000000000002' AND identifier IN ('sms-workflow-deleted','sms-workflow-recreated'); INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,sms_content,inbox_metadata) VALUES ('64a000000000000000000002','sms-workflow-recreated',true,true,'Replacement','{"id":"64a000000000000000000047"}');''')
    pq("UPDATE novu_queue_jobs SET available_at=now()+interval '1 hour' WHERE kind='experimental_twilio_sms' AND status='pending'")
finally:
    queue_action('resume')

for label,subscriber in cases:
    for _ in range(90):
        ref=json.loads(mq(f'''const d=db.getSiblingDB("novu-db"); print(JSON.stringify({{jobs:d.jobs.find({{transactionId:"{subscriber}"}}).toArray().map(j=>({{status:j.status,detail:j.deliveryLifecycleState?.detail}})),messages:d.messages.find({{transactionId:"{subscriber}"}}).toArray().map(m=>({{status:m.status,phone:m.phone}}))}}));'''))
        log=subprocess.run(['docker','exec',worker,'cat','/tmp/novu-sms-capture.jsonl'],capture_output=True,text=True).stdout.splitlines()
        captured=[x for x in map(json.loads,log) if subscriber in x.get('Body','')]
        if ref['jobs'] and all(j['status'] in ('completed','skipped','canceled','failed') for j in ref['jobs']): break
        time.sleep(.5)
    pq(f"UPDATE novu_queue_jobs SET available_at=now() WHERE id='{jobs[label]}'")
    with tempfile.TemporaryDirectory(prefix='novu-sms-recheck-') as directory:
        capture=Path(directory)/'capture.json'
        subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_sms','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_SMS_CAPTURE_PATH=str(capture),TEST_SMS_ALLOW_NO_REQUEST='1'),check=True)
        rust_request=json.loads(capture.read_text())
    rust=json.loads(pq(f"SELECT json_build_object('status',q.status,'outcome',o.status,'messages',(SELECT count(*) FROM novu_rust_sms_messages m WHERE m.environment_id=q.environment_id AND m.transaction_id='{subscriber}'))::text FROM novu_queue_jobs q JOIN novu_rust_sms_step_outcomes o ON o.environment_id=q.environment_id AND o.transaction_id='{subscriber}' WHERE q.id='{jobs[label]}'"))
    if label in ('changed','recreated','enabled_later','phone_added','actor_context','template_changed'):
        assert len(captured)==1 and rust_request==captured[0],(label,ref,captured,rust_request,rust)
        if label=='recreated':
            identities=json.loads(mq(f'''const d=db.getSiblingDB("novu-db"),s=d.subscribers.findOne({{subscriberId:"{subscriber}"}}),j=d.jobs.findOne({{transactionId:"{subscriber}",type:"sms"}}),m=d.messages.findOne({{transactionId:"{subscriber}"}});print(JSON.stringify({{current:String(s._id),job:String(j._subscriberId),message:String(m._subscriberId)}}));'''))
            rust_ids=json.loads(pq(f"SELECT json_build_object('current',s.internal_id,'job',o.subscriber_internal_id,'message',m.subscriber_internal_id)::text FROM novu_rust_subscribers s JOIN novu_rust_sms_step_outcomes o ON o.environment_id=s.environment_id AND o.subscriber_id=s.subscriber_id JOIN novu_rust_sms_messages m ON m.environment_id=o.environment_id AND m.transaction_id=o.transaction_id WHERE s.environment_id='64a000000000000000000002' AND o.transaction_id='{subscriber}'"))
            for ids in (identities,rust_ids): assert ids['job']==ids['message'] and ids['current']!=ids['job'],ids
            assert rust_request['To']=='+15550000004',rust_request
        if label=='actor_context': assert 'ActorAfter premium' in rust_request['Body'],rust_request
        if label=='template_changed': assert rust_request['Body'].startswith('Original '),rust_request
        assert rust['status']=='completed' and rust['messages']==1,rust
    elif label=='disabled':
        assert not captured and rust_request is None and rust['status']=='cancelled' and rust['outcome']=='canceled' and rust['messages']==0,(label,ref,captured,rust)
        assert any(j['status']=='canceled' and j.get('detail')=='preference' for j in ref['jobs']),ref
    else:
        assert not captured and rust_request is None and rust['status']=='failed' and rust['outcome']=='failed' and rust['messages']==0,(label,ref,captured,rust)
        assert any(j['status']=='failed' for j in ref['jobs']),ref
    print(label+': selected queued SMS behavior matched')
