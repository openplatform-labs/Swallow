#!/usr/bin/env python3
"""Inspect changes after recipient jobs are queued and before standard delivery."""
import json, os, re, subprocess, tempfile, time, urllib.request, uuid
from pathlib import Path

mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
pg=os.environ['NOVU_RUST_PG_CONTAINER']
worker=os.environ['NOVU_REFERENCE_WORKER_CONTAINER']
key=os.environ['NOVU_TEST_API_KEY']
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]

def run(*args):
    return subprocess.check_output(args,text=True).strip()
def request(base,method,path,body=None):
    data=json.dumps(body).encode() if body is not None else None
    req=urllib.request.Request(base+path,data=data,headers={'Content-Type':'application/json','Authorization':'ApiKey '+key},method=method)
    with urllib.request.urlopen(req,timeout=10) as response:
        raw=response.read()
        return response.status,json.loads(raw) if raw else None
def mongo_query(js): return run('docker','exec',mongo,'mongosh','--quiet','--eval',js)
def pg_query(sql): return run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql)
def queue_action(action):
    script="const {Queue}=require('/app/libs/application-generic/node_modules/bullmq');(async()=>{let q=new Queue('standard',{connection:{host:process.env.REDIS_HOST,port:6379}});await q.%s();await q.close()})().catch(e=>{console.error(e);process.exit(1)})" % action
    run('docker','exec',worker,'node','-e',script)

mongo_query('''const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),w=ObjectId("64a000000000000000000031"),m=ObjectId("64a000000000000000000032");
d.integrations.insertOne({_environmentId:e,_organizationId:o,providerId:"sendgrid",channel:"email",active:true,primary:true,credentials:{apiKey:"SG.fixture",from:"sender@example.test",senderName:"Sender"},deleted:false});
d.messagetemplates.insertOne({_id:m,_environmentId:e,_organizationId:o,type:"email",active:true,subject:"Hello {{subscriber.firstName}}",content:"<p>{{subscriber.email}}|{{actor.firstName}}|{{context.tenant.data.plan}}</p>",contentType:"customHtml",deleted:false});
d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:o,name:"Email Queue",active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],triggers:[{type:"event",identifier:"email-queued"}],steps:[{active:true,_templateId:m}],createdAt:new Date(),updatedAt:new Date()});
d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:w,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{email:{enabled:true}}},deleted:false});''')
pg_query('''INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html) VALUES ('64a000000000000000000002','email-queued',true,true,'Hello {{subscriber.firstName}}','<p>{{subscriber.email}}|{{actor.firstName}}|{{context.tenant.data.plan}}</p>');''')
mongo_query('''const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"); for (const [w,m,name] of [["64a000000000000000000041","64a000000000000000000042","email-template-snapshot"],["64a000000000000000000043","64a000000000000000000044","email-workflow-deleted"],["64a000000000000000000045","64a000000000000000000046","email-workflow-recreated"]]) {const wi=ObjectId(w),mi=ObjectId(m);d.messagetemplates.insertOne({_id:mi,_environmentId:e,_organizationId:o,type:"email",active:true,subject:"Original",content:"<p>Original</p>",contentType:"customHtml",deleted:false});d.notificationtemplates.insertOne({_id:wi,_environmentId:e,_organizationId:o,name,active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],triggers:[{type:"event",identifier:name}],steps:[{active:true,_templateId:mi}],createdAt:new Date(),updatedAt:new Date()});d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:wi,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{email:{enabled:true}}},deleted:false});}''')
pg_query('''INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html,inbox_metadata) VALUES ('64a000000000000000000002','email-template-snapshot',true,true,'Original','<p>Original</p>','{"id":"64a000000000000000000041"}'),('64a000000000000000000002','email-workflow-deleted',true,true,'Original','<p>Original</p>','{"id":"64a000000000000000000043"}'),('64a000000000000000000002','email-workflow-recreated',true,true,'Original','<p>Original</p>','{"id":"64a000000000000000000045"}');''')
cases=[('changed','queued-change-'+uuid.uuid4().hex),('disabled','queued-disable-'+uuid.uuid4().hex),('deleted','queued-delete-'+uuid.uuid4().hex),('recreated','queued-recreated-'+uuid.uuid4().hex),('enabled_later','queued-enable-'+uuid.uuid4().hex),('address_added','queued-address-'+uuid.uuid4().hex),('actor_context','queued-variables-'+uuid.uuid4().hex),('actor_recreated','queued-actor-recreated-'+uuid.uuid4().hex),('template_changed','queued-template-'+uuid.uuid4().hex),('workflow_deleted','queued-workflow-delete-'+uuid.uuid4().hex),('workflow_recreated','queued-workflow-recreated-'+uuid.uuid4().hex)]
actor='queued-actor-'+uuid.uuid4().hex
recreated_actor='queued-actor-recreated-'+uuid.uuid4().hex
context_id='queued-tenant-'+uuid.uuid4().hex
queue_action('pause')
try:
    for label,subscriber in cases:
        for base in bases:
            profile={'subscriberId':subscriber,'firstName':'Before'}
            if label!='address_added': profile['email']='before@example.test'
            assert request(base,'POST','/v2/subscribers',profile)[0]==201
            if label=='enabled_later': assert request(base,'PATCH','/v2/subscribers/'+subscriber+'/preferences',{'channels':{'email':False}})[0]==200
            event={'name':{'template_changed':'email-template-snapshot','workflow_deleted':'email-workflow-deleted','workflow_recreated':'email-workflow-recreated'}.get(label,'email-queued'),'to':subscriber,'transactionId':subscriber}
            if label=='actor_context': event.update({'actor':{'subscriberId':actor,'firstName':'ActorBefore'},'context':{'tenant':{'id':context_id,'data':{'plan':'basic'}}}})
            if label=='actor_recreated': event['actor']={'subscriberId':recreated_actor,'firstName':'OriginalActor'}
            assert request(base,'POST','/v1/events/trigger',event)[0]==201
    for label,subscriber in cases:
        for _ in range(90):
            ref=int(mongo_query(f'print(db.getSiblingDB("novu-db").jobs.countDocuments({{transactionId:"{subscriber}",type:"email"}}))'))
            rust=int(pg_query(f"SELECT count(*) FROM novu_rust_email_step_outcomes WHERE transaction_id='{subscriber}' AND status='queued'"))
            if ref and rust: break
            time.sleep(.5)
        assert ref==1 and rust==1,(label,ref,rust)
    for label,subscriber in cases:
        for base in bases:
            if label=='changed': status,_=request(base,'PATCH','/v2/subscribers/'+subscriber,{'email':'after@example.test','firstName':'After'})
            elif label=='disabled': status,_=request(base,'PATCH','/v2/subscribers/'+subscriber+'/preferences',{'channels':{'email':False}})
            elif label=='deleted': status,_=request(base,'DELETE','/v2/subscribers/'+subscriber)
            elif label=='recreated':
                status,_=request(base,'DELETE','/v2/subscribers/'+subscriber)
                assert status==200
                status,_=request(base,'POST','/v2/subscribers',{'subscriberId':subscriber,'email':'new@example.test','firstName':'Recreated'})
                assert status==201
                status=200
            elif label=='enabled_later': status,_=request(base,'PATCH','/v2/subscribers/'+subscriber+'/preferences',{'channels':{'email':True}})
            elif label=='address_added': status,_=request(base,'PATCH','/v2/subscribers/'+subscriber,{'email':'after@example.test','firstName':'After'})
            elif label=='actor_recreated':
                status,_=request(base,'DELETE','/v2/subscribers/'+recreated_actor)
                assert status==200
                status,_=request(base,'POST','/v2/subscribers',{'subscriberId':recreated_actor,'firstName':'ReplacementActor'})
                assert status==201
                status=200
            elif label=='actor_context':
                status,_=request(base,'PATCH','/v2/subscribers/'+actor,{'firstName':'ActorAfter'})
                assert status==200
                status,_=request(base,'PATCH','/v2/contexts/tenant/'+context_id,{'data':{'plan':'premium'}})
            else: continue
            assert status==200,(label,base,status)
    mongo_query('''const d=db.getSiblingDB("novu-db");d.messagetemplates.updateOne({_id:ObjectId("64a000000000000000000042")},{$set:{subject:"Changed",content:"<p>Changed</p>"}});d.notificationtemplates.deleteOne({_id:ObjectId("64a000000000000000000043")});const old=d.notificationtemplates.findOne({_id:ObjectId("64a000000000000000000045")});d.notificationtemplates.deleteOne({_id:old._id});old._id=ObjectId("64a000000000000000000047");d.notificationtemplates.insertOne(old);''')
    pg_query("UPDATE novu_rust_workflows SET email_subject='Changed',email_html='<p>Changed</p>' WHERE environment_id='64a000000000000000000002' AND identifier='email-template-snapshot'; DELETE FROM novu_rust_workflows WHERE environment_id='64a000000000000000000002' AND identifier IN ('email-workflow-deleted','email-workflow-recreated'); INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html,inbox_metadata) VALUES ('64a000000000000000000002','email-workflow-recreated',true,true,'Replacement','<p>Replacement</p>','{\"id\":\"64a000000000000000000047\"}');")
finally:
    queue_action('resume')

for label,subscriber in cases:
    for _ in range(90):
        ref=json.loads(mongo_query(f'''const d=db.getSiblingDB("novu-db"); print(JSON.stringify({{jobs:d.jobs.find({{transactionId:"{subscriber}"}}).toArray().map(j=>({{status:j.status,detail:j.deliveryLifecycleState?.detail}})),messages:d.messages.find({{transactionId:"{subscriber}"}}).toArray().map(m=>({{status:m.status,email:m.email}}))}}));'''))
        log=subprocess.run(['docker','exec',worker,'cat','/tmp/novu-email-capture.jsonl'],capture_output=True,text=True).stdout.splitlines()
        captured=[x for x in map(json.loads,log) if x.get('custom_args',{}).get('novuTransactionId')==subscriber]
        if ref['jobs'] and all(j['status'] in ('completed','skipped','canceled','failed') for j in ref['jobs']): break
        time.sleep(.5)
    with tempfile.TemporaryDirectory(prefix='novu-email-recheck-') as directory:
        capture=Path(directory)/'capture.json'
        subprocess.run(['cargo','test','--locked','--offline','--lib','capture_reference_workflow_email','--','--ignored'],cwd=Path(__file__).resolve().parents[3]/'worker'/'rust-queue',env=dict(os.environ,TEST_EMAIL_CAPTURE_PATH=str(capture),TEST_EMAIL_ALLOW_NO_REQUEST='1'),check=True)
        rust_request=json.loads(capture.read_text())
    rust=json.loads(pg_query(f"SELECT json_build_object('status',q.status,'outcome',o.status,'messages',(SELECT count(*) FROM novu_rust_email_messages m WHERE m.environment_id=q.environment_id AND m.transaction_id='{subscriber}'))::text FROM novu_queue_jobs q JOIN novu_rust_email_step_outcomes o ON o.transaction_id=q.payload->>'transaction_id' AND o.environment_id=q.environment_id WHERE q.kind='experimental_sendgrid_email' AND q.payload->>'transaction_id'='{subscriber}'"))
    if label in ('changed','recreated','enabled_later','address_added','actor_context','actor_recreated','template_changed'):
        assert len(captured)==1 and rust_request,(label,ref,captured,rust)
        original=captured[0]
        for message in (original,rust_request):
            custom=message['custom_args']
            assert re.fullmatch('[0-9a-f]{24}',custom['id'])
            assert custom.pop('novuMessageId')==custom.pop('id')
        assert original==rust_request,(label,original,rust_request)
        if label=='recreated':
            identities=json.loads(mongo_query(f'''const d=db.getSiblingDB("novu-db"),s=d.subscribers.findOne({{subscriberId:"{subscriber}",_environmentId:ObjectId("64a000000000000000000002")}}),j=d.jobs.findOne({{transactionId:"{subscriber}",type:"email"}}),m=d.messages.findOne({{transactionId:"{subscriber}"}});print(JSON.stringify({{current:String(s._id),job:String(j._subscriberId),message:String(m._subscriberId)}}));'''))
            rust_ids=json.loads(pg_query(f"SELECT json_build_object('current',s.internal_id,'job',o.subscriber_internal_id,'message',m.subscriber_internal_id)::text FROM novu_rust_subscribers s JOIN novu_rust_email_step_outcomes o ON o.environment_id=s.environment_id AND o.subscriber_id=s.subscriber_id JOIN novu_rust_email_messages m ON m.environment_id=o.environment_id AND m.transaction_id=o.transaction_id WHERE s.environment_id='64a000000000000000000002' AND o.transaction_id='{subscriber}'"))
            for ids in (identities,rust_ids): assert ids['job']==ids['message'] and ids['current']!=ids['job'],ids
            assert rust_request['personalizations'][0]['to']==[{'email':'new@example.test'}],rust_request
        if label=='actor_recreated': assert 'ReplacementActor' in rust_request['content'][0]['value'],rust_request
        if label=='actor_context': assert 'ActorAfter|premium' in rust_request['content'][0]['value'],rust_request
        if label=='template_changed': assert rust_request['subject']=='Original' and 'Original' in rust_request['content'][0]['value'],rust_request
        assert rust['status']=='completed' and rust['messages']==1,rust
    elif label=='disabled':
        assert not captured and rust_request is None and rust['status']=='cancelled' and rust['outcome']=='canceled' and rust['messages']==0,(label,ref,captured,rust)
        assert any(j['status']=='canceled' and j.get('detail')=='preference' for j in ref['jobs']),ref
    else:
        assert not captured and rust_request is None and rust['status']=='failed' and rust['outcome']=='failed' and rust['messages']==0,(label,ref,captured,rust)
        assert any(j['status']=='failed' for j in ref['jobs']),ref
    print(label+': original and Rust delivery behavior match')
