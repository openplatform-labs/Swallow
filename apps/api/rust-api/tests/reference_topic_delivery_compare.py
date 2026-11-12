#!/usr/bin/env python3
"""Compare bounded topic fan-out with running original and Rust workers."""
import json,os,subprocess,time,urllib.error,urllib.request,uuid
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
key=os.environ['NOVU_TEST_API_KEY']; mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER']; pg=os.environ['NOVU_RUST_PG_CONTAINER']
suffix=uuid.uuid4().hex
users=['topic-'+letter+'-'+suffix for letter in 'abc']; topics=['t-one-'+suffix,'t-two-'+suffix]
def req(base,method,path,body):
    request=urllib.request.Request(base+path,data=json.dumps(body).encode(),headers={'Authorization':'ApiKey '+key,'Content-Type':'application/json'},method=method)
    try: response=urllib.request.urlopen(request,timeout=15)
    except urllib.error.HTTPError as error: response=error
    with response:
        raw=response.read();return response.status,json.loads(raw) if raw else None
for base in bases:
    for user in users: assert req(base,'POST','/v2/subscribers',{'subscriberId':user})[0]==201
    for topic,members in zip(topics,[users[:2],users[1:]]):
        status,result=req(base,'POST','/v1/topics/'+topic+'/subscribers',{'subscribers':members})
        assert status==200 and set(result['data']['succeeded'])==set(members),(base,result)

# Create a second identity through both v2 APIs before exercising fan-out.
custom='delivery-custom-'+suffix
for base in bases:
    status,result=req(base,'POST','/v2/topics/'+topics[0]+'/subscriptions',{'subscriptions':[{'subscriberId':users[0],'identifier':custom}]})
    assert status==201,(base,status,result)

cases=[
 ('topic_one',[{'type':'Topic','topicKey':topics[0]}],users[:2]),
 ('overlap',[{'type':'Topic','topicKey':topics[0]},{'type':'Topic','topicKey':topics[1]}],users),
 ('exclude_and_direct',[{'type':'Topic','topicKey':topics[0],'exclude':[users[1]]},{'type':'Topic','topicKey':topics[1]},users[1]],[*users]),
 ('exclude_union',[{'type':'Topic','topicKey':topics[0],'exclude':[users[1]]},{'type':'Topic','topicKey':topics[1]}],[users[0],users[2]]),
]
for label,to,expected in cases:
    transaction='topic-tx-'+uuid.uuid4().hex
    for base in bases:
        status,result=req(base,'POST','/v1/events/trigger',{'name':'greeting','to':to,'transactionId':transaction})
        assert status==201 and result['data']['status']=='processed',(label,base,status,result)
    js=f'''const d=db.getSiblingDB("novu-db"); print(JSON.stringify({{jobs:d.jobs.find({{transactionId:"{transaction}"}}).toArray().map(j=>j.status),users:d.messages.find({{transactionId:"{transaction}"}}).toArray().map(m=>d.subscribers.findOne({{_id:m._subscriberId}})?.subscriberId).sort()}}));'''
    sql=f"SELECT json_build_object('jobs',COALESCE((SELECT json_agg(status) FROM novu_queue_jobs WHERE payload->>'transactionId'='{transaction}'),'[]'::json),'users',COALESCE((SELECT json_agg(subscriber_id ORDER BY subscriber_id) FROM novu_rust_in_app_messages WHERE transaction_id='{transaction}'),'[]'::json))::text"
    for _ in range(60):
        ref=json.loads(subprocess.check_output(['docker','exec',mongo,'mongosh','--quiet','--eval',js],text=True))
        rust=json.loads(subprocess.check_output(['docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql],text=True))
        if ref['jobs'] and rust['jobs'] and all(j in ('completed','canceled','skipped','failed') for j in ref['jobs']) and all(j in ('completed','failed') for j in rust['jobs']) and ref['users']==rust['users']==sorted(expected): break
        time.sleep(1)
    else: raise SystemExit(f'{label}: timeout/mismatch ref={ref} rust={rust} expected={sorted(expected)}')
    assert 'failed' not in ref['jobs'] and 'failed' not in rust['jobs'],(label,ref,rust)
    print(label+': topic delivery match')

if os.environ.get('NOVU_TEST_MONGO_REPLICA') == '1':
    # Delete both identities via v2 and verify the next actual fan-out.
    for base in bases:
        status,result=req(base,'DELETE','/v2/topics/'+topics[0]+'/subscriptions',{'subscriberIds':[users[0]]})
        assert status==200 and result['meta']['successful']==2,(base,status,result)
    transaction='topic-deleted-'+uuid.uuid4().hex
    for base in bases:
        status,result=req(base,'POST','/v1/events/trigger',{'name':'greeting','to':[{'type':'Topic','topicKey':topics[0]}],'transactionId':transaction})
        assert status==201 and result['data']['status']=='processed',(base,status,result)
    js=f'''const d=db.getSiblingDB("novu-db"); print(JSON.stringify({{jobs:d.jobs.find({{transactionId:"{transaction}"}}).toArray().map(j=>j.status),users:d.messages.find({{transactionId:"{transaction}"}}).toArray().map(m=>d.subscribers.findOne({{_id:m._subscriberId}})?.subscriberId).sort()}}));'''
    sql=f"SELECT json_build_object('jobs',COALESCE((SELECT json_agg(status) FROM novu_queue_jobs WHERE payload->>'transactionId'='{transaction}'),'[]'::json),'users',COALESCE((SELECT json_agg(subscriber_id ORDER BY subscriber_id) FROM novu_rust_in_app_messages WHERE transaction_id='{transaction}'),'[]'::json))::text"
    for _ in range(60):
        ref=json.loads(subprocess.check_output(['docker','exec',mongo,'mongosh','--quiet','--eval',js],text=True))
        rust=json.loads(subprocess.check_output(['docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql],text=True))
        if ref['jobs'] and rust['jobs'] and all(j in ('completed','canceled','skipped','failed') for j in ref['jobs']) and all(j in ('completed','failed') for j in rust['jobs']) and ref['users']==rust['users']==[users[1]]: break
        time.sleep(1)
    else: raise SystemExit(f'delete_then_delivery: timeout/mismatch ref={ref} rust={rust}')
    assert 'failed' not in ref['jobs'] and 'failed' not in rust['jobs'],(ref,rust)
    print('delete_then_delivery: topic delivery match')
    for base in bases:
        status,_=req(base,'POST','/v1/topics/'+topics[0]+'/subscribers',{'subscribers':[users[0]]})
        assert status==200,(base,status)

# A separate workflow avoids stale reference workflow/template caches.
js='''const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),w=ObjectId("64a000000000000000000007"),m=ObjectId("64a000000000000000000008"); d.messagetemplates.insertOne({_id:m,_environmentId:e,_organizationId:o,type:"in_app",active:true,content:"{{actor.firstName}} -> {{subscriber.subscriberId}}",deleted:false}); d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:o,name:"Actor Greeting",active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],triggers:[{type:"event",identifier:"actor-greeting"}],steps:[{active:true,_templateId:m}],createdAt:new Date(),updatedAt:new Date()}); d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:w,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{in_app:{enabled:true}}},deleted:false});'''
subprocess.check_call(['docker','exec',mongo,'mongosh','--quiet','--eval',js],stdout=subprocess.DEVNULL)
sql="INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content) VALUES ('64a000000000000000000002','actor-greeting',true,true,'{{actor.firstName}} -> {{subscriber.subscriberId}}')"
subprocess.check_call(['docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-c',sql],stdout=subprocess.DEVNULL)
actor='actor-'+suffix
for label,to,actor_input,expected,name in [
    ('new_actor',[{'type':'Topic','topicKey':topics[0]}],{'subscriberId':actor,'firstName':'Taylor'},users[:2],'Taylor'),
    ('actor_excluded',[{'type':'Topic','topicKey':topics[0]}],{'subscriberId':users[0],'firstName':'Alice'},[users[1]],'Alice'),
    ('actor_direct',[{'type':'Topic','topicKey':topics[0]},users[0]],users[0],users[:2],'Alice'),
]:
    transaction='actor-tx-'+uuid.uuid4().hex
    for base in bases:
        status,result=req(base,'POST','/v1/events/trigger',{'name':'actor-greeting','to':to,'actor':actor_input,'transactionId':transaction})
        assert status==201 and result['data']['status']=='processed',(label,base,status,result)
    js=f'''const d=db.getSiblingDB("novu-db"); print(JSON.stringify({{jobs:d.jobs.find({{transactionId:"{transaction}"}}).toArray().map(j=>j.status),messages:d.messages.find({{transactionId:"{transaction}"}}).toArray().map(m=>({{id:d.subscribers.findOne({{_id:m._subscriberId}})?.subscriberId,content:m.content}})).sort((a,b)=>a.id.localeCompare(b.id))}}));'''
    sql=f"SELECT json_build_object('jobs',COALESCE((SELECT json_agg(status) FROM novu_queue_jobs WHERE payload->>'transactionId'='{transaction}'),'[]'::json),'messages',COALESCE((SELECT json_agg(json_build_object('id',subscriber_id,'content',content) ORDER BY subscriber_id) FROM novu_rust_in_app_messages WHERE transaction_id='{transaction}'),'[]'::json))::text"
    wanted=[{'id':user,'content':name+' -> '+user} for user in sorted(expected)]
    for _ in range(60):
        ref=json.loads(subprocess.check_output(['docker','exec',mongo,'mongosh','--quiet','--eval',js],text=True))
        rust=json.loads(subprocess.check_output(['docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql],text=True))
        if ref['jobs'] and rust['jobs'] and all(j in ('completed','canceled','skipped','failed') for j in ref['jobs']) and all(j in ('completed','failed') for j in rust['jobs']) and ref['messages']==rust['messages']==wanted: break
        time.sleep(1)
    else: raise SystemExit(f'{label}: timeout/mismatch ref={ref} rust={rust} expected={wanted}')
    assert 'failed' not in ref['jobs'] and 'failed' not in rust['jobs'],(label,ref,rust)
    print(label+': actor delivery and rendering match')
for base in bases:
    request=urllib.request.Request(base+'/v2/subscribers/'+actor,headers={'Authorization':'ApiKey '+key})
    with urllib.request.urlopen(request,timeout=10) as response: result=json.load(response)
    assert result['data']['firstName']=='Taylor',result
