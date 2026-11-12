#!/usr/bin/env python3
"""Compare disabled global preference and read-only workflow delivery.

Only for disposable services configured by test-reference-subscribers.sh with
NOVU_TEST_PREFERENCE_DELIVERY=1. Waits for terminal jobs on both implementations,
then compares message counts and preference cancellation. It does not compare
all job fields, traces, or real-time events.
"""
import json,os,subprocess,time,urllib.error,urllib.request,uuid
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
key=os.environ['NOVU_TEST_API_KEY'];mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER'];pg=os.environ['NOVU_RUST_PG_CONTAINER'];worker=os.environ['NOVU_REFERENCE_WORKER_CONTAINER']
subscriber='delivery-'+uuid.uuid4().hex

def request(base,method,path,body):
    req=urllib.request.Request(base+path,data=json.dumps(body).encode(),headers={'Authorization':'ApiKey '+key,'Content-Type':'application/json'},method=method)
    try:r=urllib.request.urlopen(req,timeout=15)
    except urllib.error.HTTPError as e:r=e
    with r:status,raw=r.status,r.read()
    return status,json.loads(raw) if raw else None

for base in bases:
    assert request(base,'POST','/v2/subscribers',{'subscriberId':subscriber})[0]==201
    assert request(base,'PATCH','/v2/subscribers/'+subscriber+'/preferences',{'channels':{'in_app':False}})[0]==200
for label,workflow,expected,patch in [
    ('global_off','greeting',0,None),
    ('readonly','urgent',1,None),
    ('workflow_override','greeting',1,{'workflowId':'greeting','channels':{'in_app':True}}),
    ('global_clears_override','greeting',0,{'channels':{'in_app':False}}),
]:
    if patch is not None:
        for base in bases: assert request(base,'PATCH','/v2/subscribers/'+subscriber+'/preferences',patch)[0]==200
    transaction='pref-'+uuid.uuid4().hex
    for base in bases:
        status,result=request(base,'POST','/v1/events/trigger',{'name':workflow,'to':subscriber,'transactionId':transaction})
        assert status==201 and result['data']['status']=='processed',(base,status,result)
    js=f'''const d=db.getSiblingDB("novu-db"); print(JSON.stringify({{jobs:d.jobs.find({{transactionId:"{transaction}"}}).toArray().map(j=>({{status:j.status,detail:j.deliveryLifecycleState?.detail}})),messages:d.messages.countDocuments({{transactionId:"{transaction}"}})}}));'''
    sql=f"SELECT json_build_object('jobs',COALESCE((SELECT json_agg(status) FROM novu_queue_jobs WHERE payload->>'transactionId'='{transaction}'),'[]'::json),'outcomes',COALESCE((SELECT json_agg(json_build_object('status',status,'detail',detail)) FROM novu_rust_in_app_step_outcomes WHERE transaction_id='{transaction}'),'[]'::json),'messages',(SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='{transaction}'))::text"
    for attempt in range(60):
        if attempt % 5 == 0:
            running=subprocess.check_output(['docker','inspect','-f','{{.State.Running}}',worker],text=True).strip()
            if running != 'true':
                logs=subprocess.check_output(['docker','logs','--tail','50',worker],stderr=subprocess.STDOUT,text=True)
                raise SystemExit('reference worker stopped:\n'+logs)
        ref=json.loads(subprocess.check_output(['docker','exec',mongo,'mongosh','--quiet','--eval',js],text=True))
        rust=json.loads(subprocess.check_output(['docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql],text=True))
        if ref['jobs'] and rust['jobs'] and all(j['status'] in ('completed','skipped','canceled','failed') for j in ref['jobs']) and all(j in ('completed','failed') for j in rust['jobs']): break
        time.sleep(1)
    else:
        logs=subprocess.check_output(['docker','logs','--tail','30',worker],stderr=subprocess.STDOUT,text=True)
        raise SystemExit(f'timeout {workflow}: ref={ref} rust={rust}\nworker logs:\n{logs}')
    assert all(j['status']!='failed' for j in ref['jobs']) and all(j!='failed' for j in rust['jobs']),(workflow,ref,rust)
    assert ref['messages']==rust['messages']==expected,(workflow,ref,rust)
    assert len(rust['outcomes'])==1,(workflow,rust)
    if expected == 0:
        assert any(j['status']=='canceled' and j.get('detail')=='preference' for j in ref['jobs']), (workflow,ref)
        assert rust['outcomes'][0]=={'status':'canceled','detail':'preference'},(workflow,rust)
    else:
        assert rust['outcomes'][0]=={'status':'completed','detail':None},(workflow,rust)
    print(label+': message count and selected outcome match')

# Novu creates separate recipient jobs, while the experimental queue batches
# them. Verify the per-recipient result without equating the queue job models.
enabled='delivery-'+uuid.uuid4().hex
transaction='pref-'+uuid.uuid4().hex
for base in bases:
    assert request(base,'POST','/v2/subscribers',{'subscriberId':enabled})[0]==201
    status,result=request(base,'POST','/v1/events/trigger',{'name':'greeting','to':[subscriber,enabled],'transactionId':transaction})
    assert status==201 and result['data']['status']=='processed',(base,status,result)
js=f'''const d=db.getSiblingDB("novu-db"); print(JSON.stringify({{jobs:d.jobs.find({{transactionId:"{transaction}"}}).toArray().map(j=>({{status:j.status,detail:j.deliveryLifecycleState?.detail}})),messages:d.messages.countDocuments({{transactionId:"{transaction}"}})}}));'''
sql=f"SELECT json_build_object('jobs',COALESCE((SELECT json_agg(status) FROM novu_queue_jobs WHERE payload->>'transactionId'='{transaction}'),'[]'::json),'outcomes',COALESCE((SELECT json_agg(json_build_object('subscriber',subscriber_id,'status',status,'detail',detail)) FROM novu_rust_in_app_step_outcomes WHERE transaction_id='{transaction}'),'[]'::json),'messages',(SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='{transaction}'))::text"
for attempt in range(60):
    ref=json.loads(subprocess.check_output(['docker','exec',mongo,'mongosh','--quiet','--eval',js],text=True))
    rust=json.loads(subprocess.check_output(['docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql],text=True))
    if len(rust['outcomes'])==2 and rust['jobs'] and all(j=='completed' for j in rust['jobs']) and ref['messages']==1 and any(j['status']=='canceled' and j.get('detail')=='preference' for j in ref['jobs']): break
    time.sleep(1)
else: raise SystemExit(f'mixed recipients timeout: ref={ref} rust={rust}')
assert rust['messages']==1,(ref,rust)
assert {o['subscriber']:(o['status'],o['detail']) for o in rust['outcomes']}=={subscriber:('canceled','preference'),enabled:('completed',None)},(ref,rust)
print('mixed recipients: message count and preference cancellation match')
