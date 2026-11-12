#!/usr/bin/env python3
"""Compare topic subscription preference evaluation with the original worker."""
import json,os,subprocess,time,urllib.error,urllib.request,uuid
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']];key=os.environ['NOVU_TEST_API_KEY']
mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER'];pg=os.environ['NOVU_RUST_PG_CONTAINER'];suffix=uuid.uuid4().hex
users=['topic-pref-'+x+'-'+suffix for x in 'abc'];topic='topic-pref-'+suffix;custom='custom-'+suffix
wf='64a000000000000000000004';env='64a000000000000000000002';org='64a000000000000000000003'

def req(base,method,path,body):
    request=urllib.request.Request(base+path,data=json.dumps(body).encode(),headers={'Content-Type':'application/json','Authorization':'ApiKey '+key},method=method)
    try:r=urllib.request.urlopen(request,timeout=15)
    except urllib.error.HTTPError as e:r=e
    with r:
        raw=r.read();return r.status,json.loads(raw) if raw else None
for base in bases:
    for user in users:assert req(base,'POST','/v2/subscribers',{'subscriberId':user,'data':{'plan':'premium'}})[0]==201
    assert req(base,'POST','/v1/topics/'+topic+'/subscribers',{'subscribers':users[1:]})[0]==200
    assert req(base,'POST','/v2/topics/'+topic+'/subscriptions',{'subscriberIds':[users[0]],'preferences':[{'workflowId':'greeting','enabled':False}]})[0]==201
    assert req(base,'POST','/v2/topics/'+topic+'/subscriptions',{'subscriptions':[{'subscriberId':users[0],'identifier':custom}],'preferences':[{'workflowId':'greeting','enabled':True}]})[0]==201

def set_pref(identifier,preference):
    workflow={'workflowId':'greeting',**preference['all']}
    for base in bases:
        status,result=req(base,'PATCH','/v2/topics/'+topic+'/subscriptions/'+identifier,{'preferences':[workflow]})
        assert status==200,(base,status,result)
        assert len(result['data']['preferences'])==1,(base,result)

def trigger(label,tier,expected,direct=False):
    transaction='topic-pref-tx-'+uuid.uuid4().hex
    to=[{'type':'Topic','topicKey':topic}]+([users[0]] if direct else [])
    for base in bases:
        status,result=req(base,'POST','/v1/events/trigger',{'name':'greeting','to':to,'payload':{'tier':tier},'transactionId':transaction})
        assert status==201 and result['data']['status']=='processed',(base,status,result)
    js='''const d=db.getSiblingDB("novu-db"); print(JSON.stringify({jobs:d.jobs.find({transactionId:%s}).toArray().map(j=>j.status),users:d.messages.find({transactionId:%s}).toArray().map(m=>d.subscribers.findOne({_id:m._subscriberId})?.subscriberId).sort()}));''' % (json.dumps(transaction),json.dumps(transaction))
    sql="SELECT json_build_object('jobs',COALESCE((SELECT json_agg(status) FROM novu_queue_jobs WHERE payload->>'transactionId'='%s'),'[]'::json),'users',COALESCE((SELECT json_agg(subscriber_id ORDER BY subscriber_id) FROM novu_rust_in_app_messages WHERE transaction_id='%s'),'[]'::json))::text" % (transaction,transaction)
    for _ in range(60):
        ref=json.loads(subprocess.check_output(['docker','exec',mongo,'mongosh','--quiet','--eval',js],text=True))
        rust=json.loads(subprocess.check_output(['docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql],text=True))
        if ref['jobs'] and rust['jobs'] and all(j in ('completed','canceled','skipped','failed') for j in ref['jobs']) and all(j in ('completed','failed') for j in rust['jobs']) and ref['users']==rust['users']==sorted(expected):break
        time.sleep(1)
    else:raise SystemExit(f'{label}: timeout/mismatch ref={ref} rust={rust} expected={sorted(expected)}')
    assert 'failed' not in ref['jobs'] and 'failed' not in rust['jobs'],(label,ref,rust)
    print(label+': match',flush=True)

default_a='tk_'+topic+':si_'+users[0];default_b='tk_'+topic+':si_'+users[1]
set_pref(default_b,{'all':{'enabled':False,'condition':{'===':[{'var':'payload.tier'},'gold']}}})
trigger('topic_pref_condition_true','gold',users)
trigger('topic_pref_condition_false','silver',[users[0],users[2]])
set_pref(custom,{'all':{'enabled':False}})
trigger('topic_pref_all_disabled','silver',[users[2]])
trigger('topic_pref_direct_bypass','silver',[users[0],users[2]],True)
set_pref(default_b,{'all':{'enabled':True,'condition':{'var':'payload.tier'}}})
trigger('topic_pref_non_boolean','gold',[users[2]])
set_pref(default_b,{'all':{'enabled':False,'condition':{'===':[{'var':'subscriber.data.plan'},'premium']}}})
trigger('topic_pref_subscriber_condition','silver',[users[1],users[2]])
# A preference bound to another subscriber must not suppress this subscription.
default_c='tk_'+topic+':si_'+users[2]
js='''const d=db.getSiblingDB("novu-db"),e=ObjectId(%s),o=ObjectId(%s),m=d.topicsubscribers.findOne({_environmentId:e,identifier:%s}),s=d.subscribers.findOne({_environmentId:e,subscriberId:%s});d.preferences.insertOne({_environmentId:e,_organizationId:o,_subscriberId:s._id,_topicSubscriptionId:m._id,_templateId:ObjectId(%s),type:"SUBSCRIPTION_SUBSCRIBER_WORKFLOW",preferences:{all:{enabled:false}},deleted:false});''' % (json.dumps(env),json.dumps(org),json.dumps(default_c),json.dumps(users[0]),json.dumps(wf))
subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval',js],check=True,stdout=subprocess.DEVNULL)
sql="INSERT INTO novu_rust_subscription_preferences(environment_id,subscriber_internal_id,subscription_internal_id,workflow_internal_id,preferences) SELECT m.environment_id,s.internal_id,m.internal_id,'%s','{\"all\":{\"enabled\":false}}'::jsonb FROM novu_rust_topic_subscribers m JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id='%s' WHERE m.environment_id='%s' AND m.identifier='%s'" % (wf,users[0],env,default_c)
subprocess.run(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',sql],check=True,stdout=subprocess.DEVNULL)
trigger('topic_pref_cross_subscriber','silver',[users[1],users[2]])
# Without a workflow's internal identity, the worker cannot safely decide
# which stored preference applies; it must fail rather than bypass it.
sql="UPDATE novu_rust_workflows SET inbox_metadata=NULL WHERE environment_id='%s' AND identifier='greeting'" % env
subprocess.run(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',sql],check=True,stdout=subprocess.DEVNULL)
transaction='topic-pref-missing-workflow-'+uuid.uuid4().hex
status,result=req(bases[1],'POST','/v1/events/trigger',{'name':'greeting','to':[{'type':'Topic','topicKey':topic}],'transactionId':transaction})
assert status==201,(status,result)
sql="SELECT json_build_object('status',(SELECT status FROM novu_queue_jobs WHERE payload->>'transactionId'='%s'),'messages',(SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='%s'))::text" % (transaction,transaction)
for _ in range(60):
    state=json.loads(subprocess.check_output(['docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql],text=True))
    if state['status']=='failed':break
    time.sleep(1)
else:raise SystemExit(f'topic_pref_missing_workflow: timeout {state}')
assert state['messages']==0,state
print('topic_pref_missing_workflow: failed safely',flush=True)
