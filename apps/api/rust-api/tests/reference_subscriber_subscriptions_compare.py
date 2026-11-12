#!/usr/bin/env python3
"""Compare subscriber-scoped topic subscription listing against original Novu."""
import copy,json,os,subprocess,urllib.error,urllib.parse,urllib.request,uuid
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']];key=os.environ['NOVU_TEST_API_KEY'];suffix=uuid.uuid4().hex
user='sub-list-'+suffix;other='sub-list-other-'+suffix;topics=['sub-list-topic-'+str(i)+'-'+suffix for i in range(4)]
path='/v2/subscribers/'+user+'/subscriptions';custom='sub-list-custom-'+suffix

def req(base,method,path,body=None,auth=True):
    headers={'Content-Type':'application/json'}
    if auth:headers['Authorization']='ApiKey '+key
    request=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers=headers,method=method)
    try:r=urllib.request.urlopen(request,timeout=15)
    except urllib.error.HTTPError as e:r=e
    with r:
        raw=r.read();return r.status,json.loads(raw) if raw else None

def clean(result):
    status,body=copy.deepcopy(result)
    def walk(value):
        if isinstance(value,dict):
            for k in list(value):
                if k in ('_id','id') and value[k]!='64a000000000000000000004':assert len(value[k])==24;value[k]='<id>'
                elif k=='timestamp':value.pop(k)
                elif k=='errorId':uuid.UUID(value.pop(k))
                elif k in ('createdAt','updatedAt') and value[k]:value[k]='<time>'
                elif k in ('next','previous'):value[k]=bool(value[k])
                else:walk(value[k])
        elif isinstance(value,list):
            for i in value:walk(i)
    walk(body);return status,body

def query(label,params=None,paths=None,auth=True):
    paths=paths or [path+('?' + urllib.parse.urlencode(params,doseq=True) if params else '')]*2
    results=[req(base,'GET',p,auth=auth) for base,p in zip(bases,paths)]
    assert clean(results[0])==clean(results[1]),(label,results)
    print(label+': match',flush=True);return results

for base in bases:
    for subscriber in [user,other]:assert req(base,'POST','/v2/subscribers',{'subscriberId':subscriber,'firstName':'First'})[0]==201
    for index,topic in enumerate(topics):
        body={'subscriberIds':[user], 'name':'Topic '+str(index)}
        if index==0:body['preferences']=[{'workflowId':'greeting','enabled':False}]
        status,result=req(base,'POST','/v2/topics/'+topic+'/subscriptions',body)
        assert status==201,(base,status,result)
    assert req(base,'POST','/v2/topics/'+topics[0]+'/subscriptions',{'subscriptions':[{'subscriberId':user,'identifier':custom}]})[0]==201
    assert req(base,'POST','/v2/topics/'+topics[0]+'/subscriptions',{'subscriberIds':[other]})[0]==201
results=query('subscriber_subscriptions_all',{'orderDirection':'ASC'})
assert all(r[1]['totalCount']==5 for r in results)
assert all(sum('preferences' in row for row in r[1]['data'])==1 for r in results)
mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER'];pg=os.environ['NOVU_RUST_PG_CONTAINER']
js='''const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),m=d.topicsubscribers.findOne({_environmentId:e,identifier:%s}),s=d.subscribers.findOne({_environmentId:e,subscriberId:%s});for(const sub of [m._subscriberId,s._id]) d.preferences.insertOne({_environmentId:e,_organizationId:o,_subscriberId:sub,_topicSubscriptionId:m._id,_templateId:ObjectId("64a000000000000000000099"),type:"SUBSCRIPTION_SUBSCRIBER_WORKFLOW",preferences:{all:{enabled:false}},deleted:false});''' % (json.dumps(custom),json.dumps(other))
subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval',js],check=True,stdout=subprocess.DEVNULL)
sql="INSERT INTO novu_rust_subscription_preferences(environment_id,subscriber_internal_id,subscription_internal_id,workflow_internal_id,preferences) SELECT m.environment_id,s.internal_id,m.internal_id,'64a000000000000000000099','{\"all\":{\"enabled\":false}}'::jsonb FROM novu_rust_topic_subscribers m JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id IN ('%s','%s') WHERE m.environment_id='64a000000000000000000002' AND m.identifier='%s'" % (user,other,custom)
subprocess.run(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',sql],check=True,stdout=subprocess.DEVNULL)
results=query('subscriber_subscriptions_orphan_workflow',{'key':topics[0]})
for _,body in results:
    row=next(row for row in body['data'] if row['identifier']==custom)
    assert row['preferences']==[{'subscriptionId':custom,'enabled':False}],row
query('subscriber_subscriptions_topic',{'key':topics[0]})
query('subscriber_subscriptions_missing_key',{'key':'not-found'})
query('subscriber_subscriptions_order_ignored',{'orderBy':'createdAt','orderDirection':'ASC'})
query('subscriber_subscriptions_unknown_direction',{'orderDirection':'invalid'})
query('subscriber_subscriptions_limit_exceeded',{'limit':101})
query('subscriber_subscriptions_missing_cursor',{'after':'64a000000000000000000099'})
query('subscriber_subscriptions_invalid_cursor',{'after':'bad'})
query('subscriber_subscriptions_both_cursors',{'before':'a','after':'b'})
query('subscriber_subscriptions_missing',paths=['/v2/subscribers/missing-'+suffix+'/subscriptions']*2)
query('subscriber_subscriptions_unauthorized',auth=False)
for direction in ['ASC','DESC']:
    params={'limit':2,'orderDirection':direction}
    first=query('subscriber_subscriptions_first_'+direction,params)
    next_paths=[path+'?'+urllib.parse.urlencode({**params,'after':r[1]['next']}) for r in first]
    second=query('subscriber_subscriptions_next_'+direction,paths=next_paths)
    back_paths=[path+'?'+urllib.parse.urlencode({**params,'before':r[1]['previous']}) for r in second]
    back=query('subscriber_subscriptions_previous_'+direction,paths=back_paths)
    assert [[x['identifier'] for x in r[1]['data']] for r in back]==[[x['identifier'] for x in r[1]['data']] for r in first]
    include_paths=[path+'?'+urllib.parse.urlencode({**params,'after':r[1]['next'],'includeCursor':'true'}) for r in first]
    included=query('subscriber_subscriptions_include_'+direction,paths=include_paths)
    assert all(a[1]['data'][0]['identifier']==b[1]['data'][-1]['identifier'] for a,b in zip(included,first))
# Context-enabled subscription creation is not yet supported. Seed one record
# in each disposable database to compare list filtering only.
js='const d=db.getSiblingDB("novu-db");d.topicsubscribers.updateOne({_environmentId:ObjectId("64a000000000000000000002"),identifier:'+json.dumps(custom)+'},{$set:{contextKeys:["tenant:one","project:two"]}});'
subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval',js],check=True,stdout=subprocess.DEVNULL)
sql="UPDATE novu_rust_topic_subscribers SET context_keys=ARRAY['tenant:one','project:two'] WHERE environment_id='64a000000000000000000002' AND identifier='%s'" % custom
subprocess.run(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',sql],check=True,stdout=subprocess.DEVNULL)
query('subscriber_subscriptions_context_all')
query('subscriber_subscriptions_context_empty',{'contextKeys':''})
query('subscriber_subscriptions_context_exact',{'contextKeys':['project:two','tenant:one']})
query('subscriber_subscriptions_context_subset',{'contextKeys':'tenant:one'})
