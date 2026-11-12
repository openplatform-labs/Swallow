#!/usr/bin/env python3
"""Compare v2 subscription deletes against a transactional Mongo replica set."""
import copy,json,os,subprocess,urllib.error,urllib.request,uuid
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
key=os.environ['NOVU_TEST_API_KEY']; suffix=uuid.uuid4().hex
users=['delete-user-'+x+'-'+suffix for x in 'abc']

def call(base,method,path,body=None,auth=True):
    headers={'Content-Type':'application/json'}
    if auth:headers['Authorization']='ApiKey '+key
    request=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers=headers,method=method)
    try:r=urllib.request.urlopen(request,timeout=15)
    except urllib.error.HTTPError as e:r=e
    with r:
        raw=r.read();return r.status,json.loads(raw) if raw else None

def clean(result):
    status,body=copy.deepcopy(result)
    def walk(v):
        if isinstance(v,dict):
            for k in list(v):
                if k in ('_id','id'):assert len(v[k])==24;v[k]='<id>'
                elif k=='timestamp':v.pop(k)
                elif k in ('createdAt','updatedAt') and v[k]:v[k]='<time>'
                elif k in ('next','previous'):v[k]=bool(v[k])
                else:walk(v[k])
        elif isinstance(v,list):
            for i in v:walk(i)
    walk(body);return status,body

def compare(label,method,path,body=None,auth=True):
    results=[call(base,method,path,body,auth) for base in bases]
    assert clean(results[0])==clean(results[1]),(label,results)
    print(label+': match');return results

for base in bases:
    for user in users:assert call(base,'POST','/v2/subscribers',{'subscriberId':user,'firstName':'First'})[0]==201

def setup(kind):
    topic='delete-'+kind+'-'+suffix;path='/v2/topics/'+topic+'/subscriptions'
    ids=['delete-'+kind+'-'+x+'-'+suffix for x in 'abc']
    for base in bases:
        for user,ident in zip([users[0],users[0],users[1]],ids):
            status,result=call(base,'POST',path,{'subscriptions':[{'subscriberId':user,'identifier':ident,'name':'Name'}], 'name':'Topic'})
            assert status==201,(status,result)
    return path,ids

path,ids=setup('id')
status, detail=call(bases[0],'GET',path+'/'+ids[0])
assert status==200
mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
subscription_internal_id=detail['data']['id']
js='''const d=db.getSiblingDB("novu-db"),e=ObjectId("64a000000000000000000002"),o=ObjectId("64a000000000000000000003"),s=ObjectId(%s); d.preferences.insertMany([{_environmentId:e,_organizationId:o,_topicSubscriptionId:s,type:"SUBSCRIPTION_SUBSCRIBER_WORKFLOW",deleted:false},{_environmentId:e,_organizationId:o,_topicSubscriptionId:s,type:"SUBSCRIBER_GLOBAL",deleted:false}]);''' % json.dumps(subscription_internal_id)
subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval',js],check=True,stdout=subprocess.DEVNULL)
pg=os.environ['NOVU_RUST_PG_CONTAINER']
sql="INSERT INTO novu_rust_subscription_preferences(environment_id,subscriber_internal_id,subscription_internal_id,workflow_internal_id,preferences) SELECT m.environment_id,s.internal_id,m.internal_id,'64a000000000000000000004','{\"all\":{\"enabled\":false}}'::jsonb FROM novu_rust_topic_subscribers m JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id WHERE m.environment_id='64a000000000000000000002' AND m.identifier='%s'" % ids[0]
subprocess.run(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',sql],check=True,stdout=subprocess.DEVNULL)
sql="SELECT count(*) FROM novu_rust_subscription_preferences WHERE environment_id='64a000000000000000000002'"
assert subprocess.check_output(['docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql],text=True).strip()=='1'
results=compare('delete_identifier','DELETE',path,{'subscriptions':[{'identifier':ids[0]}]})
assert all(r[1]['meta']=={'totalCount':1,'successful':1,'failed':0} for r in results)
js='''const d=db.getSiblingDB("novu-db"),s=ObjectId(%s); print(JSON.stringify(d.preferences.find({_topicSubscriptionId:s}).toArray().map(p=>p.type)));''' % json.dumps(subscription_internal_id)
remaining=json.loads(subprocess.check_output(['docker','exec',mongo,'mongosh','--quiet','--eval',js],text=True))
assert remaining==['SUBSCRIBER_GLOBAL'],remaining
sql="SELECT count(*) FROM novu_rust_subscription_preferences WHERE environment_id='64a000000000000000000002'"
remaining_pg=subprocess.check_output(['docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',sql],text=True).strip()
assert remaining_pg=='0',remaining_pg
print('subscription_preferences_deleted: match')
compare('delete_identifier_gone','GET',path+'/'+ids[0])
compare('delete_identifier_rest','GET',path)
compare('delete_identifier_missing','DELETE',path,{'subscriptions':[{'identifier':ids[0]}]})
compare('delete_missing_user','DELETE',path,{'subscriberIds':['missing-'+suffix]})
compare('delete_user_no_subscription','DELETE',path,{'subscriberIds':[users[2]]})
compare('delete_empty_body','DELETE',path,{})
compare('delete_empty_list','DELETE',path,{'subscriptions':[]})
compare('delete_invalid_item','DELETE',path,{'subscriptions':[{}]})
compare('delete_missing_topic','DELETE','/v2/topics/missing-'+suffix+'/subscriptions',{'subscriberIds':[users[0]]})
compare('delete_unauthorized','DELETE',path,{'subscriberIds':[users[0]]},False)
path,ids=setup('user')
results=compare('delete_user_all','DELETE',path,{'subscriberIds':[users[0]]})
assert all(r[1]['meta']=={'totalCount':1,'successful':2,'failed':0} for r in results)
compare('delete_user_remaining','GET',path)
path,ids=setup('mixed')
results=compare('delete_mixed','DELETE',path,{'subscriptions':[{'identifier':ids[0],'subscriberId':users[0]},{'identifier':'missing-'+suffix},{'subscriberId':'missing-'+suffix}]})
assert all(r[1]['meta']=={'totalCount':3,'successful':3,'failed':2} for r in results),results
compare('delete_mixed_remaining','GET',path)
path,ids=setup('partial')
compare('delete_partial','DELETE',path,{'subscriptions':[{'identifier':ids[0]},{'identifier':'missing-'+suffix}]})
compare('delete_partial_remaining','GET',path)
