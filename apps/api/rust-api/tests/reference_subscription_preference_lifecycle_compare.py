#!/usr/bin/env python3
"""Compare lifecycle of pre-seeded subscription preferences on both databases."""
import json,os,subprocess,urllib.error,urllib.request,uuid
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']];key=os.environ['NOVU_TEST_API_KEY']
mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER'];pg=os.environ['NOVU_RUST_PG_CONTAINER'];suffix=uuid.uuid4().hex
env='64a000000000000000000002';org='64a000000000000000000003';wf='64a000000000000000000004';user='pref-life-'+suffix

def req(base,method,path,body=None):
    request=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers={'Content-Type':'application/json','Authorization':'ApiKey '+key},method=method)
    try:r=urllib.request.urlopen(request,timeout=10)
    except urllib.error.HTTPError as e:r=e
    with r:
        raw=r.read();return r.status,json.loads(raw) if raw else None

def mongo_eval(js):return subprocess.check_output(['docker','exec',mongo,'mongosh','--quiet','--eval',js],text=True).strip()
def pg_eval(sql):return subprocess.check_output(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-At','-c',sql],text=True).strip()
for base in bases:assert req(base,'POST','/v2/subscribers',{'subscriberId':user})[0]==201

def setup(kind):
    topic='pref-'+kind+'-'+suffix;ident='pref-id-'+kind+'-'+suffix;path='/v2/topics/'+topic+'/subscriptions'; ids=[]
    for base in bases:
        status,body=req(base,'POST',path,{'subscriptions':[{'subscriberId':user,'identifier':ident}]})
        assert status==201,(status,body)
        ids.append((body['data'][0]['_id'],body['data'][0]['subscriber']['_id']))
    subid,subscriber=ids[0]
    js='const d=db.getSiblingDB("novu-db"); d.preferences.insertOne({_environmentId:ObjectId(%s),_organizationId:ObjectId(%s),_subscriberId:ObjectId(%s),_topicSubscriptionId:ObjectId(%s),_templateId:ObjectId(%s),type:"SUBSCRIPTION_SUBSCRIBER_WORKFLOW",preferences:{all:{enabled:false,condition:{var:"payload.test"}}},deleted:false});' % tuple(json.dumps(x) for x in (env,org,subscriber,subid,wf))
    mongo_eval(js)
    subid,subscriber=ids[1]
    sql="INSERT INTO novu_rust_subscription_preferences(environment_id,subscriber_internal_id,subscription_internal_id,workflow_internal_id,preferences) VALUES ('%s','%s','%s','%s','{\"all\":{\"enabled\":false,\"condition\":{\"var\":\"payload.test\"}}}')" % (env,subscriber,subid,wf)
    pg_eval(sql)
    return topic,ident,path,ids

def counts(ids):
    m=mongo_eval('const d=db.getSiblingDB("novu-db"); print(d.preferences.countDocuments({_environmentId:ObjectId(%s),_topicSubscriptionId:ObjectId(%s),type:"SUBSCRIPTION_SUBSCRIBER_WORKFLOW"}));' % (json.dumps(env),json.dumps(ids[0][0])))
    p=pg_eval("SELECT count(*) FROM novu_rust_subscription_preferences WHERE environment_id='%s' AND subscription_internal_id='%s'" % (env,ids[1][0]))
    return int(m),int(p)

topic,ident,path,ids=setup('clear')
assert counts(ids)==(1,1)
for base in bases:
    status,body=req(base,'GET',path+'/'+ident)
    assert status==200 and body['data']['preferences']==[{'subscriptionId':ident,'enabled':False,'condition':{'var':'payload.test'}}],(base,status,body)
print('subscription_preferences_get: match')
workflow_js='''const d=db.getSiblingDB("novu-db"); d.notificationtemplates.insertOne({_id:ObjectId(%s),_environmentId:ObjectId(%s),_organizationId:ObjectId(%s),name:"Preference workflow",triggers:[{type:"event",identifier:"preference-workflow"}],critical:false,tags:["tag"],severity:"low",active:true,deleted:false});''' % tuple(json.dumps(x) for x in (wf,env,org))
mongo_eval(workflow_js)
pg_eval("INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,inbox_metadata) VALUES ('%s','preference-workflow',true,true,'{\"id\":\"%s\",\"name\":\"Preference workflow\",\"critical\":false,\"tags\":[\"tag\"],\"severity\":\"low\"}')" % (env,wf))
for base in bases:
    status,body=req(base,'GET',path+'/'+ident)
    assert status==200 and body['data']['preferences']==[{'subscriptionId':ident,'enabled':False,'condition':{'var':'payload.test'},'workflow':{'id':wf,'identifier':'preference-workflow','name':'Preference workflow','critical':False,'tags':['tag'],'severity':'low'}}],(base,status,body)
print('subscription_preferences_get_workflow: match')
mongo_eval('const d=db.getSiblingDB("novu-db"); d.notificationtemplates.deleteOne({_id:ObjectId(%s)});' % json.dumps(wf))
pg_eval("DELETE FROM novu_rust_workflows WHERE environment_id='%s' AND identifier='preference-workflow'" % env)
for base in bases:
    status,body=req(base,'PATCH',path+'/'+ident,{'name':'Changed','preferences':[]})
    assert status==200 and body['data']['name']=='Changed',(base,status,body)
assert counts(ids)==(0,0),counts(ids)
print('subscription_preferences_clear: match')
for kind in ['v1','topic']:
    topic,ident,path,ids=setup(kind)
    assert counts(ids)==(1,1)
    for base in bases:
        if kind=='v1':status,_=req(base,'POST','/v1/topics/'+topic+'/subscribers/removal',{'subscribers':[user]})
        else:status,_=req(base,'DELETE','/v2/topics/'+topic)
        assert status in (200,204),(base,status)
    assert counts(ids)==(1,1),(kind,counts(ids))
    print('subscription_preferences_orphan_'+kind+': match')
