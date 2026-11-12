#!/usr/bin/env python3
"""Compare subscription preference creation and replacement APIs."""
import copy,json,os,subprocess,urllib.error,urllib.request,uuid
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']];key=os.environ['NOVU_TEST_API_KEY'];suffix=uuid.uuid4().hex
user='preference-api-'+suffix;topic='preference-api-topic-'+suffix;identifier='preference-api-id-'+suffix
path='/v2/topics/'+topic+'/subscriptions';item=path+'/'+identifier

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
                if k in ('_id','id') and k!='id' or k=='id' and value[k]!='64a000000000000000000004':
                    assert len(value[k])==24;value[k]='<id>'
                elif k=='timestamp':value.pop(k)
                elif k=='errorId':uuid.UUID(value.pop(k))
                elif k in ('createdAt','updatedAt') and value[k]:value[k]='<time>'
                else:walk(value[k])
        elif isinstance(value,list):
            for item in value:walk(item)
    walk(body);return status,body

def compare(label,method,path,body=None,auth=True):
    results=[req(base,method,path,body,auth) for base in bases]
    assert clean(results[0])==clean(results[1]),(label,results)
    print(label+': match',flush=True);return results

for base in bases:assert req(base,'POST','/v2/subscribers',{'subscriberId':user,'firstName':'First'})[0]==201
compare('preference_create','POST',path,{'subscriptions':[{'identifier':identifier,'subscriberId':user}],'preferences':[{'workflowId':'greeting','enabled':False}]})
compare('preference_get','GET',item)
compare('preference_create_existing_preserves','POST',path,{'subscriptions':[{'identifier':identifier,'subscriberId':user}],'preferences':[{'workflowId':'greeting','enabled':True}]})
compare('preference_replace','PATCH',item,{'name':'Updated','preferences':[{'workflowId':'greeting','enabled':True,'condition':{'===':[{'var':'payload.tier'},'gold']}}]})
compare('preference_get_condition','GET',item)
compare('preference_replace_internal_id','PATCH',item,{'preferences':[{'workflowId':'64a000000000000000000004','enabled':False}]})
compare('preference_multiple_workflows','PATCH',item,{'preferences':[{'workflowId':'greeting','enabled':False},{'workflowId':'urgent','enabled':True}]})
compare('preference_multiple_workflows_get','GET',item)
compare('preference_replace_unknown','PATCH',item,{'preferences':[{'workflowId':'missing','enabled':False}]})
compare('preference_get_cleared','GET',item)
compare('preference_string_validation','PATCH',item,{'preferences':['greeting']})
compare('preference_default_enabled','PATCH',item,{'preferences':[{'workflowId':'greeting'}]})
mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER'];pg=os.environ['NOVU_RUST_PG_CONTAINER']
subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval','db.getSiblingDB("novu-db").preferences.updateOne({_environmentId:ObjectId("64a000000000000000000002"),_templateId:ObjectId("64a000000000000000000004"),type:"WORKFLOW_RESOURCE"},{$set:{"preferences.all.enabled":false}})'],check=True,stdout=subprocess.DEVNULL)
subprocess.run(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',"UPDATE novu_rust_workflow_preferences SET all_enabled=false WHERE environment_id='64a000000000000000000002' AND workflow_identifier='greeting'"],check=True,stdout=subprocess.DEVNULL)
compare('preference_workflow_default_disabled','PATCH',item,{'preferences':[{'workflowId':'greeting'}]})
subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval','db.getSiblingDB("novu-db").preferences.updateOne({_environmentId:ObjectId("64a000000000000000000002"),_templateId:ObjectId("64a000000000000000000004"),type:"WORKFLOW_RESOURCE"},{$set:{"preferences.all.enabled":true}})'],check=True,stdout=subprocess.DEVNULL)
subprocess.run(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',"UPDATE novu_rust_workflow_preferences SET all_enabled=true WHERE environment_id='64a000000000000000000002' AND workflow_identifier='greeting'"],check=True,stdout=subprocess.DEVNULL)
subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval','db.getSiblingDB("novu-db").notificationtemplates.updateOne({_id:ObjectId("64a000000000000000000004")},{$set:{tags:["subscription-tag"]}})'],check=True,stdout=subprocess.DEVNULL)
subprocess.run(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',"UPDATE novu_rust_workflows SET inbox_metadata=jsonb_set(inbox_metadata,'{tags}','[\"subscription-tag\"]'::jsonb) WHERE environment_id='64a000000000000000000002' AND identifier='greeting'"],check=True,stdout=subprocess.DEVNULL)
compare('preference_filter_tag','PATCH',item,{'preferences':[{'filter':{'tags':['subscription-tag']},'enabled':False}]})
compare('preference_filter_tag_get','GET',item)
subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval','db.getSiblingDB("novu-db").notificationtemplates.updateOne({_id:ObjectId("64a000000000000000000004")},{$set:{tags:[]}})'],check=True,stdout=subprocess.DEVNULL)
subprocess.run(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',"UPDATE novu_rust_workflows SET inbox_metadata=jsonb_set(inbox_metadata,'{tags}','[]'::jsonb) WHERE environment_id='64a000000000000000000002' AND identifier='greeting'"],check=True,stdout=subprocess.DEVNULL)
compare('preference_empty_reset','PATCH',item,{'preferences':[]})
compare('preference_empty_get','GET',item)
compare('preference_missing_sub','PATCH',path+'/missing',{'preferences':[{'workflowId':'greeting','enabled':False}]})
compare('preference_auth','PATCH',item,{'preferences':[{'workflowId':'greeting'}]},False)
if os.environ.get('NOVU_TEST_MONGO_REPLICA') == '1':
    compare('preference_failure_setup','PATCH',item,{'name':'Before failure','preferences':[{'workflowId':'greeting','enabled':False}]})
    pg_constraint="ALTER TABLE novu_rust_subscription_preferences ADD CONSTRAINT test_reject_enabled CHECK ((preferences->'all'->>'enabled') IS DISTINCT FROM 'true') NOT VALID"
    subprocess.run(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',pg_constraint],check=True,stdout=subprocess.DEVNULL)
    failpoint='db.adminCommand({configureFailPoint:"failCommand",mode:{times:1},data:{failCommands:["insert"],errorCode:2}})'
    try:
        subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval',failpoint],check=True,stdout=subprocess.DEVNULL)
        results=[req(base,'PATCH',item,{'name':'After failure','preferences':[{'workflowId':'greeting','enabled':True}]}) for base in bases]
        assert [r[0] for r in results]==[500,500],results
        assert clean(results[0])==clean(results[1]),results
    finally:
        subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval','db.adminCommand({configureFailPoint:"failCommand",mode:"off"})'],check=True,stdout=subprocess.DEVNULL)
        subprocess.run(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c','ALTER TABLE novu_rust_subscription_preferences DROP CONSTRAINT test_reject_enabled'],check=True,stdout=subprocess.DEVNULL)
    state=compare('preference_failed_insert_state','GET',item)
    assert all('preferences' not in body['data'] for _,body in state),state
    state=compare('preference_failed_insert_name','PATCH',item,{})
    assert all(body['data']['name']=='Before failure' for _,body in state),state
    print('preference_failed_insert_status: match',flush=True)
    compare('preference_update_failure_setup','PATCH',item,{'preferences':[{'workflowId':'greeting','enabled':False}]})
    pg_constraint="ALTER TABLE novu_rust_topic_subscribers ADD CONSTRAINT test_reject_name CHECK (name IS DISTINCT FROM 'After failure') NOT VALID"
    subprocess.run(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',pg_constraint],check=True,stdout=subprocess.DEVNULL)
    failpoint='db.adminCommand({configureFailPoint:"failCommand",mode:{times:1},data:{failCommands:["update"],errorCode:2}})'
    try:
        subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval',failpoint],check=True,stdout=subprocess.DEVNULL)
        results=[req(base,'PATCH',item,{'name':'After failure','preferences':[{'workflowId':'greeting','enabled':True}]}) for base in bases]
        assert [r[0] for r in results]==[500,500],results
        assert clean(results[0])==clean(results[1]),results
    finally:
        subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval','db.adminCommand({configureFailPoint:"failCommand",mode:"off"})'],check=True,stdout=subprocess.DEVNULL)
        subprocess.run(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c','ALTER TABLE novu_rust_topic_subscribers DROP CONSTRAINT test_reject_name'],check=True,stdout=subprocess.DEVNULL)
    state=compare('preference_failed_name_state','GET',item)
    assert all(body['data']['preferences'][0]['enabled'] is True for _,body in state),state
    state=compare('preference_failed_name_value','PATCH',item,{})
    assert all(body['data']['name']=='Before failure' for _,body in state),state
    print('preference_failed_name_status: match',flush=True)
