#!/usr/bin/env python3
"""Compare selected v2 topic writes and v1/v2 reads on disposable APIs."""
import datetime,json,os,urllib.error,urllib.request,uuid
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
key=os.environ['NOVU_TEST_API_KEY'];topic='v2-topic-'+uuid.uuid4().hex

def request(base,method,path,body=None,auth=True):
    headers={'Content-Type':'application/json'}
    if auth:headers['Authorization']='ApiKey '+key
    req=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers=headers,method=method)
    try:r=urllib.request.urlopen(req,timeout=10)
    except urllib.error.HTTPError as error:r=error
    with r: raw=r.read();return r.status,json.loads(raw) if raw else None

def clean(result):
    status,body=result;body=json.loads(json.dumps(body))
    if status>=400:body.pop('timestamp',None)
    elif isinstance(body,dict):
        item=body.get('data',{})
        if isinstance(item,list):
            for row in item:
                row.pop('_id',None)
                for field in ('createdAt','updatedAt'):
                    if field in row: datetime.datetime.fromisoformat(row.pop(field).replace('Z','+00:00'))
        if isinstance(item,dict):
            item.pop('_id',None)
            if 'subscriberId' in item:item.pop('id',None)
            for field in ('createdAt','updatedAt'):
                if field in item:datetime.datetime.fromisoformat(item.pop(field).replace('Z','+00:00'))
    return status,body

def compare(label,method,path,body=None,auth=True):
    results=[request(base,method,path,body,auth) for base in bases]
    assert clean(results[0])==clean(results[1]),(label,results)
    print(label+': match')
    return results

compare('v2_create_minimal','POST','/v2/topics',{'key':topic})
compare('v2_get_minimal','GET','/v2/topics/'+topic)
compare('v1_reads_v2','GET','/v1/topics/'+topic)
compare('v1_list_v2','GET','/v1/topics?key='+topic)
compare('v2_upsert_data','POST','/v2/topics',{'key':topic,'name':'Hello','data':{'category':'news','number':1,'active':True,'tags':['a','b']}})
compare('v2_get_data','GET','/v2/topics/'+topic)
compare('v1_reads_v2_data','GET','/v1/topics/'+topic)
compare('v1_list_v2_data','GET','/v1/topics?key='+topic)
compare('v2_upsert_unchanged','POST','/v2/topics',{'key':topic})
compare('v2_upsert_false','POST','/v2/topics?failIfExists=false',{'key':topic})
compare('v2_conflict','POST','/v2/topics?failIfExists=true',{'key':topic,'name':'Ignored'})
compare('v2_patch_name','PATCH','/v2/topics/'+topic,{'name':'Updated'})
compare('v2_clear_data','PATCH','/v2/topics/'+topic,{'data':None})
compare('v2_get_cleared','GET','/v2/topics/'+topic)
compare('v2_patch_empty','PATCH','/v2/topics/'+topic,{})
compare('v2_missing','GET','/v2/topics/missing-'+topic)
compare('v2_patch_missing','PATCH','/v2/topics/missing-'+topic,{'name':'Nobody'})
compare('v2_unauthorized','GET','/v2/topics/'+topic,auth=False)
compare('v2_invalid_key','POST','/v2/topics',{'key':'bad space'})
compare('v2_null_name','PATCH','/v2/topics/'+topic,{'name':None})
compare('v2_get_null_name','GET','/v2/topics/'+topic)
compare('v1_get_null_name','GET','/v1/topics/'+topic)
compare('v1_list_null_name','GET','/v1/topics?key='+topic)
compare('v2_missing_key','POST','/v2/topics',{})
compare('v2_bad_name','PATCH','/v2/topics/'+topic,{'name':123})
compare('v2_bad_data','PATCH','/v2/topics/'+topic,{'data':{'nested':{'a':1}}})
compare('v2_data_type','PATCH','/v2/topics/'+topic,{'data':['not-object']})
compare('v2_data_oversized','PATCH','/v2/topics/'+topic,{'data':{'large':'x'*(65*1024)}})

subscriber='delete-user-'+uuid.uuid4().hex
other_topic='delete-other-'+uuid.uuid4().hex
for base in bases:
    assert request(base,'POST','/v2/subscribers',{'subscriberId':subscriber})[0]==201
    for current in (topic,other_topic):
        status,result=request(base,'POST','/v1/topics/'+current+'/subscribers',{'subscribers':[subscriber]})
        assert status==200 and result['data']['succeeded']==[subscriber],(base,result)
compare('v2_delete_unauthorized','DELETE','/v2/topics/'+topic,auth=False)
compare('v2_delete_with_members','DELETE','/v2/topics/'+topic+'?force=false')
compare('v2_delete_then_get','GET','/v2/topics/'+topic)
compare('v2_delete_then_v1_get','GET','/v1/topics/'+topic)
compare('v2_delete_repeat','DELETE','/v2/topics/'+topic)
compare('v2_delete_subscriber_alive','GET','/v2/subscribers/'+subscriber)
result=compare('v2_delete_other_topic','GET','/v1/subscribers/'+subscriber+'?includeTopics=true')
assert result[0][1]['data']['topics']==[other_topic]
empty='delete-empty-'+uuid.uuid4().hex
compare('v2_delete_empty_create','POST','/v2/topics',{'key':empty})
compare('v2_delete_empty','DELETE','/v2/topics/'+empty)
