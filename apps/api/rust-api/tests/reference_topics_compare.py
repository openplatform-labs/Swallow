#!/usr/bin/env python3
"""Compare selected v1 topic membership and subscriber retrieval behavior."""
import json,os,urllib.error,urllib.request,uuid
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
key=os.environ['NOVU_TEST_API_KEY']; suffix=uuid.uuid4().hex
subscriber='topic-user-'+suffix; topic='topic-'+suffix

def req(base,method,path,body=None,auth=True):
    headers={'Content-Type':'application/json'}
    if auth: headers['Authorization']='ApiKey '+key
    request=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers=headers,method=method)
    try: response=urllib.request.urlopen(request,timeout=10)
    except urllib.error.HTTPError as error: response=error
    with response:
        raw=response.read()
        return response.status,json.loads(raw) if raw else None

def clean(result):
    status,body=result; body=json.loads(json.dumps(body))
    if status>=400:
        body.pop('timestamp',None)
        if 'errorId' in body:
            uuid.UUID(body.pop('errorId'))
    elif isinstance(body,dict) and isinstance(body.get('data'),list):
        for topic in body['data']:
            for field in ('_id','createdAt','updatedAt'): topic.pop(field,None)
            if 'subscribers' in topic: topic['subscribers'].sort()
    elif isinstance(body,dict) and isinstance(body.get('data'),dict) and 'subscriberId' in body['data']:
        data=body['data']; data.pop('_id',None); data.pop('id',None); data.pop('createdAt',None); data.pop('updatedAt',None)
        if 'topics' in data: data['topics'].sort()
    elif isinstance(body,dict) and isinstance(body.get('data'),dict):
        data=body['data']
        if isinstance(data.get('data'),list):
            for topic in data['data']:
                for field in ('_id','createdAt','updatedAt'): topic.pop(field,None)
                if 'subscribers' in topic: topic['subscribers'].sort()
        for key in ('_id','_topicId','_subscriberId','createdAt','updatedAt'): data.pop(key,None)
        if 'subscribers' in data: data['subscribers'].sort()
    return status,body

def compare(label,method,path,body=None,auth=True):
    a,b=[req(base,method,path,body,auth) for base in bases]
    assert clean(a)==clean(b),(label,a,b)
    print(label+': match')
    return a

for base in bases: assert req(base,'POST','/v2/subscribers',{'subscriberId':subscriber,'firstName':'Topic'})[0]==201
member='/v1/topics/'+topic+'/subscribers'
retrieve='/v1/subscribers/'+subscriber
compare('v1_get', 'GET',retrieve)
compare('v1_empty_topics','GET',retrieve+'?includeTopics=true')
compare('v1_missing','GET','/v1/subscribers/no-such-topic-user')
compare('v1_unauthorized','GET',retrieve,auth=False)
compare('topic_assign','POST',member,{'subscribers':[subscriber,'missing-'+suffix,subscriber]})
compare('topic_get','GET','/v1/topics/'+topic)
compare('topic_member_get','GET',member+'/'+subscriber)
compare('topic_member_missing','GET',member+'/missing-'+suffix)
compare('topic_delete_conflict','DELETE','/v1/topics/'+topic)
compare('topic_rename','PATCH','/v1/topics/'+topic,{'name':'New name'})
result=compare('v1_topics','GET',retrieve+'?includeTopics=true')
assert result[1]['data']['topics']==[topic],result
compare('topic_assign_idempotent','POST',member,{'subscribers':[subscriber]})
compare('topic_remove','POST',member+'/removal',{'subscribers':[subscriber,'missing-'+suffix]})
compare('topic_member_removed','GET',member+'/'+subscriber)
result=compare('v1_topics_removed','GET',retrieve+'?includeTopics=true')
assert result[1]['data']['topics']==[],result
compare('topic_remove_missing','POST','/v1/topics/missing-'+suffix+'/subscribers/removal',{'subscribers':[subscriber]})
compare('topic_unauthorized','POST',member,{'subscribers':[subscriber]},False)
compare('topic_invalid','POST',member,{'subscribers':'bad'})
compare('topic_delete_empty','DELETE','/v1/topics/'+topic)
compare('topic_get_deleted','GET','/v1/topics/'+topic)
compare('topic_delete_missing','DELETE','/v1/topics/'+topic)
created='created-'+suffix
compare('topic_create','POST','/v1/topics',{'key':created,'name':' Created '})
compare('topic_created_get','GET','/v1/topics/'+created)
compare('topic_create_conflict','POST','/v1/topics',{'key':created,'name':'Created'})
compare('topic_rename_empty','PATCH','/v1/topics/'+created,{'name':''})
compare('topic_create_bad','POST','/v1/topics',{'key':created})
compare('topic_rename_bad','PATCH','/v1/topics/'+created,{'name':5})
group='group-'+suffix
for index in range(4):
    for base in bases: assert req(base,'POST','/v1/topics',{'key':f'{group}-{index}','name':group})[0]==201
compare('topic_list_first','GET',f'/v1/topics?name={group}&pageSize=2&page=0')
compare('topic_list_second','GET',f'/v1/topics?name={group}&pageSize=2&page=1')
compare('topic_list_empty','GET',f'/v1/topics?name={group}&pageSize=2&page=2')
compare('topic_list_exact_key','GET',f'/v1/topics?key={group}-2')
compare('topic_list_mismatch','GET',f'/v1/topics?key={group}-2&name=missing')
compare('topic_list_too_large','GET','/v1/topics?pageSize=11')
compare('topic_list_negative_page','GET','/v1/topics?page=-1')
compare('topic_list_missing_auth','GET','/v1/topics',auth=False)
compare('topic_list_zero','GET','/v1/topics?pageSize=0')
compare('topic_list_unknown','GET',f'/v1/topics?name={group}&ignored=yes')
