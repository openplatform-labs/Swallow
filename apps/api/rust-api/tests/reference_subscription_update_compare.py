#!/usr/bin/env python3
"""Compare subscription name updates and related response state."""
import copy,json,os,time,urllib.error,urllib.request,uuid
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
key=os.environ['NOVU_TEST_API_KEY']; suffix=uuid.uuid4().hex
user='update-user-'+suffix;topic='update-topic-'+suffix;ident='update-ident-'+suffix
collection='/v2/topics/'+topic+'/subscriptions';path=collection+'/'+ident

def call(base,method,path,body=None,auth=True):
    headers={'Content-Type':'application/json'}
    if auth:headers['Authorization']='ApiKey '+key
    request=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers=headers,method=method)
    try:r=urllib.request.urlopen(request,timeout=10)
    except urllib.error.HTTPError as e:r=e
    with r:
        raw=r.read();return r.status,json.loads(raw) if raw else None

def clean(result):
    status,body=copy.deepcopy(result)
    def walk(v):
        if isinstance(v,dict):
            for k in list(v):
                if k in ('_id','id'):
                    assert len(v[k])==24,(k,v[k]);v[k]='<id>'
                elif k=='timestamp':v.pop(k)
                elif k in ('createdAt','updatedAt') and v[k]:v[k]='<time>'
                else:walk(v[k])
        elif isinstance(v,list):
            for item in v:walk(item)
    walk(body);return status,body

def compare(label,method,path,body=None,auth=True):
    results=[call(base,method,path,body,auth) for base in bases]
    assert clean(results[0])==clean(results[1]),(label,results)
    print(label+': match');return results

for base in bases:
    assert call(base,'POST','/v2/subscribers',{'subscriberId':user,'firstName':'First','email':'a@example.com'})[0]==201
results=compare('subscription_update_setup','POST',collection,{'subscriptions':[{'subscriberId':user,'identifier':ident,'name':'Initial'}],'name':'Topic'})
ids=[v[1]['data'][0]['_id'] for v in results]
noop=compare('subscription_update_empty','PATCH',path,{})
assert [v[1]['data']['_id'] for v in noop]==ids
again=compare('subscription_update_noop_repeat','PATCH',path,{})
assert [v[1]['data']['updatedAt'] for v in noop]==[v[1]['data']['updatedAt'] for v in again]
time.sleep(.03)
changed=compare('subscription_update_name','PATCH',path,{'name':'Changed'})
assert all(a[1]['data']['updatedAt']>b[1]['data']['updatedAt'] for a,b in zip(changed,again))
compare('subscription_update_get','GET',path)
compare('subscription_update_blank','PATCH',path,{'name':''})
compare('subscription_update_null','PATCH',path,{'name':None})
compare('subscription_get_null','GET',path)
compare('subscription_update_empty_preferences','PATCH',path,{'preferences':[]})
compare('subscription_update_unknown','PATCH',path,{'unknown':'ignored'})
compare('subscription_update_suffix','PATCH',path+':ctx_tenant:one',{'name':'Context suffix'})
compare('subscription_update_bad_name','PATCH',path,{'name':5})
compare('subscription_update_missing','PATCH',collection+'/missing',{'name':'x'})
compare('subscription_update_missing_topic','PATCH','/v2/topics/missing-'+suffix+'/subscriptions/'+ident,{'name':'x'})
compare('subscription_update_unauthorized','PATCH',path,{'name':'x'},False)
null_ident='null-'+suffix
compare('subscription_create_null_name','POST',collection,{'subscriptions':[{'subscriberId':user,'identifier':null_ident,'name':None}]})
compare('subscription_get_created_null','GET',collection+'/'+null_ident)
compare('subscription_update_created_null','PATCH',collection+'/'+null_ident,{})
