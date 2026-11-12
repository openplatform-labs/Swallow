#!/usr/bin/env python3
"""Compare v2 subscription creation, idempotency and partial failures."""
import copy,json,os,urllib.error,urllib.request,uuid
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
key=os.environ['NOVU_TEST_API_KEY']; suffix=uuid.uuid4().hex
users=['sub-'+x+'-'+suffix for x in 'abc']; topic='create-topic-'+suffix

def req(base,method,path,body=None,auth=True):
    headers={'Content-Type':'application/json'}
    if auth: headers['Authorization']='ApiKey '+key
    request=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers=headers,method=method)
    try: response=urllib.request.urlopen(request,timeout=10)
    except urllib.error.HTTPError as e: response=e
    with response:
        raw=response.read(); return response.status,json.loads(raw) if raw else None

def clean(value):
    value=copy.deepcopy(value)
    def walk(v):
        if isinstance(v,dict):
            for k in list(v):
                if k in ('_id','id'):
                    assert len(v[k])==24,(k,v[k]); v[k]='<id>'
                elif k=='timestamp': v.pop(k)
                elif k in ('createdAt','updatedAt') and v[k]: v[k]='<time>'
                else: walk(v[k])
        elif isinstance(v,(list,tuple)):
            for item in v: walk(item)
    walk(value); return value

def compare(label,method,path,body=None,auth=True):
    results=[req(base,method,path,body,auth) for base in bases]
    assert clean(results[0])==clean(results[1]),(label,results)
    print(label+': match')
    return results

for base in bases:
    for u in users: assert req(base,'POST','/v2/subscribers',{'subscriberId':u,'firstName':'First','email':'a@example.com'})[0]==201
path='/v2/topics/'+topic+'/subscriptions'
results=compare('subscription_create_default','POST',path,{'subscriberIds':users[:2],'name':'Topic'})
assert all(all(x['createdAt']=='' and x['updatedAt']=='' for x in body['data']) for _,body in results)
ids=[body['data'][0]['_id'] for _,body in results]
results=compare('subscription_existing','POST',path,{'subscriberIds':[users[0]]})
assert [body['data'][0]['_id'] for _,body in results]==ids
compare('subscription_custom','POST',path,{'subscriptions':[{'identifier':'custom-'+suffix,'subscriberId':users[0],'name':'Custom'}]})
compare('subscription_custom_idempotent','POST',path,{'subscriptions':[{'identifier':'custom-'+suffix,'subscriberId':users[0],'name':'Changed'}]})
compare('subscription_mixed','POST',path,{'subscriberIds':[users[0],'missing-'+suffix,users[2]]})
compare('subscription_missing','POST',path,{'subscriberIds':['missing-'+suffix]})
compare('subscription_oversize','POST',path,{'subscriptions':[{'identifier':'x'*513,'subscriberId':users[0]}]})
compare('subscription_missing_array','POST',path,{})
compare('subscription_empty','POST',path,{'subscriptions':[]})
compare('subscription_string_array','POST',path,{'subscriptions':[users[0]]})
compare('subscription_auth','POST',path,{'subscriberIds':[users[0]]},False)
for index in range(8):
    compare('subscription_limit_fill_'+str(index),'POST',path,{'subscriptions':[{'identifier':f'limit-{index}-{suffix}','subscriberId':users[0]}]})
compare('subscription_limit_exceeded','POST',path,{'subscriptions':[{'identifier':'limit-over-'+suffix,'subscriberId':users[0]}]})
compare('subscription_limit_existing','POST',path,{'subscriberIds':[users[0]]})
