#!/usr/bin/env python3
"""Compare online-status transitions and subsequent v2 reads on disposable APIs."""
import datetime,json,os,urllib.error,urllib.request,uuid
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
key=os.environ['NOVU_TEST_API_KEY']; subscriber='online-'+uuid.uuid4().hex

def req(base,method,path,body=None,auth=True):
    headers={'Content-Type':'application/json'}
    if auth: headers['Authorization']='ApiKey '+key
    request=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers=headers,method=method)
    try: response=urllib.request.urlopen(request,timeout=10)
    except urllib.error.HTTPError as error: response=error
    with response: return response.status,json.load(response)

def clean(result):
    status,body=result; body=json.loads(json.dumps(body))
    if status>=400: body.pop('timestamp',None)
    else:
        value=body['data']
        value.pop('_id',None); value.pop('id',None); value.pop('createdAt',None); value.pop('updatedAt',None)
        if value.get('lastOnlineAt') is not None:
            datetime.datetime.fromisoformat(value['lastOnlineAt'].replace('Z','+00:00'))
            value['lastOnlineAt']='TIMESTAMP'
    return status,body

for base in bases: assert req(base,'POST','/v2/subscribers',{'subscriberId':subscriber,'firstName':'Online'})[0]==201
path='/v1/subscribers/'+subscriber+'/online-status'
previous=[None,None]
for label,body in [('online',{'isOnline':True}),('offline',{'isOnline':False}),('online_again',{'isOnline':True}),('offline_again',{'isOnline':False})]:
    results=[req(base,'PATCH',path,body) for base in bases]
    assert clean(results[0])==clean(results[1]),(label,results)
    reads=[]
    for index,(base,result) in enumerate(zip(bases,results)):
        assert result[0]==200,(label,result)
        now=result[1]['data'].get('lastOnlineAt')
        if body['isOnline']: assert now==previous[index],(label,result,previous)
        else: assert now is not None,(label,result)
        previous[index]=now
        get=req(base,'GET','/v2/subscribers/'+subscriber)
        assert get[0]==200 and get[1]['data']['isOnline']==body['isOnline'] and get[1]['data'].get('lastOnlineAt')==now,(label,get,result)
        reads.append(get)
        listed=req(base,'GET','/v2/subscribers?subscriberId='+subscriber)
        assert listed[0]==200 and len(listed[1]['data'])==1 and listed[1]['data'][0]['isOnline']==body['isOnline'] and listed[1]['data'][0].get('lastOnlineAt')==now,(label,listed)
    assert clean(reads[0])==clean(reads[1]),(label,reads)
    print(label+': match')
for label,target,body,auth in [('missing','missing',{'isOnline':True},True),('unauthorized',subscriber,{'isOnline':True},False),('missing_field',subscriber,{},True),('invalid_type',subscriber,{'isOnline':'true'},True),('null',subscriber,{'isOnline':None},True)]:
    results=[req(base,'PATCH','/v1/subscribers/'+target+'/online-status',body,auth) for base in bases]
    assert clean(results[0])==clean(results[1]),(label,results)
    print(label+': match')
