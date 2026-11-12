#!/usr/bin/env python3
"""Compare subscriber search on two isolated, equivalently seeded APIs.

Creates four unique synthetic subscribers; generated IDs and timestamps are
normalized. Uses separately issued cursors. Not a full SDK or performance test.
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

bases=[os.environ['NOVU_REFERENCE_URL'].rstrip('/'),os.environ['NOVU_RUST_URL'].rstrip('/')]
key=os.environ['NOVU_TEST_API_KEY']
prefix='search'+uuid.uuid4().hex

def call(base,method,path,body=None):
    req=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers={'Content-Type':'application/json','Authorization':'ApiKey '+key},method=method)
    try: response=urllib.request.urlopen(req,timeout=10)
    except urllib.error.HTTPError as error: response=error
    with response: status,raw=response.status,response.read()
    return status,json.loads(raw) if raw else None

def comparable(result):
    status,body=result
    body=json.loads(json.dumps(body))
    if status>=400: body.pop('timestamp',None)
    if isinstance(body.get('data'),list):
        for row in body['data']:
            row.pop('_id',None)
            row.pop('createdAt',None)
            row.pop('updatedAt',None)
        for field in ('next','previous'):
            body[field]=bool(body[field])
    return status,body

def query(name,params):
    path='/v2/subscribers?'+urllib.parse.urlencode(params)
    results=[call(base,'GET',path) for base in bases]
    assert comparable(results[0])==comparable(results[1]),(name,results)
    print(name+': match')
    return [result[1] for result in results]

for i in range(4):
    body={'subscriberId':prefix+str(i),'firstName':prefix,'lastName':'User'+str(i),'email':f'{prefix}{i}@example.test','phone':'+123'}
    results=[call(base,'POST','/v2/subscribers',body) for base in bases]
    assert all(status==201 for status,_ in results),results
    time.sleep(0.03)
query('exact_email',{'email':f'{prefix}2@example.test'})
query('partial_email',{'email':prefix})
query('phone_and_subscriber',{'phone':'+123','subscriberId':prefix+'1'})
query('literal_name',{'name':'['})
query('case_insensitive_name',{'name':prefix.upper()+' user2'})
query('missing_cursor',{'after':'64a000000000000000000099'})
query('invalid_cursor',{'after':'invalid'})
query('both_cursors',{'after':'a','before':'b'})
for direction in ['ASC','DESC']:
    params={'name':prefix,'limit':'10','orderBy':'_id','orderDirection':direction}
    result=query('object_id_order_'+direction,params)
    expected=[prefix+str(i) for i in (range(4) if direction=='ASC' else reversed(range(4)))]
    assert [row['subscriberId'] for row in result[0]['data']]==expected, result
base_params={'name':prefix,'limit':'2','orderBy':'updatedAt','orderDirection':'ASC'}
pages=query('page_one',base_params)
second=[]
for base,page in zip(bases,pages):
    second.append(call(base,'GET','/v2/subscribers?'+urllib.parse.urlencode({**base_params,'after':page['next']})))
assert comparable(second[0])==comparable(second[1]),second
assert all(result[1]['previous'] for result in second),second
backs=[]
for base,(_,page) in zip(bases,second):
    backs.append(call(base,'GET','/v2/subscribers?'+urllib.parse.urlencode({**base_params,'before':page['previous']})))
assert comparable(backs[0])==comparable(backs[1]),backs
assert [x['subscriberId'] for x in backs[0][1]['data']]==[x['subscriberId'] for x in pages[0]['data']]
print('cursor_forward_back: match')
