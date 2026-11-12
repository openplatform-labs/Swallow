#!/usr/bin/env python3
"""Compare bounded v2 topic search and separately issued cursors."""
import json,os,time,urllib.error,urllib.parse,urllib.request,uuid
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
api_key=os.environ['NOVU_TEST_API_KEY'];prefix='list'+uuid.uuid4().hex

def call(base,method,path,body=None,auth=True):
    headers={'Content-Type':'application/json'}
    if auth:headers['Authorization']='ApiKey '+api_key
    request=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers=headers,method=method)
    try:r=urllib.request.urlopen(request,timeout=10)
    except urllib.error.HTTPError as error:r=error
    with r:raw=r.read();return r.status,json.loads(raw) if raw else None

def comparable(result):
    status,body=result;body=json.loads(json.dumps(body))
    if status>=400:
        body.pop('timestamp',None)
        if 'errorId' in body:uuid.UUID(body.pop('errorId'))
    if isinstance(body.get('data'),list):
        for row in body['data']:
            row.pop('_id',None);row.pop('createdAt',None);row.pop('updatedAt',None)
        for field in ('next','previous'):body[field]=bool(body[field])
    return status,body

def query(label,params):
    path='/v2/topics?'+urllib.parse.urlencode(params)
    results=[call(base,'GET',path) for base in bases]
    assert comparable(results[0])==comparable(results[1]),(label,results)
    print(label+': match')
    return [item[1] for item in results]

for i in range(5):
    for base in bases:
        status,result=call(base,'POST','/v2/topics',{'key':prefix+str(i),'name':prefix+' Name '+str(i),'data':{'index':i}})
        assert status==201,(status,result)
    time.sleep(0.02)
query('topic_v2_key_partial',{'key':prefix.upper()+'2'})
query('topic_v2_name_partial',{'name':prefix.upper()+' name 3'})
query('topic_v2_regex_literal',{'key':'['})
query('topic_v2_filters_combined',{'key':prefix+'1','name':prefix+' Name 2'})
query('topic_v2_unknown_parameter',{'key':prefix,'unused':'ignored'})
query('topic_v2_unknown_direction',{'key':prefix,'orderDirection':'invalid'})
query('topic_v2_missing_cursor',{'after':'64a000000000000000000099'})
query('topic_v2_invalid_cursor',{'after':'invalid'})
query('topic_v2_both_cursors',{'after':'a','before':'b'})
query('topic_v2_limit_exceeded',{'limit':'101'})
for field in ['_id','createdAt','updatedAt','key','name']:
    for direction in ['ASC','DESC']:
        params={'key':prefix,'limit':'10','orderBy':field,'orderDirection':direction}
        result=query('topic_v2_order_'+field+'_'+direction,params)
        expected=[prefix+str(i) for i in (range(5) if direction=='ASC' else reversed(range(5)))]
        assert [row['key'] for row in result[0]['data']]==expected,(field,direction,result)
for field in ['_id','createdAt','updatedAt','key','name']:
    params={'key':prefix,'limit':'2','orderBy':field,'orderDirection':'ASC'}
    first=query('topic_v2_page_'+field,params)
    second=[]
    for base,page in zip(bases,first):
        second.append(call(base,'GET','/v2/topics?'+urllib.parse.urlencode({**params,'after':page['next']})))
    assert comparable(second[0])==comparable(second[1]),(field,second)
    back=[]
    for base,(_,page) in zip(bases,second):
        back.append(call(base,'GET','/v2/topics?'+urllib.parse.urlencode({**params,'before':page['previous']})))
    assert comparable(back[0])==comparable(back[1]),(field,back)
    assert [x['key'] for x in back[0][1]['data']]==[x['key'] for x in first[0]['data']]
    for base,page in zip(bases,first):
        result=call(base,'GET','/v2/topics?'+urllib.parse.urlencode({**params,'after':page['next'],'includeCursor':'true'}))
        assert result[1]['data'][0]['key']==page['data'][-1]['key'],(field,result)
    print('topic_v2_cursor_'+field+': match')
null_prefix='null'+uuid.uuid4().hex
for i,extra in enumerate([{}, {'name':None}, {'name':''}, {'name':'Named'}]):
    for base in bases:
        status,result=call(base,'POST','/v2/topics',{'key':null_prefix+str(i),**extra})
        assert status==201,(status,result)
for direction in ['ASC','DESC']:
    query('topic_v2_null_name_order_'+direction,{'key':null_prefix,'orderBy':'name','orderDirection':direction,'limit':'10'})
    params={'key':null_prefix,'limit':'2','orderBy':'name','orderDirection':direction}
    pages=query('topic_v2_null_page_'+direction,params)
    second=[call(base,'GET','/v2/topics?'+urllib.parse.urlencode({**params,'after':page['next']})) for base,page in zip(bases,pages)]
    assert comparable(second[0])==comparable(second[1]),(direction,second)
    print('topic_v2_null_cursor_'+direction+': match')
tie_prefix='tie'+uuid.uuid4().hex
for i in range(3):
    for base in bases:
        status,result=call(base,'POST','/v2/topics',{'key':tie_prefix+str(i),'name':'same'})
        assert status==201,(status,result)
params={'key':tie_prefix,'orderBy':'name','orderDirection':'ASC','limit':'2'}
pages=query('topic_v2_tied_names_first',params)
inclusive=[call(base,'GET','/v2/topics?'+urllib.parse.urlencode({**params,'after':page['next'],'includeCursor':'true'})) for base,page in zip(bases,pages)]
assert comparable(inclusive[0])==comparable(inclusive[1]),inclusive
assert [row['key'] for row in inclusive[0][1]['data']]==[tie_prefix+'0',tie_prefix+'1'],inclusive
print('topic_v2_tied_names_inclusive: match')
results=[call(base,'GET','/v2/topics',auth=False) for base in bases]
assert comparable(results[0])==comparable(results[1]),results
print('topic_v2_missing_auth: match')
