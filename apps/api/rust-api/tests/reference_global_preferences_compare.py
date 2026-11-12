#!/usr/bin/env python3
"""Compare global preference API for an isolated single-in-app workflow fixture.

Requires both API URLs and the synthetic API key; the runner seeds a subscriber
and one active workflow with no workflow-resource preference record. Does not
compare worker delivery, overrides, schedules, contexts, critical workflows or
webhooks.
"""
import json,os,urllib.error,urllib.request
bases=[os.environ['NOVU_REFERENCE_URL'].rstrip('/'),os.environ['NOVU_RUST_URL'].rstrip('/')]
key=os.environ['NOVU_TEST_API_KEY']
path='/v2/subscribers/alice/preferences'
def call(base,method,target,body=None,auth=True):
    headers={'Content-Type':'application/json'}
    if auth: headers['Authorization']='ApiKey '+key
    req=urllib.request.Request(base+target,data=json.dumps(body).encode() if body is not None else None,headers=headers,method=method)
    try: response=urllib.request.urlopen(req,timeout=15)
    except urllib.error.HTTPError as error: response=error
    with response: status,raw=response.status,response.read()
    value=json.loads(raw) if raw else None
    if status>=400: value.pop('timestamp',None)
    return status,value
def compare(name,method,target,body=None,expected=200,auth=True):
    results=[call(base,method,target,body,auth) for base in bases]
    assert results[0]==results[1] and results[0][0]==expected,(name,results)
    print(name+': match')
    return results[0][1]
compare('preference_initial','GET',path)
compare('preference_global','GET',path+'/global')
compare('preference_disable','PATCH',path,{'channels':{'in_app':False,'email':False}})
compare('preference_after_disable','GET',path)
compare('preference_reenable','PATCH',path,{'channels':{'in_app':True}})
compare('preference_after_reenable','GET',path+'/global')
compare('preference_missing','GET','/v2/subscribers/missing/preferences',expected=404)
compare('preference_missing_patch','PATCH','/v2/subscribers/missing/preferences',{'channels':{'in_app':False}},expected=404)
compare('preference_missing_auth','GET',path,expected=401,auth=False)
