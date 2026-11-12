#!/usr/bin/env python3
"""Compare bounded workflow-specific preference API against disposable servers.

The runner seeds one editable and one read-only single-in-app workflow.
Independent subscriber/workflow preference timestamps are validated then removed.
"""
import json,os,urllib.error,urllib.request,uuid
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
key=os.environ['NOVU_TEST_API_KEY']; subscriber='wfpref-'+uuid.uuid4().hex
path='/v2/subscribers/'+subscriber+'/preferences'
def call(base,method,path,body=None):
    req=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers={'Authorization':'ApiKey '+key,'Content-Type':'application/json'},method=method)
    try:r=urllib.request.urlopen(req,timeout=15)
    except urllib.error.HTTPError as e:r=e
    with r:status,raw=r.status,r.read()
    value=json.loads(raw) if raw else None
    if status>=400:value.pop('timestamp',None)
    return status,value
def compare(name,method,body=None,expected=200):
    results=[call(base,method,path,body) for base in bases]
    for status,data in results:
        assert status==expected,(name,results)
        if status==200:
            for wf in data['data']['workflows']:
                if 'updatedAt' in wf: assert wf.pop('updatedAt').endswith('Z')
    assert results[0]==results[1],(name,results)
    print(name+': match')
for base in bases: assert call(base,'POST','/v2/subscribers',{'subscriberId':subscriber})[0]==201
compare('workflow_initial','GET')
compare('workflow_global_disable','PATCH',{'channels':{'in_app':False}})
compare('workflow_override_enable','PATCH',{'workflowId':'greeting','channels':{'in_app':True}})
compare('workflow_get_override','GET')
compare('workflow_override_by_id','PATCH',{'workflowId':'64a000000000000000000004','channels':{'in_app':False}})
compare('workflow_override_by_slug','PATCH',{'workflowId':'greeting_wf_eV5Qd9HRCowdmEZY','channels':{'in_app':True}})
compare('workflow_global_clears_override','PATCH',{'channels':{'in_app':False}})
compare('workflow_critical_rejected','PATCH',{'workflowId':'urgent','channels':{'in_app':False}},400)

compare('workflow_missing','PATCH',{'workflowId':'nonexistent','channels':{'in_app':False}},404)
