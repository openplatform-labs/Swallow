#!/usr/bin/env python3
"""Compare topic subscription listing and per-server cursor traversal."""
import copy,json,os,subprocess,urllib.error,urllib.parse,urllib.request,uuid
bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
key=os.environ['NOVU_TEST_API_KEY']; suffix=uuid.uuid4().hex
topic='list-sub-'+suffix; users=['list-user-'+x+'-'+suffix for x in 'abcd']
path='/v2/topics/'+topic+'/subscriptions'

def call(base,method,path,body=None,auth=True):
    headers={'Content-Type':'application/json'}
    if auth: headers['Authorization']='ApiKey '+key
    request=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers=headers,method=method)
    try: r=urllib.request.urlopen(request,timeout=10)
    except urllib.error.HTTPError as e: r=e
    with r:
        raw=r.read(); return r.status,json.loads(raw) if raw else None

def clean(result):
    status,body=copy.deepcopy(result)
    def walk(v):
        if isinstance(v,dict):
            for k in list(v):
                if k=='_id': assert len(v[k])==24; v[k]='<id>'
                elif k=='timestamp': v.pop(k)
                elif k=='errorId': uuid.UUID(v.pop(k))
                elif k in ('createdAt','updatedAt') and v[k]: v[k]='<time>'
                elif k in ('next','previous'): v[k]=bool(v[k])
                else: walk(v[k])
        elif isinstance(v,list):
            for item in v: walk(item)
    walk(body);return status,body

def query(label,params=None,paths=None,auth=True):
    paths=paths or [path+('?' + urllib.parse.urlencode(params,doseq=True) if params else '')]*2
    results=[call(base,'GET',p,auth=auth) for base,p in zip(bases,paths)]
    assert clean(results[0])==clean(results[1]),(label,results)
    print(label+': match');return results

for base in bases:
    for user in users:
        assert call(base,'POST','/v2/subscribers',{'subscriberId':user,'firstName':'First'})[0]==201
    for i,user in enumerate(users):
        assert call(base,'POST',path,{'subscriptions':[{'identifier':f'list-ident-{i}-{suffix}','subscriberId':user,'name':'Invisible'}], 'name':'List topic'})[0]==201
    assert call(base,'POST',path,{'subscriptions':[{'identifier':'list-extra-'+suffix,'subscriberId':users[0]}]})[0]==201

query('subscriptions_all')
query('subscriptions_subscriber',{'subscriberId':users[0]})
query('subscriptions_missing_subscriber',{'subscriberId':'absent'})
query('subscriptions_order_ignored',{'orderBy':'createdAt','orderDirection':'ASC'})
query('subscriptions_unknown_direction',{'orderDirection':'invalid'})
query('subscriptions_limit_exceeded',{'limit':101})
query('subscriptions_missing_cursor',{'after':'64a000000000000000000099'})
query('subscriptions_invalid_cursor',{'after':'invalid'})
query('subscriptions_both_cursors',{'after':'a','before':'b'})
query('subscriptions_unknown_topic',paths=['/v2/topics/missing-'+suffix+'/subscriptions']*2)
query('subscriptions_unauthorized',auth=False)
for direction in ['ASC','DESC']:
    params={'limit':2,'orderDirection':direction}
    first=query('subscriptions_first_'+direction,params)
    next_paths=[path+'?'+urllib.parse.urlencode({**params,'after':r[1]['next']}) for r in first]
    second=query('subscriptions_next_'+direction,paths=next_paths)
    back_paths=[path+'?'+urllib.parse.urlencode({**params,'before':r[1]['previous']}) for r in second]
    back=query('subscriptions_previous_'+direction,paths=back_paths)
    assert [[i['identifier'] for i in r[1]['data']] for r in back]==[[i['identifier'] for i in r[1]['data']] for r in first]
    inclusive_paths=[path+'?'+urllib.parse.urlencode({**params,'after':r[1]['next'],'includeCursor':'true'}) for r in first]
    incl=query('subscriptions_inclusive_'+direction,paths=inclusive_paths)
    assert all(i[1]['data'][0]['identifier']==f[1]['data'][-1]['identifier'] for i,f in zip(incl,first))
# The API currently cannot create context-scoped subscriptions. Populate a
# context only in the disposable test databases to compare list semantics.
identifier='list-ident-0-'+suffix
js='const d=db.getSiblingDB("novu-db"); d.topicsubscribers.updateOne({_environmentId:ObjectId("64a000000000000000000002"),identifier:'+json.dumps(identifier)+'},{$set:{contextKeys:["tenant:one","project:two"]}});'
subprocess.run(['docker','exec',os.environ['NOVU_REFERENCE_MONGO_CONTAINER'],'mongosh','--quiet','--eval',js],check=True,stdout=subprocess.DEVNULL)
sql="UPDATE novu_rust_topic_subscribers SET context_keys=ARRAY['tenant:one','project:two'] WHERE environment_id='64a000000000000000000002' AND identifier='%s'" % identifier
subprocess.run(['docker','exec',os.environ['NOVU_RUST_PG_CONTAINER'],'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',sql],check=True,stdout=subprocess.DEVNULL)
query('subscriptions_context_all')
query('subscriptions_context_empty',{'contextKeys':''})
query('subscriptions_context_exact',{'contextKeys':['project:two','tenant:one']})
query('subscriptions_context_subset',{'contextKeys':'tenant:one'})
query('subscriptions_context_duplicates',{'contextKeys':['tenant:one','tenant:one']})
