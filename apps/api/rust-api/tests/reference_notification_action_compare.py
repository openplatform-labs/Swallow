#!/usr/bin/env python3
"""Compare bounded Inbox action mutations on disposable original/Rust databases."""
import base64
import hashlib
import hmac
import json
import os
import subprocess
import urllib.error
import urllib.request

mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER']; pg=os.environ['NOVU_RUST_PG_CONTAINER']
env='64a000000000000000000002'; org='64a000000000000000000003'; sub='64a000000000000000000010'; wid='64a000000000000000000004'; mid='64a000000000000000000021'
cta={'type':'redirect','data':{'url':'/home','target':'_blank'},'action':{'buttons':[{'type':'primary','content':'Approve','url':'https://example.test/approve','target':'_self'},{'type':'secondary','content':'Decline','url':'javascript:alert(1)'}]}}
js='''const d=db.getSiblingDB("novu-db"),e=ObjectId("%s"),s=ObjectId("%s"),w=ObjectId("%s");d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:ObjectId("%s"),name:"Action",critical:false,type:"REGULAR",tags:[],severity:"none",triggers:[{type:"event",identifier:"fixture"}]});d.messages.insertOne({_id:ObjectId("%s"),_environmentId:e,_subscriberId:s,_templateId:w,channel:"in_app",content:"hello",read:false,seen:false,archived:false,tags:[],cta:%s,transactionId:"action-test",createdAt:new Date("2024-01-01T00:00:00Z")});'''%(env,sub,wid,org,mid,json.dumps(cta,separators=(',',':')))
subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval',js],check=True,stdout=subprocess.DEVNULL)
metadata={'id':wid,'identifier':'fixture','name':'Action','critical':False,'tags':[],'severity':'none','payloadAsData':True}
sql="INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,inbox_metadata) VALUES ('%s','fixture',true,true,'%s'::jsonb); INSERT INTO novu_rust_in_app_messages(id,internal_id,environment_id,subscriber_id,subscriber_internal_id,workflow_identifier,transaction_id,content,payload,tags,cta,created_at) VALUES(gen_random_uuid(),'%s','%s','alice','%s','fixture','action-test','hello','{}','[]','%s'::jsonb,'2024-01-01T00:00:00Z')"%(env,json.dumps(metadata,separators=(',',':')),mid,env,sub,json.dumps(cta,separators=(',',':')))
subprocess.run(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',sql],check=True,stdout=subprocess.DEVNULL)
subprocess.run(['docker','exec',os.environ['NOVU_REFERENCE_REDIS_CONTAINER'],'redis-cli','FLUSHALL'],check=True,stdout=subprocess.DEVNULL)

def token():
    b64=lambda b:base64.urlsafe_b64encode(b).rstrip(b'=')
    header=b64(json.dumps({'alg':'HS256','typ':'JWT'},separators=(',',':')).encode())
    payload=b64(json.dumps({'_id':sub,'environmentId':env,'organizationId':org,'subscriberId':'alice','contextKeys':[],'aud':'widget_user','iss':'novu_api','exp':4102444800},separators=(',',':')).encode())
    data=header+b'.'+payload
    return 'Bearer '+(data+b'.'+b64(hmac.new(b'local-compat-test-secret',data,hashlib.sha256).digest())).decode()

def request(base,method,path,body=None,bearer=False):
    req=urllib.request.Request(base+path,data=None if body is None else json.dumps(body).encode(),method=method,headers={'Authorization':token() if bearer else 'ApiKey '+os.environ['NOVU_TEST_API_KEY'],'Content-Type':'application/json'})
    try: r=urllib.request.urlopen(req,timeout=10)
    except urllib.error.HTTPError as e: r=e
    with r: raw=r.read(); result=json.loads(raw) if raw else None; return r.status,result

def normalized(value):
    status,body=value
    body=json.loads(json.dumps(body)) if body is not None else None
    if isinstance(body,dict):
        body.pop('timestamp',None)
        items=body.get('data')
        if isinstance(items,dict):items=[items]
        if isinstance(items,list):
            for item in items:
                if not isinstance(item,dict):continue
                for field in ['readAt','firstSeenAt','createdAt']:
                    if isinstance(item.get(field),str):item[field]='<timestamp>'
    return status,body

def compare(label,method,path,body=None,bearer=False):
    left=request(os.environ['NOVU_REFERENCE_URL'],method,path,body,bearer);right=request(os.environ['NOVU_RUST_URL'],method,path,body,bearer)
    if normalized(left)!=normalized(right):raise SystemExit(f'{label} mismatch\nreference={left!r}\nrust={right!r}')
    print(label+': match')
    return right

base='/v2/subscribers/alice/notifications/'+mid
compare('initial-list','GET','/v2/subscribers/alice/notifications')
compare('complete-primary','PATCH',base+'/actions/primary/complete')
compare('repeat-primary','PATCH',base+'/actions/primary/complete')
compare('complete-secondary','PATCH',base+'/actions/secondary/complete')
compare('revert-secondary','PATCH',base+'/actions/secondary/revert')
compare('inbox-complete-primary','PATCH','/v1/inbox/notifications/'+mid+'/complete',{'actionType':'primary'},True)
compare('inbox-revert-primary','PATCH','/v1/inbox/notifications/'+mid+'/revert',{'actionType':'primary'},True)
compare('final-list','GET','/v2/subscribers/alice/notifications')
compare('missing-notification','PATCH','/v2/subscribers/alice/notifications/64a000000000000000000099/actions/primary/complete')
compare('missing-subscriber','PATCH','/v2/subscribers/missing/notifications/'+mid+'/actions/primary/complete')
# Missing buttons and validation errors are compared before any later mutation.
js='db.getSiblingDB("novu-db").messages.updateOne({_id:ObjectId("%s")},{$set:{"cta.action.buttons":[{"type":"primary","content":"Approve"}]}});'%mid
subprocess.run(['docker','exec',mongo,'mongosh','--quiet','--eval',js],check=True,stdout=subprocess.DEVNULL)
subprocess.run(['docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',"UPDATE novu_rust_in_app_messages SET cta=jsonb_set(cta,'{action,buttons}','[{\"type\":\"primary\",\"content\":\"Approve\"}]'::jsonb) WHERE internal_id='%s'"%mid],check=True,stdout=subprocess.DEVNULL)
compare('missing-secondary','PATCH',base+'/actions/secondary/complete')
compare('invalid-action-type','PATCH',base+'/actions/tertiary/complete')
compare('invalid-notification-id','PATCH','/v2/subscribers/alice/notifications/not-an-id/actions/primary/complete')
compare('inbox-invalid-action','PATCH','/v1/inbox/notifications/'+mid+'/complete',{'actionType':'tertiary'},True)
compare('inbox-missing-action','PATCH','/v1/inbox/notifications/'+mid+'/complete',{},True)
compare('context-not-found','PATCH',base+'/actions/primary/complete?contextKeys=tenant%3Amissing')
