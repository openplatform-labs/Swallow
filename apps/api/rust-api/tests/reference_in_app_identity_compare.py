#!/usr/bin/env python3
"""Compare recreated-subscriber in-app ownership on disposable databases."""
import json
import base64
import hashlib
import hmac
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
import uuid

bases=[os.environ['NOVU_REFERENCE_URL'],os.environ['NOVU_RUST_URL']]
mongo=os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
pg=os.environ['NOVU_RUST_PG_CONTAINER']
environment='64a000000000000000000002'
subscriber='identity-'+uuid.uuid4().hex
txn='identity-'+uuid.uuid4().hex
def run(*args):return subprocess.check_output(args,text=True).strip()
def req(base,method,path,body=None,auth=None):
    request=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers={'Authorization':auth or 'ApiKey '+os.environ['NOVU_TEST_API_KEY'],'Content-Type':'application/json'},method=method)
    try: response=urllib.request.urlopen(request,timeout=10)
    except urllib.error.HTTPError as error: response=error
    with response:
        raw=response.read()
        return response.status,json.loads(raw) if raw else None

def jwt(internal,extra=None):
    encode=lambda value:base64.urlsafe_b64encode(json.dumps(value,separators=(',',':')).encode()).rstrip(b'=')
    payload={'_id':internal,'environmentId':environment,'organizationId':'64a000000000000000000003','subscriberId':subscriber,'contextKeys':[],'aud':'widget_user','iss':'novu_api','exp':4102444800}
    payload.update(extra or {})
    signed=encode({'alg':'HS256','typ':'JWT'})+b'.'+encode(payload)
    return 'Bearer '+(signed+b'.'+base64.urlsafe_b64encode(hmac.new(b'local-compat-test-secret',signed,hashlib.sha256).digest()).rstrip(b'=')).decode()

ids=[]
tokens=[]
for base in bases:
    status,old=req(base,'POST','/v2/subscribers',{'subscriberId':subscriber});assert status==201,(base,old)
    status,_=req(base,'DELETE','/v2/subscribers/'+subscriber);assert status==200
    token=jwt(old['data']['_id']);tokens.append(token)
    status,_=req(base,'GET','/v1/inbox/notifications/count?filters=%5B%7B%7D%5D',auth=token)
    assert status==401,(base,status)
    status,new=req(base,'POST','/v2/subscribers',{'subscriberId':subscriber});assert status==201 and new['data']['_id']!=old['data']['_id']
    ids.append((old['data']['_id'],new['data']['_id']))

# Reproduce messages from an older subscriber identity, e.g. after a late
# concurrent write. MongoDB does not enforce a subscriber foreign key.
old_ref,new_ref=ids[0];old_rust,new_rust=ids[1]
run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db"),e=ObjectId("{environment}");for(const id of ["{old_ref}","{new_ref}"])d.messages.insertOne({{_environmentId:e,_subscriberId:ObjectId(id),channel:"in_app",read:false,seen:false,archived:false,transactionId:"{txn}"}});''')
run('docker','exec',pg,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',f'''INSERT INTO novu_rust_in_app_messages(id,environment_id,subscriber_id,subscriber_internal_id,workflow_identifier,transaction_id,content,payload) VALUES ('{uuid.uuid4()}','{environment}','{subscriber}','{old_rust}','identity','{txn}','old','{{}}'),('{uuid.uuid4()}','{environment}','{subscriber}','{new_rust}','identity','{txn}','new','{{}}');''')
query=urllib.parse.urlencode({'filters':json.dumps([{}, {'read':False}])})
for base,token in zip(bases,tokens):
    status,body=req(base,'GET','/v2/subscribers/'+subscriber+'/notifications/count?'+query)
    assert status==200 and body['data']==[{'count':1,'filter':{}},{'count':1,'filter':{'read':False}}],(base,status,body)
    # The source API resolves the current subscriber by external ID and does
    # not bind the JWT's _id to the recreated subscriber's internal ID.
    status,body=req(base,'GET','/v1/inbox/notifications/count?'+query,auth=token)
    assert status==200 and body['data']==[{'count':1,'filter':{}},{'count':1,'filter':{'read':False}}],(base,status,body)
    for extra,expected in [({'exp':4102444800.5},200),({'nbf':-0.5},200),({'exp':1.5},401),({'nbf':4102444800.5},401),({'exp':'4102444800'},401),({'nbf':None},401)]:
        status,body=req(base,'GET','/v1/inbox/notifications/count?'+query,auth=jwt('64a000000000000000000099',extra))
        assert status==expected,(base,extra,status,body)
    status,body=req(base,'POST','/v2/subscribers/'+subscriber+'/notifications/read',{})
    assert status==204 and body is None,(base,status,body)
ref=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db");print(JSON.stringify(["{old_ref}","{new_ref}"].map(id=>d.messages.findOne({{transactionId:"{txn}",_subscriberId:ObjectId(id)}}).read)));'''))
rust=json.loads(run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT json_build_array((SELECT is_read FROM novu_rust_in_app_messages WHERE transaction_id='{txn}' AND subscriber_internal_id='{old_rust}'),(SELECT is_read FROM novu_rust_in_app_messages WHERE transaction_id='{txn}' AND subscriber_internal_id='{new_rust}'))::text"))
assert ref==rust==[False,True],(ref,rust)
for base,token in zip(bases,tokens):
    status,_=req(base,'DELETE','/v2/subscribers/'+subscriber);assert status==200
    status,_=req(base,'GET','/v1/inbox/notifications/count?filters=%5B%7B%7D%5D',auth=token)
    assert status==401,(base,status)
ref=json.loads(run('docker','exec',mongo,'mongosh','--quiet','--eval',f'''const d=db.getSiblingDB("novu-db");print(JSON.stringify(["{old_ref}","{new_ref}"].map(id=>d.messages.countDocuments({{transactionId:"{txn}",_subscriberId:ObjectId(id)}}))));'''))
rust=json.loads(run('docker','exec',pg,'psql','-U','postgres','-d','novu_compat','-At','-c',f"SELECT json_build_array((SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='{txn}' AND subscriber_internal_id='{old_rust}'),(SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='{txn}' AND subscriber_internal_id='{new_rust}'))::text"))
assert ref==rust==[1,0],(ref,rust)
print('recreated subscriber in-app count, bulk update, deletion and old JWT behavior matched')
