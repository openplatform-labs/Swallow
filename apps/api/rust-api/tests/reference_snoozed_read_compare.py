#!/usr/bin/env python3
"""Compare persisted snoozed notification reads on disposable worker-backed servers."""
import base64, datetime, hashlib, hmac, json, os, subprocess, time, urllib.error, urllib.parse, urllib.request, uuid

reference = os.environ['NOVU_REFERENCE_URL'].rstrip('/')
rust = os.environ['NOVU_RUST_URL'].rstrip('/')
key = os.environ['NOVU_TEST_API_KEY']
mongo = os.environ['NOVU_REFERENCE_MONGO_CONTAINER']
postgres = os.environ['NOVU_RUST_PG_CONTAINER']
subscriber = 'snoozed-' + uuid.uuid4().hex
prefix = 'snoozed-' + uuid.uuid4().hex

def request(base, method, path, body=None, auth=None):
    req = urllib.request.Request(base + path, data=json.dumps(body).encode() if body is not None else None, headers={'Content-Type':'application/json', 'Authorization':auth or 'ApiKey ' + key}, method=method)
    try:
        result = urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as error:
        result = error
    with result:
        return result.status, json.loads(result.read())

def mongo_eval(js):
    return subprocess.check_output(['docker','exec',mongo,'mongosh','--quiet','--eval',js], text=True).strip()

def pg_eval(sql):
    return subprocess.check_output(['docker','exec',postgres,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-At','-c',sql], text=True).strip()

for i in range(3):
    tx = f'{prefix}-{i}'
    body = {'name':'greeting','to':{'subscriberId':subscriber,'firstName':'Jane'},'transactionId':tx}
    for base in (reference, rust):
        status, result = request(base, 'POST', '/v1/events/trigger', body)
        assert status == 201, (status, result)
    for _ in range(30):
        left = int(mongo_eval(f'print(db.getSiblingDB("novu-db").messages.countDocuments({{transactionId:"{tx}"}}));'))
        right = int(pg_eval(f"SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='{tx}'"))
        if (left,right) == (1,1): break
        time.sleep(1)
    else: raise AssertionError((tx,left,right))

# A past timestamp remains snoozed until the scheduled unsnooze job clears it.
for i, stamp in ((0, '2020-01-02T03:04:05.123Z'), (1, '2099-01-02T03:04:05.123Z')):
    tx = f'{prefix}-{i}'
    mongo_eval(f'db.getSiblingDB("novu-db").messages.updateOne({{transactionId:"{tx}"}},{{$set:{{snoozedUntil:ISODate("{stamp}")}}}});')
    pg_eval(f"UPDATE novu_rust_in_app_messages SET snoozed_until='{stamp}' WHERE transaction_id='{tx}'")

path = '/v2/subscribers/' + subscriber + '/notifications'
for value in ({}, {'snoozeUntil':None}, {'snoozeUntil':'bad'}, {'snoozeUntil':'2020-01-01T00:00:00Z'}, {'snoozeUntil':'2099-01-01T00:00:00Z'}, {'snoozeUntil':1700000000000}):
    target = path + '/64a000000000000000000099/snooze'
    left,right = (request(base,'PATCH',target,value) for base in (reference,rust))
    for _, body in (left,right): body.pop('timestamp',None)
    assert left == right, (value,left,right)
print('snooze_validation_and_plan_limit: match')
valid_until = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=5)).isoformat(timespec='milliseconds').replace('+00:00','Z')
for target in (path + '/64a000000000000000000099/snooze', path + '/invalid/snooze', '/v2/subscribers/missing/notifications/64a000000000000000000099/snooze'):
    left,right = (request(base,'PATCH',target,{'snoozeUntil':valid_until}) for base in (reference,rust))
    for _, body in (left,right): body.pop('timestamp',None)
    assert left == right, (target,left,right)
print('snooze_missing_and_invalid_identifiers: match')
for suffix, count in (('',3), ('?snoozed=true',2), ('?snoozed=false',1)):
    pairs = [request(base,'GET',path + suffix) for base in (reference,rust)]
    for status, body in pairs:
        assert status == 200 and len(body['data']) == count, (suffix,status,body)
    def selected(result):
        return sorted((item['transactionId'],item['isSnoozed'],item.get('snoozedUntil')) for item in result[1]['data'])
    assert selected(pairs[0]) == selected(pairs[1]), (suffix,pairs)
    print('snoozed_list_' + (suffix or 'all') + ': match')
filters = [{},{'snoozed':True},{'snoozed':False},{'snoozed':None}]
query = urllib.parse.urlencode({'filters':json.dumps(filters,separators=(',',':'))})
left,right = (request(base,'GET',path + '/count?' + query) for base in (reference,rust))
assert left == right and [item['count'] for item in left[1]['data']] == [3,2,1,3], (left,right)
print('snoozed_counts: match')
b64 = lambda value: base64.urlsafe_b64encode(value).rstrip(b'=')
header = b64(json.dumps({'alg':'HS256','typ':'JWT'},separators=(',',':')).encode())
claims = {'_id':'64a000000000000000000001','environmentId':'64a000000000000000000002','organizationId':'64a000000000000000000003','subscriberId':subscriber,'contextKeys':[],'aud':'widget_user','iss':'novu_api','exp':4102444800}
payload = b64(json.dumps(claims,separators=(',',':')).encode())
signed = header + b'.' + payload
auth = 'Bearer ' + (signed + b'.' + b64(hmac.new(b'local-compat-test-secret',signed,hashlib.sha256).digest())).decode()
for suffix in ('?snoozed=true','?snoozed=false'):
    left,right = (request(base,'GET','/v1/inbox/notifications' + suffix,auth=auth) for base in (reference,rust))
    assert left[0] == right[0] == 200 and selected(left) == selected(right), (suffix,left,right)
left,right = (request(base,'GET','/v1/inbox/notifications/count?' + query,auth=auth) for base in (reference,rust))
assert left == right and [item['count'] for item in left[1]['data']] == [3,2,1,3], (left,right)
print('snoozed_inbox_jwt: match')
# State mutations must preserve the persisted snooze timestamp in their DTO.
for base in (reference,rust):
    status, body = request(base,'GET',path + '?snoozed=true')
    item = next(item for item in body['data'] if item['transactionId'] == prefix + '-1')
    status, result = request(base,'PATCH',path + '/' + item['id'] + '/read')
    assert status == 200 and result['data']['isSnoozed'] and result['data']['snoozedUntil'] == '2099-01-02T03:04:05.123Z', (status,result)
print('snoozed_mutation_dto: match')
for target, credential in ((path + '/64a000000000000000000099/unsnooze',None),('/v1/inbox/notifications/64a000000000000000000099/unsnooze',auth)):
    left,right = (request(base,'PATCH',target,auth=credential) for base in (reference,rust))
    for _, body in (left,right):
        body.pop('timestamp',None)
    assert left == right and left[0] == 404, (left,right)
print('unsnooze_missing_notification: match')
target = '/v2/subscribers/missing/notifications/64a000000000000000000099/unsnooze'
left,right = (request(base,'PATCH',target) for base in (reference,rust))
for _, body in (left,right): body.pop('timestamp',None)
assert left == right and left[0] == 404, (left,right)
print('unsnooze_missing_subscriber: match')
# There is intentionally no scheduled job for these seeded timestamps. The
# reference still clears the state; cancellation of scheduled jobs is tested
# separately when NOVU_TEST_UNSNOOZE_DELIVERY is enabled.
for i, inbox in ((0,False),(1,True)):
    results = []
    for base in (reference,rust):
        _, body = request(base,'GET',path + '?snoozed=true')
        item = next(item for item in body['data'] if item['transactionId'] == f'{prefix}-{i}')
        endpoint = ('/v1/inbox/notifications/' if inbox else path + '/') + item['id'] + '/unsnooze'
        status, result = request(base,'PATCH',endpoint,auth=auth if inbox else None)
        assert status == 200, (base,status,result)
        result = result['data']
        results.append({field: result.get(field) for field in ('isRead','isSeen','isArchived','isSnoozed','snoozedUntil','archivedAt')})
        status, failure = request(base,'PATCH',endpoint,auth=auth if inbox else None)
        assert status == 404, (status,failure)
        assert failure['message'] == f"Could not find a snoozed notification with id '{item['id']}'. The notification may not exist or may not be in a snoozed state.", failure
    assert results[0] == results[1] and results[0]['isSeen'] and not results[0]['isArchived'] and not results[0]['isSnoozed'], results
    print(('inbox' if inbox else 'subscriber') + '_manual_unsnooze: match')

# Scheduled redelivery appends a second timestamp in the original data model.
stamps = ['2020-01-02T03:04:05.123Z', '2021-02-03T04:05:06.456Z']
for i, values in ((0,stamps),(1,[])):
    tx = f'{prefix}-{i}'
    dates = ','.join('ISODate(' + json.dumps(value) + ')' for value in values)
    mongo_eval(f'db.getSiblingDB("novu-db").messages.updateOne({{transactionId:"{tx}"}},{{$set:{{deliveredAt:[{dates}]}}}});')
    dates = ','.join("'" + value + "'::timestamptz" for value in values)
    pg_eval(f"UPDATE novu_rust_in_app_messages SET delivered_history=ARRAY[{dates}]::timestamptz[] WHERE transaction_id='{tx}'")
for base in (reference,rust):
    status, body = request(base,'GET',path)
    assert status == 200, body
    for i, values in ((0,stamps),(1,[])):
        item = next(item for item in body['data'] if item['transactionId'] == f'{prefix}-{i}')
        assert item['deliveredAt'] == values, (base,item)
        status, changed = request(base,'PATCH',path + '/' + item['id'] + '/read')
        assert status == 200 and changed['data']['deliveredAt'] == values, (base,status,changed)
print('delivery_history_list_and_mutation: match')

if os.environ.get('NOVU_TEST_UNSNOOZE_DELIVERY') == '1':
    # Both APIs must create their own delayed job and persisted snooze state.
    cancel_tx = prefix + '-1'
    _, body = request(reference,'GET',path)
    cancel_ref = next(item['id'] for item in body['data'] if item['transactionId'] == cancel_tx)
    _, body = request(rust,'GET',path)
    cancel_rust = next(item['id'] for item in body['data'] if item['transactionId'] == cancel_tx)
    cancel_time = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10)).isoformat(timespec='milliseconds').replace('+00:00','Z')
    for base, notification_id in ((reference,cancel_ref),(rust,cancel_rust)):
        for _ in range(2):
            status, result = request(base,'PATCH',path + '/' + notification_id + '/snooze',{'snoozeUntil':cancel_time})
            assert status == 200 and result['data']['isSnoozed'] and result['data']['snoozedUntil'] == cancel_time, (base,status,result)
            assert result['data']['isRead'] and result['data']['isSeen'] and not result['data']['isArchived'] and result['data']['archivedAt'] is None, result
    sql_count = f"SELECT count(*) FROM novu_queue_jobs WHERE kind='experimental-in-app-unsnooze' AND status='pending' AND payload->>'notificationId'='{cancel_rust}'"
    assert int(pg_eval(sql_count)) == 2
    mongo_count = f'''const d=db.getSiblingDB("novu-db"),m=d.messages.findOne({{_id:ObjectId("{cancel_ref}")}}); print(d.jobs.countDocuments({{_notificationId:m._notificationId,"payload.unsnooze":true}}));'''
    assert int(mongo_eval(mongo_count)) == 2
    for base, notification_id in ((reference,cancel_ref),(rust,cancel_rust)):
        status, result = request(base,'PATCH',path + '/' + notification_id + '/unsnooze')
        assert status == 200 and not result['data']['isSnoozed'], (status,result)
    assert int(mongo_eval(mongo_count)) == 1
    assert int(pg_eval(sql_count)) == 1
    print('repeated_snooze_and_single_manual_cancellation: match')
    tx = prefix + '-0'
    _, body = request(reference,'GET',path)
    ref_id = next(item['id'] for item in body['data'] if item['transactionId'] == tx)
    _, body = request(rust,'GET',path)
    rust_id = next(item['id'] for item in body['data'] if item['transactionId'] == tx)
    stamp = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=66)).isoformat(timespec='milliseconds').replace('+00:00','Z')
    for base, notification_id, credential in ((reference,ref_id,auth),(rust,rust_id,auth)):
        endpoint = (path + '/' if credential is None else '/v1/inbox/notifications/') + notification_id + '/snooze'
        status, result = request(base,'PATCH',endpoint,{'snoozeUntil':stamp},auth=credential)
        assert status == 200 and result['data']['isSnoozed'] and result['data']['snoozedUntil'] == stamp, (base,status,result)
    # Schedule a second message in a nonempty context in the same delay window.
    ctx_tx=prefix+'-2'
    ctx_ref=mongo_eval(f'print(db.getSiblingDB("novu-db").messages.findOne({{transactionId:"{ctx_tx}"}})._id.toString());')
    ctx_rust=pg_eval(f"SELECT internal_id FROM novu_rust_in_app_messages WHERE transaction_id='{ctx_tx}'")
    mongo_eval(f'db.getSiblingDB("novu-db").messages.updateOne({{transactionId:"{ctx_tx}"}},{{$set:{{contextKeys:["tenant:a","team:b"]}}}});')
    pg_eval(f"UPDATE novu_rust_in_app_messages SET context_keys=ARRAY['tenant:a','team:b'] WHERE transaction_id='{ctx_tx}'")
    ctx_query='?'+urllib.parse.urlencode([('contextKeys','team:b'),('contextKeys','tenant:a')])
    for base, notification_id in ((reference,ctx_ref),(rust,ctx_rust)):
        status,result=request(base,'PATCH',path+'/'+notification_id+'/snooze'+ctx_query,{'snoozeUntil':stamp})
        assert status==200 and result['data']['isSnoozed'],(base,status,result)
    deadline = time.monotonic() + 105
    while time.monotonic() < deadline:
        items = []
        contextual_items = []
        for base in (reference,rust):
            status, body = request(base,'GET',path)
            assert status == 200, (base,status,body)
            items.append(next(item for item in body['data'] if item['transactionId'] == tx))
            contextual_items.append(next(item for item in body['data'] if item['transactionId'] == ctx_tx))
        if all(not item['isSnoozed'] for item in items+contextual_items): break
        time.sleep(1)
    else: raise AssertionError(('automatic unsnooze timed out',items))
    for item in items:
        assert not item['isRead'] and item['isSeen'] and not item['isArchived'], item
        assert item['readAt'] is None and item['deliveredAt'][:2] == stamps and len(item['deliveredAt']) == 3, item
        assert item['createdAt'] == item['deliveredAt'][-1], item
    for item in contextual_items:
        assert not item['isRead'] and item['isSeen'] and not item['isArchived'] and not item['isSnoozed'],item
        assert item['readAt'] is None and len(item['deliveredAt']) == 2,item
        assert item['createdAt'] == item['deliveredAt'][-1],item
    print('scheduled_unsnooze_worker_and_history: match')
    print('scheduled_unsnooze_nonempty_context: match')

# Context-scoped management snooze and unsnooze must not modify a different
# context, and exact matching is order independent.
tx=prefix+'-2'
ref_id=mongo_eval(f'print(db.getSiblingDB("novu-db").messages.findOne({{transactionId:"{tx}"}})._id.toString());')
rust_id=pg_eval(f"SELECT internal_id FROM novu_rust_in_app_messages WHERE transaction_id='{tx}'")
mongo_eval(f'db.getSiblingDB("novu-db").messages.updateOne({{transactionId:"{tx}"}},{{$set:{{contextKeys:["tenant:a","team:b"]}}}});')
pg_eval(f"UPDATE novu_rust_in_app_messages SET context_keys=ARRAY['tenant:a','team:b'] WHERE transaction_id='{tx}'")
future=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=5)).isoformat(timespec='milliseconds').replace('+00:00','Z')
for base,notification_id in ((reference,ref_id),(rust,rust_id)):
    route=path+'/'+notification_id
    wrong='?'+urllib.parse.urlencode([('contextKeys','tenant:a')])
    exact='?'+urllib.parse.urlencode([('contextKeys','team:b'),('contextKeys','tenant:a')])
    status,result=request(base,'PATCH',route+'/snooze'+wrong,{'snoozeUntil':future})
    assert status==404,(base,status,result)
    status,result=request(base,'PATCH',route+'/snooze'+exact,{'snoozeUntil':future})
    assert status==200 and result['data']['isSnoozed'] and result['data']['snoozedUntil']==future,(base,status,result)
    status,result=request(base,'PATCH',route+'/unsnooze'+wrong)
    assert status==404,(base,status,result)
    status,result=request(base,'PATCH',route+'/unsnooze'+exact)
    assert status==200 and not result['data']['isSnoozed'],(base,status,result)
print('management snooze and unsnooze exact context: match')
