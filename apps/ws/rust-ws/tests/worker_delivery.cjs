// Rust API -> PostgreSQL queue -> Rust worker -> Socket.IO, compared with an
// original worker-produced MongoDB message sent through the original gateway.
const path = require('path');
const { spawnSync } = require('child_process');
const assert = require('assert/strict');
const root = process.env.NOVU_REFERENCE_SOURCE;
const { io } = require(path.join(root,'node_modules/.pnpm/socket.io-client@4.8.3/node_modules/socket.io-client'));
const jwt = require(path.join(root,'node_modules/.pnpm/jsonwebtoken@9.0.3/node_modules/jsonwebtoken'));
const { Novu } = require(path.join(root,'packages/js/dist/cjs/index.js'));
const secret = 'local-compat-test-secret';
const claims = {_id:'64a000000000000000000010',environmentId:'64a000000000000000000002',organizationId:'64a000000000000000000003',subscriberId:'alice',aud:'widget_user'};
const token = jwt.sign(claims,secret,{expiresIn:'1h'});
const other = jwt.sign({...claims,contextKeys:['tenant:other']},secret,{expiresIn:'1h'});
const wait = ms => new Promise(resolve => setTimeout(resolve,ms));
function connect(url,credential) {
  const socket = io(url,{transports:['websocket'],query:{token:credential},reconnection:false,timeout:5000});
  socket.events=[]; socket.onAny((event,data)=>socket.events.push({event,data}));
  return socket;
}
async function ready(socket) { await new Promise((resolve,reject)=>{socket.once('connect',resolve);socket.once('connect_error',reject);setTimeout(()=>reject(Error('socket connect timeout')),5000).unref()}); }
async function trigger(base,transactionId,payload={},options={}) {
  const response = await fetch(base + '/v1/events/trigger',{method:'POST',headers:{'Content-Type':'application/json','Authorization':'ApiKey '+process.env.NOVU_TEST_API_KEY},body:JSON.stringify({name:'greeting',to:{subscriberId:'alice'},transactionId,payload,...options})});
  assert.equal(response.status,201,await response.text());
}
function referenceMessage(transactionId) {
  const js = `const m=db.getSiblingDB("novu-db").messages.findOne({transactionId:"${transactionId}"}); if(m)print(JSON.stringify(m));`;
  const result=spawnSync('docker',['exec',process.env.NOVU_REFERENCE_MONGO_CONTAINER,'mongosh','--quiet','--eval',js],{encoding:'utf8'});
  assert.equal(result.status,0,result.stderr);
  return result.stdout.trim() ? JSON.parse(result.stdout) : null;
}
function referenceId(transactionId) {
  const js = `print(db.getSiblingDB("novu-db").messages.findOne({transactionId:"${transactionId}"})._id.toHexString());`;
  const result=spawnSync('docker',['exec',process.env.NOVU_REFERENCE_MONGO_CONTAINER,'mongosh','--quiet','--eval',js],{encoding:'utf8'});
  assert.equal(result.status,0,result.stderr); return result.stdout.trim();
}
function referenceEvents(event) {
  const lua = "local out={} for _,k in ipairs(redis.call('KEYS','*ws_socket_queue:*')) do if redis.call('TYPE',k).ok=='hash' then local d=redis.call('HGET',k,'data'); if d then table.insert(out,d) end end end return out";
  const result=spawnSync('docker',['exec',process.env.NOVU_REFERENCE_REDIS_CONTAINER,'redis-cli','--raw','EVAL',lua,'0'],{encoding:'utf8'});
  assert.equal(result.status,0,result.stderr);
  return result.stdout.split('\n').filter(Boolean).map(line=>JSON.parse(line)).filter(item=>item.userId===claims._id && item.event===event && item._environmentId===claims.environmentId);
}
async function mutation(base,method,endpoint,body) {
  const response=await fetch(base+endpoint,{method,headers:{'Content-Type':'application/json','Authorization':'ApiKey '+process.env.NOVU_TEST_API_KEY},...(body===undefined?{}:{body:JSON.stringify(body)})});
  return response.status;
}
(async()=>{
  const ref=connect(process.env.REF_WS_URL,token), rust=connect(process.env.RUST_WS_URL,token), filtered=connect(process.env.RUST_WS_URL,other);
  const sdk = new Novu({applicationIdentifier:'compat-ws-app',subscriberId:'alice',apiUrl:process.env.NOVU_RUST_URL,socketUrl:process.env.RUST_WS_URL,useCache:false});
  const referenceSdk = new Novu({applicationIdentifier:'test',subscriberId:'alice',apiUrl:process.env.NOVU_REFERENCE_URL,socketUrl:process.env.REF_WS_URL,useCache:false});
  const sdkEvents = [], sdkUnread = [], sdkUnseen = [];
  const referenceSdkEvents = [];
  let sdkReady;
  const connectedSdk = new Promise((resolve,reject)=>{ sdkReady={resolve,reject}; setTimeout(()=>reject(Error('SDK socket connect timeout')),10000).unref(); });
  sdk.on('socket.connect.resolved', ({error}) => error ? sdkReady.reject(error) : sdkReady.resolve());
  sdk.on('notifications.notification_received', ({result}) => sdkEvents.push(result));
  const referenceSdkConnected = new Promise((resolve,reject)=>{
    referenceSdk.on('socket.connect.resolved', ({error}) => error ? reject(error) : resolve());
    setTimeout(()=>reject(Error('reference SDK socket connect timeout')),10000).unref();
  });
  referenceSdk.on('notifications.notification_received', ({result}) => referenceSdkEvents.push(result));
  await Promise.all([ready(ref),ready(rust),ready(filtered),connectedSdk,referenceSdkConnected]);
  sdk.on('notifications.unread_count_changed', ({result}) => sdkUnread.push(result));
  sdk.on('notifications.unseen_count_changed', ({result}) => sdkUnseen.push(result));
  await wait(500);
  const transactionId='ws-compat-' + require('crypto').randomUUID();
  await Promise.all([trigger(process.env.NOVU_REFERENCE_URL,transactionId),trigger(process.env.NOVU_RUST_URL,transactionId)]);
  let refMessage;
  for(let i=0;i<50;i++) { refMessage=referenceMessage(transactionId); if(refMessage && rust.events.some(e=>e.event==='notification_received')) break; await wait(250); }
  assert(refMessage,'original worker message did not appear');
  const response = await fetch(process.env.REF_WS_URL+'/emit',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({userId:claims._id,event:'notification_received',contextKeys:[],data:{message:refMessage}})});
  assert.equal(response.status,204);
  for(let i=0;i<30 && !ref.events.some(e=>e.event==='notification_received');i++) await wait(100);
  const left=ref.events.find(e=>e.event==='notification_received')?.data?.message;
  const right=rust.events.find(e=>e.event==='notification_received')?.data?.message;
  assert(left && right,JSON.stringify({ref:ref.events,rust:rust.events}));
  for(const field of ['transactionId','content','channel','read','seen','archived','tags','_environmentId','_organizationId','_subscriberId','_templateId','cta','providerId','deviceTokens','contextKeys','__v','payload']) assert.deepEqual(right[field],left[field],field);
  assert.equal(right._subscriberId,claims._id);
  assert.match(right._id,/^[0-9a-f]{24}$/);
  assert.match(left._notificationId,/^[0-9a-f]{24}$/);
  assert.match(right._notificationId,/^[0-9a-f]{24}$/);
  const linked = spawnSync('docker',['exec',process.env.NOVU_RUST_PG_CONTAINER,'psql','-At','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',`SELECT count(*) FROM novu_rust_notifications WHERE environment_id='${claims.environmentId}' AND internal_id='${right._notificationId}' AND transaction_id='${transactionId}'`],{encoding:'utf8'});
  assert.equal(linked.status,0,linked.stderr); assert.equal(linked.stdout.trim(),'1');
  for(let i=0;i<30 && !sdkEvents.length;i++) await wait(100);
  assert.equal(sdkEvents[0]?.id,right._id); assert.equal(sdkEvents[0]?.body,right.content);
  assert.equal(sdkEvents[0]?.transactionId,transactionId); assert.equal(sdkEvents[0]?.channelType,'in_app');
  for(let i=0;i<30 && !referenceSdkEvents.length;i++) await wait(100);
  assert(referenceSdkEvents.length, 'reference SDK did not receive the notification');
  function normalizedSdk(item) {
    const keys=['transactionId','subject','body','to','isRead','isSeen','isArchived','isSnoozed','snoozedUntil','deliveredAt','createdAt','readAt','firstSeenAt','archivedAt','avatar','primaryAction','secondaryAction','channelType','tags','redirect','data','workflow','severity'];
    const obj=Object.fromEntries(keys.map(key=>[key,item[key]]));
    assert.match(item.id,/^[0-9a-f]{24}$/);
    assert.match(item.createdAt,/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
    obj.createdAt = '<timestamp>';
    if (obj.deliveredAt) obj.deliveredAt = obj.deliveredAt.map(value=>{ assert.match(value,/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/); return '<timestamp>'; });
    return JSON.parse(JSON.stringify(obj));
  }
  assert.deepEqual(normalizedSdk(sdkEvents[0]),normalizedSdk(referenceSdkEvents[0]),'SDK notification payload');
  for(let i=0;i<30 && rust.events.filter(e=>['unseen_count_changed','unread_count_changed'].includes(e.event)).length<2;i++) await wait(100);
  const unseen=rust.events.find(e=>e.event==='unseen_count_changed');
  const unread=rust.events.find(e=>e.event==='unread_count_changed');
  assert.deepEqual(unseen?.data,{unseenCount:1,hasMore:false});
  assert.deepEqual(unread?.data,{unreadCount:1,hasMore:false,counts:{total:1,severity:{high:0,medium:0,low:0,none:1}}});
  await wait(300); assert.equal(filtered.events.length,0,'different context received event');
  // Exercise automatic unsnooze without a 60-second wait: create a real API
  // job, then advance only the disposable PostgreSQL queue's due timestamp.
  rust.events.length=0; filtered.events.length=0;
  const stamp = new Date(Date.now()+180000).toISOString();
  const snooze = await fetch(process.env.NOVU_RUST_URL + '/v2/subscribers/alice/notifications/' + right._id + '/snooze',{method:'PATCH',headers:{'Content-Type':'application/json','Authorization':'ApiKey '+process.env.NOVU_TEST_API_KEY},body:JSON.stringify({snoozeUntil:stamp})});
  assert.equal(snooze.status,200,await snooze.text());
  const sql = `UPDATE novu_queue_jobs SET available_at=now()-interval '1 second' WHERE environment_id='${claims.environmentId}' AND kind='experimental-in-app-unsnooze' AND status='pending' AND payload->>'notificationId'='${right._id}'`;
  const updated = spawnSync('docker',['exec',process.env.NOVU_RUST_PG_CONTAINER,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',sql],{encoding:'utf8'});
  assert.equal(updated.status,0,updated.stderr); assert.match(updated.stdout,/UPDATE 1/);
  for(let i=0;i<60 && rust.events.filter(e=>e.event==='notification_received').length===0;i++) await wait(100);
  const redelivered=rust.events.find(e=>e.event==='notification_received')?.data?.message;
  assert(redelivered,JSON.stringify(rust.events));
  assert.equal(redelivered._id,right._id); assert.equal(redelivered.read,false); assert.equal(redelivered.deliveredAt.length,2);
  await wait(300); assert.equal(filtered.events.length,0,'different context received unsnooze event');
  const refId = referenceId(transactionId);
  async function compareMutation(method, suffix, body, event, expected) {
    rust.events.length=0; filtered.events.length=0; sdkUnread.length=0; sdkUnseen.length=0;
    const before=referenceEvents(event).length;
    const refPath='/v2/subscribers/alice/notifications'+suffix.replace('{id}',refId);
    const rustPath='/v2/subscribers/alice/notifications'+suffix.replace('{id}',right._id);
    const refBody=body?.notificationIds ? {...body,notificationIds:body.notificationIds.map(value=>value===right._id ? refId : value)} : body;
    const leftStatus=await mutation(process.env.NOVU_REFERENCE_URL,method,refPath,refBody);
    const rightStatus=await mutation(process.env.NOVU_RUST_URL,method,rustPath,body);
    assert.equal(rightStatus,leftStatus,`${method} ${suffix}`); assert(leftStatus===200 || leftStatus===204,`${leftStatus} ${suffix}`);
    for(let i=0;i<40 && referenceEvents(event).length===before;i++) await wait(100);
    const original=referenceEvents(event); assert.equal(original.length,before+1,`${event} original queue ${suffix}`);
    assert.deepEqual(original.at(-1).contextKeys ?? [],[]);
    for(let i=0;i<40 && !rust.events.some(item=>item.event===event);i++) await wait(100);
    assert.deepEqual(rust.events.find(item=>item.event===event)?.data,expected,`${event} Rust ${suffix}`);
    const sdkValues = event === 'unread_count_changed' ? sdkUnread : sdkUnseen;
    for(let i=0;i<40 && !sdkValues.length;i++) await wait(100);
    assert.deepEqual(sdkValues[0],event === 'unread_count_changed' ? expected.counts : expected.unseenCount,`SDK ${event} ${suffix}`);
    await wait(100); assert.equal(filtered.events.length,0,`wrong context: ${suffix}`);
  }
  const unread0={unreadCount:0,hasMore:false,counts:{total:0,severity:{high:0,medium:0,low:0,none:0}}};
  const unread1={unreadCount:1,hasMore:false,counts:{total:1,severity:{high:0,medium:0,low:0,none:1}}};
  const snoozedUnread1={unreadCount:1,hasMore:false,counts:{total:1,severity:{high:0,medium:0,low:0,none:0}}};
  await compareMutation('PATCH','/{id}/read',{},'unread_count_changed',unread0);
  await compareMutation('PATCH','/{id}/unread',{},'unread_count_changed',unread1);
  await compareMutation('PATCH','/{id}/snooze',{snoozeUntil:new Date(Date.now()+180000).toISOString()},'unread_count_changed',snoozedUnread1);
  await compareMutation('PATCH','/{id}/unsnooze',{},'unread_count_changed',unread1);
  await compareMutation('POST','/seen',{notificationIds:[right._id]},'unseen_count_changed',{unseenCount:0,hasMore:false});
  await compareMutation('PATCH','/{id}/archive',{},'unread_count_changed',unread0);
  await compareMutation('PATCH','/{id}/unarchive',{},'unread_count_changed',unread0);
  await compareMutation('POST','/read',{},'unread_count_changed',unread0);
  await compareMutation('POST','/archive',{},'unread_count_changed',unread0);
  await compareMutation('POST','/read-archive',{},'unread_count_changed',unread0);
  await compareMutation('DELETE','/{id}',undefined,'unread_count_changed',unread0);
  await compareMutation('POST','/delete',{},'unread_count_changed',unread0);
  rust.events.length=0;
  const beforeEmpty=referenceEvents('unseen_count_changed').length;
  assert.equal(await mutation(process.env.NOVU_REFERENCE_URL,'POST','/v2/subscribers/alice/notifications/seen',{notificationIds:[]}),204);
  assert.equal(await mutation(process.env.NOVU_RUST_URL,'POST','/v2/subscribers/alice/notifications/seen',{notificationIds:[]}),204);
  await wait(400); assert.equal(referenceEvents('unseen_count_changed').length,beforeEmpty); assert.equal(rust.events.length,0);
  rust.events.length=0; filtered.events.length=0; sdkUnread.length=0; sdkUnseen.length=0;
  const previousContext = referenceEvents('unread_count_changed').filter(item=>JSON.stringify(item.contextKeys)===JSON.stringify(['tenant:other'])).length;
  for(const base of [process.env.NOVU_REFERENCE_URL,process.env.NOVU_RUST_URL]) {
    const response=await fetch(base+'/v1/inbox/notifications/read',{method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+other},body:'{}'});
    assert.equal(response.status,204,await response.text());
  }
  for(let i=0;i<40 && referenceEvents('unread_count_changed').filter(item=>JSON.stringify(item.contextKeys)===JSON.stringify(['tenant:other'])).length===previousContext;i++) await wait(100);
  assert.equal(referenceEvents('unread_count_changed').filter(item=>JSON.stringify(item.contextKeys)===JSON.stringify(['tenant:other'])).length,previousContext+1);
  for(let i=0;i<40 && !filtered.events.some(item=>item.event==='unread_count_changed');i++) await wait(100);
  assert.deepEqual(filtered.events.find(item=>item.event==='unread_count_changed')?.data,unread0);
  await wait(200); assert.equal(rust.events.length,0,'default context received a context-specific mutation event'); assert.equal(sdkUnread.length,0);
  console.log('context-specific JWT mutation event: match');
  console.log('reference queue events and Rust mutation count events: match');
  // Compare actual worker payloads, including the Mongoose empty-object rules.
  const payloadCases = [
    {label:'nested', payload:{value:'hello',nested:{empty:{},value:3},items:[{}, {empty:{},ok:true}],nil:null}},
    {label:'empty-nested', payload:{a:{b:{}}}},
    {label:'arrays', payload:{empty:[],items:[{}, {nested:{}}, [ {nested:{},value:1} ]]}},
  ];
  const payloadIds = [];
  for (const test of payloadCases) {
    const id='ws-payload-'+require('crypto').randomUUID();
    payloadIds.push(id);
    await Promise.all([trigger(process.env.NOVU_REFERENCE_URL,id,test.payload),trigger(process.env.NOVU_RUST_URL,id,test.payload)]);
    let original, actual;
    for (let i=0;i<60;i++) {
      original=referenceMessage(id);
      actual=rust.events.find(e=>e.event==='notification_received' && e.data?.message?.transactionId===id)?.data?.message;
      if (original && actual) break;
      await wait(250);
    }
    assert(original && actual,`${test.label}: worker delivery missing`);
    assert.deepEqual(actual.payload,original.payload,`${test.label}: raw notification payload`);
    const stored = spawnSync('docker',['exec',process.env.NOVU_RUST_PG_CONTAINER,'psql','-At','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',`SELECT payload::text FROM novu_rust_in_app_messages WHERE environment_id='${claims.environmentId}' AND transaction_id='${id}'`],{encoding:'utf8'});
    assert.equal(stored.status,0,stored.stderr);
    assert.deepEqual(JSON.parse(stored.stdout),original.payload ?? {},`${test.label}: stored notification payload`);
  }
  console.log('nested payloads: original worker message matches Rust storage and socket');
  // The reference fixture is a REGULAR (v0) workflow whose Inbox DTO falls
  // back to message.payload as data. Test existing pre-normalization rows too.
  const metadata = `UPDATE novu_rust_workflows SET inbox_metadata=jsonb_set(inbox_metadata,'{payloadAsData}','true'::jsonb) WHERE environment_id='${claims.environmentId}' AND identifier='greeting';`;
  const metaResult = spawnSync('docker',['exec',process.env.NOVU_RUST_PG_CONTAINER,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',metadata],{encoding:'utf8'});
  assert.equal(metaResult.status,0,metaResult.stderr);
  const legacyPayload = JSON.stringify({...payloadCases[0].payload,attachments:[{file:'old-attachment'}]});
  const legacySql = `UPDATE novu_rust_in_app_messages SET payload='${legacyPayload}'::jsonb WHERE environment_id='${claims.environmentId}' AND transaction_id='${payloadIds[0]}'`;
  const legacy = spawnSync('docker',['exec',process.env.NOVU_RUST_PG_CONTAINER,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',legacySql],{encoding:'utf8'});
  assert.equal(legacy.status,0,legacy.stderr);
  async function inboxNotifications(base) {
    const result=await fetch(base+'/v2/subscribers/alice/notifications?limit=100',{headers:{'Authorization':'ApiKey '+process.env.NOVU_TEST_API_KEY}});
    assert.equal(result.status,200,await result.clone().text());
    return (await result.json()).data;
  }
  const [referenceItems,rustItems]=await Promise.all([inboxNotifications(process.env.NOVU_REFERENCE_URL),inboxNotifications(process.env.NOVU_RUST_URL)]);
  for (const [i,id] of payloadIds.entries()) {
    const expected=referenceItems.find(item=>item.transactionId===id);
    const actual=rustItems.find(item=>item.transactionId===id);
    assert(expected && actual,`${payloadCases[i].label}: notification list missing`);
    assert.deepEqual(actual.data,expected.data,`${payloadCases[i].label}: notification data`);
  }
  const referenceItem=referenceItems.find(item=>item.transactionId===payloadIds[0]);
  const rustItem=rustItems.find(item=>item.transactionId===payloadIds[0]);
  async function readNotification(base,id) {
    const result=await fetch(base+'/v2/subscribers/alice/notifications/'+id+'/read',{method:'PATCH',headers:{'Authorization':'ApiKey '+process.env.NOVU_TEST_API_KEY,'Content-Type':'application/json'},body:'{}'});
    assert.equal(result.status,200,await result.clone().text());
    return (await result.json()).data;
  }
  const [referenceRead,rustRead]=await Promise.all([readNotification(process.env.NOVU_REFERENCE_URL,referenceItem.id),readNotification(process.env.NOVU_RUST_URL,rustItem.id)]);
  assert.deepEqual(rustRead.data,referenceRead.data,'legacy payload in read mutation response');
  // Explicit message data takes precedence over the fallback payload.
  const overrideSql = `UPDATE novu_rust_in_app_messages SET data='{"source":"explicit"}'::jsonb WHERE environment_id='${claims.environmentId}' AND transaction_id='${payloadIds[0]}'`;
  const overridePg=spawnSync('docker',['exec',process.env.NOVU_RUST_PG_CONTAINER,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',overrideSql],{encoding:'utf8'});
  assert.equal(overridePg.status,0,overridePg.stderr);
  const overrideJs=`db.getSiblingDB("novu-db").messages.updateOne({transactionId:"${payloadIds[0]}"},{$set:{data:{source:"explicit"}}});`;
  const overrideMongo=spawnSync('docker',['exec',process.env.NOVU_REFERENCE_MONGO_CONTAINER,'mongosh','--quiet','--eval',overrideJs],{encoding:'utf8'});
  assert.equal(overrideMongo.status,0,overrideMongo.stderr);
  const [referenceOverride,rustOverride]=await Promise.all([inboxNotifications(process.env.NOVU_REFERENCE_URL),inboxNotifications(process.env.NOVU_RUST_URL)]);
  const originalData=referenceOverride.find(item=>item.transactionId===payloadIds[0])?.data;
  const actualData=rustOverride.find(item=>item.transactionId===payloadIds[0])?.data;
  assert.deepEqual(originalData,{source:'explicit'});
  assert.deepEqual(actualData,originalData,'explicit data must override payload');
  console.log('v0 Inbox payload data, legacy row, read response and explicit data precedence: match');
  // A separately configured stateful workflow exercises actual worker severity
  // selection and a trigger override without changing the earlier fixtures.
  const fixtureJs=`const d=db.getSiblingDB("novu-db"),e=ObjectId("${claims.environmentId}"),o=ObjectId("${claims.organizationId}"),w=ObjectId("64a000000000000000000007"),m=ObjectId("64a000000000000000000005"); d.notificationtemplates.insertOne({_id:w,_environmentId:e,_organizationId:o,name:"Severity flow",active:true,draft:false,critical:false,type:"REGULAR",deleted:false,tags:[],severity:"medium",triggers:[{type:"event",identifier:"severity-flow"}],steps:[{active:true,_templateId:m}],createdAt:new Date(),updatedAt:new Date()}); d.preferences.insertOne({_environmentId:e,_organizationId:o,_templateId:w,type:"WORKFLOW_RESOURCE",preferences:{all:{enabled:true,readOnly:false},channels:{in_app:{enabled:true}}},deleted:false});`;
  const originalFixture=spawnSync('docker',['exec',process.env.NOVU_REFERENCE_MONGO_CONTAINER,'mongosh','--quiet','--eval',fixtureJs],{encoding:'utf8'});
  assert.equal(originalFixture.status,0,originalFixture.stderr);
  const fixtureSql=`INSERT INTO novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,inbox_metadata) VALUES ('${claims.environmentId}','severity-flow',true,true,'hello','{"id":"64a000000000000000000007","identifier":"severity-flow","name":"Severity flow","critical":false,"tags":[],"severity":"medium","payloadAsData":true}'::jsonb); INSERT INTO novu_rust_workflow_preferences(environment_id,workflow_identifier,all_enabled,read_only,in_app_enabled) VALUES ('${claims.environmentId}','severity-flow',true,false,true)`;
  const fixturePg=spawnSync('docker',['exec',process.env.NOVU_RUST_PG_CONTAINER,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',fixtureSql],{encoding:'utf8'});
  assert.equal(fixturePg.status,0,fixturePg.stderr);
  let highIds;
  for(const [label,expected,overrides] of [['default','medium',undefined],['override','high',{severity:'high'}]]) {
    const tx='ws-severity-'+require('crypto').randomUUID();
    rust.events.length=0; sdkUnread.length=0;
    const options={name:'severity-flow',...(overrides?{overrides}:{})};
    await Promise.all([trigger(process.env.NOVU_REFERENCE_URL,tx,{},options),trigger(process.env.NOVU_RUST_URL,tx,{},options)]);
    let original,actual;
    for(let i=0;i<60;i++) {
      original=referenceMessage(tx);
      actual=rust.events.find(e=>e.event==='notification_received' && e.data?.message?.transactionId===tx)?.data?.message;
      if(original && actual) break;
      await wait(250);
    }
    assert(original && actual,`${label}: delivery missing`);
    assert.equal(original.severity,expected); assert.equal(actual.severity,original.severity);
    if(expected==='high') highIds=[original._id,actual._id];
    const sent=await fetch(process.env.REF_WS_URL+'/emit',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({userId:claims._id,event:'notification_received',contextKeys:[],data:{message:original}})});
    assert.equal(sent.status,204);
    for(let i=0;i<30 && (!sdkEvents.some(item=>item.transactionId===tx) || !referenceSdkEvents.some(item=>item.transactionId===tx));i++) await wait(100);
    const rustSdk=sdkEvents.find(item=>item.transactionId===tx), originalSdk=referenceSdkEvents.find(item=>item.transactionId===tx);
    assert(rustSdk && originalSdk,`${label}: SDK notification missing`);
    assert.equal(rustSdk.severity,expected); assert.deepEqual(normalizedSdk(rustSdk),normalizedSdk(originalSdk));
    async function session(base,applicationIdentifier) {
      const response=await fetch(base+'/v1/inbox/session',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({applicationIdentifier,subscriberId:'alice'})});
      assert.equal(response.status,201,await response.clone().text());
      return (await response.json()).data.unreadCount;
    }
    const [originalCounts,rustCounts]=await Promise.all([session(process.env.NOVU_REFERENCE_URL,'test'),session(process.env.NOVU_RUST_URL,'compat-ws-app')]);
    assert.deepEqual(rustCounts,originalCounts,`${label}: session severity counts`);
    for(let i=0;i<30 && !sdkUnread.length;i++) await wait(100);
    assert.deepEqual(sdkUnread.at(-1),originalCounts,`${label}: SDK unread severity counts`);
  }
  console.log('workflow and override severity: original worker, session, Socket.IO and SDK match');
  const until=new Date(Date.now()+180000).toISOString();
  for(const [base,id] of [[process.env.NOVU_REFERENCE_URL,highIds[0]],[process.env.NOVU_RUST_URL,highIds[1]]]) {
    assert.equal(await mutation(base,'PATCH','/v2/subscribers/alice/notifications/'+id+'/snooze',{snoozeUntil:until}),200);
  }
  async function countsAfterSnooze(base,applicationIdentifier) {
    const response=await fetch(base+'/v1/inbox/session',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({applicationIdentifier,subscriberId:'alice'})});
    assert.equal(response.status,201,await response.clone().text());
    return (await response.json()).data.unreadCount;
  }
  const [originalSnooze,rustSnooze]=await Promise.all([countsAfterSnooze(process.env.NOVU_REFERENCE_URL,'test'),countsAfterSnooze(process.env.NOVU_RUST_URL,'compat-ws-app')]);
  assert.deepEqual(rustSnooze,originalSnooze); assert.equal(originalSnooze.severity.high,0);
  for(const [base,id] of [[process.env.NOVU_REFERENCE_URL,highIds[0]],[process.env.NOVU_RUST_URL,highIds[1]]]) {
    assert.equal(await mutation(base,'PATCH','/v2/subscribers/alice/notifications/'+id+'/unsnooze',{}),200);
  }
  console.log('session severity counts exclude snoozed notifications: match');
  await wait(300); rust.events.length=0;
  const capSql = `INSERT INTO novu_rust_in_app_messages(id,environment_id,subscriber_id,workflow_identifier,transaction_id,content,payload) SELECT gen_random_uuid(),'${claims.environmentId}','alice','greeting','ws-cap-' || n,'fixture','{}'::jsonb FROM generate_series(1,105) n; INSERT INTO novu_rust_ws_events(environment_id,subscriber_internal_id,event,data) SELECT '${claims.environmentId}','${claims._id}','notification_received',jsonb_build_object('messageId',internal_id) FROM novu_rust_in_app_messages WHERE environment_id='${claims.environmentId}' AND subscriber_id='alice' AND transaction_id='ws-cap-1';`;
  const capped = spawnSync('docker',['exec',process.env.NOVU_RUST_PG_CONTAINER,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_compat','-c',capSql],{encoding:'utf8'});
  assert.equal(capped.status,0,capped.stderr);
  for(let i=0;i<50 && rust.events.filter(e=>['unseen_count_changed','unread_count_changed'].includes(e.event)).length<2;i++) await wait(100);
  assert.deepEqual(rust.events.find(e=>e.event==='unseen_count_changed')?.data,{unseenCount:100,hasMore:true});
  assert.deepEqual(rust.events.find(e=>e.event==='unread_count_changed')?.data,{unreadCount:100,hasMore:true,counts:{total:101,severity:{high:1,medium:1,low:0,none:99}}});
  ref.disconnect(); rust.disconnect(); filtered.disconnect(); await sdk.socket.disconnect(); await referenceSdk.socket.disconnect();
  console.log('API -> worker -> Socket.IO delivery: selected original message fields match');
  console.log('unseen/unread count payloads and context isolation: verified against fixture');
  console.log('reference @novu/js notification fields on both systems: match');
  console.log('scheduled unsnooze -> Socket.IO redelivery: verified with accelerated queue');
  console.log('unseen/unread pagination and severity caps: verified against source limits');
})().catch(error=>{console.error(error);process.exit(1)});
