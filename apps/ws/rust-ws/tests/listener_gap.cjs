// Engine.IO/Socket.IO protocol test using Node's built-in WebSocket, no provider traffic.
const {createHmac} = require('crypto');
const {spawn,spawnSync} = require('child_process');
const assert = require('assert/strict');
const wait = ms => new Promise(r => setTimeout(r,ms));
const tokenPart=x=>Buffer.from(JSON.stringify(x)).toString('base64url');
const body=tokenPart({alg:'HS256',typ:'JWT'})+'.'+tokenPart({_id:'subscriber-id',environmentId:'env-a',aud:'widget_user',exp:Math.floor(Date.now()/1000)+300});
const token=body+'.'+createHmac('sha256',process.env.JWT_SECRET).update(body).digest('base64url');
const noExpBody=tokenPart({alg:'HS256',typ:'JWT'})+'.'+tokenPart({_id:'subscriber-id',environmentId:'env-a',aud:'widget_user'});
const noExpToken=noExpBody+'.'+createHmac('sha256',process.env.JWT_SECRET).update(noExpBody).digest('base64url');
function tokenWithExp(exp) {
  const body=tokenPart({alg:'HS256',typ:'JWT'})+'.'+tokenPart({_id:'subscriber-id',environmentId:'env-a',aud:'widget_user',exp});
  return body+'.'+createHmac('sha256',process.env.JWT_SECRET).update(body).digest('base64url');
}
function sql(query) { const out=spawnSync('docker',['exec',process.env.PG_CONTAINER,'psql','-q','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_ws_gap','-At','-c',query],{encoding:'utf8'});assert.equal(out.status,0,out.stderr);return out.stdout.trim(); }
async function open(port, credential=token) {
  let messages=[];let closed=false;
  const ws=new WebSocket(`ws://127.0.0.1:${port}/socket.io/?EIO=4&transport=websocket`);
  ws.addEventListener('message',e=>{const data=String(e.data);messages.push(data);if(data.startsWith('0'))ws.send('40'+JSON.stringify({token:credential}));if(data==='2')ws.send('3');});
  ws.addEventListener('close',()=>{closed=true});
  for(let i=0;i<100&&!messages.some(x=>x.startsWith('40'))&&!closed;i++)await wait(25);
  assert(messages.some(x=>x.startsWith('40')),'Socket.IO connect timeout');
  return {ws,messages,get closed(){return closed}};
}
async function until(fn,message,timeout=4000){for(let i=0;i<timeout/25;i++){if(fn())return;await wait(25);}throw Error(message);}
(async()=>{
  const noExpGuarded=await open(process.env.GUARDED_PORT,noExpToken);
  await until(()=>noExpGuarded.messages.some(x=>x.startsWith('41'))||noExpGuarded.closed,'expiry-required socket accepted a token without exp');
  noExpGuarded.ws.close();
  for(const exp of [null,'4102444800',1]) {
    const invalid=await open(process.env.GUARDED_PORT,tokenWithExp(exp));
    await until(()=>invalid.messages.some(x=>x.startsWith('41'))||invalid.closed,'expiry-required socket accepted invalid exp');
    invalid.ws.close();
  }
  const noExpLegacy=await open(process.env.LEGACY_PORT,noExpToken);
  await wait(200);
  assert(!noExpLegacy.closed&&!noExpLegacy.messages.some(x=>x.startsWith('41')),'default rejected a token without exp');
  noExpLegacy.ws.close();
  const guarded=await open(process.env.GUARDED_PORT), legacy=await open(process.env.LEGACY_PORT);
  await wait(200);
  assert.equal(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'"),'3');
  sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'");
  await until(()=>guarded.messages.some(x=>x.startsWith('41'))||guarded.closed,'guarded socket was not disconnected');
  assert(!legacy.closed && !legacy.messages.some(x=>x.startsWith('41')),'default mode disconnected');
  const during=await open(process.env.GUARDED_PORT);
  await until(()=>during.messages.some(x=>x.startsWith('41'))||during.closed,'new connection was not rejected during listener gap',700);
  during.ws.close();
  for(let i=0;i<100;i++){if(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'")==='3')break;await wait(50);if(i===99)throw Error('listeners did not recover');}
  const resumed=await open(process.env.GUARDED_PORT);await wait(200);
  sql("INSERT INTO novu_rust_ws_events(environment_id,subscriber_internal_id,event,data) VALUES ('env-a','subscriber-id','recovered_probe','{\"ok\":true}')");
  await until(()=>resumed.messages.some(x=>x.includes('recovered_probe')),'reconnected socket did not receive event');
  await until(()=>legacy.messages.some(x=>x.includes('recovered_probe')),'default socket did not receive event');
  sql("ALTER TABLE novu_rust_ws_events RENAME TO temporarily_unavailable_ws_events; INSERT INTO temporarily_unavailable_ws_events(environment_id,subscriber_internal_id,event,data) VALUES ('env-a','subscriber-id','lost_probe','{}')");
  await until(()=>resumed.messages.some(x=>x.startsWith('41'))||resumed.closed,'guarded socket was not disconnected after event lookup failure');
  assert(!legacy.closed && !legacy.messages.some(x=>x.startsWith('41')),'default socket closed after event lookup failure');
  await wait(2200);
  assert.equal((await fetch(`http://127.0.0.1:${process.env.GUARDED_PORT}/health/ready`)).status,503,'readiness recovered while event lookup remained unavailable');
  const blocked=await open(process.env.GUARDED_PORT);
  await until(()=>blocked.messages.some(x=>x.startsWith('41'))||blocked.closed,'new socket admitted during sustained event lookup failure');
  blocked.ws.close();
  const immediate=await open(process.env.IMMEDIATE_PORT);
  await wait(300);
  assert(!immediate.closed && !immediate.messages.some(x=>x.startsWith('41')),'immediate mode rejected new socket');
  immediate.ws.close();
  sql("ALTER TABLE temporarily_unavailable_ws_events RENAME TO novu_rust_ws_events");
  for(let i=0;i<100;i++){if((await fetch(`http://127.0.0.1:${process.env.GUARDED_PORT}/health/ready`)).status===204)break;await wait(50);if(i===99)throw Error('event lookup probe did not recover readiness');}
  const restored=await open(process.env.GUARDED_PORT);await wait(200);
  sql("INSERT INTO novu_rust_ws_events(environment_id,subscriber_internal_id,event,data) VALUES ('env-a','subscriber-id','second_recovery','{}')");
  await until(()=>restored.messages.some(x=>x.includes('second_recovery')),'guarded delivery did not recover after lookup failure');
  sql("BEGIN; INSERT INTO novu_rust_ws_events(id,environment_id,subscriber_internal_id,event,data) VALUES ('00000000-0000-4000-8000-000000000001','env-a','subscriber-id','missing_probe','{}'); DELETE FROM novu_rust_ws_events WHERE id='00000000-0000-4000-8000-000000000001'; COMMIT");
  await until(()=>restored.messages.some(x=>x.startsWith('41'))||restored.closed,'missing event did not disconnect guarded socket');
  assert(!legacy.closed && !legacy.messages.some(x=>x.startsWith('41')),'default socket closed for missing event');
  // A connected PostgreSQL backend can remain blocked without a connection error.
  // Notify an already committed event while a different session holds an exclusive lock.
  for(let i=0;i<100;i++){if((await fetch(`http://127.0.0.1:${process.env.GUARDED_PORT}/health/ready`)).status===204)break;await wait(50);if(i===99)throw Error('missing-event recovery did not complete');}
  const timed=await open(process.env.GUARDED_PORT);await wait(150);
  sql("INSERT INTO novu_rust_ws_events(id,environment_id,subscriber_internal_id,event,data) VALUES ('00000000-0000-4000-8000-000000000002','env-a','subscriber-id','timeout_fixture','{}')");
  await until(()=>timed.messages.some(x=>x.includes('timeout_fixture')),'pre-lock event did not arrive');
  const locker=spawn('docker',['exec',process.env.PG_CONTAINER,'psql','-q','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_ws_gap','-c',"SET application_name='novu-gap-locker'; BEGIN; LOCK TABLE novu_rust_ws_events IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(30); COMMIT"],{stdio:'ignore'});
  await until(()=>sql("SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON l.pid=a.pid WHERE a.application_name='novu-gap-locker' AND l.relation='novu_rust_ws_events'::regclass AND l.mode='AccessExclusiveLock' AND l.granted")==='1','test lock not acquired');
  sql("SELECT pg_notify('novu_rust_ws_events','00000000-0000-4000-8000-000000000002')");
  await until(()=>timed.messages.some(x=>x.startsWith('41'))||timed.closed,'timed-out event did not disconnect guarded socket',5000);
  assert.equal((await fetch(`http://127.0.0.1:${process.env.GUARDED_PORT}/health/ready`)).status,503,'timed-out lookup did not mark instance unready');
  const timeoutBlocked=await open(process.env.GUARDED_PORT);await until(()=>timeoutBlocked.messages.some(x=>x.startsWith('41'))||timeoutBlocked.closed,'new socket admitted after timed-out lookup',700);timeoutBlocked.ws.close();
  assert(!legacy.closed && !legacy.messages.some(x=>x.startsWith('41')),'default mode disconnected on lookup timeout');
  sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='novu-gap-locker'");locker.kill();
  for(let i=0;i<150;i++){if((await fetch(`http://127.0.0.1:${process.env.GUARDED_PORT}/health/ready`)).status===204)break;await wait(50);if(i===149)throw Error('timed-out event probe did not recover');}
  const timeoutRecovered=await open(process.env.GUARDED_PORT);await wait(150);
  sql("INSERT INTO novu_rust_ws_events(environment_id,subscriber_internal_id,event,data) VALUES ('env-a','subscriber-id','after_timeout','{}')");
  await until(()=>timeoutRecovered.messages.some(x=>x.includes('after_timeout')),'delivery failed after timeout recovery');timeoutRecovered.ws.close();timed.ws.close();
  restored.ws.close();resumed.ws.close();legacy.ws.close();guarded.ws.close();
  console.log('listener and event query failures close guarded sockets, preserve default, and recover');
})().catch(e=>{console.error(e);process.exitCode=1});
