// Two real Socket.IO transports and an isolated PostgreSQL instance.
const {createHmac}=require('crypto');
const {spawn,spawnSync}=require('child_process');
const assert=require('assert/strict');
const wait=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function sql(q){const r=spawnSync('docker',['exec',process.env.PG_CONTAINER,'psql','-q','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_ws_shutdown','-At','-c',q],{encoding:'utf8'});assert.equal(r.status,0,r.stderr);return r.stdout.trim();}
function token(){const encode=x=>Buffer.from(JSON.stringify(x)).toString('base64url');const body=encode({alg:'HS256',typ:'JWT'})+'.'+encode({_id:'subscriber-id',environmentId:'env-a',aud:'widget_user',exp:Math.floor(Date.now()/1000)+300});return body+'.'+createHmac('sha256',process.env.JWT_SECRET).update(body).digest('base64url');}
async function open(port){let ready=false,closed=false;const ws=new WebSocket(`ws://127.0.0.1:${port}/socket.io/?EIO=4&transport=websocket`);ws.addEventListener('message',e=>{const d=String(e.data);if(d.startsWith('0'))ws.send('40'+JSON.stringify({token:token()}));if(d.startsWith('40'))ready=true;if(d==='2')ws.send('3');});ws.addEventListener('close',()=>closed=true);for(let i=0;i<120&&!ready&&!closed;i++)await wait(25);assert(ready,'connection failed');return ws;}
async function expectRejected(port){let rejected=false;const ws=new WebSocket(`ws://127.0.0.1:${port}/socket.io/?EIO=4&transport=websocket`);ws.addEventListener('message',e=>{const d=String(e.data);if(d.startsWith('0'))ws.send('40'+JSON.stringify({token:token()}));if(d.startsWith('41'))rejected=true;if(d==='2')ws.send('3');});ws.addEventListener('close',()=>rejected=true);await until(()=>rejected,'new connection not rejected during drain');ws.close();}
async function until(fn,message){for(let i=0;i<160;i++){if(await fn())return;await wait(25);}throw Error(message);}
function count(){return sql("SELECT count(*) FROM novu_rust_ws_connections WHERE environment_id='env-a' AND subscriber_internal_id='subscriber-id'");}
function online(){return sql("SELECT is_online FROM novu_rust_subscribers WHERE environment_id='env-a' AND internal_id='subscriber-id'");}
(async()=>{
  const first=await open(process.env.WS_PORT); const peer=await open(process.env.WS_PEER_PORT);
  await until(()=>count()==='2','connections not registered');
  assert.equal(online(),'t');
  process.kill(Number(process.env.WS_PID),'SIGTERM');
  await until(async()=>{try{return (await fetch(`http://127.0.0.1:${process.env.WS_PORT}/health/ready`)).status===503;}catch{return false;}},'readiness not drained');
  assert.equal(first.readyState,WebSocket.OPEN,'existing connection closed before drain delay');
  assert.equal((await fetch(`http://127.0.0.1:${process.env.WS_PEER_PORT}/health/ready`)).status,204);
  await expectRejected(process.env.WS_PORT);
  assert.equal(count(),'2','draining instance registered a new socket');
  await until(()=>first.readyState===WebSocket.CLOSED,'connection did not close');
  await until(()=>count()==='1','draining connection not cleaned');
  assert.equal(peer.readyState,WebSocket.OPEN);
  assert.equal(online(),'t','remaining connection should stay online');
  peer.close();
  await until(()=>count()==='0','peer cleanup failed');
  assert.equal(online(),'f');
  console.log('drain rejected new connections and cleaned only its own socket');
  // A registration that passed the initial gate but is blocked on PostgreSQL
  // must be cleaned after the signal once its transaction can finish.
  const registrationLock=spawn('docker',['exec','-e','PGAPPNAME=ws_registration_lock',process.env.PG_CONTAINER,'psql','-q','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_ws_shutdown','-c',"BEGIN; SELECT 1 FROM novu_rust_subscribers WHERE environment_id='env-a' AND internal_id='subscriber-id' FOR UPDATE; SELECT pg_sleep(3); COMMIT"],{stdio:'ignore'});
  const registrationDone=new Promise((resolve,reject)=>{registrationLock.once('error',reject);registrationLock.once('exit',code=>code===0?resolve():reject(Error(`registration lock exit ${code}`)));});
  await until(()=>sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='ws_registration_lock' AND wait_event='PgSleep'")==='1','registration lock not acquired');
  const racing=await open(process.env.WS_RACE_PORT);
  await until(()=>sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-query' AND wait_event_type='Lock' AND query LIKE '%FOR UPDATE%'")!=='0','registration did not block');
  process.kill(Number(process.env.WS_RACE_PID),'SIGTERM');
  await registrationDone;
  await until(()=>racing.readyState===WebSocket.CLOSED,'racing socket did not close');
  await until(()=>count()==='0','racing registration was not cleaned');
  assert.equal(online(),'f');
  console.log('registration overlapping shutdown was cleaned');
  // Hold the subscriber lock longer than the configured grace so cleanup must
  // fail closed; the surviving instance reclaims the expired lease later.
  const doomed=await open(process.env.WS_TIMEOUT_PORT);
  await until(()=>count()==='1','timeout fixture not registered');
  const id=sql("SELECT id FROM novu_rust_ws_connections WHERE subscriber_internal_id='subscriber-id'");
  const lock=spawn('docker',['exec','-e','PGAPPNAME=ws_shutdown_lock',process.env.PG_CONTAINER,'psql','-q','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_ws_shutdown','-c',"BEGIN; SELECT 1 FROM novu_rust_subscribers WHERE environment_id='env-a' AND internal_id='subscriber-id' FOR UPDATE; SELECT pg_sleep(5); COMMIT"],{stdio:'ignore'});
  const lockDone=new Promise((resolve,reject)=>{lock.once('error',reject);lock.once('exit',code=>code===0?resolve():reject(Error(`lock exit ${code}`)));});
  await until(()=>sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='ws_shutdown_lock' AND wait_event='PgSleep'")==='1','lock fixture did not start');
  process.kill(Number(process.env.WS_TIMEOUT_PID),'SIGTERM');
  await until(()=>doomed.readyState===WebSocket.CLOSED,'timeout socket did not close');
  await wait(1500);
  assert.equal(count(),'1','cleanup unexpectedly completed while DB row was locked');
  await lockDone;
  sql(`UPDATE novu_rust_ws_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id='${id}'`);
  await until(()=>count()==='0','surviving instance did not reclaim expired connection');
  assert.equal(online(),'f');
  console.log('timeout left a lease that the surviving instance reclaimed');

})().catch(e=>{console.error(e);process.exit(1)});
