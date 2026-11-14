// Real Socket.IO connections on independently configured WS processes.
const {createHmac} = require('crypto');
const {spawnSync} = require('child_process');
const assert = require('assert/strict');
const wait = ms => new Promise(r => setTimeout(r,ms));
function sql(query) { const out=spawnSync('docker',['exec',process.env.PG_CONTAINER,'psql','-q','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_ws_presence','-At','-c',query],{encoding:'utf8'}); assert.equal(out.status,0,out.stderr); return out.stdout.trim(); }
function token(id) { const encode=x=>Buffer.from(JSON.stringify(x)).toString('base64url'); const body=encode({alg:'HS256',typ:'JWT'})+'.'+encode({_id:id,environmentId:'env-a',aud:'widget_user',exp:Math.floor(Date.now()/1000)+300}); return body+'.'+createHmac('sha256',process.env.JWT_SECRET).update(body).digest('base64url'); }
async function open(port,id) { let ready=false, closed=false; const ws=new WebSocket(`ws://127.0.0.1:${port}/socket.io/?EIO=4&transport=websocket`); ws.addEventListener('message',e=>{const data=String(e.data); if(data.startsWith('0'))ws.send('40'+JSON.stringify({token:token(id)})); if(data.startsWith('40'))ready=true; if(data==='2')ws.send('3');}); ws.addEventListener('close',()=>closed=true); for(let i=0;i<100&&!ready&&!closed;i++)await wait(25); assert(ready,'Socket.IO connection failed'); return ws; }
async function until(fn,message) { for(let i=0;i<160;i++){if(fn())return; await wait(25);} throw Error(message); }
async function count(id,n) { await until(()=>sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE environment_id='env-a' AND subscriber_internal_id='${id}'`)==String(n), 'connection count did not settle'); }
function online(id) { return sql(`SELECT is_online FROM novu_rust_subscribers WHERE environment_id='env-a' AND internal_id='${id}'`); }
(async()=>{
  let a=await open(process.env.ACTIVE_PORT,'active-id'), b=await open(process.env.ACTIVE_PORT2,'active-id');
  await count('active-id',2); assert.equal(online('active-id'),'t');
  a.close(); await count('active-id',1); assert.equal(online('active-id'),'t','one live connection must remain online');
  b.close(); await count('active-id',0); assert.equal(online('active-id'),'f');
  let c=await open(process.env.REFERENCE_PORT,'reference-id'),d=await open(process.env.REFERENCE_PORT,'reference-id');
  await count('reference-id',2); c.close(); await count('reference-id',1); assert.equal(online('reference-id'),'f','default preserves reference presence semantics');
  d.close(); await count('reference-id',0);
  // An expired lease must not be counted as an active connection.
  let e=await open(process.env.ACTIVE_PORT,'active-id'); await count('active-id',1);
  sql("INSERT INTO novu_rust_ws_connections(id,environment_id,subscriber_internal_id,lease_expires_at) VALUES ('00000000-0000-4000-8000-000000000099','env-a','active-id',clock_timestamp()-interval '1 second')");
  e.close(); await until(()=>online('active-id')==='f','expired lease incorrectly kept presence online');
  console.log('active and reference policies handle remaining sockets across instances and ignore expired leases');
})().catch(e=>{console.error(e);process.exitCode=1});
