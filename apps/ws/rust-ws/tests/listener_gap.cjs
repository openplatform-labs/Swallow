// Engine.IO/Socket.IO protocol test using Node's built-in WebSocket, no provider traffic.
const {createHmac} = require('crypto');
const {spawnSync} = require('child_process');
const assert = require('assert/strict');
const wait = ms => new Promise(r => setTimeout(r,ms));
const tokenPart=x=>Buffer.from(JSON.stringify(x)).toString('base64url');
const body=tokenPart({alg:'HS256',typ:'JWT'})+'.'+tokenPart({_id:'subscriber-id',environmentId:'env-a',aud:'widget_user',exp:Math.floor(Date.now()/1000)+300});
const token=body+'.'+createHmac('sha256',process.env.JWT_SECRET).update(body).digest('base64url');
function sql(query) { const out=spawnSync('docker',['exec',process.env.PG_CONTAINER,'psql','-q','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_ws_gap','-At','-c',query],{encoding:'utf8'});assert.equal(out.status,0,out.stderr);return out.stdout.trim(); }
async function open(port) {
  let messages=[];let closed=false;
  const ws=new WebSocket(`ws://127.0.0.1:${port}/socket.io/?EIO=4&transport=websocket`);
  ws.addEventListener('message',e=>{const data=String(e.data);messages.push(data);if(data.startsWith('0'))ws.send('40'+JSON.stringify({token}));if(data==='2')ws.send('3');});
  ws.addEventListener('close',()=>{closed=true});
  for(let i=0;i<100&&!messages.some(x=>x.startsWith('40'))&&!closed;i++)await wait(25);
  assert(messages.some(x=>x.startsWith('40')),'Socket.IO connect timeout');
  return {ws,messages,get closed(){return closed}};
}
async function until(fn,message,timeout=4000){for(let i=0;i<timeout/25;i++){if(fn())return;await wait(25);}throw Error(message);}
(async()=>{
  const guarded=await open(process.env.GUARDED_PORT), legacy=await open(process.env.LEGACY_PORT);
  await wait(200);
  assert.equal(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'"),'2');
  sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'");
  await until(()=>guarded.messages.some(x=>x.startsWith('41'))||guarded.closed,'guarded socket was not disconnected');
  assert(!legacy.closed && !legacy.messages.some(x=>x.startsWith('41')),'default mode disconnected');
  const during=await open(process.env.GUARDED_PORT);
  await until(()=>during.messages.some(x=>x.startsWith('41'))||during.closed,'new connection was not rejected during listener gap',700);
  during.ws.close();
  for(let i=0;i<100;i++){if(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'")==='2')break;await wait(50);if(i===99)throw Error('listeners did not recover');}
  const resumed=await open(process.env.GUARDED_PORT);await wait(200);
  sql("INSERT INTO novu_rust_ws_events(environment_id,subscriber_internal_id,event,data) VALUES ('env-a','subscriber-id','recovered_probe','{\"ok\":true}')");
  await until(()=>resumed.messages.some(x=>x.includes('recovered_probe')),'reconnected socket did not receive event');
  await until(()=>legacy.messages.some(x=>x.includes('recovered_probe')),'default socket did not receive event');
  resumed.ws.close();legacy.ws.close();guarded.ws.close();
  console.log('listener loss policy closes guarded sockets, preserves default, and resumes events');
})().catch(e=>{console.error(e);process.exitCode=1});
