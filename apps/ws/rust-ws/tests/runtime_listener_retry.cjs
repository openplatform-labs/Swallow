// Local PostgreSQL proxy stalls LISTEN statements after successful authentication.
const net=require('net');
const {spawn,spawnSync}=require('child_process');
const assert=require('assert/strict');
const wait=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(fn,message,timeout=12000){const end=Date.now()+timeout;while(Date.now()<end){if(await fn())return;await wait(50);}throw Error(message);}
function sql(query){const r=spawnSync(process.env.TEST_PSQL,[process.env.TEST_PG_URL,'-q','-At','-v','ON_ERROR_STOP=1','-c',query],{encoding:'utf8'});assert.equal(r.status,0,'fixture SQL failed');return r.stdout.trim();}
(async()=>{
 let block=false,listenerCount=0,child;const peers=new Set();
 const server=net.createServer(front=>{
  peers.add(front);front.on('close',()=>peers.delete(front));front.on('error',()=>{});let startup=Buffer.alloc(0);
  const initial=chunk=>{
   startup=Buffer.concat([startup,chunk]);if(startup.length<4)return;const size=startup.readUInt32BE(0);if(size<8||size>16384){front.destroy();return;}if(startup.length<size)return;
   front.removeListener('data',initial);const listener=startup.includes(Buffer.from('novu-rust-ws-listener'));if(listener)listenerCount++;else if(!startup.includes(Buffer.from('novu-rust-ws-query'))){front.destroy();return;}
   const stall=listener&&block;const back=net.connect(Number(process.env.TEST_PG_PORT),'127.0.0.1');peers.add(back);back.on('close',()=>peers.delete(back));back.on('error',()=>front.destroy());front.on('error',()=>back.destroy());front.on('close',()=>back.destroy());back.on('close',()=>front.destroy());back.write(startup);back.pipe(front);if(stall)front.on('data',()=>{});else front.pipe(back);
  };front.on('data',initial);
 });
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 try {
  child=spawn(process.env.TEST_WS_BINARY,[],{env:{...process.env,DATABASE_URL:`postgres://postgres@127.0.0.1:${server.address().port}/postgres?connect_timeout=1`,NOVU_WS_LISTENER_RECONNECT_TIMEOUT_SECONDS:'1',NOVU_WS_DISCONNECT_ON_EVENT_GAP:'true'},stdio:['ignore','ignore','pipe']});
  let stderr='';child.stderr.on('data',v=>stderr+=v.toString());const exit=new Promise(resolve=>child.once('exit',(code,signal)=>resolve({code,signal})));
  const health=`http://${process.env.LISTEN_ADDR}/health/ready`;
  await until(async()=>{try{return (await fetch(health)).status===204;}catch{return false;}},'WS did not become ready');
  assert.equal(listenerCount,1);block=true;assert.equal(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'"),'1');sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'");
  await until(async()=>{try{return (await fetch(health)).status===503;}catch{return false;}},'WS did not report listener outage');
  await until(()=>listenerCount>=3&&stderr.includes('reconnect timed out'),'stalled LISTEN setup was not retried');
  assert(Number(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'"))<=1,'timed-out listener connections accumulated');
  block=false;await until(async()=>{try{return (await fetch(health)).status===204;}catch{return false;}},'WS did not recover LISTEN');
  assert.equal(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'"),'1');child.kill('SIGTERM');assert.equal((await exit).code,0);
 } finally {
  if(child&&child.exitCode===null){child.kill('SIGKILL');await new Promise(resolve=>child.once('exit',resolve));}
  for(const socket of peers)socket.destroy();await new Promise(resolve=>server.close(resolve));
 }
 console.log('Stalled runtime LISTEN setup timed out, closed its driver and recovered.');
})().catch(e=>{console.error(e);process.exitCode=1;});
