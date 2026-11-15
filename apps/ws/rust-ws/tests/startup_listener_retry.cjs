// Local plaintext PostgreSQL proxy: query startup is allowed, LISTEN startup can be rejected or stalled.
const net=require('net');
const {spawn,spawnSync}=require('child_process');
const assert=require('assert/strict');
const wait=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(fn,message,timeout=10000){const end=Date.now()+timeout;while(Date.now()<end){if(await fn())return;await wait(50);}throw Error(message);}
function sql(query){const r=spawnSync(process.env.TEST_PSQL,[process.env.TEST_PG_URL,'-q','-At','-v','ON_ERROR_STOP=1','-c',query],{encoding:'utf8'});assert.equal(r.status,0,'fixture SQL failed');return r.stdout.trim();}
async function run(mode,action){
 let allow=false,queryCount=0,listenerCount=0,child;const peers=new Set();
 const server=net.createServer(front=>{
  peers.add(front);front.on('close',()=>peers.delete(front));front.on('error',()=>{});
  let startup=Buffer.alloc(0);
  const initial=chunk=>{
   startup=Buffer.concat([startup,chunk]);if(startup.length<4)return;const size=startup.readUInt32BE(0);if(size<8||size>16384){front.destroy();return;}if(startup.length<size)return;
   front.removeListener('data',initial);
   if(startup.includes(Buffer.from('novu-rust-ws-query')))queryCount++;
   else if(startup.includes(Buffer.from('novu-rust-ws-listener'))){listenerCount++;if(!allow){if(mode==='reject')front.destroy();return;}}
   else {front.destroy();return;}
   const back=net.connect(Number(process.env.TEST_PG_PORT),'127.0.0.1');peers.add(back);back.on('close',()=>peers.delete(back));back.on('error',()=>front.destroy());front.on('error',()=>back.destroy());front.on('close',()=>back.destroy());back.on('close',()=>front.destroy());back.write(startup);front.pipe(back).pipe(front);
  };
  front.on('data',initial);
 });
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 try {
  const retry=action==='deadline'?'2':'20';
  child=spawn(process.env.TEST_WS_BINARY,[],{env:{...process.env,DATABASE_URL:`postgres://postgres@127.0.0.1:${server.address().port}/postgres?connect_timeout=1`,SWALLOW_WS_STARTUP_RETRY_SECONDS:retry},stdio:['ignore','ignore','pipe']});
  let stderr='';child.stderr.on('data',v=>stderr+=v.toString());const exit=new Promise(resolve=>child.once('exit',(code,signal)=>resolve({code,signal})));
  await until(()=>queryCount===1&&listenerCount>=1,'query connection or blocked listener was not observed');
  assert.equal(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-query'"),'1');
  assert.equal(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'"),'0');
  if(action==='recover'){
   await until(()=>listenerCount>=2,'LISTEN attempt was not retried');
   let ready=false;try{ready=(await fetch(`http://${process.env.LISTEN_ADDR}/health/ready`)).ok;}catch{}assert.equal(ready,false);
   allow=true;await until(async()=>{try{return (await fetch(`http://${process.env.LISTEN_ADDR}/health/ready`)).status===204;}catch{return false;}},'WS did not recover');
   assert.equal(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'"),'1');child.kill('SIGTERM');assert.equal((await exit).code,0);
  } else if(action==='deadline'){
   const result=await exit;assert.notEqual(result.code,0);assert.match(stderr,/retry deadline exceeded/);
  } else {
   child.kill('SIGTERM');const result=await Promise.race([exit,wait(3000).then(()=>{throw Error('startup termination timed out');})]);assert.notEqual(result.code,0);assert.match(stderr,/startup interrupted/);
  }
 } finally {
  if(child&&child.exitCode===null){child.kill('SIGKILL');await new Promise(resolve=>child.once('exit',resolve));}
  for(const socket of peers)socket.destroy();await new Promise(resolve=>server.close(resolve));
 }
}
(async()=>{
 await run('reject','recover');
 await run('stall','deadline');
 await run('stall','terminate');
 console.log('LISTEN startup rejection, stalled deadline and termination passed with a live query connection.');
})().catch(e=>{console.error(e);process.exitCode=1;});
