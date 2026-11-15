// Real SDK and WS process with a PostgreSQL proxy that drops one listener's responses.
const { Novu }=require(process.env.NOVU_SDK_MODULE);
const {spawn,spawnSync}=require('child_process');
const net=require('net');
const assert=require('assert/strict');
const wait=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(fn,message,timeout=20000){const end=Date.now()+timeout;while(Date.now()<end){if(await fn())return;await wait(50);}throw Error(message);}
function sql(query){const local=process.env.TEST_POSTGRES_MODE==='local';const cmd=local?process.env.TEST_PSQL:'docker';const args=local?[process.env.DATABASE_URL,'-q','-v','ON_ERROR_STOP=1','-At','-c',query]:['exec',process.env.PG_CONTAINER,'psql','-q','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_recovery','-At','-c',query];const r=spawnSync(cmd,args,{encoding:'utf8'});assert.equal(r.status,0,'fixture SQL failed');return r.stdout.trim();}
async function port(){const s=net.createServer();await new Promise(r=>s.listen(0,'127.0.0.1',r));const p=s.address().port;await new Promise(r=>s.close(r));return p;}
async function trigger(id){const response=await fetch(process.env.NOVU_RUST_URL+'/v1/events/trigger',{method:'POST',headers:{'content-type':'application/json',authorization:'ApiKey compat-key'},body:JSON.stringify({name:'greeting',to:'silent-user',transactionId:id,payload:{}})});assert.equal(response.status,201,await response.text());}
(async()=>{
 let child,armed=false,dropped=0,listenerCount=0;const peers=new Set();
 const proxy=net.createServer(front=>{
  peers.add(front);front.on('close',()=>peers.delete(front));front.on('error',()=>{});let startup=Buffer.alloc(0);
  const initial=chunk=>{
   startup=Buffer.concat([startup,chunk]);if(startup.length<4)return;const n=startup.readUInt32BE(0);if(n<8||n>16384){front.destroy();return;}if(startup.length<n)return;front.removeListener('data',initial);
   const listener=startup.includes(Buffer.from('novu-rust-ws-listener'));const first=listener&&++listenerCount===1;
   if(!listener&&!startup.includes(Buffer.from('novu-rust-ws-query'))){front.destroy();return;}
   const back=net.connect(Number(process.env.TEST_PG_PORT),'127.0.0.1');peers.add(back);back.on('close',()=>peers.delete(back));back.on('error',()=>front.destroy());front.on('error',()=>back.destroy());front.on('close',()=>back.destroy());back.on('close',()=>front.destroy());back.write(startup);
   back.on('data',data=>{if(first&&armed){dropped++;return;}front.write(data);});front.pipe(back);
  };front.on('data',initial);
 });
 await new Promise(resolve=>proxy.listen(0,'127.0.0.1',resolve));let sdk,stop,stopped=false;
 try {
  const wsPort=await port(),socketUrl=`http://127.0.0.1:${wsPort}`;
  child=spawn(process.env.TEST_WS_BINARY,[],{env:{...process.env,DATABASE_URL:`postgres://postgres@127.0.0.1:${proxy.address().port}/novu_recovery?connect_timeout=1`,LISTEN_ADDR:`127.0.0.1:${wsPort}`,NOVU_WS_ENABLE_HEALTH_ENDPOINTS:'true',NOVU_WS_DISCONNECT_ON_EVENT_GAP:'true',NOVU_WS_EVENT_LOOKUP_RECOVERY:'probe',NOVU_WS_LISTENER_RECONNECT_TIMEOUT_SECONDS:'2',NOVU_WS_LISTENER_PROBE_INTERVAL_SECONDS:'1',NOVU_WS_LISTENER_PROBE_TIMEOUT_SECONDS:'1'},stdio:'ignore'});
  await until(async()=>{if(child.exitCode!==null)throw Error('fixture WS exited');try{return (await fetch(socketUrl+'/health/ready')).status===204;}catch{return false;}},'fixture WS not ready');
  const {startSocketRecovery}=await import('../sdk/recovery.mjs');sdk=new Novu({applicationIdentifier:'recovery-app',subscriberId:'silent-user',apiUrl:process.env.NOVU_RUST_URL,socketUrl,socketOptions:{reconnection:false,timeout:500}});
  let connects=0,disconnects=0,refreshes=0,recovered=[],notifications=[];
  sdk.on('socket.connect.resolved',({error})=>{if(!error)connects++;});sdk.on('socket.disconnect.resolved',()=>disconnects++);
  stop=startSocketRecovery(sdk,{retryDelayMs:100,connectTimeoutMs:500,onError:()=>{},refresh:async({signal})=>{const result=await sdk.notifications.list({useCache:false});if(result.error)throw result.error;if(!signal.aborted){recovered=result.data.notifications;refreshes++;}}});
  sdk.on('notifications.notification_received',({result})=>notifications.push(result));await until(()=>connects===1,'SDK did not connect');
  armed=true;process.kill(child.pid,'SIGSTOP');stopped=true;
  try {
   await trigger('silent-gap');await until(()=>sql("SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='silent-gap'")==='1','message not persisted');
   await until(()=>dropped>0,'proxy did not drop listener traffic');assert.equal(notifications.length,0);
  } finally {process.kill(child.pid,'SIGCONT');stopped=false;}
  await until(()=>disconnects>=1&&connects>=2&&refreshes>=1&&recovered.some(n=>n.transactionId==='silent-gap'),'SDK did not recover lost event');
  assert(!notifications.some(n=>n.transactionId==='silent-gap'),'missed event was unexpectedly replayed');assert(listenerCount>=2);
  await trigger('silent-after');await until(()=>notifications.some(n=>n.transactionId==='silent-after'),'later live event was not received');
  console.log('Published SDK recovered a missed Inbox message after a silent LISTEN stall and received a later live event.');
 } finally {
  if(stopped&&child)try{process.kill(child.pid,'SIGCONT');}catch{}
  stop?.();if(sdk)await sdk.socket.disconnect();
  if(child&&child.exitCode===null){child.kill('SIGTERM');await new Promise(resolve=>child.once('exit',resolve));}
  for(const p of peers)p.destroy();await new Promise(resolve=>proxy.close(resolve));
 }
})().catch(error=>{console.error(error);process.exitCode=1;});
