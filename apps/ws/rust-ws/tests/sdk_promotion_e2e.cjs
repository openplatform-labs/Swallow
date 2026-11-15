const { Novu } = require(process.env.NOVU_SDK_MODULE);
const { spawnSync } = require('child_process');
const assert = require('assert/strict');
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(fn, message, timeout=30000) {
  const end=Date.now()+timeout;
  while(Date.now()<end) { if(await fn())return; await wait(100); }
  throw Error(message);
}
function pgctl(data, ...args) {
  const result=spawnSync(process.env.TEST_PG_CTL,['-D',data,...args,'-w'],{encoding:'utf8',timeout:20000});
  assert.equal(result.status,0,'PostgreSQL control failed');
}
async function trigger(id) {
  let response;
  await until(async()=>{
    try { response=await fetch(process.env.NOVU_RUST_URL+'/v1/events/trigger',{method:'POST',headers:{'content-type':'application/json',authorization:'ApiKey compat-key'},body:JSON.stringify({name:'greeting',to:'alice',transactionId:id,payload:{}})});return response.status===201; }
    catch { return false; }
  },'API trigger did not recover');
  const result=await response.json();assert.equal(result.data.transactionId,id);
}
(async()=>{
  const {startSocketRecovery}=await import('../sdk/recovery.mjs');
  const clients=[];
  try {
    for(const socketUrl of [process.env.RUST_WS_ONE,process.env.RUST_WS_TWO]) {
      const sdk=new Novu({applicationIdentifier:'ha-app',subscriberId:'alice',apiUrl:process.env.NOVU_RUST_URL,socketUrl,socketOptions:{reconnection:false,timeout:500}});
      const state={sdk,connects:0,disconnects:0,refreshes:0,notifications:[],errors:[]};clients.push(state);
      sdk.on('socket.connect.resolved',({error})=>{if(!error)state.connects++;});
      sdk.on('socket.disconnect.resolved',()=>state.disconnects++);
      state.stop=startSocketRecovery(sdk,{retryDelayMs:100,connectTimeoutMs:500,backoffFactor:2,maxRetryDelayMs:1000,onError:error=>{if(state.errors.length<3)state.errors.push(String(error));},refresh:async({signal})=>{const result=await sdk.notifications.list({useCache:false});if(result.error)throw result.error;if(!signal.aborted){state.refreshed=result.data.notifications;state.refreshes++;}}});
      sdk.on('notifications.notification_received',({result})=>state.notifications.push(result));
      await until(()=>state.connects===1,'SDK did not connect to WS');
    }
    await trigger('ha-before');
    await until(()=>clients.every(c=>c.notifications.some(n=>n.transactionId==='ha-before')),'initial event did not reach both WS clients');
    pgctl(process.env.TEST_PRIMARY_DATA,'-m','immediate','stop');
    await until(()=>clients.every(c=>c.disconnects>=1),'listener loss did not disconnect both clients');
    pgctl(process.env.TEST_STANDBY_DATA,'promote');
    await until(()=>clients.every(c=>c.connects>=2&&c.refreshes>=1&&c.refreshed?.some(n=>n.transactionId==='ha-before')),'SDK clients did not reconnect and refresh after promotion');
    await trigger('ha-after');
    await until(()=>clients.every(c=>c.notifications.some(n=>n.transactionId==='ha-after')),'post-promotion event did not reach both WS clients');
    console.log('Two SDK clients reconnected, refreshed Inbox and received live events after synchronous PostgreSQL promotion.');
  } finally {
    for(const c of clients){c.stop?.();await c.sdk.socket.disconnect();}
  }
})().catch(error=>{console.error(error);process.exitCode=1;});
