const { Novu } = require(process.env.NOVU_SDK_MODULE);
const { spawnSync } = require('child_process');
const assert = require('assert/strict');
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(fn, message, timeout=15000) {
  const end=Date.now()+timeout;
  while(Date.now()<end) { if(await fn()) return; await wait(100); }
  throw Error(message);
}
function sql(query) {
  const result=spawnSync('docker',['exec',process.env.PG_CONTAINER,'psql','-q','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_recovery','-At','-c',query],{encoding:'utf8'});
  assert.equal(result.status,0,result.stderr);
  return result.stdout.trim();
}
(async()=>{
  const { startSocketRecovery }=await import('../sdk/recovery.mjs');
  const sdk=new Novu({applicationIdentifier:'recovery-app',subscriberId:'alice',apiUrl:process.env.NOVU_RUST_URL,socketUrl:process.env.RUST_WS_URL,socketOptions:{reconnectionDelay:100,reconnectionDelayMax:250}});
  let connects=0, disconnects=0, notifications=0, recovered, refreshCount=0, errors=[];
  sdk.on('socket.connect.resolved',({error})=>{if(!error)connects++});
  sdk.on('socket.disconnect.resolved',()=>disconnects++);
  const stop=startSocketRecovery(sdk,{
    retryDelayMs:100,connectTimeoutMs:1000,onError:error=>{ if(errors.length<3) errors.push(String(error.originalError ?? error)); },
    refresh:async({signal})=>{
      const result=await sdk.notifications.list({useCache:false});
      if(result.error)throw result.error;
      if(!signal.aborted) { recovered=result.data.notifications; refreshCount++; }
    },
  });
  sdk.on('notifications.notification_received',()=>notifications++);
  try {
    await until(()=>connects===1,'SDK failed to connect');
    const initial=await sdk.notifications.list({useCache:false});
    assert(!initial.error, String(initial.error));
    assert.equal(initial.data.notifications.length,0);
    sql("UPDATE novu_rust_workflows SET inbox_metadata=NULL WHERE environment_id='env-a' AND identifier='greeting'");
    // Pause WS, terminate its LISTEN connection, and store a real worker message
    // before resuming. This makes the missed event deterministic.
    process.kill(Number(process.env.WS_PID),'SIGSTOP');
    try {
      assert.equal(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'"),'1');
      sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'");
      const response=await fetch(process.env.NOVU_RUST_URL+'/v1/events/trigger',{
        method:'POST',headers:{'content-type':'application/json','authorization':'ApiKey compat-key'},
        body:JSON.stringify({name:'greeting',to:{subscriberId:'alice'},transactionId:'recovery-gap-1',payload:{test:'not inferred'}}),
      });
      assert.equal(response.status,201,await response.text());
      await until(()=>sql("SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='recovery-gap-1'")==='1','worker failed to store message');
    } finally { process.kill(Number(process.env.WS_PID),'SIGCONT'); }
    await until(()=>disconnects>=1,'SDK was not disconnected');
    try {
      await until(()=>refreshCount>=1 && recovered?.some(item=>item.transactionId==='recovery-gap-1'),'SDK did not fetch persisted notification');
    } catch (error) {
      console.error({connects,disconnects,refreshCount,recovered,errors});
      throw error;
    }
    assert.equal(notifications,0,'unexpected live replay during listener gap');
    assert(connects>=2);
    const item=recovered.find(item=>item.transactionId==='recovery-gap-1');
    assert.equal(item.body,'hello recovery');
    assert.equal(item.workflow,undefined,'missing metadata must not fabricate a workflow');
    assert.equal(item.data,undefined,'missing metadata must not infer payload mapping');
    sql("UPDATE novu_rust_in_app_messages SET data=jsonb_build_object('explicit',true) WHERE transaction_id='recovery-gap-1'");
    const withData=await sdk.notifications.list({useCache:false});
    assert(!withData.error, String(withData.error));
    assert.deepEqual(withData.data.notifications[0].data,{explicit:true});
    const update=await item.read();
    assert.equal(update.error?.originalError?.status,501,'mutation without metadata must still be rejected');
    assert.equal(sql("SELECT is_read FROM novu_rust_in_app_messages WHERE transaction_id='recovery-gap-1'"),'f');
    sql("UPDATE novu_rust_workflows SET inbox_metadata='{}'::jsonb WHERE environment_id='env-a' AND identifier='greeting'");
    const partial=await sdk.notifications.list({useCache:false});
    assert.equal(partial.error?.originalError?.status,501,'partial metadata should remain rejected');
    sql("UPDATE novu_rust_workflows SET inbox_metadata=jsonb_build_object('id','64a000000000000000000004','identifier','greeting','name','Greeting','critical',false,'tags',jsonb_build_array(),'severity','none','payloadAsData',false) WHERE environment_id='env-a' AND identifier='greeting'");
    const restored=await sdk.notifications.list({useCache:false});
    assert(!restored.error, String(restored.error));
    assert.equal(restored.data.notifications[0].workflow.id,'64a000000000000000000004');
    console.log('real SDK recovered Inbox state from Rust API after a lost WS event');
  } finally {
    stop();
    await sdk.socket.disconnect();
  }
})().catch(error=>{console.error(error);process.exit(1)});
