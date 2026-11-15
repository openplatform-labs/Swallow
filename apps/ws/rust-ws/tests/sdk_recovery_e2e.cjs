const { Novu } = require(process.env.NOVU_SDK_MODULE);
const { spawnSync } = require('child_process');
const assert = require('assert/strict');
const net = require('net');
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
async function networkFailureScenario(startSocketRecovery, initialFailure=false, stall=false) {
  let blocked=initialFailure, connections=0, refreshes=0, errors=[];
  const sockets=new Set();
  const upstream=new URL(process.env.RUST_WS_URL);
  const proxy=net.createServer(client=>{
    sockets.add(client);
    client.on('close',()=>sockets.delete(client));
    client.on('error',()=>{});
    if(blocked) { if(!stall)client.destroy(); return; }
    const target=net.connect(Number(upstream.port),upstream.hostname);
    sockets.add(target);
    target.on('close',()=>sockets.delete(target));
    target.on('error',()=>client.destroy());
    client.on('close',()=>target.destroy());
    target.on('close',()=>client.destroy());
    client.pipe(target).pipe(client);
  });
  await new Promise(resolve=>proxy.listen(0,'127.0.0.1',resolve));
  const sdk=new Novu({applicationIdentifier:'recovery-app',subscriberId:'proxy-user',apiUrl:process.env.NOVU_RUST_URL,socketUrl:`http://127.0.0.1:${proxy.address().port}`,socketOptions:{reconnection:false,timeout:stall ? 10000 : 300}});
  sdk.on('socket.connect.resolved',({error})=>{if(!error)connections++});
  const stop=startSocketRecovery(sdk,{
    retryDelayMs:100,connectTimeoutMs:300,backoffFactor:2,maxRetryDelayMs:500,jitterRatio:0.2,recoverInitialConnection:initialFailure,initialConnectTimeoutMs:stall ? 200 : undefined,
    onError:error=>errors.push(error),
    refresh:async({signal})=>{
      const result=await sdk.notifications.list({useCache:false});
      if(result.error)throw result.error;
      if(!signal.aborted)refreshes++;
    },
  });
  sdk.on('notifications.unseen_count_changed',()=>{});
  try {
    if(!initialFailure) {
      await until(()=>connections===1,'proxied SDK did not connect');
      blocked=true;
      for(const socket of sockets)socket.destroy();
    }
    await until(()=>errors.length>0,'real SDK connection error was not reported');
    if(stall)assert.equal(errors[0].code,'SWALLOW_SOCKET_INITIAL_TIMEOUT');
    assert.equal(refreshes,0,'refresh ran while the proxy was blocked');
    blocked=false;
    await until(()=>connections>=(initialFailure ? 1 : 2) && refreshes>=1,'proxied SDK did not recover');
    console.log(`real SDK ${stall ? 'stalled initial' : initialFailure ? 'initial' : 'established'} network failure: error callback and Inbox refresh verified`);
  } finally {
    stop();
    await sdk.socket.disconnect();
    for(const socket of sockets)socket.destroy();
    await new Promise(resolve=>proxy.close(resolve));
  }
}
(async()=>{
  const { startSocketRecovery }=await import('../sdk/recovery.mjs');
  const sdk=new Novu({applicationIdentifier:'recovery-app',subscriberId:'alice',apiUrl:process.env.NOVU_RUST_URL,socketUrl:process.env.RUST_WS_URL,socketOptions:{reconnectionDelay:100,reconnectionDelayMax:250}});
  let connects=0, disconnects=0, notifications=[], recovered, refreshCount=0, errors=[];
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
  sdk.on('notifications.notification_received',({result})=>notifications.push(result));
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
    assert.equal(notifications.length,0,'unexpected live replay during listener gap');
    assert(connects>=2);
    const item=recovered.find(item=>item.transactionId==='recovery-gap-1');
    assert.equal(item.body,'hello recovery');
    assert.equal(item.workflow,undefined,'missing metadata must not fabricate a workflow');
    assert.equal(item.data,undefined,'missing metadata must not infer payload mapping');
    const seen=await item.seen();
    assert(!seen.error, String(seen.error?.originalError ?? seen.error));
    assert.equal(sql("SELECT is_seen FROM novu_rust_in_app_messages WHERE transaction_id='recovery-gap-1'"),'t');
    sql("UPDATE novu_rust_in_app_messages SET data=jsonb_build_object('explicit',true) WHERE transaction_id='recovery-gap-1'");
    const withData=await sdk.notifications.list({useCache:false});
    assert(!withData.error, String(withData.error));
    assert.deepEqual(withData.data.notifications[0].data,{explicit:true});
    for(const operation of [
      ()=>item.read(), ()=>item.archive(), ()=>item.unarchive(), ()=>item.unread(),
      ()=>item.snooze(new Date(Date.now()+300000).toISOString()), ()=>item.unsnooze(),
    ]) {
      const result=await operation();
      assert(!result.error, String(result.error?.originalError ?? result.error));
      assert.equal(result.data.workflow,undefined);
    }
    assert.equal(sql("SELECT is_read::text||','||is_archived::text||','||(snoozed_until IS NULL)::text FROM novu_rust_in_app_messages WHERE transaction_id='recovery-gap-1'"),'false,false,true');
    sql("UPDATE novu_rust_in_app_messages SET cta=jsonb_build_object('action',jsonb_build_object('buttons',jsonb_build_array(jsonb_build_object('type','primary','content','Open')),'status','pending')) WHERE transaction_id='recovery-gap-1'");
    const withAction=await sdk.notifications.list({useCache:false});
    assert(!withAction.error, String(withAction.error));
    const action=withAction.data.notifications[0];
    assert(action.primaryAction);
    const completed=await action.completePrimary();
    assert(!completed.error, String(completed.error?.originalError ?? completed.error));
    assert.equal(completed.data.primaryAction.isCompleted,true);
    const reverted=await action.revertPrimary();
    assert(!reverted.error, String(reverted.error?.originalError ?? reverted.error));
    assert.equal(reverted.data.primaryAction.isCompleted,false);
    sql("UPDATE novu_rust_workflows SET inbox_metadata='{}'::jsonb WHERE environment_id='env-a' AND identifier='greeting'");
    const partial=await sdk.notifications.list({useCache:false});
    assert.equal(partial.error?.originalError?.status,501,'partial metadata should remain rejected');
    const denied=await item.unread();
    assert.equal(denied.error?.originalError?.status,501,'partial metadata must reject mutation');
    assert.equal(sql("SELECT is_read FROM novu_rust_in_app_messages WHERE transaction_id='recovery-gap-1'"),'t');
    const snoozeDenied=await item.snooze(new Date(Date.now()+300000).toISOString());
    assert.equal(snoozeDenied.error?.originalError?.status,501);
    assert.equal(sql("SELECT count(*) FROM novu_queue_jobs WHERE kind='experimental-in-app-unsnooze' AND status='pending'"),'0');
    const actionDenied=await action.completePrimary();
    assert.equal(actionDenied.error?.originalError?.status,501);
    assert.equal(sql("SELECT cta#>>'{action,status}' FROM novu_rust_in_app_messages WHERE transaction_id='recovery-gap-1'"),'pending');
    sql("UPDATE novu_rust_workflows SET inbox_metadata=jsonb_build_object('id','64a000000000000000000004','identifier','greeting','name','Greeting','critical',false,'tags',jsonb_build_array(),'severity','none','payloadAsData',false) WHERE environment_id='env-a' AND identifier='greeting'");
    const restored=await sdk.notifications.list({useCache:false});
    assert(!restored.error, String(restored.error));
    assert.equal(restored.data.notifications[0].workflow.id,'64a000000000000000000004');
    await until(async()=>(await fetch(process.env.RUST_WS_URL+'/health/ready')).status===204,'WS did not become ready');
    const live=await fetch(process.env.NOVU_RUST_URL+'/v1/events/trigger',{
      method:'POST',headers:{'content-type':'application/json','authorization':'ApiKey compat-key'},
      body:JSON.stringify({name:'greeting',to:{subscriberId:'alice'},transactionId:'recovery-live-1',payload:{}}),
    });
    assert.equal(live.status,201,await live.text());
    await until(()=>notifications.some(n=>n.transactionId==='recovery-live-1'),'SDK did not receive new live notification after recovery');
    const liveItem=notifications.find(n=>n.transactionId==='recovery-live-1');
    assert.equal(liveItem.body,'hello recovery');
    sql("UPDATE novu_rust_workflows SET inbox_metadata=NULL WHERE environment_id='env-a' AND identifier='greeting'");
    const deleted=await item.delete();
    assert(!deleted.error, String(deleted.error?.originalError ?? deleted.error));
    assert.equal(sql("SELECT count(*) FROM novu_rust_in_app_messages WHERE transaction_id='recovery-gap-1'"),'0');
    await networkFailureScenario(startSocketRecovery);
    await networkFailureScenario(startSocketRecovery,true);
    await networkFailureScenario(startSocketRecovery,true,true);
    console.log('real SDK recovered Inbox state from Rust API after a lost WS event');
  } finally {
    stop();
    await sdk.socket.disconnect();
  }
})().catch(error=>{console.error(error);process.exit(1)});
