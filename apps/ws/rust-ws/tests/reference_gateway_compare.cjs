const path = require('path');
const { spawnSync } = require('child_process');
const root = process.env.NOVU_REFERENCE_SOURCE;
const { io } = require(path.join(root, 'node_modules/.pnpm/socket.io-client@4.8.3/node_modules/socket.io-client'));
const jwt = require(path.join(root, 'node_modules/.pnpm/jsonwebtoken@9.0.3/node_modules/jsonwebtoken'));
const assert = require('assert/strict');
const env = '64a000000000000000000002';
const id = '64a000000000000000000010';
const claims = { _id:id, environmentId:env, organizationId:'64a000000000000000000003', subscriberId:'alice', aud:'widget_user' };
const token = jwt.sign(claims,process.env.JWT_SECRET,{expiresIn:'1h'});
const contextToken = jwt.sign({...claims,contextKeys:['tenant:one','region:two']},process.env.JWT_SECRET,{expiresIn:'1h'});
const badAudience = jwt.sign({...claims,aud:'user'},process.env.JWT_SECRET,{expiresIn:'1h'});
const otherEnvironment = jwt.sign({...claims,environmentId:'64a000000000000000000012'},process.env.JWT_SECRET,{expiresIn:'1h'});
const expired = jwt.sign({...claims,exp:1},process.env.JWT_SECRET);
const forged = jwt.sign(claims,'wrong-secret',{expiresIn:'1h'});
const fractional = jwt.sign({...claims,exp:4102444800.5,nbf:-0.5},process.env.JWT_SECRET);
const futureFractional = jwt.sign({...claims,exp:4102444800.5,nbf:4102444800.5},process.env.JWT_SECRET);
const wait = ms => new Promise(r => setTimeout(r,ms));
async function status(kind) {
  if (kind === 'reference') return await (await fetch(process.env.REF_WS_URL + '/status')).json();
  const query = `SELECT json_build_object('isOnline',is_online,'lastOnlineAt',CASE WHEN last_online_at IS NULL THEN NULL ELSE 'present' END)::text FROM novu_rust_subscribers WHERE environment_id='${env}' AND internal_id='${id}'`;
  const result = spawnSync('docker',['exec',process.env.PG_CONTAINER,'psql','-U','postgres','-d','novu_ws_test','-At','-c',query],{encoding:'utf8'});
  assert.equal(result.status,0,result.stderr); return JSON.parse(result.stdout);
}
async function online(kind, expected, last) {
  for (let i=0;i<40;i++) {
    const state=await status(kind);
    if (state.isOnline === expected && (last === undefined || Boolean(state.lastOnlineAt) === last)) return;
    await wait(100);
  }
  throw Error(`${kind} expected online=${expected} last=${last}: ${JSON.stringify(await status(kind))}`);
}
function client(url, options) { return io(url,{transports:['websocket'],reconnection:false,timeout:2000,...options}); }
async function connected(socket) {
  if (socket.connected) return;
  await new Promise((resolve,reject)=>{ socket.once('connect',resolve); socket.once('connect_error',reject); setTimeout(()=>reject(Error('connect timeout')),3000).unref(); });
}
async function rejected(socket) {
  let disconnected = false;
  socket.on('disconnect',()=>{disconnected=true});
  socket.on('connect_error',()=>{disconnected=true});
  for(let i=0;i<40 && !disconnected;i++) await wait(100);
  assert(disconnected, 'invalid client remained connected');
  socket.disconnect();
}
(async()=>{
  for (const [kind,url] of [['reference',process.env.REF_WS_URL],['rust',process.env.RUST_WS_URL]]) {
    for (const invalid of [undefined,'null',forged,expired,badAudience,futureFractional]) {
      const s=client(url,{query:invalid === undefined ? {} : {token:invalid}});
      await rejected(s); await online(kind,false,false);
    }
    console.log(kind+' invalid JWT: rejected');
    let other=client(url,{query:{token:otherEnvironment}}); await connected(other); await wait(200); await online(kind,false,false); other.disconnect(); await wait(200); await online(kind,false,false);
    console.log(kind+' cross-environment presence isolation: match');
    let s=client(url,{query:{token}}); await connected(s); await online(kind,true,false); s.disconnect(); await online(kind,false,true);
    console.log(kind+' query authentication and presence: match');
    let numeric=client(url,{query:{token:fractional}}); await connected(numeric); await online(kind,true,true); numeric.disconnect(); await online(kind,false,true);
    console.log(kind+' fractional and negative JWT times: accepted');
    s=io(url,{transports:['polling'],reconnection:false,timeout:2000,query:{token}}); await connected(s); await online(kind,true,true); s.disconnect(); await online(kind,false,true);
    console.log(kind+' polling authentication: match');
    s=client(url,{query:{token:contextToken}}); await connected(s); await online(kind,true,true); s.disconnect(); await online(kind,false,true);
    console.log(kind+' context-bearing token: match');
    const shortToken = jwt.sign(claims,process.env.JWT_SECRET,{expiresIn:'2s'});
    s=client(url,{query:{token:shortToken}}); await connected(s); await online(kind,true,true); await wait(2600); s.disconnect(); await wait(250); await online(kind,true,true);
    s=client(url,{query:{token}}); await connected(s); await online(kind,true,true); s.disconnect(); await online(kind,false,true);
    console.log(kind+' expired token at disconnect: match');
    s=client(url,{auth:{token},query:{token:forged}}); await connected(s); await online(kind,true,true); s.disconnect(); await online(kind,false,true);
    const wrong=client(url,{auth:{token:forged},query:{token}}); await rejected(wrong); await online(kind,false,true);
    const nonString=client(url,{auth:{token:123},query:{token}}); await rejected(nonString); await online(kind,false,true);
    s=client(url,{auth:{token:0},query:{token}}); await connected(s); await online(kind,true,true); s.disconnect(); await online(kind,false,true);
    console.log(kind+' auth precedence: match');
    const a=client(url,{query:{token}}), b=client(url,{query:{token}});
    await Promise.all([connected(a),connected(b)]); await online(kind,true,true); await wait(300);
    a.disconnect(); await online(kind,false,true);
    b.disconnect(); await online(kind,false,true);
    console.log(kind+' two-connection disconnect behavior: match');
    const c=client(url,{query:{token}}), d=client(url,{query:{token}}), e=client(url,{query:{token}});
    await Promise.all([connected(c),connected(d),connected(e)]); await online(kind,true,true); await wait(300);
    c.disconnect(); await online(kind,true,true); d.disconnect(); await online(kind,false,true); e.disconnect();
    console.log(kind+' three-connection disconnect behavior: match');
  }
  const a=client(process.env.RUST_WS_URL,{query:{token}}), b=client(process.env.RUST_WS_URL2,{query:{token}}), c=client(process.env.RUST_WS_URL2,{query:{token}});
  await Promise.all([connected(a),connected(b),connected(c)]); await online('rust',true,true); await wait(300);
  a.disconnect(); await online('rust',true,true); b.disconnect(); await online('rust',false,true); c.disconnect(); await wait(200);
  console.log('rust cross-process presence: verified');
  for (const [kind,url] of [['reference',process.env.REF_WS_URL],['rust',process.env.RUST_WS_URL]]) {
    const specs = [
      {name:'empty-one',context:[],url},
      {name:'empty-two',context:[],url:kind === 'rust' ? process.env.RUST_WS_URL2 : url},
      {name:'one',context:['tenant:one'],url},
      {name:'two',context:['tenant:one','region:two'],url},
      {name:'other-user',context:[],url,subject:'64a000000000000000000011'},
    ];
    for (const spec of specs) {
      const credential = jwt.sign({...claims,_id:spec.subject || id,contextKeys:spec.context},process.env.JWT_SECRET,{expiresIn:'1h'});
      spec.socket = client(spec.url,{query:{token:credential}});
      spec.events = [];
      spec.socket.onAny((name,data) => spec.events.push({name,data}));
    }
    await Promise.all(specs.map(spec=>connected(spec.socket))); await wait(400);
    for (const [context, expected] of [[[],['empty-one','empty-two']],[['tenant:one'],['one']],[['region:two','tenant:one'],['two']],[['tenant:one','tenant:one'],['two']]]) {
      for (const spec of specs) spec.events.length = 0;
      const data = {message:{_id:'64a000000000000000000099',content:'large-' + 'x'.repeat(9000),cta:{action:{buttons:[]}}}};
      if (kind === 'reference') {
        const response = await fetch(process.env.REF_WS_URL + '/emit',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({userId:id,event:'notification_received',contextKeys:context,data})});
        assert.equal(response.status,204);
      } else {
        const quote = value => "'" + value.replaceAll("'","''") + "'";
        const sql = `INSERT INTO novu_rust_ws_events(environment_id,subscriber_internal_id,context_keys,event,data) VALUES (${quote(env)},${quote(id)},ARRAY[${context.map(quote).join(',')}]::text[],'notification_received',${quote(JSON.stringify(data))}::jsonb)`;
        const result = spawnSync('docker',['exec',process.env.PG_CONTAINER,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_ws_test','-c',sql],{encoding:'utf8'});
        assert.equal(result.status,0,result.stderr);
      }
      await wait(500);
      for (const spec of specs) {
        assert.equal(spec.events.length,expected.includes(spec.name) ? 1 : 0, `${kind}: ${spec.name} context=${JSON.stringify(context)}`);
        if (spec.events.length) assert.deepEqual(spec.events[0],{name:'notification_received',data});
      }
    }
    for (const spec of specs) spec.socket.disconnect();
    await wait(300);
    console.log(kind+' context-filtered event fan-out: match');
  }
  // Drop only the LISTEN connections, leaving the query connections and
  // active sockets intact; both independent WS instances must resubscribe.
  const reconnectA=client(process.env.RUST_WS_URL,{query:{token}}), reconnectB=client(process.env.RUST_WS_URL2,{query:{token}});
  const deliveries=[[],[]];
  reconnectA.on('reconnect_probe',data=>deliveries[0].push(data));
  reconnectB.on('reconnect_probe',data=>deliveries[1].push(data));
  await Promise.all([connected(reconnectA),connected(reconnectB)]);
  function sql(query) {
    const result=spawnSync('docker',['exec',process.env.PG_CONTAINER,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_ws_test','-At','-c',query],{encoding:'utf8'});
    assert.equal(result.status,0,result.stderr);
    return result.stdout.trim();
  }
  assert.equal(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener'"),'2');
  assert.equal(sql("SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener') AS killed"),'2');
  for(let i=0;i<80;i++) {
    const ready=sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener' AND state='idle' AND query='LISTEN novu_rust_ws_events'");
    if (ready==='2') break;
    if(i===79) throw Error('listeners did not reconnect');
    await wait(100);
  }
  sql(`INSERT INTO novu_rust_ws_events(environment_id,subscriber_internal_id,event,data) VALUES ('${env}','${id}','reconnect_probe','{"marker":42}'::jsonb)`);
  for(let i=0;i<50 && deliveries.some(events=>events.length===0);i++) await wait(100);
  assert.deepEqual(deliveries,[[{marker:42}],[{marker:42}]]);
  console.log('rust listener reconnect and two-process fan-out: verified');
  assert.equal(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-query'"),'2');
  assert.equal(sql("SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='novu-rust-ws-query') AS killed"),'2');
  for(let i=0;i<50;i++) {
    if(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-query'")==='0') break;
    if(i===49) throw Error('old query connections did not terminate');
    await wait(100);
  }
  // Allow the asynchronous client drivers to observe backend termination.
  await wait(300);
  sql(`INSERT INTO novu_rust_ws_events(environment_id,subscriber_internal_id,event,data) VALUES ('${env}','${id}','reconnect_probe','{"marker":43}'::jsonb)`);
  for(let i=0;i<50 && deliveries.some(events=>events.length<2);i++) await wait(100);
  assert.deepEqual(deliveries,[[{marker:42},{marker:43}],[{marker:42},{marker:43}]]);
  assert.equal(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-query'"),'2');
  // Restart the disposable PostgreSQL instance to lose both listener and
  // query connections, then verify new events after resubscription.
  const restarted=spawnSync('docker',['restart',process.env.PG_CONTAINER],{encoding:'utf8',timeout:30000});
  assert.equal(restarted.status,0,restarted.stderr);
  for(let i=0;i<100;i++) {
    const ready=spawnSync('docker',['exec',process.env.PG_CONTAINER,'pg_isready','-q','-U','postgres','-d','novu_ws_test'],{encoding:'utf8'});
    if(ready.status===0) break;
    if(i===99) throw Error('PostgreSQL did not restart');
    await wait(100);
  }
  for(let i=0;i<100;i++) {
    const ready=sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='novu-rust-ws-listener' AND state='idle' AND query='LISTEN novu_rust_ws_events'");
    if(ready==='2') break;
    if(i===99) throw Error('listeners did not reconnect after database restart');
    await wait(100);
  }
  sql(`INSERT INTO novu_rust_ws_events(environment_id,subscriber_internal_id,event,data) VALUES ('${env}','${id}','reconnect_probe','{"marker":44}'::jsonb)`);
  for(let i=0;i<50 && deliveries.some(events=>events.length<3);i++) await wait(100);
  assert.deepEqual(deliveries,[[{marker:42},{marker:43},{marker:44}],[{marker:42},{marker:43},{marker:44}]]);
  console.log('rust listener and query recovery after PostgreSQL restart: verified');
  const afterReconnect=client(process.env.RUST_WS_URL,{query:{token}});
  await connected(afterReconnect); await online('rust',true,true);
  afterReconnect.disconnect(); await wait(200);
  reconnectA.disconnect(); reconnectB.disconnect(); await online('rust',false,true);
  console.log('rust query reconnect, fan-out and subsequent presence: verified');
  for(let i=0;i<50;i++) {
    if(sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE environment_id='${env}' AND subscriber_internal_id='${id}'`)==='0') break;
    if(i===49) throw Error('prior socket cleanup timed out');
    await wait(100);
  }
  // Disconnect while the database is unavailable. The token is valid when
  // disconnecting but expires before recovery, so a retry must retain the
  // original authentication decision instead of rechecking later.
  const outageToken=jwt.sign(claims,process.env.JWT_SECRET,{expiresIn:'8s'});
  const outageSocket=client(process.env.RUST_WS_URL,{query:{token:outageToken}});
  await connected(outageSocket); await online('rust',true,true);
  assert.equal(sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE environment_id='${env}' AND subscriber_internal_id='${id}'`),'1');
  const stopped=spawnSync('docker',['stop','--time','1',process.env.PG_CONTAINER],{encoding:'utf8',timeout:10000});
  assert.equal(stopped.status,0,stopped.stderr);
  assert(Date.now()/1000 < jwt.decode(outageToken).exp-1,'test token expired before disconnect');
  outageSocket.disconnect();
  await wait(9000);
  const started=spawnSync('docker',['start',process.env.PG_CONTAINER],{encoding:'utf8',timeout:10000});
  assert.equal(started.status,0,started.stderr);
  for(let i=0;i<100;i++) {
    const ready=spawnSync('docker',['exec',process.env.PG_CONTAINER,'pg_isready','-q','-U','postgres','-d','novu_ws_test'],{encoding:'utf8'});
    if(ready.status===0) break;
    if(i===99) throw Error('PostgreSQL did not start after outage');
    await wait(100);
  }
  for(let i=0;i<150;i++) {
    const remaining=sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE environment_id='${env}' AND subscriber_internal_id='${id}'`);
    if(remaining==='0' && (await status('rust')).isOnline===false) break;
    if(i===149) throw Error('disconnect cleanup did not recover after outage');
    await wait(100);
  }
  console.log('rust disconnected socket cleanup after PostgreSQL outage: verified');
  // Force registration failure after authentication. Cleanup of the failed
  // connection must not remove or mark offline another active connection.
  const healthy=client(process.env.RUST_WS_URL2,{query:{token}});
  await connected(healthy); await online('rust',true,true);
  sql("CREATE FUNCTION ws_test_reject_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected registration failure'; END $$; CREATE TRIGGER ws_test_reject_insert BEFORE INSERT ON novu_rust_ws_connections FOR EACH ROW EXECUTE FUNCTION ws_test_reject_insert()");
  const failed=client(process.env.RUST_WS_URL,{query:{token}});
  await rejected(failed); await wait(600);
  assert.equal(sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE environment_id='${env}' AND subscriber_internal_id='${id}'`),'1');
  await online('rust',true,true);
  sql('DROP TRIGGER ws_test_reject_insert ON novu_rust_ws_connections; DROP FUNCTION ws_test_reject_insert()');
  healthy.disconnect(); await online('rust',false,true);
  console.log('rust failed registration cleanup preserves other connections: verified');
})().catch(error=>{console.error(error);process.exit(1)});
