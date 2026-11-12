const path=require('path'),{spawnSync}=require('child_process'),assert=require('assert/strict');
const root=process.env.NOVU_REFERENCE_SOURCE;
const {io}=require(path.join(root,'node_modules/.pnpm/socket.io-client@4.8.3/node_modules/socket.io-client'));
const jwt=require(path.join(root,'node_modules/.pnpm/jsonwebtoken@9.0.3/node_modules/jsonwebtoken'));
const env='64a000000000000000000002',id='64a000000000000000000030';
const wait=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function sql(query){const p=spawnSync('docker',['exec',process.env.PG_CONTAINER,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_ws_test','-At','-c',query],{encoding:'utf8'});assert.equal(p.status,0,p.stderr);return p.stdout.trim();}
const token=jwt.sign({_id:id,environmentId:env,organizationId:'64a000000000000000000003',subscriberId:'crash-user',aud:'widget_user'},process.env.JWT_SECRET,{expiresIn:'1h'});
async function open(url){const s=io(url,{transports:['websocket'],reconnection:false,timeout:2000,query:{token}});for(let i=0;i<50&&!s.connected;i++)await wait(100);assert(s.connected);await wait(300);assert(s.connected);return s;}
async function until(predicate,message){for(let i=0;i<100;i++){if(predicate())return;await wait(100);}throw Error(message);}
(async()=>{
  sql(`INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('${env}','crash-user','${id}')`);
  const a=await open(process.env.STRICT_WS_URL);
  const first=sql(`SELECT id FROM novu_rust_ws_connections WHERE environment_id='${env}' AND subscriber_internal_id='${id}'`);
  assert.match(first,/^[0-9a-f-]{36}$/);
  const b=await open(process.env.STRICT_WS_URL2);
  assert.equal(sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE subscriber_internal_id='${id}'`),'2');
  process.kill(Number(process.env.STRICT_WS_PID),'SIGKILL');
  await until(()=>!a.connected,'killed instance socket remained connected');
  // Accelerate expiry; the production lease is 30 seconds.
  sql(`UPDATE novu_rust_ws_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id='${first}'`);
  await until(()=>sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE subscriber_internal_id='${id}'`)==='1','crashed instance lease was not reclaimed');
  assert(b.connected,'live instance connection was removed');
  assert.equal(sql(`SELECT is_online FROM novu_rust_subscribers WHERE internal_id='${id}'`),'t');
  b.disconnect();await until(()=>sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE subscriber_internal_id='${id}'`)==='0','live disconnect did not clean registry');
  assert.equal(sql(`SELECT is_online FROM novu_rust_subscribers WHERE internal_id='${id}'`),'f');
  console.log('crashed WS lease reclaimed without removing live instance');
  const c=await open(process.env.STRICT_WS_URL2);
  sql(`UPDATE novu_rust_ws_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE subscriber_internal_id='${id}'`);
  await until(()=>!c.connected,'expired socket remained connected');
  await until(()=>sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE subscriber_internal_id='${id}'`)==='0','expired live lease was not removed');
  assert.equal(sql(`SELECT is_online FROM novu_rust_subscribers WHERE internal_id='${id}'`),'f');
  console.log('expired live lease cannot be renewed and its socket closes');
  const orphan='11111111-1111-4111-8111-111111111111';
  sql(`INSERT INTO novu_rust_ws_connections(id,environment_id,subscriber_internal_id,lease_expires_at) VALUES ('${orphan}','${env}','deleted-internal',clock_timestamp()-interval '1 second')`);
  await until(()=>sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE id='${orphan}'`)==='0','orphaned lease was not reclaimed');
  console.log('expired orphan lease reclaimed');
})().catch(error=>{console.error(error);process.exitCode=1});
