const path = require('path');
const {spawnSync} = require('child_process');
const assert = require('assert/strict');
const root = process.env.NOVU_REFERENCE_SOURCE;
const {io} = require(path.join(root,'node_modules/.pnpm/socket.io-client@4.8.3/node_modules/socket.io-client'));
const jwt = require(path.join(root,'node_modules/.pnpm/jsonwebtoken@9.0.3/node_modules/jsonwebtoken'));
const env='64a000000000000000000002', oldId='64a000000000000000000020', newId='64a000000000000000000021';
const wait=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function sql(query) {
  const result=spawnSync('docker',['exec',process.env.PG_CONTAINER,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_ws_test','-At','-c',query],{encoding:'utf8'});
  assert.equal(result.status,0,result.stderr); return result.stdout.trim();
}
function token(id,environmentId=env) { return jwt.sign({_id:id,environmentId,organizationId:'64a000000000000000000003',subscriberId:'strict-user',aud:'widget_user'},process.env.JWT_SECRET,{expiresIn:'1h'}); }
async function socketState(auth,accepted) {
  const socket=io(process.env.STRICT_WS_URL,{transports:['websocket'],reconnection:false,timeout:2000,query:{token:auth}});
  let connected=false,disconnected=false;
  socket.on('connect',()=>{connected=true});
  socket.on('disconnect',()=>{disconnected=true});
  socket.on('connect_error',()=>{disconnected=true});
  for(let i=0;i<50 && !(accepted ? connected : disconnected);i++) await wait(100);
  if(accepted) {
    assert(connected,'valid identity did not connect');
    await wait(300);
    assert(!disconnected,'valid identity was disconnected');
  } else assert(disconnected,'stale identity remained connected');
  socket.disconnect();
  await wait(250);
}
async function open(url, auth) {
  const socket=io(url,{transports:['websocket'],reconnection:false,timeout:2000,query:{token:auth}});
  let error;socket.on('connect_error',e=>{error=e});
  for(let i=0;i<50 && !socket.connected && !error;i++) await wait(100);
  assert(socket.connected,`connection failed: ${error}`);await wait(300);assert(socket.connected);
  return socket;
}
async function disconnected(sockets,timeout=3000) {
  for(let i=0;i<timeout/100 && sockets.some(s=>s.connected);i++) await wait(100);
  assert(sockets.every(s=>!s.connected),'revoked socket remained connected');
}
(async()=>{
  sql(`INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('${env}','strict-user','${oldId}')`);
  const sockets=await Promise.all([open(process.env.STRICT_WS_URL,token(oldId)),open(process.env.STRICT_WS_URL2,token(oldId))]);
  sql(`BEGIN; DELETE FROM novu_rust_subscribers WHERE environment_id='${env}' AND subscriber_id='strict-user'; ROLLBACK`);
  await wait(500);assert(sockets.every(s=>s.connected),'rolled-back deletion revoked sockets');
  sql(`DELETE FROM novu_rust_subscribers WHERE environment_id='${env}' AND subscriber_id='strict-user'; INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('${env}','strict-user','${newId}')`);
  await disconnected(sockets);
  await socketState(token(oldId),false);await socketState(token(newId,'64a000000000000000000012'),false);await socketState(token(newId),true);
  assert.equal(sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE subscriber_internal_id IN ('${oldId}','${newId}')`),'0');
  console.log('strict WS deletion closes two instances and rollback preserves connections');
  const lostId='64a000000000000000000022';
  sql(`INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('${env}','lost-user','${lostId}')`);
  const lost=await open(process.env.STRICT_WS_URL,token(lostId));
  sql(`ALTER TABLE novu_rust_subscribers DISABLE TRIGGER novu_rust_subscriber_deleted_notify; DELETE FROM novu_rust_subscribers WHERE internal_id='${lostId}' AND environment_id='${env}'; ALTER TABLE novu_rust_subscribers ENABLE TRIGGER novu_rust_subscriber_deleted_notify`);
  await disconnected([lost],9000);
  console.log('strict WS reconciliation closes sockets after missed notification');
  const deliveryId='64a000000000000000000023';
  sql(`INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('${env}','delivery-user','${deliveryId}')`);
  const delivery=await open(process.env.STRICT_WS_URL,token(deliveryId));let received=0;delivery.on('sensitive_probe',()=>{received++});
  sql(`ALTER TABLE novu_rust_subscribers DISABLE TRIGGER novu_rust_subscriber_deleted_notify; DELETE FROM novu_rust_subscribers WHERE internal_id='${deliveryId}' AND environment_id='${env}'; ALTER TABLE novu_rust_subscribers ENABLE TRIGGER novu_rust_subscriber_deleted_notify; INSERT INTO novu_rust_ws_events(environment_id,subscriber_internal_id,event,data) VALUES ('${env}','${deliveryId}','sensitive_probe','{}')`);
  await disconnected([delivery]);assert.equal(received,0);
  console.log('strict WS event delivery rejects deleted identity');
})().catch(error=>{console.error(error);process.exitCode=1});
