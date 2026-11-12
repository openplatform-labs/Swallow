// Inject a lost PostgreSQL COMMIT acknowledgement on an isolated WS process.
const net = require('net');
const path = require('path');
const { spawn, spawnSync } = require('child_process');
const assert = require('assert/strict');
const root = process.env.NOVU_REFERENCE_SOURCE;
const { io } = require(path.join(root,'node_modules/.pnpm/socket.io-client@4.8.3/node_modules/socket.io-client'));
const jwt = require(path.join(root,'node_modules/.pnpm/jsonwebtoken@9.0.3/node_modules/jsonwebtoken'));
const env='64a000000000000000000002', id='64a000000000000000000010';
const token=jwt.sign({_id:id,environmentId:env,organizationId:'64a000000000000000000003',subscriberId:'alice',aud:'widget_user'},process.env.JWT_SECRET,{expiresIn:'1h'});
const wait=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function sql(query) {
  const result=spawnSync('docker',['exec',process.env.PG_CONTAINER,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','novu_ws_test','-At','-c',query],{encoding:'utf8'});
  assert.equal(result.status,0,result.stderr); return result.stdout.trim();
}
async function listen(server) {
  await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve)});
  return server.address().port;
}
(async()=>{
  let armed=false, dropped=0, blockUntil=0, queryStarts=0, forwardedCommits=0, child;
  const peers=new Set();
  const proxy=net.createServer(front=>{
    peers.add(front); front.on('close',()=>peers.delete(front));
    if(Date.now()<blockUntil) { front.destroy(); return; }
    const back=net.connect(Number(process.env.PG_PORT),'127.0.0.1');
    peers.add(back); back.on('close',()=>peers.delete(back));
    front.on('error',()=>back.destroy()); back.on('error',()=>front.destroy());
    front.on('close',()=>back.destroy()); back.on('close',()=>front.destroy());
    let incoming=Buffer.alloc(0), outgoing=Buffer.alloc(0), startup=true, queryConnection=false, targetCommit=false;
    front.on('data',chunk=>{
      incoming=Buffer.concat([incoming,chunk]);
      while(incoming.length >= (startup ? 4 : 5)) {
        const size=startup ? incoming.readUInt32BE(0) : 1+incoming.readUInt32BE(1);
        if(size<4 || size>16*1024*1024) {front.destroy();return;}
        if(incoming.length<size) return;
        const frame=incoming.subarray(0,size); incoming=incoming.subarray(size);
        if(startup) { queryConnection=frame.includes(Buffer.from('novu-rust-ws-query')); if(queryConnection) queryStarts++; startup=false; }
        else if(armed && queryConnection && frame[0]===0x51 && frame.subarray(5).toString()==='COMMIT\0') {targetCommit=true;armed=false;}
        back.write(frame);
      }
    });
    back.on('data',chunk=>{
      outgoing=Buffer.concat([outgoing,chunk]);
      while(outgoing.length>=5) {
        const size=1+outgoing.readUInt32BE(1);
        if(size<5 || size>16*1024*1024) {back.destroy();return;}
        if(outgoing.length<size) return;
        const frame=outgoing.subarray(0,size); outgoing=outgoing.subarray(size);
        if(targetCommit && frame[0]===0x43 && frame.subarray(5).toString()==='COMMIT\0') {
          dropped++; blockUntil=Date.now()+2200; front.destroy(); back.destroy(); return;
        }
        if(queryConnection && frame[0]===0x43 && frame.subarray(5).toString()==='COMMIT\0') forwardedCommits++;
        front.write(frame);
      }
    });
  });
  try {
    const proxyPort=await listen(proxy);
    const reserve=net.createServer(); const wsPort=await listen(reserve);
    await new Promise(resolve=>reserve.close(resolve));
    child=spawn(path.join(process.env.CARGO_TARGET_DIR,'debug/novu-rust-ws'),[],{env:{...process.env,DATABASE_URL:`postgresql://postgres@127.0.0.1:${proxyPort}/novu_ws_test`,LISTEN_ADDR:`127.0.0.1:${wsPort}`},stdio:'ignore'});
    const url=`http://127.0.0.1:${wsPort}`;
    for(let i=0;i<100;i++) {
      if(child.exitCode!==null) throw Error(`WS process exited: ${child.exitCode}`);
      try { if((await fetch(url+'/socket.io/?EIO=4&transport=polling')).ok) break; } catch {}
      if(i===99) throw Error('proxied WS did not start');
      await wait(100);
    }
    assert.equal(sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE environment_id='${env}' AND subscriber_internal_id='${id}'`),'0');
    armed=true;
    const socket=io(url,{query:{token},transports:['websocket'],reconnection:false,timeout:3000});
    let disconnected=false;
    socket.on('disconnect',()=>{disconnected=true}); socket.on('connect_error',()=>{disconnected=true});
    for(let i=0;i<60 && !disconnected;i++) await wait(100);
    assert(disconnected,'connection not rejected after lost commit acknowledgement');
    assert.equal(dropped,1,'proxy did not observe the committed transaction');
    assert.equal(sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE environment_id='${env}' AND subscriber_internal_id='${id}'`),'1','registration was not committed before acknowledgement loss');
    assert.equal(sql(`SELECT is_online FROM novu_rust_subscribers WHERE environment_id='${env}' AND internal_id='${id}'`),'t');
    for(let i=0;i<100;i++) {
      const count=sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE environment_id='${env}' AND subscriber_internal_id='${id}'`);
      if(count==='0') break;
      if(i===99) throw Error('uncertain registration was not cleaned up');
      await wait(100);
    }
    assert.equal(sql(`SELECT is_online FROM novu_rust_subscribers WHERE environment_id='${env}' AND internal_id='${id}'`),'f');
    socket.disconnect();
    console.log('lost COMMIT acknowledgement after successful registration: cleanup verified');
    // The same failure on disconnect must not apply a second presence
    // transition when the cleanup transaction is retried after its commit.
    const socket2=io(url,{query:{token},transports:['websocket'],reconnection:false,timeout:3000});
    for(let i=0;i<60;i++) {
      if(sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE environment_id='${env}' AND subscriber_internal_id='${id}'`)==='1') break;
      if(i===59) throw Error('second registration did not finish');
      await wait(100);
    }
    await wait(300);
    assert(socket2.connected,'second socket did not connect');
    const beforeRetry=queryStarts;
    armed=true; socket2.disconnect();
    for(let i=0;i<60 && dropped<2;i++) await wait(100);
    assert.equal(dropped,2,'disconnect COMMIT acknowledgement was not lost');
    assert.equal(sql(`SELECT count(*) FROM novu_rust_ws_connections WHERE environment_id='${env}' AND subscriber_internal_id='${id}'`),'0');
    const beforeCommits=forwardedCommits;
    const before=sql(`SELECT is_online || ':' || last_online_at::text || ':' || updated_at::text FROM novu_rust_subscribers WHERE environment_id='${env}' AND internal_id='${id}'`);
    assert.match(before,/^false:/);
    for(let i=0;i<100 && queryStarts===beforeRetry;i++) await wait(100);
    assert(queryStarts>beforeRetry,'disconnect cleanup did not reconnect');
    for(let i=0;i<100 && forwardedCommits===beforeCommits;i++) await wait(100);
    assert(forwardedCommits>beforeCommits,'disconnect cleanup retry did not commit');
    assert.equal(sql(`SELECT is_online || ':' || last_online_at::text || ':' || updated_at::text FROM novu_rust_subscribers WHERE environment_id='${env}' AND internal_id='${id}'`),before,'retry repeated the presence transition');
    console.log('lost COMMIT acknowledgement after disconnect: idempotent retry verified');
  } finally {
    if(child) {child.kill('SIGTERM'); await new Promise(resolve=>{if(child.exitCode!==null) resolve();else child.once('exit',resolve)});}
    for(const peer of peers) peer.destroy();
    await new Promise(resolve=>proxy.close(resolve));
  }
})().catch(error=>{console.error(error);process.exit(1)});
