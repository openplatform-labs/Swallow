// Drop the complete SELECT response only after PostgreSQL reports an idle
// transaction, then verify the committed deletion and idempotent recovery.
const net = require('node:net');
const { spawn, spawnSync } = require('node:child_process');
const assert = require('node:assert/strict');
const port = Number(process.env.RETENTION_PG_PORT);
const binary = process.env.RETENTION_BINARY;
const psql = process.env.RETENTION_PSQL;
const id = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd';
const peers = new Set();
let dropped = false, completed = false;
const server = net.createServer(front => {
  const back = net.connect(port, '127.0.0.1');
  peers.add(front); peers.add(back);
  front.on('error', () => back.destroy()); back.on('error', () => front.destroy());
  front.on('close', () => { peers.delete(front); back.destroy(); });
  back.on('close', () => { peers.delete(back); front.destroy(); });
  let input = Buffer.alloc(0), output = Buffer.alloc(0), startup = true, target = false, suppress = false;
  front.on('data', chunk => {
    input = Buffer.concat([input, chunk]);
    while (input.length >= (startup ? 4 : 5)) {
      const size = startup ? input.readUInt32BE(0) : 1 + input.readUInt32BE(1);
      if (size < 4 || size > 16 * 1024 * 1024) { front.destroy(); return; }
      if (input.length < size) return;
      const frame = input.subarray(0, size); input = input.subarray(size);
      if (startup) startup = false;
      else {
        if (frame[0] === 0x50 && frame.includes(Buffer.from('novu_rust_ws_retention_delete_v2'))) target = true;
        if (target && frame[0] === 0x45) suppress = true;
      }
      back.write(frame);
    }
  });
  back.on('data', chunk => {
    output = Buffer.concat([output, chunk]);
    while (output.length >= 5) {
      const size = 1 + output.readUInt32BE(1);
      if (size < 5 || size > 16 * 1024 * 1024) { back.destroy(); return; }
      if (output.length < size) return;
      const frame = output.subarray(0, size); output = output.subarray(size);
      if (suppress) {
        if (frame[0] === 0x43 && frame.subarray(5).toString() === 'SELECT 1\0') completed = true;
        if (frame[0] === 0x5a && frame[5] === 0x49) { dropped = true; front.destroy(); back.destroy(); return; }
      } else front.write(frame);
    }
  });
});
function sql(query) {
  const r = spawnSync(psql, [`postgres://postgres@127.0.0.1:${port}/retention_test`, '-qAt', '-v', 'ON_ERROR_STOP=1', '-c', query], { encoding: 'utf8' });
  assert.equal(r.status, 0, r.stderr); return r.stdout.trim();
}
function run(args, targetPort) {
  const child = spawn(binary, args, {env:{...process.env,DATABASE_URL:`postgres://ws_retention_test@127.0.0.1:${targetPort}/retention_test`,NOVU_POSTGRES_ALLOW_INSECURE:'true'},stdio:['ignore','pipe','pipe']});
  let out='',err=''; child.stdout.on('data',x=>out+=x); child.stderr.on('data',x=>err+=x);
  return new Promise((resolve,reject) => {const timer=setTimeout(()=>{child.kill('SIGKILL');reject(Error('CLI timeout'));},10000);child.once('error',e=>{clearTimeout(timer);reject(e)});child.once('close',code=>{clearTimeout(timer);resolve({code,out,err})});});
}
(async () => {
  await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve)});
  try {
    const args=['delete-v2','tenant-a','2024-01-01T00:00:00Z','1',id,'lost acknowledgement'];
    const failed = await run(args,server.address().port);
    assert(dropped && completed, 'proxy did not suppress a completed, committed statement');
    assert.notEqual(failed.code, 0); assert.equal(failed.out.trim(), '');
    assert.equal(sql(`SELECT count(*) FROM public.novu_rust_ws_retention_requests WHERE request_id='${id}' AND result IS NOT NULL`),'1');
    assert.equal(sql(`SELECT count(*) FROM public.novu_rust_ws_retention_audit WHERE request_id='${id}'`),'1');
    assert.equal(sql("SELECT count(*) FROM public.novu_rust_ws_events WHERE environment_id='tenant-a' AND created_at<'2024-01-01'"),'1');
    const retry=await run(args,port); assert.equal(retry.code,0,retry.err); const result=JSON.parse(retry.out);
    assert.equal(result.replayed,true); assert.equal(result.count,1); assert.equal(result.requestId,id);
    assert.equal(sql("SELECT count(*) FROM public.novu_rust_ws_events WHERE environment_id='tenant-a' AND created_at<'2024-01-01'"),'1');
    assert.equal(sql(`SELECT count(*) FROM public.novu_rust_ws_retention_audit WHERE request_id='${id}'`),'1');
    console.log('PostgreSQL committed; lost response and idempotent CLI retry verified');
  } finally {for(const peer of peers)peer.destroy();await new Promise(resolve=>server.close(resolve));}
})().catch(error=>{console.error(error);process.exitCode=1});
