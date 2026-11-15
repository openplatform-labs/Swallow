// Disposable local fixture self-test; all request bodies are synthetic.
import {spawn} from 'node:child_process';
import {setTimeout as delay} from 'node:timers/promises';
import assert from 'node:assert/strict';

const child=spawn(process.execPath,[new URL('./test-oidc-provider.mjs',import.meta.url).pathname],{
  env:{...process.env,SWALLOW_TEST_OIDC_CROSS_REDIRECT:'true',SWALLOW_TEST_OIDC_TOKEN_REDIRECT:'true'},
  stdio:['ignore','pipe','pipe'],
});
let stderr='',stdout='';child.stderr.on('data',chunk=>{stderr=(stderr+chunk.toString()).slice(-2048);});child.stdout.on('data',chunk=>{stdout=(stdout+chunk.toString()).slice(-2048);});
const base='http://127.0.0.1:4251';
async function post(path,options={}) {return fetch(`${base}${path}`,{method:'POST',...options});}
async function observations(){return (await fetch(`${base}/fixture/redirect-observations`)).json();}
try {
  let ready=false;
  for(let i=0;i<100;i++){
    if(child.exitCode!==null || child.signalCode!==null)throw new Error(`fixture exited: ${stderr}`);
    if(stdout.includes('Test OIDC issuer listening') && stdout.includes('Cross-origin test receiver listening')){ready=true;break;}
    await delay(50);
  }
  assert.ok(ready,'fixture did not start');
  const headers={'Content-Type':'application/x-www-form-urlencoded'};
  await post('/token',{headers,body:'code=fixture'});
  assert.deepEqual(await observations(),{attempts:1,leaks:0,crossOriginLeaks:1});
  await post('/fixture/reset-observations');
  for(const [status,body] of [[307,'code=fixture'],[308,'refresh_token=fixture']]){
    await post(`/fixture/token-redirect?enabled=true&status=${status}`);
    await assert.rejects(post('/token',{headers,body,redirect:'error'}));
  }
  assert.deepEqual(await observations(),{attempts:2,leaks:0,crossOriginLeaks:0});
  console.log('Cross-origin fixture: baseline follows redirect; 307 and 308 with redirect:error are blocked.');
} finally {
  child.kill('SIGTERM');
  if(child.exitCode===null && child.signalCode===null)await new Promise(resolve=>child.once('exit',resolve));
}
