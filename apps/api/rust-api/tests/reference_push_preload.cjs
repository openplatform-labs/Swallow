// Disposable reference worker only: replace FCM's Firebase SDK at its module boundary.
const fs=require('fs');
const Module=require('module');
const load=Module._load;
Module._load=function(name,parent,isMain) {
  if (parent?.filename?.endsWith('/lib/push/fcm/fcm.provider.js')) {
    if (name==='firebase-admin/app') return {cert:()=>({}),initializeApp:()=>({}),getApp:()=>({}),deleteApp:async()=>{}};
    if (name==='firebase-admin/messaging') return {getMessaging:()=>({
      send:async message=>{fs.appendFileSync('/tmp/novu-push-capture.jsonl',JSON.stringify(message)+'\n');return 'mock-id'},
      sendEachForMulticast:async message=>{fs.appendFileSync('/tmp/novu-push-capture.jsonl',JSON.stringify(message)+'\n');const barrier=message.tokens.find(token=>token.startsWith('device-invalid-barrier-'));if(barrier){const release='/tmp/novu-release-'+barrier;const until=Date.now()+90000;while(!fs.existsSync(release)){if(Date.now()>until)throw new Error('fixture barrier timeout');await new Promise(resolve=>setTimeout(resolve,50));}}if(message.tokens.includes('device-fail')||message.tokens.includes('device-known-meta')) return {successCount:0,responses:[{success:false,error:{message:'fixture FCM error'}}]};if(message.tokens.some(token=>token.startsWith('device-invalid'))) return {successCount:0,responses:[{success:false,error:{message:'Requested entity was not found.'}}]};return {successCount:message.tokens.length,responses:message.tokens.map(()=>({success:true,messageId:'mock-id'}))}}
    })};
  }
  return load.call(this,name,parent,isMain);
};
