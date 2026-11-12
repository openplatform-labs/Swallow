// Disposable reference worker only: replace FCM's Firebase SDK at its module boundary.
const fs=require('fs');
const Module=require('module');
const load=Module._load;
Module._load=function(name,parent,isMain) {
  if (parent?.filename?.endsWith('/lib/push/fcm/fcm.provider.js')) {
    if (name==='firebase-admin/app') return {cert:()=>({}),initializeApp:()=>({}),getApp:()=>({}),deleteApp:async()=>{}};
    if (name==='firebase-admin/messaging') return {getMessaging:()=>({
      send:async message=>{fs.appendFileSync('/tmp/novu-push-capture.jsonl',JSON.stringify(message)+'\n');return 'mock-id'},
      sendEachForMulticast:async message=>{fs.appendFileSync('/tmp/novu-push-capture.jsonl',JSON.stringify(message)+'\n');if(message.tokens.includes('device-fail')) return {successCount:0,responses:[{success:false,error:{message:'fixture FCM error'}}]};return {successCount:message.tokens.length,responses:message.tokens.map(()=>({success:true,messageId:'mock-id'}))}}
    })};
  }
  return load.call(this,name,parent,isMain);
};
