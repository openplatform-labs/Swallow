// Capture the actual original provider's firebase-admin calls without network or credentials.
const Module = require('module');
const path = require('path');
const load = Module._load;
let current;
Module._load = function(name, parent, isMain) {
  if (name === 'firebase-admin/app') return {cert:()=>({}), initializeApp:()=>({}),getApp:()=>({}),deleteApp:async()=>{}};
  if (name === 'firebase-admin/messaging') return {getMessaging:()=>({
    send:async message=>{current={multicast:false,message};return 'mock-id'},
    sendEachForMulticast:async message=>{current={multicast:true,message};return {successCount:1,responses:[{success:true,messageId:'mock-id'}]}}
  })};
  return load.call(this,name,parent,isMain);
};
const {FcmPushProvider}=require(path.join(process.env.NOVU_REFERENCE_SOURCE,'packages/providers/dist/cjs/lib/push/fcm/fcm.provider.js'));
const cases = [
  [{title:'Hi',content:'Hello',target:['t1','t2'],payload:{other:'ignored',__nvMessageId:'m1'}},{}],
  [{title:'Hi',content:'Hello',target:['t1'],payload:{a:1,flag:true,nested:{x:2}},overrides:{type:'data',data:{extra:'value',title:'overwritten'}}},{}],
  [{title:'Hi',content:'Hello',target:['t1'],payload:{},overrides:{badge:3,webPush:{headers:{Urgency:'high'}}}},{notification:{title:'Provider'},apns:{headers:{'apns-priority':'10'}}}],
  [{title:'Hi',content:'Hello',target:['t1'],payload:{},overrides:{badge:3}},{token:'one',topic:'orders'}],
  [{title:'Hi',content:'Hello',target:['t1'],payload:{}},{tokens:['a','',null,'b'],_passthrough:{body:{topic:'broadcast',notification:{body:'override'}}}}],
  [{title:'Hi',content:'Hello',target:['t1'],payload:{}},{token:'',tokens:['x','y'],_passthrough:{body:{topic:''}}}],
  [{title:'Hi',content:'Hello',target:['t1'],payload:{}},{condition:"'a' in topics",data:{key:'v'},android:{notification:{vibrateTimingsMillis:[1,2]}},_passthrough:{body:{android:{notification:{vibrateTimingsMillis:[3]}}}}}],
];
(async()=>{const results=[];for(const [options,data] of cases){current=undefined;await new FcmPushProvider({projectId:'fake',email:'fake',secretKey:'fake'}).sendMessage(options,data);results.push(current)}process.stdout.write(JSON.stringify(results))})().catch(e=>{console.error(e);process.exit(1)});
