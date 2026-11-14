const path=require('path');
const {createRequire}=require('module');
const r=createRequire(path.join(process.env.NOVU_REFERENCE_SOURCE,'packages/providers/dist/cjs/lib/push/fcm/fcm.provider.js'));
const entry=r.resolve('firebase-admin/messaging');
const {validateMessage}=require(path.join(path.dirname(entry),'messaging-internal.js'));
const messages=[
  {token:'device-a',notification:{title:'Hi',body:'Hello',imageUrl:'https://example.test/a.png'},data:{key:'value'}},
  {topic:'orders',data:{title:'Hi',message:'Hello'}},
  {condition:"'a' in topics",notification:{title:'Hi',body:'Hello'},data:{}},
  {token:'device-b',notification:{title:'Hi',body:'Hello'},data:{}},
  {token:'device-a',android:{priority:'high',ttl:1501,collapseKey:'orders',restrictedPackageName:'com.example',directBootOk:true,data:{kind:'update'}}},
  {topic:'news',webpush:{headers:{Urgency:'high'},data:{key:'value'},notification:{title:'News',icon:'https://example.test/icon.png'},fcmOptions:{link:'https://example.test/'}},fcmOptions:{analyticsLabel:'campaign'}},
  {token:'device-b',android:{ttl:0,priority:'normal'},webpush:{headers:{TTL:'60'}}},
  {token:'ios-a',apns:{headers:{'apns-priority':'10'},payload:{aps:{alert:{title:'Hi',locKey:'KEY',locArgs:['one'],titleLocKey:'TITLE',titleLocArgs:['two'],launchImage:'splash'},badge:2,sound:{name:'alert.aiff',critical:true,volume:0.5},contentAvailable:true,mutableContent:false,threadId:'thread'},custom:{order:5}},fcmOptions:{imageUrl:'https://example.test/image.png',analyticsLabel:'campaign'}}},
  {token:'ios-b',apns:{payload:{aps:{alert:'Hi',sound:'default',contentAvailable:false,mutableContent:true}},headers:{'apns-push-type':'alert'}}},
  {token:'android-a',android:{notification:{title:'Hello',body:'World',icon:'bell',sound:'default',color:'#AAbbCC',tag:'tag',clickAction:'OPEN',channelId:'orders',bodyLocKey:'BODY',bodyLocArgs:['one'],titleLocKey:'TITLE',titleLocArgs:['two'],imageUrl:'https://example.test/img.png',localOnly:true,priority:'high',visibility:'public',proxy:'allow',vibrateTimingsMillis:[0,1501],defaultVibrateTimings:true,defaultSound:false,defaultLightSettings:true,notificationCount:3}}},
  {token:'led-a',android:{notification:{lightSettings:{color:'#FF0080',lightOnDurationMillis:1001,lightOffDurationMillis:0}}}},
  {token:'led-b',android:{notification:{lightSettings:{color:'#11223344',lightOnDurationMillis:0,lightOffDurationMillis:2500}}}},
];
for(const message of messages) validateMessage(message);
process.stdout.write(JSON.stringify(messages.map(message=>({message}))));
