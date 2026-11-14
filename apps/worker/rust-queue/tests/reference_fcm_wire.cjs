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
];
for(const message of messages) validateMessage(message);
process.stdout.write(JSON.stringify(messages.map(message=>({message}))));
