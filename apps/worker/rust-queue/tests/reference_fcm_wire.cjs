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
];
for(const message of messages) validateMessage(message);
process.stdout.write(JSON.stringify(messages.map(message=>({message}))));
