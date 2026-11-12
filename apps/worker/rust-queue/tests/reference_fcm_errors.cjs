// Exercise the pinned firebase-admin error conversion and Novu's actual
// FCM provider classifier without sending to Firebase.
const path=require('path');
const {createRequire}=require('module');
const root=process.env.NOVU_REFERENCE_SOURCE;
const providerFile=path.join(root,'packages/providers/dist/cjs/lib/push/fcm/fcm.provider.js');
const r=createRequire(providerFile);
const messagingDir=path.dirname(r.resolve('firebase-admin/messaging'));
const {createFirebaseError}=require(path.join(messagingDir,'messaging-errors-internal.js'));
const {FcmPushProvider}=require(providerFile);
const phrase='Requested entity was not found';
const type='type.googleapis.com/google.firebase.fcm.v1.FcmError';
const bodies=[
  JSON.stringify({error:{status:'NOT_FOUND',message:phrase+'.'}}),
  JSON.stringify({error:{status:'NOT_FOUND',message:'Other error',details:[{reason:phrase}]}}),
  JSON.stringify({error:{status:'NOT_FOUND',details:[{'@type':type,errorCode:'UNREGISTERED',reason:phrase}]}}),
  JSON.stringify({error:{status:'NOT_FOUND',details:[{'@type':type,errorCode:'NEW_CODE',reason:phrase}]}}),
  JSON.stringify({error:{status:'NOT_FOUND',details:[{'@type':type,reason:phrase}]}}),
  JSON.stringify({error:{status:'NEW_CODE',message:'Other error',details:[{reason:phrase}]}}),
  JSON.stringify({error:{status:'UNSPECIFIED_ERROR',message:'Other error',details:[{reason:phrase}]}}),
  JSON.stringify({error:{status:'NOT_FOUND'}}),
  JSON.stringify({error:phrase}),
  JSON.stringify({error:'NOT_FOUND',extra:phrase}),
  JSON.stringify({error:{status:null,message:'Other error'},extra:phrase}),
  JSON.stringify({message:phrase}),
  JSON.stringify(phrase),
  phrase+'.',
  '<html>generic unavailable</html>',
  '',
];
const results=bodies.map(body=>{
  let data;let isJson=false;
  try { data=JSON.parse(body);isJson=true; } catch {}
  const response={status:404,text:body,data,isJson:()=>isJson};
  const error=createFirebaseError({response});
  return {body,invalid:FcmPushProvider.prototype.isTokenInvalid.call({INVALID_TOKEN_ERRORS:[phrase]},error.message)};
});
process.stdout.write(JSON.stringify(results));
