const fs=require('fs');
const path=require('path');
const {createRequire}=require('module');
const r=createRequire(path.join(process.env.NOVU_REFERENCE_SOURCE,'packages/providers/dist/cjs/lib/push/fcm/fcm.provider.js'));
const adminPath=r.resolve('firebase-admin/app');
const authRequire=createRequire(adminPath);
const tokenRequire=createRequire(authRequire.resolve('google-auth-library'));
const {GoogleToken}=tokenRequire('gtoken');
const key=fs.readFileSync(0,'utf8');
const scope=['https://www.googleapis.com/auth/cloud-platform','https://www.googleapis.com/auth/firebase.database','https://www.googleapis.com/auth/firebase.messaging','https://www.googleapis.com/auth/identitytoolkit','https://www.googleapis.com/auth/userinfo.email'];
const token=new GoogleToken({email:'fixture@example.test',key,scope});
token.transporter.request=async request=>{
  const [header,claims]=request.data.assertion.split('.').slice(0,2).map(v=>JSON.parse(Buffer.from(v,'base64url').toString()));
  process.stdout.write(JSON.stringify({header,claims,grant:request.data.grant_type,url:request.url}));
  return {data:{access_token:'fixture-token',expires_in:3600,token_type:'Bearer'}};
};
token.getToken().catch(e=>{console.error(e);process.exit(1)});
