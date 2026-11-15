// Disposable loopback-only OIDC fixture. Never use as an identity provider.
import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import {createHash,randomBytes,randomUUID} from 'node:crypto';
import {SignJWT,importPKCS8,importSPKI,exportJWK} from 'jose';

const issuer='http://127.0.0.1:4251';
const origin='http://127.0.0.1:4249';
const callback=`${origin}/oidc-callback`;
const privatePem=await readFile(new URL('../../../api/rust-api/tests/fixtures/operator_jwt/test-only-private.pem',import.meta.url),'utf8');
const publicPem=await readFile(new URL('../../../api/rust-api/tests/fixtures/operator_jwt/test-only-public.pem',import.meta.url),'utf8');
const privateKey=await importPKCS8(privatePem,'RS256');
const jwk=await exportJWK(await importSPKI(publicPem,'RS256'));
Object.assign(jwk,{kid:'key',use:'sig',alg:'RS256'});
const codes=new Map();
const server=createServer(async(req,res)=>{
  res.setHeader('Access-Control-Allow-Origin',origin);
  res.setHeader('Access-Control-Allow-Methods','GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  res.setHeader('Cache-Control','no-store');
  if(req.method==='OPTIONS'){res.writeHead(204).end();return;}
  const url=new URL(req.url,issuer);
  const json=(value,status=200)=>{res.writeHead(status,{'Content-Type':'application/json'}).end(JSON.stringify(value));};
  if(req.method==='GET' && url.pathname==='/.well-known/openid-configuration'){
    json({issuer,authorization_endpoint:`${issuer}/authorize`,token_endpoint:`${issuer}/token`,jwks_uri:`${issuer}/jwks`,response_types_supported:['code'],subject_types_supported:['public'],id_token_signing_alg_values_supported:['RS256'],code_challenge_methods_supported:['S256'],token_endpoint_auth_methods_supported:['none']});return;
  }
  if(req.method==='GET' && url.pathname==='/jwks'){json({keys:[jwk]});return;}
  if(req.method==='GET' && url.pathname==='/authorize'){
    const p=url.searchParams;
    if(p.get('client_id')!=='console'||p.get('redirect_uri')!==callback||p.get('response_type')!=='code'||p.get('code_challenge_method')!=='S256'||!p.get('code_challenge')||!p.get('state')||!p.get('nonce')){json({error:'invalid_request'},400);return;}
    const code=randomBytes(24).toString('base64url');
    codes.set(code,{challenge:p.get('code_challenge'),nonce:p.get('nonce'),created:Date.now()});
    const redirect=new URL(callback);redirect.searchParams.set('code',code);redirect.searchParams.set('state',p.get('state'));
    res.writeHead(302,{Location:redirect.toString()}).end();return;
  }
  if(req.method==='POST' && url.pathname==='/token'){
    let raw='';for await(const chunk of req){raw+=chunk;if(raw.length>8192){json({error:'invalid_request'},400);return;}}
    const p=new URLSearchParams(raw);const saved=codes.get(p.get('code'));codes.delete(p.get('code'));
    const challenge=createHash('sha256').update(p.get('code_verifier')||'').digest('base64url');
    if(!saved||Date.now()-saved.created>60000||p.get('client_id')!=='console'||p.get('redirect_uri')!==callback||p.get('grant_type')!=='authorization_code'||challenge!==saved.challenge){json({error:'invalid_grant'},400);return;}
    const now=Math.floor(Date.now()/1000);
    const access=await new SignJWT({client_id:'console',jti:randomUUID()}).setProtectedHeader({alg:'RS256',typ:'at+jwt',kid:'key'}).setIssuer(issuer).setSubject('ui-fixture').setAudience('swallow').setIssuedAt(now).setExpirationTime(now+600).sign(privateKey);
    const id=await new SignJWT({nonce:saved.nonce}).setProtectedHeader({alg:'RS256',kid:'key'}).setIssuer(issuer).setSubject('ui-fixture').setAudience('console').setIssuedAt(now).setExpirationTime(now+600).sign(privateKey);
    json({access_token:access,id_token:id,token_type:'Bearer',expires_in:600,scope:'openid'});return;
  }
  json({error:'not_found'},404);
});
server.listen(4251,'127.0.0.1',()=>console.log('Test OIDC issuer listening on 127.0.0.1:4251'));
