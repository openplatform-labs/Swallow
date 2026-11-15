import { afterEach, describe, expect, it, vi } from 'vitest';
import { OperatorOidc, type OidcSettings } from './operator-oidc';
import { UserManager, type User } from 'oidc-client-ts';
import { SignJWT, exportJWK, generateKeyPair } from 'jose';

const settings: OidcSettings={issuer:'https://issuer.test',clientId:'console',scope:'openid',resource:'',audience:'',apiOrigin:'https://api.test',environmentId:'env'};
const accessToken=`${btoa('{}')}.${btoa(JSON.stringify({exp:Math.floor(Date.now()/1000)+600}))}.signature`;
const metadata={issuer:'https://issuer.test',authorization_endpoint:'https://issuer.test/authorize',token_endpoint:'https://issuer.test/token',jwks_uri:'https://issuer.test/jwks',code_challenge_methods_supported:['S256']};
describe('operator OIDC configuration',()=>{
  afterEach(()=>vi.restoreAllMocks());
  it('accepts trusted HTTPS discovery advertising S256 without storing credentials',async()=>{
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValue(new Response(JSON.stringify(metadata),{status:200}));
    const prepared=await new OperatorOidc().prepare(settings);
    expect(prepared.metadata.issuer).toBe(settings.issuer);
    expect(fetchMock).toHaveBeenCalledWith('https://issuer.test/.well-known/openid-configuration',expect.objectContaining({credentials:'omit',redirect:'error',cache:'no-store'}));
  });
  it('rejects issuer mismatch, missing S256, insecure endpoints and refresh-token scope',async()=>{
    const fetchMock=vi.spyOn(globalThis,'fetch');
    const service=new OperatorOidc();
    for (const bad of [{...metadata,issuer:'https://other.test'},{...metadata,code_challenge_methods_supported:['plain']},{...metadata,token_endpoint:'http://external.test/token'}]) {
      fetchMock.mockResolvedValueOnce(new Response(JSON.stringify(bad),{status:200}));
      await expect(service.prepare(settings)).rejects.toThrow();
    }
    await expect(service.prepare({...settings,scope:'openid offline_access'})).rejects.toThrow();
    await expect(service.prepare({...settings,issuer:'http://external.test'})).rejects.toThrow();
  });
  it('rejects oversized and invalid UTF-8 discovery while reading the stream',async()=>{
    let cancelled=false;
    const body=new ReadableStream<Uint8Array>({pull(controller){controller.enqueue(new Uint8Array(40000).fill(97));},cancel(){cancelled=true;}});
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(body)).mockResolvedValueOnce(new Response(new Uint8Array([0xff]))).mockResolvedValueOnce(new Response('{}',{headers:{'content-length':'65537'}}));
    const service=new OperatorOidc();
    await expect(service.prepare(settings)).rejects.toThrow('response too large');expect(cancelled).toBe(true);
    await expect(service.prepare(settings)).rejects.toThrow();await expect(service.prepare(settings)).rejects.toThrow('response too large');expect(fetchMock).toHaveBeenCalledTimes(3);
  });
  it('bounds signing keys and operator identity responses',async()=>{
    const {privateKey,publicKey}=await generateKeyPair('RS256');const jwk={...(await exportJWK(publicKey)),kid:'test',alg:'RS256',use:'sig'};
    vi.spyOn(UserManager.prototype,'signinPopup').mockImplementation(async args=>({expired:false,token_type:'Bearer',access_token:accessToken,id_token:await new SignJWT({nonce:args?.nonce}).setProtectedHeader({alg:'RS256',kid:'test'}).setIssuer(settings.issuer).setAudience(settings.clientId).setSubject('alice').setIssuedAt().setExpirationTime('1m').sign(privateKey)} as User));
    const remove=vi.spyOn(UserManager.prototype,'removeUser').mockResolvedValue();
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response('x'.repeat(262145))).mockResolvedValueOnce(new Response(JSON.stringify({keys:[jwk]}))).mockResolvedValueOnce(new Response('x'.repeat(16385)));
    const service=new OperatorOidc();await expect(service.signIn({settings,metadata})).rejects.toThrow('response too large');await expect(service.signIn({settings,metadata})).rejects.toThrow('response too large');
    expect(fetchMock).toHaveBeenCalledTimes(3);expect(remove).toHaveBeenCalledTimes(2);
  });
  it('requires explicit opt-in for refresh tokens and verifies the renewed identity',async()=>{
    const {privateKey,publicKey}=await generateKeyPair('RS256');const jwk={...(await exportJWK(publicKey)),kid:'test',alg:'RS256',use:'sig'};
    vi.spyOn(UserManager.prototype,'signinPopup').mockImplementation(async args=>({expired:false,token_type:'Bearer',access_token:accessToken,refresh_token:'secret-refresh',id_token:await new SignJWT({nonce:args?.nonce}).setProtectedHeader({alg:'RS256',kid:'test'}).setIssuer(settings.issuer).setAudience(settings.clientId).setSubject('alice').setIssuedAt().setExpirationTime('1m').sign(privateKey)} as User));
    const silent=vi.spyOn(UserManager.prototype,'signinSilent').mockResolvedValue({expired:false,token_type:'Bearer',access_token:accessToken,refresh_token:'rotated-secret'} as User);
    vi.spyOn(UserManager.prototype,'removeUser').mockResolvedValue();
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify(metadata))).mockResolvedValueOnce(new Response(JSON.stringify({keys:[jwk]}))).mockResolvedValueOnce(new Response(JSON.stringify({issuer:settings.issuer,subject:'alice',clientId:'console',environmentId:'env'}))).mockResolvedValueOnce(new Response(JSON.stringify({issuer:settings.issuer,subject:'mallory',clientId:'console',environmentId:'env'})));
    const service=new OperatorOidc();const prepared=await service.prepare({...settings,allowRefresh:true,resource:'https://api.test/'});expect(prepared.settings.scope).toBe('openid offline_access');await service.signIn(prepared);
    await expect(service.renew()).rejects.toThrow('identity mismatch');await expect(service.renew()).rejects.toThrow('refresh unavailable');expect(silent).toHaveBeenCalledOnce();expect(silent).toHaveBeenCalledWith(expect.objectContaining({resource:'https://api.test/'}));expect(fetchMock).toHaveBeenCalledTimes(4);
  });
  it('revokes an access token at the discovered endpoint without requiring a refresh token',async()=>{
    const service=new OperatorOidc(),prepared={settings,metadata:{...metadata,revocation_endpoint:'https://issuer.test/revoke'}};
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response('',{status:200}));expect(service.canRevokeAccess(prepared)).toBe(true);await service.revokeAccess(prepared,accessToken);
    const [url,options]=mock.mock.calls[0];expect(url).toBe('https://issuer.test/revoke');expect(options).toEqual(expect.objectContaining({method:'POST',credentials:'omit',redirect:'error',cache:'no-store'}));expect(Object.fromEntries(options!.body as URLSearchParams)).toEqual({token:accessToken,token_type_hint:'access_token',client_id:'console'});
    const unsafe={settings,metadata:{...metadata,revocation_endpoint:'http://127.0.0.1:9000/revoke'}};expect(service.canRevokeAccess(unsafe)).toBe(false);await expect(service.revokeAccess(unsafe,accessToken)).rejects.toThrow();expect(mock).toHaveBeenCalledOnce();
  });
  it('revokes the refresh token only at the validated provider endpoint',async()=>{
    const {privateKey,publicKey}=await generateKeyPair('RS256');const jwk={...(await exportJWK(publicKey)),kid:'test',alg:'RS256',use:'sig'};
    vi.spyOn(UserManager.prototype,'signinPopup').mockImplementation(async args=>({expired:false,token_type:'Bearer',access_token:accessToken,refresh_token:'private-refresh',id_token:await new SignJWT({nonce:args?.nonce}).setProtectedHeader({alg:'RS256',kid:'test'}).setIssuer(settings.issuer).setAudience(settings.clientId).setSubject('alice').setIssuedAt().setExpirationTime('1m').sign(privateKey)} as User));
    vi.spyOn(UserManager.prototype,'getUser').mockResolvedValue({refresh_token:'private-refresh'} as User);vi.spyOn(UserManager.prototype,'removeUser').mockResolvedValue();
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({keys:[jwk]}))).mockResolvedValueOnce(new Response(JSON.stringify({issuer:settings.issuer,subject:'alice',clientId:'console',environmentId:'env'}))).mockResolvedValueOnce(new Response('',{status:200}));
    const service=new OperatorOidc();await service.signIn({settings:{...settings,allowRefresh:true},metadata:{...metadata,revocation_endpoint:'https://issuer.test/revoke'}});expect(service.canRevokeRefresh()).toBe(true);await service.revokeRefresh();
    expect(fetchMock.mock.calls[2][0]).toBe('https://issuer.test/revoke');const options=fetchMock.mock.calls[2][1]!;expect(options).toEqual(expect.objectContaining({method:'POST',credentials:'omit',cache:'no-store',redirect:'error'}));expect(Object.fromEntries(options.body as URLSearchParams)).toEqual({token:'private-refresh',token_type_hint:'refresh_token',client_id:'console'});service.dispose();
  });
  it('prevents token refresh while provider revocation is pending',async()=>{
    const {privateKey,publicKey}=await generateKeyPair('RS256');const jwk={...(await exportJWK(publicKey)),kid:'test',alg:'RS256',use:'sig'};
    vi.spyOn(UserManager.prototype,'signinPopup').mockImplementation(async args=>({expired:false,token_type:'Bearer',access_token:accessToken,refresh_token:'private-refresh',id_token:await new SignJWT({nonce:args?.nonce}).setProtectedHeader({alg:'RS256',kid:'test'}).setIssuer(settings.issuer).setAudience(settings.clientId).setSubject('alice').setIssuedAt().setExpirationTime('1m').sign(privateKey)} as User));
    vi.spyOn(UserManager.prototype,'getUser').mockResolvedValue({refresh_token:'private-refresh'} as User);vi.spyOn(UserManager.prototype,'removeUser').mockResolvedValue();const silent=vi.spyOn(UserManager.prototype,'signinSilent');
    let finish!:(response:Response)=>void;vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({keys:[jwk]}))).mockResolvedValueOnce(new Response(JSON.stringify({issuer:settings.issuer,subject:'alice',clientId:'console',environmentId:'env'}))).mockReturnValueOnce(new Promise(done=>finish=done));
    const service=new OperatorOidc();await service.signIn({settings:{...settings,allowRefresh:true},metadata:{...metadata,revocation_endpoint:'https://issuer.test/revoke'}});const pending=service.revokeRefresh();await Promise.resolve();await expect(service.renew()).rejects.toThrow('refresh unavailable');expect(silent).not.toHaveBeenCalled();finish(new Response('',{status:200}));await pending;service.dispose();
  });
  it('disables provider revocation for an insecure endpoint',async()=>{
    const {privateKey,publicKey}=await generateKeyPair('RS256');const jwk={...(await exportJWK(publicKey)),kid:'test',alg:'RS256',use:'sig'};
    vi.spyOn(UserManager.prototype,'signinPopup').mockImplementation(async args=>({expired:false,token_type:'Bearer',access_token:accessToken,refresh_token:'private-refresh',id_token:await new SignJWT({nonce:args?.nonce}).setProtectedHeader({alg:'RS256',kid:'test'}).setIssuer(settings.issuer).setAudience(settings.clientId).setSubject('alice').setIssuedAt().setExpirationTime('1m').sign(privateKey)} as User));
    vi.spyOn(UserManager.prototype,'removeUser').mockResolvedValue();vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({keys:[jwk]}))).mockResolvedValueOnce(new Response(JSON.stringify({issuer:settings.issuer,subject:'alice',clientId:'console',environmentId:'env'})));
    const service=new OperatorOidc();await service.signIn({settings:{...settings,allowRefresh:true},metadata:{...metadata,revocation_endpoint:'http://127.0.0.1:9000/revoke'}});expect(service.canRevokeRefresh()).toBe(false);await expect(service.revokeRefresh()).rejects.toThrow();service.dispose();
  });
  it('rejects an opted-in login if the provider does not issue a refresh token',async()=>{
    const {privateKey,publicKey}=await generateKeyPair('RS256');const jwk={...(await exportJWK(publicKey)),kid:'test',alg:'RS256',use:'sig'};
    vi.spyOn(UserManager.prototype,'signinPopup').mockImplementation(async args=>({expired:false,token_type:'Bearer',access_token:accessToken,id_token:await new SignJWT({nonce:args?.nonce}).setProtectedHeader({alg:'RS256',kid:'test'}).setIssuer(settings.issuer).setAudience(settings.clientId).setSubject('alice').setIssuedAt().setExpirationTime('1m').sign(privateKey)} as User));
    const remove=vi.spyOn(UserManager.prototype,'removeUser').mockResolvedValue();vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({keys:[jwk]}))).mockResolvedValueOnce(new Response(JSON.stringify({issuer:settings.issuer,subject:'alice',clientId:'console',environmentId:'env'})));
    const service=new OperatorOidc();await expect(service.signIn({settings:{...settings,allowRefresh:true},metadata})).rejects.toThrow('refresh token unavailable');expect(remove).toHaveBeenCalled();await expect(service.renew()).rejects.toThrow('refresh unavailable');
  });
  it('discards a refresh that completes after logout',async()=>{
    const {privateKey,publicKey}=await generateKeyPair('RS256');const jwk={...(await exportJWK(publicKey)),kid:'test',alg:'RS256',use:'sig'};
    vi.spyOn(UserManager.prototype,'signinPopup').mockImplementation(async args=>({expired:false,token_type:'Bearer',access_token:accessToken,refresh_token:'secret',id_token:await new SignJWT({nonce:args?.nonce}).setProtectedHeader({alg:'RS256',kid:'test'}).setIssuer(settings.issuer).setAudience(settings.clientId).setSubject('alice').setIssuedAt().setExpirationTime('1m').sign(privateKey)} as User));
    let finish!:(user:User)=>void;vi.spyOn(UserManager.prototype,'signinSilent').mockReturnValue(new Promise(done=>finish=done));const remove=vi.spyOn(UserManager.prototype,'removeUser').mockResolvedValue();
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({keys:[jwk]}))).mockResolvedValueOnce(new Response(JSON.stringify({issuer:settings.issuer,subject:'alice',clientId:'console',environmentId:'env'})));
    const service=new OperatorOidc();await service.signIn({settings:{...settings,allowRefresh:true},metadata});const pending=service.renew();service.dispose();finish({expired:false,token_type:'Bearer',access_token:accessToken,refresh_token:'late'} as User);await expect(pending).rejects.toThrow('refresh cancelled');expect(fetchMock).toHaveBeenCalledTimes(2);expect(remove).toHaveBeenCalled();await expect(service.renew()).rejects.toThrow('refresh unavailable');
  });
  it('accepts a session only when the API-verified identity matches the selected client',async()=>{
    const {privateKey,publicKey}=await generateKeyPair('RS256');
    const jwk={...(await exportJWK(publicKey)),kid:'test',alg:'RS256',use:'sig'};
    vi.spyOn(UserManager.prototype,'signinPopup').mockImplementation(async args=>({expired:false,token_type:'Bearer',access_token:accessToken,id_token:await new SignJWT({nonce:args?.nonce}).setProtectedHeader({alg:'RS256',kid:'test'}).setIssuer('https://issuer.test').setAudience('console').setSubject('alice').setIssuedAt().setExpirationTime('1m').sign(privateKey),profile:{sub:'alice'}} as User));
    const remove=vi.spyOn(UserManager.prototype,'removeUser').mockResolvedValue();
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({keys:[jwk]}),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify({issuer:'https://issuer.test',subject:'alice',clientId:'other',environmentId:'env'}),{status:200}));
    const service=new OperatorOidc(); const prepared={settings,metadata};
    await expect(service.signIn(prepared)).rejects.toThrow('identity mismatch');
    fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({keys:[jwk]}),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify({issuer:'https://issuer.test',subject:'alice',clientId:'console',environmentId:'env'}),{status:200}));
    expect(await service.signIn(prepared)).toEqual({token:accessToken,subject:'alice',environmentId:'env',expiresAt:JSON.parse(atob(accessToken.split('.')[1])).exp});
    expect(remove).toHaveBeenCalledTimes(2);
  });
  it('rejects an identity token signed by an untrusted key before calling the API',async()=>{
    const trusted=await generateKeyPair('RS256');
    const attacker=await generateKeyPair('RS256');
    const jwk={...(await exportJWK(trusted.publicKey)),kid:'test',alg:'RS256',use:'sig'};
    vi.spyOn(UserManager.prototype,'signinPopup').mockImplementation(async args=>({expired:false,token_type:'Bearer',access_token:accessToken,id_token:await new SignJWT({nonce:args?.nonce}).setProtectedHeader({alg:'RS256',kid:'test'}).setIssuer(settings.issuer).setAudience(settings.clientId).setSubject('alice').setIssuedAt().setExpirationTime('1m').sign(attacker.privateKey)} as User));
    vi.spyOn(UserManager.prototype,'removeUser').mockResolvedValue();
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValue(new Response(JSON.stringify({keys:[jwk]}),{status:200}));
    await expect(new OperatorOidc().signIn({settings,metadata})).rejects.toThrow();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
