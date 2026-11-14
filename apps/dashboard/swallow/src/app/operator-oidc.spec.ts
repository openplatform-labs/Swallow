import { afterEach, describe, expect, it, vi } from 'vitest';
import { OperatorOidc, type OidcSettings } from './operator-oidc';
import { UserManager, type User } from 'oidc-client-ts';
import { SignJWT, exportJWK, generateKeyPair } from 'jose';

const settings: OidcSettings={issuer:'https://issuer.test',clientId:'console',scope:'openid',resource:'',audience:'',apiOrigin:'https://api.test',environmentId:'env'};
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
  it('accepts a session only when the API-verified identity matches the selected client',async()=>{
    const {privateKey,publicKey}=await generateKeyPair('RS256');
    const jwk={...(await exportJWK(publicKey)),kid:'test',alg:'RS256',use:'sig'};
    vi.spyOn(UserManager.prototype,'signinPopup').mockImplementation(async args=>({expired:false,token_type:'Bearer',access_token:'aaa.bbb.ccc',id_token:await new SignJWT({nonce:args?.nonce}).setProtectedHeader({alg:'RS256',kid:'test'}).setIssuer('https://issuer.test').setAudience('console').setSubject('alice').setIssuedAt().setExpirationTime('1m').sign(privateKey),profile:{sub:'alice'}} as User));
    const remove=vi.spyOn(UserManager.prototype,'removeUser').mockResolvedValue();
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({keys:[jwk]}),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify({issuer:'https://issuer.test',subject:'alice',clientId:'other',environmentId:'env'}),{status:200}));
    const service=new OperatorOidc(); const prepared={settings,metadata};
    await expect(service.signIn(prepared)).rejects.toThrow('identity mismatch');
    fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({keys:[jwk]}),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify({issuer:'https://issuer.test',subject:'alice',clientId:'console',environmentId:'env'}),{status:200}));
    expect(await service.signIn(prepared)).toEqual({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env'});
    expect(remove).toHaveBeenCalledTimes(2);
  });
  it('rejects an identity token signed by an untrusted key before calling the API',async()=>{
    const trusted=await generateKeyPair('RS256');
    const attacker=await generateKeyPair('RS256');
    const jwk={...(await exportJWK(trusted.publicKey)),kid:'test',alg:'RS256',use:'sig'};
    vi.spyOn(UserManager.prototype,'signinPopup').mockImplementation(async args=>({expired:false,token_type:'Bearer',access_token:'aaa.bbb.ccc',id_token:await new SignJWT({nonce:args?.nonce}).setProtectedHeader({alg:'RS256',kid:'test'}).setIssuer(settings.issuer).setAudience(settings.clientId).setSubject('alice').setIssuedAt().setExpirationTime('1m').sign(attacker.privateKey)} as User));
    vi.spyOn(UserManager.prototype,'removeUser').mockResolvedValue();
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValue(new Response(JSON.stringify({keys:[jwk]}),{status:200}));
    await expect(new OperatorOidc().signIn({settings,metadata})).rejects.toThrow();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
