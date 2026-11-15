import { InMemoryWebStorage, UserManager, WebStorageStateStore, type OidcMetadata } from 'oidc-client-ts';
import { createRemoteJWKSet, customFetch, decodeJwt, jwtVerify } from 'jose';

export type OidcSettings = { issuer: string; clientId: string; scope: string; resource: string; audience: string; apiOrigin: string; environmentId: string; allowRefresh?: boolean };
export type VerifiedSession = { token: string; subject: string; environmentId: string; expiresAt: number };
export type PreparedOidc = { settings: OidcSettings; metadata: Partial<OidcMetadata> };

function secureUrl(value: string, allowPath: boolean): URL {
  const url=new URL(value);
  if (url.username || url.password || url.hash || (!allowPath && (url.pathname !== '/' || url.search)) || !['http:','https:'].includes(url.protocol) || (url.protocol==='http:' && !['localhost','127.0.0.1','[::1]'].includes(url.hostname))) throw new Error('invalid URL');
  return url;
}

// Enforce limits while consuming the stream, before allocating the full response.
async function boundedText(response: Response, limit: number): Promise<string> {
  const declared=response.headers.get('content-length');
  if(declared && /^\d+$/.test(declared) && Number(declared)>limit) {
    void response.body?.cancel().catch(()=>{});
    throw new Error('response too large');
  }
  if(!response.body) throw new Error('empty response');
  const reader=response.body.getReader(),decoder=new TextDecoder('utf-8',{fatal:true});
  let size=0,text='';
  try {
    for (;;) {
      const {done,value}=await reader.read();
      if(done) break;
      size+=value.byteLength;
      if(size>limit) throw new Error('response too large');
      text+=decoder.decode(value,{stream:true});
    }
    return text+decoder.decode();
  } catch(error) {
    void reader.cancel().catch(()=>{});
    throw error;
  } finally { reader.releaseLock(); }
}

export class OperatorOidc {
  private active: {manager:UserManager; settings:OidcSettings; subject:string; revocationEndpoint:string | null} | null=null;
  private renewing=false;
  private revoking=false;
  private generation=0;
  dispose(): void {
    this.generation++;
    const active=this.active;this.active=null;
    if(active) void active.manager.removeUser().catch(()=>{});
  }

  async prepare(input: OidcSettings): Promise<PreparedOidc> {
    const settings: OidcSettings={issuer:input.issuer.trim(),clientId:input.clientId.trim(),scope:input.scope.trim(),resource:input.resource.trim(),audience:input.audience.trim(),apiOrigin:input.apiOrigin.trim(),environmentId:input.environmentId.trim(),allowRefresh:input.allowRefresh === true};
    const issuer=secureUrl(settings.issuer,true); secureUrl(settings.apiOrigin,false);
    if (issuer.search || !settings.clientId || settings.clientId.length>256 || !settings.environmentId || new TextEncoder().encode(settings.environmentId).length>512 || settings.resource.length>2048 || settings.audience.length>2048 || settings.scope.length>512 || !settings.scope.split(/\s+/).includes('openid') || (!settings.allowRefresh && settings.scope.split(/\s+/).includes('offline_access'))) throw new Error('invalid settings');
    if(settings.allowRefresh && !settings.scope.split(/\s+/).includes('offline_access'))settings.scope+=' offline_access';
    if(settings.scope.length>512)throw new Error('invalid settings');
    const response=await fetch(`${settings.issuer.replace(/\/$/,'')}/.well-known/openid-configuration`,{credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
    if (!response.ok) throw new Error('discovery unavailable');
    const text=await boundedText(response,65536);
    const metadata: unknown=JSON.parse(text);
    if (typeof metadata !== 'object' || metadata === null || !('issuer' in metadata) || metadata.issuer !== settings.issuer || !('authorization_endpoint' in metadata) || typeof metadata.authorization_endpoint !== 'string' || !('token_endpoint' in metadata) || typeof metadata.token_endpoint !== 'string') throw new Error('invalid metadata');
    secureUrl(metadata.authorization_endpoint,true); secureUrl(metadata.token_endpoint,true);
    if (!('jwks_uri' in metadata) || typeof metadata.jwks_uri !== 'string') throw new Error('invalid metadata');
    secureUrl(metadata.jwks_uri,true);
    if (!('code_challenge_methods_supported' in metadata) || !Array.isArray(metadata.code_challenge_methods_supported) || !metadata.code_challenge_methods_supported.includes('S256')) throw new Error('PKCE unavailable');
    return {settings,metadata:metadata as Partial<OidcMetadata>};
  }

  async signIn(prepared: PreparedOidc): Promise<VerifiedSession> {
    this.dispose();
    const generation=this.generation;
    const {settings,metadata}=prepared;
    const callback=`${window.location.origin}/oidc-callback`;
    const stateStore=new WebStorageStateStore({store:new InMemoryWebStorage()});
    const userStore=new WebStorageStateStore({store:new InMemoryWebStorage()});
    const manager=new UserManager({authority:settings.issuer,client_id:settings.clientId,redirect_uri:callback,popup_redirect_uri:callback,response_type:'code',scope:settings.scope,metadata,resource:settings.resource || undefined,extraQueryParams:settings.audience ? {audience:settings.audience} : undefined,stateStore,userStore,automaticSilentRenew:false,loadUserInfo:false,disablePKCE:false});
    try {
      const nonce=crypto.randomUUID();
      const user=await manager.signinPopup({nonce});
      if(generation!==this.generation)throw new Error('login cancelled');
      if (user.expired || user.token_type.toLowerCase()!=='bearer' || !user.access_token || user.access_token.length>16384) throw new Error('invalid token');
      if (!user.id_token || user.id_token.length>16384 || typeof metadata.jwks_uri !== 'string') throw new Error('invalid identity token');
      const jwks=createRemoteJWKSet(secureUrl(metadata.jwks_uri,true),{timeoutDuration:5000,[customFetch]:async(url,options)=>{const response=await fetch(url,{...options,credentials:'omit',redirect:'error',cache:'no-store'});if(!response.ok)return response;const body=await boundedText(response,262144);const headers=new Headers(response.headers);headers.delete('content-length');return new Response(body,{status:response.status,statusText:response.statusText,headers});}});
      const {payload}=await jwtVerify(user.id_token,jwks,{issuer:settings.issuer,audience:settings.clientId,algorithms:['RS256','ES256'],requiredClaims:['sub','exp','iat','nonce']});
      if (payload['nonce']!==nonce || typeof payload.iat!=='number' || payload.iat>Math.floor(Date.now()/1000) || (Array.isArray(payload.aud) && payload.aud.length>1 && payload['azp']!==settings.clientId) || (payload['azp']!==undefined && payload['azp']!==settings.clientId)) throw new Error('invalid identity token');
      const session=await this.verifyAccess(user,settings,payload.sub);
      if(generation!==this.generation)throw new Error('login cancelled');
      if(settings.allowRefresh){
        if(!user.refresh_token || user.refresh_token.length>16384)throw new Error('refresh token unavailable');
        let revocationEndpoint:string|null=null;
        if(typeof metadata.revocation_endpoint==='string') {
          try { const endpoint=secureUrl(metadata.revocation_endpoint,true);if(endpoint.protocol==='http:' && new URL(settings.issuer).protocol!=='http:')throw new Error('insecure revocation endpoint');revocationEndpoint=endpoint.href; } catch { /* Optional unsupported endpoint. */ }
        }
        this.active={manager,settings,subject:session.subject,revocationEndpoint};
      }
      return session;
    } finally { if(this.active?.manager!==manager) await manager.removeUser(); }
  }

  private async verifyAccess(user: import('oidc-client-ts').User, settings:OidcSettings, subject:unknown): Promise<VerifiedSession> {
    if(user.expired || user.token_type.toLowerCase()!=='bearer' || !user.access_token || user.access_token.length>16384)throw new Error('invalid token');
    const params=new URLSearchParams({environmentId:settings.environmentId});
    const response=await fetch(`${new URL(settings.apiOrigin).origin}/v1/experimental/operator/me?${params}`,{headers:{Authorization:`Bearer ${user.access_token}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
    if(!response.ok)throw new Error('operator access denied');
    const identity: unknown=JSON.parse(await boundedText(response,16384));
    if(typeof identity!=='object' || identity===null || !('issuer' in identity) || identity.issuer!==settings.issuer || !('environmentId' in identity) || identity.environmentId!==settings.environmentId || !('clientId' in identity) || identity.clientId!==settings.clientId || !('subject' in identity) || typeof identity.subject!=='string' || !identity.subject || subject!==identity.subject)throw new Error('identity mismatch');
    const exp=decodeJwt(user.access_token).exp;
    if(typeof exp!=='number' || !Number.isSafeInteger(exp) || exp<=Math.floor(Date.now()/1000))throw new Error('invalid token expiry');
    const expiresAt=typeof user.expires_at==='number' && Number.isFinite(user.expires_at) ? Math.min(exp,user.expires_at) : exp;
    if(expiresAt<=Date.now()/1000)throw new Error('invalid token expiry');
    return {token:user.access_token,subject:identity.subject,environmentId:settings.environmentId,expiresAt};
  }

  async renew():Promise<VerifiedSession> {
    const active=this.active,generation=this.generation;
    if(!active || this.renewing || this.revoking)throw new Error('refresh unavailable');
    this.renewing=true;
    try {
      const user=await active.manager.signinSilent({silentRequestTimeoutInSeconds:10,resource:active.settings.resource || undefined});
      if(generation!==this.generation || this.active!==active)throw new Error('refresh cancelled');
      if(!user?.refresh_token || user.refresh_token.length>16384)throw new Error('refresh token unavailable');
      const session=await this.verifyAccess(user,active.settings,active.subject);
      if(generation!==this.generation || this.active!==active)throw new Error('refresh cancelled');
      return session;
    } catch(error) {
      if(this.active===active)this.dispose();
      else await active.manager.removeUser();
      throw error;
    } finally {this.renewing=false;}
  }

  canRevokeRefresh():boolean {return !!this.active?.revocationEndpoint && !this.renewing && !this.revoking;}
  async revokeRefresh():Promise<void> {
    const active=this.active,generation=this.generation;
    if(!active?.revocationEndpoint || this.renewing || this.revoking)throw new Error('refresh revocation unavailable');
    this.revoking=true;
    try {
      const user=await active.manager.getUser();
      if(generation!==this.generation || this.active!==active || !user?.refresh_token || user.refresh_token.length>16384)throw new Error('refresh revocation unavailable');
      const body=new URLSearchParams({token:user.refresh_token,token_type_hint:'refresh_token',client_id:active.settings.clientId});
      const response=await fetch(active.revocationEndpoint,{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body,credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
      if(response.status!==200)throw new Error('refresh revocation failed');
    } finally {this.revoking=false;}

  }
}

export async function oidcPopupCallback(): Promise<void> {
  const callback=`${window.location.origin}/oidc-callback`;
  const manager=new UserManager({authority:window.location.origin,client_id:'callback',redirect_uri:callback,automaticSilentRenew:false});
  await manager.signinPopupCallback();
}
