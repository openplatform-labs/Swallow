import { InMemoryWebStorage, UserManager, WebStorageStateStore, type OidcMetadata } from 'oidc-client-ts';
import { createRemoteJWKSet, customFetch, jwtVerify } from 'jose';

export type OidcSettings = { issuer: string; clientId: string; scope: string; resource: string; audience: string; apiOrigin: string; environmentId: string };
export type VerifiedSession = { token: string; subject: string; environmentId: string };
export type PreparedOidc = { settings: OidcSettings; metadata: Partial<OidcMetadata> };

function secureUrl(value: string, allowPath: boolean): URL {
  const url=new URL(value);
  if (url.username || url.password || url.hash || (!allowPath && (url.pathname !== '/' || url.search)) || !['http:','https:'].includes(url.protocol) || (url.protocol==='http:' && !['localhost','127.0.0.1','[::1]'].includes(url.hostname))) throw new Error('invalid URL');
  return url;
}

export class OperatorOidc {
  async prepare(input: OidcSettings): Promise<PreparedOidc> {
    const settings: OidcSettings={issuer:input.issuer.trim(),clientId:input.clientId.trim(),scope:input.scope.trim(),resource:input.resource.trim(),audience:input.audience.trim(),apiOrigin:input.apiOrigin.trim(),environmentId:input.environmentId.trim()};
    const issuer=secureUrl(settings.issuer,true); secureUrl(settings.apiOrigin,false);
    if (issuer.search || !settings.clientId || settings.clientId.length>256 || !settings.environmentId || new TextEncoder().encode(settings.environmentId).length>512 || settings.resource.length>2048 || settings.audience.length>2048 || settings.scope.length>512 || !settings.scope.split(/\s+/).includes('openid') || settings.scope.split(/\s+/).includes('offline_access')) throw new Error('invalid settings');
    const response=await fetch(`${settings.issuer.replace(/\/$/,'')}/.well-known/openid-configuration`,{credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
    if (!response.ok) throw new Error('discovery unavailable');
    const text=await response.text(); if (text.length>65536) throw new Error('metadata too large');
    const metadata: unknown=JSON.parse(text);
    if (typeof metadata !== 'object' || metadata === null || !('issuer' in metadata) || metadata.issuer !== settings.issuer || !('authorization_endpoint' in metadata) || typeof metadata.authorization_endpoint !== 'string' || !('token_endpoint' in metadata) || typeof metadata.token_endpoint !== 'string') throw new Error('invalid metadata');
    secureUrl(metadata.authorization_endpoint,true); secureUrl(metadata.token_endpoint,true);
    if (!('jwks_uri' in metadata) || typeof metadata.jwks_uri !== 'string') throw new Error('invalid metadata');
    secureUrl(metadata.jwks_uri,true);
    if (!('code_challenge_methods_supported' in metadata) || !Array.isArray(metadata.code_challenge_methods_supported) || !metadata.code_challenge_methods_supported.includes('S256')) throw new Error('PKCE unavailable');
    return {settings,metadata:metadata as Partial<OidcMetadata>};
  }

  async signIn(prepared: PreparedOidc): Promise<VerifiedSession> {
    const {settings,metadata}=prepared;
    const callback=`${window.location.origin}/oidc-callback`;
    const stateStore=new WebStorageStateStore({store:new InMemoryWebStorage()});
    const userStore=new WebStorageStateStore({store:new InMemoryWebStorage()});
    const manager=new UserManager({authority:settings.issuer,client_id:settings.clientId,redirect_uri:callback,popup_redirect_uri:callback,response_type:'code',scope:settings.scope,metadata,resource:settings.resource || undefined,extraQueryParams:settings.audience ? {audience:settings.audience} : undefined,stateStore,userStore,automaticSilentRenew:false,loadUserInfo:false,disablePKCE:false});
    try {
      const nonce=crypto.randomUUID();
      const user=await manager.signinPopup({nonce});
      if (user.expired || user.token_type.toLowerCase()!=='bearer' || !user.access_token || user.access_token.length>16384) throw new Error('invalid token');
      if (!user.id_token || user.id_token.length>16384 || typeof metadata.jwks_uri !== 'string') throw new Error('invalid identity token');
      const jwks=createRemoteJWKSet(secureUrl(metadata.jwks_uri,true),{timeoutDuration:5000,[customFetch]:(url,options)=>fetch(url,{...options,credentials:'omit',redirect:'error',cache:'no-store'})});
      const {payload}=await jwtVerify(user.id_token,jwks,{issuer:settings.issuer,audience:settings.clientId,algorithms:['RS256','ES256'],requiredClaims:['sub','exp','iat','nonce']});
      if (payload['nonce']!==nonce || typeof payload.iat!=='number' || payload.iat>Math.floor(Date.now()/1000) || (Array.isArray(payload.aud) && payload.aud.length>1 && payload['azp']!==settings.clientId) || (payload['azp']!==undefined && payload['azp']!==settings.clientId)) throw new Error('invalid identity token');
      const params=new URLSearchParams({environmentId:settings.environmentId});
      const response=await fetch(`${new URL(settings.apiOrigin).origin}/v1/experimental/operator/me?${params}`,{headers:{Authorization:`Bearer ${user.access_token}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
      if (!response.ok) throw new Error('operator access denied');
      const identity: unknown=await response.json();
      if (typeof identity!=='object' || identity===null || !('issuer' in identity) || identity.issuer!==settings.issuer || !('environmentId' in identity) || identity.environmentId!==settings.environmentId || !('clientId' in identity) || identity.clientId!==settings.clientId || !('subject' in identity) || typeof identity.subject!=='string' || !identity.subject || payload.sub!==identity.subject) throw new Error('identity mismatch');
      return {token:user.access_token,subject:identity.subject,environmentId:settings.environmentId};
    } finally { await manager.removeUser(); }
  }
}

export async function oidcPopupCallback(): Promise<void> {
  const callback=`${window.location.origin}/oidc-callback`;
  const manager=new UserManager({authority:window.location.origin,client_id:'callback',redirect_uri:callback,automaticSilentRenew:false});
  await manager.signinPopupCallback();
}
