import { Component, OnDestroy, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ClrAlertModule } from '@clr/angular/emphasis/alert';
import { OperatorOidc, type PreparedOidc, type VerifiedSession } from './operator-oidc';

type ConnectionState = 'unchecked' | 'checking' | 'ready' | 'unavailable';
type Subscriber = { id: string; firstName: string; lastName: string; email: string };
type AlertSlot = { channel: string; targetId: string; state: string; lastStartedAt: string | null; attemptId?: string | null };
type AlertCursor = { channel: string; targetId: string };
type ResendResult = { jobId:string; jobStatus:string; result:string; attemptState:string|null; providerIdKnown:boolean; providerIdAmbiguous:boolean; autoCorrelated:boolean; autoCorrelationConflict:boolean; autoCorrelatedAt:string|null; lastCompletedScanAt:string|null };
type DecisionHistory = { id: string; decision: string; reason: string; createdAt: string };
type DecisionRequest = { requestId: string; channel: string; targetId: string; attemptId: string; decision: string; reason: string; createdAt: string };
type RetrySnapshot = { origin: string; endpoint: string; scheme: 'Bearer' | 'SwallowConsole'; requestId: string; channel: string; targetId: string; attemptId: string; decision: string; reason: string; keyHash: string };
type PermissionIdentity = { issuer:string; subject:string; clientId:string; enabled:boolean; subscriberRead:boolean; alertRead:boolean; alertRetry:boolean; alertAccepted:boolean };
type PermissionFlags = { enabled:boolean; subscriberRead:boolean; alertRead:boolean; alertRetry:boolean; alertAccepted:boolean };
type PermissionChange = { origin:string; environment:string; keyHash:string; body:{ requestId:string; targetIssuer:string; targetSubject:string; targetClientId:string; expected:PermissionFlags|null; desired:PermissionFlags; reason:string } };
type PermissionRequestRecord = {requestId:string; targetIssuer:string; targetSubject:string; targetClientId:string; expected:PermissionFlags|null; desired:PermissionFlags; reason:string; changed:boolean; createdAt:string};
type PermissionAudit = { id:string; recordedAt:string; issuer:string; subject:string; clientId:string; actor:string; jwtIssuer:string|null; jwtSubject:string|null; jwtClientId:string|null; requestId:string|null; reason:string; previous:string; current:string };
type Section = 'overview' | 'workflows' | 'subscribers' | 'channels' | 'activity' | 'alerts' | 'settings';

@Component({
  imports: [FormsModule, ClrAlertModule],
  selector: 'app-root',
  styleUrl: './app.css',
  templateUrl: './app.html',
})
export class App implements OnDestroy {
  readonly sections: { id: Section; label: string; short: string }[] = [
    { id: 'overview', label: '개요', short: 'OV' },
    { id: 'workflows', label: '워크플로', short: 'WF' },
    { id: 'subscribers', label: '구독자', short: 'SU' },
    { id: 'channels', label: '발송 채널', short: 'CH' },
    { id: 'activity', label: '발송 이력', short: 'AC' },
    { id: 'alerts', label: '운영 경보', short: 'AL' },
    { id: 'settings', label: '설정', short: 'ST' },
  ];
  readonly section = signal<Section>('overview');
  readonly status = signal<ConnectionState>('unchecked');
  readonly checkedAt = signal<string | null>(null);
  readonly detail = signal('');
  readonly subscribers = signal<Subscriber[]>([]);
  readonly subscriberStatus = signal<'idle' | 'loading' | 'loaded' | 'error'>('idle');
  readonly subscriberError = signal('');
  readonly nextCursor = signal<string | null>(null);
  readonly previousCursor = signal<string | null>(null);
  readonly alertSlots = signal<AlertSlot[]>([]);
  readonly alertStatus = signal<'idle' | 'loading' | 'loaded' | 'error'>('idle');
  readonly alertError = signal('');
  resendJobId = '';
  readonly resendStatus = signal<'idle'|'loading'|'loaded'|'error'>('idle');
  readonly resendError = signal('');
  readonly resendResult = signal<ResendResult|null>(null);
  private resendSequence = 0;
  private resendAbort: AbortController|null = null;
  readonly resendJobs = signal<ResendResult[]>([]);
  readonly resendJobsStatus = signal<'idle'|'loading'|'loaded'|'error'>('idle');
  readonly resendJobsError = signal('');
  readonly resendJobsNext = signal<string|null>(null);
  readonly resendJobsPage = signal(1);
  private resendJobsHistory: (string|null)[] = [null];
  private resendJobsSequence = 0;
  private resendJobsAbort: AbortController|null = null;
  readonly alertsHaveMore = signal(false);
  readonly alertNext = signal<AlertCursor | null>(null);
  readonly alertPage = signal(1);
  readonly selectedAlert = signal<AlertSlot | null>(null);
  readonly decisionStatus = signal<'idle' | 'sending' | 'recorded' | 'rejected' | 'unknown' | 'unavailable'>('idle');
  decisionProtocol: 'idempotent' | 'legacy' = 'idempotent';
  readonly decisionRequestId = signal<string | null>(null);
  readonly retrySnapshot = signal<RetrySnapshot | null>(null);
  retryKey = '';
  retryConfirmed = false;
  readonly retryError = signal('');
  lookupKey = '';
  lookupId = '';
  readonly lookupStatus = signal<'idle' | 'loading' | 'found' | 'not-found' | 'error'>('idle');
  readonly lookupResult = signal<DecisionRequest | null>(null);
  private lookupSequence = 0;
  private lookupAbort: AbortController | null = null;
  decisionKey = '';
  decision: 'retry' | 'accepted' = 'retry';
  decisionReason = '';
  decisionConfirmed = false;
  historyKey = '';
  historyChannel = 'slack';
  historyTarget = '';
  historyAttempt = '';
  readonly historyStatus = signal<'idle' | 'loading' | 'loaded' | 'error'>('idle');
  readonly historyRows = signal<DecisionHistory[]>([]);
  private historyRequestId = 0;
  private historyAbort: AbortController | null = null;
  alertKey = '';
  alertAuthMode: 'restricted' | 'jwt' = 'restricted';
  alertEnvironment = '';
  private alertHistory: (AlertCursor | null)[] = [null];
  private alertRequestId = 0;
  private alertAbort: AbortController | null = null;
  apiOrigin = 'http://127.0.0.1:3000';
  apiKey = '';
  credentialMode: 'restricted' | 'api-key' | 'jwt' = 'restricted';
  subscriberEnvironment = '';
  subscriberName = '';
  private subscriberRequestId = 0;
  private subscriberAbort: AbortController | null = null;
  private requestId = 0;
  oidcIssuer = '';
  oidcClientId = '';
  oidcScope = 'openid';
  oidcResource = '';
  oidcAudience = '';
  oidcEnvironment = '';
  readonly oidcStatus = signal<'idle' | 'preparing' | 'ready' | 'signing' | 'signed-in' | 'expired' | 'error'>('idle');
  readonly oidcSubject = signal('');
  private oidcPrepared: PreparedOidc | null = null;
  private oidcSession: VerifiedSession | null = null;
  private oidcSequence = 0;
  private oidcExpiryTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly oidc = new OperatorOidc();
  permissionEnvironment = '';
  permissionKey = '';
  permissionIssuer = '';
  permissionSubject = '';
  permissionClient = '';
  readonly permissionStatus = signal<'idle'|'loading'|'identity'|'missing'|'audit'|'error'>('idle');
  readonly permissionIdentity = signal<PermissionIdentity|null>(null);
  readonly permissionAudit = signal<PermissionAudit[]>([]);
  readonly permissionNext = signal<string|null>(null);
  private permissionSequence = 0;
  private permissionAbort: AbortController|null = null;
  permissionDesired:PermissionFlags = {enabled:false,subscriberRead:false,alertRead:false,alertRetry:false,alertAccepted:false};
  permissionReason = '';
  permissionConfirmed = false;
  permissionRecoveryConfirmed = false;
  readonly permissionChangeStatus=signal<'idle'|'sending'|'recorded'|'rejected'|'unknown'|'unavailable'>('idle');
  readonly permissionChange=signal<PermissionChange|null>(null);
  readonly permissionChangeChanged=signal<boolean|null>(null);
  readonly permissionChangeMessage=signal('');
  permissionRequestId='';
  readonly permissionRequestStatus=signal<'idle'|'loading'|'found'|'missing'|'error'>('idle');
  readonly permissionRequestRecord=signal<PermissionRequestRecord|null>(null);
  private permissionRequestSequence=0;
  private permissionRequestAbort:AbortController|null=null;

  ngOnDestroy(): void {
    this.oidcSequence++;
    if (this.oidcExpiryTimer !== null) clearTimeout(this.oidcExpiryTimer);
    this.oidcExpiryTimer = null;
    this.oidcSession = null;
    this.alertKey='';
    this.resendJobId='';
    this.clearResend();
    this.clearResendJobs();
    this.clearPermissions(true);
  }

  private expireOidcIfNeeded(): boolean {
    const session=this.oidcSession;
    if (!session || Date.now() < session.expiresAt*1000) return false;
    if (this.oidcExpiryTimer !== null) clearTimeout(this.oidcExpiryTimer);
    this.oidcExpiryTimer=null; this.oidcSession=null; this.oidcSubject.set(''); this.oidcStatus.set('expired');
    // Preserve decision outcome and request ID, including an in-flight or uncertain decision.
    if (this.apiKey===session.token) { this.apiKey=''; this.clearSubscribers(); }
    if (this.alertKey===session.token) {
      this.alertKey=''; this.clearResend(); this.clearResendJobs();
      this.alertRequestId++; this.alertAbort?.abort(); this.alertAbort=null;
      this.alertSlots.set([]); this.alertStatus.set('idle'); this.alertError.set('');
      this.alertsHaveMore.set(false); this.alertNext.set(null); this.alertPage.set(1); this.alertHistory=[null];
      if (this.decisionStatus()!=='sending') this.selectedAlert.set(null);
    }
    if (this.decisionKey===session.token) this.decisionKey='';
    if (this.retryKey===session.token) this.retryKey='';
    if (this.lookupKey===session.token) { this.lookupKey=''; this.clearRequestLookup(); }
    if (this.permissionKey===session.token) this.clearPermissions(true);
    return true;
  }

  private scheduleOidcExpiry(): void {
    if (!this.oidcSession || this.expireOidcIfNeeded()) return;
    const delay=Math.min(2147483647, Math.max(1, this.oidcSession.expiresAt*1000-Date.now()));
    this.oidcExpiryTimer=setTimeout(()=>{ this.oidcExpiryTimer=null; this.scheduleOidcExpiry(); },delay);
  }

  resetOidc(): void {
    if (this.decisionStatus() === 'sending' || this.permissionChangeStatus()==='sending') return;
    if (this.oidcExpiryTimer !== null) clearTimeout(this.oidcExpiryTimer);
    this.oidcExpiryTimer=null;
    this.oidcSequence++; this.oidcPrepared = null; this.oidcSession = null;
    this.oidcStatus.set('idle'); this.oidcSubject.set('');
    this.forgetKey(); this.forgetAlertKey(); this.clearPermissions(true);
  }

  async prepareOidc(): Promise<void> {
    if (this.oidcStatus() === 'preparing' || this.oidcStatus() === 'signing') return;
    this.resetOidc(); const sequence=this.oidcSequence; this.oidcStatus.set('preparing');
    try {
      const prepared=await this.oidc.prepare({issuer:this.oidcIssuer,clientId:this.oidcClientId,scope:this.oidcScope,resource:this.oidcResource,audience:this.oidcAudience,apiOrigin:this.apiOrigin,environmentId:this.oidcEnvironment});
      if (sequence !== this.oidcSequence) return;
      this.oidcPrepared=prepared; this.oidcStatus.set('ready');
    } catch { if (sequence === this.oidcSequence) this.oidcStatus.set('error'); }
  }

  async signInOidc(): Promise<void> {
    if (this.oidcStatus() !== 'ready' || !this.oidcPrepared) return;
    const sequence=this.oidcSequence; const prepared=this.oidcPrepared; this.oidcStatus.set('signing');
    try {
      const session=await this.oidc.signIn(prepared);
      if (sequence !== this.oidcSequence) return;
      this.oidcSession=session; this.oidcSubject.set(session.subject); this.oidcStatus.set('signed-in'); this.scheduleOidcExpiry();
    } catch { if (sequence === this.oidcSequence) this.oidcStatus.set('error'); }
  }

  useOidc(section: 'subscribers' | 'alerts'): void {
    this.expireOidcIfNeeded();
    if (!this.oidcSession || this.decisionStatus() === 'sending') return;
    const session=this.oidcSession;
    this.setSection(section);
    if (section === 'subscribers') { this.forgetKey(); this.credentialMode='jwt'; this.subscriberEnvironment=session.environmentId; this.apiKey=session.token; }
    else { this.forgetAlertKey(); this.alertAuthMode='jwt'; this.alertEnvironment=session.environmentId; this.alertKey=session.token; }
  }

  canUseOidcForDecision(): boolean {
    return this.alertAuthMode==='jwt' && this.oidcSession !== null && this.oidcSession.environmentId===this.alertEnvironment.trim() && Date.now()<this.oidcSession.expiresAt*1000 && this.decisionStatus()!=='sending';
  }

  useOidcForDecision(target: 'decision' | 'retry' | 'lookup'): void {
    this.expireOidcIfNeeded();
    if (!this.canUseOidcForDecision() || !this.oidcSession) return;
    if (target==='decision') this.decisionKey=this.oidcSession.token;
    else if (target==='retry') this.retryKey=this.oidcSession.token;
    else { this.clearRequestLookup(); this.lookupKey=this.oidcSession.token; }
  }

  useOidcForPermissions(): void {
    this.expireOidcIfNeeded();
    if (!this.oidcSession || this.decisionStatus()==='sending' || this.permissionChangeStatus()==='sending') return;
    const session=this.oidcSession;
    this.clearPermissions(true);this.permissionEnvironment=session.environmentId;this.permissionKey=session.token;
  }

  clearPermissions(clearKey = false): void {
    this.permissionSequence++;this.permissionAbort?.abort();this.permissionAbort=null;
    this.permissionStatus.set('idle');this.permissionIdentity.set(null);this.permissionAudit.set([]);this.permissionNext.set(null);
    this.clearPermissionRequest(clearKey);
    if (clearKey) this.permissionKey='';
  }

  clearPermissionRequest(clearInput=false):void {
    this.permissionRequestSequence++;this.permissionRequestAbort?.abort();this.permissionRequestAbort=null;
    this.permissionRequestStatus.set('idle');this.permissionRequestRecord.set(null);
    if(clearInput)this.permissionRequestId='';
  }

  async lookupPermissionRequest():Promise<void> {
    if(this.permissionChangeStatus()==='sending')return;
    this.expireOidcIfNeeded();this.clearPermissionRequest();const sequence=this.permissionRequestSequence;
    const key=this.permissionKey.trim(),env=this.permissionEnvironment.trim(),requestId=this.permissionRequestId.trim();
    if(!this.validJwt(key)||!this.validEnvironment(env)||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(requestId)){this.permissionRequestStatus.set('error');return;}
    const controller=new AbortController();this.permissionRequestAbort=controller;this.permissionRequestStatus.set('loading');
    try {
      const params=new URLSearchParams({environmentId:env,requestId});
      const response=await fetch(`${this.validatedOrigin()}/v1/experimental/operator/permissions/request?${params}`,{headers:{Authorization:`Bearer ${key}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if(!response.ok)throw new Error('request unavailable');
      const value:unknown=await response.json();if(typeof value!=='object'||value===null||!('found' in value)||typeof value.found!=='boolean')throw new Error('invalid response');
      if(sequence!==this.permissionRequestSequence)return;
      if(!value.found){this.permissionRequestStatus.set('missing');return;}
      const r=value as Record<string,unknown>;
      if(typeof r['requestId']!=='string'||r['requestId'].toLowerCase()!==requestId.toLowerCase()||['targetIssuer','targetSubject','targetClientId','reason','createdAt'].some(k=>typeof r[k]!=='string')||typeof r['changed']!=='boolean'||!(r['expected']===null||this.isPermissionFlags(r['expected']))||!this.isPermissionFlags(r['desired']))throw new Error('invalid response');
      this.permissionRequestRecord.set({requestId:r['requestId'],targetIssuer:r['targetIssuer'] as string,targetSubject:r['targetSubject'] as string,targetClientId:r['targetClientId'] as string,expected:r['expected'] as PermissionFlags|null,desired:r['desired'],reason:r['reason'] as string,changed:r['changed'],createdAt:r['createdAt'] as string});this.permissionRequestStatus.set('found');
    } catch {if(sequence===this.permissionRequestSequence)this.permissionRequestStatus.set('error');}
    finally {if(sequence===this.permissionRequestSequence)this.permissionRequestAbort=null;}
  }

  private isPermissionFlags(value:unknown):value is PermissionFlags {
    if(typeof value!=='object'||value===null)return false;
    const record=value as Record<string,unknown>;
    return ['enabled','subscriberRead','alertRead','alertRetry','alertAccepted'].every(key=>typeof record[key]==='boolean');
  }

  async loadPermissions(mode:'identity'|'audit', next=false): Promise<void> {
    if(this.permissionChangeStatus()==='sending')return;
    this.expireOidcIfNeeded();
    const cursor=next ? this.permissionNext() : null;
    if (next && (!cursor || mode!=='audit')) return;
    this.clearPermissions();const sequence=this.permissionSequence;
    const controller=new AbortController();this.permissionAbort=controller;this.permissionStatus.set('loading');
    try {
      const origin=this.validatedOrigin();const key=this.permissionKey.trim();const env=this.permissionEnvironment.trim();
      if (!this.validJwt(key) || !this.validEnvironment(env)) throw new Error('invalid credentials');
      const params=new URLSearchParams({environmentId:env});
      if (mode==='identity') {
        const issuer=this.permissionIssuer.trim(), subject=this.permissionSubject.trim(), client=this.permissionClient.trim();
        const bytes=(s:string)=>new TextEncoder().encode(s).length;
        if (!issuer || bytes(issuer)>2048 || !subject || bytes(subject)>1024 || !client || bytes(client)>1024) throw new Error('invalid identity');
        params.set('issuer',issuer);params.set('subject',subject);params.set('clientId',client);
      } else if (cursor) { if (!/^[1-9]\d*$/.test(cursor)) throw new Error('invalid cursor');params.set('before',cursor); }
      const response=await fetch(`${origin}/v1/experimental/operator/permissions${mode==='audit'?'/audit':''}?${params}`,{headers:{Authorization:`Bearer ${key}`},credentials:'omit',redirect:'error',cache:'no-store',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if (!response.ok) throw new Error('request failed');
      const body:unknown=await response.json();
      if (typeof body!=='object'||body===null) throw new Error('invalid response');
      if (mode==='identity') {
        if (!('found' in body)||typeof body.found!=='boolean') throw new Error('invalid response');
        if (!body.found) {if(sequence===this.permissionSequence){this.permissionDesired={enabled:false,subscriberRead:false,alertRead:false,alertRetry:false,alertAccepted:false};this.permissionStatus.set('missing');}return;}
        if (!('identity' in body)||typeof body.identity!=='object'||body.identity===null) throw new Error('invalid response');
        const i=body.identity as Record<string,unknown>;
        if (i['issuer']!==this.permissionIssuer.trim()||i['subject']!==this.permissionSubject.trim()||i['client_id']!==this.permissionClient.trim()||i['environment_id']!==env || ['enabled','subscriber_read','alert_read','alert_retry','alert_accepted'].some(k=>typeof i[k]!=='boolean')) throw new Error('invalid response');
        if(sequence!==this.permissionSequence)return;
        this.permissionIdentity.set({issuer:i['issuer'] as string,subject:i['subject'] as string,clientId:i['client_id'] as string,enabled:i['enabled'] as boolean,subscriberRead:i['subscriber_read'] as boolean,alertRead:i['alert_read'] as boolean,alertRetry:i['alert_retry'] as boolean,alertAccepted:i['alert_accepted'] as boolean});
        this.permissionDesired={enabled:i['enabled'] as boolean,subscriberRead:i['subscriber_read'] as boolean,alertRead:i['alert_read'] as boolean,alertRetry:i['alert_retry'] as boolean,alertAccepted:i['alert_accepted'] as boolean};this.permissionStatus.set('identity');
      } else {
        if (!('data' in body)||!Array.isArray(body.data)||body.data.length>50||!('next' in body)||!(body.next===null||(typeof body.next==='string'&&/^[1-9]\d*$/.test(body.next)))) throw new Error('invalid response');
        const rows:PermissionAudit[]=body.data.map((row:unknown)=>{
          if(typeof row!=='object'||row===null)throw new Error('invalid row');const r=row as Record<string,unknown>;
          if(['id','recordedAt','issuer','subject','clientId','actor','reason'].some(k=>typeof r[k]!=='string')||!/^\d+$/.test(r['id'] as string)||!('previous' in r)||!('current' in r))throw new Error('invalid row');
          const optional=(key:string):string|null => typeof r[key]==='string' ? r[key] as string : null;
          return {id:r['id'] as string,recordedAt:r['recordedAt'] as string,issuer:r['issuer'] as string,subject:r['subject'] as string,clientId:r['clientId'] as string,actor:r['actor'] as string,jwtIssuer:optional('jwtIssuer'),jwtSubject:optional('jwtSubject'),jwtClientId:optional('jwtClientId'),requestId:optional('requestId'),reason:r['reason'] as string,previous:JSON.stringify(r['previous']),current:JSON.stringify(r['current'])};
        });
        if(sequence!==this.permissionSequence)return;
        this.permissionAudit.set(rows);this.permissionNext.set(body.next as string|null);this.permissionStatus.set('audit');
      }
    } catch {if(sequence===this.permissionSequence)this.permissionStatus.set('error');}
    finally {if(sequence===this.permissionSequence)this.permissionAbort=null;}
  }

  clearPermissionChange(): void {
    if(this.permissionChangeStatus()==='sending')return;
    this.permissionChange.set(null);this.permissionChangeStatus.set('idle');this.permissionChangeChanged.set(null);this.permissionChangeMessage.set('');this.permissionReason='';this.permissionConfirmed=false;this.permissionRecoveryConfirmed=false;
  }

  canChangePermissions(): boolean {
    const reason=this.permissionReason.trim();
    return this.permissionChangeStatus()==='idle' && (this.permissionStatus()==='identity'||this.permissionStatus()==='missing') && this.permissionConfirmed && this.validJwt(this.permissionKey.trim()) && this.validEnvironment(this.permissionEnvironment.trim()) && !!reason && new TextEncoder().encode(reason).length<=1000;
  }

  async changePermissions(): Promise<void> {
    this.expireOidcIfNeeded();if(!this.canChangePermissions())return;
    const identity=this.permissionIdentity();const key=this.permissionKey.trim();let origin:string;let requestId:string;
    try {origin=this.validatedOrigin();requestId=crypto.randomUUID();} catch {this.permissionChangeStatus.set('unavailable');return;}
    const expected:PermissionFlags|null=identity?{enabled:identity.enabled,subscriberRead:identity.subscriberRead,alertRead:identity.alertRead,alertRetry:identity.alertRetry,alertAccepted:identity.alertAccepted}:null;
    const body={requestId,targetIssuer:this.permissionIssuer.trim(),targetSubject:this.permissionSubject.trim(),targetClientId:this.permissionClient.trim(),expected,desired:{...this.permissionDesired},reason:this.permissionReason.trim()};
    this.permissionChangeStatus.set('sending');this.permissionChangeMessage.set('');
    try {
      let keyHash:string;try {keyHash=await this.credentialFingerprint(key);} catch {this.permissionChangeStatus.set('unavailable');return;}
      const snapshot={origin,environment:this.permissionEnvironment.trim(),keyHash,body};this.permissionChange.set(snapshot);
      const response=await fetch(`${origin}/v1/experimental/operator/permissions?${new URLSearchParams({environmentId:snapshot.environment})}`,{method:'PUT',headers:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'},body:JSON.stringify(body),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
      if(!response.ok){this.permissionChangeStatus.set([400,401,403].includes(response.status)?'rejected':'unknown');return;}
      const result:unknown=await response.json();if(typeof result!=='object'||result===null||!('changed' in result)||typeof result.changed!=='boolean'||!('replayed' in result)||typeof result.replayed!=='boolean')throw new Error('invalid response');
      this.permissionChangeChanged.set(result.changed);this.permissionChangeStatus.set('recorded');
    } catch {this.permissionChangeStatus.set('unknown');}
    finally {this.permissionReason='';this.permissionConfirmed=false;this.clearPermissions();}
  }

  async recoverPermissionChange(mode:'lookup'|'retry'): Promise<void> {
    this.expireOidcIfNeeded();const snapshot=this.permissionChange(), key=this.permissionKey.trim();
    if(!snapshot||this.permissionChangeStatus()!=='unknown'||!this.validJwt(key)||(mode==='retry'&&!this.permissionRecoveryConfirmed))return;
    this.permissionChangeStatus.set('sending');this.permissionChangeMessage.set('');
    try {
      if(this.validatedOrigin()!==snapshot.origin || this.permissionEnvironment.trim()!==snapshot.environment || await this.credentialFingerprint(key)!==snapshot.keyHash) {this.permissionChangeMessage.set('원래 요청과 같은 인증, 환경 및 API 주소가 필요합니다.');return;}
      const params=new URLSearchParams({environmentId:snapshot.environment});
      if(mode==='lookup')params.set('requestId',snapshot.body.requestId);
      const response=await fetch(`${snapshot.origin}/v1/experimental/operator/permissions${mode==='lookup'?'/request':''}?${params}`,{method:mode==='lookup'?'GET':'PUT',headers:mode==='lookup'?{Authorization:`Bearer ${key}`}:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'},...(mode==='retry'?{body:JSON.stringify(snapshot.body)}:{}),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
      if(!response.ok)throw new Error('unavailable');
      const result:unknown=await response.json();if(typeof result!=='object'||result===null)throw new Error('invalid response');
      if(mode==='lookup') {
        if(!('found' in result)||typeof result.found!=='boolean')throw new Error('invalid response');
        if(!result.found){this.permissionChangeMessage.set('기록을 찾지 못했습니다. 원래 요청이 진행 중일 수 있으므로 실패로 단정하지 마세요.');return;}
        if(!('requestId' in result)||result.requestId!==snapshot.body.requestId||!('targetIssuer' in result)||result.targetIssuer!==snapshot.body.targetIssuer||!('targetSubject' in result)||result.targetSubject!==snapshot.body.targetSubject||!('targetClientId' in result)||result.targetClientId!==snapshot.body.targetClientId||!('reason' in result)||result.reason!==snapshot.body.reason||!('expected' in result)||!this.samePermissionFlags(result.expected,snapshot.body.expected)||!('desired' in result)||!this.samePermissionFlags(result.desired,snapshot.body.desired)||!('changed' in result)||typeof result.changed!=='boolean')throw new Error('invalid response');
      } else if(!('changed' in result)||typeof result.changed!=='boolean'||!('replayed' in result)||typeof result.replayed!=='boolean')throw new Error('invalid response');
      this.permissionChangeChanged.set(result.changed as boolean);this.permissionChangeStatus.set('recorded');
    } catch {this.permissionChangeMessage.set('결과가 여전히 불명확합니다. 원래 요청과 감사 기록을 확인하세요.');}
    finally {this.permissionRecoveryConfirmed=false;if(this.permissionChangeStatus()==='sending')this.permissionChangeStatus.set('unknown');}
  }

  private samePermissionFlags(value:unknown,expected:PermissionFlags|null):boolean {
    if(expected===null)return value===null;
    if(typeof value!=='object'||value===null)return false;
    const candidate=value as Record<string,unknown>;
    return (Object.keys(expected) as (keyof PermissionFlags)[]).every(key=>candidate[key]===expected[key]);
  }

  private validJwt(key: string): boolean {
    return key.length <= 16384 && /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(key);
  }
  private validAlertKey(key: string): boolean {
    return this.alertAuthMode === 'jwt' ? this.validJwt(key) : /^[0-9a-f]{64}$/.test(key);
  }
  private validEnvironment(value: string): boolean {
    return value.length > 0 && new TextEncoder().encode(value).length <= 512;
  }

  get sectionName(): string {
    return this.sections.find((item) => item.id === this.section())?.label ?? '';
  }

  setSection(next: Section): void {
    if (this.decisionStatus() === 'sending' || this.permissionChangeStatus()==='sending') return;
    if (this.section() === 'subscribers' && next !== 'subscribers') this.forgetKey();
    if (this.section() === 'alerts' && next !== 'alerts') this.forgetAlertKey();
    if (this.section() === 'settings' && next !== 'settings') {this.clearPermissions(true);this.clearPermissionChange();}
    this.section.set(next);
  }

  connectionChanged(): void {
    this.resetOidc();
    this.apiKey = '';
    this.clearSubscribers();
    this.forgetAlertKey();
    this.requestId++;
    this.status.set('unchecked');
    this.checkedAt.set(null);
    this.detail.set('');
  }

  clearSubscribers(): void {
    this.subscriberRequestId++;
    this.subscriberAbort?.abort();
    this.subscriberAbort = null;
    this.subscribers.set([]);
    this.nextCursor.set(null);
    this.previousCursor.set(null);
    this.subscriberStatus.set('idle');
    this.subscriberError.set('');
  }

  forgetKey(): void {
    this.apiKey = '';
    this.clearSubscribers();
  }

  clearAlerts(): void {
    if (this.decisionStatus() === 'sending') return;
    this.alertRequestId++;
    this.alertAbort?.abort();
    this.alertAbort = null;
    this.alertSlots.set([]);
    this.alertsHaveMore.set(false);
    this.alertNext.set(null);
    this.alertPage.set(1);
    this.alertHistory = [null];
    this.alertStatus.set('idle');
    this.alertError.set('');
    this.clearResend();
    this.clearResendJobs();
    this.clearDecision();
    this.clearHistory(true);
  }

  clearResend(): void {
    this.resendSequence++;
    this.resendAbort?.abort();
    this.resendAbort=null;
    this.resendResult.set(null);
    this.resendError.set('');
    this.resendStatus.set('idle');
  }

  async loadResend(): Promise<void> {
    this.expireOidcIfNeeded();
    this.clearResend();
    const sequence=this.resendSequence;
    const controller=new AbortController();
    this.resendAbort=controller;
    this.resendStatus.set('loading');
    try {
      const origin=this.validatedOrigin();
      const key=this.alertKey.trim();
      const environment=this.alertEnvironment.trim();
      const job=this.resendJobId.trim();
      if (this.alertAuthMode!=='jwt' || !this.validJwt(key) || !this.validEnvironment(environment) || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(job)) throw new Error('운영자 JWT, 환경 ID와 작업 UUID를 확인해 주세요.');
      const params=new URLSearchParams({environmentId:environment,jobId:job});
      const response=await fetch(`${origin}/v1/experimental/operator/resend-result?${params}`, {
        method:'GET', headers:{Authorization:`Bearer ${key}`}, cache:'no-store', credentials:'omit', redirect:'error',
        signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)]),
      });
      if (response.status===401 || response.status===403) throw new Error('인증 값 또는 Resend 결과 조회 권한을 확인해 주세요.');
      if (!response.ok) throw new Error('작업 결과를 가져오지 못했습니다. 환경과 작업 ID를 확인해 주세요.');
      const value:unknown=await response.json();
      const result=this.parseResendResult(value,job);
      this.expireOidcIfNeeded();
      if (sequence!==this.resendSequence) return;
      this.resendResult.set(result);
      this.resendStatus.set('loaded');
    } catch (error:unknown) {
      if (sequence!==this.resendSequence) return;
      this.resendStatus.set('error');
      this.resendError.set(error instanceof Error && ['주소','원격','운영자','인증','작업','API '].some(prefix=>error.message.startsWith(prefix)) ? error.message : '서버에 연결할 수 없습니다.');
    } finally {
      if (sequence===this.resendSequence) this.resendAbort=null;
    }
  }

  private parseResendResult(value:unknown,job?:string): ResendResult {
      if (typeof value!=='object' || value===null) throw new Error('API 응답 형식을 확인해 주세요.');
      const row=value as Record<string,unknown>;
      if (typeof row['jobId']!=='string' || (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(row['jobId']) || (job!==undefined && row['jobId'].toLowerCase()!==job.toLowerCase())) || typeof row['jobStatus']!=='string' || !['pending','running','completed','failed','cancelled'].includes(row['jobStatus']) || typeof row['result']!=='string' || !['unknown','pending','delivered','failed','partial','conflicting','complained'].includes(row['result']) || !(row['attemptState']===null || (typeof row['attemptState']==='string' && ['dispatching','accepted','rejected','unknown'].includes(row['attemptState']))) || typeof row['providerIdKnown']!=='boolean' || typeof row['providerIdAmbiguous']!=='boolean' || typeof row['autoCorrelated']!=='boolean' || typeof row['autoCorrelationConflict']!=='boolean' || !(row['autoCorrelatedAt']===null || typeof row['autoCorrelatedAt']==='string') || !(row['lastCompletedScanAt']===null || typeof row['lastCompletedScanAt']==='string')) throw new Error('API 응답 형식을 확인해 주세요.');
      if (((row['autoCorrelationConflict'] || row['providerIdAmbiguous']) && row['result']==='delivered') || (row['autoCorrelationConflict'] && !row['autoCorrelated']) || (row['autoCorrelated'] !== (typeof row['autoCorrelatedAt']==='string'))) throw new Error('API 응답 형식을 확인해 주세요.');
      const result=row as ResendResult;
      return {jobId:result.jobId,jobStatus:result.jobStatus,result:result.result,attemptState:result.attemptState,providerIdKnown:result.providerIdKnown,providerIdAmbiguous:result.providerIdAmbiguous,autoCorrelated:result.autoCorrelated,autoCorrelationConflict:result.autoCorrelationConflict,autoCorrelatedAt:result.autoCorrelatedAt,lastCompletedScanAt:result.lastCompletedScanAt};
  }

  clearResendJobs(): void {
    this.resendJobsSequence++;
    this.resendJobsAbort?.abort(); this.resendJobsAbort=null;
    this.resendJobs.set([]); this.resendJobsNext.set(null); this.resendJobsPage.set(1);
    this.resendJobsHistory=[null]; this.resendJobsStatus.set('idle'); this.resendJobsError.set('');
  }

  async loadResendJobs(direction?: 'next'|'previous'): Promise<void> {
    this.expireOidcIfNeeded();
    if (direction==='previous' && this.resendJobsPage()<=1) return;
    const cursor=direction==='next' ? this.resendJobsNext() : direction==='previous' ? this.resendJobsHistory[this.resendJobsPage()-2] : null;
    if ((direction==='next' && !cursor) || cursor===undefined) return;
    const page=direction==='next' ? this.resendJobsPage()+1 : direction==='previous' ? this.resendJobsPage()-1 : 1;
    const sequence=++this.resendJobsSequence;
    this.resendJobsAbort?.abort();
    const controller=new AbortController(); this.resendJobsAbort=controller;
    this.resendJobs.set([]); this.resendJobsNext.set(null); this.resendJobsError.set(''); this.resendJobsStatus.set('loading');
    try {
      const origin=this.validatedOrigin(), key=this.alertKey.trim(), environment=this.alertEnvironment.trim();
      if (this.alertAuthMode!=='jwt' || !this.validJwt(key) || !this.validEnvironment(environment)) throw new Error('운영자 JWT와 환경 ID를 확인해 주세요.');
      const params=new URLSearchParams({environmentId:environment}); if (cursor) params.set('after',cursor);
      const response=await fetch(`${origin}/v1/experimental/operator/resend-jobs?${params}`, {method:'GET',headers:{Authorization:`Bearer ${key}`},cache:'no-store',credentials:'omit',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if (response.status===401 || response.status===403) throw new Error('인증 값 또는 Resend 목록 및 결과 조회 권한을 확인해 주세요.');
      if (!response.ok) throw new Error('작업 목록을 가져오지 못했습니다.');
      const value:unknown=await response.json();
      if (typeof value!=='object' || value===null) throw new Error('API 응답 형식을 확인해 주세요.');
      const data=value as Record<string,unknown>;
      if (!Array.isArray(data['data']) || data['data'].length>20 || typeof data['hasMore']!=='boolean' || !(data['next']===null || typeof data['next']==='string')) throw new Error('API 응답 형식을 확인해 주세요.');
      const rows=data['data'].map((row:unknown)=>this.parseResendResult(row));
      const ids=rows.map(row=>row.jobId.toLowerCase());
      if (ids.some((id:string,i:number)=> (i>0 && ids[i-1]>=id) || (!!cursor && id<=cursor.toLowerCase())) || (data['hasMore'] ? rows.length!==20 || typeof data['next']!=='string' || data['next'].toLowerCase()!==ids[ids.length-1] : data['next']!==null)) throw new Error('API 응답 형식을 확인해 주세요.');
      this.expireOidcIfNeeded(); if (sequence!==this.resendJobsSequence) return;
      this.resendJobs.set(rows); this.resendJobsNext.set(data['next'] as string|null); this.resendJobsPage.set(page);
      this.resendJobsHistory=this.resendJobsHistory.slice(0,page-1); this.resendJobsHistory.push(cursor);
      this.resendJobsStatus.set('loaded');
    } catch (error:unknown) {
      if (sequence!==this.resendJobsSequence) return;
      this.resendJobsStatus.set('error');
      this.resendJobsError.set(error instanceof Error && ['주소','원격','운영자','인증','작업','API '].some(prefix=>error.message.startsWith(prefix)) ? error.message : '서버에 연결할 수 없습니다.');
    } finally { if (sequence===this.resendJobsSequence) this.resendJobsAbort=null; }
  }

  selectResendJob(job: ResendResult): void { this.resendJobId=job.jobId; void this.loadResend(); }

  clearHistory(clearKey = false): void {
    this.historyRequestId++;
    this.historyAbort?.abort();
    this.historyAbort = null;
    this.historyRows.set([]);
    this.historyStatus.set('idle');
    if (clearKey) { this.historyKey = ''; this.historyTarget = ''; this.historyAttempt = ''; this.historyChannel = 'slack'; }
  }

  clearRequestLookup(clearInputs = false): void {
    this.lookupSequence++;
    this.lookupAbort?.abort();
    this.lookupAbort = null;
    this.lookupResult.set(null);
    this.lookupStatus.set('idle');
    if (clearInputs) { this.lookupKey = ''; this.lookupId = ''; this.decisionRequestId.set(null); this.clearRetry(); }
  }

  clearRetry(): void {
    if (this.decisionStatus() === 'sending') return;
    this.retrySnapshot.set(null); this.retryKey = ''; this.retryConfirmed = false; this.retryError.set('');
  }

  private async credentialFingerprint(key: string): Promise<string> {
    const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(key));
    return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('');
  }

  async retryDecision(): Promise<void> {
    this.expireOidcIfNeeded();
    const snapshot = this.retrySnapshot();
    const key = this.retryKey.trim();
    if (!snapshot || this.decisionStatus() !== 'unknown' || !this.retryConfirmed || !this.validAlertKey(key)) return;
    this.decisionStatus.set('sending'); this.retryError.set('');
    try {
      if (await this.credentialFingerprint(key) !== snapshot.keyHash || this.validatedOrigin() !== snapshot.origin) {
        this.retryError.set('원래 요청과 같은 인증과 API 주소가 필요합니다.');
        this.decisionStatus.set('unknown'); return;
      }
      const {origin, endpoint, scheme, keyHash: _keyHash, ...body} = snapshot;
      const response = await fetch(`${origin}${endpoint}`, {
        method: 'POST', headers: {Authorization: `${scheme} ${key}`, 'Content-Type': 'application/json'},
        body: JSON.stringify(body), cache: 'no-store', credentials: 'omit', redirect: 'error', signal: AbortSignal.timeout(10000),
      });
      if (!response.ok) throw new Error('retry unavailable');
      const result: unknown = await response.json();
      if (typeof result !== 'object' || result === null || !('recorded' in result) || result.recorded !== true || !('replayed' in result) || typeof result.replayed !== 'boolean') throw new Error('invalid response');
      this.decisionStatus.set('recorded');
      this.retrySnapshot.set(null);
    } catch {
      this.decisionStatus.set('unknown');
      this.retryError.set('결과가 여전히 불명확합니다. 원래 요청과 이 요청의 상태 및 감사를 확인하세요.');
    } finally {
      this.retryKey = ''; this.retryConfirmed = false;
    }
  }

  async lookupDecisionRequest(): Promise<void> {
    if (this.decisionStatus() === 'sending') return;
    this.expireOidcIfNeeded();
    this.clearRequestLookup();
    const sequence = this.lookupSequence;
    const key = this.lookupKey.trim();
    const requestId = this.lookupId.trim();
    const jwt = this.alertAuthMode === 'jwt';
    const environment = this.alertEnvironment.trim();
    if (!this.validAlertKey(key) || (jwt && !this.validEnvironment(environment)) || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(requestId)) { this.lookupStatus.set('error'); return; }
    const controller = new AbortController();
    this.lookupAbort = controller;
    this.lookupStatus.set('loading');
    try {
      const params = new URLSearchParams({requestId});
      if (jwt) params.set('environmentId', environment);
      const endpoint = jwt ? '/v1/experimental/operator/alerts/request' : '/v1/experimental/console/alerts/request';
      const response = await fetch(`${this.validatedOrigin()}${endpoint}?${params}`, {method:'GET',headers:{Authorization:`${jwt ? 'Bearer' : 'SwallowConsole'} ${key}`},cache:'no-store',credentials:'omit',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if (!response.ok) throw new Error('request unavailable');
      const body: unknown = await response.json();
      if (typeof body !== 'object' || body === null || !('found' in body) || typeof body.found !== 'boolean') throw new Error('invalid response');
      this.expireOidcIfNeeded();
      if (sequence !== this.lookupSequence) return;
      if (!body.found) { this.lookupStatus.set('not-found'); return; }
      if (!('requestId' in body) || typeof body.requestId !== 'string' || body.requestId.toLowerCase() !== requestId.toLowerCase() || !('channel' in body) || (body.channel !== 'slack' && body.channel !== 'email') || !('targetId' in body) || typeof body.targetId !== 'string' || !('attemptId' in body) || typeof body.attemptId !== 'string' || !('decision' in body) || (body.decision !== 'retry' && body.decision !== 'accepted') || !('reason' in body) || typeof body.reason !== 'string' || !('createdAt' in body) || typeof body.createdAt !== 'string') throw new Error('invalid response');
      this.lookupResult.set({requestId:body.requestId,channel:body.channel,targetId:body.targetId,attemptId:body.attemptId,decision:body.decision,reason:body.reason,createdAt:body.createdAt});
      this.lookupStatus.set('found');
    } catch {
      this.expireOidcIfNeeded();
      if (sequence === this.lookupSequence) this.lookupStatus.set('error');
    } finally {
      if (sequence === this.lookupSequence) this.lookupAbort = null;
    }
  }

  async loadDecisionHistory(): Promise<void> {
    if (this.decisionStatus() === 'sending') return;
    this.clearHistory();
    const id = this.historyRequestId;
    const key = this.historyKey.trim();
    const target = this.historyTarget.trim();
    const attempt = this.historyAttempt.trim();
    if (!/^[0-9a-f]{64}$/.test(key) || !['email','slack'].includes(this.historyChannel) || !/^[A-Za-z0-9._-]{1,64}$/.test(target) || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(attempt)) { this.historyStatus.set('error'); return; }
    const controller = new AbortController();
    this.historyAbort = controller;
    this.historyStatus.set('loading');
    try {
      const query = new URLSearchParams({channel:this.historyChannel,targetId:target,attemptId:attempt});
      const response = await fetch(`${this.validatedOrigin()}/v1/experimental/console/alerts/decisions?${query}`, {method:'GET',headers:{Authorization:`SwallowConsole ${key}`},cache:'no-store',credentials:'omit',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if (!response.ok) throw new Error('history unavailable');
      const body: unknown = await response.json();
      if (typeof body !== 'object' || body === null || !('data' in body) || !Array.isArray(body.data) || body.data.length > 20) throw new Error('invalid history');
      const rows: DecisionHistory[] = body.data.map((row: unknown) => {
        if (typeof row !== 'object' || row === null || !('id' in row) || typeof row.id !== 'string' || !/^\d+$/.test(row.id) || !('decision' in row) || (row.decision !== 'retry' && row.decision !== 'accepted') || !('reason' in row) || typeof row.reason !== 'string' || !('createdAt' in row) || typeof row.createdAt !== 'string') throw new Error('invalid history');
        return {id:row.id,decision:row.decision,reason:row.reason,createdAt:row.createdAt};
      });
      if (id !== this.historyRequestId) return;
      this.historyRows.set(rows); this.historyStatus.set('loaded');
    } catch {
      if (id === this.historyRequestId) this.historyStatus.set('error');
    } finally {
      if (id === this.historyRequestId) this.historyAbort = null;
    }
  }

  clearDecision(): void {
    if (this.decisionStatus() === 'sending') return;
    this.clearRetry();
    this.selectedAlert.set(null);
    this.decisionKey = '';
    this.decisionReason = '';
    this.decisionConfirmed = false;
    this.decision = 'retry';
    this.decisionStatus.set('idle');
  }

  selectAlert(slot: AlertSlot): void {
    if (this.decisionStatus() === 'sending' || this.alertStatus() !== 'loaded' || !slot.attemptId || !['unknown', 'rejected'].includes(slot.state) || !['slack', 'email'].includes(slot.channel)) return;
    this.clearDecision();
    this.selectedAlert.set(slot);
  }

  canResolve(): boolean {
    const slot = this.selectedAlert();
    const reason = this.decisionReason.trim();
    return this.decisionStatus() === 'idle' && (this.decisionProtocol === 'legacy' || this.decisionProtocol === 'idempotent') && (this.alertAuthMode !== 'jwt' || this.validEnvironment(this.alertEnvironment.trim())) && !!slot?.attemptId && this.decisionConfirmed && this.validAlertKey(this.decisionKey.trim()) && !!reason && new TextEncoder().encode(reason).length <= 512 && (this.decision === 'retry' || (this.decision === 'accepted' && slot.state === 'unknown'));
  }

  async resolveAlert(): Promise<void> {
    this.expireOidcIfNeeded();
    const slot = this.selectedAlert();
    const reason = this.decisionReason.trim();
    const key = this.decisionKey.trim();
    if (!this.canResolve() || !slot?.attemptId) return;
    let origin: string;
    try { origin = this.validatedOrigin(); } catch { this.decisionStatus.set('rejected'); return; }
    let requestId: string | null = null;
    const jwt = this.alertAuthMode === 'jwt';
    if (jwt || this.decisionProtocol === 'idempotent') {
      try { requestId = crypto.randomUUID(); } catch { this.decisionStatus.set('unavailable'); return; }
      this.clearRequestLookup(true);
      this.decisionRequestId.set(requestId);
      this.lookupId = requestId;
    } else {
      this.clearRequestLookup(true);
    }
    const payload = { channel: slot.channel, targetId: slot.targetId, attemptId: slot.attemptId, decision: this.decision, reason, ...(requestId ? {requestId} : {}) };
    const scheme = jwt ? 'Bearer' : 'SwallowConsole';
    const endpoint = jwt ? `/v1/experimental/operator/alerts/resolve?${new URLSearchParams({environmentId:this.alertEnvironment.trim()})}` : `/v1/experimental/console/alerts/${requestId ? 'resolve-v2' : 'resolve'}`;
    this.decisionStatus.set('sending');
    try {
      if (requestId) {
        try {
          const keyHash = await this.credentialFingerprint(key);
          this.retrySnapshot.set({origin, endpoint, scheme, ...payload, requestId, keyHash});
        } catch { this.decisionRequestId.set(null); this.lookupId = ''; this.decisionStatus.set('unavailable'); return; }
      }
      const response = await fetch(`${origin}${endpoint}`, {
        method: 'POST', headers: { Authorization: `${scheme} ${key}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        cache: 'no-store', credentials: 'omit', redirect: 'error', signal: AbortSignal.timeout(10000),
      });
      if (response.ok) {
        const body: unknown = await response.json();
        this.decisionStatus.set(typeof body === 'object' && body !== null && 'recorded' in body && body.recorded === true && (!requestId || ('replayed' in body && typeof body.replayed === 'boolean')) ? 'recorded' : 'unknown');
      } else {
        this.decisionStatus.set([400, 401, 403].includes(response.status) ? 'rejected' : 'unknown');
      }
    } catch {
      this.decisionStatus.set('unknown');
    } finally {
      if (this.decisionStatus() !== 'unknown') this.retrySnapshot.set(null);
      this.clearHistory(true);
      this.historyChannel = slot.channel;
      this.historyTarget = slot.targetId;
      this.historyAttempt = slot.attemptId;
      this.decisionKey = '';
      this.decisionReason = '';
      this.decisionConfirmed = false;
      this.selectedAlert.set(null);
      this.alertSlots.set([]);
      this.alertStatus.set('idle');
      this.alertsHaveMore.set(false);
      this.alertNext.set(null);
      this.alertPage.set(1);
      this.alertHistory = [null];
    }
  }

  forgetAlertKey(): void {
    this.alertKey = '';
    this.resendJobId = '';
    this.clearAlerts();
    this.clearRequestLookup(true);
  }

  async loadAlerts(direction?: 'next' | 'previous'): Promise<void> {
    if (this.decisionStatus() === 'sending') return;
    this.expireOidcIfNeeded();
    this.clearDecision();
    if (direction === 'previous' && this.alertPage() <= 1) return;
    const cursor = direction === 'next' ? this.alertNext() : direction === 'previous' ? this.alertHistory[this.alertPage() - 2] : null;
    if (direction === 'next' && !cursor) return;
    if (cursor === undefined) return;
    const nextPage = direction === 'next' ? this.alertPage() + 1 : direction === 'previous' ? this.alertPage() - 1 : 1;
    const id = ++this.alertRequestId;
    this.alertAbort?.abort();
    const controller = new AbortController();
    this.alertAbort = controller;
    this.alertSlots.set([]);
    this.alertError.set('');
    this.alertStatus.set('loading');
    try {
      const origin = this.validatedOrigin();
      const key = this.alertKey.trim();
      const jwt = this.alertAuthMode === 'jwt';
      const environment = this.alertEnvironment.trim();
      if (!this.validAlertKey(key) || (jwt && !this.validEnvironment(environment))) throw new Error('인증 값과 환경을 확인해 주세요.');
      const params = new URLSearchParams();
      if (jwt) params.set('environmentId',environment);
      if (cursor) { params.set('afterChannel',cursor.channel); params.set('afterTarget',cursor.targetId); }
      const query = params.size ? `?${params}` : '';
      const response = await fetch(`${origin}${jwt ? '/v1/experimental/operator/alerts' : '/v1/experimental/console/alerts'}${query}`, {
        method: 'GET', headers: { Authorization: `${jwt ? 'Bearer' : 'SwallowConsole'} ${key}` },
        cache: 'no-store', credentials: 'omit', redirect: 'error',
        signal: AbortSignal.any([controller.signal, AbortSignal.timeout(10000)]),
      });
      if (response.status === 401 || response.status === 403) throw new Error('인증 값 또는 경보 조회 권한을 확인해 주세요.');
      if (!response.ok) throw new Error('경보 상태를 가져오지 못했습니다.');
      const body: unknown = await response.json();
      if (typeof body !== 'object' || body === null || !('data' in body) || !Array.isArray(body.data) || !('hasMore' in body) || typeof body.hasMore !== 'boolean' || body.data.length > 100 || !('next' in body) || !(body.next === null || (typeof body.next === 'object' && 'channel' in body.next && typeof body.next.channel === 'string' && 'targetId' in body.next && typeof body.next.targetId === 'string')) || (body.hasMore !== (body.next !== null))) throw new Error('API 응답 형식을 확인해 주세요.');
      const rows: AlertSlot[] = body.data.map((row: unknown) => {
        if (typeof row !== 'object' || row === null || !('channel' in row) || typeof row.channel !== 'string' || !('targetId' in row) || typeof row.targetId !== 'string' || !('state' in row) || typeof row.state !== 'string' || !('lastStartedAt' in row) || !(typeof row.lastStartedAt === 'string' || row.lastStartedAt === null)) throw new Error('API 응답 형식을 확인해 주세요.');
        if ('attemptId' in row && row.attemptId !== null && (typeof row.attemptId !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(row.attemptId))) throw new Error('API 응답 형식을 확인해 주세요.');
        return { channel: row.channel, targetId: row.targetId, state: row.state, lastStartedAt: row.lastStartedAt, ...('attemptId' in row ? { attemptId: row.attemptId as string | null } : {}) };
      });
      this.expireOidcIfNeeded();
      if (id !== this.alertRequestId) return;
      this.alertSlots.set(rows);
      this.alertsHaveMore.set(body.hasMore);
      this.alertNext.set(body.next as AlertCursor | null);
      this.alertHistory = direction === 'next' ? [...this.alertHistory.slice(0, nextPage - 1), cursor] : direction === 'previous' ? this.alertHistory.slice(0, nextPage) : [null];
      this.alertPage.set(nextPage);
      this.alertStatus.set('loaded');
    } catch (error: unknown) {
      this.expireOidcIfNeeded();
      if (id !== this.alertRequestId) return;
      this.alertStatus.set('error');
      this.alertError.set(error instanceof Error && ['주소', '원격', '인증', '경보', 'API '].some(prefix => error.message.startsWith(prefix)) ? error.message : '서버에 연결할 수 없습니다.');
    } finally {
      if (id === this.alertRequestId) this.alertAbort = null;
    }
  }

  private validatedOrigin(): string {
    const origin = new URL(this.apiOrigin.trim());
    if (!['http:', 'https:'].includes(origin.protocol) || origin.username || origin.password ||
        origin.pathname !== '/' || origin.search || origin.hash) {
      throw new Error('주소는 경로 없이 http 또는 https 출처로 입력해 주세요.');
    }
    if (origin.protocol === 'http:' && !['localhost', '127.0.0.1', '[::1]'].includes(origin.hostname)) {
      throw new Error('원격 서버에는 HTTPS 주소를 사용해 주세요.');
    }
    return origin.origin;
  }

  async loadSubscribers(direction?: 'after' | 'before'): Promise<void> {
    this.expireOidcIfNeeded();
    const cursor = direction === 'after' ? this.nextCursor() : direction === 'before' ? this.previousCursor() : null;
    if (direction && !cursor) return;
    const id = ++this.subscriberRequestId;
    this.subscriberAbort?.abort();
    const controller = new AbortController();
    this.subscriberAbort = controller;
    this.subscriberStatus.set('loading');
    this.subscriberError.set('');
    this.subscribers.set([]);
    this.nextCursor.set(null);
    this.previousCursor.set(null);
    try {
      const origin = this.validatedOrigin();
      const key = this.apiKey.trim();
      const jwt = this.credentialMode === 'jwt';
      const environment = this.subscriberEnvironment.trim();
      if ((jwt ? !this.validJwt(key) || !this.validEnvironment(environment) : this.credentialMode === 'restricted' ? !/^[0-9a-f]{64}$/.test(key) : !key || /[^\x21-\x7e]/.test(key))) throw new Error('인증 값과 환경을 확인해 주세요.');
      const params = new URLSearchParams({ limit: '20' });
      if (jwt) params.set('environmentId', environment);
      const name = this.subscriberName.trim();
      if (name) params.set('name', name);
      if (direction && cursor) params.set(direction, cursor);
      const endpoint = jwt ? '/v1/experimental/operator/subscribers' : this.credentialMode === 'restricted' ? '/v1/experimental/console/subscribers' : '/v2/subscribers';
      const scheme = jwt ? 'Bearer' : this.credentialMode === 'restricted' ? 'SwallowConsole' : 'ApiKey';
      const response = await fetch(`${origin}${endpoint}?${params}`, {
        method: 'GET', headers: { Authorization: `${scheme} ${key}` },
        cache: 'no-store', credentials: 'omit', redirect: 'error',
        signal: AbortSignal.any([controller.signal, AbortSignal.timeout(10000)]),
      });
      if (response.status === 401 || response.status === 403) throw new Error('인증 값 또는 권한을 확인해 주세요.');
      if (!response.ok) throw new Error('구독자 목록을 가져오지 못했습니다.');
      const data: unknown = await response.json();
      if (typeof data !== 'object' || data === null || !('data' in data) || !Array.isArray(data.data) ||
          !('next' in data) || !(data.next === null || typeof data.next === 'string') ||
          !('previous' in data) || !(data.previous === null || typeof data.previous === 'string')) {
        throw new Error('API 응답 형식을 확인해 주세요.');
      }
      const rows: Subscriber[] = data.data.map((value: unknown) => {
        if (typeof value !== 'object' || value === null || !('subscriberId' in value) || typeof value.subscriberId !== 'string') {
          throw new Error('API 응답 형식을 확인해 주세요.');
        }
        const get = (key: string): string => key in value && typeof (value as Record<string, unknown>)[key] === 'string' ? (value as Record<string, string>)[key] : '';
        return { id: value.subscriberId, firstName: get('firstName'), lastName: get('lastName'), email: get('email') };
      });
      this.expireOidcIfNeeded();
      if (id !== this.subscriberRequestId) return;
      this.subscribers.set(rows);
      this.nextCursor.set(data.next);
      this.previousCursor.set(data.previous);
      this.subscriberStatus.set('loaded');
    } catch (error: unknown) {
      this.expireOidcIfNeeded();
      if (id !== this.subscriberRequestId) return;
      this.subscriberStatus.set('error');
      this.subscriberError.set(error instanceof Error && (error.message.startsWith('주소') || error.message.startsWith('원격') || error.message.startsWith('API ') || error.message.startsWith('구독자') || error.message.startsWith('인증')) ? error.message : '서버에 연결할 수 없습니다.');
    } finally {
      if (id === this.subscriberRequestId) this.subscriberAbort = null;
    }
  }

  async checkConnection(): Promise<void> {
    const id = ++this.requestId;
    this.status.set('checking');
    this.detail.set('');
    this.checkedAt.set(null);
    try {
      const origin = this.validatedOrigin();
      const response = await fetch(`${origin}/health/ready`, {
        cache: 'no-store', credentials: 'omit', signal: AbortSignal.timeout(5000),
      });
      const body: unknown = await response.json();
      if (!response.ok || typeof body !== 'object' || body === null || !('status' in body) || body.status !== 'ready') {
        throw new Error('API가 준비 상태를 반환하지 않았습니다.');
      }
      if (id !== this.requestId) return;
      this.status.set('ready');
      this.checkedAt.set(new Intl.DateTimeFormat('ko-KR', { hour: '2-digit', minute: '2-digit', second: '2-digit' }).format(new Date()));
    } catch (error: unknown) {
      if (id !== this.requestId) return;
      this.status.set('unavailable');
      this.detail.set(error instanceof Error && (error.message.startsWith('주소') || error.message.startsWith('원격')) ? error.message : 'API에 연결할 수 없거나 준비 상태가 아닙니다.');
    }
  }
}
