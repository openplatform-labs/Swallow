import { Component, OnDestroy, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ClrAlertModule } from '@clr/angular/emphasis/alert';
import { OperatorOidc, type PreparedOidc, type VerifiedSession } from './operator-oidc';

type ConnectionState = 'unchecked' | 'checking' | 'ready' | 'unavailable';
type SubscriberProfile = {firstName:string;lastName:string;email:string;phone:string};
type SubscriberDeleteBody = {requestId:string;subscriberId:string;generation:string;revision:string;reason:string};
type SubscriberDeleteRetry = {origin:string;environment:string;keyHash:string;body:SubscriberDeleteBody};
type SubscriberDeleteRecord = SubscriberDeleteBody & {createdAt:string};
type SubscriberCreateBody = {requestId:string;subscriberId:string;profile:Record<string,string>;reason:string};
type SubscriberCreateRetry = {origin:string;environment:string;keyHash:string;body:SubscriberCreateBody};
type SubscriberCreateRecord = {requestId:string;subscriberId:string;generation:string;fields:string[];reason:string;createdAt:string};
type SubscriberProfileRequestBody = {requestId:string;subscriberId:string;generation:string;revision:string;profile:Record<string,string|null>;reason:string};
type SubscriberProfileRetry = {origin:string;environment:string;keyHash:string;body:SubscriberProfileRequestBody};
type SubscriberProfileRecord = {requestId:string;subscriberId:string;generation:string;previousRevision:string;revision:string;changed:boolean;fields:string[];reason:string;createdAt:string};
type SubscriberEditSnapshot = {id:string;generation:string;revision:string;profile:SubscriberProfile};
type Subscriber = { id: string; firstName: string; lastName: string; email: string };
type AlertSlot = { channel: string; targetId: string; state: string; lastStartedAt: string | null; attemptId?: string | null };
type AlertCursor = { channel: string; targetId: string };
type ResendResult = { jobId:string; jobStatus:string; result:string; attemptState:string|null; providerIdKnown:boolean; providerIdAmbiguous:boolean; autoCorrelated:boolean; autoCorrelationConflict:boolean; autoCorrelatedAt:string|null; lastCompletedScanAt:string|null };
type WorkflowChannel = 'email'|'sms'|'push'|'slack';
type WorkflowResource = WorkflowChannel|'email-provider';
type WorkflowChangeRequest = {requestId:string;workflow:string;resource:WorkflowResource;expected:unknown;desired:unknown;reason:string};
type WorkflowRetrySnapshot = {origin:string;environment:string;keyHash:string;body:WorkflowChangeRequest};
type WorkflowAudit = {id:string;issuer:string;subject:string;clientId:string;databasePrincipal:string;reason:string;recordedAt:string;previousActive:boolean|null;newActive:boolean|null;previousTitleBytes:number|null;newTitleBytes:number|null;previousContentBytes:number|null;newContentBytes:number|null;previousProvider:string|null;newProvider:string|null};
type DecisionHistory = { id: string; decision: string; reason: string; createdAt: string };
type DecisionRequest = { requestId: string; channel: string; targetId: string; attemptId: string; decision: string; reason: string; createdAt: string };
type RetrySnapshot = { origin: string; endpoint: string; scheme: 'Bearer' | 'SwallowConsole'; requestId: string; channel: string; targetId: string; attemptId: string; decision: string; reason: string; keyHash: string };
type PermissionIdentity = { issuer:string; subject:string; clientId:string; enabled:boolean; subscriberRead:boolean; alertRead:boolean; alertRetry:boolean; alertAccepted:boolean };
type PermissionFlags = { enabled:boolean; subscriberRead:boolean; alertRead:boolean; alertRetry:boolean; alertAccepted:boolean };
type PermissionChange = { origin:string; environment:string; keyHash:string; body:{ requestId:string; targetIssuer:string; targetSubject:string; targetClientId:string; expected:PermissionFlags|null; desired:PermissionFlags; reason:string } };
type GrantBody = {requestId:string;targetIssuer:string;targetSubject:string;targetClientId:string;resource?:string;permission:string;expected:boolean;desired:boolean;reason:string};
type GrantSnapshot = {origin:string;environment:string;keyHash:string;body:GrantBody};
type GrantAudit = {id:string;actorIssuer:string;actorSubject:string;actorClientId:string;databasePrincipal:string;requestId:string;targetIssuer:string;targetSubject:string;targetClientId:string;resource:string;permission:string;previous:boolean;current:boolean;reason:string;recordedAt:string};
type GrantRecord = GrantBody & {changed:boolean;createdAt:string};
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
  workflowKey='';
  workflowAuthMode:'api-key'|'jwt'='api-key';
  workflowEnvironment='';
  workflowReason='';
  providerReason='';
  workflowIdentifier='';
  workflowChannel:WorkflowChannel='email';
  workflowTitle='';
  workflowContent='';
  workflowHasContent=false;
  workflowActive=false;
  workflowConfirmed=false;
  readonly workflowStatus=signal<'idle'|'loading'|'loaded'|'saving'|'saved'|'error'|'unknown'>('idle');
  readonly workflowMessage=signal('');
  private workflowSequence=0;
  private workflowAbort:AbortController|null=null;
  private workflowExpected:{title:string|null;content:string|null;active:boolean}|null=null;
  readonly providerStatus=signal<'idle'|'loading'|'loaded'|'saving'|'saved'|'error'|'unknown'>('idle');
  readonly providerMessage=signal('');
  providerChoice:'resend'|'sendgrid'='sendgrid';
  providerConfirmed=false;
  private providerExpected:'resend'|'sendgrid'|null=null;
  private providerSequence=0;
  private providerAbort:AbortController|null=null;
  workflowAuditResource:WorkflowChannel|'email-provider'='email';
  readonly workflowAuditRows=signal<WorkflowAudit[]>([]);
  readonly workflowAuditStatus=signal<'idle'|'loading'|'loaded'|'error'>('idle');
  readonly workflowAuditNext=signal<string|null>(null);
  readonly workflowAuditPage=signal(1);
  private workflowAuditHistory:(string|null)[]=[null];
  private workflowAuditSequence=0;
  private workflowAuditAbort:AbortController|null=null;
  workflowRequestId='';
  workflowRequestResource:WorkflowResource='email';
  readonly workflowRequestStatus=signal<'idle'|'loading'|'found'|'missing'|'error'>('idle');
  readonly workflowRequestRecord=signal<{workflow:string;resource:string;reason:string;createdAt:string}|null>(null);
  private workflowRequestSequence=0;
  private workflowRequestAbort:AbortController|null=null;
  readonly workflowRetrySnapshot=signal<WorkflowRetrySnapshot|null>(null);
  readonly workflowRetryStatus=signal<'idle'|'unknown'|'retrying'|'recorded'>('idle');
  readonly workflowRetryMessage=signal('');
  workflowRetryConfirmed=false;
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
  readonly subscriberCreateRetry=signal<SubscriberCreateRetry|null>(null);
  readonly subscriberCreateRetryStatus=signal<'idle'|'unknown'|'retrying'|'recorded'>('idle');
  readonly subscriberCreateRetryMessage=signal('');
  subscriberCreateRetryConfirmed=false;
  subscriberCreateRequestId='';
  readonly subscriberCreateRequestStatus=signal<'idle'|'loading'|'found'|'missing'|'error'>('idle');
  readonly subscriberCreateRequestRecord=signal<SubscriberCreateRecord|null>(null);
  private subscriberCreateRequestSequence=0;
  private subscriberCreateRequestAbort:AbortController|null=null;
  createReason='';
  createSubscriberId='';
  createFirstName='';
  createLastName='';
  createEmail='';
  createPhone='';
  createConfirmed=false;
  readonly subscriberProfileRetry=signal<SubscriberProfileRetry|null>(null);
  readonly subscriberProfileRetryStatus=signal<'idle'|'unknown'|'retrying'|'recorded'>('idle');
  readonly subscriberProfileRetryMessage=signal('');
  subscriberProfileRetryConfirmed=false;
  subscriberProfileRequestId='';
  readonly subscriberProfileRequestStatus=signal<'idle'|'loading'|'found'|'missing'|'error'>('idle');
  readonly subscriberProfileRequestRecord=signal<SubscriberProfileRecord|null>(null);
  private subscriberProfileRequestSequence=0;
  private subscriberProfileRequestAbort:AbortController|null=null;
  readonly subscriberDeleteRetry=signal<SubscriberDeleteRetry|null>(null);
  readonly subscriberDeleteRetryStatus=signal<'idle'|'unknown'|'retrying'|'recorded'>('idle');
  readonly subscriberDeleteRetryMessage=signal('');
  subscriberDeleteRetryConfirmed=false;
  subscriberDeleteRequestId='';
  readonly subscriberDeleteRequestStatus=signal<'idle'|'loading'|'found'|'missing'|'error'>('idle');
  readonly subscriberDeleteRequestRecord=signal<SubscriberDeleteRecord|null>(null);
  private subscriberDeleteRequestSequence=0;
  private subscriberDeleteRequestAbort:AbortController|null=null;
  subscriberDeleteReason='';
  subscriberEditId='';
  subscriberEditProfile:SubscriberProfile={firstName:'',lastName:'',email:'',phone:''};
  subscriberEditConfirmed=false;
  subscriberEditReason='';
  subscriberDeleteConfirmation='';
  readonly subscriberEditStatus=signal<'idle'|'loading'|'loaded'|'saving'|'saved'|'deleted'|'conflict'|'unknown'|'error'>('idle');
  readonly subscriberEditMessage=signal('');
  readonly subscriberEditSnapshot=signal<SubscriberEditSnapshot|null>(null);
  private subscriberEditSequence=0;
  private subscriberEditAbort:AbortController|null=null;
  readonly createStatus=signal<'idle'|'sending'|'created'|'rejected'|'unknown'>('idle');
  readonly createMessage=signal('');
  readonly createdSubscriberId=signal('');
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

  grantKind='email:read';
  grantDesired=false;
  grantReason='';
  grantConfirmed=false;
  grantRetryConfirmed=false;
  readonly grantStatus=signal<'idle'|'loading'|'loaded'|'missing'|'error'>('idle');
  readonly grantValue=signal<boolean|null>(null);
  private grantSequence=0;
  private grantAbort:AbortController|null=null;
  readonly grantChangeStatus=signal<'idle'|'sending'|'recorded'|'rejected'|'unknown'|'unavailable'>('idle');
  readonly grantChange=signal<GrantSnapshot|null>(null);
  readonly grantChanged=signal<boolean|null>(null);
  readonly grantMessage=signal('');
  readonly grantAuditStatus=signal<'idle'|'loading'|'loaded'|'error'>('idle');
  readonly grantAuditRows=signal<GrantAudit[]>([]);
  readonly grantAuditNext=signal<string|null>(null);
  readonly grantAuditPage=signal(1);
  private grantAuditHistory:(string|null)[]=[null];
  private grantAuditSequence=0;
  private grantAuditAbort:AbortController|null=null;
  grantRequestId='';
  grantRequestScope:'workflow'|'subscriber'='workflow';
  readonly grantRequestStatus=signal<'idle'|'loading'|'found'|'missing'|'error'>('idle');
  readonly grantRequestRecord=signal<GrantRecord|null>(null);
  private grantRequestSequence=0;
  private grantRequestAbort:AbortController|null=null;

  ngOnDestroy(): void {
    this.clearWorkflow(true);
    this.forgetWorkflowRequest();
    this.oidcSequence++;
    if (this.oidcExpiryTimer !== null) clearTimeout(this.oidcExpiryTimer);
    this.oidcExpiryTimer = null;
    this.oidcSession = null;
    this.alertKey='';
    this.resendJobId='';
    this.clearResend();
    this.clearResendJobs();
    this.clearPermissions(true);
    this.resetSubscriberEdit(true);this.forgetSubscriberProfileRequest();this.forgetSubscriberCreateRequest();this.forgetSubscriberDeleteRequest();
  }

  private expireOidcIfNeeded(): boolean {
    const session=this.oidcSession;
    if (!session || Date.now() < session.expiresAt*1000) return false;
    if (this.oidcExpiryTimer !== null) clearTimeout(this.oidcExpiryTimer);
    this.oidcExpiryTimer=null; this.oidcSession=null; this.oidcSubject.set(''); this.oidcStatus.set('expired');
    // Preserve decision outcome and request ID, including an in-flight or uncertain decision.
    if (this.apiKey===session.token) { this.apiKey=''; this.clearSubscribers(); this.resetSubscriberEdit(); this.clearSubscriberProfileLookup(); this.clearSubscriberCreateLookup(); this.clearSubscriberDeleteLookup(); }
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
    if (this.permissionKey===session.token) { this.permissionKey=''; this.clearPermissions(); }
    if (this.workflowKey===session.token) {
      this.workflowKey='';this.workflowReason='';this.providerReason='';this.workflowSequence++;this.workflowAbort?.abort();this.workflowAbort=null;
      this.providerSequence++;this.providerAbort?.abort();this.providerAbort=null;
      this.clearWorkflowAudit();
      this.clearWorkflowRequestLookup();
      if(this.workflowStatus()!=='saving'){this.workflowStatus.set('idle');this.workflowMessage.set('');this.workflowExpected=null;this.workflowTitle='';this.workflowContent='';this.workflowHasContent=false;this.workflowConfirmed=false;}
      if(this.providerStatus()!=='saving'){this.providerStatus.set('idle');this.providerMessage.set('');this.providerExpected=null;this.providerConfirmed=false;}
    }
    return true;
  }

  private scheduleOidcExpiry(): void {
    if (!this.oidcSession || this.expireOidcIfNeeded()) return;
    const delay=Math.min(2147483647, Math.max(1, this.oidcSession.expiresAt*1000-Date.now()));
    this.oidcExpiryTimer=setTimeout(()=>{ this.oidcExpiryTimer=null; this.scheduleOidcExpiry(); },delay);
  }

  resetOidc(): void {
    if ((this.createStatus()==='sending' || this.subscriberEditStatus()==='saving' || this.subscriberProfileRetryStatus()==='retrying' || this.subscriberCreateRetryStatus()==='retrying' || this.subscriberDeleteRetryStatus()==='retrying') || this.decisionStatus() === 'sending' || this.permissionChangeStatus()==='sending' || this.grantChangeStatus()==='sending' || this.workflowStatus()==='saving' || this.providerStatus()==='saving' || this.workflowRetryStatus()==='retrying') return;
    if (this.oidcExpiryTimer !== null) clearTimeout(this.oidcExpiryTimer);
    this.oidcExpiryTimer=null;
    this.oidcSequence++; this.oidcPrepared = null; this.oidcSession = null;
    this.oidcStatus.set('idle'); this.oidcSubject.set('');
    this.forgetKey(); this.forgetAlertKey(); this.clearPermissions(true);this.clearWorkflow(true);this.forgetWorkflowRequest();
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

  useOidc(section: 'subscribers' | 'alerts' | 'workflows'): void {
    this.expireOidcIfNeeded();
    if (!this.oidcSession || (this.createStatus()==='sending' || this.subscriberEditStatus()==='saving' || this.subscriberProfileRetryStatus()==='retrying' || this.subscriberCreateRetryStatus()==='retrying' || this.subscriberDeleteRetryStatus()==='retrying') || this.decisionStatus() === 'sending' || this.permissionChangeStatus()==='sending' || this.grantChangeStatus()==='sending' || this.workflowStatus()==='saving' || this.providerStatus()==='saving' || this.workflowRetryStatus()==='retrying') return;
    const session=this.oidcSession;
    this.setSection(section);
    if (section === 'subscribers') { this.forgetKey(); this.credentialMode='jwt'; this.subscriberEnvironment=session.environmentId; this.apiKey=session.token; }
    else if (section==='alerts') { this.forgetAlertKey(); this.alertAuthMode='jwt'; this.alertEnvironment=session.environmentId; this.alertKey=session.token; }
    else {this.clearWorkflow(true);this.workflowAuthMode='jwt';this.workflowEnvironment=session.environmentId;this.workflowKey=session.token;}
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
    if (!this.oidcSession || this.decisionStatus()==='sending' || this.permissionChangeStatus()==='sending' || this.grantChangeStatus()==='sending') return;
    const session=this.oidcSession;
    this.clearPermissions(true);this.permissionEnvironment=session.environmentId;this.permissionKey=session.token;
  }

  clearPermissions(clearKey = false): void {
    this.permissionSequence++;this.permissionAbort?.abort();this.permissionAbort=null;
    this.permissionStatus.set('idle');this.permissionIdentity.set(null);this.permissionAudit.set([]);this.permissionNext.set(null);
    this.clearPermissionRequest(clearKey);
    this.clearGrantRead();this.clearGrantRequest(clearKey);this.clearGrantAudit();
    if (clearKey) this.permissionKey='';
  }

  clearPermissionRequest(clearInput=false):void {
    this.permissionRequestSequence++;this.permissionRequestAbort?.abort();this.permissionRequestAbort=null;
    this.permissionRequestStatus.set('idle');this.permissionRequestRecord.set(null);
    if(clearInput)this.permissionRequestId='';
  }

  async lookupPermissionRequest():Promise<void> {
    if(this.permissionChangeStatus()==='sending'||this.grantChangeStatus()==='sending')return;
    this.expireOidcIfNeeded();this.clearPermissionRequest();const sequence=this.permissionRequestSequence;
    const key=this.permissionKey.trim(),env=this.permissionEnvironment.trim(),requestId=this.permissionRequestId.trim();
    if(!this.validJwt(key)||!this.validEnvironment(env)||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(requestId)){this.permissionRequestStatus.set('error');return;}
    const controller=new AbortController();this.permissionRequestAbort=controller;this.permissionRequestStatus.set('loading');
    try {
      const params=new URLSearchParams({environmentId:env,requestId});
      const response=await fetch(`${this.validatedOrigin()}/v1/experimental/operator/permissions/request?${params}`,{headers:{Authorization:`Bearer ${key}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if(!response.ok)throw new Error('request unavailable');
      const value:unknown=await response.json();if(typeof value!=='object'||value===null||!('found' in value)||typeof value.found!=='boolean')throw new Error('invalid response');
      this.expireOidcIfNeeded();
      if(sequence!==this.permissionRequestSequence)return;
      if(!value.found){this.permissionRequestStatus.set('missing');return;}
      const r=value as Record<string,unknown>;
      if(typeof r['requestId']!=='string'||r['requestId'].toLowerCase()!==requestId.toLowerCase()||['targetIssuer','targetSubject','targetClientId','reason','createdAt'].some(k=>typeof r[k]!=='string')||typeof r['changed']!=='boolean'||!(r['expected']===null||this.isPermissionFlags(r['expected']))||!this.isPermissionFlags(r['desired']))throw new Error('invalid response');
      this.permissionRequestRecord.set({requestId:r['requestId'],targetIssuer:r['targetIssuer'] as string,targetSubject:r['targetSubject'] as string,targetClientId:r['targetClientId'] as string,expected:r['expected'] as PermissionFlags|null,desired:r['desired'],reason:r['reason'] as string,changed:r['changed'],createdAt:r['createdAt'] as string});this.permissionRequestStatus.set('found');
    } catch {this.expireOidcIfNeeded();if(sequence===this.permissionRequestSequence)this.permissionRequestStatus.set('error');}
    finally {if(sequence===this.permissionRequestSequence)this.permissionRequestAbort=null;}
  }

  clearGrantAudit():void {
    this.grantAuditSequence++;this.grantAuditAbort?.abort();this.grantAuditAbort=null;this.grantAuditRows.set([]);this.grantAuditNext.set(null);this.grantAuditPage.set(1);this.grantAuditHistory=[null];this.grantAuditStatus.set('idle');
  }
  async loadGrantAudit(direction?:'next'|'previous'):Promise<void> {
    this.expireOidcIfNeeded();if(this.grantChangeStatus()==='sending'||this.permissionChangeStatus()==='sending')return;
    if(direction==='previous'&&this.grantAuditPage()<=1)return;
    const cursor=direction==='next'?this.grantAuditNext():direction==='previous'?this.grantAuditHistory[this.grantAuditPage()-2]:null;
    if((direction==='next'&&!cursor)||cursor===undefined)return;
    const page=direction==='next'?this.grantAuditPage()+1:direction==='previous'?this.grantAuditPage()-1:1;
    const sequence=++this.grantAuditSequence;this.grantAuditAbort?.abort();const controller=new AbortController();this.grantAuditAbort=controller;this.grantAuditRows.set([]);this.grantAuditNext.set(null);this.grantAuditStatus.set('loading');
    try {
      const key=this.permissionKey.trim(),environment=this.permissionEnvironment.trim();if(!this.validJwt(key)||!this.validEnvironment(environment))throw new Error('invalid input');
      const params=new URLSearchParams({environmentId:environment});if(cursor)params.set('after',cursor);
      const response=await fetch(`${this.validatedOrigin()}/v1/experimental/operator/workflow-grants/audit?${params}`,{headers:{Authorization:`Bearer ${key}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if(!response.ok)throw new Error('request failed');const value:unknown=await response.json();
      if(typeof value!=='object'||value===null)throw new Error('invalid response');const result=value as Record<string,unknown>;
      if(!Array.isArray(result['data'])||result['data'].length>50||typeof result['hasMore']!=='boolean'||!(result['next']===null||typeof result['next']==='string'))throw new Error('invalid response');
      const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
      const rows:GrantAudit[]=result['data'].map((entry:unknown)=>{
        if(typeof entry!=='object'||entry===null)throw new Error('invalid row');const r=entry as Record<string,unknown>;
        if(['id','actorIssuer','actorSubject','actorClientId','databasePrincipal','requestId','targetIssuer','targetSubject','targetClientId','resource','permission','reason','recordedAt'].some(k=>typeof r[k]!=='string')||!uuid.test(r['id'] as string)||!uuid.test(r['requestId'] as string)||typeof r['previous']!=='boolean'||typeof r['current']!=='boolean'||!(/^(email|sms|push|slack|email-provider):(read|write)$/.test(`${r['resource']}:${r['permission']}`)||`${r['resource']}:${r['permission']}`==='audit:read'))throw new Error('invalid row');
        return {id:r['id'] as string,actorIssuer:r['actorIssuer'] as string,actorSubject:r['actorSubject'] as string,actorClientId:r['actorClientId'] as string,databasePrincipal:r['databasePrincipal'] as string,requestId:r['requestId'] as string,targetIssuer:r['targetIssuer'] as string,targetSubject:r['targetSubject'] as string,targetClientId:r['targetClientId'] as string,resource:r['resource'] as string,permission:r['permission'] as string,previous:r['previous'],current:r['current'],reason:r['reason'] as string,recordedAt:r['recordedAt'] as string};
      });
      const ids=rows.map(r=>r.id.toLowerCase());if(ids.some((id,i)=>(i>0&&ids[i-1]>=id)||(!!cursor&&id<=cursor.toLowerCase()))||(result['hasMore']?rows.length!==50||typeof result['next']!=='string'||result['next'].toLowerCase()!==ids[ids.length-1]:result['next']!==null))throw new Error('invalid page');
      this.expireOidcIfNeeded();if(sequence!==this.grantAuditSequence)return;
      this.grantAuditRows.set(rows);this.grantAuditNext.set(result['next'] as string|null);this.grantAuditPage.set(page);this.grantAuditHistory=this.grantAuditHistory.slice(0,page-1);this.grantAuditHistory.push(cursor);this.grantAuditStatus.set('loaded');
    }catch{this.expireOidcIfNeeded();if(sequence===this.grantAuditSequence)this.grantAuditStatus.set('error');}
    finally{if(sequence===this.grantAuditSequence)this.grantAuditAbort=null;}
  }
  clearGrantRead():void {
    this.grantSequence++;this.grantAbort?.abort();this.grantAbort=null;
    this.grantStatus.set('idle');this.grantValue.set(null);this.grantConfirmed=false;
  }
  clearGrantRequest(clearInput=false):void {
    this.grantRequestSequence++;this.grantRequestAbort?.abort();this.grantRequestAbort=null;
    this.grantRequestStatus.set('idle');this.grantRequestRecord.set(null);
    if(clearInput)this.grantRequestId='';
  }
  clearGrantChange():void {
    if(this.grantChangeStatus()==='sending')return;
    this.grantChange.set(null);this.grantChangeStatus.set('idle');this.grantChanged.set(null);this.grantMessage.set('');
    this.grantReason='';this.grantConfirmed=false;this.grantRetryConfirmed=false;
  }
  private grantSelection():[string,string] {
    if(!/^(email|sms|push|slack|email-provider):(read|write)$/.test(this.grantKind) && !/^subscriber:(read|write|create|delete)$/.test(this.grantKind) && this.grantKind!=='audit:read')throw new Error('invalid grant');
    const [resource,permission]=this.grantKind.split(':');return [resource,permission];
  }
  private grantInputs():{origin:string;key:string;environment:string;targetIssuer:string;targetSubject:string;targetClientId:string;resource:string;permission:string} {
    const origin=this.validatedOrigin(),key=this.permissionKey.trim(),environment=this.permissionEnvironment.trim();
    const targetIssuer=this.permissionIssuer.trim(),targetSubject=this.permissionSubject.trim(),targetClientId=this.permissionClient.trim();
    const bytes=(s:string)=>new TextEncoder().encode(s).length;
    if(!this.validJwt(key)||!this.validEnvironment(environment)||!targetIssuer||bytes(targetIssuer)>2048||!targetSubject||bytes(targetSubject)>1024||!targetClientId||bytes(targetClientId)>1024)throw new Error('invalid input');
    const [resource,permission]=this.grantSelection();
    return {origin,key,environment,targetIssuer,targetSubject,targetClientId,resource,permission};
  }
  async loadGrant():Promise<void> {
    if(this.grantChangeStatus()==='sending'||this.permissionChangeStatus()==='sending')return;
    this.expireOidcIfNeeded();this.clearGrantRead();const sequence=this.grantSequence;
    const controller=new AbortController();this.grantAbort=controller;this.grantStatus.set('loading');
    try {
      const {origin,key,environment,targetIssuer,targetSubject,targetClientId,resource,permission}=this.grantInputs();
      const params=new URLSearchParams({environmentId:environment,targetIssuer,targetSubject,targetClientId,permission});if(resource!=='subscriber')params.set('resource',resource);
      const response=await fetch(`${origin}/v1/experimental/operator/${resource==='subscriber'?'subscriber-grants':'workflow-grants'}?${params}`,{headers:{Authorization:`Bearer ${key}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if(!response.ok)throw new Error('request failed');const value:unknown=await response.json();
      if(typeof value!=='object'||value===null||!('targetExists' in value)||typeof value.targetExists!=='boolean'||!('granted' in value)||typeof value.granted!=='boolean')throw new Error('invalid response');
      this.expireOidcIfNeeded();if(sequence!==this.grantSequence)return;
      if(!value.targetExists){this.grantStatus.set('missing');return;}
      this.grantValue.set(value.granted);this.grantDesired=value.granted;this.grantStatus.set('loaded');
    } catch {this.expireOidcIfNeeded();if(sequence===this.grantSequence)this.grantStatus.set('error');}
    finally {if(sequence===this.grantSequence)this.grantAbort=null;}
  }
  canChangeGrant():boolean {
    const reason=this.grantReason.trim();
    return this.grantChangeStatus()==='idle'&&this.permissionChangeStatus()!=='sending'&&this.grantStatus()==='loaded'&&this.grantValue()!==null&&this.grantConfirmed&&!!reason&&new TextEncoder().encode(reason).length<=1000;
  }
  async changeGrant():Promise<void> {
    this.expireOidcIfNeeded();if(!this.canChangeGrant())return;
    let input:ReturnType<App['grantInputs']>;let requestId:string;
    try {input=this.grantInputs();requestId=crypto.randomUUID();}catch{this.grantChangeStatus.set('unavailable');return;}
    const {origin,key,environment,targetIssuer,targetSubject,targetClientId,resource,permission}=input;
    const body:GrantBody={requestId,targetIssuer,targetSubject,targetClientId,...(resource==='subscriber'?{}:{resource}),permission,expected:this.grantValue()!,desired:this.grantDesired,reason:this.grantReason.trim()};this.grantRequestScope=resource==='subscriber'?'subscriber':'workflow';
    this.grantChangeStatus.set('sending');this.grantMessage.set('');
    try {
      let keyHash:string;try{keyHash=await this.credentialFingerprint(key);}catch{this.grantChangeStatus.set('unavailable');return;}
      this.expireOidcIfNeeded();if(this.permissionKey.trim()!==key||this.permissionEnvironment.trim()!==environment||this.validatedOrigin()!==origin){this.grantChangeStatus.set('unavailable');return;}
      this.grantChange.set({origin,environment,keyHash,body});
      await this.sendGrant(key,{origin,environment,keyHash,body});
    }catch{this.grantChangeStatus.set('unknown');}
    finally{this.grantReason='';this.grantConfirmed=false;this.clearGrantRead();}
  }
  private async sendGrant(key:string,snapshot:GrantSnapshot,retry=false):Promise<void> {
    const params=new URLSearchParams({environmentId:snapshot.environment});
    const response=await fetch(`${snapshot.origin}/v1/experimental/operator/${snapshot.body.resource===undefined?'subscriber-grants':'workflow-grants'}?${params}`,{method:'PUT',headers:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'},body:JSON.stringify(snapshot.body),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
    if(!response.ok){this.grantChangeStatus.set(!retry&&[400,401,403,409].includes(response.status)?'rejected':'unknown');if(retry)this.grantMessage.set('재제출이 거절되거나 실패했습니다. 원래 요청의 결과는 여전히 불명확합니다.');return;}
    const value:unknown=await response.json();
    if(typeof value!=='object'||value===null||!('changed' in value)||typeof value.changed!=='boolean'||!('replayed' in value)||typeof value.replayed!=='boolean'||!('requestId' in value)||value.requestId!==snapshot.body.requestId)throw new Error('invalid response');
    this.grantChanged.set(value.changed);this.grantChangeStatus.set('recorded');
  }
  async retryGrant():Promise<void> {
    this.expireOidcIfNeeded();const snapshot=this.grantChange(),key=this.permissionKey.trim();
    if(!snapshot||this.grantChangeStatus()!=='unknown'||!this.grantRetryConfirmed||!this.validJwt(key))return;
    this.grantChangeStatus.set('sending');this.grantMessage.set('');
    try {
      const keyHash=await this.credentialFingerprint(key);this.expireOidcIfNeeded();
      if(this.permissionKey.trim()!==key||keyHash!==snapshot.keyHash||this.permissionEnvironment.trim()!==snapshot.environment||this.validatedOrigin()!==snapshot.origin){this.grantMessage.set('원래 요청과 같은 토큰, 환경 및 API 주소가 필요합니다.');return;}
      await this.sendGrant(key,snapshot,true);
    }catch{this.grantMessage.set('결과가 여전히 불명확합니다. 요청 ID로 조회하세요.');}
    finally{this.grantRetryConfirmed=false;if(this.grantChangeStatus()==='sending')this.grantChangeStatus.set('unknown');}
  }
  async lookupGrantRequest():Promise<void> {
    if(this.grantChangeStatus()==='sending'||this.permissionChangeStatus()==='sending')return;
    this.expireOidcIfNeeded();this.clearGrantRequest();const sequence=this.grantRequestSequence;
    const key=this.permissionKey.trim(),environment=this.permissionEnvironment.trim(),requestId=this.grantRequestId.trim(),scope=this.grantRequestScope;
    if(!this.validJwt(key)||!this.validEnvironment(environment)||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(requestId)){this.grantRequestStatus.set('error');return;}
    const controller=new AbortController();this.grantRequestAbort=controller;this.grantRequestStatus.set('loading');
    try {
      const params=new URLSearchParams({environmentId:environment,requestId});
      const response=await fetch(`${this.validatedOrigin()}/v1/experimental/operator/${scope==='subscriber'?'subscriber-grants':'workflow-grants'}/request?${params}`,{headers:{Authorization:`Bearer ${key}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if(!response.ok)throw new Error('request failed');const value:unknown=await response.json();
      if(typeof value!=='object'||value===null||!('found' in value)||typeof value.found!=='boolean')throw new Error('invalid response');
      this.expireOidcIfNeeded();if(sequence!==this.grantRequestSequence)return;
      if(!value.found){this.grantRequestStatus.set('missing');return;}
      const r=value as Record<string,unknown>;
      if(typeof r['requestId']!=='string'||r['requestId'].toLowerCase()!==requestId.toLowerCase()||['targetIssuer','targetSubject','targetClientId','permission','reason','createdAt'].some(k=>typeof r[k]!=='string')||(scope==='workflow'&&typeof r['resource']!=='string')||['expected','desired','changed'].some(k=>typeof r[k]!=='boolean'))throw new Error('invalid response');
      this.grantRequestRecord.set({requestId:r['requestId'] as string,targetIssuer:r['targetIssuer'] as string,targetSubject:r['targetSubject'] as string,targetClientId:r['targetClientId'] as string,resource:scope==='subscriber'?'subscriber':r['resource'] as string,permission:r['permission'] as string,expected:r['expected'] as boolean,desired:r['desired'] as boolean,reason:r['reason'] as string,changed:r['changed'] as boolean,createdAt:r['createdAt'] as string});this.grantRequestStatus.set('found');
    }catch{this.expireOidcIfNeeded();if(sequence===this.grantRequestSequence)this.grantRequestStatus.set('error');}
    finally{if(sequence===this.grantRequestSequence)this.grantRequestAbort=null;}
  }

  private isPermissionFlags(value:unknown):value is PermissionFlags {
    if(typeof value!=='object'||value===null)return false;
    const record=value as Record<string,unknown>;
    return ['enabled','subscriberRead','alertRead','alertRetry','alertAccepted'].every(key=>typeof record[key]==='boolean');
  }

  async loadPermissions(mode:'identity'|'audit', next=false): Promise<void> {
    if(this.permissionChangeStatus()==='sending'||this.grantChangeStatus()==='sending')return;
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
      this.expireOidcIfNeeded();if(sequence!==this.permissionSequence)return;
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
    } catch {this.expireOidcIfNeeded();if(sequence===this.permissionSequence)this.permissionStatus.set('error');}
    finally {if(sequence===this.permissionSequence)this.permissionAbort=null;}
  }

  clearPermissionChange(): void {
    if(this.permissionChangeStatus()==='sending')return;
    this.permissionChange.set(null);this.permissionChangeStatus.set('idle');this.permissionChangeChanged.set(null);this.permissionChangeMessage.set('');this.permissionReason='';this.permissionConfirmed=false;this.permissionRecoveryConfirmed=false;
  }

  canChangePermissions(): boolean {
    const reason=this.permissionReason.trim();
    return this.permissionChangeStatus()==='idle' && this.grantChangeStatus()!=='sending' && (this.permissionStatus()==='identity'||this.permissionStatus()==='missing') && this.permissionConfirmed && this.validJwt(this.permissionKey.trim()) && this.validEnvironment(this.permissionEnvironment.trim()) && !!reason && new TextEncoder().encode(reason).length<=1000;
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
      this.expireOidcIfNeeded();if(this.permissionKey.trim()!==key){this.permissionChangeStatus.set('unavailable');return;}
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
      const keyHash=await this.credentialFingerprint(key);this.expireOidcIfNeeded();
      if(this.validatedOrigin()!==snapshot.origin || this.permissionEnvironment.trim()!==snapshot.environment || this.permissionKey.trim()!==key || keyHash!==snapshot.keyHash) {this.permissionChangeMessage.set('원래 요청과 같은 인증, 환경 및 API 주소가 필요합니다.');return;}
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

  clearWorkflow(clearKey=false): void {
    this.clearWorkflowAudit();
    this.clearWorkflowRequestLookup();
    this.workflowSequence++;this.workflowAbort?.abort();this.workflowAbort=null;
    this.workflowStatus.set('idle');this.workflowMessage.set('');this.workflowTitle='';this.workflowContent='';this.workflowHasContent=false;this.workflowActive=false;this.workflowConfirmed=false;
    this.workflowExpected=null;
    this.workflowReason='';this.providerReason='';
    this.providerSequence++;this.providerAbort?.abort();this.providerAbort=null;
    this.providerStatus.set('idle');this.providerMessage.set('');this.providerExpected=null;this.providerConfirmed=false;this.providerChoice='sendgrid';
    if(clearKey){this.workflowKey='';this.workflowIdentifier='';}
  }

  clearWorkflowAudit():void {
    this.workflowAuditSequence++;this.workflowAuditAbort?.abort();this.workflowAuditAbort=null;this.workflowAuditRows.set([]);this.workflowAuditStatus.set('idle');this.workflowAuditNext.set(null);this.workflowAuditPage.set(1);this.workflowAuditHistory=[null];
  }

  async loadWorkflowAudit(direction?:'next'|'previous'):Promise<void> {
    this.expireOidcIfNeeded();
    if(this.workflowStatus()==='saving'||this.providerStatus()==='saving'||this.workflowAuthMode!=='jwt')return;
    if(direction==='previous'&&this.workflowAuditPage()<=1)return;
    const cursor=direction==='next'?this.workflowAuditNext():direction==='previous'?this.workflowAuditHistory[this.workflowAuditPage()-2]:null;
    if((direction==='next'&&!cursor)||cursor===undefined)return;
    const page=direction==='next'?this.workflowAuditPage()+1:direction==='previous'?this.workflowAuditPage()-1:1;
    const sequence=++this.workflowAuditSequence;this.workflowAuditAbort?.abort();const controller=new AbortController();this.workflowAuditAbort=controller;
    this.workflowAuditRows.set([]);this.workflowAuditNext.set(null);this.workflowAuditStatus.set('loading');
    try {
      this.jwtWorkflowEndpoint(this.workflowAuditResource);const environment=this.workflowEnvironment.trim(),identifier=this.workflowIdentifier.trim(),resource=this.workflowAuditResource;
      const params=new URLSearchParams({environmentId:environment,workflow:identifier,resource});if(cursor)params.set('after',cursor);
      const response=await fetch(`${this.validatedOrigin()}/v1/experimental/operator/workflow-audit?${params}`,{headers:{Authorization:this.workflowAuthorization()},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if(!response.ok)throw new Error('request failed');const value:unknown=await response.json();
      if(typeof value!=='object'||value===null)throw new Error('invalid response');const result=value as Record<string,unknown>;
      if(!Array.isArray(result['data'])||result['data'].length>50||typeof result['hasMore']!=='boolean'||!(result['next']===null||typeof result['next']==='string'))throw new Error('invalid response');
      const rows:WorkflowAudit[]=result['data'].map((value:unknown)=>{
        if(typeof value!=='object'||value===null)throw new Error('invalid row');const row=value as Record<string,unknown>;
        const required=['id','issuer','subject','clientId','databasePrincipal','reason','recordedAt'];
        const nullableBoolean=['previousActive','newActive'], nullableNumber=['previousTitleBytes','newTitleBytes','previousContentBytes','newContentBytes'], nullableString=['previousProvider','newProvider'];
        if(required.some(k=>typeof row[k]!=='string')||row['workflowIdentifier']!==identifier||row['resource']!==resource||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(row['id'] as string)||nullableBoolean.some(k=>!(row[k]===null||typeof row[k]==='boolean'))||nullableNumber.some(k=>!(row[k]===null||(typeof row[k]==='number'&&Number.isSafeInteger(row[k])&&(row[k] as number)>=0)))||nullableString.some(k=>!(row[k]===null||row[k]==='resend'||row[k]==='sendgrid')))throw new Error('invalid row');
        return {id:row['id'] as string,issuer:row['issuer'] as string,subject:row['subject'] as string,clientId:row['clientId'] as string,databasePrincipal:row['databasePrincipal'] as string,reason:row['reason'] as string,recordedAt:row['recordedAt'] as string,previousActive:row['previousActive'] as boolean|null,newActive:row['newActive'] as boolean|null,previousTitleBytes:row['previousTitleBytes'] as number|null,newTitleBytes:row['newTitleBytes'] as number|null,previousContentBytes:row['previousContentBytes'] as number|null,newContentBytes:row['newContentBytes'] as number|null,previousProvider:row['previousProvider'] as string|null,newProvider:row['newProvider'] as string|null};
      });
      const ids=rows.map(row=>row.id.toLowerCase());
      if(ids.some((id,i)=>(i>0&&ids[i-1]>=id)||(!!cursor&&id<=cursor.toLowerCase())) || (result['hasMore'] ? rows.length!==50 || typeof result['next']!=='string' || result['next'].toLowerCase()!==ids[ids.length-1] : result['next']!==null))throw new Error('invalid page');
      this.expireOidcIfNeeded();if(sequence!==this.workflowAuditSequence)return;
      this.workflowAuditRows.set(rows);this.workflowAuditNext.set(result['next'] as string|null);this.workflowAuditPage.set(page);this.workflowAuditHistory=this.workflowAuditHistory.slice(0,page-1);this.workflowAuditHistory.push(cursor);this.workflowAuditStatus.set('loaded');
    } catch {this.expireOidcIfNeeded();if(sequence===this.workflowAuditSequence)this.workflowAuditStatus.set('error');}
    finally {if(sequence===this.workflowAuditSequence)this.workflowAuditAbort=null;}
  }

  private providerEndpoint(write=false):string {
    if(this.workflowAuthMode==='jwt')return this.jwtWorkflowEndpoint('email-provider',write);
    this.workflowEndpoint();
    return `${this.validatedOrigin()}/v1/experimental/workflows/${encodeURIComponent(this.workflowIdentifier.trim())}/email-provider`;
  }

  private jwtWorkflowEndpoint(resource:string,write=false):string {
    const identifier=this.workflowIdentifier.trim(),environment=this.workflowEnvironment.trim();
    if(!/^[A-Za-z0-9_-][A-Za-z0-9._-]{0,255}$/.test(identifier)||!this.validEnvironment(environment)||!this.validJwt(this.workflowKey.trim()))throw new Error('운영자 인증과 워크플로 식별자를 확인해 주세요.');
    const params=new URLSearchParams({environmentId:environment});if(!write){params.set('workflow',identifier);params.set('resource',resource);}
    return `${this.validatedOrigin()}/v1/experimental/operator/workflow?${params}`;
  }

  private workflowAuthorization():string {return `${this.workflowAuthMode==='jwt'?'Bearer':'ApiKey'} ${this.workflowKey.trim()}`;}
  private validWorkflowReason(reason:string):boolean {return !!reason.trim() && !reason.includes('\0') && new TextEncoder().encode(reason.trim()).length<=1000;}

  clearWorkflowRequestLookup():void {this.workflowRequestSequence++;this.workflowRequestAbort?.abort();this.workflowRequestAbort=null;this.workflowRequestStatus.set('idle');this.workflowRequestRecord.set(null);}
  forgetWorkflowRequest():void {if(this.workflowRetryStatus()==='retrying')return;this.clearWorkflowRequestLookup();this.workflowRetrySnapshot.set(null);this.workflowRetryStatus.set('idle');this.workflowRetryMessage.set('');this.workflowRetryConfirmed=false;this.workflowRequestId='';}

  private async submitJwtWorkflow(body:Omit<WorkflowChangeRequest,'requestId'>):Promise<'saved'|'rejected'|'unknown'|'unavailable'> {
    if(this.workflowRetrySnapshot())return 'unavailable';
    this.workflowRetryStatus.set('idle');this.workflowRequestId='';this.clearWorkflowRequestLookup();
    const key=this.workflowKey.trim(), environment=this.workflowEnvironment.trim();let origin:string,requestId:string,keyHash:string;
    try {origin=this.validatedOrigin();requestId=crypto.randomUUID();keyHash=await this.credentialFingerprint(key);}catch{return 'unavailable';}
    this.expireOidcIfNeeded();if(this.workflowKey.trim()!==key||this.workflowAuthMode!=='jwt'||!this.validJwt(key))return 'unavailable';
    const snapshot:WorkflowRetrySnapshot={origin,environment,keyHash,body:{...body,requestId}};
    this.workflowRetrySnapshot.set(snapshot);this.workflowRetryStatus.set('unknown');this.workflowRetryMessage.set('');this.workflowRequestId=requestId;this.workflowRequestResource=body.resource;this.clearWorkflowRequestLookup();
    try {
      const response=await fetch(`${origin}/v1/experimental/operator/workflow-v2?${new URLSearchParams({environmentId:environment})}`,{method:'PUT',headers:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'},body:JSON.stringify(snapshot.body),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
      this.expireOidcIfNeeded();
      if(!response.ok){if([400,401,403,404,409].includes(response.status)){this.workflowRetrySnapshot.set(null);this.workflowRetryStatus.set('idle');return 'rejected';}return 'unknown';}
      const value:unknown=await response.json();this.expireOidcIfNeeded();
      if(typeof value!=='object'||value===null||!('recorded' in value)||value.recorded!==true||!('replayed' in value)||typeof value.replayed!=='boolean'||!('requestId' in value)||value.requestId!==requestId)throw new Error('invalid response');
      this.workflowRetrySnapshot.set(null);this.workflowRetryStatus.set('recorded');return 'saved';
    } catch {this.expireOidcIfNeeded();return 'unknown';}
  }

  async retryWorkflowRequest():Promise<void> {
    this.expireOidcIfNeeded();const snapshot=this.workflowRetrySnapshot(),key=this.workflowKey.trim();
    if(!snapshot||this.workflowRetryStatus()!=='unknown'||!this.workflowRetryConfirmed||this.workflowAuthMode!=='jwt'||!this.validJwt(key))return;
    this.workflowRetryStatus.set('retrying');this.workflowRetryMessage.set('');
    try {
      const fingerprint=await this.credentialFingerprint(key);this.expireOidcIfNeeded();
      if(this.workflowKey.trim()!==key||fingerprint!==snapshot.keyHash||this.validatedOrigin()!==snapshot.origin||this.workflowEnvironment.trim()!==snapshot.environment){this.workflowRetryMessage.set('원래 요청과 같은 토큰, 환경과 API 주소가 필요합니다.');return;}
      const response=await fetch(`${snapshot.origin}/v1/experimental/operator/workflow-v2?${new URLSearchParams({environmentId:snapshot.environment})}`,{method:'PUT',headers:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'},body:JSON.stringify(snapshot.body),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
      this.expireOidcIfNeeded();if(!response.ok)throw new Error('retry unavailable');const value:unknown=await response.json();this.expireOidcIfNeeded();
      if(typeof value!=='object'||value===null||!('recorded' in value)||value.recorded!==true||!('replayed' in value)||typeof value.replayed!=='boolean'||!('requestId' in value)||value.requestId!==snapshot.body.requestId)throw new Error('invalid response');
      this.workflowRetrySnapshot.set(null);this.workflowRetryStatus.set('recorded');
      if(this.workflowIdentifier.trim()===snapshot.body.workflow){if(snapshot.body.resource==='email-provider'&&this.providerStatus()==='unknown')this.providerStatus.set('saved');else if(snapshot.body.resource!=='email-provider'&&this.workflowStatus()==='unknown')this.workflowStatus.set('saved');}
    } catch {this.expireOidcIfNeeded();this.workflowRetryMessage.set('결과가 여전히 불명확합니다. 요청 조회와 현재 설정을 확인하세요.');}
    finally {this.workflowRetryConfirmed=false;if(this.workflowRetryStatus()==='retrying')this.workflowRetryStatus.set('unknown');}
  }

  async lookupWorkflowRequest():Promise<void> {
    this.expireOidcIfNeeded();if(this.workflowStatus()==='saving'||this.providerStatus()==='saving'||this.workflowRetryStatus()==='retrying')return;
    this.clearWorkflowRequestLookup();const sequence=this.workflowRequestSequence,key=this.workflowKey.trim(),environment=this.workflowEnvironment.trim(),id=this.workflowRequestId.trim(),resource=this.workflowRequestResource;
    if(this.workflowAuthMode!=='jwt'||!this.validJwt(key)||!this.validEnvironment(environment)||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)||!['email','sms','push','slack','email-provider'].includes(resource)){this.workflowRequestStatus.set('error');return;}
    const controller=new AbortController();this.workflowRequestAbort=controller;this.workflowRequestStatus.set('loading');
    try {
      const params=new URLSearchParams({environmentId:environment,resource,requestId:id});const response=await fetch(`${this.validatedOrigin()}/v1/experimental/operator/workflow-request?${params}`,{headers:{Authorization:`Bearer ${key}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if(!response.ok)throw new Error('lookup failed');const value:unknown=await response.json();this.expireOidcIfNeeded();if(sequence!==this.workflowRequestSequence)return;
      if(typeof value!=='object'||value===null||!('found' in value)||typeof value.found!=='boolean')throw new Error('invalid response');
      if(!value.found){this.workflowRequestStatus.set('missing');return;}
      if(!('requestId' in value)||typeof value.requestId!=='string'||value.requestId.toLowerCase()!==id.toLowerCase()||!('workflow' in value)||typeof value.workflow!=='string'||!('resource' in value)||value.resource!==resource||!('reason' in value)||typeof value.reason!=='string'||!('createdAt' in value)||typeof value.createdAt!=='string')throw new Error('invalid response');
      this.workflowRequestRecord.set({workflow:value.workflow,resource,reason:value.reason,createdAt:value.createdAt});this.workflowRequestStatus.set('found');
    } catch {this.expireOidcIfNeeded();if(sequence===this.workflowRequestSequence)this.workflowRequestStatus.set('error');}
    finally {if(sequence===this.workflowRequestSequence)this.workflowRequestAbort=null;}
  }

  async loadProvider():Promise<void> {
    this.expireOidcIfNeeded();
    if(this.workflowStatus()==='saving'||this.providerStatus()==='saving')return;
    const sequence=++this.providerSequence;this.providerAbort?.abort();const controller=new AbortController();this.providerAbort=controller;
    this.providerExpected=null;this.providerConfirmed=false;this.providerMessage.set('');this.providerStatus.set('loading');
    try {
      const endpoint=this.providerEndpoint(),identifier=this.workflowIdentifier.trim();
      const response=await fetch(endpoint,{headers:{Authorization:this.workflowAuthorization()},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if(!response.ok)throw new Error('request failed');const value:unknown=await response.json();
      this.expireOidcIfNeeded();
      if(sequence!==this.providerSequence)return;
      if(typeof value!=='object'||value===null||!('data' in value)||typeof value.data!=='object'||value.data===null)throw new Error('invalid response');
      const data=value.data as Record<string,unknown>;
      if(data['workflowIdentifier']!==identifier||(data['provider']!=='resend'&&data['provider']!=='sendgrid'))throw new Error('invalid response');
      this.providerExpected=data['provider'];this.providerChoice=data['provider'];this.providerStatus.set('loaded');
    } catch {this.expireOidcIfNeeded();if(sequence===this.providerSequence){this.providerStatus.set('error');this.providerMessage.set('공급자를 조회하지 못했습니다. 식별자와 공급자 읽기 권한을 확인하세요.');}}
    finally {if(sequence===this.providerSequence)this.providerAbort=null;}
  }

  async saveProvider():Promise<void> {
    this.expireOidcIfNeeded();
    if(this.providerStatus()!=='loaded'||!this.providerConfirmed||!this.providerExpected||!['resend','sendgrid'].includes(this.providerChoice)||this.workflowStatus()==='saving')return;
    if(this.workflowAuthMode==='jwt'&&!this.validWorkflowReason(this.providerReason)){this.providerMessage.set('변경 사유를 1,000바이트 이내로 입력하세요.');return;}
    let endpoint:string;try {endpoint=this.providerEndpoint(true);}catch{this.providerStatus.set('error');this.providerMessage.set('워크플로 식별자와 인증을 확인하세요.');return;}
    const identifier=this.workflowIdentifier.trim(),choice=this.providerChoice,expected=this.providerExpected;
    const payload={provider:choice,expected};
    this.providerStatus.set('saving');this.providerMessage.set('');
    try {
      if(this.workflowAuthMode==='jwt'){
        const result=await this.submitJwtWorkflow({workflow:identifier,resource:'email-provider',expected:{provider:expected},desired:{provider:choice},reason:this.providerReason.trim()});
        this.providerStatus.set(result==='saved'?'saved':result==='unknown'?'unknown':'error');
        if(result!=='saved')this.providerMessage.set(result==='unknown'?'변경 결과가 불명입니다. 요청 ID를 보관하고 처리 상태를 확인하세요.':result==='unavailable'?'요청을 준비하지 못했습니다. 인증 또는 기존 불명 요청을 확인하세요.':'변경이 거절됐습니다. 설정과 권한을 다시 확인하세요.');
        return;
      }
      const response=await fetch(endpoint,{method:'PUT',headers:{Authorization:this.workflowAuthorization(),'Content-Type':'application/json'},body:JSON.stringify(payload),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
      this.expireOidcIfNeeded();
      if(!response.ok){this.providerStatus.set([400,401,403,404,409].includes(response.status)?'error':'unknown');this.providerMessage.set(response.status===409?'설정이 변경되었거나 접근 권한을 확인할 수 없습니다. 다시 조회하세요.':this.providerStatus()==='error'?'공급자 변경이 거절됐습니다. 권한과 서버 버전을 확인하세요.':'변경 결과가 불명입니다. 재시도 전에 현재 설정과 감사 기록을 확인하세요.');return;}
      const value:unknown=await response.json();this.expireOidcIfNeeded();
      if(typeof value!=='object'||value===null||!('data' in value)||typeof value.data!=='object'||value.data===null||!('workflowIdentifier' in value.data)||value.data.workflowIdentifier!==identifier||!('provider' in value.data)||value.data.provider!==choice)throw new Error('invalid response');
      this.providerStatus.set('saved');
    } catch {this.expireOidcIfNeeded();this.providerStatus.set('unknown');this.providerMessage.set('변경 결과가 불명입니다. 재시도 전에 현재 설정과 감사 기록을 확인하세요.');}
    finally {this.expireOidcIfNeeded();this.providerConfirmed=false;this.providerReason='';}
  }

  private workflowEndpoint(): string {
    if(this.workflowAuthMode==='jwt')return this.jwtWorkflowEndpoint(this.workflowChannel);
    const identifier=this.workflowIdentifier.trim();
    if (!/^[A-Za-z0-9_-][A-Za-z0-9._-]{0,255}$/.test(identifier) || !this.workflowKey.trim() || /[^\x21-\x7e]/.test(this.workflowKey.trim())) throw new Error('워크플로 식별자와 API 키를 확인해 주세요.');
    if (!['email','sms','push','slack'].includes(this.workflowChannel)) throw new Error('채널을 확인해 주세요.');
    const path=`/v1/experimental/workflows/${encodeURIComponent(identifier)}`;
    return `${this.validatedOrigin()}${path}${this.workflowChannel==='slack'?'/slack-step':`/channel-steps/${this.workflowChannel}`}`;
  }

  private applyWorkflow(value:unknown, identifier:string, channel:WorkflowChannel): void {
    if(typeof value!=='object'||value===null||!('data' in value)||typeof value.data!=='object'||value.data===null) throw new Error('invalid response');
    const data=value.data as Record<string,unknown>;
    if(data['workflowIdentifier']!==identifier || (channel!=='slack' && data['channel']!==channel) || typeof data['active']!=='boolean' || !(data['content']===null || typeof data['content']==='string') || (channel!=='slack' && !(data['title']===null || typeof data['title']==='string')) || (channel==='sms' && data['title']!==null) || ((channel==='email'||channel==='push') && ((data['title']===null)!==(data['content']===null)))) throw new Error('invalid response');
    this.workflowExpected={title:(data['title'] as string|null|undefined)??null,content:data['content'] as string|null,active:data['active']};
    this.workflowHasContent=data['content']!==null;this.workflowContent=(data['content'] as string|null)??'';this.workflowTitle=(data['title'] as string|null)??'';this.workflowActive=data['active'];this.workflowConfirmed=false;
  }

  async loadWorkflow():Promise<void> {
    this.expireOidcIfNeeded();
    if(this.workflowStatus()==='saving'||this.providerStatus()==='saving')return;
    this.clearWorkflow();const sequence=this.workflowSequence;const controller=new AbortController();this.workflowAbort=controller;this.workflowStatus.set('loading');
    try {
      const endpoint=this.workflowEndpoint(), identifier=this.workflowIdentifier.trim(), channel=this.workflowChannel;
      const response=await fetch(endpoint,{headers:{Authorization:this.workflowAuthorization()},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if(!response.ok)throw new Error('조회하지 못했습니다. 워크플로와 읽기 권한을 확인해 주세요.');
      const value:unknown=await response.json();this.expireOidcIfNeeded();if(sequence!==this.workflowSequence)return;
      this.applyWorkflow(value,identifier,channel);this.workflowStatus.set('loaded');
    } catch(error:unknown) {this.expireOidcIfNeeded();if(sequence!==this.workflowSequence)return;this.workflowStatus.set('error');this.workflowMessage.set(error instanceof Error && ['워크플로','운영자','채널','주소','원격','조회'].some(prefix=>error.message.startsWith(prefix))?error.message:'조회 응답 또는 서버 연결을 확인해 주세요.');}
    finally {if(sequence===this.workflowSequence)this.workflowAbort=null;}
  }

  async saveWorkflow():Promise<void> {
    this.expireOidcIfNeeded();
    if(this.workflowStatus()!=='loaded'||!this.workflowConfirmed||!this.workflowExpected||this.providerStatus()==='saving')return;
    let endpoint:string;
    try {endpoint=this.workflowAuthMode==='jwt'?this.jwtWorkflowEndpoint(this.workflowChannel,true):this.workflowEndpoint();} catch {this.workflowStatus.set('error');this.workflowMessage.set('워크플로 식별자와 인증을 확인해 주세요.');return;}
    if(this.workflowAuthMode==='jwt'&&!this.validWorkflowReason(this.workflowReason)){this.workflowMessage.set('변경 사유를 1,000바이트 이내로 입력하세요.');return;}
    const bytes=(s:string)=>new TextEncoder().encode(s).length;
    if(this.workflowHasContent && (bytes(this.workflowContent)>40000 || this.workflowContent.includes('\0') || ((this.workflowChannel==='email'||this.workflowChannel==='push') && (bytes(this.workflowTitle)>40000 || this.workflowTitle.includes('\0'))))) {this.workflowMessage.set('제목과 내용은 각각 40,000바이트 이하여야 하며 NUL 문자를 포함할 수 없습니다.');return;}
    const identifier=this.workflowIdentifier.trim(),channel=this.workflowChannel;
    const content=this.workflowHasContent?this.workflowContent:null;
    const expected=channel==='slack'?{content:this.workflowExpected.content,active:this.workflowExpected.active}:this.workflowExpected;
    const desired=channel==='slack'?{content,active:this.workflowActive}:{title:this.workflowHasContent&&(channel==='email'||channel==='push')?this.workflowTitle:null,content,active:this.workflowActive};
    const body={...desired,expected};
    this.workflowStatus.set('saving');this.workflowMessage.set('');
    try {
      if(this.workflowAuthMode==='jwt'){
        const result=await this.submitJwtWorkflow({workflow:identifier,resource:channel,expected,desired,reason:this.workflowReason.trim()});
        this.workflowStatus.set(result==='saved'?'saved':result==='unknown'?'unknown':'error');
        if(result!=='saved')this.workflowMessage.set(result==='unknown'?'변경 결과가 불명입니다. 요청 ID를 보관하고 처리 상태를 확인하세요.':result==='unavailable'?'요청을 준비하지 못했습니다. 인증 또는 기존 불명 요청을 확인하세요.':'변경이 거절됐습니다. 설정과 권한을 다시 확인하세요.');
        return;
      }
      const response=await fetch(endpoint,{method:'PUT',headers:{Authorization:this.workflowAuthorization(),'Content-Type':'application/json'},body:JSON.stringify(body),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
      this.expireOidcIfNeeded();
      if(!response.ok){this.workflowStatus.set([400,401,403,404,409].includes(response.status)?'error':'unknown');this.workflowMessage.set(response.status===409?'설정이 변경되었거나 접근 권한을 확인할 수 없습니다. 다시 조회해 주세요.':this.workflowStatus()==='error'?'변경이 거절되었습니다. 입력과 쓰기 권한 및 서버 버전을 확인해 주세요.':'변경 결과를 확인할 수 없습니다. 재시도 전에 현재 설정과 감사 기록을 확인하세요.');return;}
      const value:unknown=await response.json();this.expireOidcIfNeeded();this.applyWorkflow(value,identifier,channel);this.workflowStatus.set('saved');
    } catch {this.expireOidcIfNeeded();this.workflowStatus.set('unknown');this.workflowMessage.set('변경 결과를 확인할 수 없습니다. 재시도 전에 현재 설정과 감사 기록을 확인하세요.');}
    finally {this.expireOidcIfNeeded();this.workflowConfirmed=false;this.workflowReason='';if(!this.workflowKey){this.workflowExpected=null;this.workflowTitle='';this.workflowContent='';this.workflowHasContent=false;}}
  }

  setSection(next: Section): void {
    if ((this.createStatus()==='sending' || this.subscriberEditStatus()==='saving' || this.subscriberProfileRetryStatus()==='retrying' || this.subscriberCreateRetryStatus()==='retrying' || this.subscriberDeleteRetryStatus()==='retrying') || this.decisionStatus() === 'sending' || this.permissionChangeStatus()==='sending' || this.grantChangeStatus()==='sending' || this.workflowStatus()==='saving' || this.providerStatus()==='saving' || this.workflowRetryStatus()==='retrying') return;
    if (this.section()==='workflows' && next!=='workflows') {this.clearWorkflow(true);this.forgetWorkflowRequest();}
    if (this.section() === 'subscribers' && next !== 'subscribers') this.forgetKey();
    if (this.section() === 'alerts' && next !== 'alerts') this.forgetAlertKey();
    if (this.section() === 'settings' && next !== 'settings') {this.clearPermissions(true);this.clearPermissionChange();this.clearGrantChange();}
    this.section.set(next);
  }

  connectionChanged(): void {
    this.clearWorkflow(true);
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
    if(this.createStatus()==='sending'||this.subscriberEditStatus()==='saving'||this.subscriberProfileRetryStatus()==='retrying'||this.subscriberCreateRetryStatus()==='retrying'||this.subscriberDeleteRetryStatus()==='retrying')return;
    this.resetSubscriberEdit(true);this.forgetSubscriberProfileRequest();this.forgetSubscriberCreateRequest();this.forgetSubscriberDeleteRequest();this.resetCreateSubscriber();
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
      const keyHash=await this.credentialFingerprint(key);this.expireOidcIfNeeded();
      if (keyHash !== snapshot.keyHash || this.retryKey.trim()!==key || this.validatedOrigin() !== snapshot.origin) {
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
          this.expireOidcIfNeeded();
          if(this.decisionKey.trim()!==key){this.decisionRequestId.set(null);this.lookupId='';this.decisionStatus.set('unavailable');return;}
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

  clearSubscriberProfileLookup(clearId=false):void {
    this.subscriberProfileRequestSequence++;this.subscriberProfileRequestAbort?.abort();this.subscriberProfileRequestAbort=null;this.subscriberProfileRequestStatus.set('idle');this.subscriberProfileRequestRecord.set(null);if(clearId)this.subscriberProfileRequestId='';
  }
  forgetSubscriberProfileRequest():void {
    if(this.subscriberProfileRetryStatus()==='retrying')return;
    this.clearSubscriberProfileLookup(true);this.subscriberProfileRetry.set(null);this.subscriberProfileRetryStatus.set('idle');this.subscriberProfileRetryMessage.set('');this.subscriberProfileRetryConfirmed=false;
  }
  async lookupSubscriberProfileRequest():Promise<void> {
    if(this.subscriberEditStatus()==='saving'||this.subscriberProfileRetryStatus()==='retrying'||this.credentialMode!=='jwt')return;
    this.expireOidcIfNeeded();this.clearSubscriberProfileLookup();const sequence=this.subscriberProfileRequestSequence;
    const key=this.apiKey.trim(),environment=this.subscriberEnvironment.trim(),requestId=this.subscriberProfileRequestId.trim();
    if(!this.validJwt(key)||!this.validEnvironment(environment)||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(requestId)){this.subscriberProfileRequestStatus.set('error');return;}
    const controller=new AbortController();this.subscriberProfileRequestAbort=controller;this.subscriberProfileRequestStatus.set('loading');
    try {
      const params=new URLSearchParams({environmentId:environment,requestId});const response=await fetch(`${this.validatedOrigin()}/v1/experimental/operator/subscriber-profile-request?${params}`,{headers:{Authorization:`Bearer ${key}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if(!response.ok)throw new Error('request failed');const value:unknown=await response.json();if(typeof value!=='object'||value===null||!('found' in value)||typeof value.found!=='boolean')throw new Error('invalid response');
      this.expireOidcIfNeeded();if(sequence!==this.subscriberProfileRequestSequence)return;if(!value.found){this.subscriberProfileRequestStatus.set('missing');return;}
      const r=value as Record<string,unknown>;if(typeof r['requestId']!=='string'||r['requestId'].toLowerCase()!==requestId.toLowerCase()||['subscriberId','generation','previousRevision','revision','reason','createdAt'].some(k=>typeof r[k]!=='string')||typeof r['changed']!=='boolean'||!Array.isArray(r['fields'])||r['fields'].some((f:unknown)=>typeof f!=='string'||!['firstName','lastName','email','phone'].includes(f)))throw new Error('invalid response');
      this.subscriberProfileRequestRecord.set({requestId:r['requestId'],subscriberId:r['subscriberId'] as string,generation:r['generation'] as string,previousRevision:r['previousRevision'] as string,revision:r['revision'] as string,changed:r['changed'],fields:r['fields'] as string[],reason:r['reason'] as string,createdAt:r['createdAt'] as string});this.subscriberProfileRequestStatus.set('found');
    }catch{this.expireOidcIfNeeded();if(sequence===this.subscriberProfileRequestSequence)this.subscriberProfileRequestStatus.set('error');}
    finally{if(sequence===this.subscriberProfileRequestSequence)this.subscriberProfileRequestAbort=null;}
  }
  async retrySubscriberProfileRequest():Promise<void> {
    this.expireOidcIfNeeded();const snapshot=this.subscriberProfileRetry(),key=this.apiKey.trim();
    if(!snapshot||this.subscriberProfileRetryStatus()!=='unknown'||!this.subscriberProfileRetryConfirmed||this.credentialMode!=='jwt'||!this.validJwt(key))return;
    this.subscriberProfileRetryStatus.set('retrying');this.subscriberProfileRetryMessage.set('');
    try {
      const hash=await this.credentialFingerprint(key);this.expireOidcIfNeeded();if(this.apiKey.trim()!==key||hash!==snapshot.keyHash||this.subscriberEnvironment.trim()!==snapshot.environment||this.validatedOrigin()!==snapshot.origin){this.subscriberProfileRetryMessage.set('원래 토큰, 환경과 API 주소가 필요합니다.');return;}
      const response=await fetch(`${snapshot.origin}/v1/experimental/operator/subscriber-profile-v2?${new URLSearchParams({environmentId:snapshot.environment})}`,{method:'PATCH',headers:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'},body:JSON.stringify(snapshot.body),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
      if(!response.ok)throw new Error('request failed');const value:unknown=await response.json();if(typeof value!=='object'||value===null||!('requestId' in value)||value.requestId!==snapshot.body.requestId||!('subscriberId' in value)||value.subscriberId!==snapshot.body.subscriberId||!('generation' in value)||value.generation!==snapshot.body.generation||!('revision' in value)||typeof value.revision!=='string'||!('changed' in value)||typeof value.changed!=='boolean'||!('replayed' in value)||typeof value.replayed!=='boolean')throw new Error('invalid response');
      this.subscriberProfileRetryStatus.set('recorded');this.subscriberEditStatus.set('saved');this.clearSubscribers();
    }catch{this.subscriberProfileRetryMessage.set('재제출 결과가 불명확합니다. 요청 ID로 조회하세요.');}
    finally{this.subscriberProfileRetryConfirmed=false;if(this.subscriberProfileRetryStatus()==='retrying')this.subscriberProfileRetryStatus.set('unknown');}
  }
  resetSubscriberEdit(clearId=false):void {
    if(this.subscriberEditStatus()==='saving')return;
    this.subscriberEditSequence++;this.subscriberEditAbort?.abort();this.subscriberEditAbort=null;
    this.subscriberEditSnapshot.set(null);this.subscriberEditStatus.set('idle');this.subscriberEditMessage.set('');this.subscriberEditProfile={firstName:'',lastName:'',email:'',phone:''};this.subscriberEditConfirmed=false;this.subscriberEditReason='';this.subscriberDeleteReason='';this.subscriberDeleteConfirmation='';if(clearId)this.subscriberEditId='';
  }
  async loadSubscriberEdit():Promise<void> {
    if(this.subscriberEditStatus()==='saving'||this.subscriberProfileRetryStatus()==='retrying'||this.createStatus()==='sending'||!['api-key','jwt'].includes(this.credentialMode))return;
    this.expireOidcIfNeeded();this.resetSubscriberEdit();const sequence=this.subscriberEditSequence,id=this.subscriberEditId.trim(),key=this.apiKey.trim();
    const controller=new AbortController();this.subscriberEditAbort=controller;this.subscriberEditStatus.set('loading');
    try {
      const jwt=this.credentialMode==='jwt',environment=this.subscriberEnvironment.trim();
      if(!id||new TextEncoder().encode(id).length>512||id.includes('\0')||(jwt?(!this.validJwt(key)||!this.validEnvironment(environment)):(!key||/[^\x21-\x7e]/.test(key))))throw new Error('invalid input');
      const endpoint=jwt?`/v1/experimental/operator/subscriber-profile?${new URLSearchParams({environmentId:environment,subscriberId:id})}`:`/v1/experimental/subscribers/${encodeURIComponent(id)}`;
      const response=await fetch(`${this.validatedOrigin()}${endpoint}`,{headers:{Authorization:`${jwt?'Bearer':'ApiKey'} ${key}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if(!response.ok)throw new Error('request failed');const value:unknown=await response.json();
      if(typeof value!=='object'||value===null)throw new Error('invalid response');
      const row=jwt?(value as Record<string,unknown>):('data' in value&&typeof value.data==='object'&&value.data!==null?value.data as Record<string,unknown>:null);
      if(!row||(jwt&&row['found']!==true))throw new Error('invalid response');if(row['subscriberId']!==id||typeof row['generation']!=='string'||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(row['generation'])||typeof row['revision']!=='string'||!/^(0|[1-9][0-9]*)$/.test(row['revision'])||typeof row['profile']!=='object'||row['profile']===null||Array.isArray(row['profile']))throw new Error('invalid response');
      const profile=row['profile'] as Record<string,unknown>,fields=['firstName','lastName','email','phone'] as const;
      if(fields.some(k=>profile[k]!==undefined&&profile[k]!==null&&typeof profile[k]!=='string'))throw new Error('invalid profile');
      const normalized:SubscriberProfile={firstName:(profile['firstName'] as string|null|undefined)??'',lastName:(profile['lastName'] as string|null|undefined)??'',email:(profile['email'] as string|null|undefined)??'',phone:(profile['phone'] as string|null|undefined)??''};
      this.expireOidcIfNeeded();if(sequence!==this.subscriberEditSequence)return;
      this.subscriberEditSnapshot.set({id,generation:row['generation'],revision:row['revision'],profile:normalized});this.subscriberEditProfile={...normalized};this.subscriberEditStatus.set('loaded');
    }catch{this.expireOidcIfNeeded();if(sequence===this.subscriberEditSequence){this.subscriberEditStatus.set('error');this.subscriberEditMessage.set('구독자를 조회하지 못했습니다. ID, 인증 및 API 상태를 확인하세요.');}}
    finally{if(sequence===this.subscriberEditSequence)this.subscriberEditAbort=null;}
  }
  canEditSubscriber():boolean {
    const snapshot=this.subscriberEditSnapshot();return ['api-key','jwt'].includes(this.credentialMode)&&this.subscriberEditStatus()==='loaded'&&this.createStatus()!=='sending'&&this.subscriberCreateRetryStatus()!=='retrying'&&this.subscriberDeleteRetryStatus()!=='retrying'&&!!snapshot&&this.subscriberEditConfirmed&&(this.credentialMode!=='jwt'||this.subscriberProfileRetryStatus()==='idle')&&(this.credentialMode!=='jwt'||(!!this.subscriberEditReason.trim()&&new TextEncoder().encode(this.subscriberEditReason.trim()).length<=1000))&&(Object.keys(snapshot.profile) as (keyof SubscriberProfile)[]).some(k=>this.subscriberEditProfile[k]!==snapshot.profile[k]);
  }
  canDeleteSubscriber():boolean {
    const snapshot=this.subscriberEditSnapshot(),reason=this.subscriberDeleteReason.trim();return ['api-key','jwt'].includes(this.credentialMode)&&this.subscriberEditStatus()==='loaded'&&this.createStatus()!=='sending'&&this.subscriberCreateRetryStatus()!=='retrying'&&this.subscriberProfileRetryStatus()!=='retrying'&&this.subscriberDeleteRetryStatus()==='idle'&&!!snapshot&&this.subscriberDeleteConfirmation===snapshot.id&&(this.credentialMode!=='jwt'||(!!reason&&new TextEncoder().encode(reason).length<=1000));
  }
  async changeSubscriberEdit(deleting:boolean):Promise<void> {
    const snapshot=this.subscriberEditSnapshot();if(!snapshot||(deleting?!this.canDeleteSubscriber():!this.canEditSubscriber()))return;
    if(deleting&&this.credentialMode==='jwt'){await this.deleteSubscriberJwt(snapshot);return;}
    this.expireOidcIfNeeded();const key=this.apiKey.trim(),jwt=this.credentialMode==='jwt',environment=this.subscriberEnvironment.trim();let origin:string;try{origin=this.validatedOrigin();if(jwt?(!this.validJwt(key)||!this.validEnvironment(environment)):(!key||/[^\x21-\x7e]/.test(key)))throw new Error('invalid key');}catch{this.subscriberEditStatus.set('error');this.subscriberEditMessage.set('API 주소와 인증을 확인하세요.');return;}
    const profile:Record<string,string|null>={};for(const k of Object.keys(snapshot.profile) as (keyof SubscriberProfile)[])if(this.subscriberEditProfile[k]!==snapshot.profile[k])profile[k]=this.subscriberEditProfile[k]||null;
    let body:object=deleting?{generation:snapshot.generation,revision:snapshot.revision}:{generation:snapshot.generation,revision:snapshot.revision,profile};
    this.subscriberEditStatus.set('saving');this.subscriberEditMessage.set('');
    try {
      if(jwt){
        let requestId:string,keyHash:string;try{requestId=crypto.randomUUID();keyHash=await this.credentialFingerprint(key);}catch{this.subscriberEditStatus.set('error');this.subscriberEditMessage.set('안전한 요청 정보를 만들지 못해 전송하지 않았습니다.');return;}
        this.expireOidcIfNeeded();if(this.apiKey.trim()!==key||this.subscriberEnvironment.trim()!==environment||this.validatedOrigin()!==origin){this.subscriberEditStatus.set('error');this.subscriberEditMessage.set('인증이 변경되어 요청을 전송하지 않았습니다.');return;}
        const request:SubscriberProfileRequestBody={requestId,subscriberId:snapshot.id,generation:snapshot.generation,revision:snapshot.revision,profile,reason:this.subscriberEditReason.trim()};body=request;
        this.subscriberProfileRetry.set({origin,environment,keyHash,body:request});this.subscriberProfileRequestId=requestId;
      }
      const endpoint=jwt?`/v1/experimental/operator/subscriber-profile-v2?${new URLSearchParams({environmentId:environment})}`:`/v1/experimental/subscribers/${encodeURIComponent(snapshot.id)}`;
      const response=await fetch(`${origin}${endpoint}`,{method:deleting?'DELETE':'PATCH',headers:{Authorization:`${jwt?'Bearer':'ApiKey'} ${key}`,'Content-Type':'application/json'},body:JSON.stringify(body),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
      if(!response.ok){if(jwt&&[400,401,403,404,409].includes(response.status)){this.subscriberProfileRetry.set(null);this.subscriberProfileRetryStatus.set('idle');}else if(jwt)this.subscriberProfileRetryStatus.set('unknown');if(response.status===409||response.status===404){this.subscriberEditStatus.set('conflict');this.subscriberEditMessage.set('조회한 구독자가 변경되었거나 삭제됐습니다. 다시 조회하세요.');}else if([400,401,403].includes(response.status)){this.subscriberEditStatus.set('error');this.subscriberEditMessage.set('요청이 거절됐습니다. 입력과 인증을 확인하세요.');}else{this.subscriberEditStatus.set('unknown');this.subscriberEditMessage.set('결과를 확인할 수 없습니다. 다시 조회해 상태를 확인하세요.');}return;}
      const value:unknown=await response.json();if(typeof value!=='object'||value===null)throw new Error('invalid response');
      const result=jwt?value:('data' in value&&typeof value.data==='object'&&value.data!==null?value.data:null);
      if(!result||!('subscriberId' in result)||result.subscriberId!==snapshot.id||(deleting&&(!('deleted' in result)||result.deleted!==true))||(!deleting&&(!('generation' in result)||result.generation!==snapshot.generation||!('revision' in result)||typeof result.revision!=='string'||!/^(0|[1-9][0-9]*)$/.test(result.revision)))||(jwt&&(!('changed' in result)||typeof result.changed!=='boolean'||!('replayed' in result)||typeof result.replayed!=='boolean'||!('requestId' in result)||result.requestId!==this.subscriberProfileRetry()?.body.requestId)))throw new Error('invalid response');
      if(jwt){this.subscriberProfileRetry.set(null);this.subscriberProfileRetryStatus.set('idle');}
      this.subscriberEditStatus.set(deleting?'deleted':'saved');this.clearSubscribers();
    }catch{if(jwt&&this.subscriberProfileRetry())this.subscriberProfileRetryStatus.set('unknown');this.subscriberEditStatus.set('unknown');this.subscriberEditMessage.set('결과를 확인할 수 없습니다. 다시 조회해 상태를 확인하세요.');}
    finally{this.subscriberEditConfirmed=false;this.subscriberDeleteConfirmation='';this.subscriberEditReason='';}
  }

  resetCreateSubscriber():void {
    if(this.createStatus()==='sending')return;
    this.createReason='';this.createSubscriberId='';this.createFirstName='';this.createLastName='';this.createEmail='';this.createPhone='';this.createConfirmed=false;this.createdSubscriberId.set('');this.createMessage.set('');this.createStatus.set('idle');
  }
  clearSubscriberDeleteLookup(clearId=false):void {
    this.subscriberDeleteRequestSequence++;this.subscriberDeleteRequestAbort?.abort();this.subscriberDeleteRequestAbort=null;this.subscriberDeleteRequestStatus.set('idle');this.subscriberDeleteRequestRecord.set(null);if(clearId)this.subscriberDeleteRequestId='';
  }
  forgetSubscriberDeleteRequest():void {
    if(this.subscriberDeleteRetryStatus()==='retrying')return;
    this.clearSubscriberDeleteLookup(true);this.subscriberDeleteRetry.set(null);this.subscriberDeleteRetryStatus.set('idle');this.subscriberDeleteRetryMessage.set('');this.subscriberDeleteRetryConfirmed=false;
  }
  async lookupSubscriberDeleteRequest():Promise<void> {
    if(this.subscriberEditStatus()==='saving'||this.subscriberDeleteRetryStatus()==='retrying'||this.credentialMode!=='jwt')return;
    this.expireOidcIfNeeded();this.clearSubscriberDeleteLookup();const sequence=this.subscriberDeleteRequestSequence;
    const key=this.apiKey.trim(),environment=this.subscriberEnvironment.trim(),requestId=this.subscriberDeleteRequestId.trim();
    if(!this.validJwt(key)||!this.validEnvironment(environment)||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(requestId)){this.subscriberDeleteRequestStatus.set('error');return;}
    const controller=new AbortController();this.subscriberDeleteRequestAbort=controller;this.subscriberDeleteRequestStatus.set('loading');
    try {
      const params=new URLSearchParams({environmentId:environment,requestId});const response=await fetch(`${this.validatedOrigin()}/v1/experimental/operator/subscriber-delete-request?${params}`,{headers:{Authorization:`Bearer ${key}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if(!response.ok)throw new Error('request failed');const value:unknown=await response.json();if(typeof value!=='object'||value===null||!('found' in value)||typeof value.found!=='boolean')throw new Error('invalid response');
      this.expireOidcIfNeeded();if(sequence!==this.subscriberDeleteRequestSequence)return;if(!value.found){this.subscriberDeleteRequestStatus.set('missing');return;}
      const r=value as Record<string,unknown>;if(typeof r['requestId']!=='string'||r['requestId'].toLowerCase()!==requestId.toLowerCase()||['subscriberId','generation','revision','reason','createdAt'].some(k=>typeof r[k]!=='string'))throw new Error('invalid response');
      this.subscriberDeleteRequestRecord.set({requestId:r['requestId'],subscriberId:r['subscriberId'] as string,generation:r['generation'] as string,revision:r['revision'] as string,reason:r['reason'] as string,createdAt:r['createdAt'] as string});this.subscriberDeleteRequestStatus.set('found');
    }catch{this.expireOidcIfNeeded();if(sequence===this.subscriberDeleteRequestSequence)this.subscriberDeleteRequestStatus.set('error');}
    finally{if(sequence===this.subscriberDeleteRequestSequence)this.subscriberDeleteRequestAbort=null;}
  }
  private async sendSubscriberDelete(key:string,snapshot:SubscriberDeleteRetry):Promise<Response> {
    return fetch(`${snapshot.origin}/v1/experimental/operator/subscriber-delete?${new URLSearchParams({environmentId:snapshot.environment})}`,{method:'POST',headers:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'},body:JSON.stringify(snapshot.body),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
  }
  private validSubscriberDeleteAck(value:unknown,snapshot:SubscriberDeleteRetry):boolean {
    if(typeof value!=='object'||value===null)return false;const r=value as Record<string,unknown>;return r['requestId']===snapshot.body.requestId&&r['subscriberId']===snapshot.body.subscriberId&&r['generation']===snapshot.body.generation&&r['revision']===snapshot.body.revision&&typeof r['replayed']==='boolean';
  }
  private async deleteSubscriberJwt(target:SubscriberEditSnapshot):Promise<void> {
    this.expireOidcIfNeeded();const key=this.apiKey.trim(),environment=this.subscriberEnvironment.trim();let origin:string;
    try{origin=this.validatedOrigin();if(!this.validJwt(key)||!this.validEnvironment(environment))throw new Error('invalid');}catch{this.subscriberEditStatus.set('error');this.subscriberEditMessage.set('API 주소와 인증을 확인하세요.');return;}
    const reason=this.subscriberDeleteReason.trim();this.subscriberEditStatus.set('saving');this.subscriberEditMessage.set('');
    try {
      let requestId:string,keyHash:string;try{requestId=crypto.randomUUID();keyHash=await this.credentialFingerprint(key);}catch{this.subscriberEditStatus.set('error');this.subscriberEditMessage.set('안전한 요청 정보를 만들지 못해 전송하지 않았습니다.');return;}
      this.expireOidcIfNeeded();if(this.apiKey.trim()!==key||this.subscriberEnvironment.trim()!==environment||this.validatedOrigin()!==origin){this.subscriberEditStatus.set('error');this.subscriberEditMessage.set('인증이 변경되어 요청을 전송하지 않았습니다.');return;}
      const body:SubscriberDeleteBody={requestId,subscriberId:target.id,generation:target.generation,revision:target.revision,reason};const snapshot={origin,environment,keyHash,body};this.subscriberDeleteRetry.set(snapshot);this.subscriberDeleteRequestId=requestId;
      const response=await this.sendSubscriberDelete(key,snapshot);
      if(!response.ok){if([400,401,403,409].includes(response.status)){this.subscriberDeleteRetry.set(null);this.subscriberDeleteRetryStatus.set('idle');this.subscriberEditStatus.set(response.status===409?'conflict':'error');this.subscriberEditMessage.set(response.status===409?'조회한 구독자가 변경되었거나 삭제됐습니다. 다시 조회하세요.':'요청이 거절됐습니다. 입력과 인증을 확인하세요.');}else{this.subscriberDeleteRetryStatus.set('unknown');this.subscriberEditStatus.set('unknown');this.subscriberEditMessage.set('결과를 확인할 수 없습니다. 요청 ID로 확인하세요.');}return;}
      if(!this.validSubscriberDeleteAck(await response.json(),snapshot))throw new Error('invalid response');
      this.subscriberDeleteRetry.set(null);this.subscriberDeleteRetryStatus.set('idle');this.subscriberEditStatus.set('deleted');this.clearSubscribers();
    }catch{if(this.subscriberDeleteRetry())this.subscriberDeleteRetryStatus.set('unknown');this.subscriberEditStatus.set('unknown');this.subscriberEditMessage.set('결과를 확인할 수 없습니다. 요청 ID로 확인하세요.');}
    finally{this.subscriberDeleteConfirmation='';this.subscriberDeleteReason='';}
  }
  async retrySubscriberDeleteRequest():Promise<void> {
    this.expireOidcIfNeeded();const snapshot=this.subscriberDeleteRetry(),key=this.apiKey.trim();
    if(!snapshot||this.subscriberDeleteRetryStatus()!=='unknown'||!this.subscriberDeleteRetryConfirmed||this.credentialMode!=='jwt'||!this.validJwt(key))return;
    this.subscriberDeleteRetryStatus.set('retrying');this.subscriberDeleteRetryMessage.set('');
    try {
      const hash=await this.credentialFingerprint(key);this.expireOidcIfNeeded();if(this.apiKey.trim()!==key||hash!==snapshot.keyHash||this.subscriberEnvironment.trim()!==snapshot.environment||this.validatedOrigin()!==snapshot.origin){this.subscriberDeleteRetryMessage.set('원래 토큰, 환경과 API 주소가 필요합니다.');return;}
      const response=await this.sendSubscriberDelete(key,snapshot);if(!response.ok||!this.validSubscriberDeleteAck(await response.json(),snapshot))throw new Error('request failed');
      this.subscriberDeleteRetryStatus.set('recorded');this.subscriberEditStatus.set('deleted');this.clearSubscribers();
    }catch{this.subscriberDeleteRetryMessage.set('재제출 결과가 불명확합니다. 요청 ID로 조회하세요.');}
    finally{this.subscriberDeleteRetryConfirmed=false;if(this.subscriberDeleteRetryStatus()==='retrying')this.subscriberDeleteRetryStatus.set('unknown');}
  }
  clearSubscriberCreateLookup(clearId=false):void {
    this.subscriberCreateRequestSequence++;this.subscriberCreateRequestAbort?.abort();this.subscriberCreateRequestAbort=null;this.subscriberCreateRequestStatus.set('idle');this.subscriberCreateRequestRecord.set(null);if(clearId)this.subscriberCreateRequestId='';
  }
  forgetSubscriberCreateRequest():void {
    if(this.subscriberCreateRetryStatus()==='retrying')return;
    this.clearSubscriberCreateLookup(true);this.subscriberCreateRetry.set(null);this.subscriberCreateRetryStatus.set('idle');this.subscriberCreateRetryMessage.set('');this.subscriberCreateRetryConfirmed=false;
  }
  async lookupSubscriberCreateRequest():Promise<void> {
    if(this.createStatus()==='sending'||this.subscriberCreateRetryStatus()==='retrying'||this.credentialMode!=='jwt')return;
    this.expireOidcIfNeeded();this.clearSubscriberCreateLookup();const sequence=this.subscriberCreateRequestSequence;
    const key=this.apiKey.trim(),environment=this.subscriberEnvironment.trim(),requestId=this.subscriberCreateRequestId.trim();
    if(!this.validJwt(key)||!this.validEnvironment(environment)||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(requestId)){this.subscriberCreateRequestStatus.set('error');return;}
    const controller=new AbortController();this.subscriberCreateRequestAbort=controller;this.subscriberCreateRequestStatus.set('loading');
    try {
      const params=new URLSearchParams({environmentId:environment,requestId});const response=await fetch(`${this.validatedOrigin()}/v1/experimental/operator/subscriber-create-request?${params}`,{headers:{Authorization:`Bearer ${key}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if(!response.ok)throw new Error('request failed');const value:unknown=await response.json();if(typeof value!=='object'||value===null||!('found' in value)||typeof value.found!=='boolean')throw new Error('invalid response');
      this.expireOidcIfNeeded();if(sequence!==this.subscriberCreateRequestSequence)return;if(!value.found){this.subscriberCreateRequestStatus.set('missing');return;}
      const r=value as Record<string,unknown>;if(typeof r['requestId']!=='string'||r['requestId'].toLowerCase()!==requestId.toLowerCase()||['subscriberId','generation','reason','createdAt'].some(k=>typeof r[k]!=='string')||!Array.isArray(r['fields'])||r['fields'].some((f:unknown)=>typeof f!=='string'||!['firstName','lastName','email','phone'].includes(f)))throw new Error('invalid response');
      this.subscriberCreateRequestRecord.set({requestId:r['requestId'],subscriberId:r['subscriberId'] as string,generation:r['generation'] as string,fields:r['fields'] as string[],reason:r['reason'] as string,createdAt:r['createdAt'] as string});this.subscriberCreateRequestStatus.set('found');
    }catch{this.expireOidcIfNeeded();if(sequence===this.subscriberCreateRequestSequence)this.subscriberCreateRequestStatus.set('error');}
    finally{if(sequence===this.subscriberCreateRequestSequence)this.subscriberCreateRequestAbort=null;}
  }
  async retrySubscriberCreateRequest():Promise<void> {
    this.expireOidcIfNeeded();const snapshot=this.subscriberCreateRetry(),key=this.apiKey.trim();
    if(!snapshot||this.subscriberCreateRetryStatus()!=='unknown'||!this.subscriberCreateRetryConfirmed||this.credentialMode!=='jwt'||!this.validJwt(key))return;
    this.subscriberCreateRetryStatus.set('retrying');this.subscriberCreateRetryMessage.set('');
    try {
      const hash=await this.credentialFingerprint(key);this.expireOidcIfNeeded();if(this.apiKey.trim()!==key||hash!==snapshot.keyHash||this.subscriberEnvironment.trim()!==snapshot.environment||this.validatedOrigin()!==snapshot.origin){this.subscriberCreateRetryMessage.set('원래 토큰, 환경과 API 주소가 필요합니다.');return;}
      const response=await fetch(`${snapshot.origin}/v1/experimental/operator/subscriber-create?${new URLSearchParams({environmentId:snapshot.environment})}`,{method:'POST',headers:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'},body:JSON.stringify(snapshot.body),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
      if(!response.ok)throw new Error('request failed');const value:unknown=await response.json();if(typeof value!=='object'||value===null||!('requestId' in value)||value.requestId!==snapshot.body.requestId||!('subscriberId' in value)||value.subscriberId!==snapshot.body.subscriberId||!('generation' in value)||typeof value.generation!=='string'||!('replayed' in value)||typeof value.replayed!=='boolean')throw new Error('invalid response');
      this.subscriberCreateRetryStatus.set('recorded');this.createStatus.set('created');this.clearSubscribers();
    }catch{this.subscriberCreateRetryMessage.set('재제출 결과가 불명확합니다. 요청 ID로 조회하세요.');}
    finally{this.subscriberCreateRetryConfirmed=false;if(this.subscriberCreateRetryStatus()==='retrying')this.subscriberCreateRetryStatus.set('unknown');}
  }
  canCreateSubscriber():boolean {
    const id=this.createSubscriberId.trim(),key=this.apiKey.trim(),jwt=this.credentialMode==='jwt',reason=this.createReason.trim();
    return ['api-key','jwt'].includes(this.credentialMode)&&this.createStatus()==='idle'&&this.subscriberEditStatus()!=='saving'&&this.subscriberProfileRetryStatus()!=='retrying'&&this.subscriberDeleteRetryStatus()!=='retrying'&&this.subscriberCreateRetryStatus()==='idle'&&this.createConfirmed&&!!id&&new TextEncoder().encode(id).length<=512&&!id.includes('\0')&&(jwt?this.validJwt(key)&&this.validEnvironment(this.subscriberEnvironment.trim())&&!!reason&&new TextEncoder().encode(reason).length<=1000:!!key&&!/[^\x21-\x7e]/.test(key));
  }
  async createSubscriber():Promise<void> {
    this.expireOidcIfNeeded();if(!this.canCreateSubscriber())return;
    let origin:string;try{origin=this.validatedOrigin();}catch{this.createStatus.set('rejected');this.createMessage.set('API 주소를 확인하세요.');return;}
    const key=this.apiKey.trim(),subscriberId=this.createSubscriberId.trim(),jwt=this.credentialMode==='jwt',environment=this.subscriberEnvironment.trim();
    const profile:Record<string,string>={};for(const [name,value] of [['firstName',this.createFirstName],['lastName',this.createLastName],['email',this.createEmail],['phone',this.createPhone]])if(value.trim())profile[name]=value.trim();
    let body:object={subscriberId,...profile};this.createdSubscriberId.set(subscriberId);this.createStatus.set('sending');this.createMessage.set('');
    try {
      if(jwt){
        let requestId:string,keyHash:string;try{requestId=crypto.randomUUID();keyHash=await this.credentialFingerprint(key);}catch{this.createStatus.set('rejected');this.createMessage.set('안전한 요청 정보를 만들지 못해 전송하지 않았습니다.');return;}
        this.expireOidcIfNeeded();if(this.apiKey.trim()!==key||this.subscriberEnvironment.trim()!==environment||this.validatedOrigin()!==origin){this.createStatus.set('rejected');this.createMessage.set('인증이 변경되어 요청을 전송하지 않았습니다.');return;}
        const request:SubscriberCreateBody={requestId,subscriberId,profile,reason:this.createReason.trim()};body=request;this.subscriberCreateRetry.set({origin,environment,keyHash,body:request});this.subscriberCreateRequestId=requestId;
      }
      const endpoint=jwt?`/v1/experimental/operator/subscriber-create?${new URLSearchParams({environmentId:environment})}`:'/v2/subscribers?failIfExists=true';
      const response=await fetch(`${origin}${endpoint}`,{method:'POST',headers:{Authorization:`${jwt?'Bearer':'ApiKey'} ${key}`,'Content-Type':'application/json'},body:JSON.stringify(body),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
      if(!response.ok){
        if(jwt&&[400,401,403,409,501].includes(response.status)){this.subscriberCreateRetry.set(null);this.subscriberCreateRetryStatus.set('idle');}else if(jwt)this.subscriberCreateRetryStatus.set('unknown');
        if([400,401,403,409,501].includes(response.status)){this.createStatus.set('rejected');this.createMessage.set(response.status===409?'이미 존재하는 구독자 ID 또는 충돌한 요청입니다. 기존 구독자는 변경되지 않았습니다.':'입력 값, 인증과 API 지원 여부를 확인하세요.');}
        else {this.createStatus.set('unknown');this.createMessage.set('요청 결과를 확인할 수 없습니다. 같은 ID를 조회해 확인하세요.');}return;
      }
      const value:unknown=await response.json();if(typeof value!=='object'||value===null)throw new Error('invalid response');
      if(jwt){if(!('subscriberId' in value)||value.subscriberId!==subscriberId||!('requestId' in value)||value.requestId!==this.subscriberCreateRetry()?.body.requestId||!('generation' in value)||typeof value.generation!=='string'||!('replayed' in value)||typeof value.replayed!=='boolean')throw new Error('invalid response');this.subscriberCreateRetry.set(null);this.subscriberCreateRetryStatus.set('idle');}
      else if(response.status!==201||!('data' in value)||typeof value.data!=='object'||value.data===null||!('subscriberId' in value.data)||value.data.subscriberId!==subscriberId)throw new Error('invalid response');
      this.createStatus.set('created');this.clearSubscribers();
    }catch{if(jwt&&this.subscriberCreateRetry())this.subscriberCreateRetryStatus.set('unknown');this.createStatus.set('unknown');this.createMessage.set('요청 결과를 확인할 수 없습니다. 같은 ID를 조회해 확인하세요.');}
    finally{this.createConfirmed=false;this.createReason='';}
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
