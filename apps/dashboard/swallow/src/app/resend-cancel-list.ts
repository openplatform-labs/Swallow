import { Component, Input, OnChanges, OnDestroy, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
type LegacyRow={requestId:string;jobId:string;httpStatus:408|409;createdAt:string};
type Row={requestId:string;jobId:string;createdAt:string;overdue:boolean};
type Detail={requestId:string;jobId:string;providerId:string;databasePrincipal:string;reason:string;state:string;httpStatus:number|null;createdAt:string;updatedAt:string;resolution:{resolutionId:string;decision:string;databasePrincipal:string;reason:string;recordedAt:string;operator:{issuer:string;subject:string;clientId:string;resolutionId:string}|null}|null};
@Component({selector:'app-resend-cancel-list',imports:[FormsModule],templateUrl:'./resend-cancel-list.html'})
export class ResendCancelList implements OnChanges,OnDestroy {
  @Input() apiOrigin='';@Input() environment='';@Input() token='';
  grace=300;readonly status=signal<'idle'|'loading'|'loaded'|'error'>('idle');readonly rows=signal<Row[]>([]);readonly next=signal<string|null>(null);readonly page=signal(1);
  readonly legacyHealth=signal<number|null>(null);readonly legacyHealthStatus=signal<'idle'|'loading'|'loaded'|'error'>('idle');
  readonly legacyRows=signal<LegacyRow[]>([]);readonly legacyNext=signal<string|null>(null);readonly legacyPage=signal(1);readonly legacyListStatus=signal<'idle'|'loading'|'loaded'|'error'>('idle');
  private legacyCursors:(string|null)[]=[null];private legacyHealthSeq=0;private legacyListSeq=0;private legacyHealthAbort:AbortController|null=null;private legacyListAbort:AbortController|null=null;
  commandJobId='';commandReason='';commandLookupId='';commandConfirm=false;
  readonly commandDraft=signal<{origin:string;environment:string;token:string;requestId:string;jobId:string;reason:string}|null>(null);
  readonly commandStatus=signal<'idle'|'prepared'|'sending'|'recorded'|'unknown'>('idle');
  readonly commandLookupStatus=signal<'idle'|'loading'|'loaded'|'missing'|'error'>('idle');
  readonly command=signal<{requestId:string;jobId:string;state:string;attemptState:string|null}|null>(null);
  private commandSeq=0;private commandAbort:AbortController|null=null;
  decision:'confirmed'|'not_cancelled'='confirmed';reason='';confirm=false;readonly resolutionStatus=signal<'idle'|'prepared'|'sending'|'recorded'|'unknown'>('idle');
  readonly resolutionDraft=signal<{origin:string;environment:string;token:string;resolutionId:string;requestId:string;decision:string;reason:string}|null>(null);
  requestId='';readonly detail=signal<Detail|null>(null);readonly detailStatus=signal<'idle'|'loading'|'loaded'|'missing'|'error'>('idle');
  private detailSeq=0;private detailAbort:AbortController|null=null;
  private seq=0;private abort:AbortController|null=null;private cursors:(string|null)[]=[null];
  ngOnChanges():void{this.clear();}ngOnDestroy():void{this.clear();this.commandDraft.set(null);this.resolutionDraft.set(null);this.token='';}
  clear():void{this.clearLegacy();this.clearCommandLookup();this.clearDetail();this.seq++;this.abort?.abort();this.abort=null;this.rows.set([]);this.next.set(null);this.page.set(1);this.cursors=[null];this.status.set('idle');}
  clearLegacy():void{this.legacyHealthSeq++;this.legacyListSeq++;this.legacyHealthAbort?.abort();this.legacyListAbort?.abort();this.legacyHealthAbort=null;this.legacyListAbort=null;this.legacyHealth.set(null);this.legacyRows.set([]);this.legacyNext.set(null);this.legacyPage.set(1);this.legacyCursors=[null];this.legacyHealthStatus.set('idle');this.legacyListStatus.set('idle');}
  private legacyInputs():{origin:string;environment:string;token:string}{const origin=this.origin(),environment=this.environment.trim(),token=this.token.trim();if(!environment||new TextEncoder().encode(environment).length>512||/[\0\r\n]/.test(environment)||token.length>16384||!/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(token))throw Error('input');return {origin,environment,token};}
  private legacySame(v:ReturnType<ResendCancelList['legacyInputs']>):boolean{try{const x=this.legacyInputs();return x.origin===v.origin&&x.environment===v.environment&&x.token===v.token;}catch{return false;}}
  async loadLegacyHealth():Promise<void>{this.legacyHealthSeq++;this.legacyHealthAbort?.abort();const seq=this.legacyHealthSeq,controller=new AbortController();this.legacyHealthAbort=controller;this.legacyHealth.set(null);this.legacyHealthStatus.set('loading');
    try{const v=this.legacyInputs(),params=new URLSearchParams({environmentId:v.environment}),response=await fetch(`${v.origin}/v1/experimental/operator/resend-cancel-legacy-health?${params}`,{headers:{Authorization:`Bearer ${v.token}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});if(!response.ok)throw Error('request');const data:unknown=await response.json();if(!data||typeof data!=='object')throw Error('response');const count=(data as Record<string,unknown>)['legacyAmbiguous'];if(!Number.isSafeInteger(count)||(count as number)<0)throw Error('count');if(seq!==this.legacyHealthSeq)return;if(!this.legacySame(v)){this.clearLegacy();return;}this.legacyHealth.set(count as number);this.legacyHealthStatus.set('loaded');}
    catch{if(seq===this.legacyHealthSeq){this.legacyHealth.set(null);this.legacyHealthStatus.set('error');}}finally{if(seq===this.legacyHealthSeq)this.legacyHealthAbort=null;}}
  async listLegacy(direction?:'next'|'previous'):Promise<void>{const after=direction==='next'?this.legacyNext():direction==='previous'?this.legacyCursors[this.legacyPage()-2]:null;if((direction==='next'&&!after)||after===undefined)return;const page=direction==='next'?this.legacyPage()+1:direction==='previous'?this.legacyPage()-1:1;this.legacyListSeq++;this.legacyListAbort?.abort();const seq=this.legacyListSeq,controller=new AbortController();this.legacyListAbort=controller;this.legacyRows.set([]);this.legacyNext.set(null);this.legacyListStatus.set('loading');
    try{const v=this.legacyInputs(),params=new URLSearchParams({environmentId:v.environment});if(after)params.set('after',after);const response=await fetch(`${v.origin}/v1/experimental/operator/resend-cancel-legacy-list?${params}`,{headers:{Authorization:`Bearer ${v.token}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});if(!response.ok)throw Error('request');const data:unknown=await response.json();if(!data||typeof data!=='object')throw Error('response');const r=data as Record<string,unknown>;if(!Array.isArray(r['data'])||r['data'].length>50||!(r['next']===null||this.uuid(r['next'])))throw Error('page');const rows:LegacyRow[]=r['data'].map((v:unknown)=>{if(!v||typeof v!=='object')throw Error('row');const x=v as Record<string,unknown>;if(!this.uuid(x['requestId'])||!this.uuid(x['jobId'])||(x['httpStatus']!==408&&x['httpStatus']!==409)||typeof x['createdAt']!=='string'||!x['createdAt']||x['createdAt'].length>128)throw Error('row');return {requestId:x['requestId'],jobId:x['jobId'],httpStatus:x['httpStatus'],createdAt:x['createdAt']};});if(rows.some((x,i)=>i>0&&rows[i-1].requestId.toLowerCase()>=x.requestId.toLowerCase())||(after&&rows.some(x=>x.requestId.toLowerCase()<=after.toLowerCase()))||(r['next']!==null&&(rows.length!==50||r['next']!==rows[rows.length-1].requestId)))throw Error('page');if(seq!==this.legacyListSeq)return;if(!this.legacySame(v)){this.clearLegacy();return;}this.legacyRows.set(rows);this.legacyNext.set(r['next'] as string|null);this.legacyPage.set(page);this.legacyCursors=this.legacyCursors.slice(0,page-1);this.legacyCursors.push(after);this.legacyListStatus.set('loaded');}
    catch{if(seq===this.legacyListSeq){this.legacyRows.set([]);this.legacyNext.set(null);this.legacyListStatus.set('error');}}finally{if(seq===this.legacyListSeq)this.legacyListAbort=null;}}
  clearCommandLookup():void{this.commandSeq++;this.commandAbort?.abort();this.commandAbort=null;this.command.set(null);this.commandLookupStatus.set('idle');}
  private parseCommand(value:unknown):{requestId:string;jobId:string;state:string;attemptState:string|null} {
    if(!value||typeof value!=='object')throw Error('command');const c=value as Record<string,unknown>;
    if(!this.uuid(c['requestId'])||!this.uuid(c['jobId'])||!['pending','dispatching','finished','failed'].includes(c['state'] as string)||!(c['attemptState']===null||['unknown','confirmed','rejected','not_cancelled'].includes(c['attemptState'] as string)))throw Error('command');
    return {requestId:c['requestId'],jobId:c['jobId'],state:c['state'] as string,attemptState:c['attemptState'] as string|null};
  }
  prepareCommand():void{
    if(this.commandStatus()!=='idle')return;
    try{const origin=this.origin(),environment=this.environment.trim(),token=this.token.trim(),jobId=this.commandJobId.trim(),reason=this.commandReason.trim();
      if(!this.uuid(jobId)||!environment||new TextEncoder().encode(environment).length>512||/[\0\r\n]/.test(environment)||token.length>16384||!/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(token)||!reason||new TextEncoder().encode(reason).length>500||/[\x00-\x1f\x7f]/.test(reason))throw Error('input');
      const requestId=crypto.randomUUID();this.commandLookupId=requestId;this.commandDraft.set({origin,environment,token,requestId,jobId,reason});this.commandConfirm=false;this.commandStatus.set('prepared');
    }catch{this.commandStatus.set('idle');}
  }
  resetCommand():void{if(this.commandStatus()==='sending')return;this.commandDraft.set(null);this.commandConfirm=false;this.commandStatus.set('idle');}
  async submitCommand():Promise<void>{
    const draft=this.commandDraft();if(!draft||!this.commandConfirm||!['prepared','unknown'].includes(this.commandStatus()))return;
    try{if(this.origin()!==draft.origin||this.environment.trim()!==draft.environment||this.token.trim()!==draft.token)throw Error('identity changed');}catch{this.commandStatus.set('unknown');return;}
    this.commandStatus.set('sending');
    try{const params=new URLSearchParams({environmentId:draft.environment}),body={requestId:draft.requestId,jobId:draft.jobId,reason:draft.reason};const response=await fetch(`${draft.origin}/v1/experimental/operator/resend-cancel-command?${params}`,{method:'POST',headers:{Authorization:`Bearer ${draft.token}`,'Content-Type':'application/json'},body:JSON.stringify(body),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});if(!response.ok)throw Error('result');const value:unknown=await response.json();if(!value||typeof value!=='object')throw Error('result');const result=value as Record<string,unknown>;if(result['requestId']!==draft.requestId||typeof result['replayed']!=='boolean')throw Error('result');const command=this.parseCommand(result['command']);if(command.jobId.toLowerCase()!==draft.jobId.toLowerCase()||this.origin()!==draft.origin||this.environment.trim()!==draft.environment||this.token.trim()!==draft.token)throw Error('context');this.command.set(command);this.commandLookupStatus.set('loaded');this.commandStatus.set('recorded');}catch{this.commandStatus.set('unknown');}
  }
  async lookupCommand():Promise<void>{
    this.clearCommandLookup();const seq=this.commandSeq,controller=new AbortController();this.commandAbort=controller;this.commandLookupStatus.set('loading');
    try{const origin=this.origin(),token=this.token.trim(),environment=this.environment.trim(),requestId=this.commandLookupId.trim();if(!this.uuid(requestId)||!environment||new TextEncoder().encode(environment).length>512||/[\0\r\n]/.test(environment)||token.length>16384||!/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(token))throw Error('input');
      const params=new URLSearchParams({environmentId:environment,requestId});const response=await fetch(`${origin}/v1/experimental/operator/resend-cancel-command?${params}`,{headers:{Authorization:`Bearer ${token}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});if(!response.ok)throw Error('request');const value:unknown=await response.json();if(!value||typeof value!=='object')throw Error('response');const r=value as Record<string,unknown>;if(r['requestId']!==requestId)throw Error('request');if(seq!==this.commandSeq)return;if(origin!==this.origin()||token!==this.token.trim()||environment!==this.environment.trim()||requestId!==this.commandLookupId.trim()){this.clearCommandLookup();return;}if(r['command']===null){this.commandLookupStatus.set('missing');return;}this.command.set(this.parseCommand(r['command']));this.commandLookupStatus.set('loaded');
    }catch{if(seq===this.commandSeq){this.command.set(null);this.commandLookupStatus.set('error');}}finally{if(seq===this.commandSeq)this.commandAbort=null;}
  }
  prepareResolution():void {
    const item=this.detail();
    if(this.resolutionStatus()!=='idle'||!item||!(item.state==='unknown'||(item.state==='rejected'&&(item.httpStatus===408||item.httpStatus===409))))return;
    try {const origin=this.origin(),environment=this.environment.trim(),token=this.token.trim(),requestId=this.detail()!.requestId,reason=this.reason.trim();
      if(!this.uuid(requestId)||!environment||new TextEncoder().encode(environment).length>512||/[\0\r\n]/.test(environment)||token.length>16384||!/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(token)||!['confirmed','not_cancelled'].includes(this.decision)||!reason||new TextEncoder().encode(reason).length>500||/[\x00-\x1f\x7f]/.test(reason))throw Error('input');
      this.resolutionDraft.set({origin,environment,token,resolutionId:crypto.randomUUID(),requestId,decision:this.decision,reason});this.confirm=false;this.resolutionStatus.set('prepared');
    }catch{this.resolutionStatus.set('idle');}
  }
  resetResolution():void{if(this.resolutionStatus()==='sending')return;this.resolutionDraft.set(null);this.confirm=false;this.resolutionStatus.set('idle');}
  async submitResolution():Promise<void>{
    const draft=this.resolutionDraft();if(!draft||!this.confirm||!['prepared','unknown'].includes(this.resolutionStatus()))return;
    try{if(this.origin()!==draft.origin||this.environment.trim()!==draft.environment||this.token.trim()!==draft.token)throw Error('identity changed');}catch{this.resolutionStatus.set('unknown');return;}
    this.resolutionStatus.set('sending');
    try{const params=new URLSearchParams({environmentId:draft.environment});const body={resolutionId:draft.resolutionId,requestId:draft.requestId,decision:draft.decision,reason:draft.reason};
      const response=await fetch(`${draft.origin}/v1/experimental/operator/resend-cancel-resolution?${params}`,{method:'POST',headers:{Authorization:`Bearer ${draft.token}`,'Content-Type':'application/json'},body:JSON.stringify(body),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
      if(!response.ok)throw Error('result');const value:unknown=await response.json();if(!value||typeof value!=='object')throw Error('result');const result=value as Record<string,unknown>;
      if(result['resolutionId']!==draft.resolutionId||result['requestId']!==draft.requestId||result['decision']!==draft.decision||typeof result['replayed']!=='boolean')throw Error('result');
      if(this.origin()!==draft.origin||this.environment.trim()!==draft.environment||this.token.trim()!==draft.token)throw Error('context');
      this.resolutionStatus.set('recorded');this.clearDetail();this.clearLegacy();
    }catch{this.resolutionStatus.set('unknown');}
  }
  clearDetail():void{this.detailSeq++;this.detailAbort?.abort();this.detailAbort=null;this.detail.set(null);this.detailStatus.set('idle');}
  async loadDetail(id?:string):Promise<void>{
    if(id)this.requestId=id;this.clearDetail();const seq=this.detailSeq,controller=new AbortController();this.detailAbort=controller;this.detailStatus.set('loading');
    try{
      const origin=this.origin(),token=this.token.trim(),env=this.environment.trim(),requestId=this.requestId.trim();
      if(!env||new TextEncoder().encode(env).length>512||/[\0\r\n]/.test(env)||token.length>16384||!/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(token)||!this.uuid(requestId))throw Error('input');
      const params=new URLSearchParams({environmentId:env,requestId});const response=await fetch(`${origin}/v1/experimental/operator/resend-cancel-detail?${params}`,{headers:{Authorization:`Bearer ${token}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});if(!response.ok)throw Error('request');
      const value:unknown=await response.json();if(!value||typeof value!=='object')throw Error('response');const data=value as Record<string,unknown>;
      if(seq!==this.detailSeq)return;if(origin!==this.origin()||token!==this.token.trim()||env!==this.environment.trim()||requestId!==this.requestId.trim()){this.clearDetail();return;}
      if(data['request']===null&&data['resolution']===null){this.detailStatus.set('missing');return;}
      if(!data['request']||typeof data['request']!=='object')throw Error('request');const r=data['request'] as Record<string,unknown>;
      const text=(v:unknown,max:number):v is string=>typeof v==='string'&&v.length>0&&v.length<=max;
      if(!this.uuid(r['requestId'])||r['requestId'].toLowerCase()!==requestId.toLowerCase()||!this.uuid(r['jobId'])||!text(r['providerId'],1024)||!text(r['databasePrincipal'],128)||!text(r['reason'],500)||!text(r['createdAt'],128)||!text(r['updatedAt'],128)||!['unknown','confirmed','rejected','not_cancelled'].includes(r['state'] as string)||!(r['httpStatus']===null||(Number.isInteger(r['httpStatus'])&&(r['httpStatus'] as number)>=400&&(r['httpStatus'] as number)<=499)))throw Error('request');
      let resolution:Detail['resolution']=null;
      if(data['resolution']!==null){if(!data['resolution']||typeof data['resolution']!=='object')throw Error('resolution');const v=data['resolution'] as Record<string,unknown>;if(!this.uuid(v['resolutionId'])||!['confirmed','not_cancelled'].includes(v['decision'] as string)||v['decision']!==r['state']||!text(v['databasePrincipal'],128)||!text(v['reason'],500)||!text(v['recordedAt'],128))throw Error('resolution');let operator:NonNullable<Detail['resolution']>['operator']=null;if(v['operator']!==null && v['operator']!==undefined){if(typeof v['operator']!=='object')throw Error('operator');const o=v['operator'] as Record<string,unknown>;if(!text(o['issuer'],2048)||!text(o['subject'],1024)||!text(o['clientId'],1024)||!this.uuid(o['resolutionId']))throw Error('operator');operator={issuer:o['issuer'],subject:o['subject'],clientId:o['clientId'],resolutionId:o['resolutionId']};}resolution={resolutionId:v['resolutionId'],decision:v['decision'] as string,databasePrincipal:v['databasePrincipal'],reason:v['reason'],recordedAt:v['recordedAt'],operator};}
      this.detail.set({requestId:r['requestId'],jobId:r['jobId'],providerId:r['providerId'],databasePrincipal:r['databasePrincipal'],reason:r['reason'],state:r['state'] as string,httpStatus:r['httpStatus'] as number|null,createdAt:r['createdAt'],updatedAt:r['updatedAt'],resolution});this.detailStatus.set('loaded');
    }catch{if(seq===this.detailSeq){this.detail.set(null);this.detailStatus.set('error');}}finally{if(seq===this.detailSeq)this.detailAbort=null;}
  }
  private uuid(v:unknown):v is string{return typeof v==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v);}
  private origin():string{const u=new URL(this.apiOrigin.trim());if(!['http:','https:'].includes(u.protocol)||u.username||u.password||u.pathname!=='/'||u.search||u.hash||(u.protocol==='http:'&&!['localhost','127.0.0.1','[::1]'].includes(u.hostname)))throw Error('origin');return u.origin;}
  async list(direction?:'next'|'previous'):Promise<void>{
    const after=direction==='next'?this.next():direction==='previous'?this.cursors[this.page()-2]:null;
    if((direction==='next'&&!after)||after===undefined)return;
    const page=direction==='next'?this.page()+1:direction==='previous'?this.page()-1:1,seq=++this.seq;this.abort?.abort();const controller=new AbortController();this.abort=controller;this.rows.set([]);this.next.set(null);this.status.set('loading');
    try{
      const token=this.token.trim(),env=this.environment.trim(),grace=this.grace,origin=this.origin();
      if(!env||new TextEncoder().encode(env).length>512||/[\0\r\n]/.test(env)||token.length>16384||!/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(token)||!Number.isInteger(grace)||grace<1||grace>86400)throw Error('input');
      const params=new URLSearchParams({environmentId:env,graceSeconds:String(grace)});if(after)params.set('after',after);
      const response=await fetch(`${origin}/v1/experimental/operator/resend-cancel-list?${params}`,{headers:{Authorization:`Bearer ${token}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});if(!response.ok)throw Error('request');
      const value:unknown=await response.json();if(!value||typeof value!=='object')throw Error('response');const data=value as Record<string,unknown>;
      if(!Array.isArray(data['data'])||data['data'].length>50||!(data['next']===null||this.uuid(data['next'])))throw Error('page');
      const rows:Row[]=data['data'].map((v:unknown)=>{if(!v||typeof v!=='object')throw Error('row');const r=v as Record<string,unknown>;if(!this.uuid(r['requestId'])||!this.uuid(r['jobId'])||typeof r['createdAt']!=='string'||r['createdAt'].length>128||typeof r['overdue']!=='boolean')throw Error('row');return {requestId:r['requestId'],jobId:r['jobId'],createdAt:r['createdAt'],overdue:r['overdue']};});
      if(rows.some((r,i)=>i>0&&rows[i-1].requestId.toLowerCase()>=r.requestId.toLowerCase())||(after&&rows.some(r=>r.requestId.toLowerCase()<=after.toLowerCase()))||(data['next']!==null&&(rows.length!==50||data['next']!==rows[rows.length-1].requestId)))throw Error('page');
      if(seq!==this.seq)return;if(origin!==this.origin()||token!==this.token.trim()||env!==this.environment.trim()||grace!==this.grace){this.clear();return;}
      this.rows.set(rows);this.next.set(data['next'] as string|null);this.page.set(page);this.cursors=this.cursors.slice(0,page-1);this.cursors.push(after);this.status.set('loaded');
    }catch{if(seq===this.seq){this.rows.set([]);this.next.set(null);this.status.set('error');}}finally{if(seq===this.seq)this.abort=null;}
  }
}
