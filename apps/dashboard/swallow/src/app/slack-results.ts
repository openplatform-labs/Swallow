import { Component, EventEmitter, Input, OnChanges, OnDestroy, Output, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { validChannelPage } from './channel-page';
type Decision={requestId:string;jobId:string;attemptId:string;decision:'retry'|'accepted';reason:string};
type Result={jobId:string;jobStatus:string;kind:string;attemptState:string|null;attemptId?:string|null;attemptHttpStatus?:number|null;retryAfterSeconds?:number|null;retryNotBefore?:string|null;providerIdKnown:boolean;hasResolutionHistory:boolean;failureCategory:string|null};
@Component({selector:'app-slack-results',imports:[FormsModule],templateUrl:'./slack-results.html'})
export class SlackResults implements OnChanges,OnDestroy {
  @Input() apiOrigin='';@Input() environment='';@Input() token='';@Output() busyChange=new EventEmitter<boolean>();
  decisionChoice:'retry'|'accepted'='retry';decisionReason='';decisionConfirmed=false;resetConfirmed=false;lookupId='';
  readonly decisionStatus=signal<'idle'|'prepared'|'sending'|'recorded'|'unknown'|'unavailable'>('idle');
  readonly decisionSnapshot=signal<Decision|null>(null);readonly lookupStatus=signal<'idle'|'loading'|'found'|'not-found'|'error'>('idle');readonly lookupRecord=signal<Decision|null>(null);
  private decisionToken:string|null=null;
  private decisionAbort:AbortController|null=null;private decisionSequence=0;private lookupSequence=0;private lookupAbort:AbortController|null=null;
  jobId=''; readonly rows=signal<Result[]>([]); readonly result=signal<Result|null>(null); readonly next=signal<string|null>(null);
  readonly listStatus=signal<'idle'|'loading'|'loaded'|'error'>('idle');readonly resultStatus=signal<'idle'|'loading'|'loaded'|'error'>('idle');
  private listSequence=0;private resultSequence=0;private listAbort:AbortController|null=null;private resultAbort:AbortController|null=null;
  ngOnChanges():void {this.clear();} ngOnDestroy():void {this.clear();this.token='';}
  clear():void {this.listSequence++;this.resultSequence++;this.listAbort?.abort();this.resultAbort?.abort();this.listAbort=null;this.resultAbort=null;this.rows.set([]);this.result.set(null);this.next.set(null);this.listStatus.set('idle');this.resultStatus.set('idle');this.jobId='';this.lookupSequence++;this.lookupAbort?.abort();this.lookupAbort=null;this.lookupStatus.set('idle');this.lookupRecord.set(null);this.decisionToken=null;if(this.decisionStatus()==='sending'){this.decisionSequence++;this.decisionAbort?.abort();this.decisionAbort=null;this.decisionStatus.set('unknown');this.busyChange.emit(false);}else if(this.decisionStatus()==='prepared'){this.decisionStatus.set('idle');this.decisionSnapshot.set(null);}this.decisionConfirmed=false;}
  clearResult():void {this.resultSequence++;this.resultAbort?.abort();this.resultAbort=null;this.result.set(null);this.resultStatus.set('idle');}
  private validUuid(value:unknown):value is string {return typeof value==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);}
  private origin():string {const url=new URL(this.apiOrigin.trim());if(!['http:','https:'].includes(url.protocol)||url.username||url.password||url.pathname!=='/'||url.search||url.hash||(url.protocol==='http:'&&!['localhost','127.0.0.1','[::1]'].includes(url.hostname)))throw new Error('origin');return url.origin;}
  private params():URLSearchParams {const token=this.token.trim(),environment=this.environment.trim();if(token.length>16384||!/^[-_A-Za-z0-9]+\.[-_A-Za-z0-9]+\.[-_A-Za-z0-9]+$/.test(token)||!environment||new TextEncoder().encode(environment).length>512||/[\0\r\n]/.test(environment))throw new Error('credentials');return new URLSearchParams({environmentId:environment});}
  private parse(value:unknown):Result {if(!value||typeof value!=='object')throw new Error('response');const r=value as Record<string,unknown>;
    if(!this.validUuid(r['jobId'])||typeof r['jobStatus']!=='string'||!['pending','running','completed','failed','cancelled'].includes(r['jobStatus'])||typeof r['kind']!=='string'||!['app','webhook','workflow-fanout','workflow-delivery'].includes(r['kind'])||!(r['attemptState']===null||(typeof r['attemptState']==='string'&&['dispatching','accepted','rejected','unknown'].includes(r['attemptState'])))||!(r['attemptId']===undefined||r['attemptId']===null||this.validUuid(r['attemptId']))||(r['attemptId']!==undefined&&((r['attemptId']===null)!==(r['attemptState']===null)))||!(r['attemptHttpStatus']===undefined||r['attemptHttpStatus']===null||(Number.isInteger(r['attemptHttpStatus'])&&(r['attemptHttpStatus'] as number)>=100&&(r['attemptHttpStatus'] as number)<=599))||!(r['retryAfterSeconds']===undefined||r['retryAfterSeconds']===null||(Number.isInteger(r['retryAfterSeconds'])&&(r['retryAfterSeconds'] as number)>=0&&(r['retryAfterSeconds'] as number)<=86400))||!(r['retryNotBefore']===undefined||r['retryNotBefore']===null||(typeof r['retryNotBefore']==='string'&&r['retryNotBefore'].length>0&&r['retryNotBefore'].length<=128))||((r['retryAfterSeconds']===null)!==(r['retryNotBefore']===null))||((r['retryAfterSeconds']===undefined)!==(r['retryNotBefore']===undefined))||(r['retryAfterSeconds']!==undefined&&r['retryAfterSeconds']!==null&&(r['attemptHttpStatus']!==429||r['attemptState']!=='rejected'))||typeof r['providerIdKnown']!=='boolean'||typeof r['hasResolutionHistory']!=='boolean'||!(r['failureCategory']===null||(typeof r['failureCategory']==='string'&&['unknown','provider-rejected','endpoint-review','endpoint-missing','credentials-missing','workflow-policy','identity-review','preference','invalid-request','other'].includes(r['failureCategory']))))throw new Error('response');const item=r as Result;return {jobId:item.jobId,jobStatus:item.jobStatus,kind:item.kind,attemptState:item.attemptState,...(item.attemptId===undefined?{}:{attemptId:item.attemptId}),attemptHttpStatus:item.attemptHttpStatus,retryAfterSeconds:item.retryAfterSeconds,retryNotBefore:item.retryNotBefore,providerIdKnown:item.providerIdKnown,hasResolutionHistory:item.hasResolutionHistory,failureCategory:item.failureCategory};}
  async list(more=false):Promise<void> {if(more&&!this.next())return;const after=this.next();const seq=++this.listSequence;this.listAbort?.abort();const controller=new AbortController();this.listAbort=controller;this.rows.set([]);this.listStatus.set('loading');
    try{const params=this.params();if(more&&after)params.set('after',after);const response=await fetch(`${this.origin()}/v1/experimental/operator/slack-jobs?${params}`,{headers:{Authorization:`Bearer ${this.token.trim()}`},credentials:'omit',cache:'no-store',redirect:'error',signal:controller.signal});if(!response.ok)throw new Error('request');const body:unknown=await response.json();if(!body||typeof body!=='object')throw new Error('response');const data=body as Record<string,unknown>;
      if(!Array.isArray(data['data'])||data['data'].length>20||typeof data['hasMore']!=='boolean'||(data['hasMore']?!this.validUuid(data['next']):data['next']!==null))throw new Error('response');const rows=data['data'].map(v=>this.parse(v));if(!validChannelPage(rows,data['hasMore'] as boolean,data['next'],more?after:null))throw new Error('response');if(seq!==this.listSequence)return;this.rows.set(rows);this.next.set(data['next'] as string|null);this.listStatus.set('loaded');
    }catch{if(seq===this.listSequence){this.rows.set([]);this.next.set(null);this.listStatus.set('error');}}
  }
  select(row:Result):void {this.jobId=row.jobId;this.clearResult();}
  async load():Promise<void> {this.clearResult();const seq=this.resultSequence,controller=new AbortController();this.resultAbort=controller;this.resultStatus.set('loading');
    try{const params=this.params();if(!this.validUuid(this.jobId.trim()))throw new Error('job');params.set('jobId',this.jobId.trim());const response=await fetch(`${this.origin()}/v1/experimental/operator/slack-result?${params}`,{headers:{Authorization:`Bearer ${this.token.trim()}`},credentials:'omit',cache:'no-store',redirect:'error',signal:controller.signal});if(!response.ok)throw new Error('request');const result=this.parse(await response.json());if(result.jobId.toLowerCase()!==this.jobId.trim().toLowerCase())throw new Error('response');if(seq!==this.resultSequence)return;this.result.set(result);this.resultStatus.set('loaded');
    }catch{if(seq===this.resultSequence){this.result.set(null);this.resultStatus.set('error');}}
  }
  prepareDecision():void {
    const result=this.result(),reason=this.decisionReason.trim();
    if(this.decisionStatus()!=='idle'||!result||result.jobStatus!=='failed'||!this.validUuid(result.attemptId)||!['unknown','rejected'].includes(result.attemptState || '')||!['retry','accepted'].includes(this.decisionChoice)||(this.decisionChoice==='accepted'&&result.attemptState!=='unknown')||!reason||new TextEncoder().encode(reason).length>512||/[\0]/.test(reason))return;
    try {const requestId=crypto.randomUUID();this.decisionSnapshot.set({requestId,jobId:result.jobId,attemptId:result.attemptId,decision:this.decisionChoice,reason});this.clearLookup();this.lookupId=requestId;this.decisionConfirmed=false;this.decisionStatus.set('prepared');}
    catch {this.decisionStatus.set('unavailable');}
  }
  async submitDecision():Promise<void> {
    const snapshot=this.decisionSnapshot();if(!snapshot||this.decisionStatus()!=='prepared'||!this.decisionConfirmed)return;
    let url:string,params:URLSearchParams;try{url=this.origin();params=this.params();}catch{this.decisionStatus.set('unknown');return;}
    const sequence=++this.decisionSequence,controller=new AbortController();this.decisionAbort=controller;this.decisionToken=this.token.trim();this.decisionStatus.set('sending');this.busyChange.emit(true);
    try {
      const response=await fetch(`${url}/v1/experimental/operator/slack-decision?${params}`,{method:'POST',headers:{Authorization:`Bearer ${this.token.trim()}`,'Content-Type':'application/json'},body:JSON.stringify(snapshot),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if(!response.ok)throw new Error('uncertain');const value:unknown=await response.json();
      if(!value||typeof value!=='object'||(value as Record<string,unknown>)['recorded']!==true||typeof (value as Record<string,unknown>)['replayed']!=='boolean')throw new Error('uncertain');
      if(sequence!==this.decisionSequence)return;this.decisionStatus.set('recorded');this.clearResult();this.rows.set([]);this.next.set(null);this.listStatus.set('idle');
    } catch {if(sequence===this.decisionSequence)this.decisionStatus.set('unknown');}
    finally {if(sequence===this.decisionSequence){this.decisionAbort=null;this.busyChange.emit(false);}}
  }
  resetDecision():void {if(this.decisionStatus()==='sending'||(this.decisionStatus()==='unknown'&&!this.resetConfirmed))return;this.decisionSnapshot.set(null);this.decisionStatus.set('idle');this.decisionToken=null;this.decisionConfirmed=false;this.resetConfirmed=false;this.decisionReason='';this.clearLookup();this.lookupId='';}
  clearLookup():void {this.lookupSequence++;this.lookupAbort?.abort();this.lookupAbort=null;this.lookupRecord.set(null);this.lookupStatus.set('idle');}
  async lookupDecision():Promise<void> {
    const requestId=this.lookupId.trim().toLowerCase();if(!this.validUuid(requestId)||this.decisionStatus()==='sending')return;
    const sequence=++this.lookupSequence;this.lookupAbort?.abort();const controller=new AbortController();this.lookupAbort=controller;this.lookupRecord.set(null);this.lookupStatus.set('loading');
    try {const params=this.params();params.set('requestId',requestId);const response=await fetch(`${this.origin()}/v1/experimental/operator/slack-decision-request?${params}`,{headers:{Authorization:`Bearer ${this.token.trim()}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
      if(!response.ok)throw new Error('request');const value:unknown=await response.json();if(!value||typeof value!=='object')throw new Error('response');const row=value as Record<string,unknown>;
      if(row['found']===false){if(sequence===this.lookupSequence)this.lookupStatus.set('not-found');return;}
      if(row['found']!==true||row['requestId']!==requestId||!this.validUuid(row['jobId'])||!this.validUuid(row['attemptId'])||!['retry','accepted'].includes(row['decision'] as string)||typeof row['reason']!=='string'||typeof row['createdAt']!=='string')throw new Error('response');
      const snap=this.decisionSnapshot();if(snap&&snap.requestId===requestId&&(snap.jobId!==row['jobId']||snap.attemptId!==row['attemptId']||snap.decision!==row['decision']||snap.reason!==row['reason']))throw new Error('response');
      if(sequence!==this.lookupSequence)return;this.lookupRecord.set({requestId,jobId:row['jobId'] as string,attemptId:row['attemptId'] as string,decision:row['decision'] as 'retry'|'accepted',reason:row['reason'] as string});this.lookupStatus.set('found');if(snap&&snap.requestId===requestId&&this.decisionToken!==null&&this.decisionToken===this.token.trim()) {this.decisionStatus.set('recorded');this.clearResult();this.rows.set([]);this.next.set(null);this.listStatus.set('idle');}
    }catch{if(sequence===this.lookupSequence)this.lookupStatus.set('error');}finally{if(sequence===this.lookupSequence)this.lookupAbort=null;}
  }

}
