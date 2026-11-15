import { Component, Input, OnDestroy, OnInit, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';

@Component({selector:'app-workflow-trigger',imports:[FormsModule],templateUrl:'./workflow-trigger.html'})
export class WorkflowTrigger implements OnInit, OnDestroy {
  @Input() apiOrigin='';
  apiKey=''; workflow=''; subscriber=''; payload='{}'; mode:'sandbox'|'normal'='sandbox';
  rememberTransaction=false;
  readonly status=signal<'idle'|'sending'|'accepted'|'unknown'|'invalid'>('idle');
  readonly transaction=signal(''); readonly outcome=signal('');
  readonly savedOrigin=signal(''); readonly storageError=signal(false);
  readonly lookupStatus=signal<'idle'|'loading'|'loaded'|'error'>('idle');
  readonly lookupResult=signal<{recorded:boolean;jobCount:number;job:{jobId:string;jobStatus:string}|null}|null>(null);
  private readonly storageKey='swallow.workflow-trigger.transaction.v1';
  private abort:AbortController|null=null;
  private lookupAbort:AbortController|null=null; private lookupSequence=0;
  ngOnInit():void {
    let raw:string|null;
    try {raw=sessionStorage.getItem(this.storageKey);} catch {this.storageError.set(true);return;}
    if(!raw)return;
    this.rememberTransaction=true;
    try {
      const record:unknown=JSON.parse(raw);
      if(!record||typeof record!=='object')throw new Error('invalid stored record');
      const value=record as Record<string,unknown>;
      if(value['version']!==1||typeof value['transactionId']!=='string'||!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value['transactionId'])||typeof value['origin']!=='string'||value['origin'].length>2048)throw new Error('invalid stored record');
      this.transaction.set(value['transactionId']);this.savedOrigin.set(value['origin']);this.rememberTransaction=true;this.status.set('unknown');
    } catch {this.storageError.set(true);this.status.set('unknown');}
  }
  ngOnDestroy():void {this.abort?.abort();this.clearLookup();this.apiKey='';}
  private origin():string {
    const url=new URL(this.apiOrigin.trim());
    if(!['http:','https:'].includes(url.protocol)||url.username||url.password||url.pathname!=='/'||url.search||url.hash||(url.protocol==='http:'&&!['localhost','127.0.0.1','[::1]'].includes(url.hostname)))throw new Error('origin');
    return url.origin;
  }
  clearLookup():void {this.lookupSequence++;this.lookupAbort?.abort();this.lookupAbort=null;this.lookupStatus.set('idle');this.lookupResult.set(null);}
  reset():void {if(this.status()==='sending')return;if(this.rememberTransaction){try{sessionStorage.removeItem(this.storageKey);}catch{this.storageError.set(true);return;}}this.clearLookup();this.storageError.set(false);this.transaction.set('');this.savedOrigin.set('');this.outcome.set('');this.status.set('idle');}
  async lookup():Promise<void> {
    this.clearLookup();const seq=this.lookupSequence,controller=new AbortController();this.lookupAbort=controller;
    let origin:string,key:string,id:string;
    try {origin=this.origin();key=this.apiKey.trim();id=this.transaction();if(origin!==this.savedOrigin()||!key||key.length>4096||/[\r\n\0]/.test(key)||!id)throw new Error('input');}
    catch {this.lookupStatus.set('error');return;}
    this.lookupStatus.set('loading');const timeout=setTimeout(()=>controller.abort(),30000);
    try {
      const params=new URLSearchParams({transactionId:id});
      const response=await fetch(`${origin}/v1/experimental/workflow-trigger-result?${params}`,{headers:{Authorization:`ApiKey ${key}`},credentials:'omit',cache:'no-store',redirect:'error',signal:controller.signal});
      if(!response.ok)throw new Error('request');const value:unknown=await response.json();
      if(!value||typeof value!=='object')throw new Error('response');const data=(value as Record<string,unknown>)['data'];if(!data||typeof data!=='object')throw new Error('response');const d=data as Record<string,unknown>;
      if(d['transactionId']!==id||typeof d['recorded']!=='boolean'||![0,1,2].includes(d['jobCount'] as number))throw new Error('response');
      let job:{jobId:string;jobStatus:string}|null=null;
      if(d['jobCount']===1){const j=d['job'];if(!j||typeof j!=='object')throw new Error('response');const v=j as Record<string,unknown>;if(typeof v['jobId']!=='string'||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v['jobId'])||typeof v['jobStatus']!=='string'||!['pending','running','completed','failed','cancelled'].includes(v['jobStatus']))throw new Error('response');job={jobId:v['jobId'],jobStatus:v['jobStatus']};}else if(d['job']!==null)throw new Error('response');
      if(seq!==this.lookupSequence)return;
      if(origin!==this.origin()||key!==this.apiKey.trim()||id!==this.transaction()){this.clearLookup();return;}
      this.lookupResult.set({recorded:d['recorded'] as boolean,jobCount:d['jobCount'] as number,job});this.lookupStatus.set('loaded');
    } catch {if(seq===this.lookupSequence){this.lookupResult.set(null);this.lookupStatus.set('error');}}
    finally {clearTimeout(timeout);if(seq===this.lookupSequence)this.lookupAbort=null;}
  }
  async submit():Promise<void> {
    if(this.status()!=='idle'&&this.status()!=='invalid')return;
    let body:Record<string,unknown>, origin:string, key:string;
    try {
      origin=this.origin(); key=this.apiKey.trim();const workflow=this.workflow.trim(),subscriber=this.subscriber.trim();
      if(!key||key.length>4096||/[\r\n\0]/.test(key)||!workflow||!subscriber||new TextEncoder().encode(workflow).length>512||new TextEncoder().encode(subscriber).length>512||/[\r\n\0]/.test(workflow+subscriber)||!['sandbox','normal'].includes(this.mode)||new TextEncoder().encode(this.payload).length>65536)throw new Error('input');
      const payload:unknown=JSON.parse(this.payload);if(!payload||Array.isArray(payload)||typeof payload!=='object')throw new Error('payload');
      const transactionId=crypto.randomUUID();
      body={name:workflow,to:subscriber,payload,transactionId};
      if(this.mode==='sandbox')body['overrides']={providers:{sendgrid:{mailSettings:{sandboxMode:{enable:true}}}}};
      if(this.rememberTransaction) {
        try {if(sessionStorage.getItem(this.storageKey)!==null)throw new Error('record already exists');sessionStorage.setItem(this.storageKey,JSON.stringify({version:1,transactionId,origin}));}
        catch {this.storageError.set(true);throw new Error('storage unavailable');}
        this.savedOrigin.set(origin);
      }
      this.savedOrigin.set(origin);this.transaction.set(transactionId);this.outcome.set('');
    } catch {this.status.set('invalid');return;}
    const controller=new AbortController();this.abort=controller;this.status.set('sending');
    const timeout=setTimeout(()=>controller.abort(),30000);
    try {
      const response=await fetch(`${origin}/v1/events/trigger`,{method:'POST',headers:{Authorization:`ApiKey ${key}`,'Content-Type':'application/json'},body:JSON.stringify(body),credentials:'omit',cache:'no-store',redirect:'error',signal:controller.signal});
      if(response.status!==201)throw new Error('outcome');
      const result:unknown=await response.json();
      if(!result||typeof result!=='object')throw new Error('outcome');
      const data=(result as Record<string,unknown>)['data'];
      if(!data||typeof data!=='object')throw new Error('outcome');
      const d=data as Record<string,unknown>;
      if(d['acknowledged']!==true||d['transactionId']!==this.transaction()||typeof d['status']!=='string'||!['processed','trigger_not_active','no_workflow_steps_defined','no_tenant_found'].includes(d['status']))throw new Error('outcome');
      this.outcome.set(d['status']);this.status.set('accepted');
    } catch {this.status.set('unknown');}
    finally {clearTimeout(timeout);this.abort=null;}
  }
}
