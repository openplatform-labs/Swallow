import { Component, Input, OnDestroy, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';

@Component({selector:'app-workflow-trigger',imports:[FormsModule],templateUrl:'./workflow-trigger.html'})
export class WorkflowTrigger implements OnDestroy {
  @Input() apiOrigin='';
  apiKey=''; workflow=''; subscriber=''; payload='{}'; mode:'sandbox'|'normal'='sandbox';
  readonly status=signal<'idle'|'sending'|'accepted'|'unknown'|'invalid'>('idle');
  readonly transaction=signal(''); readonly outcome=signal('');
  private abort:AbortController|null=null;
  ngOnDestroy():void {this.abort?.abort();this.apiKey='';}
  private origin():string {
    const url=new URL(this.apiOrigin.trim());
    if(!['http:','https:'].includes(url.protocol)||url.username||url.password||url.pathname!=='/'||url.search||url.hash||(url.protocol==='http:'&&!['localhost','127.0.0.1','[::1]'].includes(url.hostname)))throw new Error('origin');
    return url.origin;
  }
  reset():void {if(this.status()==='sending')return;this.transaction.set('');this.outcome.set('');this.status.set('idle');}
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
      this.transaction.set(transactionId);this.outcome.set('');
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
