import { Component, Input, OnChanges, OnDestroy, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';

type AuditRow = {id:string;workflowIdentifier:string;transactionId:string;issuer:string;subject:string;clientId:string;databasePrincipal:string;recordedAt:string};
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
@Component({selector:'app-execution-audit',imports:[FormsModule],templateUrl:'./execution-audit.html'})
export class ExecutionAudit implements OnChanges, OnDestroy {
  @Input() apiOrigin='';
  environment=''; token='';
  readonly status=signal<'idle'|'loading'|'loaded'|'denied'|'error'>('idle');
  readonly rows=signal<AuditRow[]>([]); readonly next=signal<string|null>(null); readonly page=signal(1);
  private history:(string|null)[]=[null]; private sequence=0; private abort:AbortController|null=null;
  ngOnChanges():void {this.clear();}
  ngOnDestroy():void {this.clear();this.token='';}
  clear():void {this.sequence++;this.abort?.abort();this.abort=null;this.rows.set([]);this.next.set(null);this.page.set(1);this.history=[null];this.status.set('idle');}
  private origin():string {
    const url=new URL(this.apiOrigin.trim());
    if(!['http:','https:'].includes(url.protocol)||url.username||url.password||url.pathname!=='/'||url.search||url.hash||(url.protocol==='http:'&&!['localhost','127.0.0.1','[::1]'].includes(url.hostname)))throw new Error('origin');
    return url.origin;
  }
  async load(direction?:'next'|'previous'):Promise<void> {
    if(this.status()==='loading'||direction==='next'&&!this.next()||direction==='previous'&&this.page()<=1)return;
    const cursor=direction==='next'?this.next():direction==='previous'?this.history[this.page()-2]:null;
    const page=direction==='next'?this.page()+1:direction==='previous'?this.page()-1:1;
    const seq=++this.sequence;const controller=new AbortController();this.abort=controller;this.rows.set([]);this.next.set(null);this.status.set('loading');
    const timeout=setTimeout(()=>controller.abort(),30000);
    try {
      const origin=this.origin(),token=this.token.trim(),environment=this.environment.trim();
      if(!/^[-_A-Za-z0-9]+\.[-_A-Za-z0-9]+\.[-_A-Za-z0-9]+$/.test(token)||token.length>16384||!environment||new TextEncoder().encode(environment).length>512||/[\r\n\0]/.test(environment))throw new Error('input');
      const params=new URLSearchParams({environmentId:environment});if(cursor)params.set('after',cursor);
      const response=await fetch(`${origin}/v1/experimental/operator/execution-audit?${params}`,{headers:{Authorization:`Bearer ${token}`},credentials:'omit',cache:'no-store',redirect:'error',signal:controller.signal});
      if(seq!==this.sequence)return;
      if(response.status===401||response.status===403){this.status.set('denied');return;}
      if(!response.ok)throw new Error('request');
      const data:unknown=await response.json();if(!data||typeof data!=='object')throw new Error('response');const value=data as Record<string,unknown>;
      if(!Array.isArray(value['data'])||value['data'].length>50||typeof value['hasMore']!=='boolean'||(value['hasMore']?(typeof value['next']!=='string'||!uuid.test(value['next'])):value['next']!==null))throw new Error('response');
      const keys=['id','workflowIdentifier','transactionId','issuer','subject','clientId','databasePrincipal','recordedAt'] as const;
      const rows:AuditRow[]=value['data'].map((entry:unknown)=>{if(!entry||typeof entry!=='object')throw new Error('row');const row=entry as Record<string,unknown>;if(keys.some(key=>typeof row[key]!=='string'||(row[key] as string).length>2048)||!uuid.test(row['id'] as string))throw new Error('row');return Object.fromEntries(keys.map(key=>[key,row[key]])) as AuditRow;});
      if(seq!==this.sequence)return;
      if(origin!==this.origin()||token!==this.token.trim()||environment!==this.environment.trim()){this.clear();return;}
      this.rows.set(rows);this.next.set(value['next'] as string|null);this.page.set(page);this.history=this.history.slice(0,page-1);this.history.push(cursor);this.status.set('loaded');
    } catch {if(seq===this.sequence){this.rows.set([]);this.status.set('error');}}
    finally {clearTimeout(timeout);if(seq===this.sequence)this.abort=null;}
  }
}
