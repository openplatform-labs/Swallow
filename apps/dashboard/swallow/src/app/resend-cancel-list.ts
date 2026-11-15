import { Component, Input, OnChanges, OnDestroy, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
type Row={requestId:string;jobId:string;createdAt:string;overdue:boolean};
@Component({selector:'app-resend-cancel-list',imports:[FormsModule],templateUrl:'./resend-cancel-list.html'})
export class ResendCancelList implements OnChanges,OnDestroy {
  @Input() apiOrigin='';@Input() environment='';@Input() token='';
  grace=300;readonly status=signal<'idle'|'loading'|'loaded'|'error'>('idle');readonly rows=signal<Row[]>([]);readonly next=signal<string|null>(null);readonly page=signal(1);
  private seq=0;private abort:AbortController|null=null;private cursors:(string|null)[]=[null];
  ngOnChanges():void{this.clear();}ngOnDestroy():void{this.clear();this.token='';}
  clear():void{this.seq++;this.abort?.abort();this.abort=null;this.rows.set([]);this.next.set(null);this.page.set(1);this.cursors=[null];this.status.set('idle');}
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
