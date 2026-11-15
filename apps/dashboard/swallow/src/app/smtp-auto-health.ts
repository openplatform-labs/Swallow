import { Component, Input, OnChanges, OnDestroy, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';

type Health={enabled:boolean;eligible:number;overdue:number;exhausted:number};
@Component({selector:'app-smtp-auto-health',imports:[FormsModule],templateUrl:'./smtp-auto-health.html'})
export class SmtpAutoHealth implements OnChanges,OnDestroy {
 @Input() apiOrigin=''; @Input() environment=''; @Input() token=''; graceSeconds=300;
 readonly health=signal<Health|null>(null); readonly status=signal<'idle'|'loading'|'loaded'|'error'>('idle');
 private seq=0;private abort:AbortController|null=null;
 ngOnChanges():void {this.clear();} ngOnDestroy():void {this.clear();this.token='';}
 clear():void {this.seq++;this.abort?.abort();this.abort=null;this.health.set(null);this.status.set('idle');}
 async load():Promise<void> {
  this.clear();const seq=this.seq,controller=new AbortController();this.abort=controller;this.status.set('loading');
  try {
   const u=new URL(this.apiOrigin.trim()),env=this.environment.trim(),token=this.token.trim();
   if(!['http:','https:'].includes(u.protocol)||u.username||u.password||u.pathname!=='/'||u.search||u.hash||(u.protocol==='http:'&&!['localhost','127.0.0.1','[::1]'].includes(u.hostname))||!env||new TextEncoder().encode(env).length>512||/[\0\r\n]/.test(env)||token.length>16384||!/^[-_A-Za-z0-9]+\.[-_A-Za-z0-9]+\.[-_A-Za-z0-9]+$/.test(token)||!Number.isInteger(this.graceSeconds)||this.graceSeconds<1||this.graceSeconds>86400)throw Error('input');
   const params=new URLSearchParams({environmentId:env,graceSeconds:String(this.graceSeconds)});
   const response=await fetch(`${u.origin}/v1/experimental/operator/smtp-auto-health?${params}`,{headers:{Authorization:`Bearer ${token}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});
   if(!response.ok)throw Error('request');const value:unknown=await response.json();if(!value||typeof value!=='object')throw Error('response');const r=value as Record<string,unknown>;
   if(['eligible','overdue','exhausted'].some(k=>!Number.isSafeInteger(r[k])||(r[k] as number)<0)||typeof r['enabled']!=='boolean'||(r['overdue'] as number)>(r['eligible'] as number))throw Error('response');
   if(seq!==this.seq)return;this.health.set({enabled:r['enabled'] as boolean,eligible:r['eligible'] as number,overdue:r['overdue'] as number,exhausted:r['exhausted'] as number});this.status.set('loaded');
  } catch {if(seq===this.seq)this.status.set('error');} finally {if(seq===this.seq)this.abort=null;}
 }
}
