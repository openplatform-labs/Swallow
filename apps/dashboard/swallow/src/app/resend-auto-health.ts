import { Component, Input, OnChanges, OnDestroy, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
type Health={rateLimited:number;exhausted:number;guardDeferred:number;overdueAuto:number};
@Component({selector:'app-resend-auto-health',imports:[FormsModule],templateUrl:'./resend-auto-health.html'})
export class ResendAutoHealth implements OnChanges,OnDestroy {
  @Input() apiOrigin='';@Input() environment='';@Input() token='';
  graceSeconds=300;readonly status=signal<'idle'|'loading'|'loaded'|'error'>('idle');readonly health=signal<Health|null>(null);
  private sequence=0;private abort:AbortController|null=null;
  ngOnChanges():void {this.clear();}ngOnDestroy():void {this.clear();this.token='';}
  clear():void {this.sequence++;this.abort?.abort();this.abort=null;this.health.set(null);this.status.set('idle');}
  private origin():string {const u=new URL(this.apiOrigin.trim());if(!['http:','https:'].includes(u.protocol)||u.username||u.password||u.pathname!=='/'||u.search||u.hash||(u.protocol==='http:'&&!['localhost','127.0.0.1','[::1]'].includes(u.hostname)))throw new Error('origin');return u.origin;}
  async load():Promise<void> {this.clear();const seq=this.sequence,controller=new AbortController();this.abort=controller;this.status.set('loading');
    try{const token=this.token.trim(),env=this.environment.trim(),grace=this.graceSeconds;if(!env||new TextEncoder().encode(env).length>512||/[\0\r\n]/.test(env)||token.length>16384||!/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(token)||!Number.isInteger(grace)||grace<1||grace>86400)throw new Error('input');const params=new URLSearchParams({environmentId:env,graceSeconds:String(grace)});const response=await fetch(`${this.origin()}/v1/experimental/operator/resend-auto-health?${params}`,{headers:{Authorization:`Bearer ${token}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])});if(!response.ok)throw new Error('request');const value:unknown=await response.json();if(!value||typeof value!=='object')throw new Error('response');const row=value as Record<string,unknown>;const fields=['rateLimited','exhausted','guardDeferred','overdueAuto'];if(fields.some(k=>typeof row[k]!=='number'||!Number.isSafeInteger(row[k])||(row[k] as number)<0)||(row['exhausted'] as number)>(row['rateLimited'] as number))throw new Error('response');if(seq!==this.sequence)return;this.health.set({rateLimited:row['rateLimited'] as number,exhausted:row['exhausted'] as number,guardDeferred:row['guardDeferred'] as number,overdueAuto:row['overdueAuto'] as number});this.status.set('loaded');}
    catch{if(seq===this.sequence){this.health.set(null);this.status.set('error');}}finally{if(seq===this.sequence)this.abort=null;}}
}
