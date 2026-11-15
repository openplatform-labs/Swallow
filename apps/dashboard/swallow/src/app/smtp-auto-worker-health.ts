import { Component, Input, OnChanges, OnDestroy, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
type Health={healthy:number;stale:number;minimumHealthy:number;needsAttention:boolean};
@Component({selector:'app-smtp-auto-worker-health',imports:[FormsModule],templateUrl:'./smtp-auto-worker-health.html'})
export class SmtpAutoWorkerHealth implements OnChanges,OnDestroy {
 @Input() apiOrigin='';@Input() environment='';@Input() token='';maxAgeSeconds=60;minimumHealthy=1;
 readonly health=signal<Health|null>(null);readonly status=signal<'idle'|'loading'|'loaded'|'error'>('idle');
 private seq=0;private abort:AbortController|null=null;
 ngOnChanges():void{this.clear();}ngOnDestroy():void{this.clear();this.token='';}
 clear():void{this.seq++;this.abort?.abort();this.abort=null;this.health.set(null);this.status.set('idle');}
 async load():Promise<void>{this.clear();const seq=this.seq,c=new AbortController();this.abort=c;this.status.set('loading');
  try {const u=new URL(this.apiOrigin.trim()),env=this.environment.trim(),token=this.token.trim();
   if(!['http:','https:'].includes(u.protocol)||u.username||u.password||u.pathname!=='/'||u.search||u.hash||(u.protocol==='http:'&&!['localhost','127.0.0.1','[::1]'].includes(u.hostname))||!env||new TextEncoder().encode(env).length>512||/[\0\r\n]/.test(env)||token.length>16384||!/^[-_A-Za-z0-9]+\.[-_A-Za-z0-9]+\.[-_A-Za-z0-9]+$/.test(token)||!Number.isInteger(this.maxAgeSeconds)||this.maxAgeSeconds<1||this.maxAgeSeconds>86400||!Number.isInteger(this.minimumHealthy)||this.minimumHealthy<1||this.minimumHealthy>1000)throw Error('input');
   const params=new URLSearchParams({environmentId:env,maxAgeSeconds:String(this.maxAgeSeconds),minimumHealthy:String(this.minimumHealthy)});
   const response=await fetch(`${u.origin}/v1/experimental/operator/smtp-auto-worker-health?${params}`,{headers:{Authorization:`Bearer ${token}`},credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.any([c.signal,AbortSignal.timeout(10000)])});
   if(!response.ok)throw Error('request');const value:unknown=await response.json();if(!value||typeof value!=='object')throw Error('response');const r=value as Record<string,unknown>;
   if(['healthy','stale'].some(k=>!Number.isSafeInteger(r[k])||(r[k] as number)<0)||r['minimumHealthy']!==this.minimumHealthy||typeof r['needsAttention']!=='boolean'||r['needsAttention']!==((r['healthy'] as number)<this.minimumHealthy))throw Error('response');
   if(seq!==this.seq)return;this.health.set({healthy:r['healthy'] as number,stale:r['stale'] as number,minimumHealthy:r['minimumHealthy'] as number,needsAttention:r['needsAttention']});this.status.set('loaded');
  }catch{if(seq===this.seq)this.status.set('error');}finally{if(seq===this.seq)this.abort=null;}
 }
}
