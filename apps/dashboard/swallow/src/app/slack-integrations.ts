import { Component, EventEmitter, Input, OnDestroy, Output, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
type Audit={id:string;integrationIdentifier:string;connectionIdentifier:string|null;action:string;userId:string;recordedAt:string};
type ListKind='integrations'|'connections';
type Operation='create-integration'|'delete-integration'|'create-connection'|'delete-connection';
@Component({selector:'app-slack-integrations',imports:[FormsModule],templateUrl:'./slack-integrations.html'})
export class SlackIntegrations implements OnDestroy {
  @Input() apiOrigin=''; @Output() busy=new EventEmitter<boolean>();
  key=''; integration=''; connection=''; operation:Operation='create-integration'; confirmed=false;
  readonly items=signal<string[]>([]); readonly next=signal<string|null>(null);readonly listKind=signal<ListKind>('integrations');
  readonly listStatus=signal<'idle'|'loading'|'loaded'|'error'>('idle');
  readonly writeStatus=signal<'idle'|'sending'|'saved'|'rejected'|'unknown'>('idle'); readonly message=signal('');
  readonly audits=signal<Audit[]>([]);readonly auditNext=signal<string|null>(null);readonly auditStatus=signal<'idle'|'loading'|'loaded'|'error'>('idle');
  private auditSequence=0;private auditAbort:AbortController|null=null;
  private sequence=0;private abort:AbortController|null=null;
  ngOnDestroy():void {this.clear(true);}
  clear(forget=false):void {if(this.writeStatus()==='sending')return;this.auditSequence++;this.auditAbort?.abort();this.auditAbort=null;this.audits.set([]);this.auditNext.set(null);this.auditStatus.set('idle');this.sequence++;this.abort?.abort();this.abort=null;this.items.set([]);this.next.set(null);this.listStatus.set('idle');this.writeStatus.set('idle');this.message.set('');this.confirmed=false;if(forget)this.key='';}
  private origin():string {const url=new URL(this.apiOrigin.trim());if(!['http:','https:'].includes(url.protocol)||url.username||url.password||url.pathname!=='/'||url.search||url.hash||(url.protocol==='http:'&&!['localhost','127.0.0.1','[::1]'].includes(url.hostname)))throw new Error('invalid origin');return url.origin;}
  private valid(value:string):boolean{return value!=='.'&&value!=='..'&&/^[A-Za-z0-9._-]{1,128}$/.test(value);}
  private cursor(value:unknown):value is string{return typeof value==='string'&&value.length>0&&[...value].length<=255&&!value.includes('\0')&&new TextEncoder().encode(value).length<=1020;}
  private path(kind:ListKind):string {if(kind==='integrations')return '/v1/experimental/slack-integrations';if(!this.valid(this.integration))throw new Error('invalid integration');return `/v1/experimental/slack-integrations/${encodeURIComponent(this.integration)}/connections`;}
  async list(kind:ListKind,more=false):Promise<void> {
    if(this.writeStatus()==='sending'||(more&&(kind!==this.listKind()||!this.next())))return;
    const oldCursor=this.next(),sequence=++this.sequence;this.abort?.abort();const controller=new AbortController();this.abort=controller;this.items.set([]);this.listKind.set(kind);this.listStatus.set('loading');
    try {if(!this.key.trim())throw new Error('key');const params=new URLSearchParams();if(more&&oldCursor)params.set('after',oldCursor);
      const response=await fetch(`${this.origin()}${this.path(kind)}${params.size?'?'+params:''}`,{headers:{Authorization:`ApiKey ${this.key.trim()}`},credentials:'omit',cache:'no-store',redirect:'error',signal:controller.signal});
      if(!response.ok)throw new Error('request');const result:unknown=await response.json();if(!result||typeof result!=='object')throw new Error('response');const page=result as Record<string,unknown>;
      if(!Array.isArray(page['data'])||page['data'].length>100||!page['data'].every(v=>this.cursor(v))||typeof page['hasMore']!=='boolean'||(page['hasMore']?!this.cursor(page['next']):page['next']!==null))throw new Error('response');
      if(sequence!==this.sequence)return;this.items.set(page['data'] as string[]);this.next.set(page['next'] as string|null);this.listStatus.set('loaded');this.writeStatus.set('idle');this.message.set('');
    }catch{if(sequence===this.sequence){this.items.set([]);this.next.set(null);this.listStatus.set('error');}}
  }
  async audit(more=false):Promise<void> {
    if(this.writeStatus()==='sending'||(more&&!this.auditNext()))return;
    const sequence=++this.auditSequence;this.auditAbort?.abort();const controller=new AbortController();this.auditAbort=controller;this.audits.set([]);this.auditStatus.set('loading');
    try {if(!this.key.trim())throw new Error('key');const params=new URLSearchParams();if(more&&this.auditNext())params.set('after',this.auditNext()!);
      const response=await fetch(`${this.origin()}/v1/experimental/slack-integration-audit${params.size?'?'+params:''}`,{headers:{Authorization:`ApiKey ${this.key.trim()}`},credentials:'omit',cache:'no-store',redirect:'error',signal:controller.signal});
      if(!response.ok)throw new Error('request');const result:unknown=await response.json();if(!result||typeof result!=='object')throw new Error('response');const page=result as Record<string,unknown>;
      const uuid=(v:unknown)=>typeof v==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(v);
      if(!Array.isArray(page['data'])||page['data'].length>100||(page['nextCursor']!==null&&!uuid(page['nextCursor'])))throw new Error('response');
      const rows=page['data'].map((v:unknown)=>{if(!v||typeof v!=='object')throw new Error('response');const row=v as Record<string,unknown>;if(!uuid(row['id'])||!this.cursor(row['integrationIdentifier'])||(row['connectionIdentifier']!==null&&!this.cursor(row['connectionIdentifier']))||!['create','delete'].includes(row['action'] as string)||typeof row['userId']!=='string'||typeof row['recordedAt']!=='string')throw new Error('response');return row as Audit;});
      if(sequence!==this.auditSequence)return;this.audits.set(rows);this.auditNext.set(page['nextCursor'] as string|null);this.auditStatus.set('loaded');
    }catch{if(sequence===this.auditSequence){this.audits.set([]);this.auditNext.set(null);this.auditStatus.set('error');}}
  }
  async save():Promise<void>{
    if(!this.confirmed||this.writeStatus()==='sending'||this.writeStatus()==='unknown'||this.listStatus()==='loading')return;
    const connection=this.operation.endsWith('connection'),remove=this.operation.startsWith('delete');let origin:string;
    try{origin=this.origin();if(!this.key.trim()||!this.valid(this.integration)||(connection&&!this.valid(this.connection)))throw new Error('input');}catch{this.writeStatus.set('rejected');this.message.set('API 주소, 인증과 식별자를 확인하세요.');return;}
    const integration=this.integration,id=this.connection,base='/v1/experimental/slack-integrations';
    const url=connection?`${base}/${encodeURIComponent(integration)}/connections${remove?'/'+encodeURIComponent(id):''}`:`${base}${remove?'/'+encodeURIComponent(integration):''}`;
    this.auditSequence++;this.auditAbort?.abort();this.audits.set([]);this.auditNext.set(null);this.auditStatus.set('idle');
    this.writeStatus.set('sending');this.busy.emit(true);this.confirmed=false;
    try {const response=await fetch(origin+url,{method:remove?'DELETE':'POST',headers:{Authorization:`ApiKey ${this.key.trim()}`,...(remove?{}:{'Content-Type':'application/json'})},...(remove?{}:{body:JSON.stringify({identifier:connection?id:integration})}),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
      if(response.status>=400&&response.status<500&&response.status!==408){this.writeStatus.set('rejected');this.message.set('요청이 거절되었습니다. 권한과 연결 상태를 확인하세요.');}
      else if(!response.ok)throw new Error('unknown');else{const value:unknown=await response.json();if(!value||typeof value!=='object')throw new Error('response');const data=(value as Record<string,unknown>)['data'];if(!data||typeof data!=='object')throw new Error('response');const result=data as Record<string,unknown>;if(result['integrationIdentifier']!==integration||result['connectionIdentifier']!==(connection?id:null))throw new Error('response');this.writeStatus.set('saved');this.message.set('변경 응답을 확인했습니다. 목록을 다시 조회하세요.');}
    }catch{this.writeStatus.set('unknown');this.message.set('변경 결과가 불명입니다. 다시 조치하기 전에 목록과 서버 감사 기록을 확인하세요.');}
    finally{this.items.set([]);this.next.set(null);this.listStatus.set('idle');this.busy.emit(false);}
  }
}
