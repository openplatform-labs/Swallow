import { Component, EventEmitter, Input, OnDestroy, Output, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
type Audit = {id:string;internalId:string;identifier:string;action:string;oldActive:boolean|null;newActive:boolean;userId:string;recordedAt:string};
type Integration = {internalId:string;identifier:string;active:boolean;createdAt:string};
@Component({selector:'app-fcm-integrations',imports:[FormsModule],templateUrl:'./fcm-integrations.html'})
export class FcmIntegrations implements OnDestroy {
  @Input() apiOrigin=''; @Output() busy=new EventEmitter<boolean>();
  key=''; identifier=''; active=false; confirmed=false;
  readonly selected=signal<Integration|null>(null);
  readonly rows=signal<Integration[]>([]); readonly next=signal<string|null>(null);
  readonly listStatus=signal<'idle'|'loading'|'loaded'|'error'>('idle');
  readonly writeStatus=signal<'idle'|'sending'|'saved'|'rejected'|'unknown'>('idle'); readonly message=signal('');
  readonly audits=signal<Audit[]>([]); readonly auditNext=signal<string|null>(null);
  readonly auditStatus=signal<'idle'|'loading'|'loaded'|'error'>('idle');
  private auditSequence=0; private auditAbort:AbortController|null=null;
  private sequence=0; private abort:AbortController|null=null;
  ngOnDestroy():void { this.clear(true); }
  clear(forgetKey=false):void {
    if(this.writeStatus()==='sending') return;
    this.auditSequence++;this.auditAbort?.abort();this.auditAbort=null;this.audits.set([]);this.auditNext.set(null);this.auditStatus.set('idle');
    this.sequence++;this.abort?.abort();this.abort=null;this.rows.set([]);this.next.set(null);this.selected.set(null);
    this.listStatus.set('idle');this.writeStatus.set('idle');this.message.set('');this.confirmed=false;if(forgetKey)this.key='';
  }
  private origin():string {
    const url=new URL(this.apiOrigin.trim());
    if(!['http:','https:'].includes(url.protocol)||url.username||url.password||url.pathname!=='/'||url.search||url.hash||
      (url.protocol==='http:'&&!['localhost','127.0.0.1','[::1]'].includes(url.hostname)))throw new Error('invalid API origin');
    return url.origin;
  }
  private validIdentifier(value:string):boolean {return value!=='.'&&value!=='..'&&/^[A-Za-z0-9._-]{1,128}$/.test(value);}
  private parse(value:unknown):Integration {
    if(!value||typeof value!=='object')throw new Error('invalid response');const row=value as Record<string,unknown>;
    if(typeof row['internalId']!=='string'||!row['internalId'] || row['internalId'].length>128||typeof row['identifier']!=='string'||!this.validIdentifier(row['identifier'])||typeof row['active']!=='boolean'||typeof row['createdAt']!=='string')throw new Error('invalid response');
    return row as Integration;
  }
  select(row:Integration):void {if(this.writeStatus()==='sending')return;this.selected.set(row);this.identifier=row.identifier;this.active=row.active;this.confirmed=false;}
  identifierChanged():void {this.selected.set(null);this.confirmed=false;}
  async list(more=false):Promise<void> {
    if(this.writeStatus()==='sending'||(more&&!this.next()))return;
    const sequence=++this.sequence;this.abort?.abort();const controller=new AbortController();this.abort=controller;
    this.rows.set([]);this.selected.set(null);this.confirmed=false;this.listStatus.set('loading');
    try {
      if(!this.key.trim())throw new Error('missing key');const params=new URLSearchParams();if(more&&this.next())params.set('after',this.next()!);
      const response=await fetch(`${this.origin()}/v1/experimental/fcm-integrations${params.size?'?'+params:''}`,{headers:{Authorization:`ApiKey ${this.key.trim()}`},credentials:'omit',cache:'no-store',redirect:'error',signal:controller.signal});
      if(!response.ok)throw new Error('request failed');const result:unknown=await response.json();
      if(!result||typeof result!=='object')throw new Error('invalid response');const data=result as Record<string,unknown>;
      if(!Array.isArray(data['data'])||data['data'].length>100||typeof data['hasMore']!=='boolean'||(data['hasMore']?typeof data['next']!=='string'||!data['next'] || data['next'].length>128:data['next']!==null))throw new Error('invalid response');
      const rows=data['data'].map(value=>this.parse(value));if(sequence!==this.sequence)return;
      this.rows.set(rows);this.next.set(data['next'] as string|null);this.listStatus.set('loaded');this.writeStatus.set('idle');this.message.set('');
    }catch{if(sequence===this.sequence){this.rows.set([]);this.next.set(null);this.listStatus.set('error');}}
  }
  async audit(more=false):Promise<void> {
    if(this.writeStatus()==='sending'||(more&&!this.auditNext()))return;
    const sequence=++this.auditSequence;this.auditAbort?.abort();const controller=new AbortController();this.auditAbort=controller;this.audits.set([]);this.auditStatus.set('loading');
    try {
      if(!this.key.trim())throw new Error('missing key');const params=new URLSearchParams();if(more&&this.auditNext())params.set('after',this.auditNext()!);
      const response=await fetch(`${this.origin()}/v1/experimental/fcm-integration-audit${params.size?'?'+params:''}`,{headers:{Authorization:`ApiKey ${this.key.trim()}`},credentials:'omit',cache:'no-store',redirect:'error',signal:controller.signal});
      if(!response.ok)throw new Error('request failed');const result:unknown=await response.json();if(!result||typeof result!=='object')throw new Error('invalid response');const page=result as Record<string,unknown>;
      const uuid=(v:unknown)=>typeof v==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(v);
      if(!Array.isArray(page['data'])||page['data'].length>100||(page['nextCursor']!==null&&!uuid(page['nextCursor'])))throw new Error('invalid response');
      const rows=page['data'].map((v:unknown)=>{if(!v||typeof v!=='object')throw new Error('invalid response');const row=v as Record<string,unknown>;
        if(!uuid(row['id'])||typeof row['internalId']!=='string'||!row['internalId']||row['internalId'].length>128||typeof row['identifier']!=='string'||!this.validIdentifier(row['identifier'])||!['create','update'].includes(row['action'] as string)||(row['oldActive']!==null&&typeof row['oldActive']!=='boolean')||typeof row['newActive']!=='boolean'||typeof row['userId']!=='string'||typeof row['recordedAt']!=='string')throw new Error('invalid response');return row as Audit;});
      if(sequence!==this.auditSequence)return;this.audits.set(rows);this.auditNext.set(page['nextCursor'] as string|null);this.auditStatus.set('loaded');
    }catch{if(sequence===this.auditSequence){this.audits.set([]);this.auditNext.set(null);this.auditStatus.set('error');}}
  }
  async save(mode:'create'|'update'):Promise<void> {
    if(!this.confirmed||this.writeStatus()==='sending'||this.writeStatus()==='unknown'||this.listStatus()==='loading')return;
    const expected=this.selected();
    let origin:string;try{origin=this.origin();if(!this.key.trim()||!this.validIdentifier(this.identifier)||(mode==='update'&&(!expected||expected.identifier!==this.identifier)))throw new Error('invalid');}catch{this.writeStatus.set('rejected');this.message.set('API 주소, 인증과 식별자를 확인하세요.');return;}
    this.auditSequence++;this.auditAbort?.abort();this.audits.set([]);this.auditNext.set(null);this.auditStatus.set('idle');
    this.writeStatus.set('sending');this.busy.emit(true);this.confirmed=false;const identifier=this.identifier;
    try {
      const path=mode==='create'?'':`/${encodeURIComponent(identifier)}`;
      const response=await fetch(`${origin}/v1/experimental/fcm-integrations${path}`,{method:mode==='create'?'POST':'PATCH',headers:{Authorization:`ApiKey ${this.key.trim()}`,'Content-Type':'application/json'},body:JSON.stringify(mode==='create'?{identifier,active:this.active}:{active:this.active,expectedActive:expected!.active,expectedInternalId:expected!.internalId}),credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
      if(response.status>=400&&response.status<500&&response.status!==408){this.writeStatus.set('rejected');this.message.set('요청이 거절되었습니다. 권한과 현재 상태를 확인하세요.');}
      else if(!response.ok)throw new Error('unknown');else{const result:unknown=await response.json();if(!result||typeof result!=='object')throw new Error('invalid response');const row=this.parse((result as Record<string,unknown>)['data']);if(row.identifier!==identifier||row.active!==this.active)throw new Error('invalid response');this.writeStatus.set('saved');this.message.set('변경 응답을 확인했습니다. 목록을 다시 조회하세요.');}
    }catch{this.writeStatus.set('unknown');this.message.set('변경 결과가 불명입니다. 재요청 전에 목록과 감사 기록을 확인하세요.');}
    finally{this.rows.set([]);this.next.set(null);this.selected.set(null);this.listStatus.set('idle');this.busy.emit(false);}
  }
}
