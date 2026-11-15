import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { AlertTiming } from './alert-timing';
import { App } from './app';
const current={revision:'0',grace:null,heartbeat:null,cooldown:null};
function setup(){const fixture=TestBed.createComponent(AlertTiming),c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.destination='ops';c.token='aaa.bbb.ccc';return {fixture,c};}
describe('alert timing',()=>{
 afterEach(()=>vi.restoreAllMocks());
 it('reads, prepares, submits, and checks request and audit with null inheritance',async()=>{
  const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify(current))).mockImplementationOnce(async (_url,opts)=>{const b=JSON.parse(opts!.body as string);return new Response(JSON.stringify({requestId:b.requestId,replayed:false,settings:{revision:'1',grace:60,heartbeat:null,cooldown:120}}));});
  const {fixture,c}=setup();await c.load();const u=new URL(mock.mock.calls[0][0] as string);expect(u.searchParams.get('targetId')).toBe('ops');expect(u.searchParams.get('channel')).toBe('slack');c.grace=60;c.cooldown=120;c.reason='ticket';c.confirm=true;await c.prepare();const draft=c.snapshot()!.body;expect(draft.heartbeat).toBeNull();c.submitConfirm=true;await c.submit();expect(c.changeStatus()).toBe('recorded');
  c.lookupId=draft.requestId;mock.mockResolvedValueOnce(new Response(JSON.stringify({found:true,...draft,settings:{revision:'1',grace:60,heartbeat:null,cooldown:120},createdAt:'today'})));await c.lookup();expect(c.lookupRecord()?.grace).toBe(60);
  mock.mockResolvedValueOnce(new Response(JSON.stringify({data:[{id:'11111111-1111-4111-8111-000000000001',actorIssuer:'issuer',actorSubject:'alice',actorClientId:'console',databasePrincipal:'api',requestId:draft.requestId,previous:current,current:{revision:'1',grace:60,heartbeat:null,cooldown:120},reason:'ticket',recordedAt:'today'}],hasMore:false,next:null})));await c.audit();expect(c.auditRows()[0].current.cooldown).toBe(120);fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('경보 대상별 시간 설정');fixture.destroy();
 });
 it('requires valid timings and prevents preparing without confirmation',async()=>{
  vi.spyOn(globalThis,'fetch').mockResolvedValue(new Response(JSON.stringify(current)));const {fixture,c}=setup();await c.load();c.reason='ticket';expect(c.canPrepare()).toBe(false);c.confirm=true;c.cooldown=59;expect(c.canPrepare()).toBe(false);c.cooldown=60;c.grace=1.5;expect(c.canPrepare()).toBe(false);c.grace=null;expect(c.canPrepare()).toBe(true);fixture.destroy();
 });
 it('preserves an uncertain write and only retries with the original channel and token',async()=>{
  const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify(current))).mockRejectedValueOnce(new Error('lost')).mockImplementationOnce(async (_url,opts)=>{const b=JSON.parse(opts!.body as string);return new Response(JSON.stringify({requestId:b.requestId,replayed:true,settings:{revision:'1',grace:60,heartbeat:null,cooldown:null}}));});const {fixture,c}=setup();await c.load();c.grace=60;c.reason='ticket';c.confirm=true;await c.prepare();c.submitConfirm=true;await c.submit();expect(c.changeStatus()).toBe('unknown');c.channel='email';c.retryConfirm=true;await c.submit(true);expect(mock).toHaveBeenCalledTimes(2);c.channel='slack';c.retryConfirm=true;await c.submit(true);expect(c.changeStatus()).toBe('recorded');expect(mock.mock.calls[1][1]!.body).toEqual(mock.mock.calls[2][1]!.body);fixture.destroy();
 });
 it('clears stale reads when channel changes, rejects malformed results, and blocks navigation during writes',async()=>{
  let finish!:(r:Response)=>void;const mock=vi.spyOn(globalThis,'fetch').mockReturnValueOnce(new Promise(done=>finish=done));const {fixture,c}=setup();const pending=c.load();c.channel='email';c.destinationChanged();finish(new Response(JSON.stringify(current)));await pending;expect(c.policy()).toBeNull();mock.mockResolvedValueOnce(new Response(JSON.stringify({...current,cooldown:1})));await c.load();expect(c.readStatus()).toBe('error');fixture.destroy();const f=TestBed.createComponent(App),app=f.componentInstance;app.setSection('settings');app.alertTimingBusy=true;app.setSection('overview');expect(app.section()).toBe('settings');f.destroy();
 });
});
