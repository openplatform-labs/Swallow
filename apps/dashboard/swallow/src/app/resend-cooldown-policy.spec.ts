import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ResendCooldownPolicy } from './resend-cooldown-policy';
import { App } from './app';
const policy={fallbackSeconds:30,notBefore:null,revision:'0'};
function setup(){const fixture=TestBed.createComponent(ResendCooldownPolicy);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';return {fixture,c};}
describe('Resend shared cooldown policy',()=>{
 afterEach(()=>vi.restoreAllMocks());
 it('prepares before sending and loads request and separate audit',async()=>{
  const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({exists:false}))).mockImplementationOnce(async (_url,opts)=>{const b=JSON.parse(opts!.body as string);return new Response(JSON.stringify({requestId:b.requestId,replayed:false,policy}));});
  const {fixture,c}=setup();await c.load();c.fallbackSeconds=30;c.reason='ticket';c.confirm=true;await c.prepare();const draft=c.snapshot()!.body;expect(c.changeStatus()).toBe('prepared');expect(mock).toHaveBeenCalledTimes(1);await c.submit();expect(mock).toHaveBeenCalledTimes(1);c.submitConfirm=true;await c.submit();expect(c.changeStatus()).toBe('recorded');expect(JSON.parse(mock.mock.calls[1][1]!.body as string)).toEqual(draft);
  c.lookupId=draft.requestId;mock.mockResolvedValueOnce(new Response(JSON.stringify({found:true,...draft,policy,createdAt:'today',secret:'discard'})));await c.lookup();expect(c.lookupStatus()).toBe('found');expect(JSON.stringify(c.lookupRecord())).not.toContain('discard');
  const row={id:'11111111-1111-4111-8111-000000000001',actorIssuer:'issuer',actorSubject:'alice',actorClientId:'console',databasePrincipal:'api',requestId:draft.requestId,previous:{fallbackSeconds:20,revision:'0'},current:policy,reason:'ticket',recordedAt:'today',secret:'discard'};mock.mockResolvedValueOnce(new Response(JSON.stringify({data:[row],hasMore:false,next:null})));await c.audit();expect(c.auditStatus()).toBe('loaded');expect(c.auditRows()[0].requestId).toBe(draft.requestId);expect(JSON.stringify(c.auditRows())).not.toContain('discard');fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('정책 변경 이력');fixture.destroy();
 });
 it('keeps uncertain results and retries only with the original credentials and request',async()=>{
  const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({exists:true,...policy}))).mockRejectedValueOnce(new Error('lost')).mockImplementationOnce(async (_url,opts)=>{const b=JSON.parse(opts!.body as string);return new Response(JSON.stringify({requestId:b.requestId,replayed:true,policy:{...policy,fallbackSeconds:45,revision:'1'}}));});
  const {fixture,c}=setup();await c.load();c.fallbackSeconds=45;c.reason='ticket';c.confirm=true;await c.prepare();c.submitConfirm=true;await c.submit();expect(c.changeStatus()).toBe('unknown');c.reset();expect(c.snapshot()).not.toBeNull();c.token='different.token.value';c.retryConfirm=true;await c.submit(true);expect(mock).toHaveBeenCalledTimes(2);expect(c.changeStatus()).toBe('unknown');c.token='aaa.bbb.ccc';c.retryConfirm=true;await c.submit(true);expect(c.changeStatus()).toBe('recorded');expect(mock.mock.calls[1][1]!.body).toEqual(mock.mock.calls[2][1]!.body);fixture.destroy();
 });
 it('clears stale responses on credential change and rejects malformed results',async()=>{
  let finish!:(r:Response)=>void;vi.spyOn(globalThis,'fetch').mockReturnValueOnce(new Promise(done=>finish=done)).mockResolvedValueOnce(new Response(JSON.stringify({exists:true,...policy,fallbackSeconds:86401})));
  const {fixture,c}=setup();const pending=c.load();c.token='different.token.value';c.ngOnChanges();finish(new Response(JSON.stringify({exists:true,...policy})));await pending;expect(c.readStatus()).toBe('idle');expect(c.policy()).toBeNull();await c.load();expect(c.readStatus()).toBe('error');fixture.destroy();
 });
 it('marks an interrupted write unknown and preserves the request',async()=>{
  let finish!:(r:Response)=>void;vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({exists:true,...policy}))).mockReturnValueOnce(new Promise(done=>finish=done));
  const {fixture,c}=setup();await c.load();c.fallbackSeconds=45;c.reason='ticket';c.confirm=true;await c.prepare();const id=c.snapshot()!.body.requestId;c.submitConfirm=true;const pending=c.submit();await vi.waitFor(()=>expect(c.changeStatus()).toBe('sending'));c.token='other.token.value';c.ngOnChanges();expect(c.changeStatus()).toBe('unknown');expect(c.snapshot()!.body.requestId).toBe(id);finish(new Response(JSON.stringify({requestId:id,replayed:false,policy:{...policy,fallbackSeconds:45,revision:'1'}})));await pending;expect(c.changeStatus()).toBe('unknown');fixture.destroy();
 });
 it('validates audit pagination and clears rows after authorization changes',async()=>{
  const row=(i:number)=>({id:`11111111-1111-4111-8111-${String(i).padStart(12,'0')}`,actorIssuer:'issuer',actorSubject:'alice',actorClientId:'console',databasePrincipal:'api',requestId:'22222222-2222-4222-8222-222222222222',previous:null,current:policy,reason:'ticket',recordedAt:'today'});const first=Array.from({length:50},(_,i)=>row(i+1));
  const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:first,hasMore:true,next:first[49].id}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[row(51)],hasMore:false,next:null}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[row(1)],hasMore:true,next:row(1).id})));
  const {fixture,c}=setup();await c.audit();expect(c.auditRows().length).toBe(50);await c.audit('next');expect(c.auditPage()).toBe(2);expect(new URL(mock.mock.calls[1][0] as string).searchParams.get('after')).toBe(first[49].id);c.token='new.token.value';c.ngOnChanges();expect(c.auditRows()).toEqual([]);await c.audit();expect(c.auditStatus()).toBe('error');expect(c.auditRows()).toEqual([]);fixture.destroy();
 });
 it('does not prepare or submit while another settings write is active',async()=>{
  const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({exists:false})));const {fixture,c}=setup();await c.load();c.reason='ticket';c.confirm=true;c.blocked=true;await c.prepare();expect(c.changeStatus()).toBe('idle');expect(mock).toHaveBeenCalledTimes(1);fixture.destroy();
 });
 it('blocks navigation while the policy request is in flight',()=>{const fixture=TestBed.createComponent(App),app=fixture.componentInstance;app.setSection('settings');app.resendCooldownPolicyBusy=true;app.setSection('overview');expect(app.section()).toBe('settings');app.resendCooldownPolicyBusy=false;app.setSection('overview');expect(app.section()).toBe('overview');fixture.destroy();});
});
