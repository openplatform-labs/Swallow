import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { SmtpTransportPolicy } from './smtp-transport-policy';
import { App } from './app';
const current={mode:'disabled',revision:'0'};
function setup(){const fixture=TestBed.createComponent(SmtpTransportPolicy);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';return {fixture,c};}
describe('SMTP transport policy',()=>{
 afterEach(()=>vi.restoreAllMocks());
 it('prepares and confirms a change, looks up its result, and displays audit',async()=>{
  const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify(current))).mockImplementationOnce(async (_url,opts)=>{const b=JSON.parse(opts!.body as string);return new Response(JSON.stringify({requestId:b.requestId,replayed:false,policy:{mode:'starttls',revision:'1'}}));});
  const {fixture,c}=setup();await c.load();expect(c.policy()).toEqual(current);c.mode='starttls';c.reason='ticket';c.confirm=true;await c.prepare();const draft=c.snapshot()!.body;expect(draft.expectedRevision).toBe('0');expect(draft.mode).toBe('starttls');await c.submit();expect(mock).toHaveBeenCalledTimes(1);c.submitConfirm=true;await c.submit();expect(c.changeStatus()).toBe('recorded');expect(new URL(mock.mock.calls[1][0] as string).pathname).toBe('/v1/experimental/operator/smtp-transport-policy');
  c.lookupId=draft.requestId;mock.mockResolvedValueOnce(new Response(JSON.stringify({found:true,...draft,policy:{mode:'starttls',revision:'1'},createdAt:'today'})));await c.lookup();expect(c.lookupRecord()?.mode).toBe('starttls');
  mock.mockResolvedValueOnce(new Response(JSON.stringify({data:[{id:'11111111-1111-4111-8111-000000000001',actorIssuer:'issuer',actorSubject:'alice',actorClientId:'console',databasePrincipal:'api',requestId:draft.requestId,previous:current,current:{mode:'starttls',revision:'1'},reason:'ticket',recordedAt:'today'}],hasMore:false,next:null})));await c.audit();expect(c.auditRows()[0].current.mode).toBe('starttls');fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('SMTP 연결 정책');fixture.destroy();
 });
 it('preserves an uncertain request and retries only with matching credentials',async()=>{
  const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify(current))).mockRejectedValueOnce(new Error('lost')).mockImplementationOnce(async (_url,opts)=>{const b=JSON.parse(opts!.body as string);return new Response(JSON.stringify({requestId:b.requestId,replayed:true,policy:{mode:'starttls',revision:'1'}}));});
  const {fixture,c}=setup();await c.load();c.mode='starttls';c.reason='ticket';c.confirm=true;await c.prepare();c.submitConfirm=true;await c.submit();expect(c.changeStatus()).toBe('unknown');c.reset();expect(c.snapshot()).not.toBeNull();c.token='other.token.value';c.retryConfirm=true;await c.submit(true);expect(mock).toHaveBeenCalledTimes(2);c.token='aaa.bbb.ccc';c.retryConfirm=true;await c.submit(true);expect(c.changeStatus()).toBe('recorded');expect(mock.mock.calls[1][1]!.body).toEqual(mock.mock.calls[2][1]!.body);fixture.destroy();
 });
 it('rejects malformed policy and invalid write response',async()=>{
  vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({mode:'invalid',revision:'0'}))).mockResolvedValueOnce(new Response(JSON.stringify(current))).mockImplementationOnce(async (_url,opts)=>{const b=JSON.parse(opts!.body as string);return new Response(JSON.stringify({requestId:b.requestId,replayed:false,policy:{mode:'none',revision:'1'}}));});
  const {fixture,c}=setup();await c.load();expect(c.readStatus()).toBe('error');await c.load();c.mode='starttls';c.reason='ticket';c.confirm=true;await c.prepare();c.submitConfirm=true;await c.submit();expect(c.changeStatus()).toBe('unknown');fixture.destroy();
 });
 it('clears stale reads when credentials change and blocks navigation during a write',async()=>{
  let finish!:(r:Response)=>void;vi.spyOn(globalThis,'fetch').mockReturnValueOnce(new Promise(done=>finish=done));const {fixture,c}=setup();const pending=c.load();c.token='other.token.value';c.ngOnChanges();finish(new Response(JSON.stringify(current)));await pending;expect(c.policy()).toBeNull();fixture.destroy();
  const appFixture=TestBed.createComponent(App),app=appFixture.componentInstance;app.setSection('settings');app.smtpTransportPolicyBusy=true;app.setSection('overview');expect(app.section()).toBe('settings');app.smtpTransportPolicyBusy=false;app.setSection('overview');expect(app.section()).toBe('overview');appFixture.destroy();
 });
});
