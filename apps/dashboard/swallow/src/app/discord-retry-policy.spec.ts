import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { DiscordRetryPolicy } from './discord-retry-policy';
import { App } from './app';
const current={mode:'off',maxRetries:0,fallbackSeconds:30,revision:'0'};
function setup(){const fixture=TestBed.createComponent(DiscordRetryPolicy),c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.destination='ops';c.token='aaa.bbb.ccc';return {fixture,c};}
describe('Discord retry policy',()=>{
 afterEach(()=>vi.restoreAllMocks());
 it('reads the destination, changes the policy, and checks request and audit',async()=>{
  const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify(current))).mockImplementationOnce(async (_url,opts)=>{const b=JSON.parse(opts!.body as string);return new Response(JSON.stringify({requestId:b.requestId,replayed:false,policy:{mode:'known',maxRetries:2,fallbackSeconds:30,revision:'1'}}));});
  const {fixture,c}=setup();await c.load();expect(c.policy()).toEqual(current);expect(new URL(mock.mock.calls[0][0] as string).searchParams.get('destinationId')).toBe('ops');c.mode='known';c.maxRetries=2;c.reason='ticket';c.confirm=true;await c.prepare();const draft=c.snapshot()!.body;c.submitConfirm=true;await c.submit();expect(c.changeStatus()).toBe('recorded');
  c.lookupId=draft.requestId;mock.mockResolvedValueOnce(new Response(JSON.stringify({found:true,...draft,policy:{mode:'known',maxRetries:2,fallbackSeconds:30,revision:'1'},createdAt:'today'})));await c.lookup();expect(c.lookupRecord()?.mode).toBe('known');
  mock.mockResolvedValueOnce(new Response(JSON.stringify({data:[{id:'11111111-1111-4111-8111-000000000001',actorIssuer:'issuer',actorSubject:'alice',actorClientId:'console',databasePrincipal:'api',requestId:draft.requestId,previous:current,current:{mode:'known',maxRetries:2,fallbackSeconds:30,revision:'1'},reason:'ticket',recordedAt:'today'}],hasMore:false,next:null})));await c.audit();expect(c.auditRows()[0].current.mode).toBe('known');fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('Discord 429 재시도 정책');fixture.destroy();
 });
 it('accepts audited no-op revisions and rejects incorrect revisions',async()=>{
  const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify(current))).mockImplementationOnce(async (_url,opts)=>{const b=JSON.parse(opts!.body as string);return new Response(JSON.stringify({requestId:b.requestId,replayed:false,policy:current}));});const {fixture,c}=setup();await c.load();c.reason='no change';c.confirm=true;await c.prepare();const draft=c.snapshot()!.body;c.submitConfirm=true;await c.submit();expect(c.changeStatus()).toBe('recorded');
  mock.mockResolvedValueOnce(new Response(JSON.stringify({found:true,...draft,policy:current,createdAt:'today'})));c.lookupId=draft.requestId;await c.lookup();expect(c.lookupStatus()).toBe('found');c.reset();mock.mockResolvedValueOnce(new Response(JSON.stringify(current))).mockImplementationOnce(async (_url,opts)=>{const b=JSON.parse(opts!.body as string);return new Response(JSON.stringify({requestId:b.requestId,replayed:false,policy:{...current,revision:'1'}}));});await c.load();c.reason='no change';c.confirm=true;await c.prepare();c.submitConfirm=true;await c.submit();expect(c.changeStatus()).toBe('unknown');fixture.destroy();
 });
 it('preserves uncertain writes and binds retries to original destination and credentials',async()=>{
  const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify(current))).mockRejectedValueOnce(new Error('lost')).mockImplementationOnce(async (_url,opts)=>{const b=JSON.parse(opts!.body as string);return new Response(JSON.stringify({requestId:b.requestId,replayed:true,policy:{mode:'known',maxRetries:2,fallbackSeconds:30,revision:'1'}}));});const {fixture,c}=setup();await c.load();c.mode='known';c.maxRetries=2;c.reason='ticket';c.confirm=true;await c.prepare();c.submitConfirm=true;await c.submit();expect(c.changeStatus()).toBe('unknown');c.destination='other';c.retryConfirm=true;await c.submit(true);expect(mock).toHaveBeenCalledTimes(2);c.destination='ops';c.retryConfirm=true;await c.submit(true);expect(c.changeStatus()).toBe('recorded');expect(mock.mock.calls[1][1]!.body).toEqual(mock.mock.calls[2][1]!.body);fixture.destroy();
 });
 it('clears stale reads after destination changes and blocks navigation during writes',async()=>{
  let finish!:(r:Response)=>void;vi.spyOn(globalThis,'fetch').mockReturnValueOnce(new Promise(done=>finish=done));const {fixture,c}=setup();const pending=c.load();c.destination='other';c.destinationChanged();finish(new Response(JSON.stringify(current)));await pending;expect(c.policy()).toBeNull();fixture.destroy();const f=TestBed.createComponent(App),app=f.componentInstance;app.setSection('settings');app.discordRetryPolicyBusy=true;app.setSection('overview');expect(app.section()).toBe('settings');f.destroy();
 });
});
