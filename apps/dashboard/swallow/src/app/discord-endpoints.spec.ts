import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { DiscordEndpoints } from './discord-endpoints';
import { App } from './app';
const row={identifier:'binding',subscriberId:'subscriber',destinationId:'target',contextKeys:['tenant:one'],revision:'11111111-1111-4111-8111-111111111111'};
function setup(){const fixture=TestBed.createComponent(DiscordEndpoints),c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.key='test-api-key';return {fixture,c};}
describe('Discord endpoints',()=>{
 afterEach(()=>vi.restoreAllMocks());
 it('lists, creates, reads, updates and deletes with revision fencing',async()=>{
  const next={...row,destinationId:'target2',contextKeys:[],revision:'22222222-2222-4222-8222-222222222222'};
  const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[row],next:null}))).mockResolvedValueOnce(new Response(JSON.stringify({data:row}),{status:201})).mockResolvedValueOnce(new Response(JSON.stringify({data:row}))).mockResolvedValueOnce(new Response(JSON.stringify({data:next}))).mockResolvedValueOnce(new Response(JSON.stringify({data:next}))).mockResolvedValueOnce(new Response(JSON.stringify({data:{deleted:true}})));
  const {fixture,c}=setup();await c.list();expect(c.rows()).toEqual([row]);c.identifier='binding';c.subscriberId='subscriber';c.destinationId='target';c.contextText='{"tenant":"one"}';c.confirmed=true;await c.save();expect(c.writeStatus()).toBe('saved');expect(JSON.parse(mock.mock.calls[1][1]!.body as string)).toEqual({identifier:'binding',subscriberId:'subscriber',destinationId:'target',context:{tenant:'one'}});
  c.operation='update';await c.review();expect(c.contextText).toContain('tenant');c.destinationId='target2';c.contextText='';c.confirmed=true;await c.save();expect(c.writeStatus()).toBe('saved');expect(JSON.parse(mock.mock.calls[3][1]!.body as string)).toEqual({expectedRevision:row.revision,destinationId:'target2',context:null});
  c.operation='delete';await c.review();c.confirmed=true;await c.save();expect(c.writeStatus()).toBe('saved');expect(new URL(mock.mock.calls[5][0] as string).searchParams.get('revision')).toBe(next.revision);fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('Discord 수신자 연결');fixture.destroy();
 });
 it('preserves uncertain writes until confirmed and never retries automatically',async()=>{
  const mock=vi.spyOn(globalThis,'fetch').mockRejectedValueOnce(new Error('lost'));const {fixture,c}=setup();c.identifier='binding';c.subscriberId='subscriber';c.destinationId='target';c.confirmed=true;await c.save();expect(c.writeStatus()).toBe('unknown');c.clear();expect(c.writeStatus()).toBe('unknown');c.confirmed=true;await c.save();expect(mock).toHaveBeenCalledTimes(1);c.ackUnknown=true;c.clear();expect(c.writeStatus()).toBe('idle');fixture.destroy();
 });
 it('rejects malformed context, path traversal identifiers and incorrect responses',async()=>{
  const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:{...row,contextKeys:[]}}),{status:201}));const {fixture,c}=setup();c.identifier='..';c.subscriberId='subscriber';c.destinationId='target';c.confirmed=true;await c.save();expect(c.writeStatus()).toBe('rejected');expect(mock).not.toHaveBeenCalled();c.identifier='binding';c.contextText='{"tenant":{"id":"one"}}';c.confirmed=true;await c.save();expect(mock).not.toHaveBeenCalled();c.contextText='{"tenant":"one"}';c.confirmed=true;await c.save();expect(c.writeStatus()).toBe('unknown');fixture.destroy();
 });
 it('discards an in-flight response after key changes and blocks navigation during writes',async()=>{
  let finish!:(r:Response)=>void;vi.spyOn(globalThis,'fetch').mockReturnValueOnce(new Promise(resolve=>finish=resolve));const {fixture,c}=setup();const pending=c.list();c.key='new-key';c.clear();finish(new Response(JSON.stringify({data:[row],next:null})));await pending;expect(c.rows()).toEqual([]);fixture.destroy();
  const appFixture=TestBed.createComponent(App),app=appFixture.componentInstance;app.setSection('channels');app.discordEndpointsBusy=true;app.setSection('overview');expect(app.section()).toBe('channels');app.discordEndpointsBusy=false;app.setSection('overview');expect(app.section()).toBe('overview');appFixture.destroy();
 });
});
