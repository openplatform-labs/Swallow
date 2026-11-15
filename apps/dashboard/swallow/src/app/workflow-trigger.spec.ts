import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { WorkflowTrigger } from './workflow-trigger';

describe('workflow trigger',()=>{
  afterEach(()=>{vi.useRealTimers();vi.restoreAllMocks();});
  function setup(){const fixture=TestBed.createComponent(WorkflowTrigger);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.apiKey='test-key';c.workflow='welcome';c.subscriber='person';return {fixture,c};}
  it('sends a sandbox override and displays the trigger acknowledgement',async()=>{
    const id='11111111-1111-4111-8111-111111111111';vi.spyOn(crypto,'randomUUID').mockReturnValue(id);
    const fetcher=vi.spyOn(globalThis,'fetch').mockResolvedValue(new Response(JSON.stringify({data:{acknowledged:true,status:'processed',transactionId:id}}),{status:201}));
    const {fixture,c}=setup();c.payload='{"name":"Lee"}';await c.submit();fixture.detectChanges();
    const [url,options]=fetcher.mock.calls[0];expect(url).toBe('https://api.example.test/v1/events/trigger');expect(options).toMatchObject({method:'POST',headers:{Authorization:'ApiKey test-key','Content-Type':'application/json'},credentials:'omit',redirect:'error',cache:'no-store'});
    expect(JSON.parse(options!.body as string)).toEqual({name:'welcome',to:'person',payload:{name:'Lee'},transactionId:id,overrides:{providers:{sendgrid:{mailSettings:{sandboxMode:{enable:true}}}}}});
    expect(c.status()).toBe('accepted');expect(fixture.nativeElement.textContent).toContain('다른 이메일 공급자는 실제로 실행될 수 있습니다');await c.submit();expect(fetcher).toHaveBeenCalledTimes(1);fixture.destroy();
  });
  it('sends a normal trigger only when selected and prevents automatic retries on uncertain responses',async()=>{
    const id='22222222-2222-4222-8222-222222222222';vi.spyOn(crypto,'randomUUID').mockReturnValue(id);
    const fetcher=vi.spyOn(globalThis,'fetch').mockRejectedValue(new Error('offline'));const {fixture,c}=setup();c.mode='normal';await c.submit();
    expect(JSON.parse(fetcher.mock.calls[0][1]!.body as string)).toEqual({name:'welcome',to:'person',payload:{},transactionId:id});expect(c.status()).toBe('unknown');expect(c.transaction()).toBe(id);
    await c.submit();expect(fetcher).toHaveBeenCalledTimes(1);c.reset();expect(c.status()).toBe('idle');fixture.destroy();expect(c.apiKey).toBe('');
  });
  it('fails closed on unsafe origin, invalid payload and mismatched acknowledgement',async()=>{
    const id='33333333-3333-4333-8333-333333333333';vi.spyOn(crypto,'randomUUID').mockReturnValue(id);
    const fetcher=vi.spyOn(globalThis,'fetch').mockResolvedValue(new Response(JSON.stringify({data:{acknowledged:true,status:'processed',transactionId:'different'}}),{status:201}));const {fixture,c}=setup();
    c.apiOrigin='http://remote.example.test';await c.submit();expect(c.status()).toBe('invalid');expect(fetcher).not.toHaveBeenCalled();
    c.apiOrigin='https://api.example.test';c.payload='[]';await c.submit();expect(c.status()).toBe('invalid');expect(fetcher).not.toHaveBeenCalled();
    c.payload='{}';await c.submit();expect(c.status()).toBe('unknown');expect(c.transaction()).toBe(id);fixture.destroy();
  });
  it('marks a timed out request unknown and keeps its transaction ID',async()=>{
    vi.useFakeTimers();const id='44444444-4444-4444-8444-444444444444';vi.spyOn(crypto,'randomUUID').mockReturnValue(id);
    vi.spyOn(globalThis,'fetch').mockImplementation((_url,options)=>new Promise((_resolve,reject)=>options!.signal!.addEventListener('abort',()=>reject(new Error('aborted')))));
    const {fixture,c}=setup();const pending=c.submit();expect(c.status()).toBe('sending');await vi.advanceTimersByTimeAsync(30000);await pending;expect(c.status()).toBe('unknown');expect(c.transaction()).toBe(id);fixture.destroy();
  });
});
