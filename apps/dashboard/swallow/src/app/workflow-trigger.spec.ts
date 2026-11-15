import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { WorkflowTrigger } from './workflow-trigger';

describe('workflow trigger',()=>{
  afterEach(()=>{vi.useRealTimers();vi.restoreAllMocks();sessionStorage.clear();});
  function setup(){const fixture=TestBed.createComponent(WorkflowTrigger);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.apiKey='test-key';c.workflow='welcome';c.subscriber='person';return {fixture,c};}
  it('sends a sandbox override and displays the trigger acknowledgement',async()=>{
    const id='11111111-1111-4111-8111-111111111111';vi.spyOn(crypto,'randomUUID').mockReturnValue(id);
    const fetcher=vi.spyOn(globalThis,'fetch').mockResolvedValue(new Response(JSON.stringify({data:{acknowledged:true,status:'processed',transactionId:id}}),{status:201}));
    const {fixture,c}=setup();c.payload='{"name":"Lee"}';await c.submit();fixture.detectChanges();
    const [url,options]=fetcher.mock.calls[0];expect(url).toBe('https://api.example.test/v1/events/trigger');expect(options).toMatchObject({method:'POST',headers:{Authorization:'ApiKey test-key','Content-Type':'application/json'},credentials:'omit',redirect:'error',cache:'no-store'});
    expect(JSON.parse(options!.body as string)).toEqual({name:'welcome',to:'person',payload:{name:'Lee'},transactionId:id,overrides:{providers:{sendgrid:{mailSettings:{sandboxMode:{enable:true}}}}}});
    expect(c.status()).toBe('accepted');expect(fixture.nativeElement.textContent).toContain('다른 이메일 공급자는 실제로 실행될 수 있습니다');expect(fixture.nativeElement.textContent).toContain('새 구독자로 생성될 수 있으며');await c.submit();expect(fetcher).toHaveBeenCalledTimes(1);fixture.destroy();
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
  it('optionally restores only transaction ID and origin as unknown after reopening',async()=>{
    const id='55555555-5555-4555-8555-555555555555';vi.spyOn(crypto,'randomUUID').mockReturnValue(id);
    const fetcher=vi.spyOn(globalThis,'fetch').mockRejectedValue(new Error('offline'));const first=setup();first.fixture.detectChanges();first.c.rememberTransaction=true;first.c.payload='{"private":"secret"}';await first.c.submit();
    expect(JSON.parse(sessionStorage.getItem('swallow.workflow-trigger.transaction.v1')!)).toEqual({version:1,transactionId:id,origin:'https://api.example.test'});first.fixture.destroy();
    const second=setup();second.fixture.detectChanges();expect(second.c.status()).toBe('unknown');expect(second.c.transaction()).toBe(id);expect(second.c.savedOrigin()).toBe('https://api.example.test');await second.c.submit();expect(fetcher).toHaveBeenCalledTimes(1);
    second.c.reset();expect(second.c.status()).toBe('idle');expect(sessionStorage.getItem('swallow.workflow-trigger.transaction.v1')).toBeNull();second.fixture.destroy();
  });
  it('does not submit when requested storage is unavailable or contains a prior record',async()=>{
    const fetcher=vi.spyOn(globalThis,'fetch');const {fixture,c}=setup();fixture.detectChanges();c.rememberTransaction=true;
    const setter=vi.spyOn(Storage.prototype,'setItem').mockImplementation(()=>{throw Error('denied')});await c.submit();expect(c.status()).toBe('invalid');expect(c.storageError()).toBe(true);expect(fetcher).not.toHaveBeenCalled();setter.mockRestore();
    sessionStorage.setItem('swallow.workflow-trigger.transaction.v1','existing');await c.submit();expect(c.status()).toBe('invalid');expect(sessionStorage.getItem('swallow.workflow-trigger.transaction.v1')).toBe('existing');expect(fetcher).not.toHaveBeenCalled();fixture.destroy();
  });
  it('blocks on malformed stored records but allows clearing them',()=>{
    sessionStorage.setItem('swallow.workflow-trigger.transaction.v1','invalid');const {fixture,c}=setup();fixture.detectChanges();expect(c.status()).toBe('unknown');expect(c.storageError()).toBe(true);c.reset();expect(c.status()).toBe('idle');expect(sessionStorage.getItem('swallow.workflow-trigger.transaction.v1')).toBeNull();fixture.destroy();
  });
  it('allows memory-only use when session storage cannot be read',async()=>{
    const getter=vi.spyOn(Storage.prototype,'getItem').mockImplementation(()=>{throw Error('denied')});vi.spyOn(globalThis,'fetch').mockRejectedValue(new Error('offline'));const {fixture,c}=setup();fixture.detectChanges();expect(c.status()).toBe('idle');expect(c.storageError()).toBe(true);await c.submit();expect(c.status()).toBe('unknown');c.reset();expect(c.status()).toBe('idle');getter.mockRestore();fixture.destroy();
  });
  it('looks up the current transaction with its original API origin and environment key',async()=>{
    const id='66666666-6666-4666-8666-666666666666';vi.spyOn(crypto,'randomUUID').mockReturnValue(id);
    const fetcher=vi.spyOn(globalThis,'fetch').mockRejectedValueOnce(new Error('offline')).mockResolvedValueOnce(new Response(JSON.stringify({data:{transactionId:id,recorded:true,jobCount:1,job:{jobId:id,jobStatus:'completed'}}})));
    const {fixture,c}=setup();await c.submit();await c.lookup();expect(fetcher.mock.calls[1][0]).toBe(`https://api.example.test/v1/experimental/workflow-trigger-result?transactionId=${id}`);expect(fetcher.mock.calls[1][1]).toMatchObject({headers:{Authorization:'ApiKey test-key'},credentials:'omit',redirect:'error'});expect(c.lookupResult()).toEqual({recorded:true,jobCount:1,job:{jobId:id,jobStatus:'completed'}});fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('부모 작업 상태는 개별 채널 전달 결과가 아닙니다');
    c.apiOrigin='https://other.example.test';await c.lookup();expect(c.lookupStatus()).toBe('error');expect(fetcher).toHaveBeenCalledTimes(2);fixture.destroy();
  });
  it('rejects inconsistent or stale transaction lookup results',async()=>{
    const id='77777777-7777-4777-8777-777777777777';vi.spyOn(crypto,'randomUUID').mockReturnValue(id);
    let finish!:(response:Response)=>void;const fetcher=vi.spyOn(globalThis,'fetch').mockRejectedValueOnce(new Error('offline')).mockResolvedValueOnce(new Response(JSON.stringify({data:{transactionId:id,recorded:false,jobCount:0,job:{jobId:id,jobStatus:'completed'}}}))).mockImplementationOnce(()=>new Promise(resolve=>finish=resolve));
    const {fixture,c}=setup();await c.submit();await c.lookup();expect(c.lookupStatus()).toBe('error');expect(c.lookupResult()).toBeNull();const pending=c.lookup();c.apiKey='other-key';c.clearLookup();finish(new Response(JSON.stringify({data:{transactionId:id,recorded:true,jobCount:1,job:{jobId:id,jobStatus:'completed'}}})));await pending;expect(c.lookupStatus()).toBe('idle');expect(c.lookupResult()).toBeNull();expect(fetcher).toHaveBeenCalledTimes(3);fixture.destroy();
  });
  it('uses an independent operator JWT and environment for lookup',async()=>{
    const id='88888888-8888-4888-8888-888888888888';vi.spyOn(crypto,'randomUUID').mockReturnValue(id);
    const fetcher=vi.spyOn(globalThis,'fetch').mockRejectedValueOnce(new Error('offline')).mockResolvedValueOnce(new Response(JSON.stringify({transactionId:id,recorded:true,jobCount:0,job:null})));
    const {fixture,c}=setup();await c.submit();c.lookupAuth='jwt';c.lookupToken='aaa.bbb.ccc';c.lookupEnvironment='env-a';await c.lookup();
    expect(fetcher.mock.calls[1][0]).toBe(`https://api.example.test/v1/experimental/operator/workflow-trigger-result?transactionId=${id}&environmentId=env-a`);expect(fetcher.mock.calls[1][1]).toMatchObject({headers:{Authorization:'Bearer aaa.bbb.ccc'}});expect(c.lookupResult()).toEqual({recorded:true,jobCount:0,job:null});fixture.destroy();expect(c.lookupToken).toBe('');
  });
});
