import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ExecutionAudit } from './execution-audit';

describe('operator execution audit',()=>{
  afterEach(()=>{vi.restoreAllMocks();vi.useRealTimers();});
  function setup(){const fixture=TestBed.createComponent(ExecutionAudit);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env-a';c.token='aaa.bbb.ccc';return {fixture,c};}
  const id='11111111-1111-4111-8111-111111111111';
  const row={id,workflowIdentifier:'welcome',transactionId:'txn',issuer:'issuer',subject:'person',clientId:'client',databasePrincipal:'api',recordedAt:'2026-01-01'};
  it('reads and pages a scoped audit with explicit credentials',async()=>{
    const fetcher=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[row],hasMore:true,next:id}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[],hasMore:false,next:null}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[row],hasMore:true,next:id})));
    const {fixture,c}=setup();await c.load();expect(fetcher.mock.calls[0][0]).toBe('https://api.example.test/v1/experimental/operator/execution-audit?environmentId=env-a');expect(fetcher.mock.calls[0][1]).toMatchObject({headers:{Authorization:'Bearer aaa.bbb.ccc'},credentials:'omit',redirect:'error',cache:'no-store'});expect(c.rows()).toEqual([row]);fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('welcome');
    await c.load('next');expect(fetcher.mock.calls[1][0]).toContain(`after=${id}`);expect(c.page()).toBe(2);await c.load('previous');expect(c.page()).toBe(1);fixture.destroy();expect(c.token).toBe('');
  });
  it('distinguishes denied, invalid responses and unsafe origins',async()=>{
    const fetcher=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response('',{status:401})).mockResolvedValueOnce(new Response(JSON.stringify({data:[{id}],hasMore:false,next:null})));
    const {fixture,c}=setup();await c.load();expect(c.status()).toBe('denied');await c.load();expect(c.status()).toBe('error');c.apiOrigin='http://remote.example';await c.load();expect(c.status()).toBe('error');expect(fetcher).toHaveBeenCalledTimes(2);fixture.destroy();
  });
  it('discards in-flight results after credentials change',async()=>{
    let resolve!:(r:Response)=>void;vi.spyOn(globalThis,'fetch').mockImplementation(()=>new Promise(r=>resolve=r));const {fixture,c}=setup();const pending=c.load();c.environment='env-b';c.clear();resolve(new Response(JSON.stringify({data:[row],hasMore:false,next:null})));await pending;expect(c.status()).toBe('idle');expect(c.rows()).toEqual([]);fixture.destroy();
  });
  it('times out stalled requests',async()=>{
    vi.useFakeTimers();vi.spyOn(globalThis,'fetch').mockImplementation((_url,options)=>new Promise((_resolve,reject)=>options!.signal!.addEventListener('abort',()=>reject(new Error('aborted')))));const {fixture,c}=setup();const pending=c.load();await vi.advanceTimersByTimeAsync(30000);await pending;expect(c.status()).toBe('error');fixture.destroy();
  });
});
