import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { TwilioResults } from './twilio-results';
const id='11111111-1111-4111-8111-000000000001';
const row={jobId:id,jobStatus:'completed',result:'delivered',attemptState:'accepted',providerIdKnown:true,providerIdAmbiguous:false,lastCompletedScanAt:null,attemptHttpStatus:null,retryAfterSeconds:null,retryNotBefore:null};
const page=Array.from({length:20},(_,i)=>({...row,jobId:`11111111-1111-4111-8111-${String(i+1).padStart(12,'0')}`}));
const last=page[19].jobId;
describe('Twilio operator results',()=>{
  afterEach(()=>vi.restoreAllMocks());
  it('loads paginated jobs and a selected result with bearer authentication',async()=>{
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:page.map(r=>({...r,unexpectedSecret:"fixture-sensitive"})),hasMore:true,next:last}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[],hasMore:false,next:null}))).mockResolvedValueOnce(new Response(JSON.stringify({...row,unexpectedSecret:"fixture-sensitive"})));
    const fixture=TestBed.createComponent(TwilioResults);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';await c.list();expect(c.rows()).toEqual(page);await c.list(true);expect(mock.mock.calls[1][0]).toContain(`after=${last}`);c.select(row);await c.load();expect(c.result()).toEqual(row);expect(mock.mock.calls[2][1]).toMatchObject({headers:{Authorization:'Bearer aaa.bbb.ccc'},credentials:'omit',cache:'no-store',redirect:'error'});fixture.destroy();
  });
  it('rejects contradictory evidence and clears results after denial',async()=>{
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({...row,providerIdAmbiguous:true}))).mockResolvedValueOnce(new Response('',{status:403}));const fixture=TestBed.createComponent(TwilioResults);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';c.jobId=id;await c.load();expect(c.resultStatus()).toBe('error');expect(c.result()).toBeNull();await c.list();expect(c.listStatus()).toBe('error');expect(c.rows()).toEqual([]);expect(mock).toHaveBeenCalledTimes(2);fixture.destroy();
  });
  it('rejects malformed cursors and unsorted result pages',async()=>{
    vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[row],hasMore:true,next:id}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[page[1],page[0]],hasMore:false,next:null}))).mockResolvedValueOnce(new Response(JSON.stringify({data:page,hasMore:true,next:page[18].jobId}))).mockResolvedValueOnce(new Response(JSON.stringify({data:page,hasMore:true,next:last}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[row],hasMore:false,next:null})));
    const fixture=TestBed.createComponent(TwilioResults);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';
    await c.list();expect(c.listStatus()).toBe('error');expect(c.rows()).toEqual([]);
    await c.list();expect(c.listStatus()).toBe('error');expect(c.rows()).toEqual([]);
    await c.list();expect(c.listStatus()).toBe('error');await c.list();expect(c.listStatus()).toBe('loaded');await c.list(true);expect(c.listStatus()).toBe('error');expect(c.rows()).toEqual([]);fixture.destroy();
  });
  it('rejects unsafe origin and ignores delayed data after identity changes',async()=>{
    let resolve!:(r:Response)=>void;const mock=vi.spyOn(globalThis,'fetch').mockImplementation(()=>new Promise(r=>resolve=r));const fixture=TestBed.createComponent(TwilioResults);const c=fixture.componentInstance;c.apiOrigin='http://remote.example.test';c.environment='env';c.token='aaa.bbb.ccc';await c.list();expect(mock).not.toHaveBeenCalled();c.apiOrigin='https://api.example.test';const pending=c.list();c.token='ddd.eee.fff';c.ngOnChanges();resolve(new Response(JSON.stringify({data:[row],hasMore:false,next:null})));await pending;expect(c.rows()).toEqual([]);expect(c.listStatus()).toBe('idle');fixture.destroy();expect(c.token).toBe('');
  });
  it('shows bounded 429 guidance and rejects inconsistent responses',async()=>{vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({...row,jobStatus:'failed',result:'unknown',attemptState:'rejected',attemptHttpStatus:429,retryAfterSeconds:120,retryNotBefore:'2026-11-15 12:00:00+00'}))).mockResolvedValueOnce(new Response(JSON.stringify({...row,retryAfterSeconds:120,retryNotBefore:null})));const fixture=TestBed.createComponent(TwilioResults);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';c.jobId=id;await c.load();fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('120초');await c.load();expect(c.resultStatus()).toBe('error');fixture.destroy();});
});
