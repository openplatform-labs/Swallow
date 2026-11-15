import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { FcmResults } from './fcm-results';
const id='11111111-1111-4111-8111-000000000001';
const row={jobId:id,jobStatus:'failed',attemptCount:3,acceptedCount:1,rejectedCount:1,unknownCount:1,dispatchingCount:0,skippedCount:0,operatorAcceptedCount:1,operatorNotAcceptedCount:1};
const page=Array.from({length:20},(_,i)=>({...row,jobId:`11111111-1111-4111-8111-${String(i+1).padStart(12,'0')}`}));
const last=page[19].jobId;
describe('FCM operator results',()=>{
  afterEach(()=>vi.restoreAllMocks());
  it('lists and loads attempt counts with bearer authorization',async()=>{
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:page.map(r=>({...r,unexpectedSecret:"fixture-sensitive"})),hasMore:true,next:last}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[],hasMore:false,next:null}))).mockResolvedValueOnce(new Response(JSON.stringify({...row,unexpectedSecret:"fixture-sensitive"})));
    const fixture=TestBed.createComponent(FcmResults);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';await c.list();expect(c.rows()).toEqual(page);await c.list(true);expect(mock.mock.calls[1][0]).toContain(`after=${last}`);c.select(row);await c.load();expect(c.result()).toEqual(row);expect(mock.mock.calls[2][1]).toMatchObject({headers:{Authorization:'Bearer aaa.bbb.ccc'},credentials:'omit',cache:'no-store',redirect:'error'});fixture.destroy();
  });
  it('shows bounded rate-limit guidance and rejects inconsistent advice',async()=>{
    vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({...row,rateLimitedCount:1,rateLimitAdviceCount:1,rateLimitNotBefore:'2026-11-15T12:00:00Z'}))).mockResolvedValueOnce(new Response(JSON.stringify({...row,rateLimitedCount:1,rateLimitAdviceCount:2,rateLimitNotBefore:'2026-11-15T12:00:00Z'})));
    const fixture=TestBed.createComponent(FcmResults);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';c.jobId=id;await c.load();fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('FCM 429 제한: 1');await c.load();expect(c.resultStatus()).toBe('error');fixture.destroy();
  });
  it('rejects inconsistent counts and clears results on denial',async()=>{
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({...row,acceptedCount:9}))).mockResolvedValueOnce(new Response('',{status:403}));const fixture=TestBed.createComponent(FcmResults);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';c.jobId=id;await c.load();expect(c.resultStatus()).toBe('error');expect(c.result()).toBeNull();await c.list();expect(c.listStatus()).toBe('error');expect(c.rows()).toEqual([]);expect(mock).toHaveBeenCalledTimes(2);fixture.destroy();
  });
  it('rejects malformed cursors and unsorted result pages',async()=>{
    vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[row],hasMore:true,next:id}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[page[1],page[0]],hasMore:false,next:null}))).mockResolvedValueOnce(new Response(JSON.stringify({data:page,hasMore:true,next:page[18].jobId}))).mockResolvedValueOnce(new Response(JSON.stringify({data:page,hasMore:true,next:last}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[row],hasMore:false,next:null})));
    const fixture=TestBed.createComponent(FcmResults);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';
    await c.list();expect(c.listStatus()).toBe('error');expect(c.rows()).toEqual([]);
    await c.list();expect(c.listStatus()).toBe('error');expect(c.rows()).toEqual([]);
    await c.list();expect(c.listStatus()).toBe('error');await c.list();expect(c.listStatus()).toBe('loaded');await c.list(true);expect(c.listStatus()).toBe('error');expect(c.rows()).toEqual([]);fixture.destroy();
  });
  it('rejects unsafe origins and ignores late responses after identity changes',async()=>{
    let resolve!:(r:Response)=>void;const mock=vi.spyOn(globalThis,'fetch').mockImplementation(()=>new Promise(r=>resolve=r));const fixture=TestBed.createComponent(FcmResults);const c=fixture.componentInstance;c.apiOrigin='http://remote.example.test';c.environment='env';c.token='aaa.bbb.ccc';await c.list();expect(mock).not.toHaveBeenCalled();c.apiOrigin='https://api.example.test';const pending=c.list();c.environment='other';c.ngOnChanges();resolve(new Response(JSON.stringify({data:[row],hasMore:false,next:null})));await pending;expect(c.rows()).toEqual([]);expect(c.listStatus()).toBe('idle');fixture.destroy();expect(c.token).toBe('');
  });
});
