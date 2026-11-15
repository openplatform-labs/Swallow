import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { DiscordReviews } from './discord-reviews';
const job='11111111-1111-4111-8111-111111111111';
const revision='22222222-2222-4222-8222-222222222222';
const requestId='33333333-3333-4333-8333-333333333333';
const row={jobId:job,kind:'experimental_discord_workflow',status:'failed',attemptState:'unknown',attemptRevision:revision,failureRevision:'7',attempts:1,httpStatus:null,retryAfterSeconds:null,retryNotBefore:null};
function component(){const fixture=TestBed.createComponent(DiscordReviews);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';return {fixture,c};}
describe('Discord operator reviews',()=>{
  afterEach(()=>vi.restoreAllMocks());
  it('lists, confirms and looks up a revision-bound review',async()=>{
    vi.spyOn(crypto,'randomUUID').mockReturnValue(requestId);
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{...row,secret:'discard'}],hasMore:false,next:null}))).mockResolvedValueOnce(new Response(JSON.stringify({recorded:true,replayed:false}))).mockResolvedValueOnce(new Response(JSON.stringify({found:true,requestId,jobId:job,attemptRevision:revision,failureRevision:'7',reason:'checked',recorded:true,createdAt:'2026-11-15T00:00:00Z'})));
    const {fixture,c}=component();await c.list();expect(c.rows()).toEqual([row]);fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('Discord 발송 실패 검토');c.reason='checked';c.prepare(c.rows()[0]);await c.submit();expect(mock).toHaveBeenCalledTimes(1);c.confirmed=true;await c.submit();expect(c.reviewStatus()).toBe('recorded');expect(JSON.parse(mock.mock.calls[1][1]?.body as string)).toEqual({requestId,jobId:job,attemptRevision:revision,failureRevision:'7',reason:'checked'});await c.lookup();expect(c.lookupStatus()).toBe('found');expect(mock.mock.calls[1][1]).toMatchObject({credentials:'omit',cache:'no-store',redirect:'error'});fixture.destroy();
  });
  it('keeps an uncertain request and only retries with original identity',async()=>{
    vi.spyOn(crypto,'randomUUID').mockReturnValue(requestId);
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[row],hasMore:false,next:null}))).mockRejectedValueOnce(new Error('lost')).mockResolvedValueOnce(new Response(JSON.stringify({recorded:true,replayed:true})));
    const {fixture,c}=component();await c.list();c.reason='checked';c.prepare(c.rows()[0]);c.confirmed=true;await c.submit();expect(c.reviewStatus()).toBe('unknown');c.resetReview();expect(c.review()).not.toBeNull();c.retryConfirmed=true;c.token='other.token.value';await c.submit(true);expect(mock).toHaveBeenCalledTimes(2);c.token='aaa.bbb.ccc';await c.submit(true);expect(c.reviewStatus()).toBe('recorded');expect(c.recorded()).toBe(true);fixture.destroy();
  });
  it('rejects malformed pages and ignores stale responses after environment changes',async()=>{
    let resolve!:(r:Response)=>void;const mock=vi.spyOn(globalThis,'fetch').mockImplementationOnce(()=>new Promise(r=>resolve=r)).mockResolvedValueOnce(new Response(JSON.stringify({data:[{...row,failureRevision:'9223372036854775808'}],hasMore:false,next:null})));
    const {fixture,c}=component();const pending=c.list();c.environment='other';c.ngOnChanges();resolve(new Response(JSON.stringify({data:[row],hasMore:false,next:null})));await pending;expect(c.rows()).toEqual([]);await c.list();expect(c.listStatus()).toBe('error');expect(mock).toHaveBeenCalledTimes(2);fixture.destroy();
  });
  it('does not prepare an active attempt and supports pre-dispatch failure',async()=>{
    const running={...row,status:'running',attemptState:'dispatching'};const pre={...row,jobId:'44444444-4444-4444-8444-444444444444',attemptState:null,attemptRevision:null};vi.spyOn(crypto,'randomUUID').mockReturnValue(requestId);vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[running,pre],hasMore:false,next:null})));
    const {fixture,c}=component();await c.list();c.reason='checked';c.prepare(c.rows()[0]);expect(c.review()).toBeNull();c.prepare(c.rows()[1]);expect(c.review()?.attemptRevision).toBeNull();fixture.destroy();
  });
  it('shows rate-limit guidance and resolves a lost retry request',async()=>{
    const limited={...row,attemptState:'rejected',httpStatus:429,retryAfterSeconds:60,retryNotBefore:'2026-11-15T00:01:00Z'};vi.spyOn(crypto,'randomUUID').mockReturnValue(requestId);
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[limited],hasMore:false,next:null}))).mockRejectedValueOnce(new Error('lost')).mockResolvedValueOnce(new Response(JSON.stringify({found:true,requestId,jobId:job,attemptRevision:revision,failureRevision:'7',decision:'retry',reason:'reviewed',recorded:true,createdAt:'2026-11-15T00:00:00Z'})));
    const {fixture,c}=component();await c.list();fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('60초');expect(fixture.nativeElement.textContent).toContain('즉시 재발송');c.reason='reviewed';c.prepare(c.rows()[0],'accepted');expect(c.review()).toBeNull();c.prepare(c.rows()[0],'retry');c.confirmed=true;await c.submit();expect(c.reviewStatus()).toBe('unknown');expect(mock.mock.calls[1][0]).toContain('/discord-resolution?');await c.lookup();expect(c.reviewStatus()).toBe('recorded');expect(mock.mock.calls[2][0]).toContain('decision=retry');fixture.destroy();
  });
  it('supports retrying pre-dispatch failures without allowing acceptance',async()=>{
    const pre={...row,attemptState:null,attemptRevision:null};vi.spyOn(crypto,'randomUUID').mockReturnValue(requestId);vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[pre],hasMore:false,next:null})));const {fixture,c}=component();await c.list();c.reason='fixed';c.prepare(c.rows()[0],'accepted');expect(c.review()).toBeNull();c.prepare(c.rows()[0],'retry');expect(c.review()?.attemptRevision).toBeNull();fixture.destroy();
  });

  it('offers guided retry only for a matching 429 with guidance',async()=>{
    const limited={...row,attemptState:'rejected',httpStatus:429,retryAfterSeconds:60,retryNotBefore:'2026-11-15T00:01:00Z'};vi.spyOn(crypto,'randomUUID').mockReturnValue(requestId);const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[limited],hasMore:false,next:null}))).mockResolvedValueOnce(new Response(JSON.stringify({recorded:true,replayed:false}))).mockResolvedValueOnce(new Response(JSON.stringify({found:true,requestId,jobId:job,attemptRevision:revision,failureRevision:'7',decision:'retry_after',reason:'wait',recorded:true,createdAt:'2026-11-15T00:00:00Z'})));
    const {fixture,c}=component();await c.list();c.reason='wait';c.prepare({...c.rows()[0],retryAfterSeconds:null},'retry_after');expect(c.review()).toBeNull();c.prepare(c.rows()[0],'retry_after');c.confirmed=true;await c.submit();expect(JSON.parse(mock.mock.calls[1][1]?.body as string).decision).toBe('retry_after');await c.lookup();expect(c.lookupStatus()).toBe('found');expect(mock.mock.calls[2][0]).toContain('decision=retry_after');fixture.destroy();
  });

});
