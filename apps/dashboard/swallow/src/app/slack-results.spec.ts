import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { SlackResults } from './slack-results';
const id='11111111-1111-4111-8111-000000000001';
const row={jobId:id,jobStatus:'failed',kind:'workflow-delivery',attemptState:'unknown',providerIdKnown:false,hasResolutionHistory:false,failureCategory:'unknown'};
const page=Array.from({length:20},(_,i)=>({...row,jobId:`11111111-1111-4111-8111-${String(i+1).padStart(12,'0')}`}));
const last=page[19].jobId;
describe('Slack operator results',()=>{
  afterEach(()=>vi.restoreAllMocks());
  it('lists and loads a selected status with bearer authorization',async()=>{
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:page.map(r=>({...r,unexpectedSecret:"fixture-sensitive"})),hasMore:true,next:last}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[],hasMore:false,next:null}))).mockResolvedValueOnce(new Response(JSON.stringify({...row,unexpectedSecret:"fixture-sensitive"})));
    const fixture=TestBed.createComponent(SlackResults);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';await c.list();expect(c.rows()).toEqual(page);await c.list(true);expect(mock.mock.calls[1][0]).toContain(`after=${last}`);c.select(row);await c.load();expect(c.result()).toEqual(row);expect(mock.mock.calls[2][1]).toMatchObject({headers:{Authorization:'Bearer aaa.bbb.ccc'},credentials:'omit',cache:'no-store',redirect:'error'});fixture.destroy();
  });
  it('shows the current attempt ID and rejects inconsistent IDs',async()=>{
    const attempt='22222222-2222-4222-8222-222222222222';
    vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({...row,attemptId:attempt}))).mockResolvedValueOnce(new Response(JSON.stringify({...row,attemptId:null})));
    const fixture=TestBed.createComponent(SlackResults);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';c.jobId=id;
    await c.load();fixture.detectChanges();expect(c.result()?.attemptId).toBe(attempt);expect(fixture.nativeElement.textContent).toContain(attempt);
    await c.load();expect(c.resultStatus()).toBe('error');expect(c.result()).toBeNull();fixture.destroy();
  });
  it('requires confirmation, preserves unknown outcomes and checks the request ID',async()=>{
    const attempt='22222222-2222-4222-8222-222222222222';let fail!:(error:Error)=>void;
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({...row,attemptId:attempt}))).mockImplementationOnce(()=>new Promise((_,reject)=>{fail=reject;}));
    const fixture=TestBed.createComponent(SlackResults);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';c.jobId=id;await c.load();c.decisionReason='ticket-123';c.prepareDecision();
    const draft=c.decisionSnapshot()!;expect(draft.attemptId).toBe(attempt);expect(c.decisionStatus()).toBe('prepared');await c.submitDecision();expect(mock).toHaveBeenCalledTimes(1);
    c.decisionConfirmed=true;const busy=vi.fn();c.busyChange.subscribe(busy);const pending=c.submitDecision();expect(c.decisionStatus()).toBe('sending');await c.submitDecision();expect(mock).toHaveBeenCalledTimes(2);
    expect(JSON.parse(mock.mock.calls[1][1]!.body as string)).toEqual(draft);fail(new Error('lost response'));await pending;expect(c.decisionStatus()).toBe('unknown');expect(busy.mock.calls).toEqual([[true],[false]]);c.resetDecision();expect(c.decisionStatus()).toBe('unknown');
    mock.mockResolvedValueOnce(new Response(JSON.stringify({found:false}))).mockResolvedValueOnce(new Response(JSON.stringify({found:true,...draft,createdAt:'2026-01-01'})));
    await c.lookupDecision();expect(c.lookupStatus()).toBe('not-found');expect(c.decisionStatus()).toBe('unknown');await c.lookupDecision();expect(c.lookupStatus()).toBe('found');expect(c.decisionStatus()).toBe('recorded');expect(c.result()).toBeNull();fixture.destroy();
  });
  it('does not treat a lookup under changed credentials as proof of the earlier request',async()=>{
    const attempt='22222222-2222-4222-8222-222222222222';let finish!:(r:Response)=>void;
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({...row,attemptId:attempt}))).mockReturnValueOnce(new Promise(done=>{finish=done;}));
    const fixture=TestBed.createComponent(SlackResults);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';c.jobId=id;await c.load();c.decisionReason='ticket';c.prepareDecision();c.decisionConfirmed=true;const draft=c.decisionSnapshot()!;
    const pending=c.submitDecision();c.token='ddd.eee.fff';c.ngOnChanges();finish(new Response(JSON.stringify({recorded:true,replayed:false})));await pending;expect(c.decisionStatus()).toBe('unknown');
    mock.mockResolvedValueOnce(new Response(JSON.stringify({found:true,...draft,createdAt:'2026-01-01'})));await c.lookupDecision();expect(c.lookupStatus()).toBe('found');expect(c.decisionStatus()).toBe('unknown');expect(c.lookupRecord()?.jobId).toBe(id);fixture.destroy();
  });
  it('shows Slack 429 guidance and rejects inconsistent or invalid response data',async()=>{
    const limited={...row,attemptState:'rejected',attemptHttpStatus:429,retryAfterSeconds:120,retryNotBefore:'2026-01-01 00:02:00+00'};
    vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify(limited))).mockResolvedValueOnce(new Response(JSON.stringify({...limited,retryAfterSeconds:86401}))).mockResolvedValueOnce(new Response(JSON.stringify({...limited,retryNotBefore:null})));
    const fixture=TestBed.createComponent(SlackResults),c=fixture.componentInstance;c.apiOrigin='http://127.0.0.1:3000';c.environment='env';c.token='aaa.bbb.ccc';c.jobId=id;
    await c.load();fixture.detectChanges();expect(c.result()?.retryAfterSeconds).toBe(120);expect(fixture.nativeElement.textContent).toContain('120초');expect(fixture.nativeElement.textContent).toContain('대기를 강제하지 않습니다');
    await c.load();expect(c.resultStatus()).toBe('error');expect(c.result()).toBeNull();await c.load();expect(c.resultStatus()).toBe('error');fixture.destroy();
  });
  it('schedules a Slack 429 after guidance and safely replays a lost response',async()=>{
    const attempt='22222222-2222-4222-8222-222222222222';const limited={...row,attemptState:'rejected',attemptId:attempt,attemptHttpStatus:429,retryAfterSeconds:120,retryNotBefore:'2026-01-01 00:02:00+00'};
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify(limited))).mockRejectedValueOnce(new Error('lost')).mockResolvedValueOnce(new Response(JSON.stringify({found:false}))).mockResolvedValueOnce(new Response(JSON.stringify({recorded:true,replayed:true,availableAt:'2026-01-01 00:02:00+00'})));
    const fixture=TestBed.createComponent(SlackResults),c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';c.jobId=id;await c.load();c.decisionChoice='retry-after';c.decisionReason='reviewed';c.prepareDecision();const draft=c.decisionSnapshot()!;expect(draft.decision).toBe('retry-after');
    c.decisionConfirmed=true;await c.submitDecision();expect(c.decisionStatus()).toBe('unknown');expect(new URL(mock.mock.calls[1][0] as string).pathname).toBe('/v1/experimental/operator/slack-retry-after');await c.lookupDecision();expect(c.lookupStatus()).toBe('not-found');c.environment='other';c.ngOnChanges();c.retryConfirmed=true;await c.submitDecision(true);expect(c.decisionStatus()).toBe('unknown');c.environment='env';c.ngOnChanges();c.retryConfirmed=true;await c.submitDecision(true);expect(c.decisionStatus()).toBe('recorded');expect(c.decisionAvailableAt()).toBe('2026-01-01 00:02:00+00');expect(mock.mock.calls[3][1]!.body).toBe(mock.mock.calls[1][1]!.body);fixture.destroy();
  });
  it('requires valid guidance and rejects replay after context changes',async()=>{
    const attempt='22222222-2222-4222-8222-222222222222';const limited={...row,attemptState:'rejected',attemptId:attempt,attemptHttpStatus:429,retryAfterSeconds:null,retryNotBefore:null};
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify(limited))).mockResolvedValueOnce(new Response(JSON.stringify({...limited,retryAfterSeconds:120,retryNotBefore:'today'}))).mockRejectedValueOnce(new Error('lost'));
    const fixture=TestBed.createComponent(SlackResults),c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';c.jobId=id;await c.load();c.decisionChoice='retry-after';c.decisionReason='reviewed';c.prepareDecision();expect(c.decisionStatus()).toBe('idle');await c.load();c.prepareDecision();c.decisionConfirmed=true;await c.submitDecision();expect(c.decisionStatus()).toBe('unknown');c.environment='other';c.retryConfirmed=true;await c.submitDecision(true);expect(c.decisionStatus()).toBe('unknown');expect(c.decisionMessage()).toContain('같은 토큰');expect(mock).toHaveBeenCalledTimes(3);fixture.destroy();
  });
  it('looks up a delayed decision and validates its original schedule',async()=>{
    const attempt='22222222-2222-4222-8222-222222222222';const limited={...row,attemptState:'rejected',attemptId:attempt,attemptHttpStatus:429,retryAfterSeconds:120,retryNotBefore:'today'};
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify(limited))).mockRejectedValueOnce(new Error('lost'));
    const fixture=TestBed.createComponent(SlackResults),c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';c.jobId=id;await c.load();c.decisionChoice='retry-after';c.decisionReason='ticket';c.prepareDecision();const draft=c.decisionSnapshot()!;c.decisionConfirmed=true;await c.submitDecision();
    mock.mockResolvedValueOnce(new Response(JSON.stringify({found:true,...draft,availableAt:null,createdAt:'today'}))).mockResolvedValueOnce(new Response(JSON.stringify({found:true,...draft,availableAt:'tomorrow',createdAt:'today'})));await c.lookupDecision();expect(c.lookupStatus()).toBe('error');expect(c.decisionStatus()).toBe('unknown');await c.lookupDecision();expect(c.lookupStatus()).toBe('found');expect(c.decisionStatus()).toBe('recorded');expect(c.decisionAvailableAt()).toBe('tomorrow');fixture.destroy();
  });
  it('rejects an acceptance decision for a rejected attempt and records successful retry',async()=>{
    const attempt='22222222-2222-4222-8222-222222222222';const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({...row,attemptState:'rejected',attemptId:attempt}))).mockResolvedValueOnce(new Response(JSON.stringify({recorded:true,replayed:false})));
    const fixture=TestBed.createComponent(SlackResults);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';c.jobId=id;await c.load();c.decisionReason='checked';c.decisionChoice='accepted';c.prepareDecision();expect(c.decisionStatus()).toBe('idle');c.decisionChoice='retry';c.prepareDecision();c.decisionConfirmed=true;await c.submitDecision();expect(c.decisionStatus()).toBe('recorded');expect(c.result()).toBeNull();expect(mock).toHaveBeenCalledTimes(2);fixture.destroy();
  });
  it('rejects unsupported response values and clears results on denial',async()=>{
    vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({...row,failureCategory:'raw private error'}))).mockResolvedValueOnce(new Response('',{status:403}));const fixture=TestBed.createComponent(SlackResults);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';c.jobId=id;await c.load();expect(c.resultStatus()).toBe('error');expect(c.result()).toBeNull();await c.list();expect(c.listStatus()).toBe('error');expect(c.rows()).toEqual([]);
    fixture.destroy();
  });
  it('rejects malformed cursors and unsorted result pages',async()=>{
    vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[row],hasMore:true,next:id}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[page[1],page[0]],hasMore:false,next:null}))).mockResolvedValueOnce(new Response(JSON.stringify({data:page,hasMore:true,next:page[18].jobId}))).mockResolvedValueOnce(new Response(JSON.stringify({data:page,hasMore:true,next:last}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[row],hasMore:false,next:null})));
    const fixture=TestBed.createComponent(SlackResults);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';
    await c.list();expect(c.listStatus()).toBe('error');expect(c.rows()).toEqual([]);
    await c.list();expect(c.listStatus()).toBe('error');expect(c.rows()).toEqual([]);
    await c.list();expect(c.listStatus()).toBe('error');await c.list();expect(c.listStatus()).toBe('loaded');await c.list(true);expect(c.listStatus()).toBe('error');expect(c.rows()).toEqual([]);fixture.destroy();
  });
  it('rejects unsafe origins and ignores delayed results after credentials change',async()=>{
    let resolve!:(r:Response)=>void;const mock=vi.spyOn(globalThis,'fetch').mockImplementation(()=>new Promise(r=>resolve=r));const fixture=TestBed.createComponent(SlackResults);const c=fixture.componentInstance;c.apiOrigin='http://remote.example.test';c.environment='env';c.token='aaa.bbb.ccc';await c.list();expect(mock).not.toHaveBeenCalled();c.apiOrigin='https://api.example.test';const pending=c.list();c.token='ddd.eee.fff';c.ngOnChanges();resolve(new Response(JSON.stringify({data:[row],hasMore:false,next:null})));await pending;expect(c.rows()).toEqual([]);expect(c.listStatus()).toBe('idle');fixture.destroy();expect(c.token).toBe('');
  });
});
