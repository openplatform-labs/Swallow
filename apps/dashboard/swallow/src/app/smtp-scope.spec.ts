import { TestBed } from '@angular/core/testing';
import { SimpleChange } from '@angular/core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { SmtpScope } from './smtp-scope';
const id='11111111-1111-4111-8111-111111111111';
function component(){const fixture=TestBed.createComponent(SmtpScope);const c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';c.target='worker';return {fixture,c};}
describe('SMTP scope management',()=>{
  afterEach(()=>vi.restoreAllMocks());
  it('reads, prepares, submits and looks up an authorized change',async()=>{
    vi.spyOn(crypto,'randomUUID').mockReturnValue(id);
    const result={previous:'absent',state:'enabled',changed:true};
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({available:true,state:'absent'}))).mockResolvedValueOnce(new Response(JSON.stringify({requestId:id,replayed:false,result}))).mockResolvedValueOnce(new Response(JSON.stringify({found:true,requestId:id,target:'worker',expected:'absent',desired:true,reason:'ticket',result,createdAt:'today'})));
    const {fixture,c}=component();await c.load();expect(c.state()).toBe('absent');c.reason='ticket';c.confirmed=true;c.prepare();expect(c.changeStatus()).toBe('prepared');await c.submit();expect(c.changeStatus()).toBe('recorded');expect(JSON.parse(mock.mock.calls[1][1]?.body as string)).toEqual({requestId:id,targetLogin:'worker',expected:'absent',desired:true,reason:'ticket'});await c.lookup();expect(c.lookupStatus()).toBe('found');fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('SMTP 워커 환경 격리');fixture.destroy();
  });
  it('retains uncertain request and requires original credentials to retry',async()=>{
    vi.spyOn(crypto,'randomUUID').mockReturnValue(id);const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({available:true,state:'enabled'}))).mockRejectedValueOnce(new Error('lost')).mockResolvedValueOnce(new Response(JSON.stringify({requestId:id,replayed:true,result:{previous:'enabled',state:'disabled',changed:true}})));
    const {fixture,c}=component();await c.load();c.reason='done';c.confirmed=true;c.prepare();await c.submit();expect(c.changeStatus()).toBe('unknown');c.reset();expect(c.snapshot()).not.toBeNull();c.retryConfirmed=true;c.token='new.token.value';await c.submit(true);expect(mock).toHaveBeenCalledTimes(2);c.token='aaa.bbb.ccc';await c.submit(true);expect(c.changeStatus()).toBe('recorded');fixture.destroy();
  });
  it('blocks concurrent changes and keeps an in-flight request when only the busy input changes',async()=>{
    vi.spyOn(crypto,'randomUUID').mockReturnValue(id);let resolve!:(r:Response)=>void;
    const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({available:true,state:'absent'}))).mockImplementationOnce(()=>new Promise(r=>resolve=r));
    const {fixture,c}=component();await c.load();c.reason='ticket';c.confirmed=true;c.prepare();c.blocked=true;await c.submit();expect(mock).toHaveBeenCalledTimes(1);
    c.blocked=false;const pending=c.submit();c.blocked=true;c.ngOnChanges({blocked:new SimpleChange(false,true,false)});expect(c.changeStatus()).toBe('sending');
    resolve(new Response(JSON.stringify({requestId:id,replayed:false,result:{previous:'absent',state:'enabled',changed:true}})));await pending;expect(c.changeStatus()).toBe('recorded');fixture.destroy();
  });
  it('discards lookup responses after the request ID changes',async()=>{
    let resolve!:(r:Response)=>void;vi.spyOn(globalThis,'fetch').mockImplementationOnce(()=>new Promise(r=>resolve=r));
    const {fixture,c}=component();c.lookupId=id;const pending=c.lookup();c.lookupId='22222222-2222-4222-8222-222222222222';c.clearLookup();
    resolve(new Response(JSON.stringify({found:true,requestId:id,target:'worker',expected:'absent',desired:true,reason:'ticket',result:{previous:'absent',state:'enabled',changed:true},createdAt:'today'})));await pending;
    expect(c.lookupStatus()).toBe('idle');expect(c.lookupRecord()).toBeNull();fixture.destroy();
  });
  it('rejects stale reads and invalid audit rows',async()=>{
    let resolve!:(r:Response)=>void;vi.spyOn(globalThis,'fetch').mockImplementationOnce(()=>new Promise(r=>resolve=r)).mockResolvedValueOnce(new Response(JSON.stringify({data:[{id:'invalid'}],hasMore:false,next:null})));
    const {fixture,c}=component();const pending=c.load();c.environment='other';c.ngOnChanges();resolve(new Response(JSON.stringify({available:true,state:'absent'})));await pending;expect(c.state()).toBeNull();await c.audit();expect(c.auditStatus()).toBe('error');fixture.destroy();
  });
});
