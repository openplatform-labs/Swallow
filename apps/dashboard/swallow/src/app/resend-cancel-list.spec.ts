import { TestBed } from '@angular/core/testing';
import { afterEach,describe,expect,it,vi } from 'vitest';
import { ResendCancelList } from './resend-cancel-list';
const id=(n:number)=>`00000000-0000-4000-8000-${n.toString().padStart(12,'0')}`;
const row=(n:number)=>({requestId:id(n),jobId:id(n+100),createdAt:'2026-11-16 09:00:00+00',overdue:true,privateField:'discard'});
function setup(){const fixture=TestBed.createComponent(ResendCancelList),c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';return {fixture,c};}
describe('Resend cancellation list',()=>{
  afterEach(()=>vi.restoreAllMocks());
  it('loads scoped pages and discards extra response fields',async()=>{
    const fetcher=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:Array.from({length:50},(_,i)=>row(i+1)),next:id(50)}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[row(51)],next:null})));
    const {fixture,c}=setup();await c.list();expect(c.status()).toBe('loaded');expect(c.rows()).toHaveLength(50);expect(c.rows()[0]).not.toHaveProperty('privateField');expect(c.next()).toBe(id(50));fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('취소 결과 불명 요청');
    await c.list('next');expect(c.page()).toBe(2);expect(c.rows()[0].requestId).toBe(id(51));expect(new URL(fetcher.mock.calls[1][0] as string).searchParams.get('after')).toBe(id(50));fixture.destroy();
  });
  it('clears on denied or malformed pages and ignores a stale response',async()=>{
    let done!:(r:Response)=>void;vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response('',{status:403})).mockResolvedValueOnce(new Response(JSON.stringify({data:[row(2),row(1)],next:null}))).mockReturnValueOnce(new Promise(resolve=>done=resolve));
    const {fixture,c}=setup();await c.list();expect(c.status()).toBe('error');expect(c.rows()).toEqual([]);await c.list();expect(c.status()).toBe('error');const pending=c.list();c.environment='other';c.ngOnChanges();done(new Response(JSON.stringify({data:[row(1)],next:null})));await pending;expect(c.status()).toBe('idle');expect(c.rows()).toEqual([]);fixture.destroy();
  });
  it('requires independent detail access and distinguishes manual resolution',async()=>{
    const request={requestId:id(1),jobId:id(101),providerId:'provider-1',databasePrincipal:'service',reason:'investigation',state:'confirmed',httpStatus:null,createdAt:'today',updatedAt:'today'};
    const resolution={resolutionId:id(2),decision:'confirmed',databasePrincipal:'resolver',reason:'reviewed',recordedAt:'today'};
    vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({request,resolution}))).mockResolvedValueOnce(new Response('',{status:403}));
    const {fixture,c}=setup();await c.loadDetail(id(1));expect(c.detail()?.resolution?.decision).toBe('confirmed');fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('운영자 판정');await c.loadDetail(id(1));expect(c.detailStatus()).toBe('error');expect(c.detail()).toBeNull();fixture.destroy();
  });
  it('prepares a confirmed decision and retries an uncertain response with the same ID',async()=>{
    const resolutionId=id(42),request={requestId:id(1),jobId:id(101),providerId:'provider-1',databasePrincipal:'service',reason:'investigation',state:'unknown',httpStatus:null,createdAt:'today',updatedAt:'today'};
    vi.spyOn(crypto,'randomUUID').mockReturnValue(resolutionId as `${string}-${string}-${string}-${string}-${string}`);
    const fetcher=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({request,resolution:null}))).mockRejectedValueOnce(Error('network')).mockResolvedValueOnce(new Response(JSON.stringify({resolutionId,requestId:id(1),decision:'not_cancelled',replayed:true})));
    const {fixture,c}=setup();await c.loadDetail(id(1));c.decision='not_cancelled';c.reason='provider checked';c.prepareResolution();expect(c.resolutionStatus()).toBe('prepared');
    await c.submitResolution();expect(fetcher).toHaveBeenCalledTimes(1);c.confirm=true;await c.submitResolution();expect(c.resolutionStatus()).toBe('unknown');
    c.environment='other';await c.submitResolution();expect(fetcher).toHaveBeenCalledTimes(2);c.environment='env';await c.submitResolution();expect(c.resolutionStatus()).toBe('recorded');
    const first=JSON.parse(fetcher.mock.calls[1][1]!.body as string),second=JSON.parse(fetcher.mock.calls[2][1]!.body as string);expect(first).toEqual(second);expect(first.resolutionId).toBe(resolutionId);fixture.destroy();
  });
  it('offers resolution for legacy 408 and 409 but not other rejections',async()=>{
    const base={requestId:id(1),jobId:id(101),providerId:'provider',databasePrincipal:'service',reason:'fixture',state:'rejected',createdAt:'today',updatedAt:'today'};
    vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({request:{...base,httpStatus:408},resolution:null}))).mockResolvedValueOnce(new Response(JSON.stringify({request:{...base,httpStatus:409},resolution:null}))).mockResolvedValueOnce(new Response(JSON.stringify({request:{...base,httpStatus:400},resolution:null})));
    const {fixture,c}=setup();c.reason='checked';
    for(const code of [408,409]){await c.loadDetail(id(1));fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('과거 408·409');c.prepareResolution();expect(c.resolutionStatus()).toBe('prepared');c.resetResolution();}
    await c.loadDetail(id(1));c.prepareResolution();expect(c.resolutionStatus()).toBe('idle');fixture.destroy();
  });
  it('preserves the cancellation command ID across an uncertain submission',async()=>{
    const requestId=id(42),jobId=id(100),commandId=id(200);vi.spyOn(crypto,'randomUUID').mockReturnValue(requestId as `${string}-${string}-${string}-${string}-${string}`);
    const mock=vi.spyOn(globalThis,'fetch').mockRejectedValueOnce(Error('network')).mockResolvedValueOnce(new Response(JSON.stringify({requestId,command:{requestId:commandId,jobId,state:'pending',attemptState:null},replayed:true})));
    const {fixture,c}=setup();c.commandJobId=jobId;c.commandReason='cancel requested';c.prepareCommand();await c.submitCommand();expect(mock).not.toHaveBeenCalled();c.commandConfirm=true;await c.submitCommand();expect(c.commandStatus()).toBe('unknown');c.token='other.token.value';await c.submitCommand();expect(mock).toHaveBeenCalledTimes(1);c.token='aaa.bbb.ccc';await c.submitCommand();expect(c.commandStatus()).toBe('recorded');expect(c.command()?.state).toBe('pending');expect(mock.mock.calls[0][1]!.body).toEqual(mock.mock.calls[1][1]!.body);fixture.destroy();
  });
});
