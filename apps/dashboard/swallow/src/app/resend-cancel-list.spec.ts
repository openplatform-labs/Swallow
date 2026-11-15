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
});
