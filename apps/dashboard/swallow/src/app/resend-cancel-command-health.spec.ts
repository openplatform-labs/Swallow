import { TestBed } from '@angular/core/testing';
import { afterEach,describe,expect,it,vi } from 'vitest';
import { ResendCancelCommandHealth } from './resend-cancel-command-health';
const id=(n:number)=>`00000000-0000-4000-8000-${n.toString().padStart(12,'0')}`;
const row=(n:number)=>({requestId:id(n),jobId:id(n+100),failedAt:'today',secret:'ignored'});
function setup(){const fixture=TestBed.createComponent(ResendCancelCommandHealth),c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';return {fixture,c};}
describe('Resend cancellation command monitoring',()=>{
 afterEach(()=>vi.restoreAllMocks());
 it('reads separate health and list endpoints and removes extra data',async()=>{const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({pending:3,overdue:1,unreviewed:1}))).mockResolvedValueOnce(new Response(JSON.stringify({data:[row(1)],next:null})));const {fixture,c}=setup();await c.load();await c.list();expect(c.health()).toEqual({pending:3,overdue:1,unreviewed:1});expect(c.rows()).toEqual([{requestId:id(1),jobId:id(101),failedAt:'today'}]);expect(new URL(mock.mock.calls[0][0] as string).pathname).toContain('command-health');expect(new URL(mock.mock.calls[1][0] as string).pathname).toContain('command-failures');fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('미확인 실패');fixture.destroy();});
 it('clears unauthorized data and ignores stale responses',async()=>{let finish!:(r:Response)=>void;vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response('',{status:403})).mockReturnValueOnce(new Promise(resolve=>finish=resolve));const {fixture,c}=setup();await c.load();expect(c.healthStatus()).toBe('error');const pending=c.list();c.environment='other';c.ngOnChanges();finish(new Response(JSON.stringify({data:[row(1)],next:null})));await pending;expect(c.listStatus()).toBe('idle');expect(c.rows()).toEqual([]);fixture.destroy();});
});
