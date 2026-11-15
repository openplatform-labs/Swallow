import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { SmtpAutoHealth } from './smtp-auto-health';
function setup(){const fixture=TestBed.createComponent(SmtpAutoHealth),c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';return {fixture,c};}
describe('SMTP automatic retry health',()=>{
 afterEach(()=>vi.restoreAllMocks());
 it('reads and renders scoped aggregate data',async()=>{const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({enabled:true,eligible:3,overdue:2,exhausted:1})));const {fixture,c}=setup();await c.load();expect(c.health()).toEqual({enabled:true,eligible:3,overdue:2,exhausted:1});expect(new URL(mock.mock.calls[0][0] as string).searchParams.get('environmentId')).toBe('env');fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('대상 3건');fixture.destroy();});
 it('rejects invalid responses and invalid input',async()=>{const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({enabled:true,eligible:0,overdue:2,exhausted:0})));const {fixture,c}=setup();await c.load();expect(c.status()).toBe('error');c.graceSeconds=0;await c.load();expect(c.status()).toBe('error');expect(mock).toHaveBeenCalledTimes(1);fixture.destroy();});
 it('does not display a stale response after credentials change',async()=>{let finish!:(r:Response)=>void;vi.spyOn(globalThis,'fetch').mockReturnValueOnce(new Promise(done=>finish=done));const {fixture,c}=setup();const pending=c.load();c.environment='other';c.ngOnChanges();finish(new Response(JSON.stringify({enabled:true,eligible:1,overdue:0,exhausted:0})));await pending;expect(c.health()).toBeNull();expect(c.status()).toBe('idle');fixture.destroy();});
});
