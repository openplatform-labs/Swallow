import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { SmtpAutoWorkerHealth } from './smtp-auto-worker-health';
function setup(){const fixture=TestBed.createComponent(SmtpAutoWorkerHealth), c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';return {fixture,c};}
describe('SMTP automatic retry worker health',()=>{
 afterEach(()=>vi.restoreAllMocks());
 it('renders separately authorized worker counts',async()=>{vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({healthy:1,stale:2,minimumHealthy:1,needsAttention:false})));const {fixture,c}=setup();await c.load();expect(c.status()).toBe('loaded');fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('정상 1개');fixture.destroy();});
 it('rejects inconsistent data and clears stale results',async()=>{let finish!:(r:Response)=>void;vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({healthy:0,stale:1,minimumHealthy:1,needsAttention:false}))).mockReturnValueOnce(new Promise(done=>finish=done));const {fixture,c}=setup();await c.load();expect(c.status()).toBe('error');const pending=c.load();c.environment='other';c.ngOnChanges();finish(new Response(JSON.stringify({healthy:1,stale:0,minimumHealthy:1,needsAttention:false})));await pending;expect(c.status()).toBe('idle');expect(c.health()).toBeNull();fixture.destroy();});
});
