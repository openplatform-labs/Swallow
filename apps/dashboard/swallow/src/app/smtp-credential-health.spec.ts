import { TestBed } from '@angular/core/testing';
import { afterEach,describe,expect,it,vi } from 'vitest';
import { SmtpCredentialHealth } from './smtp-credential-health';
function setup(){const fixture=TestBed.createComponent(SmtpCredentialHealth),c=fixture.componentInstance;c.apiOrigin='https://api.example.test';c.environment='env';c.token='aaa.bbb.ccc';return {fixture,c};}
describe('SMTP credential health',()=>{
 afterEach(()=>vi.restoreAllMocks());
 it('renders valid counts with a scope and threshold',async()=>{const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({healthy:1,unhealthy:0,stale:2,minimumHealthy:1,needsAttention:false})));const {fixture,c}=setup();await c.load();expect(c.health()?.needsAttention).toBe(false);expect(new URL(mock.mock.calls[0][0] as string).searchParams.get('minimumHealthy')).toBe('1');fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('정상 1개');fixture.destroy();});
 it('rejects inconsistent response and invalid threshold',async()=>{const mock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({healthy:0,unhealthy:1,stale:0,minimumHealthy:1,needsAttention:false})));const {fixture,c}=setup();await c.load();expect(c.status()).toBe('error');c.minimumHealthy=0;await c.load();expect(c.status()).toBe('error');expect(mock).toHaveBeenCalledTimes(1);fixture.destroy();});
 it('clears an in-flight response when the environment changes',async()=>{let finish!:(r:Response)=>void;vi.spyOn(globalThis,'fetch').mockReturnValueOnce(new Promise(done=>finish=done));const {fixture,c}=setup();const pending=c.load();c.environment='other';c.ngOnChanges();finish(new Response(JSON.stringify({healthy:1,unhealthy:0,stale:0,minimumHealthy:1,needsAttention:false})));await pending;expect(c.health()).toBeNull();expect(c.status()).toBe('idle');fixture.destroy();});
});
