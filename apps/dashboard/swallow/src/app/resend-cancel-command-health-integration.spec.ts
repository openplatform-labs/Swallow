import { TestBed } from '@angular/core/testing';
import { describe,expect,it } from 'vitest';
import { ResendCancelCommandHealth } from './resend-cancel-command-health';
const env=(globalThis as unknown as {process?:{env:Record<string,string|undefined>}}).process?.env;
const origin=env?.['SWALLOW_UI_TEST_API'],token=env?.['SWALLOW_UI_TEST_JWT'];
describe.skipIf(!origin||!token)('Resend cancellation command monitoring with local API',()=>{
 it('reads and reviews a failed command through independent permissions',async()=>{const fixture=TestBed.createComponent(ResendCancelCommandHealth),c=fixture.componentInstance;c.apiOrigin=origin!;c.environment='cancel-api-env';c.token=token!;await c.load();await c.list();expect(c.healthStatus()).toBe('loaded');expect(c.health()?.unreviewed).toBe(2);expect(c.rows()).toHaveLength(2);fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('미확인 실패');c.selectFailure(c.rows()[0]);c.reason='fixture review';c.prepareReview();c.confirm=true;await c.submitReview();expect(c.reviewStatus()).toBe('recorded');await c.lookupReview();expect(c.reviewLookupStatus()).toBe('found');fixture.destroy();});
});
