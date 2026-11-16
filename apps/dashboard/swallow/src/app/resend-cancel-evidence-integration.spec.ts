import { TestBed } from '@angular/core/testing';
import { describe,expect,it } from 'vitest';
import { ResendCancelEvidence } from './resend-cancel-evidence';
const env=(globalThis as unknown as {process?:{env:Record<string,string|undefined>}}).process?.env;
const origin=env?.['SWALLOW_UI_TEST_API'],token=env?.['SWALLOW_UI_TEST_JWT'];
describe.skipIf(!origin||!token)('Resend cancellation evidence with local API',()=>{
 it('loads independently authorized counts and evidence',async()=>{const f=TestBed.createComponent(ResendCancelEvidence),c=f.componentInstance;c.apiOrigin=origin!;c.environment='cancel-api-env';c.token=token!;await c.load();await c.list();expect(c.healthStatus()).toBe('loaded');expect(c.health()?.conflicting).toBe(1);expect(c.rows()).toHaveLength(1);f.detectChanges();expect(f.nativeElement.textContent).toContain('발송 이벤트 충돌');f.destroy();});
});
