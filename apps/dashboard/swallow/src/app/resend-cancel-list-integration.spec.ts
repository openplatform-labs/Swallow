import { TestBed } from '@angular/core/testing';
import { describe,expect,it } from 'vitest';
import { ResendCancelList } from './resend-cancel-list';
const env=(globalThis as unknown as {process?:{env:Record<string,string|undefined>}}).process?.env;
const origin=env?.['SWALLOW_UI_TEST_API'],token=env?.['SWALLOW_UI_TEST_JWT'];
describe.skipIf(!origin||!token)('Resend cancellation list with local API',()=>{
  it('shows both pages from the restricted JWT API',async()=>{
    const fixture=TestBed.createComponent(ResendCancelList),c=fixture.componentInstance;c.apiOrigin=origin!;c.environment='cancel-api-env';c.token=token!;c.grace=60;
    await c.list();expect(c.status()).toBe('loaded');expect(c.rows()).toHaveLength(50);fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('취소 결과 불명 요청');
    await c.loadDetail(c.rows()[0].requestId);expect(c.detailStatus()).toBe('loaded');expect(c.detail()?.state).toBe('unknown');fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('private-provider');
    c.reason='fixture evidence';c.prepareResolution();expect(c.resolutionStatus()).toBe('prepared');c.confirm=true;await c.submitResolution();expect(c.resolutionStatus()).toBe('recorded');
    await c.list('next');expect(c.status()).toBe('loaded');expect(c.rows()).toHaveLength(6);expect(c.next()).toBeNull();fixture.destroy();
  });
});
