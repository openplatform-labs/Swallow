import { TestBed } from '@angular/core/testing';
import { describe,expect,it } from 'vitest';
import { ResendCancelList } from './resend-cancel-list';
const env=(globalThis as unknown as {process?:{env:Record<string,string|undefined>}}).process?.env;
const origin=env?.['SWALLOW_UI_TEST_API'],token=env?.['SWALLOW_UI_TEST_JWT'];
describe.skipIf(!origin||!token)('Legacy cancellation resolution with local API',()=>{
 it('reviews a historical 408 while retaining the original status',async()=>{const f=TestBed.createComponent(ResendCancelList),c=f.componentInstance;c.apiOrigin=origin!;c.environment='cancel-api-env';c.token=token!;const id='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1';await c.loadDetail(id);expect(c.detail()?.state).toBe('rejected');expect(c.detail()?.httpStatus).toBe(408);c.reason='checked provider';c.prepareResolution();expect(c.resolutionStatus()).toBe('prepared');c.confirm=true;await c.submitResolution();expect(c.resolutionStatus()).toBe('recorded');c.resetResolution();await c.loadDetail(id);expect(c.detail()?.state).toBe('confirmed');expect(c.detail()?.httpStatus).toBe(408);expect(c.detail()?.resolution?.operator?.subject).toBe('fixture');f.destroy();});
});
