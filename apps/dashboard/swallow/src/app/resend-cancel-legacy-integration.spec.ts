import { TestBed } from '@angular/core/testing';
import { describe,expect,it,vi } from 'vitest';
import { ResendCancelList } from './resend-cancel-list';
const env=(globalThis as unknown as {process?:{env:Record<string,string|undefined>}}).process?.env;
const origin=env?.['SWALLOW_UI_TEST_API'],token=env?.['SWALLOW_UI_TEST_JWT'];
describe.skipIf(!origin||!token)('Legacy cancellation resolution with local API',()=>{
 it('reviews a historical 408 while retaining the original status',async()=>{const f=TestBed.createComponent(ResendCancelList),c=f.componentInstance;c.apiOrigin=origin!;c.environment='cancel-api-env';c.token=token!;const id='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1';await c.loadLegacyHealth();await c.listLegacy();expect(c.legacyHealth()).toBe(1);expect(c.legacyRows()[0].requestId).toBe(id);await c.loadDetail(c.legacyRows()[0].requestId);expect(c.detail()?.state).toBe('rejected');expect(c.detail()?.httpStatus).toBe(408);c.reason='checked provider';c.prepareResolution();expect(c.resolutionStatus()).toBe('prepared');c.confirm=true;
  const original=globalThis.fetch.bind(globalThis);let received!:()=>void,release!:()=>void;const started=new Promise<void>(r=>received=r),gate=new Promise<void>(r=>release=r);let first=true;
  const mock=vi.spyOn(globalThis,'fetch').mockImplementation(async(input,init)=>{const response=await original(input,init);if(first&&String(input).includes('/resend-cancel-resolution?')){first=false;received();await gate;}return response;});
  try{const pending=c.submitResolution();await started;c.environment='cancel-api-other';c.ngOnChanges();release();await pending;expect(c.resolutionStatus()).toBe('unknown');c.environment='cancel-api-env';c.ngOnChanges();await c.submitResolution();expect(c.resolutionStatus()).toBe('recorded');expect(mock.mock.calls[0][1]?.body).toEqual(mock.mock.calls[1][1]?.body);}finally{mock.mockRestore();}
  expect(c.legacyRows()).toHaveLength(0);await c.loadLegacyHealth();expect(c.legacyHealth()).toBe(0);c.resetResolution();await c.loadDetail(id);expect(c.detail()?.state).toBe('confirmed');expect(c.detail()?.httpStatus).toBe(408);expect(c.detail()?.resolution?.operator?.subject).toBe('fixture');f.destroy();});
});
