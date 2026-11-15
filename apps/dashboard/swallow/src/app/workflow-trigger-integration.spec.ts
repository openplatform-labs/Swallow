import { TestBed } from '@angular/core/testing';
import { describe, expect, it, vi } from 'vitest';
import { WorkflowTrigger } from './workflow-trigger';

const env=(globalThis as unknown as {process?:{env:Record<string,string|undefined>}}).process?.env;
const origin=env?.['SWALLOW_UI_TEST_API'];
describe.skipIf(!origin)('scheduled workflow trigger with real local API',()=>{
  it('submits a scheduled Resend override from the rendered form',async()=>{
    const fixture=TestBed.createComponent(WorkflowTrigger), c=fixture.componentInstance;
    c.apiOrigin=origin!;c.apiKey='schedule-fixture-key';c.workflow='schedule-fixture';c.subscriber='fixture-subscriber';c.mode='normal';
    fixture.detectChanges();await fixture.whenStable();fixture.detectChanges();
    const checkbox=fixture.nativeElement.querySelector('input[name=resendScheduleEnabled]') as HTMLInputElement;
    checkbox.click();await fixture.whenStable();fixture.detectChanges();
    const input=fixture.nativeElement.querySelector('#trigger-resend-schedule') as HTMLInputElement;
    input.value='2030-01-01T09:00:00+09:00';input.dispatchEvent(new Event('input'));fixture.detectChanges();
    (fixture.nativeElement.querySelector('form') as HTMLFormElement).dispatchEvent(new Event('submit',{bubbles:true,cancelable:true}));
    await vi.waitFor(()=>expect(c.status()).toBe('accepted'),{timeout:10000});
    expect(c.outcome()).toBe('processed');fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('접수 상태');
    fixture.destroy();
  });
});
