import { TestBed } from '@angular/core/testing';
import { describe, expect, it } from 'vitest';
import { SmtpTransportPolicy } from './smtp-transport-policy';

// Opt-in integration against a disposable local API and test-only JWT.
const env=(globalThis as unknown as {process?:{env:Record<string,string|undefined>}}).process?.env;
const origin=env?.['SWALLOW_UI_TEST_API'], token=env?.['SWALLOW_UI_TEST_JWT'];
describe.skipIf(!origin || !token)('SMTP transport policy with real local API',()=>{
  it('reads, confirms, changes, looks up and audits a policy',async()=>{
    const fixture=TestBed.createComponent(SmtpTransportPolicy), component=fixture.componentInstance;
    component.apiOrigin=origin!;component.environment='fixture-env';component.token=token!;
    await component.load();expect(component.readStatus()).toBe('loaded');expect(component.policy()).toEqual({mode:'disabled',revision:'0'});
    fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('SMTP 연결 정책');
    component.mode='starttls';component.reason='integration fixture';component.confirm=true;await component.prepare();
    const draft=component.snapshot()!.body;expect(component.changeStatus()).toBe('prepared');
    let direct=await fetch(`${origin}/v1/experimental/operator/smtp-transport-policy?environmentId=fixture-env`,{headers:{Authorization:`Bearer ${token}`}});
    expect((await direct.json()).mode).toBe('disabled');
    component.submitConfirm=true;await component.submit();expect(component.changeStatus()).toBe('recorded');
    component.lookupId=draft.requestId;await component.lookup();expect(component.lookupRecord()?.mode).toBe('starttls');
    await component.audit();expect(component.auditRows().some(row=>row.requestId===draft.requestId&&row.actorSubject==='admin')).toBe(true);
    fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('integration fixture');
    direct=await fetch(`${origin}/v1/experimental/operator/smtp-transport-policy?environmentId=fixture-env`,{headers:{Authorization:`Bearer ${token}`}});
    expect(await direct.json()).toEqual({mode:'starttls',revision:'1'});
    fixture.destroy();
  });
});
