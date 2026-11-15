import { TestBed } from '@angular/core/testing';
import { describe, expect, it } from 'vitest';
import { App } from './app';

const env=(globalThis as unknown as {process?:{env:Record<string,string|undefined>}}).process?.env;
const origin=env?.['SWALLOW_UI_TEST_API'],token=env?.['SWALLOW_UI_TEST_JWT'];
describe.skipIf(!origin || !token)('SMTP auto retry grants with real local API',()=>{
  it('grants, audits and revokes a target permission',async()=>{
    const fixture=TestBed.createComponent(App),app=fixture.componentInstance;
    app.apiOrigin=origin!;app.setSection('settings');app.permissionEnvironment='fixture-env';app.permissionKey=token!;
    app.permissionIssuer='https://issuer.test';app.permissionSubject='target';app.permissionClient='console';app.grantKind='smtp-auto:history';
    await app.loadGrant();expect(app.grantStatus()).toBe('loaded');expect(app.grantValue()).toBe(false);
    app.grantDesired=true;app.grantReason='grant fixture';app.grantConfirmed=true;await app.changeGrant();expect(app.grantChangeStatus()).toBe('prepared');
    const request=app.grantChange()!.body.requestId;expect(app.grantRequestScope).toBe('smtp-auto');
    app.grantSubmitConfirmed=true;await app.submitGrant();expect(app.grantChangeStatus()).toBe('recorded');
    app.grantRequestId=request;await app.lookupGrantRequest();expect(app.grantRequestRecord()?.resource).toBe('smtp-auto');
    app.grantAuditScope='smtp-auto';await app.loadGrantAudit();expect(app.grantAuditRows().some(row=>row.requestId===request && row.targetSubject==='target')).toBe(true);
    fixture.detectChanges();expect(fixture.nativeElement.textContent).toContain('SMTP 자동 재시도 정책 권한');
    app.clearGrantChange();await app.loadGrant();expect(app.grantValue()).toBe(true);app.grantDesired=false;app.grantReason='revoke fixture';app.grantConfirmed=true;await app.changeGrant();app.grantSubmitConfirmed=true;await app.submitGrant();expect(app.grantChangeStatus()).toBe('recorded');
    app.clearGrantChange();await app.loadGrant();expect(app.grantValue()).toBe(false);fixture.destroy();
  });
});
