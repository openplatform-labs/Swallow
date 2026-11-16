import { TestBed } from '@angular/core/testing';
import { describe,expect,it } from 'vitest';
import { App } from './app';
const env=(globalThis as unknown as {process?:{env:Record<string,string|undefined>}}).process?.env;
const origin=env?.['SWALLOW_UI_TEST_API'],token=env?.['SWALLOW_UI_TEST_JWT'];
describe.skipIf(!origin||!token)('Resend cancellation grants with local API',()=>{
  it('delegates and revokes list access through the settings component',async()=>{
    const fixture=TestBed.createComponent(App),app=fixture.componentInstance;app.apiOrigin=origin!;app.permissionEnvironment='cancel-api-env';app.permissionKey=token!;app.permissionIssuer='https://issuer.test';app.permissionSubject='target';app.permissionClient='console';app.grantKind='resend-cancel:list';
    await app.loadGrant();expect(app.grantValue()).toBe(false);app.grantDesired=true;app.grantReason='fixture grant';app.grantConfirmed=true;await app.changeGrant();app.grantSubmitConfirmed=true;await app.submitGrant();expect(app.grantChangeStatus()).toBe('recorded');app.clearGrantChange();
    await app.loadGrant();expect(app.grantValue()).toBe(true);app.grantDesired=false;app.grantReason='fixture revoke';app.grantConfirmed=true;await app.changeGrant();app.grantSubmitConfirmed=true;await app.submitGrant();expect(app.grantChangeStatus()).toBe('recorded');app.clearGrantChange();
    await app.loadGrant();expect(app.grantValue()).toBe(false);fixture.destroy();
  });
});
