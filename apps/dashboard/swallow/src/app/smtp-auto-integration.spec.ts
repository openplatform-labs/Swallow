import { TestBed } from '@angular/core/testing';
import { describe, expect, it } from 'vitest';
import { SmtpAutoPolicy } from './smtp-auto-policy';

const env = (globalThis as unknown as { process?: { env: Record<string, string | undefined> } }).process?.env;
const origin = env?.['SWALLOW_UI_TEST_API'], token = env?.['SWALLOW_UI_TEST_JWT'];
describe.skipIf(!origin || !token)('SMTP auto retry policy with real local API', () => {
  it('reads, confirms, changes, looks up and audits policy and retry history', async () => {
    const fixture = TestBed.createComponent(SmtpAutoPolicy), c = fixture.componentInstance;
    c.apiOrigin = origin!; c.environment = 'fixture-env'; c.token = token!;
    await c.load(); expect(c.readStatus()).toBe('loaded');
    expect(c.policy()).toEqual({ enabled: false, maxRetries: 1, delaySeconds: 60, eligibleSince: null, revision: '0' });
    c.enabled = true; c.maxRetries = 2; c.delaySeconds = 120; c.cutoff = 'now'; c.reason = 'integration fixture'; c.confirm = true;
    await c.prepare(); const draft = c.snapshot()!.body; expect(c.changeStatus()).toBe('prepared');
    let response = await fetch(`${origin}/v1/experimental/operator/smtp-auto-policy?environmentId=fixture-env`, { headers: { Authorization: `Bearer ${token}` } });
    expect((await response.json()).enabled).toBe(false);
    c.submitConfirm = true; await c.submit(); expect(c.changeStatus()).toBe('recorded');
    c.lookupId = draft.requestId; await c.lookup(); expect(c.lookupStatus()).toBe('found');
    await c.audit(); expect(c.auditStatus()).toBe('loaded'); expect(c.auditRows().some(row => row.requestId === draft.requestId && row.actorSubject === 'admin')).toBe(true);
    await c.history(); expect(c.historyStatus()).toBe('loaded'); expect(c.historyRows()).toHaveLength(1);
    expect(c.historyRows()[0].jobId).toBe('11111111-1111-4111-8111-111111111111'); expect(JSON.stringify(c.historyRows())).not.toContain('fixture-secret');
    fixture.detectChanges(); expect(fixture.nativeElement.textContent).toContain('SMTP 자동 재시도 이력');
    response = await fetch(`${origin}/v1/experimental/operator/smtp-auto-policy?environmentId=fixture-env`, { headers: { Authorization: `Bearer ${token}` } });
    expect(await response.json()).toMatchObject({ enabled: true, maxRetries: 2, delaySeconds: 120, revision: '1' });
    fixture.destroy();
  });
});
