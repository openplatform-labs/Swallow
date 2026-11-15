import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { SmtpAutoPolicy } from './smtp-auto-policy';

const initial = { enabled: false, maxRetries: 1, delaySeconds: 60, eligibleSince: null, revision: '0' };
function setup() {
  const fixture = TestBed.createComponent(SmtpAutoPolicy);
  const c = fixture.componentInstance;
  c.apiOrigin = 'https://api.example.test'; c.environment = 'env'; c.token = 'aaa.bbb.ccc';
  return { fixture, c };
}
describe('SMTP automatic retry policy', () => {
  afterEach(() => vi.restoreAllMocks());
  it('requires confirmation and sends the selected historical cutoff', async () => {
    const mock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify(initial)))
      .mockImplementationOnce(async (_url, options) => {
        const b = JSON.parse(options!.body as string);
        return new Response(JSON.stringify({ requestId: b.requestId, replayed: false, policy: { enabled: true, maxRetries: 2, delaySeconds: 120, eligibleSince: '-infinity', revision: '1' } }));
      });
    const { fixture, c } = setup(); await c.load();
    c.enabled = true; c.maxRetries = 2; c.delaySeconds = 120; c.cutoff = 'all'; c.reason = 'ticket'; c.confirm = true;
    await c.prepare(); expect(c.changeStatus()).toBe('prepared'); await c.submit(); expect(mock).toHaveBeenCalledTimes(1);
    c.submitConfirm = true; await c.submit(); expect(c.changeStatus()).toBe('recorded');
    expect(JSON.parse(mock.mock.calls[1][1]!.body as string)).toMatchObject({ cutoff: 'all', delaySeconds: 120, expectedRevision: '0' });
    fixture.detectChanges(); expect(fixture.nativeElement.textContent).toContain('SMTP'); fixture.destroy();
  });
  it('paginates redacted history and clears it when credentials change', async () => {
    const row = (id: number) => ({ id: String(id), jobId: '11111111-1111-4111-8111-111111111111', attemptRevision: '22222222-2222-4222-8222-222222222222', failureRevision: '1', databasePrincipal: 'api', maxRetries: 2, delaySeconds: 60, recordedAt: 'today', secret: 'discard' });
    const first = Array.from({ length: 50 }, (_, i) => row(100 - i));
    const mock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({ data: first, next: '51' })))
      .mockResolvedValueOnce(new Response(JSON.stringify({ data: [row(50)], next: null })))
      .mockResolvedValueOnce(new Response('', { status: 403 }));
    const { fixture, c } = setup(); await c.history(); expect(c.historyRows()).toHaveLength(50);
    expect(JSON.stringify(c.historyRows())).not.toContain('discard'); await c.history('next'); expect(c.historyPage()).toBe(2);
    expect(new URL(mock.mock.calls[1][0] as string).searchParams.get('before')).toBe('51');
    c.token = 'new.token.value'; c.ngOnChanges(); expect(c.historyRows()).toEqual([]); await c.history(); expect(c.historyStatus()).toBe('error'); fixture.destroy();
  });
});
