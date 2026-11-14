import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { App } from './app';

describe('Swallow console', () => {
  afterEach(() => vi.restoreAllMocks());

  it('shows unimplemented management without invented operational data', async () => {
    const fixture = TestBed.createComponent(App);
    await fixture.whenStable();
    expect(fixture.nativeElement.textContent).toContain('관리자 로그인과 관리 기능은 개발 중');
    expect(fixture.nativeElement.textContent).toContain('연결 확인 필요');
  });

  it('only reports ready for the API ready response', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({ status: 'ready' }), { status: 200, headers: { 'content-type': 'application/json' } }));
    const app = TestBed.createComponent(App).componentInstance;
    await app.checkConnection();
    expect(app.status()).toBe('ready');
    expect(fetchMock).toHaveBeenCalledWith('http://127.0.0.1:3000/health/ready', expect.objectContaining({ credentials: 'omit' }));
    fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({ status: 'not_ready' }), { status: 503 }));
    await app.checkConnection();
    expect(app.status()).toBe('unavailable');
  });

  it('ignores a result after the API address has changed', async () => {
    let resolve!: (value: Response) => void;
    vi.spyOn(globalThis, 'fetch').mockReturnValueOnce(new Promise<Response>((done) => { resolve = done; }));
    const app = TestBed.createComponent(App).componentInstance;
    const pending = app.checkConnection();
    app.apiOrigin = 'https://another.example';
    app.connectionChanged();
    resolve(new Response(JSON.stringify({ status: 'ready' }), { status: 200 }));
    await pending;
    expect(app.status()).toBe('unchecked');
  });

  it('rejects insecure remote API addresses without sending a request', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch');
    const app = TestBed.createComponent(App).componentInstance;
    app.apiOrigin = 'http://example.test';
    await app.checkConnection();
    expect(app.status()).toBe('unavailable');
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
