import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { App } from './app';

describe('Swallow console', () => {
  afterEach(() => vi.restoreAllMocks());

  it('shows unimplemented management without invented operational data', async () => {
    const fixture = TestBed.createComponent(App);
    await fixture.whenStable();
    expect(fixture.nativeElement.textContent).toContain('관리자 로그인과 대부분의 관리 기능은 개발 중');
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

  it('loads only display fields and sends the API key without cookies or redirect following', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{subscriberId:'alice',firstName:'Alice',email:'a@example.test',channels:[{credentials:{deviceTokens:['secret']}}]}],next:'next',previous:null}),{status:200}));
    const app = TestBed.createComponent(App).componentInstance;
    app.apiKey = 'fixture-key';
    await app.loadSubscribers();
    expect(app.subscriberStatus()).toBe('loaded');
    expect(app.subscribers()).toEqual([{id:'alice',firstName:'Alice',lastName:'',email:'a@example.test'}]);
    expect(fetchMock).toHaveBeenCalledWith('http://127.0.0.1:3000/v2/subscribers?limit=20',expect.objectContaining({headers:{Authorization:'ApiKey fixture-key'},credentials:'omit',redirect:'error'}));
    app.forgetKey();
    expect(app.apiKey).toBe('');
    expect(app.subscribers()).toEqual([]);
  });

  it('requests the API cursors for next and previous pages', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(new Response(JSON.stringify({data:[{subscriberId:'first'}],next:'cursor-next',previous:null}),{status:200}))
      .mockResolvedValueOnce(new Response(JSON.stringify({data:[{subscriberId:'second'}],next:null,previous:'cursor-prev'}),{status:200}))
      .mockResolvedValueOnce(new Response(JSON.stringify({data:[{subscriberId:'first'}],next:'cursor-next',previous:null}),{status:200}));
    const app = TestBed.createComponent(App).componentInstance;
    app.apiKey = 'fixture-key';
    await app.loadSubscribers();
    await app.loadSubscribers('after');
    expect(fetchMock.mock.calls[1][0]).toContain('after=cursor-next');
    expect(app.subscribers()[0].id).toBe('second');
    await app.loadSubscribers('before');
    expect(fetchMock.mock.calls[2][0]).toContain('before=cursor-prev');
    expect(app.subscribers()[0].id).toBe('first');
  });

  it('clears rows and reports authorization failures', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response('',{status:401}));
    const app = TestBed.createComponent(App).componentInstance;
    app.apiKey = 'fixture-key';
    await app.loadSubscribers();
    expect(app.subscriberStatus()).toBe('error');
    expect(app.subscriberError()).toContain('인증');
    expect(app.subscribers()).toEqual([]);
  });

  it('discards a subscriber response after the API key changes', async () => {
    let resolve!: (response: Response) => void;
    vi.spyOn(globalThis, 'fetch').mockReturnValueOnce(new Promise<Response>((done) => { resolve = done; }));
    const app = TestBed.createComponent(App).componentInstance;
    app.apiKey = 'first-key';
    const pending = app.loadSubscribers();
    app.apiKey = 'second-key';
    app.clearSubscribers();
    resolve(new Response(JSON.stringify({data:[{subscriberId:'old'}],next:null,previous:null}),{status:200}));
    await pending;
    expect(app.subscriberStatus()).toBe('idle');
    expect(app.subscribers()).toEqual([]);
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
