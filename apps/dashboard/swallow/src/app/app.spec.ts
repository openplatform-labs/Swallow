import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { App } from './app';
import { OperatorOidc } from './operator-oidc';

describe('Swallow console', () => {
  afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });

  it('expires the popup token without losing an uncertain decision or a different credential', async () => {
    vi.useFakeTimers(); vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+2});
    const fixture=TestBed.createComponent(App); const app=fixture.componentInstance;
    app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';
    await app.prepareOidc();await app.signInOidc();app.useOidc('alerts');
    app.apiKey='different.credential.value';app.decisionStatus.set('unknown');app.decisionRequestId.set('retained-request');
    vi.advanceTimersByTime(2000);
    expect(app.oidcStatus()).toBe('expired');expect(app.oidcSubject()).toBe('');expect(app.alertKey).toBe('');
    expect(app.apiKey).toBe('different.credential.value');expect(app.decisionStatus()).toBe('unknown');expect(app.decisionRequestId()).toBe('retained-request');
    app.useOidc('alerts');expect(app.alertKey).toBe('');
    fixture.destroy();
  });

  it('checks expiry before applying a session even if the browser delayed its timer', async () => {
    vi.useFakeTimers();vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+2});
    const fixture=TestBed.createComponent(App);const app=fixture.componentInstance;
    app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';
    await app.prepareOidc();await app.signInOidc();vi.setSystemTime(new Date('2026-01-01T00:00:03Z'));
    app.useOidc('subscribers');expect(app.oidcStatus()).toBe('expired');expect(app.apiKey).toBe('');
    fixture.destroy();
  });

  it('uses an operator session only after login and removes it on local logout', async () => {
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    vi.spyOn(OperatorOidc.prototype,'signIn').mockResolvedValue({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+600});
    const app=TestBed.createComponent(App).componentInstance;
    app.oidcIssuer='https://issuer.test'; app.oidcClientId='console'; app.oidcEnvironment='env-a';
    await app.prepareOidc(); expect(app.oidcStatus()).toBe('ready');
    await app.signInOidc(); expect(app.oidcStatus()).toBe('signed-in'); expect(app.oidcSubject()).toBe('alice');
    app.useOidc('subscribers'); expect(app.credentialMode).toBe('jwt'); expect(app.apiKey).toBe('aaa.bbb.ccc');
    app.useOidc('alerts'); expect(app.alertAuthMode).toBe('jwt'); expect(app.alertKey).toBe('aaa.bbb.ccc');
    app.resetOidc(); expect(app.alertKey).toBe(''); expect(app.oidcStatus()).toBe('idle');
    app.useOidc('alerts'); expect(app.alertKey).toBe('');
  });

  it('ignores a login result after settings or API address change', async () => {
    vi.spyOn(OperatorOidc.prototype,'prepare').mockImplementation(async settings=>({settings,metadata:{issuer:settings.issuer}}));
    let finish!: (session:{token:string;subject:string;environmentId:string;expiresAt:number})=>void;
    vi.spyOn(OperatorOidc.prototype,'signIn').mockReturnValue(new Promise(done=>{finish=done;}));
    const app=TestBed.createComponent(App).componentInstance;
    app.oidcIssuer='https://issuer.test';app.oidcClientId='console';app.oidcEnvironment='env-a';
    await app.prepareOidc();const pending=app.signInOidc();app.connectionChanged();
    finish({token:'aaa.bbb.ccc',subject:'alice',environmentId:'env-a',expiresAt:Math.floor(Date.now()/1000)+600});await pending;
    expect(app.oidcStatus()).toBe('idle');expect(app.oidcSubject()).toBe('');app.useOidc('alerts');expect(app.alertKey).toBe('');
  });

  it('loads subscriber pages with an operator JWT and an explicit environment', async () => {
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValue(new Response(JSON.stringify({data:[{subscriberId:'alice',firstName:'Alice',email:'alice@example.test'}],next:'alice',previous:null}),{status:200}));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('subscribers'); app.credentialMode='jwt'; app.apiKey='aaa.bbb.ccc'; app.subscriberEnvironment='env / one';
    await app.loadSubscribers();
    expect(app.subscribers()[0].id).toBe('alice');
    const url=new URL(fetchMock.mock.calls[0][0] as string);
    expect(url.pathname).toBe('/v1/experimental/operator/subscribers'); expect(url.searchParams.get('environmentId')).toBe('env / one');
    expect(fetchMock.mock.calls[0][1]).toEqual(expect.objectContaining({headers:{Authorization:'Bearer aaa.bbb.ccc'},credentials:'omit',redirect:'error'}));
    await app.loadSubscribers('after');
    expect(new URL(fetchMock.mock.calls[1][0] as string).searchParams.get('after')).toBe('alice');
    app.setSection('overview'); expect(app.apiKey).toBe(''); expect(app.subscribers()).toEqual([]);
  });

  it('uses JWT alert permissions for decisions and preserves the exact uncertain request', async () => {
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200})).mockRejectedValueOnce(new TypeError('network')).mockResolvedValueOnce(new Response(JSON.stringify({recorded:true,replayed:true}),{status:200}));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertAuthMode='jwt'; app.alertEnvironment='env-a'; app.alertKey='aaa.bbb.ccc';
    await app.loadAlerts();
    expect(fetchMock.mock.calls[0][0]).toBe('http://127.0.0.1:3000/v1/experimental/operator/alerts?environmentId=env-a');
    app.selectAlert(app.alertSlots()[0]); app.decisionProtocol='legacy'; app.decisionKey='aaa.bbb.ccc'; app.decisionReason='verified'; app.decisionConfirmed=true;
    await app.resolveAlert();
    expect(app.decisionStatus()).toBe('unknown');
    expect(fetchMock.mock.calls[1][0]).toBe('http://127.0.0.1:3000/v1/experimental/operator/alerts/resolve?environmentId=env-a');
    expect(JSON.parse(fetchMock.mock.calls[1][1]?.body as string).requestId).toBe(app.decisionRequestId());
    expect(fetchMock.mock.calls[1][1]?.headers).toEqual({Authorization:'Bearer aaa.bbb.ccc','Content-Type':'application/json'});
    app.retryKey='aaa.bbb.ccc'; app.retryConfirmed=true; await app.retryDecision();
    expect(fetchMock.mock.calls[2][0]).toBe(fetchMock.mock.calls[1][0]); expect(fetchMock.mock.calls[2][1]?.body).toBe(fetchMock.mock.calls[1][1]?.body);
    expect(app.decisionStatus()).toBe('recorded'); expect(app.retryKey).toBe('');
    fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({found:false}),{status:200}));
    app.lookupKey='ddd.eee.fff'; app.lookupId=app.decisionRequestId()!; await app.lookupDecisionRequest();
    const url=new URL(fetchMock.mock.calls[3][0] as string); expect(url.pathname).toBe('/v1/experimental/operator/alerts/request'); expect(url.searchParams.get('environmentId')).toBe('env-a');
    expect(fetchMock.mock.calls[3][1]?.headers).toEqual({Authorization:'Bearer ddd.eee.fff'});
    app.setSection('overview'); expect(app.alertKey).toBe(''); expect(app.lookupKey).toBe('');
  });

  it('does not send a JWT without an environment or with malformed token bytes', async () => {
    const fetchMock=vi.spyOn(globalThis,'fetch'); const app=TestBed.createComponent(App).componentInstance;
    app.credentialMode='jwt'; app.apiKey='aaa.bbb.ccc'; app.subscriberEnvironment=''; await app.loadSubscribers();
    app.alertAuthMode='jwt'; app.alertEnvironment='env-a'; app.alertKey='bad token'; await app.loadAlerts();
    expect(fetchMock).not.toHaveBeenCalled(); expect(app.subscriberStatus()).toBe('error'); expect(app.alertStatus()).toBe('error');
  });

  it('shows unimplemented management without invented operational data', async () => {
    const fixture = TestBed.createComponent(App);
    await fixture.whenStable();
    expect(fixture.nativeElement.textContent).toContain('대부분의 관리 기능은 개발 중');
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

  it('uses the separate read-only credential by default', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[],next:null,previous:null}),{status:200}));
    const app = TestBed.createComponent(App).componentInstance;
    app.apiKey = 'a'.repeat(64);
    await app.loadSubscribers();
    expect(app.subscriberStatus()).toBe('loaded');
    expect(fetchMock).toHaveBeenCalledWith('http://127.0.0.1:3000/v1/experimental/console/subscribers?limit=20',expect.objectContaining({headers:{Authorization:`SwallowConsole ${'a'.repeat(64)}`}}));
  });

  it('loads only display fields and sends the API key without cookies or redirect following', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{subscriberId:'alice',firstName:'Alice',email:'a@example.test',channels:[{credentials:{deviceTokens:['secret']}}]}],next:'next',previous:null}),{status:200}));
    const app = TestBed.createComponent(App).componentInstance;
    app.credentialMode = 'api-key';
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
    app.credentialMode = 'api-key';
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
    app.credentialMode = 'api-key';
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
    app.credentialMode = 'api-key';
    app.apiKey = 'first-key';
    const pending = app.loadSubscribers();
    app.apiKey = 'second-key';
    app.clearSubscribers();
    resolve(new Response(JSON.stringify({data:[{subscriberId:'old'}],next:null,previous:null}),{status:200}));
    await pending;
    expect(app.subscriberStatus()).toBe('idle');
    expect(app.subscribers()).toEqual([]);
  });

  it('loads alert slots only with a restricted credential and clears them on leaving', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null}],hasMore:true,next:{channel:'slack',targetId:'ops'}}),{status:200}));
    const app = TestBed.createComponent(App).componentInstance;
    app.setSection('alerts');
    app.alertKey = 'a'.repeat(64);
    await app.loadAlerts();
    expect(app.alertSlots()).toEqual([{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null}]);
    expect(app.alertsHaveMore()).toBe(true);
    expect(fetchMock).toHaveBeenCalledWith('http://127.0.0.1:3000/v1/experimental/console/alerts',expect.objectContaining({headers:{Authorization:`SwallowConsole ${'a'.repeat(64)}`},credentials:'omit',redirect:'error'}));
    app.setSection('overview');
    expect(app.alertKey).toBe('');
    expect(app.alertSlots()).toEqual([]);
  });

  it('shows attempt identifiers and rejects malformed ones', async () => {
    const id = '1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const body = (attemptId: unknown) => ({data:[{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId}],hasMore:false,next:null});
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify(body(id)),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify(body('bad-id')),{status:200}));
    const fixture = TestBed.createComponent(App);
    const app = fixture.componentInstance;
    app.setSection('alerts');
    app.alertKey = 'a'.repeat(64);
    await app.loadAlerts();
    fixture.detectChanges();
    expect(app.alertSlots()[0].attemptId).toBe(id);
    expect(fixture.nativeElement.textContent).toContain(id);
    await app.loadAlerts();
    expect(app.alertStatus()).toBe('error');
    expect(app.alertSlots()).toEqual([]);
  });

  it('distinguishes absent attempts from an older API response', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'one',state:'idle',lastStartedAt:null,attemptId:null},{channel:'slack',targetId:'two',state:'unknown',lastStartedAt:null}],hasMore:false,next:null}),{status:200}));
    const fixture = TestBed.createComponent(App);
    const app = fixture.componentInstance;
    app.setSection('alerts');
    app.alertKey = 'a'.repeat(64);
    await app.loadAlerts();
    fixture.detectChanges();
    expect(app.alertSlots()[0].attemptId).toBeNull();
    expect(app.alertSlots()[1].attemptId).toBeUndefined();
    expect(fixture.nativeElement.textContent).toContain('API 업데이트 필요');
  });

  it('submits a separately authorized decision once and clears sensitive inputs', async () => {
    const id = '1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:id}],hasMore:false,next:null}),{status:200}));
    let resolve!: (response: Response) => void;
    fetchMock.mockReturnValueOnce(new Promise<Response>(done => { resolve = done; }));
    const app = TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey = 'a'.repeat(64);
    await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]);
    app.decisionProtocol = 'legacy'; app.decisionKey = 'b'.repeat(64); app.decision = 'accepted'; app.decisionReason = 'ticket'; app.decisionConfirmed = true;
    const pending = app.resolveAlert();
    await app.resolveAlert();
    app.setSection('overview');
    expect(app.section()).toBe('alerts');
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[1][0]).toBe('http://127.0.0.1:3000/v1/experimental/console/alerts/resolve');
    expect(fetchMock.mock.calls[1][1]).toEqual(expect.objectContaining({method:'POST',headers:{Authorization:`SwallowConsole ${'b'.repeat(64)}`,'Content-Type':'application/json'},credentials:'omit',redirect:'error',body:JSON.stringify({channel:'slack',targetId:'ops',attemptId:id,decision:'accepted',reason:'ticket'})}));
    resolve(new Response(JSON.stringify({recorded:true}),{status:200}));
    await pending;
    expect(app.decisionStatus()).toBe('recorded');
    expect(app.decisionKey).toBe(''); expect(app.decisionReason).toBe('');
    expect(app.alertSlots()).toEqual([]); expect(app.alertStatus()).toBe('idle');
  });

  it('requires evidence confirmation and treats an interrupted request as unknown', async () => {
    const id = '1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'ops',state:'rejected',lastStartedAt:null,attemptId:id}],hasMore:false,next:null}),{status:200})).mockRejectedValueOnce(new TypeError('network'));
    const app = TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey = 'a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionProtocol = 'legacy'; app.decisionKey = 'b'.repeat(64); app.decisionReason = 'ticket';
    await app.resolveAlert(); expect(fetchMock).toHaveBeenCalledTimes(1);
    app.decisionConfirmed = true; app.decision = 'accepted';
    await app.resolveAlert(); expect(fetchMock).toHaveBeenCalledTimes(1);
    app.decision = 'retry'; await app.resolveAlert();
    expect(fetchMock).toHaveBeenCalledTimes(2); expect(app.decisionStatus()).toBe('unknown');
    expect(app.decisionKey).toBe(''); expect(app.alertSlots()).toEqual([]);
    await app.resolveAlert(); expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('uses a new request ID and can look up its committed result', async () => {
    const attempt = '1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify({recorded:true,replayed:false}),{status:200}));
    const fixture = TestBed.createComponent(App);
    const app = fixture.componentInstance;
    app.setSection('alerts'); app.alertKey = 'a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionKey = 'b'.repeat(64); app.decisionReason = 'verified'; app.decisionConfirmed = true;
    await app.resolveAlert();
    const id = app.decisionRequestId();
    expect(id).toMatch(/^[0-9a-f-]{36}$/);
    expect(fetchMock.mock.calls[1][0]).toBe('http://127.0.0.1:3000/v1/experimental/console/alerts/resolve-v2');
    expect(JSON.parse(fetchMock.mock.calls[1][1]?.body as string)).toEqual({channel:'slack',targetId:'ops',attemptId:attempt,decision:'retry',reason:'verified',requestId:id});
    expect(app.lookupId).toBe(id); expect(app.decisionKey).toBe('');
    fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({found:true,requestId:id,channel:'slack',targetId:'ops',attemptId:attempt,decision:'retry',reason:'verified',createdAt:'2026-01-01T00:00:00Z'}),{status:200}));
    app.lookupKey = 'b'.repeat(64);
    await app.lookupDecisionRequest(); fixture.detectChanges();
    expect(app.lookupStatus()).toBe('found');
    expect(fixture.nativeElement.textContent).toContain('verified');
    expect(fetchMock.mock.calls[2][0]).toContain(`requestId=${id}`);
    app.setSection('overview'); expect(app.lookupKey).toBe(''); expect(app.lookupId).toBe(''); expect(app.decisionRequestId()).toBeNull();
  });

  it('preserves an ambiguous request ID without retrying and treats absence as inconclusive', async () => {
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200})).mockRejectedValueOnce(new TypeError('network')).mockResolvedValueOnce(new Response(JSON.stringify({found:false}),{status:200}));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey='a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionKey='b'.repeat(64); app.decisionReason='investigation'; app.decisionConfirmed=true;
    await app.resolveAlert();
    expect(app.decisionStatus()).toBe('unknown'); expect(app.lookupId).toBe(app.decisionRequestId());
    app.lookupKey='b'.repeat(64); await app.lookupDecisionRequest();
    expect(app.lookupStatus()).toBe('not-found'); expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  it('replays only the identical ambiguous request with the original credential', async () => {
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200})).mockRejectedValueOnce(new TypeError('network')).mockResolvedValueOnce(new Response(JSON.stringify({recorded:true,replayed:true}),{status:200}));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey='a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionKey='b'.repeat(64); app.decisionReason='investigation'; app.decisionConfirmed=true;
    await app.resolveAlert();
    expect(app.decisionStatus()).toBe('unknown');
    app.retryKey='c'.repeat(64); app.retryConfirmed=true; await app.retryDecision();
    expect(fetchMock).toHaveBeenCalledTimes(2); expect(app.decisionStatus()).toBe('unknown'); expect(app.retryKey).toBe('');
    app.retryKey='b'.repeat(64); app.retryConfirmed=true; await app.retryDecision();
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(fetchMock.mock.calls[2][1]?.body).toBe(fetchMock.mock.calls[1][1]?.body);
    expect(app.decisionStatus()).toBe('recorded'); expect(app.retrySnapshot()).toBeNull(); expect(app.retryKey).toBe('');
  });

  it('keeps an ambiguous result after a failed replay and clears it when leaving', async () => {
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200})).mockRejectedValueOnce(new TypeError('network')).mockResolvedValueOnce(new Response('',{status:403}));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey='a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionKey='b'.repeat(64); app.decisionReason='investigation'; app.decisionConfirmed=true;
    await app.resolveAlert(); app.retryKey='b'.repeat(64); app.retryConfirmed=true; await app.retryDecision();
    expect(fetchMock).toHaveBeenCalledTimes(3); expect(app.decisionStatus()).toBe('unknown'); expect(app.retrySnapshot()).not.toBeNull();
    app.setSection('overview'); expect(app.retrySnapshot()).toBeNull(); expect(app.retryKey).toBe('');
  });

  it('fails closed when credential fingerprinting is unavailable', async () => {
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200}));
    vi.spyOn(globalThis.crypto.subtle,'digest').mockRejectedValueOnce(new Error('unavailable'));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey='a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionKey='b'.repeat(64); app.decisionReason='investigation'; app.decisionConfirmed=true;
    await app.resolveAlert();
    expect(fetchMock).toHaveBeenCalledTimes(1); expect(app.decisionStatus()).toBe('unavailable');
    expect(app.decisionRequestId()).toBeNull(); expect(app.retrySnapshot()).toBeNull(); expect(app.decisionKey).toBe('');
  });

  it('does not issue concurrent retries or allow leaving during a retry', async () => {
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200})).mockRejectedValueOnce(new TypeError('network'));
    let finish!: (response: Response) => void;
    fetchMock.mockReturnValueOnce(new Promise<Response>(done => { finish=done; }));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey='a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionKey='b'.repeat(64); app.decisionReason='investigation'; app.decisionConfirmed=true;
    await app.resolveAlert(); app.retryKey='b'.repeat(64); app.retryConfirmed=true;
    const pending=app.retryDecision(); await app.retryDecision(); app.setSection('overview');
    expect(app.section()).toBe('alerts');
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3));
    finish(new Response(JSON.stringify({recorded:true,replayed:true}),{status:200})); await pending;
    expect(app.decisionStatus()).toBe('recorded');
  });

  it('does not downgrade or send when secure request IDs cannot be created', async () => {
    const attempt='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock=vi.spyOn(globalThis,'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{channel:'email',targetId:'ops',state:'unknown',lastStartedAt:null,attemptId:attempt}],hasMore:false,next:null}),{status:200}));
    vi.spyOn(globalThis.crypto,'randomUUID').mockImplementation(() => { throw new Error('unavailable'); });
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.alertKey='a'.repeat(64); await app.loadAlerts();
    app.selectAlert(app.alertSlots()[0]); app.decisionKey='b'.repeat(64); app.decisionReason='ticket'; app.decisionConfirmed=true;
    await app.resolveAlert();
    expect(app.decisionStatus()).toBe('unavailable'); expect(fetchMock).toHaveBeenCalledTimes(1); expect(app.decisionRequestId()).toBeNull();
  });

  it('ignores stale request lookups and clears their credentials', async () => {
    let resolve!: (response: Response) => void;
    vi.spyOn(globalThis,'fetch').mockReturnValueOnce(new Promise<Response>(done => { resolve=done; }));
    const app=TestBed.createComponent(App).componentInstance;
    app.setSection('alerts'); app.lookupKey='b'.repeat(64); app.lookupId='1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const pending=app.lookupDecisionRequest();
    app.setSection('overview');
    resolve(new Response(JSON.stringify({found:false}),{status:200}));
    await pending;
    expect(app.lookupStatus()).toBe('idle'); expect(app.lookupKey).toBe(''); expect(app.lookupId).toBe('');
  });

  it('retrieves only a specified decision history and clears it on leaving', async () => {
    const attempt = '1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify({data:[{id:'1',decision:'retry',reason:'reviewed',createdAt:'2026-01-01T00:00:00Z'}]}),{status:200}));
    const fixture = TestBed.createComponent(App);
    const app = fixture.componentInstance;
    app.setSection('alerts'); app.historyKey = 'b'.repeat(64); app.historyChannel = 'slack'; app.historyTarget = 'ops'; app.historyAttempt = attempt;
    await app.loadDecisionHistory(); fixture.detectChanges();
    expect(app.historyStatus()).toBe('loaded');
    expect(fixture.nativeElement.textContent).toContain('reviewed');
    expect(fetchMock.mock.calls[0][0]).toContain(`channel=slack&targetId=ops&attemptId=${attempt}`);
    expect(fetchMock.mock.calls[0][1]).toEqual(expect.objectContaining({headers:{Authorization:`SwallowConsole ${'b'.repeat(64)}`},credentials:'omit',redirect:'error'}));
    app.setSection('overview');
    expect(app.historyKey).toBe(''); expect(app.historyRows()).toEqual([]);
  });

  it('discards an obsolete history response', async () => {
    let resolve!: (response: Response) => void;
    vi.spyOn(globalThis, 'fetch').mockReturnValueOnce(new Promise<Response>(done => { resolve = done; }));
    const app = TestBed.createComponent(App).componentInstance;
    app.historyKey = 'b'.repeat(64); app.historyTarget = 'ops'; app.historyAttempt = '1889f88e-c0a3-4ab5-b861-2fdc1f26f254';
    const pending = app.loadDecisionHistory();
    app.clearHistory(true);
    resolve(new Response(JSON.stringify({data:[{id:'1',decision:'retry',reason:'old',createdAt:'2026-01-01T00:00:00Z'}]}),{status:200}));
    await pending;
    expect(app.historyStatus()).toBe('idle'); expect(app.historyRows()).toEqual([]); expect(app.historyKey).toBe('');
  });

  it('requests the next alert page and can return to the first', async () => {
    const first = {data:[{channel:'email',targetId:'ops',state:'accepted',lastStartedAt:null}],hasMore:true,next:{channel:'email',targetId:'ops'}};
    const second = {data:[{channel:'slack',targetId:'ops',state:'unknown',lastStartedAt:null}],hasMore:false,next:null};
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(new Response(JSON.stringify(first),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify(second),{status:200})).mockResolvedValueOnce(new Response(JSON.stringify(first),{status:200}));
    const app = TestBed.createComponent(App).componentInstance;
    app.alertKey = 'a'.repeat(64);
    await app.loadAlerts();
    await app.loadAlerts('next');
    expect(fetchMock.mock.calls[1][0]).toContain('afterChannel=email&afterTarget=ops');
    expect(app.alertPage()).toBe(2);
    expect(app.alertSlots()[0].channel).toBe('slack');
    await app.loadAlerts();
    expect(app.alertPage()).toBe(1);
    expect(fetchMock.mock.calls[2][0]).not.toContain('?');
  });

  it('returns to earlier alert pages using prior cursors', async () => {
    const pages = [
      {data:[{channel:'email',targetId:'one',state:'accepted',lastStartedAt:null}],hasMore:true,next:{channel:'email',targetId:'one'}},
      {data:[{channel:'email',targetId:'two',state:'accepted',lastStartedAt:null}],hasMore:true,next:{channel:'email',targetId:'two'}},
      {data:[{channel:'slack',targetId:'three',state:'unknown',lastStartedAt:null}],hasMore:false,next:null},
    ];
    const fetchMock = vi.spyOn(globalThis, 'fetch');
    for (const n of [0,1,2,1,0]) fetchMock.mockResolvedValueOnce(new Response(JSON.stringify(pages[n]),{status:200}));
    const app = TestBed.createComponent(App).componentInstance;
    app.alertKey = 'a'.repeat(64);
    await app.loadAlerts();
    await app.loadAlerts('next');
    await app.loadAlerts('next');
    await app.loadAlerts('previous');
    expect(app.alertPage()).toBe(2);
    expect(fetchMock.mock.calls[3][0]).toContain('afterChannel=email&afterTarget=one');
    await app.loadAlerts('previous');
    expect(app.alertPage()).toBe(1);
    expect(fetchMock.mock.calls[4][0]).not.toContain('?');
  });

  it('does not show an obsolete or unauthorized alert response', async () => {
    let resolve!: (response: Response) => void;
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockReturnValueOnce(new Promise<Response>((done) => { resolve = done; }));
    const app = TestBed.createComponent(App).componentInstance;
    app.alertKey = 'a'.repeat(64);
    const pending = app.loadAlerts();
    app.forgetAlertKey();
    resolve(new Response(JSON.stringify({data:[{channel:'email',targetId:'old',state:'unknown',lastStartedAt:null}],hasMore:false,next:null}),{status:200}));
    await pending;
    expect(app.alertStatus()).toBe('idle');
    expect(app.alertSlots()).toEqual([]);
    fetchMock.mockResolvedValueOnce(new Response('',{status:401}));
    app.alertKey = 'b'.repeat(64);
    await app.loadAlerts();
    expect(app.alertStatus()).toBe('error');
    expect(app.alertError()).toContain('권한');
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
