import { Component, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ClrAlertModule } from '@clr/angular/emphasis/alert';

type ConnectionState = 'unchecked' | 'checking' | 'ready' | 'unavailable';
type Subscriber = { id: string; firstName: string; lastName: string; email: string };
type AlertSlot = { channel: string; targetId: string; state: string; lastStartedAt: string | null };
type Section = 'overview' | 'workflows' | 'subscribers' | 'channels' | 'activity' | 'alerts' | 'settings';

@Component({
  imports: [FormsModule, ClrAlertModule],
  selector: 'app-root',
  styleUrl: './app.css',
  templateUrl: './app.html',
})
export class App {
  readonly sections: { id: Section; label: string; short: string }[] = [
    { id: 'overview', label: '개요', short: 'OV' },
    { id: 'workflows', label: '워크플로', short: 'WF' },
    { id: 'subscribers', label: '구독자', short: 'SU' },
    { id: 'channels', label: '발송 채널', short: 'CH' },
    { id: 'activity', label: '발송 이력', short: 'AC' },
    { id: 'alerts', label: '운영 경보', short: 'AL' },
    { id: 'settings', label: '설정', short: 'ST' },
  ];
  readonly section = signal<Section>('overview');
  readonly status = signal<ConnectionState>('unchecked');
  readonly checkedAt = signal<string | null>(null);
  readonly detail = signal('');
  readonly subscribers = signal<Subscriber[]>([]);
  readonly subscriberStatus = signal<'idle' | 'loading' | 'loaded' | 'error'>('idle');
  readonly subscriberError = signal('');
  readonly nextCursor = signal<string | null>(null);
  readonly previousCursor = signal<string | null>(null);
  readonly alertSlots = signal<AlertSlot[]>([]);
  readonly alertStatus = signal<'idle' | 'loading' | 'loaded' | 'error'>('idle');
  readonly alertError = signal('');
  readonly alertsHaveMore = signal(false);
  alertKey = '';
  private alertRequestId = 0;
  private alertAbort: AbortController | null = null;
  apiOrigin = 'http://127.0.0.1:3000';
  apiKey = '';
  credentialMode: 'restricted' | 'api-key' = 'restricted';
  subscriberName = '';
  private subscriberRequestId = 0;
  private subscriberAbort: AbortController | null = null;
  private requestId = 0;

  get sectionName(): string {
    return this.sections.find((item) => item.id === this.section())?.label ?? '';
  }

  setSection(next: Section): void {
    if (this.section() === 'subscribers' && next !== 'subscribers') this.forgetKey();
    if (this.section() === 'alerts' && next !== 'alerts') this.forgetAlertKey();
    this.section.set(next);
  }

  connectionChanged(): void {
    this.apiKey = '';
    this.clearSubscribers();
    this.forgetAlertKey();
    this.requestId++;
    this.status.set('unchecked');
    this.checkedAt.set(null);
    this.detail.set('');
  }

  clearSubscribers(): void {
    this.subscriberRequestId++;
    this.subscriberAbort?.abort();
    this.subscriberAbort = null;
    this.subscribers.set([]);
    this.nextCursor.set(null);
    this.previousCursor.set(null);
    this.subscriberStatus.set('idle');
    this.subscriberError.set('');
  }

  forgetKey(): void {
    this.apiKey = '';
    this.clearSubscribers();
  }

  clearAlerts(): void {
    this.alertRequestId++;
    this.alertAbort?.abort();
    this.alertAbort = null;
    this.alertSlots.set([]);
    this.alertsHaveMore.set(false);
    this.alertStatus.set('idle');
    this.alertError.set('');
  }

  forgetAlertKey(): void {
    this.alertKey = '';
    this.clearAlerts();
  }

  async loadAlerts(): Promise<void> {
    const id = ++this.alertRequestId;
    this.alertAbort?.abort();
    const controller = new AbortController();
    this.alertAbort = controller;
    this.alertSlots.set([]);
    this.alertsHaveMore.set(false);
    this.alertError.set('');
    this.alertStatus.set('loading');
    try {
      const origin = this.validatedOrigin();
      const key = this.alertKey.trim();
      if (!/^[0-9a-f]{64}$/.test(key)) throw new Error('인증 값을 확인해 주세요.');
      const response = await fetch(`${origin}/v1/experimental/console/alerts`, {
        method: 'GET', headers: { Authorization: `SwallowConsole ${key}` },
        cache: 'no-store', credentials: 'omit', redirect: 'error',
        signal: AbortSignal.any([controller.signal, AbortSignal.timeout(10000)]),
      });
      if (response.status === 401 || response.status === 403) throw new Error('인증 값 또는 경보 조회 권한을 확인해 주세요.');
      if (!response.ok) throw new Error('경보 상태를 가져오지 못했습니다.');
      const body: unknown = await response.json();
      if (typeof body !== 'object' || body === null || !('data' in body) || !Array.isArray(body.data) || !('hasMore' in body) || typeof body.hasMore !== 'boolean' || body.data.length > 100) throw new Error('API 응답 형식을 확인해 주세요.');
      const rows: AlertSlot[] = body.data.map((row: unknown) => {
        if (typeof row !== 'object' || row === null || !('channel' in row) || typeof row.channel !== 'string' || !('targetId' in row) || typeof row.targetId !== 'string' || !('state' in row) || typeof row.state !== 'string' || !('lastStartedAt' in row) || !(typeof row.lastStartedAt === 'string' || row.lastStartedAt === null)) throw new Error('API 응답 형식을 확인해 주세요.');
        return { channel: row.channel, targetId: row.targetId, state: row.state, lastStartedAt: row.lastStartedAt };
      });
      if (id !== this.alertRequestId) return;
      this.alertSlots.set(rows);
      this.alertsHaveMore.set(body.hasMore);
      this.alertStatus.set('loaded');
    } catch (error: unknown) {
      if (id !== this.alertRequestId) return;
      this.alertStatus.set('error');
      this.alertError.set(error instanceof Error && ['주소', '원격', '인증', '경보', 'API '].some(prefix => error.message.startsWith(prefix)) ? error.message : '서버에 연결할 수 없습니다.');
    } finally {
      if (id === this.alertRequestId) this.alertAbort = null;
    }
  }

  private validatedOrigin(): string {
    const origin = new URL(this.apiOrigin.trim());
    if (!['http:', 'https:'].includes(origin.protocol) || origin.username || origin.password ||
        origin.pathname !== '/' || origin.search || origin.hash) {
      throw new Error('주소는 경로 없이 http 또는 https 출처로 입력해 주세요.');
    }
    if (origin.protocol === 'http:' && !['localhost', '127.0.0.1', '[::1]'].includes(origin.hostname)) {
      throw new Error('원격 서버에는 HTTPS 주소를 사용해 주세요.');
    }
    return origin.origin;
  }

  async loadSubscribers(direction?: 'after' | 'before'): Promise<void> {
    const cursor = direction === 'after' ? this.nextCursor() : direction === 'before' ? this.previousCursor() : null;
    if (direction && !cursor) return;
    const id = ++this.subscriberRequestId;
    this.subscriberAbort?.abort();
    const controller = new AbortController();
    this.subscriberAbort = controller;
    this.subscriberStatus.set('loading');
    this.subscriberError.set('');
    this.subscribers.set([]);
    this.nextCursor.set(null);
    this.previousCursor.set(null);
    try {
      const origin = this.validatedOrigin();
      const key = this.apiKey.trim();
      if (this.credentialMode === 'restricted' ? !/^[0-9a-f]{64}$/.test(key) : !key || /[^\x21-\x7e]/.test(key)) throw new Error('인증 값을 확인해 주세요.');
      const params = new URLSearchParams({ limit: '20' });
      const name = this.subscriberName.trim();
      if (name) params.set('name', name);
      if (direction && cursor) params.set(direction, cursor);
      const endpoint = this.credentialMode === 'restricted' ? '/v1/experimental/console/subscribers' : '/v2/subscribers';
      const scheme = this.credentialMode === 'restricted' ? 'SwallowConsole' : 'ApiKey';
      const response = await fetch(`${origin}${endpoint}?${params}`, {
        method: 'GET', headers: { Authorization: `${scheme} ${key}` },
        cache: 'no-store', credentials: 'omit', redirect: 'error',
        signal: AbortSignal.any([controller.signal, AbortSignal.timeout(10000)]),
      });
      if (response.status === 401 || response.status === 403) throw new Error('인증 값 또는 권한을 확인해 주세요.');
      if (!response.ok) throw new Error('구독자 목록을 가져오지 못했습니다.');
      const data: unknown = await response.json();
      if (typeof data !== 'object' || data === null || !('data' in data) || !Array.isArray(data.data) ||
          !('next' in data) || !(data.next === null || typeof data.next === 'string') ||
          !('previous' in data) || !(data.previous === null || typeof data.previous === 'string')) {
        throw new Error('API 응답 형식을 확인해 주세요.');
      }
      const rows: Subscriber[] = data.data.map((value: unknown) => {
        if (typeof value !== 'object' || value === null || !('subscriberId' in value) || typeof value.subscriberId !== 'string') {
          throw new Error('API 응답 형식을 확인해 주세요.');
        }
        const get = (key: string): string => key in value && typeof (value as Record<string, unknown>)[key] === 'string' ? (value as Record<string, string>)[key] : '';
        return { id: value.subscriberId, firstName: get('firstName'), lastName: get('lastName'), email: get('email') };
      });
      if (id !== this.subscriberRequestId) return;
      this.subscribers.set(rows);
      this.nextCursor.set(data.next);
      this.previousCursor.set(data.previous);
      this.subscriberStatus.set('loaded');
    } catch (error: unknown) {
      if (id !== this.subscriberRequestId) return;
      this.subscriberStatus.set('error');
      this.subscriberError.set(error instanceof Error && (error.message.startsWith('주소') || error.message.startsWith('원격') || error.message.startsWith('API ') || error.message.startsWith('구독자') || error.message.startsWith('인증')) ? error.message : '서버에 연결할 수 없습니다.');
    } finally {
      if (id === this.subscriberRequestId) this.subscriberAbort = null;
    }
  }

  async checkConnection(): Promise<void> {
    const id = ++this.requestId;
    this.status.set('checking');
    this.detail.set('');
    this.checkedAt.set(null);
    try {
      const origin = this.validatedOrigin();
      const response = await fetch(`${origin}/health/ready`, {
        cache: 'no-store', credentials: 'omit', signal: AbortSignal.timeout(5000),
      });
      const body: unknown = await response.json();
      if (!response.ok || typeof body !== 'object' || body === null || !('status' in body) || body.status !== 'ready') {
        throw new Error('API가 준비 상태를 반환하지 않았습니다.');
      }
      if (id !== this.requestId) return;
      this.status.set('ready');
      this.checkedAt.set(new Intl.DateTimeFormat('ko-KR', { hour: '2-digit', minute: '2-digit', second: '2-digit' }).format(new Date()));
    } catch (error: unknown) {
      if (id !== this.requestId) return;
      this.status.set('unavailable');
      this.detail.set(error instanceof Error && (error.message.startsWith('주소') || error.message.startsWith('원격')) ? error.message : 'API에 연결할 수 없거나 준비 상태가 아닙니다.');
    }
  }
}
