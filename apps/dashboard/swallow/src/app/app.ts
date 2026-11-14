import { Component, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ClrAlertModule } from '@clr/angular/emphasis/alert';

type ConnectionState = 'unchecked' | 'checking' | 'ready' | 'unavailable';
type Section = 'overview' | 'workflows' | 'subscribers' | 'channels' | 'activity' | 'settings';

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
    { id: 'settings', label: '설정', short: 'ST' },
  ];
  readonly section = signal<Section>('overview');
  readonly status = signal<ConnectionState>('unchecked');
  readonly checkedAt = signal<string | null>(null);
  readonly detail = signal('');
  apiOrigin = 'http://127.0.0.1:3000';
  private requestId = 0;

  get sectionName(): string {
    return this.sections.find((item) => item.id === this.section())?.label ?? '';
  }

  connectionChanged(): void {
    this.requestId++;
    this.status.set('unchecked');
    this.checkedAt.set(null);
    this.detail.set('');
  }

  async checkConnection(): Promise<void> {
    const id = ++this.requestId;
    this.status.set('checking');
    this.detail.set('');
    this.checkedAt.set(null);
    try {
      const origin = new URL(this.apiOrigin.trim());
      if (!['http:', 'https:'].includes(origin.protocol) || origin.username || origin.password ||
          origin.pathname !== '/' || origin.search || origin.hash) {
        throw new Error('주소는 경로 없이 http 또는 https 출처로 입력해 주세요.');
      }
      if (origin.protocol === 'http:' && !['localhost', '127.0.0.1', '[::1]'].includes(origin.hostname)) {
        throw new Error('원격 서버에는 HTTPS 주소를 사용해 주세요.');
      }
      const response = await fetch(`${origin.origin}/health/ready`, {
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
