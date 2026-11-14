import { bootstrapApplication } from '@angular/platform-browser';
import { appConfig } from './app/app.config';
import { App } from './app/app';
import { oidcPopupCallback } from './app/operator-oidc';

if (window.location.pathname === '/oidc-callback') {
  oidcPopupCallback().then(() => window.history.replaceState(null, '', '/oidc-callback')).catch(() => { document.body.textContent = '로그인 응답을 처리하지 못했습니다.'; });
} else {
  bootstrapApplication(App, appConfig).catch((err) => console.error(err));
}
