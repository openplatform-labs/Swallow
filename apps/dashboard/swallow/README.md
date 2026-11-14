# Swallow 관리 콘솔

Angular 22와 Clarity 18 기반의 초기 관리 화면이다. 현재 개요·메뉴와 Rust API 준비 상태 확인만 동작한다. 관리자 로그인·권한, 워크플로·구독자·채널 관리 및 발송 이력은 연결되지 않았으며 운영 제품이 아니다. API 키를 브라우저에 저장하거나 테스트 데이터를 실제 운영 데이터처럼 표시하지 않는다.

Node.js 22.22.3 이상 22.x 또는 Angular 22가 지원하는 Node 버전과 npm이 필요하다. 이 디렉터리에서 `npm ci`, `npm start`를 실행하면 기본 `http://localhost:4200`에서 개발 서버가 시작된다. `npm run build`는 프로덕션 번들을 만들고 `npm test -- --watch=false`는 단위 테스트를 실행한다. `package-lock.json`은 이 독립 앱의 의존성을 고정한다. Clarity 18.4는 Angular 22를 지원하며, Clarity가 사용하는 `@angular/animations`는 Angular에서 deprecated된 의존성이므로 향후 교체를 확인해야 한다.

API 준비 상태 확인에는 Rust API의 `SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true`와 화면의 정확한 origin을 `SWALLOW_CORS_ALLOWED_ORIGINS`에 지정해야 한다. 예를 들어 `http://127.0.0.1:4200`에서 화면을 열 때 같은 값을 허용한다. `localhost`와 `127.0.0.1`은 다른 origin이다. 준비 상태는 DB의 순간적인 연결 확인이며 인증, 스키마, 채널 발송이나 전체 서비스의 정상 상태를 보증하지 않는다. 서버 health API는 인증되지 않으므로 외부 네트워크 접근을 제한해야 한다. 이 화면은 API 주소를 저장하지 않으며 원격 API에는 HTTPS만 허용한다.

원본 Novu React 대시보드는 상위 디렉터리에 별도로 남아 있다. 이 앱이 원본 Dashboard와 동일한 UI·기능·API 계약이라고 주장하지 않는다.
