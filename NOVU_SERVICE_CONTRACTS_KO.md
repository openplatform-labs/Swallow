# Novu 서비스·SDK 계약과 Rust 재구현 기준

> 기준: `novuhq/novu`의 `next`, 커밋 `895388f37efe9e06de0aa333e5727303e6ad6853`. 정적 코드 분석이며 모든 설정·버전·배포 모드를 실행 검증한 명세는 아니다. 100% 호환을 선언하기 위한 테스트 목록의 출발점으로 유지한다.

## 패키지와 서비스 경계

사용자가 제공한 [GitHub Packages 목록](https://github.com/orgs/novuhq/packages?repo_name=novu)은 배포된 패키지 목록이며 그 자체가 의존성 그래프나 현재 서비스 집합을 증명하지 않는다. 해당 화면의 `web`, `widget`, `embed`, `sdk`, `base`를 기준 커밋에서 모두 필수 서비스로 간주하지 않는다. 이미지 태그와 소스 커밋의 대응도 별도로 확인해야 한다.

기준 커밋의 [Community Compose](docker/community/docker-compose.yml)는 `api`, `worker`, `ws`, `dashboard`, MongoDB, Redis를 기동한다. Compose 이미지 태그 `3.19.0`은 이 체크아웃 커밋과 같다는 증거가 아니다. 코드 기준 구성은 다음과 같다.

```text
서비스 애플리케이션 ── API key ──> API ── 워크플로 큐 ──> worker
                                                     ├─ 수신자 처리·단계 작업
                                                     ├─ provider 발송
                                                     └─ 인앱 메시지 저장 → WS 큐 → WS
제품의 Inbox SDK ── 세션 발급 ──> API                    │
      ├─ subscriber JWT ──> Inbox API                    │
      └─ subscriber JWT ──> Socket.IO <──────────────────┘
관리자 ──> dashboard ──> API
```

이는 경계와 대표 흐름을 그린 것으로 모든 feature flag, SQS 경로, Cloud 전용 대체 구성이나 에이전트 기능을 포함하지 않는다. [`packages/js`](packages/js/CONTEXT.md)는 프레임워크 중립 Inbox 엔진이며 [`packages/react`](packages/react)·[`packages/nextjs`](packages/nextjs)는 프레임워크 통합이다. JS SDK는 설정에 따라 Socket.IO 또는 PartySocket을 선택하므로 둘의 프로토콜을 혼동하지 않는다. [socket-factory.ts](packages/js/src/ws/socket-factory.ts)

## 이벤트 API

[`events.controller.ts`](apps/api/src/app/events/events.controller.ts)는 `POST /v1/events/trigger`에서 `name`, `to`, `payload`, `transactionId` 등을 받아 유스케이스에 전달한다. 외부 API 키 접근과 `EVENT_WRITE` 권한을 선언한다. 성공 응답은 [응답 인터셉터](apps/api/src/app/shared/framework/response.interceptor.ts)에 의해 `data`로 감싼다. 수신자 객체·topic·actor·tenant·context·bridge·첨부·설정 override 등은 별도 계약과 유효성 검증이 필요하다.

[처리 유스케이스](apps/api/src/app/events/usecases/parse-event-request/parse-event-request.usecase.ts)는 일반 저장 워크플로에서 존재 여부 및 선택적 payload 검증, tenant, 활성화 여부, 단계 존재를 확인한 뒤 큐에 전달한다. `workflow_not_found`는 422, 비활성·단계 없음 등은 `acknowledged`와 상태를 반환한다. 트리거가 `processed`인 것은 처리 접수이며 최종 채널 전달 성공을 뜻하지 않는다. 세부 오류·검증 응답, 중복 동작, 보존 기간은 실제 통합 테스트로 확인해야 한다.

## 인증은 서로 다른 경계다

- **사용자/서버 API:** [Community 가드](apps/api/src/app/auth/framework/community.user.auth.guard.ts)는 `Authorization`의 `ApiKey` 또는 `Bearer` 스킴을 구분한다. API 키는 [전략](apps/api/src/app/auth/services/passport/apikey.strategy.ts)과 [서비스](apps/api/src/app/auth/services/community.auth.service.ts)에서 SHA-256 해시로 환경 키를 조회하고 연결된 사용자도 확인한다. 해당 Community 구현은 API 키 사용자에 전체 권한을 부여한다. 엔드포인트 접근 가능 여부와 권한 검사는 별도로 적용되며 다른 에디션과 경로의 정책은 추가 확인해야 한다.
- **Inbox 세션:** [`POST /inbox/session`](apps/api/src/app/inbox/inbox.controller.ts)은 application identifier, subscriber, 선택적 subscriber/context HMAC 등을 입력받는다. [세션 유스케이스](apps/api/src/app/inbox/usecases/session/session.usecase.ts)는 환경과 활성 인앱 integration 확인, HMAC 설정에 따른 검증, 구독자·context 처리와 응답 생성을 수행한다. [요청](apps/api/src/app/inbox/dtos/subscriber-session-request.dto.ts), [응답](apps/api/src/app/inbox/dtos/subscriber-session-response.dto.ts)의 필드는 별도 계약이다.
- **구독자 API/WS:** 세션 토큰은 `aud=widget_user`, `iss=novu_api`, 내부 구독자 ID·환경·조직·외부 subscriberId·contextKeys 등을 포함한다. 만료 기본값은 15일이다. [토큰 생성](apps/api/src/app/auth/services/community.auth.service.ts), [subscriber JWT 전략](apps/api/src/app/auth/services/passport/subscriber-jwt.strategy.ts). Inbox 리소스의 HTTP 가드는 bearer 토큰을 사용하며 구독자를 다시 검증한다. WS 연결은 JWT 검증 및 audience 확인을 수행한다. 토큰 발급과 HMAC 검증을 생략한 개발용 인증을 호환 구현으로 취급하면 안 된다.

## Inbox API와 SDK

[Inbox 컨트롤러](apps/api/src/app/inbox/inbox.controller.ts)는 세션 외에 `/inbox/notifications`, `/notifications/count`, `/preferences`, 읽음·보관·snooze 및 여러 일괄 작업을 제공한다. 알림 목록은 limit, offset/after, tags, read, archived, snoozed, seen, data, severity, 생성 시각 및 context 필터를 받는다. 정확한 DTO, 응답·커서·정렬·오류는 각 엔드포인트의 DTO와 e2e를 기준으로 검증해야 한다.

[InboxService](packages/js/src/api/inbox-service.ts)는 세션 응답 토큰을 HTTP 클라이언트에 설정하고, [HTTP 클라이언트](packages/js/src/api/http-client.ts)는 `Bearer` 인증을 사용한다. 따라서 단순히 메시지 테이블에 행을 넣는 것만으로 SDK 호환성이 생기지 않는다.

## 큐와 작업 계약

[큐 이름](packages/shared/src/config/job-queue.ts)은 `trigger-handler`, `process-subscriber`, `standard`, `ws_socket_queue` 등으로 구분된다. 실제 backend는 설정에 따라 `bullmq`, `sqs_bullmq`, `sqs` 경로가 있다. 큐 이름과 DTO는 기존 큐를 공유하거나 무중단 마이그레이션할 때 특히 중요하다. PostgreSQL 단독 재구현에서는 물리적인 큐 포맷을 유지할 필요가 있는지 이행 계획에서 결정하되, 외부에서 관찰되는 의미와 장애 동작은 보존·검증해야 한다.

[워크플로 워커](apps/worker/src/app/workflow/services/workflow.worker.ts)와 [수신자 워커](apps/worker/src/app/workflow/services/subscriber-process.worker.ts)의 주석은 해당 단계의 실패 시 at-most-once 정책을 설명한다. [standard 워커](apps/worker/src/app/workflow/services/standard.worker.ts)는 실패 유형에 따른 별도 재시도와 backoff 경로를 둔다. 하나의 전역 재시도 설정으로 이를 대체하면 동작이 달라질 수 있다. 지연·digest·취소·동시성·중복·재시도·크래시 복구는 단계별 원본 동작에 대한 테스트가 필요하다.

## Socket.IO 계약

[서버 gateway](apps/ws/src/socket/ws.gateway.ts)는 handshake의 `auth.token` 또는 `query.token`을 받아 JWT를 확인하고 내부 구독자 ID의 room에 연결한다. contextKeys를 소켓 데이터에 보관하며 메시지 이벤트는 contextKeys의 길이와 구성에 따라 필터링한다. 다중 WS 노드에는 [Socket.IO Redis adapter](apps/ws/src/shared/framework/in-memory-io.adapter.ts)가 사용된다. Redis를 제거한다면 같은 격리·fan-out·복구 결과를 다른 수단으로 검증해야 한다.

[SDK의 Socket.IO 클라이언트](packages/js/src/ws/socket.ts)는 websocket transport와 `query.token`을 사용하며 아래 이벤트를 처리한다. **Socket.IO는 임의의 raw WebSocket JSON 프로토콜과 같지 않다.**

| 이벤트 | 클라이언트가 소비하는 주요 payload |
|---|---|
| `notification_received` | `{ message }`: `_id`, `content`, `subscriber`, `cta`, 상태, 시각 등 SDK 매핑에 필요한 메시지 |
| `unread_count_changed` | `{ counts: { total, severity }, unreadCount, hasMore }` |
| `unseen_count_changed` | `{ unseenCount, hasMore }` |
| `agent_event` | agent event envelope; 별도 프로토콜 범위 확인 필요 |

원본 인앱 워커는 WS 큐에 `messageId`를 넣는다. WS 계층은 메시지를 조회·변환해 클라이언트로 보낼 수 있으므로 큐 내부 payload와 Socket.IO 최종 payload를 동일시하지 않는다. [인앱 발송](apps/worker/src/app/workflow/usecases/send-message/send-message-in-app.usecase.ts), [WS 작업 DTO](libs/application-generic/src/dtos/web-sockets-job.dto.ts), [socket worker](libs/application-generic/src/services/socket-worker/socket-worker.service.ts).

## Rust 구현과 호환성 검증 상태

현재 구현은 별도 실험용 API·worker 및 PostgreSQL 스키마다. 공통의 제한된 트리거 판단, 템플릿 기능, 내부 실험용 작업 이름을 `apps/api/rust-contracts`로 분리하여 API의 실행 시 워커 crate 의존을 제거했다. Socket.IO 이벤트 이름과 context 필터 함수도 공통 계약에 추가했으며, context 함수는 체크아웃한 원본 gateway의 함수를 직접 실행해 제한된 입력을 비교했다. 이는 **Rust 내부 의존 방향 정리**이며 원본의 모든 DTO나 모듈을 재현한 것은 아니다. API의 worker 의존은 통합 테스트에만 남는다.

실험용 API 키는 환경과 사용자 식별자를 보관하고, 키 소유 사용자가 없으면 `User not found`로 거절한다. 이는 Community API 키 인증의 일부에 대한 구현이다. 사용자 JWT 검증은 미구현으로 모든 Bearer 요청을 거절하며, 구성원·역할과 다른 에디션의 권한 정책은 구현하지 않았다. 키의 `enabled` 필드는 실험용 모델의 자체 기능으로 원본과 동일한 폐기 모델을 뜻하지 않는다.

부분 확인: PostgreSQL 큐 및 단일 인앱 경로, 문자열 수신자 검증/중복 제거, 제한된 Handlebars 치환. 미구현 또는 미검증: 전체 API·인증·권한·Inbox·Socket.IO·실시간 다중 노드·provider·workflow 기능과 실제 원본 서버 대 Rust 서버 종단 간 비교. 실험용 큐 이름은 원본 `trigger-handler` 등의 이름이나 DTO와 호환되는 것으로 간주하지 않는다.

후속 작업에서 원본과 동일한 커밋의 실행 환경을 구축해 API/SDK 시나리오, 인증 거부와 격리, 각 큐의 실패·재시도, Socket.IO 연결·이벤트·context 필터, 사용자 설정 및 채널 발송을 비교한다. 기준 커밋과 배포 이미지의 대응이 확인되지 않은 상태에서 다른 버전의 이미지를 동등한 비교 기준으로 쓰지 않는다. 테스트별 성공/실패와 미지원 동작을 기록하고 100% 호환 여부를 전체 범위의 결과로 판단한다.
