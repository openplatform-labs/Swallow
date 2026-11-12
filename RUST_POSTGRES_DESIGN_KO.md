# Novu 기반 Rust·PostgreSQL 재구현 검토

> 상태: **초기 제안 및 분석 기록**. 아래의 제안과 변경 기록은 작성 당시의 내용이며 현재 구현을 나타내지 않는다. Novu 호환 구현과 데이터 이전은 완료되지 않았다.
> 작성: 2026-11-12 · 원본 저장소: https://github.com/novuhq/novu · 분석 기준: `next` 브랜치, 커밋 `895388f37efe9e06de0aa333e5727303e6ad6853` (2026-11-11).

> 구현되지 않은 기능과 검증하지 않은 범위는 [구현·검증 현황](RUST_IMPLEMENTATION_STATUS_KO.md)을 참조한다.

## 1. 배경과 목적

Novu를 Rust로 재작성하면서 기존 MongoDB와 Redis 의존성을 PostgreSQL 중심의 구조로 바꾸는 방안을 검토한다. 이 문서는 후속 구현자가 **왜 구조를 바꾸려는지, 원본의 어떤 동작을 보존하거나 재결정해야 하는지, 변경을 어떻게 검증해야 하는지** 추적하기 위한 출발점이다.

요구사항은 PostgreSQL을 사용하는 Rust 재구현이며 **Novu와 100% 호환**이 목표다. 기준은 우선 위에 고정한 원본 커밋이다. API뿐 아니라 SDK, 워크플로, 오류, 인증, 실시간 동작과 에디션별 기능까지 차이를 확인해야 한다. 목표 처리량, 응답 지연, 가용성, 기존 데이터 이전 여부 및 별도 엔터프라이즈 소스 접근 범위는 아직 정해지지 않았다. 아래 아키텍처는 이러한 미결 사항에 종속된 **초기 제안**이며 확정된 구현 계약이 아니다.

## 2. 핵심 판단

- MongoDB와 Redis가 반드시 필요한 것은 아니다. PostgreSQL과 Rust로 핵심 알림 서비스를 구현할 수 있다.
- 영속 데이터는 일반 관계형 테이블을 중심으로 하고, 구조가 유동적인 설정과 payload에 `JSONB`를 선택적으로 사용한다.
- Redis의 큐, 지연 실행, 중복 제어, 캐시, 서버 간 전달은 단순한 데이터 저장 이상의 동작이다. 테이블로 옮기는 것과 해당 동작을 재현하는 것은 별개의 일이다.
- 초기 제안은 PostgreSQL을 공유하는 Rust API, 워커, 실시간 서버다. 실제 부하·가용성 목표를 확인한 뒤 큐, 캐시 또는 분석 저장소를 분리할 수 있도록 경계를 유지한다.
- `LISTEN/NOTIFY`는 깨우기 신호로 사용하고, 복구에 필요한 정보는 영속 테이블에 둔다.
- PostgreSQL 기본 기능으로 출발한다. 파티셔닝, advisory lock, 확장 기능, 별도 인프라는 요구사항과 측정 결과에 따라 도입한다.

## 3. 원본에서 확인한 구조와 분석 한계

원본은 TypeScript 중심 pnpm/Nx 모노레포다. 주요 영역은 다음과 같다.

| 구성 요소 | 역할 및 근거 |
|---|---|
| API | 이벤트 트리거 수신: [`events.controller.ts`](apps/api/src/app/events/events.controller.ts). 요청 처리는 [`parse-event-request.usecase.ts`](apps/api/src/app/events/usecases/parse-event-request/parse-event-request.usecase.ts)에서 워크플로 큐에 전달된다. |
| 워크플로 및 수신자 처리 | [`workflow.worker.ts`](apps/worker/src/app/workflow/services/workflow.worker.ts), [`subscriber-process.worker.ts`](apps/worker/src/app/workflow/services/subscriber-process.worker.ts), [`trigger-multicast.usecase.ts`](libs/application-generic/src/usecases/trigger-multicast/trigger-multicast.usecase.ts). |
| 채널 발송 | [`send-message`](apps/worker/src/app/workflow/usecases/send-message) 아래에 인앱, 이메일, SMS, 푸시, 채팅 등 채널별 처리가 있다. |
| 큐 | [`queue-base.service.ts`](libs/application-generic/src/services/queues/queue-base.service.ts)에 BullMQ 및 SQS 관련 경로가 있다. [`standard-queue.service.ts`](libs/application-generic/src/services/queues/standard-queue.service.ts)는 지연과 작업 ID에 관한 동작을 설명한다. |
| 실시간 통신 | [`in-memory-io.adapter.ts`](apps/ws/src/shared/framework/in-memory-io.adapter.ts)는 Socket.IO Redis 어댑터를 사용한다. |
| 클라이언트 | [`packages/js`](packages/js)와 [`packages/react`](packages/react)는 Inbox SDK와 React 연동을 제공한다. |
| 배포 | [`docker/community/docker-compose.yml`](docker/community/docker-compose.yml)은 MongoDB, Redis, API, 워커, WS, 대시보드를 포함한다. ClickHouse 관련 코드도 저장소에 있으나 이 Compose에 포함되어 있지는 않다. |

이번 검토는 정적 소스 분석이다. 전체 기능, 설정 조합, 런타임 동작, 성능과 운영 안정성을 검증하지 않았다. 이 클론은 shallow clone이며 엔터프라이즈 서브모듈 `.source`는 초기화되지 않았다. 원본은 Open Core 구조로, 적용되는 라이선스와 사용할 기능 범위는 별도 확인이 필요하다. [원본 저장소](https://github.com/novuhq/novu), [`.gitmodules`](.gitmodules).

### 대표 처리 흐름

```text
클라이언트 → 트리거 API → 워크플로 큐 → 워크플로 검증
           → 수신자/topic 확장 → 수신자별 작업 → 단계별 발송
           → provider 또는 Inbox → 실시간 클라이언트 갱신
```

이는 이해를 위한 개략도이며 모든 설정과 예외 경로를 표현하지 않는다. 원본을 업데이트하거나 기능을 구현할 때는 해당 커밋에서 실제 코드와 테스트를 다시 확인한다.

## 4. PostgreSQL 모델링 원칙

초기 후보 엔터티는 `organizations`, `environments`, `subscribers`, `topics`, `topic_subscriptions`, `workflows`, `workflow_steps`, `integrations`, `notifications`, `jobs`, `preferences`다. 명칭과 관계는 확정 스키마가 아니다.

- 테넌트 경계, 식별자, 상태, 시각, 외래키, 빈번한 검색 조건은 명시적인 컬럼으로 표현한다.
- 단계별 설정, 가변 이벤트 payload, provider별 속성 등은 `JSONB`를 검토한다. 자주 조회하거나 유일성·무결성을 보장해야 하는 값은 별도 컬럼을 우선한다.
- 수신자 식별자의 유일성은 예컨대 `(environment_id, subscriber_id)`처럼 실제 범위에 맞춰 강제한다. 동일 환경을 요구하는 관계에는 가능하면 환경 ID를 포함한 복합 외래키를 사용한다.
- 모든 읽기와 변경에서 조직 또는 환경 범위를 적용한다. PostgreSQL 행 수준 보안은 추가 방어층 후보이나, 도입 시 연결 역할과 풀링을 포함해 별도 설계·검증한다.
- 외부 식별자, NULL과 누락 필드의 차이, 시간대, 정렬 및 페이지네이션, 배열·중첩 객체, MongoDB ObjectId 변환을 호환성 검토 항목에 포함한다.
- `JSONB`는 입력 JSON의 원래 공백, 키 순서 및 중복 키를 그대로 보존하는 수단이 아니다. 바이트 단위 보존이 계약인 데이터가 있는지 확인한다.

## 5. 작업 큐와 전달 보장

### 제안 구조

`jobs` 테이블에 작업 유형, payload, 상태, `available_at`, 시도 횟수, 소유 워커, 임대 만료 시각, 오류 정보와 멱등성 키 등을 둔다. 대기 중이고 실행 시각이 된 행을 워커가 짧은 트랜잭션에서 `FOR UPDATE SKIP LOCKED`로 선점하고, 소유권 및 임대 정보를 기록한 뒤 커밋한다. 외부 provider 호출은 트랜잭션 밖에서 수행한다.

완료·실패·임대 연장은 **현재 소유권을 가진 실행만** 반영해야 한다. 만료 후 다른 워커가 선점한 작업을 이전 워커가 늦게 완료하는 상황을 막기 위해 실행마다 바뀌는 lease token 또는 세대 번호를 조건에 포함한다. 장시간 작업의 임대 연장, 워커 종료 후 만료 작업 회수, 시간 동기화 및 DB 기준 시각을 설계한다.

`SKIP LOCKED`는 대기 중인 다른 작업을 처리하도록 돕지만 엄격한 FIFO, 공정성 또는 우선순위를 보장하지 않는다. 오래 대기하는 작업의 탐지, 환경별 공정성 및 배치 크기는 부하에 맞춰 검증한다. [PostgreSQL SELECT 문서](https://www.postgresql.org/docs/current/sql-select.html).

### 반드시 결정할 동작

1. **전달 정책:** 단계별 at-most-once 또는 at-least-once, 일시적 오류의 재시도와 영구 오류의 종료 기준.
2. **중복 방지:** 요청의 멱등성 범위, 보존 기간, 동시 요청의 충돌 결과, 작업별 중복과 발송별 중복의 구분. 트리거 API는 선택적 `transactionId`와 보존 기간을 설명한다. [`events.controller.ts`](apps/api/src/app/events/events.controller.ts).
3. **장애 복구:** 임대 만료, 재시도 횟수와 백오프, 최종 실패 작업의 보존·운영자 재처리.
4. **예약과 취소:** 지연 시간, 취소와 실행의 경쟁, digest의 수집 범위 및 경계 시각.
5. **관측:** 큐 대기 시간, 오래된 작업, 실패·재시도, 임대 만료, 발송 결과를 환경과 채널별로 측정.

원본의 워크플로 및 수신자 처리 워커에는 실패를 재시도하지 않는 at-most-once 정책이 명시되어 있다. 이것을 모든 큐와 모든 장애에 대한 시스템 전체 보장으로 일반화하지 않는다. 이 동작을 보존할지 개선할지는 API 계약과 운영 요구사항을 확인한 뒤 기록한다.

### 외부 발송의 한계

provider가 요청을 수락한 직후 워커가 종료되면, DB 완료 기록이 없어 재시도 시 중복 발송이 발생할 수 있다. DB 트랜잭션만으로 외부 시스템까지 정확히 한 번 실행하는 것은 보장할 수 없다. provider가 멱등성 키를 지원하는지 채널별로 확인하고, 지원하지 않는 경우 중복 가능성과 재처리 정책을 명시한다. 상태 변경과 새 작업 생성은 같은 DB 트랜잭션에 넣거나 transactional outbox를 사용해 유실 위험을 줄인다. 이 역시 외부 발송의 exactly-once를 보장하지는 않는다.

## 6. 실시간 전달과 캐시

알림을 DB에 영속화한 뒤 `NOTIFY`로 실시간 서버를 깨우고, 서버는 필요한 데이터를 조회해 연결된 클라이언트에 전달하는 구조를 검토한다. 클라이언트와 서버는 재연결 시 영속 데이터와 커서 등으로 누락분을 복구해야 한다. `LISTEN/NOTIFY` 자체는 재접속 후 전달을 보장하는 영속 메시지 큐가 아니다. 리스너 연결과 풀러의 세션 동작도 검증한다. [PostgreSQL LISTEN 문서](https://www.postgresql.org/docs/current/sql-listen.html).

Socket.IO 프로토콜, 인증, 이벤트 형식 및 읽음·보관 상태를 기존 Inbox SDK와 호환해야 하는지는 미결이다. 원본 SDK를 그대로 사용할 계획이라면 이를 별도의 호환성 목표와 테스트로 다룬다.

캐시는 초기에는 DB 조회 또는 제한적인 프로세스 내 캐시로 시작할 수 있다. 여러 인스턴스에서 workflow나 설정이 바뀔 때 오래된 캐시를 어떻게 무효화할지 명시해야 한다. 단일 PostgreSQL에 큐·조회·알림 부하가 집중되므로 연결 수와 자원 경쟁을 측정한다.

## 7. 보안과 운영

- 조직·환경 격리, 인증·인가, 구독자 인증을 API와 작업자 경로에서 검증한다. 작업 payload에서 전달된 테넌트 ID를 무조건 신뢰하지 않는다.
- API 키와 provider 자격 증명은 애플리케이션 수준 암호화 및 키 관리 방안을 마련하고 로그·응답에 노출하지 않는다. 원본의 관련 지침은 [`AGENTS.md`](AGENTS.md)를 참고한다.
- 작업과 메시지에 개인 정보가 포함될 수 있으므로 로그 마스킹, 접근 권한, 데이터 보존 및 삭제 정책을 정한다.
- DB 백업·복원, 장애 조치, 스키마 마이그레이션과 롤백을 시험한다. 큐의 잦은 변경·삭제에 따른 autovacuum, 테이블/인덱스 팽창, WAL, 연결 수와 저장 공간을 관찰한다.
- 대량의 이력·분석 데이터는 운영성 데이터와 요구사항을 구분한다. 보존 기간과 실제 쿼리 결과에 따라 파티셔닝이나 별도 분석 저장소를 검토한다. [PostgreSQL 파티셔닝 문서](https://www.postgresql.org/docs/current/ddl-partitioning.html).

## 8. REA의 역할

소스와 테스트를 우선 활용한다. REA는 실제 실행 결과가 불명확하거나 배포된 JavaScript 산출물, 브라우저 동작 또는 네트워크 캡처를 분석해 소스 기반 가설을 확인해야 할 때 보조적으로 사용한다. 정적 분석, 실행 관찰, 추론을 구분하고 원본 및 Rust 구현에 동일한 시나리오를 적용한다. 캡처에 포함될 수 있는 자격 증명과 개인 정보를 보호한다.

이 문서 작성 시 REA 실행이나 동작 비교는 수행하지 않았다. REA를 사용한다는 사실만으로 기능 호환성이나 성능이 검증되는 것은 아니다.

## 9. 단계별 진행 제안

1. **범위 확정:** 대상 에디션, API·SDK 호환성, 필요한 채널, 처리량·지연·가용성, 운영 환경과 데이터 이전 여부를 정한다.
2. **계약 수집:** 기준 커밋의 트리거 응답, 오류, 인증, subscriber/topic, 중복 방지, 발송 상태를 소스·테스트·필요한 실행 관찰로 정리한다.
3. **최소 수직 경로:** Rust API → PostgreSQL 작업 큐 → 워커 → 단일 provider를 구현하고 원본과 비교한다. 기존 대시보드·SDK의 재사용 여부는 호환성 검증 후 결정한다.
4. **복구와 실시간:** 워커 장애 및 재시도, 지연·취소, Inbox와 재연결 복구를 추가한다.
5. **범위 확장:** 채널, preferences, topic fan-out, digest, 관리 기능을 우선순위에 따라 추가한다.
6. **운영 전 검증:** 목표 부하 및 장애 주입, 보안 격리, 백업 복원과 운영 절차를 검증한다. 병목이 확인되면 별도 큐·캐시·분석 저장소를 평가한다.

### 데이터 이전이 필요한 경우

기존 서비스를 대체하는지부터 결정한다. 이전이 필요하다면 원본과 대상 스키마 매핑, 식별자 변환, 암호화 키 처리, 데이터 정합성 검증, 증분 동기화, 중복 발송 방지, 중단 시간과 cutover/rollback 기준을 별도 계획으로 작성한다. 이 문서는 실시간 전환이나 무중단 이전을 보장하지 않는다.

## 10. 초기 검증 체크리스트와 당시 변경 기록

- [ ] 목표 기능과 호환되지 않아도 되는 동작이 문서화되었다.
- [ ] 원본과 Rust 구현에 같은 입력을 사용해 응답, 오류, 저장 상태와 발송 결과를 비교했다.
- [ ] 서로 다른 환경 간 조회·수정 및 관계 연결이 차단되는지 확인했다.
- [ ] 동시 트리거, 중복 키, worker 중단, 임대 만료 후 늦은 완료, provider 수락 직후 중단을 시험했다.
- [ ] 예약·취소 경쟁, digest 경계, 대량 topic 수신자 확장을 검증했다.
- [ ] 실시간 연결 끊김과 재연결 시 알림 복구를 확인했다.
- [ ] 목표 처리량과 지연에서 DB, 큐, provider 제한을 측정하고 백업 복원을 검증했다.
- [ ] 원본의 라이선스와 포함할 기능 범위를 확인했다.

후속 변경 시 날짜, 작성자, 기준 원본 커밋, 결정 및 이유, 의도적인 호환성 차이, 근거·검증 결과, 남은 위험을 이 문서 또는 연결된 설계 결정 기록에 남긴다. 원본 업데이트 시에는 이 문서의 기준 커밋과 해당 기능의 소스·테스트를 비교한 후 판단을 갱신한다.

| 날짜 | 변경 | 근거와 상태 |
|---|---|---|
| 2026-11-12 | Rust·PostgreSQL 전환 초기 분석 작성 | 정적 소스 검토. 구현·실행·성능 검증은 미수행. |
| 2026-11-12 | 100% 호환 목표 명시 및 PostgreSQL 큐 기반 구현 시작 | [`apps/worker/rust-queue`](apps/worker/rust-queue/README.md)는 독립 실험 코드이며 Novu에 연결되지 않았다. 원본 대비 호환성은 미검증. |
| 2026-11-12 | 큐의 지연 등록·재시도·대기 취소 및 트리거 분기 일부 구현 | PostgreSQL 17 Docker에서 큐 통합 테스트를 실행했다. 원본 코드와 트리거 분기를 대조했으나 실제 원본과의 종단 간 비교 및 전체 호환성은 미검증. |
| 2026-11-12 | 실험적인 Rust API와 원본 코드 차등 테스트 추가 | [`apps/api/rust-api`](apps/api/rust-api/README.md)의 API 키 및 트리거 분기 일부를 PostgreSQL로 테스트했다. 원본 TypeScript 수신자 검증 함수와 Rust를 제한된 입력 집합으로 직접 비교했다. 원본 서버 전체 비교, 활성 워크플로, 채널 발송, 인증/권한 전체는 미구현 또는 미검증이다. |
| 2026-11-12 | 단일 정적 인앱 단계의 실험 경로 추가 | API→큐→워커→구독자 및 메시지 DB 저장을 Docker PostgreSQL로 검증했다. 문자열 수신자 중복 제거는 원본 TypeScript 함수와 일부 입력을 비교했다. Inbox API·실시간·선호 설정·템플릿·다른 채널 및 전체 호환성은 미구현 또는 미검증이다. |
| 2026-11-12 | 인앱 경로에 제한된 변수 치환 추가 | 원본 lockfile의 Handlebars 4.7.10으로 지원하는 변수 치환 예제를 직접 비교하고 PostgreSQL 종단 경로를 검증했다. 원본 서버 전체와의 종단 간 비교는 미수행이며 helpers, 블록 구문 등은 미지원이다. 기준 커밋 해시 태그의 공식 API 이미지는 찾지 못했다. |
| 2026-11-12 | 서비스·SDK 계약 조사 및 Rust 의존 분리 | [서비스 계약 문서](NOVU_SERVICE_CONTRACTS_KO.md)를 추가하고 공통 Rust 계약을 분리했다. 원본 WS context 필터 함수와 제한된 입력으로 직접 비교했다. 실제 Socket.IO 서버 및 전체 계약은 미구현이다. |
| 2026-11-12 | Community API 키 사용자 연관 확인 추가 | 환경·사용자·키 실험 스키마와 사용자 누락 시 거절을 추가했다. JWT 및 역할·권한 전체는 미구현이며 원본 서버와 종단 간 비교하지 않았다. |
| 2026-11-12 | 인앱 수신자 이름 생성·갱신 추가 | 문자열과 이름 필드만 포함하는 객체의 fan-out, 부분 갱신·null·렌더링 및 환경 격리를 PostgreSQL에서 검증했다. 객체 중복 처리는 원본 TypeScript 함수와 제한된 입력으로 비교했다. 나머지 수신자 속성과 전체 종단 간 호환성은 미검증이다. |
| 2026-11-12 | 트랜잭션 ID 형식 수정 및 원본 빌드 시도 | 기준 소스의 timestamp 및 base36 형식을 반영하고 PostgreSQL 테스트에서 검증했다. 디스크 공간 확보 후 원본 API·워커 의존성 설치와 빌드를 시도 중이며 원본 서버와 종단 간 비교는 아직 미수행이다. |
| 2026-11-12 | 원본 실행 및 제한된 종단 간 비교 | 기준 커밋의 API·워커를 Docker에서 빌드하고 13개 HTTP 사례와 단일 인앱 메시지 한 사례를 Rust와 비교했다. 전체 기능과 SDK·실시간 전달은 여전히 미검증이다. |
