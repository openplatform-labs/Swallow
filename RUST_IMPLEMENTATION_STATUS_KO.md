# Rust 재구현 현황과 검증 범위

> 기준: 원본 Novu 커밋 `895388f37efe9e06de0aa333e5727303e6ad6853` · 작성: 2026-11-12.
> **현재 구현은 실험용이며 Novu와 100% 호환되지 않는다. 운영에 사용하거나 기존 Novu의 대체품으로 배포하지 않는다.**

## 구현된 범위

- 제한적인 트리거 API: API 키 해시로 환경과 연결된 사용자를 조회하고, 워크플로 없음·비활성·단계 없음의 일부 응답을 처리한다. 트리거와 관리 API의 사용자 Bearer 인증은 구현되지 않아 거절한다.
- 자동 트랜잭션 ID는 기준 소스의 `txn_` + 8자리 Unix 초 hex + 12자리 영숫자 형식을 따른다. 전체 서버 간 비교는 아직 수행하지 않았다.
- PostgreSQL 작업 큐: 등록·지연·선점·임대·완료·실패·명시적 재시도/복구 및 대기 작업 취소의 기본 연산이 있다.
- 실험 인앱 워커는 시작 시와 최대 5초 간격으로 **자신의 작업 종류에 한해** 만료 임대를 복구한다. 지원하지 않는 템플릿이나 잘못된 작업은 고정 진단과 함께 실패 처리한 뒤 다음 작업을 처리한다. DB 장애는 프로세스를 종료하며 외부 재시작이 필요하고 충돌 복구의 횟수 제한은 없다. 이는 원본의 전체 재시도·실패 정책을 구현한 것이 아니다.
- 인앱 실험 경로: 문자열 수신자 또는 `subscriberId`·이름·제한적인 ASCII `email`·`phone`·`avatar`·`locale`·`timezone`·`data`로 제한된 수신자 객체, 단일 인앱 단계, 제한된 변수 치환에 한해 API → 큐 → 워커 → 구독자 및 메시지 테이블 저장이 가능하다. 제공된 속성만 갱신하고 `data`는 객체 전체를 교체하며 명시적 null도 저장한다. 지원하지 않는 경로는 일부 501로 거절한다.
- 제한적인 `GET /v2/subscribers/:subscriberId`: API 키의 환경 범위에서 실험용 구독자를 조회하고 저장된 프로필과 일부 기본 필드를 반환한다. 실제 채널·온라인 상태는 구현되지 않았으며, 관리 API 전체를 대체하지 못한다.
- 제한적인 `POST /v2/subscribers`: 이름·제한적인 ASCII 이메일·전화·아바타·locale·`data`로 구독자를 생성하거나 기존 구독자를 갱신하며, `failIfExists`의 제한된 값을 지원한다. ID 공백 제거 및 기본 오류 검증을 처리한다. 빈 `data` 객체는 GET·POST 응답에서 생략되나 명시적으로 갱신한 PATCH 응답에는 포함된다. 일부 이메일 형식과 전체 입력 검증·권한은 미구현이다. locale은 제한된 validator.js 호환 검증을 지원한다.
- 제한적인 `PATCH /v2/subscribers/:subscriberId`: 이름·제한적인 ASCII 이메일·전화·아바타·locale·`data`의 부분 갱신과 null을 처리하고 동일 구독자의 동시 갱신을 직렬화한다. 지원 필드의 타입 오류는 제한적으로 422를 반환한다. 일부 이메일 형식과 전체 입력 검증·권한은 미구현이며, 지원하지 않는 필드는 501로 거절한다.
- 제한적인 `DELETE /v2/subscribers/:subscriberId`: API 키 환경에 속한 실험 구독자와 저장된 인앱 메시지, 구독별 설정을 하나의 PostgreSQL 문으로 삭제하며 전역·워크플로 설정과 토픽 구독은 외래 키로 함께 정리한다. 구독별 설정은 v1 관계 삭제 등으로 구독이 이미 없어진 경우에도 구독자의 내부 ID를 기준으로 제거한다. 채널 연결 데이터의 삭제와 대기 작업에 대한 의미는 미구현 또는 미검증이다.
- 제한적인 `GET /v2/subscribers/:subscriberId/notifications/count`: API 키 환경에 속한 인앱 메시지를 `read`·`seen`·`archived`와 태그 필터별로 세고 각 결과를 최대 100으로 제한한다. 태그는 배열·`or`·여러 `or` 그룹의 `and`를 지원한다. 나머지 필터·인증 방식 및 Inbox 전체 계약은 미구현이다.
- 제한적인 구독자 알림 일괄 상태 변경: `POST .../notifications/read`, `archive`, `read-archive`에서 전체 또는 태그·data 조건에 맞는 인앱 메시지의 읽음·확인·보관 상태와 관련 시각을 갱신하고 제한적인 읽지 않은 수 이벤트를 발행한다. 관리 API의 context 필터와 웹훅·추적은 구현하지 않았다.
- 제한적인 `POST .../notifications/seen`: 인앱 메시지 전체, 태그·data 조건 또는 24자리 hex ID 목록을 확인 처리하고 최초·마지막 확인 시각을 저장한다. ID 목록이 있으면 필터보다 우선하며 빈 ID 배열은 원본처럼 구독자 유무와 무관하게 성공한다. 제한적인 미확인 수 이벤트를 발행하며 관리 API의 context 필터, 웹훅·추적은 미구현이다.
- 제한적인 `GET .../notifications`: 실험 워크플로 메타데이터가 설정된 메시지의 ID, 구독자, 내용, 상태, 시각, 워크플로 및 data 또는 payload를 조회한다. `read`·`seen`·`archived`, 단일 또는 반복 `tags` OR 필터, 제한적인 `data` 필터와 `limit`·`offset`·같은 환경의 `after` 커서를 지원한다. 메타데이터 누락·미지원 옵션은 501이며 나머지 Inbox DTO와 필터 전체를 구현한 것은 아니다.
- 제한적인 구독자 JWT Inbox 경로: `JWT_SECRET`이 설정된 경우 HS256 `widget_user` 토큰 서명·만료·not-before·audience를 검사하고 환경·외부 구독자 ID로 존재를 재확인한다. `/v1/inbox/notifications`의 목록·카운트, 개별 읽음·보관·스누즈·해제·삭제 및 일괄 상태 변경·삭제를 지원한다. 문자열 context의 목록·카운트는 context 키 집합이 정확히 일치하는 메시지만 반환한다. 개별 변경은 정확히 일치하는 context에 적용하며 일괄 변경은 원본처럼 일부 키가 겹치는 항목에 적용한다. 변경에 따른 제한적인 카운트 이벤트를 발행한다. 완전한 스누즈 입력과 요금제 계약, action 및 전체 이벤트 계약은 미구현이다. 토큰을 관리 API의 인증으로 사용하지 않는다.
- 제한적인 `POST /v1/inbox/session`: 명시적으로 설정한 환경·인앱 통합에 대해 구독자를 생성하고 15일 HS256 토큰과 읽지 않은 수를 반환한다. HMAC 필수 설정을 확인하고 활성 API 키 해시와 일치하는 프로세스 설정 키로 서명을 검증한다. 서명 없이 기존 프로필을 갱신하지 않으며 키가 누락된 필수 HMAC 환경은 실패한다. 문자열·객체형 context와 필수 context HMAC 검증을 제한적으로 지원한다. 기본 스케줄·keyless·완전한 입력 검증 및 실제 통합 선택은 미구현이다. 테스트용 별도 설정 테이블과 프로세스 환경 키는 운영용 키 관리가 아니다.
- 제한적인 알림 삭제: `DELETE .../notifications/:notificationId`와 `POST .../notifications/delete`에서 해당 환경과 구독자의 실험 인앱 메시지를 삭제한다. 일괄 삭제는 전체 또는 태그·data 조건을 지원하며 제한적인 읽지 않은 수 이벤트를 발행한다. 관리 API의 context 필터, 웹훅·분석·추적은 미구현이다.
- 메시지 `data`와 트리거 `payload`는 별개다. 마이그레이션 `0008`·`0009`의 data 필터는 스칼라, OR·AND 그룹, 2단계 객체 경로와 일부 MongoDB 배열 경로 탐색을 지원한다. 잘못된 필터 구조의 오류 계약, 성능 및 전체 MongoDB 배열 의미는 미검증이다. 기존 실험 데이터에서 없던 `data`는 복원되지 않는다.
- 제한적인 개별 알림 상태 변경: `PATCH .../notifications/:notificationId/{read,unread,archive,unarchive}`는 해당 구독자의 인앱 메시지 상태·시각과 응답 DTO를 갱신하고 제한적인 읽지 않은 수 이벤트를 발행한다. 마이그레이션 `0007`은 시각 필드의 부재와 명시적 null을 구분한다. 기존 데이터에서 이미 지워진 시각의 과거 null 여부는 복구할 수 없다. 관리 API의 context 필터와 웹훅·분석·추적은 미구현이다.
- 공통 계약: 일부 트리거 상태, Socket.IO 이벤트 이름, context 비교 함수를 분리했다. 별도 실험 WS 서비스는 연결·상태 처리와 제한적인 인앱 이벤트를 지원한다.

## 미구현 또는 불완전

| 영역 | 남은 기능 및 제한 |
|---|---|
| API와 관리 | 대다수 Novu API, workflow·integration·subscriber 관리 API, OpenAPI 전반, 전체 DTO·오류·페이지네이션·API 버전 및 공식 SDK 호환성. |
| 인증·권한 | 사용자 JWT, OAuth, 구독자 세션의 context HMAC 소수 오류 응답·keyless·실제 통합 선택·오류 경계와 JWT의 나머지 알고리즘·만료 설정, 운영용 비밀 관리, 사용자 조직 멤버십, 역할·권한 및 엔터프라이즈 인증. 실험용 DB 연결은 `NoTls`이며 운영 구성에 적합하지 않다. |
| 워크플로 | 다단계 실행, 조건·분기, 지연·digest의 실제 워크플로 처리, 취소, bridge/stateless 워크플로, actor·tenant·복잡한 context, 첨부와 overrides. 큐 기본 연산의 존재는 워크플로 지원을 뜻하지 않는다. |
| 구독자·설정 | Unicode·따옴표 이메일과 해당 오류 응답, 나머지 구독자 관리 API, topic, 나머지 사용자별/워크플로별 알림 설정, channel 연결·자격증명. 실험 스키마는 원본 데이터 모델의 대체물이 아니다. |
| 템플릿 | Handlebars 전체 문법, helpers, blocks, 부동소수·안전 범위를 벗어난 정수·배열·객체 보간, 번역, 일부 사용자 프로필 및 사용자 정의 branding. 지원 범위의 일부 입력만 검증되었다. |
| Inbox·실시간 | Inbox 목록의 나머지 DTO·복잡한 태그 쿼리·필터 오류·커서 사례, data 필터의 미검증 MongoDB 배열 경계, snooze·action 등 나머지 개별 상태 변경, 일괄 작업 및 삭제의 context 필터·설정 API, 카운트의 severity 필터와 잘못된 태그 오류 응답, 나머지 session 계약, Socket.IO 이벤트·SDK 전체 호환성, 상태 변경과 삭제 이벤트·웹훅, 실시간 전달과 다중 인스턴스 fan-out·복구. |
| 채널 | 이메일, SMS, 푸시, 채팅 및 provider 통합, 외부 발송 결과·재시도·웹훅. 현재 인앱은 실험 테이블에 저장할 뿐 완전한 Inbox 전달이 아니다. |
| 큐 정책 | 원본 각 단계의 전달·실패·재시도·백오프·중복·취소·보존 정책 전체. 실험용 트리거 ID 기록은 무기한 유지되며 원본의 요금제별 보존과 같지 않다. DB 장애 시 워커는 종료하며 외부 재시작이 필요하다. 만료 인앱 작업의 실험적 자동 복구는 원본과 정책을 비교하지 않았다. |
| 운영·이전 | 운영용 DB 마이그레이션과 MongoDB 데이터 이전, 키 관리, TLS, 관측성, 백업 복구·고가용성·부하 및 용량 계획. 실험 SQL은 빈 테스트 DB에만 적용한다. |
| 에디션·범위 | 클론은 shallow이고 enterprise 서브모듈 `.source`는 초기화되지 않았다. Enterprise/Cloud 전용 기능과 라이선스 범위, 외부 패키지·서비스는 별도 확인이 필요하다. |

## 수행한 검증

- Rust 컴파일·포맷·Clippy 및 제한된 단위 테스트.
- 일회용 Docker PostgreSQL 17에서 큐와 API → 작업 → 인앱 메시지 저장의 통합 테스트. 트랜잭션 롤백, 작업 선점, 만료·복구, 토큰 기반 완료, 예약·취소, 일부 동시 요청, 환경 범위의 API 키 인증을 다뤘다.
- 같은 PostgreSQL 테스트에서 구독자 조회, 존재하지 않는 구독자, 인증 누락, 다른 환경의 구독자 격리를 확인했다. 기준 커밋의 원본 API와 Rust API에 동일한 격리 fixture를 넣어 조회 성공·없는 구독자·인증 누락·잘못된 키 4개 HTTP 응답을 비교했고 모두 일치했다. 오류 시각은 형식만 검증했다. [비교 스크립트](apps/api/rust-api/tests/reference_subscriber_compare.py). 이메일 등 다른 속성이나 실제 채널·온라인 상태는 비교하지 않았다.
- PostgreSQL 통합 테스트에서 `phone`·`avatar`·`data` 생성, 일부 갱신과 명시적 null, `data` 객체 교체, 조회 및 제한적 템플릿 치환을 확인했다. 해당 속성을 포함한 일부 수신자 분리 결과는 원본 TypeScript 함수와, 치환 예시는 원본 lockfile의 Handlebars 4.7.10과 비교했다.
- PostgreSQL 통합 테스트에서 구독자 PATCH의 부분 갱신·null·기존 속성 유지·환경 격리·인증·동시 갱신을 확인했다.
- PostgreSQL 통합 테스트에서 구독자 POST의 생성·갱신·기본 `data`·중복 충돌·환경 격리·동시 중복 생성을 확인했다. 동일 기준 커밋의 원본 API와 격리 실행해 생성, 갱신, `failIfExists` true·false, 기본값·null, ID 공백·오류, 잘못된 필드, 인증 누락 등 13개 응답과 성공 후 조회를 비교했다. 독립 생성되는 ID와 시각은 형식만 확인하고 서버 간 값 비교에서 제외했다. [비교 스크립트](apps/api/rust-api/tests/reference_subscriber_create_compare.py).
- GET·POST의 빈 `data` 생략과 명시적 PATCH 빈 객체 응답을 반영한 뒤, 원본 PATCH 차등 테스트를 빈 `data` 사례까지 확장하여 총 9개 사례를 재검증했다. 이는 중첩 빈 객체나 전체 MongoDB 직렬화 규칙에 대한 검증은 아니다.
- Docker PostgreSQL 통합 테스트에서 구독자 삭제·메시지 정리·환경 격리를 확인하고, 외래 키로 삭제 실패를 유발하여 메시지와 구독별 설정 삭제가 롤백되는지 확인했다. MongoDB 단일 노드 복제본 세트의 원본 API와 비교해 인증 누락·잘못된 키·미존재·삭제·반복 삭제, 메시지 및 구독 관계와 잔존 구독 설정의 정리, 다른 구독자 및 다른 환경의 설정 보존, 동일 ID 재생성 시 새 내부 ID 및 빈 구독 목록을 검증했다. 오류 시각의 값은 비교하지 않았다. [삭제 비교 스크립트](apps/api/rust-api/tests/reference_subscriber_delete_compare.py). 채널 연결, 동시 작업 및 대기 작업은 비교하지 않았다. 구독별 설정 정리는 기본 키의 환경·구독자 접두 인덱스를 사용하며 삭제 시 추가 DB 작업이 발생한다.
- 원본 API와 Rust API의 알림 수에서 상태별 필터·빈 필터·허용되지 않는 조합·미존재·필수 필터 오류·인증·100개 상한을 비교했다. 이 비교는 수작업 상태별 메시지 fixture를 사용했다. 별도로 원본과 Rust의 API·워커를 실행해 단일 인앱 워크플로 트리거 → 메시지 저장 → 상태 필터별 카운트까지 비교했다. [카운트 비교](apps/api/rust-api/tests/reference_notification_count_compare.py), [워크플로 비교](apps/api/rust-api/tests/reference_in_app_compare.py). 나머지 필터와 개별 상태 변경·실시간 전달은 검증하지 않았다.
- 원본과 Rust에서 합성 메시지 세 건에 `read-archive` → `read` → `archive`를 적용해 204 응답, 카운트, 저장 상태와 시각의 존재·기존 최초 확인 시각 보존을 비교했다. 미존재 구독자 및 인증 오류도 비교했다. 또한 단일 인앱 워크플로를 실제 양쪽 워커에서 실행한 뒤 같은 일괄 상태 변경과 카운트를 검증했다. [일괄 상태 비교](apps/api/rust-api/tests/reference_notification_bulk_compare.py), [워크플로 비교](apps/api/rust-api/tests/reference_in_app_compare.py). 이벤트·웹훅·필터는 비교하지 않았다.
- 원본 API와 Rust API에서 메시지 ID 지정·전체 확인, 빈 배열, 잘못된 ID 및 배열 오류, 없는 메시지·구독자, 다른 구독자 메시지 격리, 인증과 상태·카운트를 비교했다. 별도로 양쪽 API·워커로 생성된 메시지의 각 ID를 사용해 확인 처리와 카운트를 종단 간 비교했다. [확인 비교](apps/api/rust-api/tests/reference_notification_seen_compare.py), [워크플로 비교](apps/api/rust-api/tests/reference_in_app_compare.py). 원본 ID의 실제 값과 독립 생성된 Rust ID는 비교하지 않았고, Socket.IO·웹훅·추적은 검증하지 않았다.
- 원본과 Rust의 API·워커로 세 개의 인앱 메시지를 생성해 목록 DTO·payload·상태 필터·커서 및 오프셋 페이지네이션·확인 처리 후 목록·인증 오류를 비교했다. 독립 생성되는 메시지·구독자 ID와 시각은 형식만 확인하고 값 비교에서 제외했다. [목록 비교](apps/api/rust-api/tests/reference_notification_list_compare.py). 이 테스트는 고정된 `REGULAR` 단일 워크플로 및 설정된 메타데이터에 한정되며, 전체 Inbox SDK·필터·정렬 경계 조건을 검증하지 않았다.
- 원본과 Rust API에서 동일한 메시지 fixture로 개별 삭제·반복 삭제·전체 삭제·인증과 유효성 오류·다른 구독자 격리 및 삭제 후 카운트를 비교했다. 별도로 양쪽 워커에서 생성한 메시지에 개별 삭제와 전체 삭제를 수행하고 삭제 후 목록을 비교했다. [삭제 비교](apps/api/rust-api/tests/reference_notification_delete_compare.py), [워크플로 목록·삭제 비교](apps/api/rust-api/tests/reference_notification_list_compare.py). 필터 삭제와 Socket.IO·웹훅·분석·추적은 비교하지 않았다.
- 양쪽 API·워커에서 생성한 메시지를 보관 → 읽지 않음 → 읽음 → 보관 해제하고 응답 DTO와 변경 후 목록, ID 검증·미존재·인증 오류를 비교했다. 시각의 형식과 null·부재는 비교하고 독립 생성된 시각 값과 ID는 비교에서 제외했다. [종단 간 비교](apps/api/rust-api/tests/reference_notification_list_compare.py). context 및 이벤트·웹훅·분석·추적은 검증하지 않았다.
- 양쪽 워커가 생성한 세 메시지에 동일한 태그를 설정한 뒤 단일·반복 태그 목록 및 태그 배열, `or`, `and` 그룹·빈 그룹·상태 조합 카운트를 원본과 비교했다. [태그 비교](apps/api/rust-api/tests/reference_notification_list_compare.py). 태그는 생성 후 테스트에서 직접 설정했으며, 복잡한 목록 쿼리 형식과 잘못된 입력의 오류 응답은 비교하지 않았다.
- 양쪽 워커로 생성하고 테스트에서 태그를 부여한 메시지에 대해 태그 조건을 사용한 일괄 확인·읽음·보관·읽은 항목 보관·삭제와 변경 후 목록을 비교했다. 확인 처리에서 메시지 ID가 태그 조건보다 우선하는 사례도 비교했다. [일괄 태그 비교](apps/api/rust-api/tests/reference_notification_list_compare.py). data·context 조건과 이벤트·웹훅은 검증하지 않았다.
- 양쪽 워커로 생성한 메시지에 테스트에서 동일한 `data`를 설정하고 스칼라·중첩·OR·AND·배열 원소·배열 경로·없는 필드의 null·빈 필터·payload 비일치를 목록에서 비교했다. `data` 조건의 확인·읽음·보관, 태그와 배열 경로를 조합한 삭제, ID 지정 시 필터 무시도 비교했다. [data 비교](apps/api/rust-api/tests/reference_notification_list_compare.py). 구조 오류 응답, context, 이벤트·웹훅, 부하는 비교하지 않았다.
- MongoDB와 PostgreSQL에서 17개 문서에 배열의 객체·빈 배열·숫자 인덱스·null·누락·중첩 배열을 포함한 16개 경로 조건을 직접 비교했다. [배열 경로 비교](apps/api/rust-api/tests/reference_data_array_compare.py). 이는 MongoDB의 모든 배열·BSON 타입 및 성능 동등성을 증명하지 않는다.
- 원본 Novu에서 실제로 발급한 구독자 세션 JWT를 양쪽 Inbox 목록·카운트 API에 사용해 응답을 비교했다. 서명 위조·만료·다른 audience·없는 구독자 및 API 키의 Inbox 접근 거절도 비교했다. PostgreSQL 테스트는 다른 환경·not-before·서명 키 누락·관리 API 격리를 추가로 확인했다. [JWT 종단 간 비교](apps/api/rust-api/tests/reference_notification_list_compare.py). Rust의 세션 발급·HMAC·비어 있지 않은 context 및 실시간 전달은 검증하지 않았다.
- 같은 원본 세션 JWT를 이용해 개별 읽음·읽지 않음·보관·보관 해제·삭제, 태그 조건의 일괄 확인·읽음·보관·읽은 알림 보관 및 전체 삭제를 비교했다. PostgreSQL 통합 테스트에서 다른 구독자 메시지 격리, 인증 누락·API 키 거절과 태그 조건 삭제도 확인했다. [Inbox 쓰기 비교](apps/api/rust-api/tests/reference_notification_list_compare.py). context와 Socket.IO·웹훅·분석·추적, snooze·action은 검증하지 않았다.
- 원본과 Rust의 세션 생성·읽지 않은 수·HMAC 미사용 시 무서명 프로필 갱신 차단·유효 서명 갱신·HMAC 필수 시 누락/위조/유효 사례 및 선택된 오류를 비교했다. 각 서버가 발급한 토큰을 양쪽 Inbox API에서 교차 확인했다. PostgreSQL에서 키 교체·폐기·환경 격리·서명 키 누락을 검증했고, 원본 빌드의 `@novu/js`로 Rust 세션과 빈 목록·카운트도 실행했다. [세션 비교](apps/api/rust-api/tests/reference_inbox_session_compare.py), [SDK 스모크 테스트](apps/api/rust-api/tests/reference_sdk_smoke.cjs). context·스케줄·실시간 및 전체 SDK 테스트가 아니다.
- 원본 API와 구독자 이메일 생성·PATCH·null·잘못된 주소를 비교했다. 별도로 원본과 Rust API·워커에서 이메일 생성·변경·null 후 프로필과 인앱 템플릿 렌더링을 비교했고, ASCII 사례 147개를 원본 lockfile의 validator.js 13.15.26과 비교했다. [생성 비교](apps/api/rust-api/tests/reference_subscriber_create_compare.py), [PATCH 비교](apps/api/rust-api/tests/reference_subscriber_patch_compare.py), [워크플로 비교](apps/api/rust-api/tests/reference_subscriber_workflow_compare.py), [검증 비교](apps/api/rust-api/tests/reference_email.rs). Unicode·따옴표 주소와 이메일 채널 발송은 검증하지 않았다.
- Docker PostgreSQL과 실제 Rust 워커 프로세스에서 만료된 인앱 작업의 복구·완료, 이전 임대 토큰의 차단, 렌더링 실패 시 메시지·구독자 롤백, 실패 후 다음 작업 처리 및 다른 종류의 작업 격리를 검증했다. 잠긴 만료 작업을 복구 과정에서 기다리지 않는지도 확인했다. [워커 복구 테스트](apps/worker/rust-queue/tests/in_app_recovery.rs). 변경된 Rust 워커와 원본을 실행해 단일 인앱 트리거·목록·카운트·일괄 상태 변경도 다시 비교했다. [정상 흐름 비교](apps/api/rust-api/tests/reference_in_app_compare.py). 원본 Novu와 실패·재시도 정책을 비교한 테스트는 아니다.
- 기준 커밋의 원본 API와 Rust API에서 동일한 구독자 fixture를 사용해 PATCH의 변경·기존 값 유지·null·변경 없음·복수 필드 타입 오류·미존재·인증 누락 7개 사례를 비교했다. 성공 시 후속 GET도 비교하고 각 서버의 PATCH 응답 `updatedAt`이 직전 GET과 같은지 검증했다. 양 서버가 독립적으로 부여하는 수정 시각과 오류 발생 시각은 서버 간 값 비교에서 제외했다. [비교 스크립트](apps/api/rust-api/tests/reference_subscriber_patch_compare.py). 지원하지 않는 필드와 전체 입력 검증은 비교하지 않았다.
- 원본 API·워커와 Rust API·워커를 격리 실행하고 동일 구독자에 대해 생성, `phone` null 및 `data` 교체, `data` null의 세 트리거를 순서대로 비교했다. 각 트리거 응답, 조회 응답(서로 독립 생성되는 `_id`·생성/수정 시각 제외), 인앱 메시지 내용·채널·트랜잭션·수신자 ID가 일치했다. 원본 fixture에는 활성 인앱 통합 및 기본 워크플로 알림 설정을 넣었다. [종단 간 비교 스크립트](apps/api/rust-api/tests/reference_subscriber_workflow_compare.py). 이 테스트는 알림 설정의 변경·차단이나 실시간 전달을 비교하지 않는다.
- 원본 TypeScript의 수신자 검증, 문자열 수신자 중복 제거, WS context 비교 함수를 추출·실행하여 **제한된 입력**에 대해 Rust 결과와 비교했다.
- 이름이 포함된 수신자 객체의 중복 처리도 원본 TypeScript 함수와 제한된 입력으로 비교했으며, 생성·부분 갱신·null·기존 프로필 렌더링을 일회용 PostgreSQL에서 검증했다. 이 비교는 원본 서버 전체를 실행한 것은 아니다.
- 원본 lockfile 버전 Handlebars 4.7.10과 제한된 변수 치환 사례를 비교했다. Handlebars 전체와 원본의 모든 helper 동작을 검증한 것은 아니다.
- 동일 기준 커밋에서 직접 빌드한 원본 API·워커와 Rust API·워커를 격리 실행하여 인증·검증·비활성/빈 워크플로 13개 HTTP 사례를 비교했다. 오류 응답의 시각은 형식만 확인하고 값은 비교에서 제외했다. 또한 단일 수신자·단일 인앱 워크플로 한 사례의 접수 응답과 저장된 메시지 내용·채널·트랜잭션·수신자 ID를 비교했다. [비교 테스트](apps/api/rust-api/tests/reference_http_compare.py), [인앱 비교](apps/api/rust-api/tests/reference_in_app_compare.py).

명령과 테스트의 데이터 삭제 범위는 [API README](apps/api/rust-api/README.md)와 [워커 README](apps/worker/rust-queue/README.md)에 기록되어 있다. PostgreSQL 테스트는 기본 `cargo test`에서 무시되며 Docker 스크립트로 별도 실행한다.

## 아직 수행하지 않은 검증

- **전체 기능에 대한 원본 대 Rust 종단 간 차등 테스트.** 기준 커밋 소스에서 API와 워커를 빌드해 위의 제한된 사례는 비교했으나, 아래의 나머지 기능은 비교하지 않았다. 다른 버전의 Docker 이미지를 기준 커밋과 동일하다고 간주하지 않는다.
- 원본 인앱 비교는 수작업으로 생성한 격리 fixture와 단일 단계·수신자에 한정된다. 저장 메시지 전체 스키마, Inbox SDK·Socket.IO 및 다양한 실패·재시도 조건을 비교하지 않았다.
- 원본 전체 e2e·SDK·Dashboard·Socket.IO 테스트, 모든 인증 및 권한/테넌트 격리 시나리오.
- 외부 provider를 통한 실제 이메일·SMS·푸시·채팅 발송과 실패·중복·재처리 검증.
- 장애 주입, 부하·지연·장시간 운용, 보안 감사, 백업·복원, 실 데이터 마이그레이션과 배포 전환 검증.

후속 변경에서는 이 문서의 항목을 실제 구현·테스트 결과에 맞춰 갱신하고, 원본 대비 차이가 확인되지 않은 동작은 호환 완료로 표시하지 않는다. 근거와 구조는 [서비스 계약 분석](NOVU_SERVICE_CONTRACTS_KO.md) 및 [설계 기록](RUST_POSTGRES_DESIGN_KO.md)을 참조한다.

### context 실험 구현과 제한

- `0010_experimental_context.sql`부터 `0013_experimental_context_cursor.sql`까지의 마이그레이션은 환경별 context 키·data와 메시지의 `context_keys`를 저장한다. 트리거와 Inbox 세션은 영숫자·`_`·`-`로 이루어진 ID의 문자열·객체형 context 최대 5개를 지원하고, 세션 HMAC 필수 환경에서는 정렬된 context JSON에 대한 별도 HMAC을 활성 환경 API 키로 검증한다. Inbox 목록·카운트·세션 읽지 않은 수는 키 집합이 정확히 일치하는 메시지만 선택한다. 기존 메시지는 빈 context로 취급하며 원본 데이터 가져오기는 구현하지 않았다.
- 개별 상태 변경·삭제는 context의 정확한 일치를 요구한다. 일괄 확인·읽음·보관·삭제는 원본 MongoDB 조건처럼 context 키가 하나 이상 겹치는 메시지를 처리하며, 빈 context에서는 context 조건을 생략한다. **따라서 빈 context 세션의 일괄 작업이 동일 구독자의 다른 context 메시지에도 적용된다.** 인앱 템플릿의 단순 변수 치환은 context 데이터를 지원한다. 관리 API의 알림 context 필터 및 고급 context 관리, 워크플로 조건·다른 채널에서의 context 사용, 관련 이벤트와 실시간 fan-out은 미구현이다.
- Docker PostgreSQL 통합 테스트에서 문자열·객체형 context의 트리거·세션 저장, 세션에서 기존 데이터 변경 방지, 트리거 데이터 갱신, 환경 격리, 개별·일괄 변경 조건 및 필수 context HMAC을 확인했다. 선택된 중첩·유니코드·제어 문자 사례와 5,000개 비트 패턴 중 정수형 배정밀도 숫자의 직렬화는 원본 `@tufjs/canonical-json` 패키지와 직접 비교했다. HMAC 정규화는 안전 정수 범위를 넘는 JavaScript 배정밀도 정수의 표기를 지원한다. 소수에 대한 원본 오류 응답, 큰 숫자의 저장·다른 기능에서의 JavaScript 반올림 및 모든 숫자 경계는 아직 완전히 검증하지 않았다. [DB 조건 비교](apps/api/rust-api/tests/reference_context_query_compare.py)로 원본 MongoDB의 정확 일치·일괄 부분 일치 조건과 PostgreSQL 결과를 비교했다. 인앱 템플릿의 context ID·중첩 데이터·이스케이프는 원본 Handlebars 4.7.10과 비교했으며, Docker PostgreSQL에서 발송 시점 최신 데이터·환경 격리·payload의 context 덮어쓰기 방지 동작을 테스트했다. 워커는 context가 있는 작업마다 환경과 context 키를 조건으로 PostgreSQL을 한 번 추가 조회한다. [원본 세션 비교](apps/api/rust-api/tests/reference_context_session_compare.py)에서 객체형 context와 기존 데이터 보호, HMAC 누락·위조·정상, 양 서버의 JWT 교차 인증, 직접 삽입한 메시지의 정확·부분·빈 context별 읽지 않은 수를 비교했다. 원본 워커를 통한 context 메시지 발송과 context 목록 DTO·쓰기 이벤트, 부하·동시성은 비교하지 않았다. 이 결과는 전체 호환성 검증을 의미하지 않는다.

### context 관리 API의 현재 범위

- API 키로 `POST /v2/contexts`, `GET/PATCH/DELETE /v2/contexts/:type/:id`를 사용할 수 있다. 데이터 수정은 교체이며 삭제는 메시지를 삭제하지 않는다. 환경별 조회·수정·삭제 격리, 중복 생성, 미존재, 인증 누락 및 삭제된 context가 대기 중인 작업에서 되살아나지 않는 동작을 Docker PostgreSQL에서 검증했다.
- `GET /v2/contexts`는 유형·ID·리터럴 대소문자 무시 검색, 생성·수정 시각 정렬, 앞뒤 커서와 커서 포함을 지원한다. Docker PostgreSQL에서 동시간 항목·오름/내림차순·앞뒤 페이지·환경 격리를 확인했다. 커서 ID는 PostgreSQL에서 신규 생성되므로 원본 MongoDB 커서와 교환할 수 없고, 1,000개를 넘는 페이지 크기와 일부 정렬·오류 경계는 지원하지 않는다. 사용자 JWT/권한, `bridgeUrl`과 원본의 SSRF 검증, 정확한 입력 검증 및 오류 응답은 미구현이다. 지원하지 않는 입력은 501이다. 마이그레이션 이전 데이터의 원본 생성·수정 시각은 복구할 수 없다. [원본 서버 비교](apps/api/rust-api/tests/reference_context_api_compare.py)에서 생성·조회·중복·교체·삭제, 검색과 선택된 앞뒤 커서·오류 사례를 비교했다. 독립 생성된 시각과 커서 값은 동일성 비교에서 제외했다.

### 구독자 locale 지원

- 구독자 생성·PATCH, Inbox 세션 구독자 객체와 트리거 수신자 객체의 `locale`을 저장하고 단순 인앱 템플릿에서 `subscriber.locale`로 사용할 수 있다. Docker PostgreSQL에서 생성·변경·null·유효성 오류·세션·워커 렌더링을 확인했다.
- [locale 비교 테스트](apps/worker/rust-queue/tests/reference_locale.rs)는 원본의 validator.js 13.15.26과 여러 언어 태그·구분자·유효하지 않은 조합을 직접 비교한다. 재귀 입력을 제한하기 위해 1,024바이트보다 긴 값은 거절하며, 이는 원본과 차이가 있다. 원본 서버의 생성·PATCH·후속 조회에서 정상·잘못된 값·null·숫자 타입을 비교했다. 모든 언어 태그를 검증한 것은 아니다. locale 기반 번역과 timezone 기반 일정은 미구현이다.

### 구독자 timezone 지원

- 구독자 생성·PATCH·Inbox 세션·트리거 수신자에서 `timezone`을 검증하고 저장하며 제한된 인앱 템플릿에 제공한다. null과 잘못된 값 처리도 지원한다. 스케줄 계산·timezone 변환이나 번역은 구현하지 않았다.
- 원본 기준 Node 22.23.4의 ICU 78.3 / tz 2026e와 로컬 IANA zoneinfo 후보에서 생성한 고정 이름·별칭 목록과 숫자 오프셋을 검증한다. Node/ICU 버전이나 후보 zoneinfo 데이터가 바뀌면 차이가 생길 수 있으며 모든 가능한 별칭을 증명한 것은 아니다. [생성 스크립트](apps/api/rust-contracts/tools/generate-timezones.cjs), [원본 런타임 비교](apps/worker/rust-queue/tests/reference_timezone.rs).
- Docker PostgreSQL에서 구독자 API·세션·워커 템플릿 흐름을 검증했다. 원본 Novu API와 생성·PATCH·후속 조회의 정상·잘못된 값·null·숫자 타입·별칭·숫자 오프셋을 비교했다.

- 원본 기준 커밋의 API와 Rust API를 격리 실행하는 [구독자 비교 실행기](apps/api/rust-api/scripts/test-reference-subscribers.sh)를 추가했다. MongoDB·Redis·PostgreSQL과 합성 데이터만 사용하며 종료 시 생성한 리소스를 정리한다. locale·timezone 비교는 실제 API의 생성 및 PATCH와 후속 조회에 한정되며 워커·채널 발송·SDK 전체 비교가 아니다.

### 구독자 검색 API의 현재 범위

- `GET /v2/subscribers`에서 이메일·전화·구독자 ID의 정확한 일치, 이름의 대소문자 무시 리터럴 부분 검색, 내부 ID·수정 시각 정렬, 앞뒤 커서·커서 포함과 최대 50,000건의 카운트를 지원한다. 환경 범위를 적용하고 유효하지 않거나 다른 환경의 커서는 빈 결과를 반환한다. Docker PostgreSQL에서 검색·페이지 경계·다른 환경 격리를 검증했다.
- 기준 커밋 원본 API와 [검색 비교](apps/api/rust-api/tests/reference_subscriber_list_compare.py)를 실행해 필터·없는 커서·오류·수정 시각 기준 앞뒤 페이지를 비교했다. 독립 생성 ID와 시각은 값 비교에서 제외했다. `0014_experimental_subscriber_objectid.sql` 적용 후 신규 구독자는 시각·설치별 무작위 접두사·공유 카운터로 ID를 생성하며, 연속 생성의 기본 `_id` 오름·내림차순을 원본 API와 비교했다. 기존 ID는 변경하지 않으므로 이전 데이터의 생성 순서 차이는 남는다. 커서의 데이터 이전·동시 변경, Unicode 대소문자·1,000건 초과 페이지·고급 구독자 필드와 사용자 JWT/권한은 미검증 또는 미구현이다.

### 구독자 내부 ID 생성

- API 생성, Inbox 세션, 워커가 PostgreSQL의 공통 함수로 24자리 hex 내부 ID를 생성한다. 4바이트 초 단위 시각, 설치별 5바이트 무작위 접두사와 공유 3바이트 순번을 사용하고, 이전에 저장된 ID는 보존한다. 마이그레이션을 반복 적용해도 접두사는 유지된다.
- Docker PostgreSQL에서 연속 ID 2,000개의 형식·정렬, 서로 다른 연결 4개에서 동시 생성한 ID 1,000개의 유일성, API·워커 경로를 확인했다. 원본 API와는 단일 API 프로세스의 연속 생성 및 목록 정렬을 비교했다. 원본은 프로세스별 접두사를 사용하므로 다중 프로세스 생성의 상대 순서는 다를 수 있고, 시계 역행·카운터 순환·대규모 부하·기존 중복 ID 마이그레이션은 검증하지 않았다. 신규 또는 충돌하는 구독자 upsert마다 ID 생성 함수에서 작은 설정 테이블을 한 번 읽는다.

### 실험적인 전역 채널 설정

- `0015_experimental_global_preferences.sql`은 환경과 구독자별 전역 채널 값을 저장하고 구독자 삭제 시 외래 키로 함께 삭제한다. `GET/PATCH /v2/subscribers/:subscriberId/preferences`와 `GET .../preferences/global`은 현재 지원하는 단일 인앱 워크플로 범위에서 활성 인앱 채널과 전역 설정을 반환한다. 워커는 수신자별로 전역 `in_app=false`를 확인하여 메시지를 건너뛰고, 다시 활성화되면 새 작업을 처리한다. 구독자 행 잠금을 API와 워커가 같은 순서로 획득한다.
- Docker PostgreSQL에서 설정 기본값·저장·끄기/켜기 발송·환경 격리·삭제 연계를 검증했다. [원본 API 비교](apps/api/rust-api/tests/reference_global_preferences_compare.py)에서는 워크플로 리소스 설정이 없는 단일 인앱 fixture에 대해 설정 조회·변경·없는 구독자·인증을 비교했다. [원본 워커 비교](apps/api/rust-api/tests/reference_preference_delivery_compare.py)에서 전역 인앱 설정이 꺼진 일반 워크플로의 차단과 읽기 전용 중요 워크플로의 발송을 비교했다.
- 워크플로 리소스의 전체 활성·읽기 전용·인앱 활성 값과 구독자 설정의 병합은 명시적으로 설정된 단일 인앱 워크플로에서 지원한다. 워크플로 관리 API를 통한 리소스 설정, 스케줄·context별 설정, Inbox 설정 API, 다른 채널 발송, 설정 변경 웹훅·Socket.IO는 미구현이다. 워커의 설정 조회에 대한 부하·경합·원본과 동시성 비교는 수행하지 않았다.

- 워크플로 리소스 설정은 [0016 마이그레이션](apps/worker/rust-queue/migrations/0016_experimental_workflow_preferences.sql)의 명시적 데이터로 설정해야 한다. 단일 채널 병합 108개 조합을 [원본 병합 함수](apps/worker/rust-queue/tests/reference_preference.rs)와 직접 비교했고, 실제 원본 API·워커와 Rust API·워커에서 전역 차단, 읽기 전용 예외, 워크플로별 구독자 허용, 전역 변경으로 개별 설정 제거 후 차단을 비교했다. 원본 워크플로를 PostgreSQL로 자동 이전하거나 중요 표시만으로 해당 설정을 생성하지는 않는다.

### 실험적인 워크플로별 구독자 설정

- [0017 마이그레이션](apps/worker/rust-queue/migrations/0017_experimental_subscriber_workflow_preferences.sql)은 환경·구독자·워크플로별 채널 설정을 저장하고 구독자 또는 워크플로 삭제 시 이를 삭제한다. `PATCH /v2/subscribers/:id/preferences`의 `workflowId`로 단일 인앱 설정을 바꾸고, 전역 채널을 갱신하면 해당 채널의 워크플로별 값을 제거한다. 워커는 리소스 읽기 전용 → 구독자 워크플로 → 구독자 전역 → 리소스 기본값 순서로 인앱 발송을 결정한다.
- 기준 원본 API와 [설정 비교](apps/api/rust-api/tests/reference_workflow_preferences_compare.py)에서 워크플로 identifier·내부 ID·제공된 slug, 설정 조회·변경, 전역 변경, 중요 워크플로 거절, 없는 워크플로 오류를 비교했다. [실제 발송 비교](apps/api/rust-api/tests/reference_preference_delivery_compare.py)에서는 전역 차단·워크플로 허용·전역 변경 후 재차단과 읽기 전용 발송을 양쪽 워커에서 비교했다.
- 워크플로 메타데이터의 ID·이름·critical이 필요하며 slug 자동 생성은 제한된 소문자 ASCII 이름만 지원한다. 더 복잡한 이름은 사전에 slug를 제공해야 한다. 여러 워크플로 정렬, workflow/user 설정·스케줄·context 설정·다른 채널·완전한 검증 및 설정 웹훅은 미구현이다. 전체 원본 발송 상태와 이벤트는 아직 다르며 이 실험 구현은 운영용이 아니다.

- [0018 마이그레이션](apps/worker/rust-queue/migrations/0018_experimental_in_app_outcomes.sql)은 단일 인앱 단계의 수신자별 완료 또는 설정에 의한 `canceled`/`preference` 결과를 큐 완료 및 메시지 저장과 같은 트랜잭션에 기록한다. 원본의 작업처럼 구독자 삭제 후에도 결과를 보존하며, 재생성된 구독자와 구분할 내부 ID를 저장한다. 기존 작업에는 결과를 소급 생성하지 않는다. Docker PostgreSQL에서 혼합 수신자, 삭제 후 이력 보존 및 실패 시 결과 롤백을 확인했고, [원본 워커 비교](apps/api/rust-api/tests/reference_preference_delivery_compare.py)에서 설정 차단 사유와 혼합 수신자의 메시지 수를 비교했다. 성공 결과의 전체 원본 작업 필드는 비교하지 않았다.
- Rust 큐는 여러 수신자를 한 작업으로 처리하므로 그 큐 작업 자체는 전체 처리 성공 시 `completed`이고, 위 결과 테이블은 원본의 전체 jobs 모델이나 공개 API를 구현하지 않는다. 실패한 배치의 수신자별 실패·재시도·부분 성공, 작업 이벤트와 Socket.IO는 미구현이다. 결과 보존 기간·삭제 정책, 원본 작업 이력 이전 및 동시성/부하 검증도 남아 있다.

- 여러 워커의 인앱 배치가 같은 수신자를 다른 순서로 처리할 때 행 잠금 교착을 피하도록, 수신자별 PostgreSQL 트랜잭션 advisory lock을 해시 키 순서로 획득한다. 실제 구독자 갱신과 메시지 생성은 입력 순서대로 유지한다. 작업당 조회 1회와 고유 해시 키당 잠금 1개가 추가되며, 해시 충돌은 서로 다른 수신자 또는 환경의 작업을 불필요하게 직렬화할 수 있다. Docker PostgreSQL에서 반대 순서의 두 배치가 동시에 완료되는지 검증했다. 원본과 동시성·처리량 및 충돌이 많은 경우는 비교하지 않았다.
- 임대가 인앱 작업 처리 또는 advisory lock 대기 중 만료되면 해당 트랜잭션을 롤백하고 워커는 계속 실행한다. 기존의 주기적 만료 작업 복구가 이를 재대기열에 넣는다. 큐 완료·실패·재시도·임대 연장은 행 잠금 획득 후 실제 시각으로 임대를 재검사한다. Docker PostgreSQL에서 처리·잠금 대기 중 만료 후 롤백과 재처리, 그리고 네 큐 명령이 잠금 대기 중 만료된 임대를 거절하는 것을 검증했다. 장시간 경합 시 반복 만료나 무제한 재시도는 여전히 가능하며 원본의 재시도 정책과 비교한 것은 아니다.

- [0019 마이그레이션](apps/worker/rust-queue/migrations/0019_experimental_subscriber_online.sql)은 구독자의 온라인 여부와 마지막 오프라인 전환 시각을 저장한다. API 키를 사용하는 `PATCH /v1/subscribers/:id/online-status`와 v2 구독자 단건·목록 응답에 반영하며, 오프라인 변경 시 시각을 갱신하고 온라인 변경 시 기존 시각을 보존한다. Docker PostgreSQL에서 상태 전환·목록 반영·환경 격리를 확인했고, [원본 API 비교](apps/api/rust-api/tests/reference_subscriber_online_compare.py)에서 반복 전환·후속 단건 및 목록 조회·없는 구독자·인증 및 입력 오류를 비교했다. 독립 생성 ID와 시각의 값은 비교하지 않았다. MongoDB의 기존 상태를 이관하지 않고 v1의 기존 MongoDB `__v` 값이나 명시적 null과 필드 부재를 재현하지 않는다. Socket.IO 연결 기반 자동 상태 변경, 사용자 JWT 권한과 관련 이벤트는 미구현이다.

- [0020 마이그레이션](apps/worker/rust-queue/migrations/0020_experimental_topics.sql)은 환경별 토픽 및 구독 관계를 저장한다. API 키로 `POST /v1/topics/:key/subscribers`와 `POST /v1/topics/:key/subscribers/removal`을 사용하며, 추가 시 없는 토픽은 자동 생성되고 없는 구독자는 `failed.notFound`로 보고한다. `GET /v1/subscribers/:id?includeTopics=true`는 해당 관계를 조회하며 구독자 삭제 시 관계를 정리한다. Docker PostgreSQL에서 환경 격리와 삭제 연계를 확인했고, [원본 비교](apps/api/rust-api/tests/reference_topics_compare.py)에서 자동 생성·중복 추가·없는 구독자·제거·구독자 조회·인증과 일부 입력 오류를 비교했다.
- v1 토픽 생성·단건 조회·이름 변경·삭제와 토픽 구독자 개별 조회를 지원한다. 구독자가 남은 토픽의 삭제는 충돌을 반환한다. [0021 마이그레이션](apps/worker/rust-queue/migrations/0021_topic_delete_restrict.sql)은 삭제와 구독 추가의 경합에서도 기존 구독을 보존하도록 외래 키를 강화한다. 선택된 정상·오류 사례는 [원본 API 비교](apps/api/rust-api/tests/reference_topics_compare.py)에서 확인했다. v2 토픽 구독 생성·목록·단건 조회·이름 수정·삭제 이외의 구독 API와 사용자 JWT 권한은 미구현이다. 토픽 대량 목록, 모든 동시성 경계 및 기존 MongoDB 데이터 이전은 검증하지 않았다. v1 구독자 조회는 현재 모델에 없는 원본 필드와 기존 `__v` 값, 명시적 null 시각을 복원하지 못한다.

- `GET /v1/topics`는 환경별 정확한 key·name 필터와 0부터 시작하는 페이지, 최대 10의 페이지 크기, 전체 수 및 구독자 목록을 반환한다. 원본에서 `pageSize=0`은 500을 반환하며 동일한 응답 형식을 재현한다. Docker PostgreSQL에서 페이지 경계·환경 격리를, [원본 비교](apps/api/rust-api/tests/reference_topics_compare.py)에서 선택된 페이지·필터·인증·입력 오류를 확인했다. 내부 ID 오름차순으로 결과를 반환하지만 원본은 명시적 정렬 없이 MongoDB 순서를 사용하므로 이관 데이터 및 다중 프로세스 순서가 다를 수 있다. 0이 아닌 정수 외의 JavaScript 숫자 표기·중복 쿼리·대량 결과·동시 변경은 미검증 또는 미지원이다.

- [0022 마이그레이션](apps/worker/rust-queue/migrations/0022_experimental_topic_data.sql)은 토픽 이름의 부재·null 구분과 사용자 데이터를 저장한다. `POST /v2/topics`의 생성·upsert·`failIfExists`, `GET/PATCH /v2/topics/:key`의 조회·수정 및 `data: null` 삭제를 지원하며 v1 조회·목록에서도 같은 데이터를 보여준다. 선택된 입력·오류 및 v1/v2 교차 조회는 [원본 API 비교](apps/api/rust-api/tests/reference_topics_v2_compare.py)로 확인했다. 원본 실행에서 기존 토픽 upsert의 HTTP 상태도 201이다. 대형 숫자의 JavaScript 직렬화 차이, 복잡한 입력·전체 검증 오류, API 키 외 권한 및 기존 MongoDB 데이터 이전은 검증하지 않았다.

- `DELETE /v2/topics/:key`는 원본 컨트롤러처럼 구독 관계가 있어도 토픽과 관계를 함께 삭제하고 구독자 및 다른 환경·토픽은 보존한다. 원본과 선택된 삭제·인증·반복 삭제·후속 조회를 비교했다. PostgreSQL에서는 한 문장의 트랜잭션으로 삭제하며, 구독 추가와 경합해 외래 키 충돌이 발생하면 새 스냅샷으로 최대 세 차례 시도한다. Docker 테스트에서 삭제 실패 시 관계 롤백과 동시 구독 추가 경합을 확인했다. 계속되는 경합이 재시도 횟수를 넘으면 500을 반환하며, 원본 MongoDB 삭제와 동시성·대기 작업·웹훅의 전체 동작은 비교하지 않았다.

- `GET /v2/topics`는 key·name의 대소문자 무시 리터럴 부분 검색, `_id`·생성/수정 시각·key·name 정렬과 전후 커서, 커서 포함 및 최대 50,000건 카운트를 지원한다. Docker PostgreSQL에서 동시간 항목·환경 격리·커서 경계를 확인했고, [원본 비교](apps/api/rust-api/tests/reference_topics_v2_list_compare.py)에서 선택된 검색·오름/내림차순·전후 페이지·null 이름·동일 이름 포함 커서 및 오류를 비교했다. 원본 MongoDB의 타입별 비교 때문에 null 이름 커서에서 문자열 이름으로 이동하지 않는 동작도 재현한다. `includeCursor=true`에서는 원본처럼 같은 정렬값을 가진 커서 이전 항목도 포함될 수 있다. 독립 생성 ID와 시각은 값 비교에서 제외했다. 원본과 PostgreSQL의 Unicode 대소문자 처리·모든 커서 오류 및 숫자 입력·50,000건 초과 부하·동시 변경·데이터 이전의 순서 일치는 미검증이다.

- 단일 인앱 워크플로 트리거에서 `type: "Topic"` 수신자, 여러 토픽, 직접 수신자와 토픽 제외 목록을 지원한다. 워커가 작업을 처리할 때 환경별 구독 관계를 조회하고, 직접 수신자를 우선하며 토픽 간 중복을 제거한다. 원본처럼 토픽 제외 목록은 모든 토픽에 적용되고 직접 지정된 수신자는 제외하지 않는다. 토픽이 있는 작업마다 구독자 조회가 1회 추가된다. Docker PostgreSQL에서 요청 접수 후 구독 관계를 바꿔 워커 실행 시 반영되는 것과 없는 토픽의 빈 결과를 확인했다. [원본 API·워커 비교](apps/api/rust-api/tests/reference_topic_delivery_compare.py)에서 단일·중복 토픽, 직접 수신자 혼합 및 제외 목록을 실제 발송된 수신자 기준으로 비교했다.
- 토픽 대상 발송은 실험용 단일 인앱 단계에 한정된다. 원본의 토픽 context 필터·토픽 관련 trace와 단계 메타데이터는 미구현이고, 대량 팬아웃의 페이지 처리 및 동시 구독 변경은 미검증이다. PostgreSQL 구현은 구독자를 한 번에 조회해 단일 배치로 처리하므로 원본의 페이지별 비동기 작업, 부분 실패와 관측 시점이 다르며, 구독 변경 타이밍 자체는 원본과 종단 간 비교하지 않았다. 없는 토픽 처리의 원본 작업 완료 여부도 별도로 비교하지 않았다.

- 단일 인앱 트리거의 `actor`는 문자열 ID 또는 지원되는 구독자 프로필로 지정할 수 있다. 워커가 actor를 생성·갱신하고 토픽 확장에서는 제외하되 직접 지정된 수신자로는 발송한다. 템플릿에서 지원되는 `actor` 속성을 사용할 수 있다. Docker PostgreSQL에서 actor가 직접 수신자인 경우와 프로필 반영을 검증했고, [원본 워커 비교](apps/api/rust-api/tests/reference_topic_delivery_compare.py)에서 새 actor 생성·토픽 제외·직접 수신 및 `actor.firstName` 렌더링을 비교했다. actor가 있으면 작업당 구독자 upsert 1회와 advisory lock 대상 1개가 추가된다. 구독자 생성 및 메시지 저장은 같은 배치 트랜잭션이므로 개별 단계로 처리되는 원본과 실패·동시성 시점이 다를 수 있다. actor의 메시지 DTO·아바타 설정, 구독 조건·다른 채널·모든 입력 검증은 미구현이다.

### 토픽 구독 식별자와 v2 단건 조회

- [0023 마이그레이션](apps/worker/rust-queue/migrations/0023_experimental_topic_subscriptions.sql)은 구독마다 내부 ID·환경별 고유 identifier·이름·context 키·수정 시각을 저장해 같은 토픽과 구독자의 복수 구독을 허용한다. 기존 v1 관계는 원본의 `tk_{topic}:si_{subscriber}` 식별자로 이관하며 식별자가 충돌하면 트랜잭션을 롤백하고 운영자가 충돌을 해소해야 한다. 고유 인덱스로 구독 생성마다 인덱스 쓰기가 추가된다. v1 구독자 토픽 목록은 토픽을 중복 제거하고, 토픽 조회는 원본처럼 구독별 구독자 항목을 반환하며, v1 제거는 해당 구독자의 모든 관계를 삭제한다.
- `GET /v2/topics/:key/subscriptions/:identifier`는 API 키 인증, 이름과 context 키 반환, 없는 구독자의 204 및 원본의 context 기능 플래그가 꺼진 경우 접미사 제거를 지원한다. [원본 비교](apps/api/rust-api/tests/reference_topic_multiple_subscriptions_compare.py)는 양쪽 v2 API로 별도 식별자의 구독을 생성해 단건 조회와 v1 교차 조회·중복 추가·제거를 비교한다. 생성된 내부 ID는 형식만 확인했다. Docker PostgreSQL 통합 테스트에서 기존 관계 이관, 마이그레이션 재실행, 식별자 충돌 시 롤백과 환경별 관계 삭제를 확인했다. [원본 워커 비교](apps/api/rust-api/tests/reference_topic_delivery_compare.py)에서는 같은 구독자에 서로 다른 구독이 두 개 있을 때도 선택된 인앱 토픽 발송 결과가 일치함을 확인했다. 이 워커 비교는 양쪽 v2 API를 통해 두 번째 관계를 생성한다.
- 전체 v2 구독 설정 생성·수정, 모든 채널의 구독별 설정 발송, context 기능 플래그가 켜진 동작과 사용자 JWT 권한은 아직 구현하지 않았다. 전체 v2 발송 계약과 MongoDB 데이터의 실제 이전도 검증하거나 제공하지 않는다.

- `POST /v2/topics/:key/subscriptions`는 API 키 인증과 기본 context 기능 플래그가 꺼진 범위에서 `subscriberIds` 또는 식별자 객체의 생성, 기존 구독 재요청, 부분·전체 실패 및 구독자별 토픽당 10개 제한을 지원한다. 새 구독 생성 응답의 시각은 원본처럼 빈 문자열이며 재요청 시 저장된 시각을 반환한다. [원본 비교](apps/api/rust-api/tests/reference_subscription_create_compare.py)는 기본·사용자 식별자, 재요청, 부분/전체 실패, 과도한 식별자, 입력 및 인증 오류를 비교한다. context는 501로 거절하며, preferences는 아래에 기술한 범위를 지원한다. 중복 식별자가 다른 구독자나 토픽에 속한 경우와 동시 생성 충돌도 현재 501로 처리하며 원본과 다르다. 구독 수 제한의 동시 경합, 모든 입력 오류, 대량 처리 및 부분 실패 시 DB 오류의 원본 동작은 미검증이다. 이 관리 API는 토픽 upsert와 구독자 조회 외에 발견된 구독자마다 최대 세 번의 DB 쿼리를 수행하며, 발송 핫패스에 쿼리를 추가하지 않는다.

- `GET /v2/topics/:key/subscriptions`는 API 키와 환경별 토픽 확인 후 구독자와 context 키의 정확한 일치, 구독 ID 기준 오름·내림차순, 앞뒤 커서와 포함 옵션 및 50,000건 상한 카운트를 지원한다. 원본은 `orderBy` 값을 받아도 이 목록에서는 구독 ID로 정렬하며 같은 동작을 따른다. [원본 비교](apps/api/rust-api/tests/reference_subscription_list_compare.py)에서 양쪽이 발급한 커서로 페이지를 왕복하고 조회 필터·오류·인증을 비교했다. context가 있는 관계는 양쪽 테스트 DB에 직접 준비했으므로 context를 통한 구독 생성과 발송은 검증하지 않았다. [0024 인덱스](apps/worker/rust-queue/migrations/0024_experimental_topic_subscription_cursor.sql)는 환경·토픽·구독 ID 조회를 지원하며 구독 삽입과 삭제마다 인덱스 쓰기가 추가된다. 목록 호출은 토픽 조회·상한 카운트·페이지 조회와 필요 시 커서 조회 및 반대편 페이지 확인을 수행한다. 대량 데이터·동시 변경·특이한 숫자 입력과 사용자 JWT 권한은 미검증 또는 미구현이다.

- `PATCH /v2/topics/:key/subscriptions/:identifier`는 API 키 인증으로 구독 이름을 수정하고 미전달·빈 문자열·명시적 null을 구분한다. 내용이 없으면 수정 시각을 유지하고 이름 변경 시 갱신하며, 기본 context 기능 플래그가 꺼진 경우 원본처럼 식별자의 context 접미사를 제거한다. [0025 마이그레이션](apps/worker/rust-queue/migrations/0025_experimental_subscription_name_presence.sql)은 이름 존재 여부를 저장하고 이전의 비 null 이름을 보존한다. 이전 SQL null이 원래 부재였는지 명시적 null이었는지는 복원할 수 없어 부재로 처리한다. [원본 비교](apps/api/rust-api/tests/reference_subscription_update_compare.py)에서 생성·이름 변경·null·후속 조회·빈 요청의 시간 유지·인증과 선택된 오류를 비교했다. 구독 preferences 교체는 아래에 기술한 제한된 워크플로 모델을 지원하며 빈 배열은 해당 구독과 구독자에 연결된 설정을 삭제한다. 나머지 구독 설정 생성·변경, context 기능 플래그가 켜진 모드, 사용자 JWT 및 동시 수정은 미구현 또는 미검증이다. 이 관리 경로는 토픽 조회 1회와 구독 조회 또는 갱신 1회를 사용하고 발송 핫패스에는 쿼리를 추가하지 않는다.

- `DELETE /v2/topics/:key/subscriptions`는 API 키 인증으로 식별자별 구독 또는 구독자별 모든 구독을 삭제하고 부분·전체 실패를 보고한다. 식별자와 구독자를 함께 요청하면 원본처럼 두 조회 결과를 합쳐 동일 구독이 응답에 중복될 수 있지만 PostgreSQL 행은 한 번 삭제한다. [원본 비교](apps/api/rust-api/tests/reference_subscription_delete_compare.py)는 트랜잭션이 가능한 Docker MongoDB 복제 세트에서 선택된 삭제·혼합 요청·오류·후속 조회를 비교한다. Docker PostgreSQL에서는 외래 키 충돌로 삭제가 실패할 때 전체 관계가 보존되고 다른 환경이 격리되는 것을 확인했다. [원본 워커 비교](apps/api/rust-api/tests/reference_topic_delivery_compare.py)에서 구독 삭제 후 인앱 발송 수신자도 비교했다. 구독별 워크플로 설정도 관계 삭제와 같은 PostgreSQL 문장에서 제거하며 원본의 트랜잭션과 선택된 사례를 비교했다. 구독자·토픽과 다른 환경은 보존한다. 조회 및 응답 구성은 삭제 문장과 별도이므로 동시성·원본과의 실패 시점, 모든 입력 검증 및 사용자 JWT 권한은 미검증 또는 미구현이다. 이 관리 API는 토픽 조회와 최대 두 관계 조회·구독자 조회 후 삭제 한 번을 수행하며 발송 핫패스에 쿼리를 추가하지 않는다.

- [0026 마이그레이션](apps/worker/rust-queue/migrations/0026_experimental_subscription_preferences.sql)은 환경·내부 구독자·구독·워크플로·context 해시별 원본 형식의 설정 JSON을 저장하고, v2 구독 삭제 시 해당 설정과 관계를 함께 삭제한다. 외래 키 충돌로 삭제가 실패하면 설정과 구독이 함께 롤백된다. 빈 preferences 배열로 PATCH하면 구독자까지 일치하는 설정만 제거한다. [원본 비교](apps/api/rust-api/tests/reference_subscription_preference_lifecycle_compare.py)는 양쪽 테스트 DB에 설정을 직접 준비해 구독 단건 조회·워크플로 메타데이터·빈 설정 초기화 및 v1 제거/토픽 삭제 후 설정 보존을 확인했다. 원본의 후자 동작에 맞춰 구독 외래 키를 두지 않으므로 고아 설정이 남을 수 있다. 단건 GET에 설정 조회 1회와 설정 인덱스 쓰기가 추가된다. 워커에서 추가되는 조회 비용은 아래에 기술한다. API 생성·수정과 워커 평가의 지원 범위는 아래에 기술한다. context 해시 생성, 데이터 이관, 모든 메타데이터와 동시성은 미검증 또는 미구현이다.

- 단일 인앱 워커의 토픽 확장은 각 구독의 워크플로 설정을 수신자·환경에 맞춰 함께 조회한다. 복수 구독 중 하나라도 통과하면 발송하고, 모두 차단되면 발송하지 않는다. 조건이 있으면 JSON Logic을 평가해 불리언 true만 허용하며 조건이 없을 때 all.enabled를 사용한다. 직접 지정한 수신자는 토픽 설정을 적용하지 않는다. [원본 워커 비교](apps/api/rust-api/tests/reference_topic_subscription_preference_delivery_compare.py)는 양쪽 API로 설정을 생성·변경해 조건 참·거짓·비불리언·구독자 속성·복수 구독·직접 수신자를 비교한다. 잘못 연결된 타 구독자의 설정만 DB에 직접 준비했다. [JSON Logic 비교](apps/worker/rust-queue/tests/reference_subscription_jsonlogic.rs)는 선택된 연산을 원본 JavaScript 구현과 비교했다. 전체 JSON Logic·JavaScript 숫자 및 문자열 의미, 모든 subscriber/actor 필드, context 플래그 활성화, 원본 trace·작업 기록과 대량 팬아웃은 미검증이다. 설정 대상 워크플로의 내부 ID가 없으면 잘못 발송하지 않도록 해당 작업을 실패 처리하며 원본과 다르다. 구독 설정 API의 전체 계약과 다른 채널은 미구현이다. 토픽이 있는 작업마다 기존 확장 조회에 구독마다 최대 한 번의 인덱스 설정 조회가 추가되며, 토픽 구독 관계 수에 비례해 DB 작업과 메모리 사용이 늘어난다.

- `POST /v2/topics/:key/subscriptions`의 preferences와 `PATCH /v2/topics/:key/subscriptions/:identifier`의 preferences는 워크플로 식별자·내부 ID·태그 필터, enabled 및 JSON Logic 조건의 선택된 범위를 지원한다. 새 구독 생성 시 설정을 만들고 기존 구독에 대한 생성 재요청은 설정을 보존하며, PATCH는 기존 설정을 새 목록으로 교체한다. 존재하지 않는 워크플로만 지정해 PATCH하면 기존 설정이 제거된다. [원본 API 비교](apps/api/rust-api/tests/reference_subscription_preference_api_compare.py)는 생성·재요청·복수 워크플로·조건·태그·워크플로 기본 활성값·초기화와 선택된 오류를 비교한다. [원본 워커 비교](apps/api/rust-api/tests/reference_topic_subscription_preference_delivery_compare.py)는 양쪽 API에서 설정을 생성·변경한 뒤 단일 인앱 발송을 비교한다. 내부 ID나 태그 검색에 필요한 워크플로 메타데이터가 누락되면 501로 거절하며 원본과 다르다. 전체 검증 오류·조건 연산·context 활성화·모든 워크플로 종류 및 채널, 동시성은 미검증 또는 미구현이다. 설정 요청마다 환경의 워크플로 전체를 조회하며, 새 구독은 선택된 워크플로별 쓰기 1회, 기존 구독은 설정 조회 1회, 수정은 설정 삭제 1회와 선택된 워크플로별 삽입 1회가 추가된다. 원본과 같이 설정 삭제, 개별 설정 생성 및 이름 변경이 별도 DB 문장이므로 장애·동시 변경 시 전체 작업은 원자적이지 않고 이전 설정이 사라지거나 일부 새 설정만 남을 수 있다.

- 구독 설정 PATCH는 원본의 순서대로 기존 설정을 삭제하고 각 워크플로 설정을 생성한 뒤 이름을 변경한다. [원본 장애 비교](apps/api/rust-api/tests/reference_subscription_preference_api_compare.py)는 격리된 MongoDB 복제 세트의 failpoint와 PostgreSQL 제약으로 설정 삽입과 이름 갱신을 각각 실패시켜 HTTP 500 본문과 이후 설정·이름 상태를 비교했다. 오류 ID와 시각의 실제 값은 비교하지 않았다. 두 단계는 트랜잭션으로 묶이지 않아 이전 설정이 삭제되거나 일부 새 설정만 생성된 상태가 남을 수 있다. 같은 설정의 동시 삽입으로 유일 제약이 충돌하면 원본처럼 이미 저장된 값을 읽어 응답하며, PostgreSQL에서 경합을 재현했다. 모든 동시 삭제·생성 조합 및 다중 워크플로 중간 실패의 원본 비교는 미수행이다.

- `GET /v2/subscribers/:subscriberId/subscriptions`는 구독자 존재 확인 후 토픽 키·context 키 필터, 구독 ID 기준 전후 커서와 50,000건 상한 카운트를 지원한다. 페이지에 포함된 구독별 워크플로 설정은 한 번의 조회로 가져오며, 구독자·환경이 일치하지 않는 설정은 제외한다. [원본 비교](apps/api/rust-api/tests/reference_subscriber_subscriptions_compare.py)는 여러 토픽과 구독, 구독 설정, 양방향 커서, 필터, 인증과 선택된 오류를 비교한다. context가 있는 구독과 잘못 연결된 설정은 양쪽 테스트 DB에 직접 준비했다. [0027 인덱스](apps/worker/rust-queue/migrations/0027_experimental_subscriber_subscription_cursor.sql)는 환경·구독자별 구독 ID 순회를 돕고 관계 삽입·삭제마다 인덱스 쓰기가 추가된다. 목록은 구독자 확인·상한 카운트·페이지 조회, 필요 시 커서·반대 페이지 확인, 비어 있지 않은 페이지의 설정 일괄 조회를 수행한다. context 플래그가 켜진 설정 조회, 사용자 JWT 권한, 전체 입력 오류·메타데이터·대량 및 동시 변경은 미구현 또는 미검증이다.

- [0028 마이그레이션](apps/worker/rust-queue/migrations/0028_experimental_snoozed_until.sql)은 메시지에 스누즈 시각을 저장한다. 관리 API와 구독자 JWT Inbox의 목록·카운트 및 개별 읽음 등 변경 응답은 저장된 시각을 반영한다. 원본과 같이 시각이 과거라도 값이 존재하면 자동 해제 작업이 지울 때까지 스누즈로 판단한다. [원본 API·워커 비교](apps/api/rust-api/tests/reference_snoozed_read_compare.py)에서 양쪽 워커가 생성한 메시지에 과거·미래 시각을 DB에 직접 설정한 뒤 목록, 카운트, JWT 접근 및 읽음 응답의 선택된 필드를 비교했다. 관리 API와 JWT Inbox의 수동 해제는 읽음 상태를 유지하고 확인 처리하며 보관을 해제한다. 비교 테스트는 예약 작업 없이 시각을 직접 설정한 경우의 정상 해제·반복 해제·없는 알림과 선택된 응답 필드를 검증했다. 완전한 실시간 재전달 이벤트, 원본과의 모든 동시 변경 및 전체 DTO 비교는 미구현 또는 미검증이다. 기존 메시지에는 기본값 NULL을 사용하며 읽기 경로에서 추가 DB 왕복은 없다.

- [0029 마이그레이션](apps/worker/rust-queue/migrations/0029_experimental_delivery_history.sql)은 인앱 메시지의 여러 전달 시각을 순서대로 보존하고 목록과 개별 상태 변경 DTO에 반환한다. 배열이 NULL이면 기존 단일 전달 시각을 사용하고 빈 배열은 그대로 반환한다. 원본의 자동 스누즈 해제는 전달 이력에 시각을 추가하므로 예약 처리에 필요한 데이터 모델이다. [원본 비교](apps/api/rust-api/tests/reference_snoozed_read_compare.py)는 양쪽 워커가 만든 메시지의 이력을 DB에 직접 설정해 복수 시각·빈 배열과 읽음 처리 응답을 비교했다. 전체 재전달 이벤트는 아직 구현·검증하지 않았다. 목록 및 변경 쿼리는 각 행에서 배열을 문자열로 변환하며 추가 DB 왕복은 없다.

- 실험 인앱 워커는 `experimental-in-app-unsnooze` 대기 작업의 예정 시각이 되면 구독자를 잠그고 스누즈를 해제해 읽지 않음·전달 시각 이력 추가·생성 시각 갱신과 작업 완료를 하나의 PostgreSQL 문으로 처리한다. 수동 해제는 같은 구독자 잠금 순서를 사용하고 일치하는 대기 작업 하나를 취소한다. Docker PostgreSQL에서 두 워커의 중복 처리 방지, 수동 해제 경합, 작업 완료 실패 시 상태 롤백과 삭제된 구독자 작업 실패를 검증했다. [원본 워커 비교](apps/api/rust-api/tests/reference_snoozed_read_compare.py)는 양쪽 API로 생성한 실제 지연 작업의 자동 해제·전달 이력과 중복 스누즈 뒤 수동 취소를 비교한다. 원본은 수동 해제 시 작업을 삭제하지만 Rust는 취소 상태로 남긴다. 원본의 재시도·실행 상세, 완전한 Socket.IO 수신 이벤트, 다중 메시지와 기타 실패·동시 경계는 미구현 또는 미검증이다. 워커는 인앱 작업 확인 전 예약 작업을 조회하며 대기 작업이 없으면 고아 작업 확인 쿼리도 실행한다.

- 관리 API와 JWT Inbox의 스누즈 요청은 알림 상태와 예약 작업을 원자적으로 기록한다. RFC 3339 시각과 밀리초 정수, 최소 60초 및 기본 무료 요금제의 24시간 제한을 지원한다. [요금제 마이그레이션](apps/api/rust-api/migrations/0003_experimental_snooze_policy.sql)의 조직별 최대 밀리초를 직접 설정할 수 있고 NULL은 무제한이며, 원본 구독 요금제와 자동 동기화하지 않는다. 원본과 생성·중복 생성·수동 취소·자동 해제, 기본 제한 및 선택된 입력 오류를 비교했고, PostgreSQL에서 큐 삽입 실패 시 상태 롤백과 직접 설정한 조직별 한도를 확인했다. JavaScript Date의 모든 입력 변환, 전체 유효성 오류, 실제 요금제 연동, 실시간 이벤트·웹훅·분석·추적과 모든 동시 요청은 미구현 또는 미검증이다. 스누즈마다 요금제 조회 1회와 상태 변경·큐 삽입 문 1회를 수행한다.

- [실험 Rust WS 서비스](apps/ws/rust-ws/README.md)는 Socket.IO websocket·polling 연결, HS256 구독자 JWT의 인증, 구독자 내부 ID room 및 연결·해제 시 온라인 상태를 지원한다. 연결 기록을 PostgreSQL에 저장하여 별도 프로세스 간 상태를 공유하고 구독자 행 잠금으로 동시 상태 변경을 직렬화한다. [비교 테스트](apps/ws/rust-ws/scripts/test-reference-gateway.sh)는 원본 gateway와 온라인 상태 서비스의 소스 코드를 실제 Socket.IO 서버·클라이언트에 연결하되 원본 저장소·Nest 의존성·관측 모듈을 대체해 JWT 오류·인증 우선순위·전송 방식·다중 연결 상태를 비교했다. 두 Rust 프로세스 간 상태 공유는 별도로 확인했다. 원본 WS 앱 전체, MongoDB·Redis adapter 및 SDK 전체를 실행한 비교가 아니며, 서버 종료·비정상 중단 후 연결 기록 정리, 다른 JWT 알고리즘·모든 claim 형태, 전체 이벤트 종류 및 재연결 복구는 미구현 또는 미검증이다. 프로세스가 비정상 종료되면 연결 기록이 남아 온라인 상태가 틀릴 수 있다. 연결마다 삽입·상태 변경, 해제마다 삭제·연결 수 조회·상태 변경 트랜잭션을 수행한다.

- [0030 이벤트 마이그레이션](apps/worker/rust-queue/migrations/0030_experimental_ws_events.sql)은 이벤트 본문을 PostgreSQL에 저장하고 `NOTIFY`에는 UUID만 보낸다. 인앱 워커는 새 메시지 저장 및 자동 스누즈 해제와 같은 트랜잭션에 이벤트 기록을 시도하며, 각 WS 인스턴스는 수신자·환경·context가 일치하는 로컬 소켓에 제한적인 `notification_received`, `unseen_count_changed`, `unread_count_changed`를 전달한다. [gateway 비교](apps/ws/rust-ws/tests/reference_gateway_compare.cjs)는 원본 gateway와 context 순서·중복·불일치, 다른 수신자, 8KB를 넘는 본문 및 두 Rust 인스턴스의 fan-out을 비교했다. [발송 테스트](apps/ws/rust-ws/tests/worker_delivery.cjs)는 양쪽 API·워커가 만든 메시지의 선택된 원본 필드를 원본 gateway를 통해 비교하고 Rust API → 워커 → WS → 원본 빌드 `@novu/js`의 수신을 확인했다. 자동 해제 이벤트는 테스트 DB에서 예약 시각을 앞당겨 검증했다. 원본 WS worker의 전체 발송 경로와 전체 payload, 모든 SDK 기능 및 전체 severity는 미검증 또는 미구현이다. 원본 gateway는 내부 ID room을 기준으로 라우팅하지만 Rust는 추가로 환경을 확인한다. `LISTEN/NOTIFY`는 연결 중단 시 이벤트를 재전송하지 않으며 이벤트 테이블의 정리 정책이 없어 행이 계속 쌓인다. 생성 메시지마다 이벤트 삽입 1회가 추가되고 WS 인스턴스마다 이벤트 조회, 일치하는 수신자에 대해 메시지와 카운트 조회가 추가된다.

- 읽음·읽지 않음·보관·보관 해제·스누즈·해제·삭제와 일괄 변경은 `unread_count_changed`, 확인 처리는 `unseen_count_changed`를 발행한다. 빈 확인 ID 배열은 원본처럼 이벤트를 발행하지 않는다. [원본 비교](apps/ws/rust-ws/tests/worker_delivery.cjs)는 원본 API가 Redis WS 큐에 넣은 이벤트 종류와 context를 확인하고 Rust Socket.IO 및 원본 빌드 SDK가 받은 카운트와 선택된 context 격리를 검증한다. 원본 WS worker 자체의 전체 경로는 비교하지 않았다. [0031 마이그레이션](apps/worker/rust-queue/migrations/0031_experimental_ws_best_effort.sql)은 원본 WS 큐의 best-effort 정책처럼 이벤트 저장 실패 시 알림 변경이나 워커 발송은 성공시키고 이벤트만 버린다. PostgreSQL 제약으로 실패를 주입해 API·워커에서 이 동작을 확인했다. 이 정책은 이벤트 유실 가능성이 있으며 원본처럼 별도 경고 로그를 남기지 않는다. 요청마다 이벤트 삽입과 실패 격리용 DB 서브트랜잭션이 추가되고, 연결된 수신자가 있으면 WS 인스턴스마다 상한 있는 카운트 조회가 추가된다. 전체 필터·severity·고부하·장애 조합과 다른 이벤트 부수효과는 미검증이다.

- 실시간 인앱 메시지의 원시 이벤트에 저장된 조직 ID와 설정된 워크플로 ID, 빈 context·device token, 인앱 provider와 MongoDB 버전 기본값을 반영했다. 첨부파일은 이벤트 payload에서 제거하고 빈 객체는 생략한다. [발송 비교](apps/ws/rust-ws/tests/worker_delivery.cjs)는 빈 payload의 선택된 원시 필드와 각 서버가 발급한 세션을 이용한 양쪽 `@novu/js`의 선택된 알림 필드를 비교한다. 독립 생성 ID와 시각은 형식만 확인한다. 원본 MongoDB 메시지를 원본 gateway에 수동 전달하므로 원본 WS worker 전체 경로의 종단 간 검증이 아니며, 다양한 워크플로와 첨부파일의 전체 발송 경로는 아직 원본 비교가 필요하다. 원본 메시지의 notification/job/message-template ID와 updatedAt은 Rust 이벤트에 없고, provider는 현재 실험 인앱 구현의 `novu` 고정값이다.

- 인앱 워커는 렌더링에 원래 payload를 사용한 뒤 저장 메시지에서는 첨부파일을 제거하고 중첩 빈 객체를 제거한다. 배열의 빈 원소는 유지한다. 과거 프로토타입 행은 수정하지 않고 WS 전송 시에만 동일 정규화를 적용한다. [발송 비교](apps/ws/rust-ws/tests/worker_delivery.cjs)에서 일반 값, 빈 중첩 객체와 배열을 양쪽 실제 API·워커에 전달해 원본 MongoDB 메시지와 Rust PostgreSQL 저장값 및 Socket.IO payload를 비교했다. 이 정규화는 메시지당 payload 복사와 순회를 추가한다. 첨부파일이 포함된 트리거는 Rust API가 501로 거절하므로 원본의 첨부파일 검증·저장 업로드와 전체 발송은 미구현이며, 첨부파일 제거는 함수 단위 테스트만 수행했다. 대형 payload의 성능은 검증하지 않았다.

- 기존 실험 행의 payload가 정규화되지 않은 경우에도 Inbox DTO에서 payload를 `data`로 사용하는 기존 유형(v0) 워크플로의 응답을 정규화한다. 실제 원본 API·워커와 Rust를 비교해 중첩 빈 객체·배열을 포함한 새 메시지와 정규화 전 행의 목록 및 읽음 응답, 명시적 메시지 `data`가 payload보다 우선하는 동작을 확인했다. 해당 비교는 단일 `REGULAR` 워크플로 fixture와 선택된 payload에 한정되며, 전체 워크플로 유형과 과거 저장 행 이관은 검증하지 않았다. 이 DTO 변환은 응답마다 payload 복사와 순회를 수행한다.

- WS의 PostgreSQL 이벤트 전용 리스너 연결이 종료되면 1초 간격으로 재연결하고 `LISTEN`을 다시 설정한다. [gateway 테스트](apps/ws/rust-ws/tests/reference_gateway_compare.cjs)는 두 Rust WS 프로세스의 리스너를 PostgreSQL에서 종료하고 두 연결이 재구독한 후 기존 소켓에 이벤트가 전달되는 것을 확인했다. 최초 서버 시작은 리스너 설정 완료를 기다린다. 연결이 끊긴 동안의 이벤트 재생은 없으며, 별도의 조회·presence DB 연결도 닫힘을 감지하면 다음 작업 시 다시 연결한다. 이벤트 테이블 보존·정리와 비정상 종료 시 stale 연결 정리는 여전히 미구현이다. 재시도 중 프로세스마다 초당 최대 한 번 DB 연결을 시도한다.

- WS 조회·presence용 PostgreSQL 연결은 닫힌 경우 다음 작업에서 재연결한다. [gateway 테스트](apps/ws/rust-ws/tests/reference_gateway_compare.cjs)는 두 프로세스의 조회 연결 종료와 격리 PostgreSQL 컨테이너 전체 재시작을 주입하고, 리스너 재구독 후 기존 소켓 이벤트 전달과 새 소켓의 presence를 확인했다. 이 테스트는 Rust 장애 복구 검증이며 원본 전체 WS 앱과의 장애 동등성 비교는 아니다. 이미 실행 중 실패한 일반 쿼리는 자동 재시도하지 않는다. 연결 해제 정리는 아래의 별도 재시도 정책을 사용한다. 연결 손실을 감지하기 전 첫 작업이 실패할 수 있고, 연결 복구 전 이벤트의 재생은 없다.

- DB 장애 중 소켓이 끊기면 연결 기록 삭제와 온라인 상태 갱신을 메모리에서 재시도한다. 재시도는 1초부터 최대 30초까지 지수적으로 지연하고, 해제 시점에 판정한 JWT 유효성을 유지한다. 삭제는 이미 처리된 연결 ID에 대해 상태를 다시 변경하지 않아 불확실한 커밋 뒤 재시도할 수 있다. [gateway 테스트](apps/ws/rust-ws/tests/reference_gateway_compare.cjs)는 격리 PostgreSQL을 중단하고 유효한 토큰의 소켓을 해제한 뒤 토큰 만료와 DB 복구를 거쳐 연결 기록 삭제·오프라인 변경을 확인했다. 원본 WS 서비스와 장애 시 동등성을 비교한 테스트는 아니다. 재시도 작업은 메모리에만 있고 개수 제한이 없어 장기 장애 시 리소스가 증가할 수 있으며, WS 프로세스가 종료되면 유실된다. 늦게 처리된 해제는 당시와 다른 연결 수를 관찰할 수 있고 프로세스 비정상 종료의 stale 기록도 정리하지 않는다.

- WS 연결 등록이 오류로 끝나면 트랜잭션 커밋 응답이 유실됐을 가능성에 대비해 해당 연결 ID의 정리도 재시도한다. [gateway 테스트](apps/ws/rust-ws/tests/reference_gateway_compare.cjs)는 PostgreSQL 삽입 오류를 주입해 실패한 소켓이 거절되고 다른 정상 소켓의 기록과 온라인 상태가 유지되는지 확인했다. 삽입 오류 테스트는 롤백된 등록 사례다. DB 장애 중 유효한 연결 시도가 많으면 정리 재시도 작업이 늘어날 수 있고, 재시도는 프로세스 종료를 견디지 못한다.

- [COMMIT 응답 유실 테스트](apps/ws/rust-ws/tests/commit_ack_loss.cjs)는 격리 PostgreSQL 앞의 테스트 전용 TCP 프록시가 실제 `COMMIT` 완료 응답을 받은 후 Rust 연결에 전달하지 않고 끊는다. 등록 트랜잭션이 실제 커밋되어 연결 기록과 온라인 상태가 생긴 것을 DB에서 확인한 다음 정리 재시도로 기록이 제거되는지 검증했다. 연결 해제 커밋 응답 유실도 주입해 정리 재시도가 다시 커밋된 뒤 온라인 상태 및 변경 시각이 중복 갱신되지 않는지 확인했다. 이는 Rust의 오류 처리 검증이며 원본 Novu의 동일 장애 동작 비교는 아니다. 프록시는 테스트에만 사용하며 다른 DB 오류와 WS 프로세스 종료 중 재시도는 여전히 미검증이다.

- [0032 마이그레이션](apps/worker/rust-queue/migrations/0032_experimental_message_severity.sql)은 메시지의 severity와 필드 존재 여부를 별도로 저장한다. Inbox·구독자 알림 카운트의 severity 필터는 `none`에 원본처럼 필드가 없는 메시지를 포함하고 명시적 null은 제외하며, 단일 값·배열·빈 배열·null과 읽음·태그 조건을 지원한다. [원본 API 비교](apps/api/rust-api/tests/reference_count_severity_compare.py)는 격리 DB에 직접 삽입한 메시지로 이 조합을 비교한다. 전체 워크플로 유형과 채널의 severity 전달은 아직 완전히 구현하지 않았다. 잘못된 severity 입력은 원본 유효성 오류 대신 501을 반환하고, context·대량 데이터·성능과 모든 조합은 미검증이다. 새 열에 인덱스는 없으며 기존 카운트 쿼리의 후보 행마다 필터를 평가한다.

- 알림 목록의 단일·반복 severity 필터와 메시지 DTO의 severity 및 별도의 워크플로 severity를 지원한다. 누락과 명시적 null은 DTO에서 `none`으로 표현하되 `none` 필터는 누락만 포함한다. [원본 API 비교](apps/api/rust-api/tests/reference_count_severity_compare.py)는 직접 삽입한 메시지와 워크플로 fixture로 목록 필터·응답과 개별 읽음 응답을 비교한다. 원본 워크플로를 통해 다른 severity를 생성하거나 실제 SDK·Socket.IO로 전달한 비교는 아니며, 목록의 다른 severity 쿼리 형식, 오류·context·환경 격리 조합은 미검증이다. 필터는 기존 페이지 조회에서 후보 행마다 severity를 평가하고 새 인덱스는 추가하지 않는다.

- 실험 단일 인앱 워크플로는 워커 실행 시 워크플로 severity를 조회하고 유효한 `overrides.severity`가 있으면 이를 우선해 메시지에 기록한다. Inbox 세션은 스누즈된 알림을 제외해 severity별 100개 상한 카운트를 반환하며, WS 수신 메시지와 읽지 않은 알림 이벤트는 저장된 severity와 각 99개 상한 카운트를 반영한다. [발송 비교](apps/ws/rust-ws/tests/worker_delivery.cjs)는 실제 양쪽 API·워커에서 `medium` 기본값과 `high` override, 원본 빌드 SDK, 세션과 스누즈 후 카운트를 비교한다. 원본 메시지는 gateway에 수동 전달하므로 원본 WS worker 전체 발송 비교는 아니다. 잘못된 override 오류, 다른 워크플로 단계·채널, 전체 severity·context 조합과 부하 테스트는 남아 있다. 세션은 총계와 각 severity에 대해 최대 100개씩 조회하고, WS 카운트 재계산은 기존 총계·확인 수 조회에 더해 severity별 최대 99개를 조회한다. 관련 인덱스나 성능 검증은 아직 없다.

- [대기 작업 비교](apps/api/rust-api/tests/reference_queued_severity_compare.py)는 원본과 Rust 워커를 정지한 뒤 새 워크플로를 트리거하고 워크플로 severity를 변경한 다음 재개해, 변경된 기본값과 명시적 override의 우선순위를 비교한다. 원본 워커의 캐시가 없는 새 워크플로 사례다. Rust는 이전 형식으로 이미 대기 중인 작업에 대해 기존 severity 스냅샷을 유지하고 새 형식의 override 없는 작업에서만 실행 시 환경·식별자로 워크플로를 조회한다. 원본의 선택적 프로세스별 LRU 캐시 및 무효화, 워크플로 삭제·환경 변경, 복수 수신자 처리 도중 변경되는 경계는 아직 원본과 비교하지 않았고 Rust는 그 캐시를 구현하지 않는다. 새 작업에서 기본 severity를 사용할 때 작업당 워크플로 조회 1회가 추가된다.

- 새 형식 큐 작업은 override가 있어도 처리 시 환경과 트리거 식별자로 워크플로 존재를 확인한다. 워크플로가 없어졌으면 실패 처리하고 메시지를 생성하지 않는다. [원본 비교](apps/api/rust-api/tests/reference_queued_severity_compare.py)는 워커를 정지한 뒤 삭제하거나 같은 식별자로 재생성하고 재개한다. 원본 trigger 큐의 실패 사유를 확인하기 위해 테스트에서만 실패 작업 보존 옵션을 바꾸었다. 삭제 시 양쪽에서 메시지가 없고, 재생성 시 양쪽이 현재 식별자의 워크플로로 발송하는 것을 확인했다. 기존 형식 큐 작업은 이전 처리를 유지한다. 원본은 이후 구독자 처리 단계에서도 워크플로 ID를 다시 조회하며 선택적 캐시를 사용하지만 Rust는 단일 작업으로 처리하므로 단계 사이의 삭제·재생성과 캐시 경계는 미구현이다. 새 형식 작업은 아래와 같이 현재 단일 인앱 내용을 조회하지만 다단계 템플릿과 처리 중 수정은 아직 비교하지 않았다.

- 새 형식의 실험 인앱 작업은 워커 처리 시 현재 워크플로의 인앱 내용을 조회해 렌더링한다. [대기 작업 비교](apps/api/rust-api/tests/reference_queued_severity_compare.py)는 양쪽 워커를 정지한 상태에서 `before`를 `after`로 바꾸고 재개한 결과와, 같은 식별자로 다른 내용을 가진 워크플로를 재생성한 결과를 실제 원본과 비교했다. 이전 형식으로 이미 대기 중인 작업은 큐의 내용 스냅샷을 유지한다. 이는 단일 인앱 단계와 선택된 정적 템플릿에 대한 비교다. 원본의 캐시, 구독자별 단계 처리 중 변경, 다른 단계·변형·번역과 템플릿 관리 API는 아직 구현 또는 검증되지 않았다. severity 조회와 같은 작업당 워크플로 쿼리에서 내용을 함께 가져오므로 추가 DB 왕복은 없다.

- 새 형식 인앱 작업은 처리 시점의 워크플로 태그를 메시지에 저장하고 이전 형식 큐 작업은 기존 태그 스냅샷을 유지한다. [대기 작업 비교](apps/api/rust-api/tests/reference_queued_severity_compare.py)는 워커가 정지한 동안 태그를 변경한 후 실제 원본과 메시지 태그 및 Inbox 태그 필터 결과를 비교했다. 기본 단일 인앱 워크플로에 한정되며 원본 캐시, 태그 변경과 복수 수신자의 경합, 다른 채널·단계는 미검증이다. 기존 작업당 워크플로 조회에서 태그를 함께 가져와 추가 DB 왕복은 없다.

- [0033 마이그레이션](apps/worker/rust-queue/migrations/0033_experimental_in_app_step_active.sql)은 단일 인앱 단계의 활성 여부를 단계 존재 여부와 분리한다. 새 큐 작업은 처리 시 단계가 없거나 비활성이라면 구독자는 갱신하되 인앱 메시지와 단계 결과를 생성하지 않는다. 이전 형식 작업은 기존 동작을 유지한다. 원본은 활성 단계가 없어도 notification 레코드를 만들지만 Rust는 전체 notification 모델이 없어 이를 생성하지 않는다. [대기 작업 비교](apps/api/rust-api/tests/reference_queued_severity_compare.py)는 접수 후 워크플로 비활성·초안 전환, 단계 비활성화 및 단계 제거를 원본과 비교한다. 단계 관리 API, 다단계·채널, 처리 도중 변경과 원본 캐시 동작은 미구현 또는 미검증이다. 기존 워크플로 조회에 boolean 두 개만 추가하며 새 DB 왕복은 없다.
