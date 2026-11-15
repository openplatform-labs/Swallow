FCM 반복 선점 브라우저 종단 간 시험: 실제 Angular 화면에서 위임 관리자가 집계·목록 권한을 부여하고 이력을 확인한 뒤 대상 JWT로 건수와 작업을 조회했다. 집계 권한을 회수하면 건수 조회는 거절되고 목록은 유지되며 목록 권한 회수 후 목록도 거절되는 것을 확인했다. 격리 PostgreSQL·제한 API DB 로그인·시험 JWT를 사용하며 외부 IdP와 운영 HA는 미검증이다. `TEST_PG_BIN=/path/to/postgresql/bin scripts/test-fcm-claim-grants-browser.sh`로 재현한다.

설정의 FCM 권한 위임에서 반복 선점 `집계`와 `목록` 권한을 따로 부여·회수한다. API 0116은 기존 FCM 위임 관리자의 권한 범위를 확장하므로 기존 관리자와 `can_read`·`can_write`를 검토해야 한다. 초기 관리자와 API DB 로그인 `alert_view`는 별도 프로비저닝한다. Angular 테스트 233건과 제한 DB 로그인·임시 PostgreSQL의 API 권한 회수·재제출 시험을 수행했다. 실제 브라우저–API 위임은 위 최신 시험을 참고하며 외부 IdP·운영 HA는 미검증이다.

운영 경보의 FCM 반복 선점 조사는 최소 차이와 경과 기준을 선택해 건수와 작업 목록을 별도로 조회한다. API 0115, JWT별 `can_health`·`can_list`와 API DB 로그인의 환경 `alert_view` 권한이 각각 필요하다. 초기 위임 관리자는 DBA가 등록하며 권한 위임은 위 최신 항목을 참고한다. 목록은 UUID순 100건씩이며 건수와 페이지가 단일 스냅샷이 아니고 정상 재선점도 나타날 수 있다. Angular 테스트 233건과 빌드, 제한 로그인·임시 PostgreSQL의 API 시험을 수행했다. 실제 브라우저–API 연결은 위 최신 시험을 참고하며 외부 IdP 및 운영 HA는 미검증이다.

Resend 공유 대기 정책 변경에서 HTTP 401·403은 인증·권한 오류, 409는 충돌 응답으로 표시한다. 이전 시도의 결과는 알 수 없으므로 모두 결과 불명 상태와 요청 ID를 유지하고 확인 후 재제출하도록 한다. 500·연결 오류는 일반 결과 불명 안내를 유지한다. Angular 테스트 229건과 임시 PostgreSQL·실제 브라우저에서 권한 회수 후 401 안내를 확인했다. 외부 프록시·IdP·운영 HA는 미검증이다.

실제 브라우저 회귀 시험은 `TEST_PG_BIN=/path/to/postgresql/bin scripts/test-resend-cooldown-grants-browser.sh`로 실행한다. 테스트 JWT와 임시 PostgreSQL에서 정책 조회·변경·감사 권한을 위임하고 대상 운영자가 기본 대기를 30초에서 45초로 변경해 이력과 DB 값을 확인한다. 쓰기 권한 회수 후 변경이 반영되지 않고 읽기는 유지되며, 읽기 권한 회수 후 조회가 거절돼도 감사 조회는 유지됨을 확인했다. 거절된 변경은 결과 불명으로 유지하며 인증·권한 오류 안내를 표시한다. 외부 IdP·운영 HA는 검증하지 않는다.

설정의 Resend 429 위임 권한에서 공유 대기 정책의 조회·변경·감사 권한을 각각 관리할 수 있다. API 0114를 적용하면 기존 Resend 429 관리자의 권한 범위가 넓어지므로 사전에 해당 관리자와 `can_read`·`can_write`를 검토해야 한다. 워커 정책에 대한 API DB 로그인 권한은 별도로 부여하며 초기 위임 관리자는 DBA가 등록한다. 제한 DB 로그인·임시 PostgreSQL에서 실제 정책 접근과 개별 권한 회수를 확인했고 Angular 테스트를 통과했다. 실제 브라우저·제한 API DB 로그인·임시 PostgreSQL에서 세 권한 위임 후 대상 운영자의 정책 변경과 감사 조회를 확인했다. 외부 IdP와 운영 HA는 미검증이다.

설정 화면에서 `Resend 공유 대기 정책`의 기본 대기 초를 조회·변경하고 요청 결과와 이력을 확인할 수 있다. 별도의 JWT 읽기·쓰기·감사 권한과 API DB 로그인의 워커 정책 읽기·쓰기 권한이 필요하다. 변경을 준비한 후 별도 확인으로 제출하며 응답 유실 시 요청을 보관해 원래 인증으로 재제출하거나 동일 운영자 권한으로 조회할 수 있다. fallback 변경은 이미 기록된 대기 종료 시각을 줄이지 않는다. 사용자 권한의 초기 프로비저닝과 워커 활성화는 별도다. Angular 단위 테스트와 제한 DB 로그인·임시 PostgreSQL의 API 테스트를 수행했다. 실제 브라우저와 임시 PostgreSQL의 정책 변경·이력 조회도 확인했다. 외부 IdP와 운영 HA는 미검증이다.

설정의 운영자 권한 관리에서 `Resend 공유 대기 조회`를 선택해 별도 위임 관리자가 대상 운영자의 JWT 조회 권한을 확인·변경할 수 있다. 기존 Resend 429 권한 관리·요청 ID·감사 흐름을 사용한다. 이 기능은 API 0112를 적용하면 기존 Resend 429 관리자의 권한 범위를 넓히므로 배포 전 관리자를 검토해야 한다. DB 로그인 `alert_view` 권한은 별도로 부여해야 한다. Angular 단위 시험과 제한 계정·임시 PostgreSQL의 API 시험을 수행했으며 격리 로컬 PostgreSQL·제한 API DB 로그인·실제 브라우저에서 권한 부여, 감사 조회, 대상의 상태 조회와 회수 후 거절을 확인했다. 첫 두 시도는 모호한 메뉴 선택자와 대문자로 표시된 버튼 이름을 수정한 뒤 통과했다. `TEST_PG_BIN=/path/to/postgresql/bin scripts/test-resend-cooldown-grants-browser.sh`로 재현하며 외부 IdP·운영 HA는 미검증이다.

운영 경보 화면의 Resend 공유 대기 조회는 별도의 운영자 JWT 권한과 API DB 로그인의 `alert_view`를 필요로 한다. 선택한 환경의 활성 상태, 종료 시각, 기본 대기, 지연 및 기한 초과 건수를 표시하며 기존 자동 재시도 집계와 별개다. 입력이나 인증 변경 시 결과를 지우고 지연된 응답을 무시한다. 상세 권한·마이그레이션은 Rust API README를 참고한다. Angular 테스트로 정상 응답, 권한 거절과 잘못되거나 지연된 응답을 확인했으며 실제 Rust API와 연결한 브라우저 시험은 위 항목을 참고하며, 외부 IdP 및 운영 HA는 미검증이다.

개요의 `운영자 서명 키 갱신`에서 선택한 API 주소의 `/health/operator-keys`를 수동 조회한다. 응답의 HTTP 상태와 본문이 각각 200/healthy, 503/unhealthy와 일치할 때만 정상 또는 장애로 표시하며, 비활성 경로·네트워크·CORS·잘못된 응답은 확인 불가로 표시한다. 서버의 `SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true`, `SWALLOW_API_OPERATOR_KEY_HEALTH_ENABLED=true`와 JWT 키 파일 갱신 설정 및 필요 시 브라우저 CORS 설정이 필요하다. 응답은 로드밸런서 뒤의 한 인스턴스에 대한 순간 상태이며, 자동 새로고침·경보나 전체 인스턴스 상태를 제공하지 않는다. API 주소 변경 시 기존 표시를 지우고 늦은 응답을 무시한다. Angular 단위 테스트와 실제 브라우저·로컬 모의 HTTP API에서 정상과 장애 표시를 확인했다. 실제 Rust API·임시 로컬 PostgreSQL·브라우저에서 키 파일을 잘못된 내용으로 교체한 뒤 정상·장애·복구 표시도 확인했다. `TEST_PG_BIN=/path/to/postgresql/bin scripts/test-operator-key-health-browser.sh`로 재현하며 ego-browser, Rust와 Node가 필요하다. 이 시험은 로컬 전용 DB와 테스트 공개키를 사용하고 API 상태 경로만 검증하며 운영자 로그인, 실제 로드밸런서 및 다중 인스턴스는 검증하지 않는다.

## Slack 429 지연 재시도 화면

The FCM results panel displays the shared cooldown's active state, end time, fallback and deferred/overdue job counts using the independent operator JWT and DB `alert_view` grants. Apply API `0108` after worker `0188`–`0190`. With worker migration 0193 and API migration 0110, the panel displays the status that extended the active deadline and its recorded time; missing or legacy evidence is shown as unknown. It does not prove that every worker uses the policy or that devices received pushes. A local browser with restricted API and disposable PostgreSQL verified display and clearing after permission revocation; real IdP and production HA remain unverified.



The existing permission settings screen supports delegated FCM partial-failure `health`, `recent-list`, `unreviewed`, `review`, and `cooldown-health` JWT grants, as well as request lookup and independently authorized grant audit. It uses the existing prepare/confirm and same-request recovery flow. API migrations `0107` and `0109` and separate FCM grant administrator/audit-viewer provisioning are required; migration `0109` expands the authority of existing FCM grant administrators, so review their assignments before applying it; granting a JWT permission does not provision the API DB login's `alert_view` or `queue_ack` right. The target identity must already exist and self-changes are denied. A local browser with a restricted API and disposable PostgreSQL exercised grant, audit and revoke; Angular tests cover all five permission types and endpoint routing. Real IdP, production HA and direct DBA operations remain outside that validation.


The FCM results panel supports durable unreviewed-job listing, review preparation with a reason, explicit confirmation, request lookup and same-request replay after an uncertain response. Apply worker migrations through `0187` and API migration `0106`; provision separate unreviewed-list and review JWT grants, plus the API DB login's `alert_view` and `queue_ack` permissions and the documented function EXECUTE grants. A request ID should be recorded before sending: the UI keeps it only in memory and loses it when leaving the page. A missing lookup record does not prove the request failed; same-request replay requires the original API origin, environment and token. Acknowledgement does not resend or establish delivery. The recent-window panel remains independent and does not exclude reviewed jobs. See the API and worker READMEs for replay, revision, retention and HA limitations. Angular tests cover identity changes, malformed responses and lost requests; a local browser with a restricted API and disposable PostgreSQL exercised preparation, confirmation, persisted audit, lookup and refreshed list. Real IdP and production HA remain unverified.


The FCM partial-health panel can list matching completed jobs and select one for the existing FCM result lookup. List access needs the partial-health and separate partial-jobs JWT grants plus `alert_view`; details use the existing FCM result grant. Lists are UUID-ordered, page size 20, and change between requests; aging out is not acknowledgement. A local browser against the restricted DB/API fixture checked selection, details, and clearing after list-grant revocation.


The FCM results panel can query recent partial failure health with an independently granted operator JWT and an environment `alert_view` DB permission. The window is 1–604800 seconds; the count excludes aged-out unreviewed work and is not an acknowledgement. Apply worker migration `0185` followed by API migration `0104`. A local browser test against a disposable PostgreSQL/API fixture checked the count and result clearing after JWT permission revocation; real identity providers and production deployment remain unverified.


The FCM operator result view shows 429 target count, count with recorded numeric advice, and the latest recorded not-before timestamp when API migration `0103` is present. A partial count does not supply guidance for every target. This view does not retry or throttle sends. Older API responses remain readable.


운영 경보에서 Slack 작업 결과를 조회한 후 실패한 429 시도에 유효한 대기 안내가 있으면 `권장 시각 이후 재시도`를 선택할 수 있다. 즉시 재시도와 접수 판정도 별도로 선택한다. 요청 ID·작업·시도·사유를 확인한 뒤 제출하고, 결과 불명 시 같은 토큰·환경·API 주소에서 동일 요청을 재제출하거나 같은 운영자와 환경에서 요청 ID를 조회한다. 기록이 없더라도 요청이 진행 중일 수 있다. 요청 정보는 메모리에만 보관하므로 ID를 따로 기록한다. 인증 변경 시 과거 요청을 자동으로 확정하거나 다른 인증으로 재제출하지 않으며 조회 결과는 운영자가 대조해야 한다.

워커 0170–0171과 API 0096–0097, 기존 Slack 결과 및 `retry` JWT 권한, API DB `slack_retry` 및 해당 함수 EXECUTE가 필요하다. 즉시·지연은 동일 `retry` 권한을 사용한다. 권장 시각과 시도 ID는 조회 시점의 정보이고 Slack 수신이나 중복 방지를 증명하지 않는다. 선택형 환경 공유 대기는 별도 워커 설정으로 제공하며, 정확한 작업공간·채널 단위 요청률이나 참여하지 않은 발송자는 제어하지 않는다. [API README](../../api/rust-api/README.md)와 [워커 README](../../worker/rust-queue/README.md)를 참고한다.

Angular 테스트 193개와 빌드가 통과했다. 실제 로컬 브라우저·임시 PostgreSQL 17·제한 API DB 로그인·시험 JWT에서 합성 429 작업의 준비 전 상태, 예약·감사 및 요청 조회를 확인했다. 응답 유실과 인증 변경은 단위 테스트이고 실제 Slack·외부 IdP·운영 HA는 미검증이다. 개발 서버 동시 실행 중 테스트 임시 파일 오류가 있었으며 서버 종료 후 재실행은 통과했다. 기존 CSS 예산 경고가 있다. 재현은 `SWALLOW_UI_SLACK_429_FIXTURE=true bash scripts/preview-channel-results-local.sh`와 별도 UI 서버를 사용한다.

## Slack 429 재시도 대기 안내

운영 경보의 Slack 작업 상세에서 HTTP 429와 지원되는 대기 초·DB 기준 권장 시각을 보여준다. 워커 0170, API 0096과 기존 Slack 결과 조회 권한이 필요하며, 이전 API는 업데이트 필요로 표시한다. 이 값은 자동 예약하지 않고 기존 화면의 수동 재시도에도 대기 시각을 강제하지 않는다. 선택형 지연 예약은 위 최신 화면, CLI와 JWT API에서 사용할 수 있다. [워커 README](../../worker/rust-queue/README.md)의 제한을 확인한다.

Angular 테스트 190개와 빌드, 제한 API DB 로그인·시험 JWT·임시 PostgreSQL 및 실제 로컬 브라우저에서 합성 429 표시를 확인했다. 실제 Slack·원본 서버의 해당 오류·운영 HA는 미검증이며 CSS 예산 경고가 있다. 재현은 `SWALLOW_UI_SLACK_429_FIXTURE=true bash scripts/preview-channel-results-local.sh`와 별도 UI 서버를 사용한다.

## Resend 429 재시도 판정 이력

운영 경보의 `429 재시도 판정 이력`에서 JWT API를 통해 기록된 즉시·지연 재시도 판정의 요청·작업·시도 ID, 운영자, DB 로그인, 예약 시각과 사유를 조회한다. 워커 0167 및 API 0093–0094, 환경별 `novu_rust_operator_jwt_resend_429_audit_grants`와 API DB 로그인에 `novu_rust_operator_jwt_resend_429_audit_read(text,text,text,text,bigint,uuid)` 함수 EXECUTE가 필요하다. 이 권한은 설정 화면에서 위임할 수 있다. 권한 위임의 변경 이력과 별도이며 CLI·DB 직접 변경 기록은 포함하지 않는다.

각 페이지는 최대 50건을 UUID순으로 조회한다. UUID는 시간순이 아니므로 조회 후 추가된 기록이 이미 지나간 위치에 들어갈 수 있고 페이지는 단일 스냅샷이 아니다. 민감할 수 있는 운영자와 사유가 보이므로 접근과 보존을 관리한다. 인증 변경·만료 또는 조회 실패 시 이전 화면 결과를 지운다.

Angular 테스트 189개와 빌드가 통과했다. 실제 로컬 브라우저·시험 JWT·제한 API DB 로그인·임시 PostgreSQL 17에서 합성 기록 51건의 두 페이지와 감사 권한 회수 후 결과 제거를 확인했다. 실제 IdP·대규모 감사 기록·운영 HA는 미검증이며 CSS 예산 경고가 있다. 재현은 `SWALLOW_UI_RESEND_429_AUDIT_FIXTURE=true bash scripts/preview-channel-results-local.sh`와 별도 UI 서버를 사용한다.

## Resend 429 권한 위임 화면

설정의 위임 권한 목록에서 `Resend 429 즉시 재시도`, `지연 재시도`, `판정 감사 조회`를 각각 선택해 다른 운영자의 현재 상태를 조회하고 부여·회수한다. 변경 요청 ID·대상·이전 및 변경 상태·사유를 확인 후 제출하며 동일 요청 재제출, 요청 조회 및 별도 권한의 위임 이력 조회를 지원한다. 요청과 조회 결과는 화면 메모리에만 있으므로 요청 ID를 따로 보관한다. 기록이 없어도 요청이 진행 중일 수 있다.

API 0095, 환경별 Resend 429 위임 관리자와 API DB 함수 EXECUTE 권한이 필요하며 위임 이력은 별도 감사 권한이 필요하다. 관리자 부트스트랩, DB `retry` 권한 및 결과 조회 권한은 [API README](../../api/rust-api/README.md)를 참고한다. 이 기능은 JWT 권한을 바꾸며 DB 권한이나 함수 권한을 자동으로 부여하지 않는다. 위임 이력은 UUID순으로 시간순이 아니며 페이지마다 다른 시점이다.

Angular 테스트 187개와 빌드가 통과했다. 실제 로컬 브라우저·임시 PostgreSQL·제한 API DB 로그인·시험 JWT에서 지연 재시도 권한의 준비 전후 DB 상태, 부여·회수와 위임 이력을 확인했다. 세 권한의 경로와 잘못된 감사 입력은 단위 테스트이며 실제 IdP·네트워크 장애·운영 HA는 미검증이다. 재현은 `SWALLOW_UI_RESEND_429_GRANT_FIXTURE=true bash scripts/preview-channel-results-local.sh`와 별도 UI 서버를 사용한다. 기존 CSS 예산 경고가 있다.

## Resend 429 수동 재시도

운영 경보에서 JWT와 환경을 선택하고 Resend 결과 조회로 실패한 429 작업을 연다. 현재 시도 ID와 제공자의 권장 시각을 확인한 뒤 `즉시` 또는 `권장 시각 이후`를 선택하고 사유를 입력해 판정을 준비한다. 요청 ID·작업·시도·방식·사유를 기록하고 별도 확인 후 제출한다. 권장 시각이 없으면 지연 방식은 사용할 수 없다. 워커 0167과 API 0093–0094, 결과 조회 권한 및 각 모드별 JWT 권한과 API DB `retry`·함수 EXECUTE가 필요하다. JWT 권한 위임과 별도 감사 조회는 [API README](../../api/rust-api/README.md)를 참고한다.

응답이 불명이면 동일 토큰·환경·API 주소에서 같은 요청을 재제출하거나 같은 운영자와 환경에서 요청 ID로 조회한다. 기록이 없더라도 요청이 진행 중일 수 있다. JWT가 바뀌면 이전 토큰으로만 재제출할 수 있으며 새 토큰으로 조회한 내용은 운영자가 대조한다. 화면의 요청 정보는 메모리에만 있으므로 새로고침 전에 ID를 따로 저장한다. 늦은 콜백과 기존 CLI·구버전 워커의 우회, 공급자의 미확인 접수에 따른 중복 위험이 있다. 실제 전달을 보장하지 않는다.

Angular 테스트 186개와 빌드가 통과했다. 실제 로컬 브라우저·시험 JWT·제한 API DB 로그인·임시 PostgreSQL 17에서 합성 429 시도 조회, 지연 요청 확인과 예약·감사 한 건을 확인했다. 응답 유실·인증 변경은 단위 테스트이며 실제 Resend 429, 외부 IdP, 실제 네트워크 단절과 운영 HA는 미검증이다. 기존 CSS 예산 경고가 있다.

Resend 자동 재시도 상태 화면에서 오래된 429·횟수 소진·보호 설정 연기·예약 기한 초과의 작업 목록을 선택하고 기존 발송 상세를 열 수 있다. 워커 0166과 API 0092, 집계 권한과 별도의 JWT 목록 권한, API DB `alert_view` 및 함수 EXECUTE가 필요하다. 발송 상세는 별도의 Resend 결과 권한이 필요하다. 목록은 payload·수신자를 표시하지 않지만 UUID와 시각은 민감할 수 있다. 페이지와 집계는 서로 다른 시점이며 UUID 순서는 시간순이 아니다. 조회 권한이 회수되면 다음 조회에서 기존 목록을 지운다.

Angular 테스트 184개와 빌드가 통과했다. 실제 브라우저·제한 API DB 계정·테스트 JWT·임시 PostgreSQL에서 합성 보류 작업 목록·상세 연결과 목록 권한 회수 후 결과 제거를 확인했다. API 시험은 네 필터, 별도 권한 및 21건의 두 페이지를 검증했다. 실제 IdP·외부 발송·운영 HA는 미검증이며 기존 CSS 예산 경고가 있다. 재현은 `SWALLOW_UI_RESEND_AUTO_HEALTH_FIXTURE=true bash scripts/preview-channel-results-local.sh`와 별도 UI 서버를 사용한다.

운영 경보 화면에서 운영자 JWT를 선택하면 Resend 자동 재시도 상태를 조회할 수 있다. 기준 시간은 이 조회에만 적용되며 실제 경보의 설정은 바꾸지 않는다. 오래된 429, 그중 횟수 소진, 보호 설정으로 연기된 작업과 예약 기한이 지난 자동 재시도 작업을 보여주며 건수는 겹칠 수 있다. 워커 0165와 API 0091, 별도의 환경별 JWT 모니터링 권한·API DB `alert_view` 및 함수 실행 권한이 필요하다. 발송 결과·정책 권한만으로는 사용할 수 없다. 조회 실패나 인증 변경 시 이전 수치를 지우며, JavaScript의 안전한 정수 범위를 벗어난 응답은 거절한다.

Angular 테스트 182개와 빌드가 통과했다. 실제 브라우저·제한 API DB 계정·테스트 JWT·임시 PostgreSQL에서 합성 보류 건수 표시와 JWT 권한 회수 후 거절·기존 결과 제거를 확인했다. 실제 IdP·외부 발송·운영 HA는 미검증이며 기존 CSS 예산 경고가 있다. 재현은 `SWALLOW_UI_RESEND_AUTO_HEALTH_FIXTURE=true bash scripts/preview-channel-results-local.sh`와 별도 UI 서버를 사용한다. 집계 의미와 한계는 [워커 README](../../worker/rust-queue/README.md)를 참고한다.

설정 화면의 Resend 자동 재시도 정책에서 JWT로 현재 상태를 조회하고 활성화·최대 횟수·대상 시작 시각을 선택할 수 있다. API 0090, 워커 0163–0164, API DB 로그인과 JWT 각각의 정책 권한 및 함수 실행 권한이 필요하다. 기존 운영자·채널 권한이 자동으로 정책 권한을 부여하지 않는다. 정책을 활성화해도 워커 설정과 권한을 별도로 갖추어야 하며 비활성화는 이미 예약된 작업을 취소하지 않는다. `preserve`는 과거 실패를 포함할 수 있고 자동 재발송은 중복 위험이 있다. [API README](../../api/rust-api/README.md)와 [워커 README](../../worker/rust-queue/README.md)의 배포 조건을 먼저 확인한다.

변경은 조회한 revision으로 요청 ID를 준비하고 사용자가 ID·내용을 기록한 뒤 별도 확인해 제출한다. 응답 유실은 불명으로 표시하며 동일 토큰·환경·API 주소에서 정확히 같은 요청만 재제출한다. 쓰기 권한의 요청 조회에서 내용을 확인할 수 있으나 기록이 없어도 요청이 진행 중일 수 있고 결과는 현재 정책이 아닐 수 있다. 감사에는 별도 권한이 필요하고 JWT 경로만 표시한다. 요청 정보는 메모리에만 있어 새로고침·화면 이탈로 사라지며 불명 상태를 지우려면 확인이 필요하다. JWT 갱신 등 인증이 바뀌면 원래 토큰이 없는 재제출은 불가하고 새 인증의 조회 결과를 운영자가 대조해야 한다.

Angular 테스트 179개와 빌드가 통과했다. 실제 브라우저·제한 API DB 로그인·테스트 JWT·임시 PostgreSQL에서 제출 전 DB 미변경, 정책 생성·요청 조회·감사 및 조회 권한 회수 후 거절을 확인했다. 응답 유실·인증 변경·감사 페이지는 단위 테스트이며 실제 IdP·Resend 발송·운영 HA는 미검증이다. 재현은 `SWALLOW_UI_RESEND_AUTO_POLICY_FIXTURE=true bash scripts/preview-channel-results-local.sh`와 별도 UI 서버가 필요하다. 기존 CSS 예산 경고가 남아 있다.

Resend 429 상세 화면에 공급자의 지원되는 `Retry-After` 초 값과 DB 기준 권장 시각을 표시한다. 워커 0159, API 0089, 최신 워커 및 해당 DB 권한이 필요하다. 안내 자체는 재시도를 실행하거나 대기를 강제하지 않으며, 별도 선택형 워커 정책은 [워커 README](../../worker/rust-queue/README.md)를 참고한다. 누락·지원하지 않는 헤더와 구버전 API를 구분한다. 실제 브라우저·제한 JWT API·임시 PostgreSQL의 합성 429 결과와 Angular 단위 테스트로 확인했다. 실제 Resend 429는 미검증이다.

워크플로·구독자·채널 조회·Slack 판정 권한 위임은 먼저 변경 요청을 준비해 요청 ID와 대상·사유·이전 및 변경 상태를 보여준다. 이 단계에서는 서버에 변경을 전송하지 않는다. 운영자가 ID와 내용을 보관하고 별도 확인 후 제출하며, 제출 전 인증·환경·API 주소가 준비 시점과 일치해야 한다. 준비된 정보는 메모리에만 있어 화면 이탈·새로고침 후 사라진다. 전송 이후 응답이 유실되면 결과 불명으로 표시하고 같은 토큰과 내용으로 재제출하거나 요청 ID로 조회한다. 이 절차는 위임 권한 화면에 적용되며 다른 관리 화면에 일괄 적용된 것은 아니다.

Angular 테스트 172개와 빌드가 통과했다. 실제 브라우저·제한 API 계정·테스트 JWT·임시 로컬 PostgreSQL에서 Slack 판정 권한 요청을 준비했을 때 DB에 권한이 없고 별도 제출 후 생성되는 것을 확인했다. 인증 변경 차단과 중복 제출 방지는 단위 테스트로 확인했다. 실제 IdP·운영 HA는 미검증이며 CSS 크기 예산 경고가 있다.

설정 화면에서 Slack 발송 `retry`·`accepted` 판정 권한을 선택해 조회·변경하고 요청 및 감사 이력을 확인할 수 있다. API 0088과 별도의 Slack 판정 권한 관리 조회·쓰기 권한 및 감사 조회 권한, API DB 함수 실행 권한이 필요하며 기존 채널 관리자 권한만으로는 사용할 수 없다. 초기 관리 권한과 API DB 권한은 관리자가 별도로 설정한다. 권한은 대상의 현재 상태를 조회한 후 사유와 확인을 입력해 조건부 변경하며 자신의 권한은 변경할 수 없다. 판정 실행에는 이 위임과 별도로 API DB의 판정 함수 실행 권한도 필요하다.

요청 ID는 따로 보관한다. 응답 유실 시 같은 인증과 동일한 내용의 요청만 재제출할 수 있으며 조회에서 기록이 없다는 사실만으로 진행 중 요청의 실패를 확정할 수 없다. 감사 이력은 UUID 순이고 페이지 전체가 단일 스냅샷이 아니다. 재현 fixture는 `SWALLOW_UI_SLACK_GRANT_FIXTURE=true bash scripts/preview-channel-results-local.sh`로 실행하며 대상은 `target-fixture`다. Angular 테스트 171개와 빌드, 실제 브라우저·제한 API DB 계정·테스트 JWT·임시 PostgreSQL에서 권한 부여와 감사 이력 표시를 확인했다. 실제 Slack 발송, 외부 IdP 및 운영 HA는 검증하지 않았다. 세부 권한과 제한은 [API README](../../api/rust-api/README.md)를 참고한다.

Slack 작업 상세에서 현재 실패 시도를 조회한 뒤 `retry` 또는 `accepted` 판정과 사유를 선택하고, 생성된 요청 ID·작업·시도를 확인한 후 명시적으로 제출할 수 있다. 워커 0158·API 0087과 별도의 운영자 판정 권한 및 API DB 권한이 필요하다. `accepted`는 결과 불명 시도에만 허용되며 공급자 접수에 대한 운영자 판단이고 수신 증거가 아니다. `retry`는 대기열에 다시 넣으며 중복 발송될 수 있다. 화면이 공급자의 상태를 확인하지 않으므로 운영자가 독립적으로 조사해야 한다.

제출 중 화면 이동과 인증 입력 변경을 막는다. 정상 응답은 기록됨으로 표시하고 현재 작업 조회를 지운다. 서버 오류·권한 거절·시간 초과·응답 형식 오류는 모두 보수적으로 결과 불명으로 표시하고 자동 재시도하지 않는다. 요청 ID를 제출 전에 별도로 기록한다. 화면 이동·새로고침·브라우저 종료 후에는 메모리의 요청 정보가 사라질 수 있다. 결과 조회에서 `found:false`는 진행 중인 요청의 실패를 증명하지 않는다. 같은 토큰으로 조회한 동일 요청만 화면이 자동으로 기록됨으로 바꾸며, 인증이 바뀐 후에는 현재 인증으로 조회된 작업·시도·판정·사유를 보여주고 운영자가 대조해야 한다. 불명 상태를 지우고 새 판정을 준비하려면 별도 조사 확인이 필요하다. 서버 감사와 현재 상태를 확인하기 전 새 요청을 보내면 중복 위험이 있다. 브라우저 종료나 요청 취소가 서버 작업을 취소한다는 보장은 없다.

Angular 테스트 169개와 빌드가 통과했다. 실제 브라우저·제한 API 로그인·테스트 JWT·임시 로컬 PostgreSQL에서 합성 Slack 시도의 재시도 판정, 요청 조회, 감사와 권한 회수 후 거절을 확인했다. 워커를 실행하지 않아 실제 Slack 전송은 하지 않았다. 재현에는 로컬 PostgreSQL 17과 UI 서버가 필요하며 `SWALLOW_UI_SLACK_DECISION_FIXTURE=true bash scripts/preview-channel-results-local.sh`를 사용한다. 결과 유실·인증 변경은 단위 테스트로만 확인했고 실제 외부 IdP, 운영 HA 및 전체 Novu 호환은 미검증이다. 권한 설정과 API 계약은 [API README](../../api/rust-api/README.md)를 참고한다.

Slack 결과 상세는 현재 시도의 로컬 UUID를 표시한다. 워커 0158과 API 0086이 필요하며 이전 API는 업데이트 필요로 표시한다. 표시된 ID는 별도 권한을 가진 운영자가 `slack-resolve-v2` CLI에 사용할 수 있으나 Slack 접수 증거가 아니다. 화면 판정은 위 최신 항목을 참고하며 다른 작업자가 재시도했을 수 있으므로 판정 전에 다시 조회해야 한다. Angular 테스트 165개와 빌드, 실제 브라우저·제한 API DB 계정·테스트 JWT·임시 로컬 PostgreSQL에서 ID 표시를 확인했다. 실제 Slack과 운영 HA는 미검증이다. 재현 fixture의 Slack 작업 `11111111-1111-4111-8111-000000000001`과 시도 `22222222-2222-4222-8222-222222222222`를 사용할 수 있다. [API README](../../api/rust-api/README.md)와 [워커 README](../../worker/rust-queue/README.md)를 참고한다.

Twilio·FCM·Slack·SendGrid 결과 화면은 API 응답에서 검증된 표시 필드만 결과 상태에 저장한다. 예상하지 않은 필드는 목록과 상세 상태에서 버린다. 이 처리는 서버가 민감정보를 보내는 것을 막지 않으며 응답과 브라우저·네트워크 접근을 보호할 책임은 별도다. 네 목록은 20건 미만인데 다음 페이지가 있거나 다음 커서가 마지막 UUID와 다르거나 UUID가 중복·역순·이전 커서 이하인 응답을 오류로 처리한다. 각 페이지는 독립 조회이므로 정상 순서여도 동시 변경에 따른 누락은 감지하지 못한다.

Angular 테스트 164개와 빌드가 통과했다. 실제 브라우저와 제한된 Rust API DB 로그인·임시 로컬 PostgreSQL에서 네 채널의 첫 페이지 20건과 두 번째 페이지 1건을 조회했다. 테스트는 생성된 작업을 사용하며 실제 공급자 발송, 대규모 부하와 운영 HA는 미검증이다. 재현은 `npm run build` 후 `python3 -m http.server 4249 --bind 127.0.0.1 --directory dist/swallow/browser`와 별도 터미널의 `bash scripts/preview-channel-results-local.sh`를 실행하고 출력된 API 주소, `ui-fixture-env`와 테스트 JWT를 사용한다. 로컬 PostgreSQL 17이 필요하며 `SWALLOW_TEST_POSTGRES_BIN`으로 실행 파일 경로를 지정할 수 있다. 스크립트 종료 시 임시 DB와 키를 정리한다.

Resend 작업 목록은 전체, 결과 불명, 거절, HTTP 429 거절 중 선택해 조회하며 목록에 시도 상태와 HTTP 상태도 표시한다. 필터는 현재 시도만 대상으로 하고 변경 시 이전 결과와 페이지를 지운다. API 0085, 기존 목록·단건 권한과 새 DB 함수 실행 권한이 필요하다. UUID순 페이지는 단일 스냅샷이 아니므로 변경 중인 작업은 놓칠 수 있다. Angular 테스트 160개와 빌드가 통과했고 실제 브라우저·Rust API·임시 PostgreSQL에서 429 결과와 필터 전환 후 이전 목록 제거를 확인했다. 실제 공급자, 운영 부하와 HA는 미검증이다. [API README](../../api/rust-api/README.md)를 참고한다.

재현하려면 로컬 PostgreSQL 17 실행 파일을 설치한 뒤 `npm run build`와 `python3 -m http.server 4249 --bind 127.0.0.1 --directory dist/swallow/browser`를 실행하고, 별도 터미널에서 `bash scripts/preview-resend-status-local.sh`를 실행한다. 기본 경로는 `/opt/homebrew/opt/postgresql@17/bin`이며 `SWALLOW_TEST_POSTGRES_BIN`으로 변경할 수 있다. 출력된 API 주소, 환경 `ui-fixture-env`, 임시 디렉터리의 테스트 JWT와 작업 UUID `1889f88e-c0a3-4ab5-b861-2fdc1f26f254`로 운영 경보의 Resend 결과를 조회한다. 이 스크립트는 임시 DB를 생성하고 종료 시 정리하며 실제 이메일을 발송하지 않는다. 테스트 키·데이터만 사용하고 API 서버 DB 로그인에는 필요한 함수 실행 권한만 부여한다. PostgreSQL 관리자와 로컬 파일 접근은 테스트 환경에 한정한다.

Resend 발송 결과 상세에 현재 시도의 공급자 HTTP 상태를 표시한다. API 0084가 필요하며 구버전 API에서는 업데이트가 필요하다고 표시한다. `rejected`와 429를 함께 확인하면 운영자에게 별도 권한의 수동 재시도 절차를 안내한다. 화면에서 직접 재시도하지 않는다. 상태는 조회 시점의 현재 시도 값으로 전달 증거나 이전 시도 전체 이력이 아니다. 재시도 시 이전 값은 감사에서 확인해야 한다. Angular 단위 테스트와 빌드가 통과했고, 실제 브라우저·제한 API DB 계정·테스트 JWT·임시 로컬 PostgreSQL에서 429 표시 및 결과 조회 권한 회수 후 기존 결과 제거를 확인했다. 실제 Resend 429는 미검증이다. [API README](../../api/rust-api/README.md), [워커 README](../../worker/rust-queue/README.md)를 참고한다.

운영자 JWT API에 선택형 IdP 토큰 상태 확인이 추가됐다. API 인스턴스에서 설정하면 공급자 회수 상태를 각 요청에서 확인하며, 미설정 시 기존 오프라인 검증을 사용한다. 시험 범위와 경합·장애 제약은 [Rust API README](../../api/rust-api/README.md)를 참고한다. 아래 과거 시험 결과는 당시 오프라인 설정에 대한 기록이다.

# Swallow 관리 콘솔

‘발송 채널’에서 환경 API 키와 별도 읽기·쓰기 권한으로 FCM 통합 메타데이터를 조회·등록하고 활성 상태를 변경할 수 있다. API 마이그레이션 0018·0019·0073, 워커 0046·0047·0051과 최신 API 바이너리 및 API DB 권한이 필요하다. 목록에서 통합을 선택해야 상태를 변경할 수 있으며 조회한 내부 ID, 활성 상태와 변경 버전을 함께 보내 현재 값이 다르면 거절한다. 중간에 상태를 바꿨다가 복구한 경우에도 버전이 달라 거절된다. 조건 없이 수정하거나 버전을 제공하지 않는 구형 클라이언트는 기존 방식대로 작동한다. 자격증명은 여기서 설정하지 않으며 워커 자격증명 맵과 공급자 측 설정을 별도로 준비해야 한다. 활성화는 실제 발송이나 수신을 증명하지 않으며 비활성화는 기존 대기·진행 중 발송을 취소하지 않는다. 이 화면은 운영자 JWT가 아닌 환경 API 키를 사용하므로 키의 권한을 제한하고 보호한다.

변경 요청에는 확인이 필요하고 진행 중 화면 이동을 막는다. 타임아웃·서버 오류·응답 해석 오류는 결과 불명이며 자동 재시도하지 않는다. 다시 조치하기 전에 목록과 관리자의 서버 감사 기록을 확인해야 한다. 목록 조회로 다시 시도 제한이 해제되어도 요청이 아직 처리 중일 수 있다. 인증 값과 목록은 화면을 떠나거나 인증을 지우면 메모리에서 제거한다. 목록 페이지는 단일 스냅샷이 아니며 직접 DB 변경은 감사되지 않는다. 이 API와 UI는 Novu 원본 통합 관리 계약과 동일하지 않다.

Angular 테스트 114개와 빌드가 통과했다. 격리된 Docker PostgreSQL, 실제 Rust API 및 브라우저에서 등록·목록·권한 회수, 다른 관리자의 변경 뒤 충돌 거절, 재조회 후 변경과 감사 기록을 확인했다. 별도 DB 통합 테스트는 행 잠금을 기다린 뒤 변경된 상태에 대한 조건부 요청을 거절함을 확인한다. 이 fixture의 API DB 계정은 PostgreSQL 관리자이므로 최소 권한 배포를 검증하지 않았다. 실제 FCM 발송, 외부 인증 공급자, 운영 HA 및 원본 UI 비교는 미검증이다.

Angular 22와 Clarity 18 기반의 초기 관리 화면이다. 현재 개요·메뉴, Rust API 준비 상태 확인과 환경별 구독자 목록 조회·이름 검색·페이지 이동과 별도 권한의 운영 경보 대상 상태 조회·판정과 운영자 권한·감사 조회가 동작한다. 구독자 조회는 조회 전용 인증, 운영자 JWT 또는 개발 검증용 API 키를 선택하고, 경보는 콘솔 인증 또는 운영자 JWT를 선택한다. 전체 워크플로·채널 관리 및 발송 이력은 연결되지 않았다. 환경 API 키 또는 운영자 JWT로 제한적인 구독자 생성·프로필 수정·조건부 삭제를 할 수 있다. 제한적인 워크플로 단계·공급자 관리와 선택적인 OIDC 운영자 로그인이 제공된다. 운영 제품이 아니다.

Node.js 22.22.3 이상 22.x 또는 Angular 22가 지원하는 Node 버전과 npm이 필요하다. 이 디렉터리에서 `npm ci`, `npm start`를 실행하면 기본 `http://localhost:4200`에서 개발 서버가 시작된다. `npm run build`는 프로덕션 번들을 만들고 `npm test -- --watch=false`는 단위 테스트를 실행한다. `package-lock.json`은 이 독립 앱의 의존성을 고정한다. Clarity 18.4는 Angular 22를 지원하며, Clarity가 사용하는 `@angular/animations`는 Angular에서 deprecated된 의존성이므로 향후 교체를 확인해야 한다.

API 준비 상태 확인에는 Rust API의 `SWALLOW_API_HEALTH_ENDPOINTS_ENABLED=true`와 화면의 정확한 origin을 `SWALLOW_CORS_ALLOWED_ORIGINS`에 지정해야 한다. 예를 들어 `http://127.0.0.1:4200`에서 화면을 열 때 같은 값을 허용한다. `localhost`와 `127.0.0.1`은 다른 origin이다. 준비 상태는 DB의 순간적인 연결 확인이며 인증, 스키마, 채널 발송이나 전체 서비스의 정상 상태를 보증하지 않는다. 서버 health API는 인증되지 않으므로 외부 네트워크 접근을 제한해야 한다. 이 화면은 API 주소를 저장하지 않으며 원격 API에는 HTTPS만 허용한다.

‘발송 채널’에서는 환경 API 키로 SendGrid 웹훅 공개키의 목록·등록·활성화·비활성화·삭제를 수행한다. API 마이그레이션 0064~0066, 해당 API 키의 별도 `read`·`write` 권한과 API DB 함수 실행 권한을 먼저 설정한다. 활성화에는 저장된 공개키를 다시 입력해야 한다. 이 화면의 인증 방식은 API 키이며 운영자 OIDC 토큰을 사용하지 않는다. 목록은 다음 페이지로 이동할 수 있고 API 키와 입력한 공개키는 화면 이탈 또는 지우기로 제거한다. 변경 중 화면 이동을 막고 네트워크 오류나 서버 오류는 결과 불명으로 표시하며 자동 재시도하지 않는다. 목록을 다시 조회해 상태를 확인하고 필요하면 아래 변경 이력에서 감사 기록을 살펴봐야 한다. 이력에는 API 키의 별도 `audit` 권한이 필요하다. 마지막 활성 키를 끄거나 삭제하면 웹훅 수신이 중단될 수 있으며 이미 검증 중인 요청은 완료될 수 있다. Angular 단위 테스트는 모의 API에서 페이지 이동, 활성화 요청, 응답 유실과 지연된 조회를 확인했고 브라우저에서 화면 표시 및 선택을 확인했다. 실제 Rust API에 연결한 브라우저 검증은 아래 재현 항목을 참고한다. 실제 SendGrid 키 교체·공급자 수신, 외부 IdP와 운영 부하는 미검증이다.

격리된 브라우저 재현에는 `npm run build` 후 `python3 -m http.server 4259 --bind 127.0.0.1 --directory dist/swallow/browser`와 별도 터미널의 `bash scripts/preview-sendgrid-key-fixture.sh`를 사용한다. 스크립트가 출력하는 API 주소를 UI에 입력하고 테스트 API 키 `ui-sendgrid-test-key`와 출력된 임시 디렉터리의 `public-key`를 사용한다. Docker PostgreSQL과 제한 DB 계정·실제 Rust API만 사용하며 Ctrl-C로 컨테이너와 임시 키를 삭제한다. 브라우저에서 등록·목록·비활성화·재활성화·삭제, DB의 네 감사 기록과 읽기 권한 회수 후 조회 거절을 확인했다. 이 시험은 실제 SendGrid나 외부 발송을 호출하지 않았고 운영 IdP·장애 전환·실제 공급자 회전은 미검증이다.

같은 화면의 ‘공개키 변경 이력’은 API 키의 별도 `audit` 권한으로 환경별 감사 이력을 조회한다. 이전·다음 페이지를 제공하며 인증을 지우거나 바꾸면 표시와 대기 중인 요청을 지운다. 권한 회수 등 조회 오류에서도 이전 결과를 지운다. UUID 정렬은 시간·커밋 순서가 아니고 페이지마다 다른 시점이므로 신규 기록을 놓칠 수 있다. 이력에 없는 항목은 미실행의 증거가 아니며 직접 DB 변경이나 감사 위변조도 검출하지 못한다. Angular 테스트에서 페이지·인증 오류·지연 응답을 확인했고 위 격리 fixture의 실제 API와 브라우저에서 키 등록 이력 표시 및 감사 권한 회수 후 이력 제거를 확인했다. 운영 규모·원본 UI와의 비교, 외부 감사 연동은 미검증이다.

원본 Novu React 대시보드는 상위 디렉터리에 별도로 남아 있다. 이 앱이 원본 Dashboard와 동일한 UI·기능·API 계약이라고 주장하지 않는다.

구독자 화면에서 기존 API 키 방식을 선택하면 `Authorization: ApiKey`로 `GET /v2/subscribers`를 조회한다. 키는 애플리케이션 메모리에만 유지하고 브라우저 저장소에 쓰지 않으며 키 지우기·페이지 이탈·API 주소 변경 시 결과와 함께 지운다. HTTP 로컬 호스트 외의 서버는 HTTPS를 요구하고 redirect·쿠키·캐시를 사용하지 않는다. 이름·이메일·구독자 ID만 화면에 표시하지만 API 응답 자체는 기기 토큰 등 민감한 채널 데이터를 포함할 수 있다. 네트워크 도구, 브라우저 확장, 악성 스크립트, 사용 중인 기기 및 로그에 대한 보호는 이 기능만으로 제공되지 않는다. 기존 API 키는 환경의 넓은 권한을 가질 수 있으므로 격리된 테스트 환경의 키만 사용하고 운영 키를 브라우저에 입력하지 않는다. 정식 운영은 사용자 세션·최소 권한 관리 API가 필요하다.

조회 전용 인증은 기본 선택이다. 별도 발급된 64자리 소문자 16진수 인증 값을 `SwallowConsole` 헤더로 `/v1/experimental/console/subscribers`에 보낸다. 구독자 권한이 있는 인증만 이 경로를 사용할 수 있으며 구독자 ID·이름·이메일만 반환하고 기기 토큰은 반환하지 않는다. 인증 값은 다른 일반 API에서 허용되지 않고, DB에서 환경별 만료·회수된다. 서버 관리자는 API 마이그레이션 0024–0031을 적용하고 API DB 로그인에 `GRANT EXECUTE ON FUNCTION public.novu_rust_console_subscribers(text,text,text,text)`를 부여해야 한다. 자세한 발급과 한계는 Rust API README를 참고한다. 이 값도 브라우저 메모리와 네트워크에 존재하는 비밀이며, 개인별 로그인·세션·권한 관리나 악성 브라우저 확장 방지 수단이 아니다.

운영 경보 화면은 별도로 조회 권한이 부여된 인증으로 `/v1/experimental/console/alerts`를 호출한다. API 0028–0030, 워커 경보 테이블과 실행 권한·인증별 경보 조회 권한이 필요하며 상세 절차는 Rust API README를 참고한다. 페이지당 최대 100개를 표시하며 다음·이전 페이지와 처음으로 이동할 수 있다. 각 페이지는 별도 시점이므로 변경 중인 목록은 처음부터 다시 확인해야 한다. 경보 발송 상태는 수신 보장이나 서비스 전체 정상 여부가 아니다. 자동 새로고침은 기본으로 꺼져 있으며 15·30·60초 중 선택할 수 있다. 첫 페이지에서 조회가 완료되고 판정을 검토하지 않을 때만 실행한다. 다른 페이지·판정 검토·판정 진행 중에는 건너뛰고, 조회 실패·인증 변경·화면 이탈·인증 만료 시 중지한다. 조회 완료 후 다음 간격을 기다리므로 요청이 겹치지 않는다. 선택을 끄더라도 이미 진행 중인 조회는 완료될 수 있다. 경보 설정·발송은 미구현이다. 인증은 화면 이탈·주소 변경 시 지워지지만 브라우저와 기기의 보안은 별도 관리해야 한다.

API 마이그레이션 0035 적용 시 운영 경보 표에 현재 발송 시도 ID가 표시된다. 시도가 없으면 `—`이고 이전 API의 응답에 필드가 없으면 `API 업데이트 필요`로 표시한다. ID는 조회 시점의 값으로 이후 바뀔 수 있다. 실제 결과를 독립적으로 확인한 뒤 최신 ID로 별도 판정 권한 인증을 사용해 화면에서 판정하거나 워커의 `alert-resolve-v2` 명령에 전달할 수 있다. 화면에서 직접 재발송하지는 않는다. ID만으로 공급자 접수·수신이 확인되는 것은 아니다. 기존 경보 조회 권한을 가진 인증에는 ID도 노출되므로 해당 접근 범위를 검토한다.

격리된 Docker PostgreSQL과 제한 DB 로그인으로 Rust API를 실행하고 실제 브라우저에서 권한 없는 인증 거절, 105건의 100건·5건 조회, 이전 이동과 화면 이탈 시 인증 삭제를 확인했다. 별도의 테스트 JWT 시험에서는 15초 자동 갱신으로 DB 경보 상태 변경이 반영되고, 판정 검토 중 사유가 유지되며 조회 권한 회수 시 오류 표시와 자동 갱신 중지를 확인했다. 실제 운영 인증·부하 및 공급자 전송을 검증한 것은 아니다.

경보 판정 화면은 별도의 `retry` 또는 `accepted` 권한 인증, 사유와 조사 확인을 요구한다. `accepted`는 결과 불명 상태에서만 선택할 수 있고 수신 보장이 아니다. `retry`는 다음 예약 발송을 허용하며 중복 위험이 있다. 전송 중에는 화면 이동과 중복 제출을 막고, 완료 또는 오류 후 목록과 판정 인증을 지운다. 네트워크 오류·응답 해석 실패 등의 결과 불명은 자동 재시도하지 않으며, 재조치 전에 상태와 감사를 확인해야 한다. 브라우저 종료·새로고침이나 통신 중단이 서버 작업을 취소한다는 보장은 없다. 읽기와 판정은 서로 다른 인증 값을 사용할 수 있다. API 0036과 워커 0131, 각 DB 권한이 필요하다. 정식 개인별 로그인·SSO는 제공하지 않는다.

`내 판정 이력 확인`은 API 0037을 통해 현재 판정 권한이 있는 **같은 인증**으로 기록한 해당 환경·채널·대상·시도의 최근 20건을 보여준다. 화면 판정 이후 대상과 시도는 미리 채워지며 인증은 다시 입력한다. 인증 변경·화면 이탈 시 결과를 지우고, 사용자는 이력 지우기를 선택할 수 있다. 이력이 비어도 원래 요청이 진행 중이거나 다른 인증·환경을 사용했을 수 있으므로 실패로 단정하지 말고 관리자·워커 감사와 원래 트랜잭션을 확인한다. 인증이 만료·회수되거나 마지막 판정 권한이 회수되면 조회할 수 없으며, 인증을 교체하면 이전 인증의 이력을 조회할 수 없다. 이 기능은 공유 인증의 개인 신원을 확인하거나 공급자 전달을 증명하지 않는다.

경보 판정은 기본적으로 API 0038의 요청 ID 방식을 사용하며, 이전 API 배포에서는 운영자가 명시적으로 기존 방식을 선택할 수 있다. 요청 ID 방식은 브라우저가 암호학적으로 안전한 UUID 또는 인증 지문을 만들지 못하면 요청을 보내지 않고 자동으로 기존 방식으로 전환하지 않는다. 결과 화면과 요청 조회에 ID를 표시하므로 새로고침·화면 이탈 전에 따로 보관해야 한다. ID와 인증은 브라우저 저장소에 저장하지 않으며, 화면 이탈 시 지운다. 같은 인증으로 요청 ID의 커밋 여부를 조회할 수 있지만, 조회에서 찾지 못해도 원래 트랜잭션이 진행 중일 수 있다. 결과가 불명확하면 운영자가 같은 인증과 API 주소로 원래 요청을 확인한 뒤 동일 요청 ID와 내용을 수동 재제출할 수 있다. 자동 재제출은 없고 기존 방식은 재제출을 제공하지 않는다. 같은 ID의 재제출 계약, 기록 보존과 장애 전환 제한은 API README를 참고한다. 이 기능에는 API 0038과 해당 함수의 별도 EXECUTE 권한이 필요하다.

재제출을 위해 원래 요청 내용과 인증 값의 SHA-256 지문을 화면 메모리에만 보관하며, 인증 원문은 다시 입력받는다. 다른 인증이나 주소로는 보내지 않는다. 재제출이 거절되거나 통신이 실패해도 원래 요청이 성공했을 수 있으므로 결과는 불명으로 남는다. 화면을 떠나거나 목록을 다시 조회하거나 새로고침하면 재제출 정보는 사라진다. DB 요청 기록 보존과 동일 DB 사용이 중복 방지의 전제이며 오래된 백업 복원, 기록 삭제 또는 비동기 장애 전환은 이를 깨뜨릴 수 있다. 외부 발송의 중복이나 실제 수신을 보장하지 않는다. UI 단위 테스트에서 인증 불일치, 동일 본문 재제출, 거절 후 결과 불명, 동시 제출 차단과 지문 생성 실패를 검증했다. 격리된 Docker PostgreSQL·제한 API DB 로그인과 실제 브라우저에서 최초 API 성공 응답 직후 브라우저 fetch가 오류를 내도록 응답 유실을 모의했다. 동일 요청 재제출은 `replayed: true`였고 요청과 판정 감사는 각각 한 건이었다. 실제 네트워크 단절·운영 부하·HA 장애 전환은 검증하지 않았다.

운영자 JWT 방식은 외부 인증 공급자가 미리 발급한 접근 토큰과 환경 ID를 입력받아 별도 운영자 구독자·경보 API를 호출한다. 접근 토큰을 직접 입력하거나 아래의 실험적인 OIDC 팝업 로그인을 선택할 수 있다. 수동 입력 토큰은 자동 갱신하지 않는다. 팝업 로그인은 선택형 자동 갱신을 지원한다. API 0040–0042의 설정과 주체별 권한, DB 로그인 권한이 필요하며 상세 설정은 Rust API README를 참고한다. 구독자 읽기와 경보 읽기·판정은 독립 권한이다. JWT 판정은 요청 ID 방식만 사용하며 이 화면에서 직접 재제출하려면 최초 토큰과 동일한 토큰을 입력하거나 해당 로그인 토큰을 선택해야 한다. 요청 결과 조회는 같은 발급자·주체·클라이언트·환경에 속한 새 토큰도 사용할 수 있다. 시도별 판정 이력 화면은 콘솔 인증에서만 제공한다. 직접 입력한 토큰은 브라우저 메모리와 HTTPS 요청에 존재하며 화면 이탈이나 인증 방식 변경 시 지워진다. 팝업 로그인 세션은 로그아웃 또는 설정 변경 시 지워진다. 원격 API에는 HTTPS가 필요하지만 브라우저 확장·악성 스크립트·기기에 대한 보호나 서버 측 권한 관리를 대신하지 않는다.

UI 빌드와 단위 테스트 30개를 통과했다. 테스트 전용 키로 서명한 토큰, 격리된 Docker PostgreSQL과 실제 브라우저에서 구독자·경보 조회, 판정과 요청 조회를 확인했다. 판정의 최초 성공 응답 직후 브라우저 fetch 오류로 응답 유실을 모의하고 동일 토큰으로 다시 제출했을 때 서버가 `replayed:true`를 반환했으며 요청·감사는 각각 한 건이었다. 실제 인증 공급자·네트워크 단절·운영 부하·HA는 검증하지 않았다.

### 채널별 단일 워크플로 단계

워크플로 화면에서 환경 API 키와 기존 워크플로 식별자를 입력하고 이메일·SMS·푸시·Slack 중 하나를 선택해 해당 단계의 제목·내용·활성 상태를 조회하고 수정할 수 있다. 템플릿 사용을 해제하면 내용이 null이 된다. SMS와 Slack에는 제목이 없다. UI 식별자는 ASCII 영문·숫자·하이픈·밑줄로 시작하고 뒤에 점을 포함할 수 있으며 최대 256자다. 채널 설정별 읽기와 쓰기는 별도 DB 권한을 요구한다. 설정과 배포 절차는 [API README](../../api/rust-api/README.md)의 `channel-steps`와 `slack-step`을 참고한다. API 키는 다른 권한도 가질 수 있으므로 접근을 제한한다.

인증 방식을 운영자 JWT로 선택하면 별도의 환경 ID와 토큰으로 같은 단계와 이메일 공급자를 관리한다. 개요에서 OIDC 로그인 후 `워크플로 설정`을 선택하거나 토큰을 직접 입력할 수 있다. API `0050`·`0051`과 주체·환경·자원별 별도 읽기·쓰기 권한이 필요하고, 쓰기에는 감사 사유를 입력한다. `0052`와 별도 감사 조회 권한이 있으면 워크플로와 대상을 선택해 UUID순 이력을 50건씩 조회할 수 있다. 시간순이 아니며 페이지는 같은 스냅샷이 아니다. 사유와 주체 등 민감한 정보가 표시되지만 템플릿 원문은 표시하지 않는다. 요청 ID가 없어 감사만으로 결과 유실된 특정 요청을 확정하지 못할 수 있다. 권한 부여는 현재 DB 관리자가 수행한다. 토큰 만료 시 조회 중인 요청과 설정을 지우며 이미 전송한 변경의 성공·실패·불명 결과는 유지한다. Angular 테스트 75개와 빌드가 통과했고, 실제 브라우저·Rust API·격리 DB에서 테스트 JWT로 이메일 단계와 공급자 변경·감사, 쓰기 및 감사 권한 회수를 확인했다. 브라우저 fixture의 API DB 로그인에는 API 키 경로 시험을 위한 추가 권한이 있다. 실제 외부 IdP와 운영 부하·HA는 미검증이다.

JWT 변경은 API `0053`의 요청 ID 방식으로 전송하며 DB 로그인에 해당 쓰기·요청 조회 함수 실행 권한이 추가로 필요하다. 결과가 불명일 때 같은 토큰·환경·주소로 원래 내용을 재제출하거나 같은 운영자의 새 토큰으로 요청 ID와 자원을 조회할 수 있다. 기록 없음은 실패 확정이 아니다. 재제출용 원문은 이 화면 메모리에만 보관하며 화면 이탈·로그아웃·새로고침 시 삭제되므로 요청 ID를 따로 보관한다. API 키 방식은 요청 ID를 사용하지 않는다. Angular 테스트 77개와 빌드가 통과했고 브라우저에서 실제 테스트 API의 커밋 후 응답 유실을 모의해 조회와 재제출을 확인했다. 실제 네트워크 단절·외부 IdP·운영 HA는 미검증이다.

같은 화면의 `이메일 공급자`에서 Resend 또는 SendGrid를 조회·선택할 수 있다. 별도 공급자 읽기·쓰기 권한이 필요하며 자격증명은 변경하지 않는다. 조회된 이전 공급자를 조건으로 보내므로 다른 설정이 먼저 적용되면 409가 반환된다. 신규 워크플로 처리에 반영되며 이미 생성된 이메일 작업에는 적용되지 않는다. 새 API를 먼저 배포해야 한다. 브라우저·Rust API·격리 PostgreSQL에서 SendGrid→Resend 변경, 감사 기록과 충돌을 확인했으며 실제 발송은 하지 않았다.

이 기능은 단일 단계에 한정하며 전체 워크플로 생성·순서·전체 공급자 설정 또는 원본 Novu 관리 API를 구현하지 않는다. 최신 API를 먼저 배포해야 하며, 화면은 조회한 이전 설정을 함께 보내 변경 여부를 비교한다. 구버전 API는 이 요청을 거절한다. 동일 값으로 복원된 변경이나 조건 없는 구버전 클라이언트의 덮어쓰기는 방지하지 못한다. 타임아웃·연결 오류·5xx는 저장 결과가 불명이며 자동 재시도하지 않는다. 원래 요청이 계속 진행될 수 있으므로 다시 저장하기 전에 설정과 감사 기록을 확인한다. 기존 대기 작업의 처리 방식은 워커 정책에 달려 있다. 키와 템플릿은 탭 메모리에 보관하며 화면 이탈 시 지운다. Angular 테스트 63개와 빌드가 통과했고, 격리 PostgreSQL과 실제 API·브라우저에서 이메일과 Slack 조회·변경 및 감사 기록, 이메일의 동시 변경 충돌을 확인했다. SMS·푸시는 UI 단위 테스트로 요청 형식을 검증했다. 실제 공급자 발송, 외부 IdP와 원본 Novu UI 비교는 미검증이다.

### 실험적인 OIDC 팝업 로그인

운영자 OIDC 토큰 교환·갱신 요청은 리다이렉트를 거절하고 쿠키를 보내지 않으며 응답을 64 KiB로 제한한다. 별도의 토큰 회수 요청도 기존처럼 리다이렉트를 거절한다. 현재 고정된 `oidc-client-ts` 3.5.0은 인스턴스별 전송 훅을 제공하지 않아 두 내부 토큰 클라이언트를 제한하고 예상 구조가 없으면 로그인 자체를 거절한다. 의존성을 업데이트할 때는 내부 구조와 실제 인증 코드·갱신 경로를 다시 검증해야 한다. 공급자 인증 페이지의 브라우저 내 이동은 이 제한의 범위가 아니다. 로컬 OIDC 공급자·Docker PostgreSQL·실제 브라우저에서 정상 로그인과 인증 코드 교환 및 갱신 시 각각 307 응답을 확인했다. 각 토큰 엔드포인트 요청은 도착했고 리다이렉트 대상에는 요청이 없었다. 단위 테스트는 두 토큰 클라이언트의 요청 옵션과 응답 크기 제한을 확인한다. 별도 로컬 포트의 교차 출처 수신기를 사용한 실제 브라우저 시험에서 보호하지 않은 가짜 POST가 수신기에 도달하는 것을 확인한 뒤, 인증 코드의 307과 갱신 토큰의 308은 각각 원래 토큰 엔드포인트까지 도달했지만 수신기에 전달되지 않았다. `node scripts/test-oidc-redirect-fixture.mjs`는 같은 loopback fixture의 기본 리다이렉트와 차단을 합성 데이터로 재현하며 포트 4251·4252가 비어 있어야 한다. 실제 외부 IdP와 다른 라이브러리 버전은 미검증이다.

HTTPS 발급자의 검색 결과에서 인증·토큰·JWKS 주소가 HTTP이면 로컬 주소라도 거절하며 로그인 직전에도 재검사한다. 로컬 HTTP 발급자의 로컬 HTTP 엔드포인트는 개발 fixture 용도로 유지한다. 이는 [OpenID Connect Discovery](https://openid.net/specs/openid-connect-discovery-1_0.html)의 HTTPS 요구와 별도로 둔 개발용 예외다. Angular 테스트는 세 주소 각각의 하향 전환과 사전 검증 우회, 로컬 fixture를 확인한다. 격리된 Docker PostgreSQL·시험 OIDC 공급자·실제 브라우저에서 로컬 로그인도 확인했다. 첫 시도는 fixture의 `operator_jwt_self` 실행 권한이 없어 실패해 임시 DB 권한을 보완한 뒤 성공했으며 fixture 스크립트에도 이를 추가했다. 실제 외부 IdP와 엔드포인트의 리다이렉트 정책은 이번에 검증하지 않았다.

개요 화면에서 OIDC 발급자, 공개 클라이언트 ID, 환경 ID와 필요한 scope·resource·audience를 설정하고 공급자 확인 후 로그인할 수 있다. Authorization Code와 PKCE S256을 사용하며 ID 토큰의 서명, 발급자, 대상, nonce를 확인하고 Rust API에서 검증한 주체와 환경이 일치할 때만 구독자·경보 화면에 적용한다. 기존 수동 토큰 입력과 콘솔 인증도 선택할 수 있다. 자동 갱신은 선택 사항이며 로컬 로그아웃은 공급자 세션이나 이미 발급된 토큰을 회수하지 않는다. 토큰은 브라우저 메모리에 존재하며 새로고침하면 사라진다.

로그인 토큰의 만료 시 해당 토큰의 구독자·경보 목록과 판정 요청 조회 결과를 지우고 관련 진행 중 조회를 취소한다. 응답 도착 시에도 만료를 확인해 타이머가 늦어진 경우 늦은 응답을 표시하지 않는다. 판정 요청의 결과 불명 상태와 요청 ID는 보존한다. 이 처리는 클라이언트 시계에 의존하며 서버 검증과 원격 토큰 회수를 대신하지 않는다. Angular 단위 테스트로 기존 결과와 지연 응답 정리를 확인했으며 실제 외부 IdP로 이번 동작을 검증하지 않았다.

권한 신원·감사·보관 요청 조회도 응답 적용 전에 만료를 다시 확인하고, 만료 시 결과를 지우면서 입력한 보관 요청 ID는 보존한다. 권한 변경과 경보 판정의 인증 지문 계산 중 토큰이 만료되면 요청을 보내지 않는다. 이미 전송된 권한 변경의 응답과 요청 ID, 결과 불명 경보 판정의 요청은 보존해 후속 확인을 할 수 있다. Angular 단위 테스트는 지연 조회와 발송 전·후 만료를 포함해 68개가 통과했다. 실제 외부 IdP와 실제 네트워크 지연은 검증하지 않았다.

공급자는 검색 메타데이터에서 S256을 광고하고 브라우저에서 메타데이터·토큰·JWKS에 접근할 수 있어야 한다. 등록된 콜백은 UI origin의 `/oidc-callback`이고 서버는 이 경로를 앱으로 제공해야 한다. API에는 기존 실험 운영자 JWT 설정과 환경별 권한이 필요하고 공급자가 발급한 접근 토큰이 API의 검증 계약을 충족해야 한다. 원격 통신에는 HTTPS가 필요하다. 콜백 URL이 프록시 접근 로그 등에 기록되지 않도록 배포 환경을 설정한다.

이 기능은 실험적이며 Novu의 관리자 로그인과 호환된다는 의미가 아니다. 실제 외부 인증 공급자와 운영 환경에서 검증하지 않았고, 공급자 세션 로그아웃과 완전한 사용자·권한 관리는 제공하지 않는다. 선택형 공급자 갱신 토큰 회수는 아래 설명을 참고한다. 브라우저 확장과 악성 스크립트로부터 토큰을 보호하지 못한다.

격리된 Docker PostgreSQL, 제한 DB 로그인, 테스트 전용 로컬 OIDC 공급자와 실제 브라우저로 PKCE 팝업 로그인, ID 토큰 서명 검증, API 신원 확인, 구독자 조회 및 로컬 로그아웃 후 화면 인증 삭제를 확인했다. 테스트 공급자는 실제 상용 공급자의 동작이나 운영 환경을 대변하지 않는다. 단위 테스트는 신뢰하지 않은 키로 서명된 ID 토큰의 거절도 확인한다.

팝업으로 로그인한 접근 토큰은 API 검증 후 JWT 만료 시각과 공급자 응답의 만료 시각 중 더 이른 시각에 화면에서 제거하고 재로그인을 안내한다. 브라우저 시간이 부정확하거나 탭이 중지되면 화면의 만료 표시는 실제 시각과 차이가 날 수 있다. 권한과 만료는 요청마다 서버가 최종 확인하며, 권한 변경은 만료 안내와 별개이다. 만료 시 결과 불명인 경보 판정의 요청 ID와 결과는 지우지 않는다. 선택형 자동 갱신과 공급자 갱신 토큰 회수는 아래 설명을 참고한다. 단위 테스트는 지연된 브라우저 타이머와 판정 정보 보존을 확인했고, 격리된 Docker PostgreSQL·로컬 OIDC 공급자와 실제 브라우저에서 짧은 수명의 토큰이 만료될 때 안내와 입력란 삭제를 확인했다. 실제 공급자 및 운영 부하는 검증하지 않았다.

OIDC 로그인으로 경보 화면을 열면 판정·결과 조회·동일 요청 재제출에서 현재 로그인을 선택해 인증 값을 입력할 수 있다. JWT 방식과 로그인 환경이 일치할 때만 선택할 수 있고 판정·조회 권한은 요청마다 서버가 확인한다. 재제출은 최초 요청과 정확히 같은 토큰이 필요하므로 다른 토큰으로 다시 로그인했다면 직접 재제출은 거절되며 같은 주체의 유효한 토큰으로 요청 결과를 조회할 수 있다. 만료 시 해당 입력란의 로그인 토큰을 지우되 판정 요청 ID와 결과는 보존한다. UI 단위 테스트에서 선택, 환경 불일치, 만료 및 결과 불명 후 동일 토큰 재제출을 검증했고 격리된 Docker PostgreSQL·로컬 OIDC 공급자와 실제 브라우저에서 로그인 후 경보 판정과 요청 결과 조회를 확인했다. 실제 공급자와 실제 네트워크 장애는 미검증이다.

설정 화면에서 현재 OIDC 로그인 또는 직접 입력한 운영자 JWT와 환경 ID로 특정 발급자·주체·클라이언트의 권한을 조회하고 별도로 허가된 감사 이력을 조회할 수 있다. 서버에 API 0045와 해당 주체의 신원 조회·감사 조회 권한을 따로 설정해야 한다. 조회 이력은 최대 50건씩 다음 페이지로 이동하며 이전·현재 값과 사유를 보여준다. 페이지는 독립 스냅샷이고 기록 ID는 커밋 순서를 보장하지 않으므로 동시 변경을 누락할 수 있다. 감사 데이터는 민감하며 화면 이탈·토큰 만료·인증 또는 환경 변경 시 이 화면의 인증이나 결과를 지운다. 관리 권한 자체를 부여하는 기능은 아직 없다. 기본 권한 변경은 아래를 참고한다. UI 단위 테스트에서 권한 거절·페이지 이동·늦게 도착한 응답 무시·만료 및 화면 이탈을 확인했다. 로컬 OIDC 공급자·제한 API 로그인·격리된 Docker PostgreSQL과 실제 브라우저에서 권한과 감사 조회, 화면 이탈 후 삭제를 확인했다. 실제 외부 인증 공급자와 운영 환경은 미검증이다.

API 0046 이후 JWT를 사용한 HTTP 권한 변경 감사는 DB 로그인 외에 JWT 발급자·주체·클라이언트와 요청 ID도 표시한다. 이전 DB 직접 변경에는 JWT 작업자가 없다. 이 화면에서 기본 권한을 변경할 수 있다.

설정 화면에서 대상 권한을 조회한 뒤 다섯 기본 권한과 사유를 입력해 변경하거나 없는 주체를 등록할 수 있다. 서버에 API 0046과 별도 변경 권한이 필요하며 자신의 기본 권한이나 관리 권한은 이 API로 변경할 수 없다. 조회한 이전 상태가 달라지면 서버가 요청을 거절한다. 결과 불명 시 화면에 요청 ID와 이전·변경 값이 남으며 같은 토큰·환경·API 주소로 요청 결과를 조회하거나 확인 후 동일 요청을 재제출할 수 있다. 원래 요청이 아직 진행 중일 수 있으므로 조회 결과가 없어도 실패로 단정하지 않는다. 화면을 떠나거나 새로고침하면 요청 정보가 지워지므로 ID와 내용을 별도로 보관해야 한다. 자동 재제출과 실제 외부 IdP·실제 네트워크 단절 검증은 제공하지 않거나 미완료다. 새 토큰을 이용한 보관 요청 조회는 아래를 참고한다. UI 테스트에서 요청 원문·인증 일치와 처리 중 화면 이동 차단을 확인했고, 로컬 OIDC 공급자·Docker PostgreSQL·실제 브라우저에서 성공 응답 직후 fetch 오류를 두 차례 모의해 결과 조회 및 동일 요청 재제출을 확인했다. 두 변경 각각 요청과 감사는 한 건이었다. 백업 복원이나 비동기 장애 전환 등 서버 중복 방지 한계는 API README를 참고한다.

화면 이탈 또는 새로고침 이후에도 보관한 요청 ID를 설정 화면의 `보관한 변경 요청 확인`에 입력하면 같은 발급자·주체·클라이언트·환경의 유효한 새 JWT로 커밋된 요청을 조회할 수 있다. 현재 변경 권한이 필요하며 다른 주체의 요청은 찾을 수 없다. 반환된 대상, 사유와 이전·변경 값을 보관한 원본과 대조해야 한다. 찾지 못했다는 응답은 원래 요청이 진행 중일 수 있으므로 실패의 증거가 아니다. 이 조회는 변경을 재실행하지 않는다. UI 단위 테스트와 로컬 OIDC 공급자·Docker PostgreSQL·실제 브라우저에서 성공 응답 유실 모의 후 새로고침, 서로 다른 새 토큰으로 재로그인 및 보관 요청 조회를 확인했다. 다른 주체의 요청 격리는 Docker PostgreSQL과 API 라우터 테스트로 확인했다. 실제 외부 IdP와 실제 네트워크 장애는 미검증이다.


운영 경보 화면에서 인증 방식을 운영자 JWT로 선택하면 환경 ID와 접근 토큰을 사용해 `이메일 발송 결과`에서 Resend 작업을 목록으로 조회하고 선택하거나 UUID를 직접 입력하여 개별 결과를 확인할 수 있다. 단건 조회에는 API `0048`의 결과 권한이, 목록에는 API `0049`의 별도 목록 권한과 결과 권한이 모두 필요하다. 기존 경보 조회 권한과 독립적이며 권한은 현재 DB 관리자가 부여해야 한다. 목록은 UUID순 20건씩 표시하며 시간순 정렬이나 검색을 지원하지 않는다. 페이지는 같은 시점의 스냅샷이 아니다. 응답의 작업·발송 시도 상태, 전달 결과, 자동 복구 및 충돌과 집계 시각을 표시한다. 충돌과 전달 완료가 함께 오는 등 모순된 응답은 거절한다. Twilio SMS 결과는 아래의 별도 화면을 사용하며 이 화면에서 결과를 변경하지 않는다.

인증과 결과는 화면 메모리에만 두며 인증·환경·작업 ID 변경, 메뉴 이탈 및 토큰 만료 시 관련 결과를 지우고 진행 중인 요청의 늦은 응답을 무시한다. 직접 입력한 JWT의 유효성은 서버에서 확인한다. 공급자의 접수·전달은 메일 열람을 뜻하지 않고 집계는 비동기다. Angular 단위 테스트 55개와 빌드가 통과했다. 격리된 Docker PostgreSQL과 실제 Rust API에 테스트 JWT를 사용해 브라우저에서 목록과 단건 조회, 목록 권한 회수 후 목록 거부와 단건 조회 유지를 확인했다. 이전 시험에서는 합성 충돌 및 단건 권한 회수 후 오류, 메뉴 이탈 시 결과 삭제를 확인했다. 실제 외부 IdP 로그인, 실제 Resend와 운영 HA는 미검증이다. CSS 크기와 실행 환경의 Node/npm 버전 경고가 남아 있다.


로컬 재현용 `scripts/preview-resend-fixture.sh`는 Docker PostgreSQL, 실제 Rust API, 제한 DB 로그인, 테스트 전용 JWT와 하나의 합성 작업을 준비한다. 먼저 `npm start -- --host 127.0.0.1 --port 4249`로 화면을 실행하고 별도 터미널에서 스크립트를 시작한다. 출력된 `API_URL`을 화면의 API 주소로 지정하고, 출력된 `FIXTURE_DIR/token`의 테스트 토큰, 환경 `ui-fixture-env`, 작업 ID `1889f88e-c0a3-4ab5-b861-2fdc1f26f254`로 조회한다. 스크립트는 브라우저 동작을 자동화하지 않으며 Ctrl-C 시 테스트 DB·API·임시 토큰을 삭제한다. 테스트 키는 저장소의 공개된 테스트 전용 키이므로 운영 환경에 사용하지 않는다. 포트 4249는 UI CORS 테스트를 위한 고정값이다.

## 워크플로 권한 위임

설정 화면의 인증과 대상 발급자·주체·클라이언트 ID를 입력하고 리소스 및 권한을 선택해 현재 부여 상태를 조회한다. 대상은 해당 환경에 이미 등록되어 있어야 하며 별도의 위임 관리 조회·변경 권한이 필요하다. 조회된 이전 값과 변경 사유를 확인해 부여 또는 회수한다. 서버가 상태 변경·감사·요청 기록을 원자적으로 처리한다. 초기 위임 관리자 등록과 DB 함수 실행 권한은 신뢰된 DB 관리자가 설정한다. 상세 조건은 [API README](../../api/rust-api/README.md)의 API 0054를 참고한다.

응답이 유실되면 요청 ID와 표시된 내용을 보관한다. 같은 토큰·환경·API 주소로 동일 요청을 재제출하거나 같은 운영자·클라이언트의 유효한 새 토큰과 요청 ID로 결과를 조회할 수 있다. 재제출 실패는 최초 요청의 실패를 증명하지 않으며 기록이 없더라도 최초 요청이 진행 중일 수 있다. 화면 이탈, 로그아웃 또는 새로고침 시 재제출 정보는 사라진다. 설정 화면에서 별도의 위임 감사 조회 권한으로 변경 이력을 볼 수 있다. UUID순 페이지는 시간순이 아니며, 거절·동일 상태 요청과 DB 직접 수정은 기록되지 않는다. 실제 IdP와 운영 HA는 미검증이다.

## 구독자 생성

구독자 화면에서 환경 API 키 또는 운영자 JWT를 선택하고 ID와 선택적인 이름·이메일·전화번호를 입력한다. JWT 생성은 별도의 생성 권한과 사유가 필요하다. API 키는 `POST /v2/subscribers?failIfExists=true`, JWT는 별도 생성 API를 사용한다. 두 방식 모두 해당 환경에 같은 ID가 존재하면 기존 프로필을 덮어쓰지 않고 충돌을 표시한다. 조회 전용 인증은 생성에 사용할 수 없다. 환경 API 키는 넓은 API 권한을 가질 수 있으므로 격리된 테스트 환경에서만 사용하고 토큰과 환경을 보호한다. 생성 요청 중 메뉴 전환과 인증 변경을 막으며, 요청은 자동 재시도하지 않는다. 성공 응답이 유실되거나 서버 오류가 나면 결과 불명으로 표시한다. 목록 또는 단건 API에서 상태를 확인하고, 존재한다는 사실만으로 해당 요청이 생성한 것으로 단정하지 않는다. 원본에 같은 ID의 별도 요청이 동시에 들어올 수도 있다. JWT 생성 권한은 등록된 주체별로 부여한다. 조건부 수정·삭제는 아래 내용을 참고한다.

## 구독자 수정·삭제

환경 API 키 또는 운영자 JWT를 선택한 뒤 구독자 ID를 조회하면 프로필 revision과 이름·성·이메일·전화번호를 표시한다. JWT는 프로필 조회·수정에 각각 별도 권한과 변경 사유가 필요하다. 변경한 항목만 조건부 PATCH로 보내며 빈 값은 null이다. 삭제는 대상 ID를 다시 입력해야 하며 API 키는 조건부 DELETE, JWT는 별도 권한·사유·요청 ID를 사용하는 삭제 API를 사용한다. 조회 이후 프로필이 변경되거나 구독자가 같은 ID로 재생성되면 거절하고 다시 조회해야 한다. 삭제 시 API가 연관 알림과 일부 이력을 정리한다. 프로필 revision은 자격증명·구독 설정 등 다른 자료의 변경은 추적하지 않는다. 요청 결과가 유실되면 자동 재시도하지 않으며 다시 조회해도 해당 요청의 완료 여부를 항상 확정할 수 없다. 환경 API 키는 광범위한 권한이 있어 개발·격리 환경용이다. JWT 삭제 요청은 아래 복구 절차를 참고한다. API 0056과 [API README](../../api/rust-api/README.md)의 제약을 확인한다.

운영자 JWT의 프로필 수정은 요청 UUID를 생성한다. 응답 유실 시 같은 토큰·환경·API 주소로 동일 요청을 재제출하거나, 같은 운영자의 새 유효 토큰과 보관한 요청 ID로 기록을 조회할 수 있다. 기록이 없어도 요청이 진행 중일 수 있다. 요청 원문은 현재 화면 메모리에만 있고 화면 이탈·로그아웃·새로고침 시 사라진다. 조회 결과는 변경된 프로필 값을 반환하지 않으며 제출한 필드와 사유를 보관한 요청과 비교해야 한다. API 키 방식과 기존 구버전 JWT API에는 이 복구가 적용되지 않는다.

JWT 구독자 생성은 요청 UUID를 사용한다. 응답 유실 시 같은 운영자의 현재 생성 권한으로 요청 결과를 조회하거나 원래 토큰·환경·API 주소로 동일 요청을 재제출할 수 있다. 생성 기록이 있다는 것은 현재 구독자가 동일하게 존재한다는 뜻이 아니다. 기록이 없어도 요청이 진행 중일 수 있다. 재제출 원문은 화면 메모리에만 있으며 이탈·로그아웃·새로고침 시 사라진다. 상세 제한은 [API README](../../api/rust-api/README.md)를 참고한다.

JWT 삭제는 조회한 생성 세대와 프로필 revision을 검사하며 별도 삭제 권한과 사유가 필요하다. 응답 유실 시 같은 운영자의 유효한 토큰으로 요청 ID를 조회하거나 원래 토큰·환경·API 주소로 동일 요청을 재제출할 수 있다. 조회된 삭제 기록과 재제출 성공은 **그 세대의 과거 삭제**를 뜻하며, 같은 ID가 재생성되어 현재 존재할 수 있다. 기록이 없어도 요청이 진행 중일 수 있다. 원문은 화면 메모리에만 있고 이탈·로그아웃·새로고침 시 사라진다. 프로필 revision은 자격증명·구독 설정 등 다른 데이터의 변경을 추적하지 않으며 삭제는 연관 자료를 정리한다. [API README](../../api/rust-api/README.md)에 권한과 제약이 있다.

설정 화면의 위임 권한 선택에서 구독자 프로필 조회·변경과 구독자 생성·삭제를 선택해 별도 위임 관리자 권한으로 조회·부여·회수할 수 있다. 이는 구독자 목록 조회 권한과 별개다. 응답이 유실되면 요청 종류를 구독자 권한으로 선택해 보관한 요청 ID를 조회하거나 같은 토큰·환경·API 주소로 원래 요청을 재제출한다. 원문은 화면 이탈·새로고침 시 사라지며, 기록이 없더라도 요청이 진행 중일 수 있다. 위임 관리자 초기 등록과 권한은 DB 관리자가 설정하며, 설정 화면의 위임 이력 종류에서 구독자 권한을 선택하면 별도 감사 조회 권한으로 이력을 볼 수 있다. UUID순이므로 시간순이 아니며 페이지별 조회 시점이 다르다.

설정의 구독자 변경 이력에서 생성·프로필 변경·삭제 중 하나를 선택해 해당 종류의 별도 감사 조회 권한으로 이력을 볼 수 있다. JWT 관리 경로의 기록만 표시하고 프로필 원문은 반환하지 않는다. 프로필 변경 감사에는 요청 ID가 없다. UUID순 페이지는 시간순이 아니며, 거절·무변경·API 키·직접 DB 변경은 포함되지 않는다.
# 채널 화면 안내

개요의 발송 채널 카드는 현재 제공하는 SendGrid 웹훅 서명 키 관리 화면으로 연결됩니다. 이메일·SMS·푸시 공급자 연결 전체를 이 화면에서 설정하는 기능은 제공하지 않습니다.
# 운영자 토큰 회수와 로그아웃

운영자 OIDC 로그인 후 개요에서 **이 화면에서 로그아웃** 또는 **현재 토큰 회수 후 로그아웃**을 선택할 수 있다. 후자는 확인 체크 후 현재 JWT의 Swallow 자기 회수 API를 호출하고 로컬 인증을 지운다. API 0071까지의 마이그레이션, `check_token_revocation:true` 및 자기 회수 함수 권한이 필요하다. 두 방식 모두 IdP 세션이나 다른 토큰은 종료하지 않는다.

요청 중 화면 이동·인증 설정 변경을 막고 성공 응답은 회수 확인으로 표시한다. 네트워크 오류, 비정상 응답 또는 서버 오류는 회수 결과 불명으로 표시하면서 로컬 인증을 지우고 자동 재시도하지 않는다. 이 경우 관리자가 별도 조회·감사로 확인해야 한다. 브라우저 종료·새로고침과 이미 처리된 다른 요청은 막을 수 없다. Angular 테스트 109개와 빌드가 통과했다. 실제 브라우저·Rust API·Docker PostgreSQL에서 서명된 테스트 JWT 세션을 개발 도구로 주입한 후 버튼으로 회수와 DB 회수·감사 각 한 건을 확인했다. 추가 시험에서는 로컬 테스트 OIDC 공급자와 실제 브라우저에서 인증 코드·PKCE 로그인, 토큰 회수, 새 토큰 재로그인과 로컬 로그아웃을 확인했다. 회수와 감사는 각 한 건이었다. 테스트 DB 로그인은 다른 기능의 권한도 가진다. 외부 IdP는 사용하지 않았다. 운영 IdP·장애 전환은 미검증이다. 개발 환경의 Node/npm 호환성 경고와 기존 CSS 크기 경고가 남아 있다.

로컬 재현은 이 디렉터리에서 각각 별도 터미널로 `node scripts/test-oidc-provider.mjs`, `SWALLOW_UI_TEST_OIDC=true SWALLOW_UI_SELF_REVOKE=true bash scripts/preview-resend-fixture.sh`, `npm run start -- --host 127.0.0.1 --port 4249`를 실행한다. 화면에 출력된 테스트 API 주소, 발급자 `http://127.0.0.1:4251`, 클라이언트 `console`, 환경 `ui-fixture-env`를 입력한다. 테스트 공급자는 인증 절차를 자동으로 승인하고 공개 테스트 키를 사용하므로 개발 격리 환경에서만 사용한다. 종료 시 각 프로세스를 중지하면 fixture 컨테이너와 임시 파일이 정리된다.

FCM 통합 카드의 **변경 이력 조회**는 API `0072`와 환경 API 키의 별도 `audit` 권한을 요구한다. 이전·변경 상태와 사용자 ID를 페이지당 최대 100건 표시하며 UUID순은 시간이나 커밋 순서가 아니다. 인증 변경·권한 회수·화면 이탈 시 표시가 지워진다. 페이지는 단일 스냅샷이 아니고 거절된 요청과 직접 DB 변경을 기록하지 않으므로 비어 있는 이력만으로 요청 실패를 확정할 수 없다. Angular 테스트 116개와 빌드가 통과했고, 격리 Docker PostgreSQL·실제 API·브라우저에서 이력 표시와 감사 권한 회수 후 표시 제거를 확인했다. 이 브라우저 fixture의 API DB 계정은 관리자 권한이며 실제 FCM과 운영 HA는 미검증이다.

‘발송 채널’의 Slack 통합과 연결 카드는 환경 API 키로 통합 및 연결 식별자를 목록 조회·등록·삭제한다. 워커 `0075`·`0076`, API `0022`, 해당 API 키의 별도 Slack 통합 `read`·`write` 권한과 API DB 권한이 필요하다. 목록은 식별자순으로 최대 100건씩 반환하고 다음 페이지로 이동한다. 기존 DB의 생성 규칙 밖 식별자는 목록에 나타나더라도 화면에서 수정·삭제할 수 없다. 페이지는 단일 스냅샷이 아니다. 키는 화면 메모리에서만 사용하며 화면 이탈·인증 지우기 때 제거한다.

Slack 토큰, 채널 엔드포인트, 워커의 자격증명 맵이나 실제 Slack 연결 여부는 여기서 관리하지 않는다. 참조 중인 항목은 서버가 삭제를 거절할 수 있다. 화면에서는 삭제 전에 대상을 확인하고 생성 UUID를 조건으로 보내므로 확인한 대상을 다른 관리자가 같은 ID로 새로 만들면 삭제를 거절한다. 조건 없는 구형 API 호출은 계속 허용되며 대기 작업과 워커 설정은 별도로 확인해야 한다. 결과 불명 시 자동 재요청하지 않고 목록과 서버 감사 기록을 확인한다. 별도 감사 권한으로 이력을 조회할 수 있으나 삭제된 메타데이터와 실제 공급자 상태의 복구는 제공하지 않는다. 전체 Novu 통합·연결 API와 같은 계약이 아니다.

Angular 테스트 120개와 빌드가 통과했다. 격리 Docker PostgreSQL·실제 Rust API·브라우저에서 통합·연결 등록과 목록, 참조 중인 통합 삭제 거절, 연결과 통합 삭제, 성공 감사 네 건과 읽기 권한 회수를 확인했다. 브라우저 fixture의 API DB 계정은 관리자 권한이므로 최소 권한 배포를 검증하지 않았다. 실제 Slack, 외부 인증 공급자, 원본 UI 및 운영 HA는 미검증이다.

Slack 카드의 **Slack 변경 이력 조회**는 API `0074`와 해당 환경 API 키의 별도 `audit` 권한이 필요하다. 통합·연결 ID, 작업, 사용자 및 기록 시각을 최대 100건씩 표시하며 UUID 순서는 시간이나 커밋 순서가 아니다. 인증 변경·권한 회수·화면 이탈 시 이력을 지운다. 페이지는 같은 스냅샷이 아니며 거절된 요청과 직접 DB 변경은 기록하지 않으므로 이력이 없더라도 원래 요청의 실패를 확정할 수 없다. Angular 테스트 122개와 빌드가 통과했고 격리 Docker PostgreSQL·실제 API·브라우저에서 이력 표시 및 감사 권한 회수 후 표시 제거를 확인했다. 브라우저 fixture의 API DB 계정은 관리자 권한이며 실제 Slack과 운영 HA는 미검증이다.

Slack 카드에서 삭제를 선택하면 먼저 **삭제 대상 확인**으로 단건 조회하고 생성 UUID를 확인한 뒤 삭제 요청에 포함한다. 다른 클라이언트가 그 대상을 삭제하고 같은 ID로 다시 만들면 이전 삭제 요청은 거절된다. API `0075`와 최신 서버가 필요하며 읽기와 쓰기 권한이 모두 필요하다. 조건 없는 구형 API 호출, 직접 DB 변경, 복원 및 복제 손실은 이 보호 범위 밖이다. 참조 중인 항목과 결과 불명에 대한 기존 확인 절차도 필요하다. Angular 테스트 123개와 빌드가 통과했고 Docker PostgreSQL·실제 API·브라우저에서 이전 대상 확인 후 재생성된 통합의 삭제 거절을 검증했다. 브라우저 fixture는 관리자 DB 로그인을 사용했으며 실제 Slack과 운영 HA는 미검증이다.

운영 경보 화면에서 운영자 JWT를 선택하면 **Twilio SMS 발송 결과**에서 작업 목록을 조회하고 선택하거나 UUID를 입력해 결과를 확인할 수 있다. API `0076`의 환경별 단건 조회 권한이 필요하며 목록에는 별도의 목록 권한도 필요하다. 공급자 ID가 중복되면 결과가 `unknown`으로 표시된다. 전화번호·공급자 ID 원문·payload는 이 API에서 제공하지 않는다. 전달 집계와 마지막 순회 시각은 최신 콜백 반영이나 단말 수신을 보장하지 않는다. 목록은 UUID순 20건씩이며 시간순이나 단일 스냅샷이 아니다. 인증·환경 변경과 화면 이탈 시 결과를 지우며 이 화면에서 발송을 재시도하지 않는다.

Angular 테스트 126개와 빌드가 통과했다. 격리 Docker PostgreSQL·제한 API DB 로그인·테스트 JWT·실제 브라우저에서 목록과 단건 조회 및 목록 권한 회수 후 단건 결과 유지를 확인했다. 로컬 재현은 `npm run build` 후 `python3 -m http.server 4249 --bind 127.0.0.1 --directory dist/swallow/browser`와 별도 터미널의 `SWALLOW_UI_TWILIO_FIXTURE=true bash scripts/preview-resend-fixture.sh`를 사용한다. 스크립트가 출력하는 API 주소와 임시 디렉터리의 테스트 토큰, 환경 `ui-fixture-env`를 입력한다. 테스트 데이터와 키만 사용하고 종료 시 컨테이너와 임시 파일을 제거한다. 실제 Twilio·외부 IdP 및 운영 HA는 미검증이다.

운영 경보 화면에서 운영자 JWT를 선택하면 **FCM 푸시 발송 시도**에서 작업 목록 또는 UUID로 대상별 시도 상태의 건수를 조회할 수 있다. API `0077`과 워커 마이그레이션 `0126`·`0128` 및 환경별 단건 조회 권한이 필요하며 목록에는 별도 목록 권한도 필요하다. 취소된 푸시 작업의 별도 수동 판정 표시가 남은 접수·미접수 건수는 별도로 표시하지만 다른 경로의 모든 수동 판정을 포괄하지는 않는다. 기기 토큰과 공급자 ID, payload는 API에서 제공하지 않는다. 아직 시도하지 않은 대상은 집계되지 않고 접수는 실제 단말 수신을 보증하지 않는다. 목록은 UUID순 20건씩이며 시간순이나 단일 스냅샷이 아니다. 인증·환경 변경과 화면 이탈 시 결과를 지우고 이 화면에서 재발송하지 않는다.

Angular 테스트 129개와 빌드가 통과했다. 격리 Docker PostgreSQL·제한 API DB 로그인·테스트 JWT·실제 브라우저에서 목록과 단건 조회 및 목록 권한 회수 후 단건 결과 유지를 확인했다. 로컬 재현은 `npm run build` 후 `python3 -m http.server 4249 --bind 127.0.0.1 --directory dist/swallow/browser`와 별도 터미널의 `SWALLOW_UI_FCM_RESULT_FIXTURE=true bash scripts/preview-resend-fixture.sh`를 사용한다. 스크립트가 출력하는 API 주소와 임시 디렉터리의 테스트 토큰, 환경 `ui-fixture-env`를 입력한다. 테스트 데이터와 키만 사용하고 종료 시 컨테이너와 임시 파일을 제거한다. 실제 FCM·외부 IdP 및 운영 HA는 미검증이다.

운영 경보 화면에서 운영자 JWT를 선택하면 **Slack 작업 상태**에서 앱·웹훅·워크플로 분기·발송 작업을 목록이나 UUID로 조회할 수 있다. API `0078`의 환경별 단건 조회 권한이 필요하며 목록에는 별도 목록 권한도 필요하다. 화면은 작업 상태, 발송 시도, 제한된 실패 분류, 공급자 ID 기록 여부와 과거 판정 이력 존재 여부를 보여준다. 채널·토큰·payload와 원본 오류·판정 사유는 제공하지 않는다. 판정 이력은 이전 시도의 기록일 수 있고 접수는 실제 Slack 수신을 보장하지 않는다. 목록은 UUID순 20건씩이며 시간순이나 단일 스냅샷이 아니다. 인증·환경 변경과 화면 이탈 시 결과를 지우고 이 화면에서 재발송하지 않는다.

Angular 테스트 132개와 빌드가 통과했다. 격리 Docker PostgreSQL·제한 API DB 로그인·테스트 JWT·실제 브라우저에서 목록과 단건 조회 및 목록 권한 회수 후 단건 결과 유지를 확인했다. 로컬 재현은 `npm run build` 후 `python3 -m http.server 4249 --bind 127.0.0.1 --directory dist/swallow/browser`와 별도 터미널의 `SWALLOW_UI_SLACK_RESULT_FIXTURE=true bash scripts/preview-resend-fixture.sh`를 사용한다. 스크립트가 출력하는 API 주소와 임시 디렉터리의 테스트 토큰, 환경 `ui-fixture-env`를 입력한다. 테스트 데이터와 키만 사용하고 종료 시 컨테이너와 임시 파일을 제거한다. 실제 Slack·외부 IdP 및 운영 HA는 미검증이다.

운영 경보 화면의 운영자 JWT 모드에서 **SendGrid 발송 결과**를 작업 목록 또는 UUID로 조회할 수 있다. API `0079`의 환경별 단건 권한이 필요하고 목록에는 별도의 목록 권한도 필요하다. 현재 발송 시도에 기록된 수신자별 결과 건수와 기존 전체 집계를 보여 주며 수신자 주소와 메시지 내용은 반환하지 않는다. 집계는 지연되거나 실제 전체 수신자를 포함하지 않을 수 있으며 접수와 전달 결과가 메일 열람을 보증하지 않는다. 인증·환경 변경과 화면 이탈 시 결과를 지운다. Angular 단위 테스트 135개와 빌드가 통과했다. 격리 Docker PostgreSQL·제한 API DB 로그인·테스트 JWT·실제 브라우저에서 목록과 단건 조회 및 목록 권한 회수 후 단건 결과 유지를 확인했다. 로컬 재현은 `npm run build` 후 `python3 -m http.server 4249 --bind 127.0.0.1 --directory dist/swallow/browser`와 별도 터미널의 `SWALLOW_UI_SENDGRID_RESULT_FIXTURE=true bash scripts/preview-resend-fixture.sh`를 사용한다. 스크립트가 출력하는 API 주소와 임시 디렉터리의 테스트 토큰, 환경 `ui-fixture-env`를 입력한다. 종료 시 컨테이너와 임시 파일을 제거한다. 실제 SendGrid 이벤트, 외부 IdP 및 운영 HA는 검증하지 않았다.

SendGrid 결과 화면은 기록된 수신자 수와 모순되는 `delivered`·`failed` 응답을 거절한다. API `0080`은 이런 모순을 `unknown`으로 반환한다. 이 검사는 기록된 수신자나 이벤트의 완전성을 보장하지 않는다. Angular 테스트 136개와 빌드가 통과했다.

설정 화면의 **워크플로·구독자·채널 조회 권한 위임**에서 SendGrid·Resend·Twilio·FCM·Slack의 결과 및 작업 목록 권한을 선택할 수 있다. API `0081`과 별도 위임 조회·변경·감사 권한이 필요하다. 목록 조회에는 결과와 목록 권한이 모두 필요하다. 이전 상태 조건, 변경 사유, 요청 ID 조회와 감사는 기존 위임 화면과 같은 방식이며 같은 발급자·주체의 자기 변경은 거절된다. 결과 불명 시 요청 기록을 확인한다. Angular 테스트 137개와 빌드가 통과했고, 실제 브라우저·Rust API·제한 DB 로그인에서 권한 부여·이력 조회·쓰기 권한 회수 후 변경 거절을 확인했다. 재현하려면 `npm run build` 후 `python3 -m http.server 4249 --bind 127.0.0.1 --directory dist/swallow/browser`와 별도 터미널에서 `bash scripts/preview-channel-access-local.sh`를 실행하고 출력된 API 주소와 테스트 토큰을 입력한다. 환경은 `ui-fixture-env`, 대상은 `https://issuer.test` / `target-fixture` / `console`이다. 종료 시 임시 PostgreSQL과 테스트 파일이 정리된다. 외부 IdP와 운영 HA는 미검증이다.

API `0082`를 적용하면 위임 관리자를 채널별로 제한할 수 있다. 기존 전체 채널 관리자 권한은 계속 우선하며, 감사 조회 권한은 별도다. 동일한 로컬 화면 fixture는 SendGrid만 관리할 수 있는 테스트 운영자로 시작한다. 실제 브라우저에서 SendGrid 권한 부여와 Twilio 권한 조회 거절을 확인했다.

API `0083` 적용 시 위임 이력의 종류를 채널로 선택하고 공급자를 고르면 해당 공급자만 조회한다. `전체`는 기존 환경 전체 감사 권한이 필요하다. 공급자를 변경하면 기존 결과와 진행 중 요청은 지워진다. DB 관리자가 공급자별 감사 권한과 함수 실행 권한을 설정해야 한다. Angular 테스트에서 필터·다른 공급자 응답 거절·오래된 응답 차단을 확인했다. 위 로컬 fixture는 SendGrid로 감사 범위를 제한한다. 실제 브라우저에서 SendGrid 권한 변경·이력 조회, Twilio 및 전체 조회 거절, 선택 변경 시 결과 제거를 확인했다. 운영 IdP는 미검증이다.

운영자 OIDC 로그인에서 discovery 응답은 65,536바이트, JWKS는 262,144바이트, API 신원 확인 응답은 16,384바이트까지 스트리밍 중에 검사한다. 초과하거나 유효하지 않은 UTF-8이면 로그인을 거절하며, 선언된 Content-Length와 실제 읽은 바이트를 모두 검사한다. 신원 토큰·접근 토큰 검증과 기존 PKCE 정책은 유지된다. Angular 테스트에서 실제 크기가 한도를 넘는 스트림, 잘못된 UTF-8, 큰 JWKS·신원 응답을 거절하고 스트림 취소를 확인했다. 실제 브라우저에서 테스트 OIDC 공급자, 제한된 Rust API 로그인과 임시 PostgreSQL을 연결해 정상 로그인을 확인했다. 외부 IdP, 모든 공급자의 응답 크기와 운영 환경은 검증하지 않았다. 제한은 지원할 공급자의 실제 응답 크기에 영향을 줄 수 있다.

### 운영자 접근 토큰 자동 갱신

개요에서 **접근 토큰 자동 갱신 사용**을 선택하면 `offline_access` 범위를 요청한다. 선택하지 않으면 기본적으로 갱신하지 않으며 `offline_access`를 직접 넣을 수 없다. 공개 클라이언트에 대한 Authorization Code + PKCE와 브라우저 refresh token 발급·CORS를 허용하는 신뢰된 공급자가 필요하다. 공급자가 갱신 토큰을 주지 않으면 선택한 로그인은 실패한다. 토큰은 탭의 메모리에만 보관하며 새로고침·로그아웃 또는 만료 시 버린다. 브라우저 확장·악성 스크립트로부터 보호하지 못한다.

만료 전에 한 번 갱신하며 갱신된 접근 토큰은 Rust API에서 매번 검증하고 최초 로그인과 같은 발급자·주체·클라이언트·환경인지 확인한다. 성공하면 기존 로그인 토큰을 사용하던 조회 입력에 새 토큰을 적용하고 이전 조회 결과와 진행 중 읽기를 지운다. 이미 보낸 변경의 결과와 불명 요청 ID는 보존한다. 불명 요청의 동일 토큰 재제출은 새 토큰으로 불가능하므로 기존 복구 조회 절차를 사용한다. 갱신 실패 또는 응답 유실은 공급자가 토큰을 회전했을 가능성이 있어 자동 재시도하지 않는다. 오류를 표시하고 기존 접근 토큰은 만료까지만 사용한 뒤 재로그인을 요구한다. 탭이 중단되어 갱신이 늦거나 서버 검증에 실패하면 세션은 유지되지 않을 수 있다.

로컬 로그아웃은 갱신 토큰을 메모리에서 삭제하지만 공급자에게 회수를 요청하지 않는다. 별도 버튼으로 공급자 갱신 토큰 회수를 요청할 수 있다. Swallow 자체 회수는 **현재 접근 토큰 하나**만 대상으로 하며 공급자의 갱신 토큰·세션이나 이전에 발급된 접근 토큰은 회수하지 않는다. 유출 대응은 IdP에서 세션과 갱신 토큰을 별도로 폐기하고 다른 접근 토큰도 관리해야 한다. 클라이언트의 토큰 응답 자체는 현재 스트리밍 크기 제한 대상이 아니다. 실제 외부 IdP, 제3자 쿠키 정책, 운영 HA 및 모든 공급자의 회전 정책은 미검증이다.

Angular 테스트 148개와 빌드가 통과했고, 실제 브라우저에서 9초짜리 테스트 토큰과 로컬 OIDC 공급자·Rust API·임시 PostgreSQL을 연결해 여러 번의 회전, API 신원 재검증 및 갱신 후 권한 조회를 확인했다. 테스트 공급자의 회전 동작은 `SWALLOW_TEST_OIDC_REFRESH=true node scripts/test-oidc-provider.mjs`로 활성화할 수 있으며, 공개 테스트 키와 자동 승인 설정이므로 격리 개발 환경에서만 사용한다.

### 공급자 갱신 토큰 회수

자동 갱신으로 로그인했고 신뢰된 공급자의 discovery가 안전한 `revocation_endpoint`를 제공하면, 개요에서 별도 확인 후 **공급자 갱신 토큰 회수 후 로그아웃**을 선택할 수 있다. 회수 요청은 자동 갱신과 겹치지 않으며 현재 갱신 토큰과 공개 클라이언트 ID를 form POST로 보낸다. 원격 공급자에는 HTTPS가 필요하고 루프백 HTTP는 로컬 테스트용이다. 공급자가 브라우저 CORS와 공개 클라이언트의 [RFC 7009](https://www.rfc-editor.org/rfc/rfc7009) 회수를 지원해야 한다. 엔드포인트가 없거나 안전하지 않으면 버튼은 비활성이다.

HTTP 200은 요청 수락으로 표시한다. RFC 7009에서는 이미 유효하지 않은 토큰에도 200을 반환할 수 있으므로 이 응답만으로 토큰 상태나 공급자 세션 종료를 증명할 수 없다. 오류·시간 초과·응답 유실은 결과 불명으로 표시하고 로컬 로그아웃하며, 공급자 관리 도구에서 확인한다. 이 기능은 Swallow의 현재 접근 토큰 회수와 별개이고, 다른 접근 토큰·공급자 로그인 세션·동일 권한의 다른 갱신 토큰을 모두 회수한다고 보장하지 않는다. 완료 또는 결과 불명 후 이 화면은 토큰을 보관하지 않아 같은 요청을 재전송하지 않는다.

Angular 테스트 152개와 빌드가 통과했다. 실제 브라우저·Rust API·임시 PostgreSQL과 로컬 테스트 공급자에서 회수 요청 후 로그아웃을 확인했고, 테스트 공급자의 해당 토큰 재사용은 HTTP 400으로 거절됐다. 이는 자체 테스트 공급자의 결과이며 실제 IdP의 CORS·토큰 회전·회수 정책이나 운영 HA는 검증하지 않았다.

### 공급자 접근 토큰 회수

자동 갱신을 사용하지 않는 OIDC 로그인도 공급자의 안전한 회수 endpoint가 있으면 **공급자 접근 토큰 회수 후 로그아웃**을 선택할 수 있다. 요청 전 확인이 필요하며 `이 접근 토큰을 Swallow에서도 회수 요청`은 기본 선택으로, 해제할 수 있다. 선택하면 Swallow 자체 회수를 먼저 요청한 후 공급자 회수를 요청하며, 두 결과를 각각 표시한다. 한쪽이 실패하거나 응답을 잃어도 다른 쪽을 계속 요청하고 성공 여부를 합쳐서 단정하지 않는다. 요청 결과가 불명이면 각 관리자 도구와 감사를 확인한다. 둘은 원자적인 단일 작업이 아니며 완료 후 로컬 토큰은 지워진다.

Swallow 자체 회수에는 API의 토큰 회수 설정과 권한이 필요하다. 공급자 회수만으로 Swallow가 오프라인 JWT 검증에서 토큰을 즉시 거절한다고 보장할 수 없다. 공급자 회수 요청 수락은 현재 접근 토큰 외의 갱신 토큰·다른 접근 토큰·공급자 로그인 세션 종료를 의미하지 않는다. 보안 사고 시 필요한 각 대상의 회수와 권한 제거를 별도로 처리한다. 공급자의 접근 토큰 회수 지원, CORS 및 결과 전파는 공급자마다 다르다.

Angular 테스트 156개와 빌드가 통과했다. 실제 브라우저, 로컬 테스트 OIDC 공급자, Rust API의 회수 검사와 임시 PostgreSQL에서 공급자와 Swallow 양쪽에 회수를 요청해 기존 JWT의 API 응답이 200에서 401로 바뀌는 것을 확인했다. 별도 시험에서 공급자만 회수하면 같은 JWT가 Swallow에서 계속 200이었던 것도 확인했다. 이 결과는 로컬 테스트 구성에 대한 것이며 실제 외부 IdP의 회수·인증 정책이나 운영 HA를 검증하지 않았다.

자동 갱신으로 로그인한 경우 접근 토큰 회수 화면에서 **공급자에 현재 갱신 토큰도 회수 요청**을 추가로 선택할 수 있다. 선택한 경우 공급자 갱신 토큰, Swallow 접근 토큰, 공급자 접근 토큰 순서로 요청하며, 선택하지 않은 항목은 건너뛴다. 요청 하나가 실패해도 다음 요청을 시도하고 결과를 각각 표시한다. HTTP 200은 공급자의 요청 수락이며 토큰이 유효했는지 또는 다른 토큰과 세션이 회수됐는지 증명하지 않는다. 세 요청은 원자적이지 않으며 중간 만료·네트워크 실패나 응답 유실 시 부분 성공 또는 불명 상태가 남을 수 있다. 화면은 마지막에 로그아웃하므로 공급자 및 Swallow 관리자 도구에서 불명 결과를 확인해야 한다.

Angular 테스트 158개와 빌드가 통과했다. 실제 브라우저·로컬 테스트 OIDC 공급자·Rust API·임시 PostgreSQL에서 세 항목을 함께 요청해 각각 수락 또는 회수 기록을 확인하고 테스트 갱신 토큰 재사용이 거절되는 것을 확인했다. 실제 외부 IdP, 운영 HA 및 모든 실패 조합은 미검증이다.

The JWT operations screen can query the optional Slack shared cooldown snapshot. Provision the separate API 0098 identity grant, API database `alert_view` and function EXECUTE, and a worker cooldown policy; see the Rust API README. The threshold affects this query only. A failed lookup clears the previous result. The snapshot does not show whether every worker has enabled the guard. The local preview fixture flag `SWALLOW_UI_SLACK_COOLDOWN_HEALTH_FIXTURE=true` seeds a policy and deferred jobs for browser testing; it does not contact Slack.

The Twilio result view shows 429 retry guidance when API 0099 and worker 0178 provide it. Older API responses without these optional fields remain supported. The values do not schedule a retry; verify current evidence before choosing an existing retry operation.

The Twilio result view supports a reviewed 429 delayed retry when the current dispatch ID and numeric guidance are available. It prepares a request ID, requires explicit confirmation, allows lookup and same-request replay after an uncertain response, and does not send automatically. Configure the separately scoped JWT and DB permissions in the Rust API README; apply worker 0178–0181 and API 0100–0101 first. The request ID should be saved independently because leaving the view can discard in-memory state. Provider evidence can arrive after the dispatch fence and duplicate delivery remains possible. The local test flag `SWALLOW_UI_TWILIO_RETRY_FIXTURE=true` seeds a synthetic job and grants in a disposable DB; it does not send SMS.

The operations view displays Twilio delayed-retry aggregate health using the separate API 0102 JWT grant and environment DB monitoring permission. Its grace applies to this query only, not alert settings. Failed requests clear the previous counts. `SWALLOW_UI_TWILIO_DELAYED_HEALTH_FIXTURE=true` seeds synthetic delayed jobs for the disposable preview fixture.


### Discord failure review

The operator JWT section of the alerts page includes Discord attention lists and revision-bound review. It uses the existing environment and operator token and requires the separate API and database permissions documented in `../../api/rust-api/README.md` under operator JWT Discord review. It shows the request ID before submission, supports lookup or same-request replay after an uncertain response, and does not claim that review proves delivery or retries a message. The token and pending request are kept in page memory only. Leaving or reloading the page loses the snapshot; record the request ID and investigate an uncertain request before starting another. The review panel does not manage Discord endpoint bindings; the settings grant panel can delegate its list and review permissions to registered operator identities.

Angular tests cover selection, review, ambiguous responses, identity changes, malformed pages and pre-dispatch failures. A local browser test used simulated API responses to exercise the visible list and confirmation; it did not connect to the Rust API or a real identity provider. Production load, original Novu compatibility and real Discord remain unverified.


### Discord review grant administration

The settings permission panel supports the Discord review list and review grants, request lookup and grant audit. Administrator read/write and audit-view privileges must be provisioned independently, along with the worker-side permissions described in the API README. A local browser integration can be reproduced with `scripts/test-discord-grants-browser.sh`; it builds the API and UI, starts a disposable Docker PostgreSQL, uses restricted API database credentials and test-signed JWTs, grants both permissions, records a review and revokes list access. It removes its test container and processes afterwards. It does not contact Discord or an external identity provider, and does not validate production HA.

# Discord 워크플로 정책

설정 화면에서 환경별 Discord 발송 정책을 조회하고 활성·비활성을 선택할 수 있다. 변경은 조회한 revision과 새 요청 ID를 고정해 준비한 뒤 별도 확인을 받아 제출한다. 결과가 불명이면 요청 ID로 조회하거나 같은 토큰·환경·API 주소로 동일 요청을 재제출할 수 있다. 요청과 감사 조회는 각기 권한이 필요하며, JWT와 API DB 로그인에 모두 해당 권한이 있어야 한다. 설정 화면에서 정책 조회·변경·감사 JWT 권한을 독립적으로 위임할 수 있으며 위임 관리자 및 DB 로그인 권한은 DBA가 사전에 설정한다. API와 워커 배포 순서는 [API README](../../api/rust-api/README.md)와 [워커 README](../../worker/rust-queue/README.md)를 따른다.

요청 정보와 토큰은 화면 메모리에만 보관하므로 새로고침 전에 요청 ID와 내용을 기록한다. 감사 목록은 JWT 경로만 보여 주고 단일 스냅샷이나 전체 변경 이력을 보장하지 않는다. Angular 테스트 242개와 빌드가 통과했다. `scripts/test-discord-policy-browser.sh`는 격리 Docker PostgreSQL, 제한 API 로그인, 테스트 JWT 및 실제 브라우저에서 제출 전 미변경, 정책 변경, 요청·감사 조회 및 다른 사용자·환경의 거절을 검증한다. 외부 IdP, 실제 Discord 및 운영 HA는 검증하지 않았다. 빌드에 기존 번들·CSS 예산 경고가 남아 있다.

권한 위임 화면을 포함한 Angular 테스트 243개와 빌드가 통과했다. `scripts/test-discord-policy-grants-browser.sh`는 격리 PostgreSQL, 제한 API 로그인, 테스트 JWT 및 실제 브라우저에서 세 정책 권한 부여와 위임 감사 조회, 대상의 정책 조회·감사 접근, 조회 권한 회수와 다른 권한 유지를 확인한다. 외부 IdP와 운영 HA는 검증하지 않았다.

# Discord 수신자 연결

채널 화면에서 환경 API 키로 Discord 연결을 목록 조회하고 구독자와 미리 등록된 대상 ID를 연결·변경·삭제한다. 읽기 권한으로 등록된 대상 ID 목록을 조회해 선택하거나 직접 입력할 수 있다. 목록에는 URL이나 해시가 없으며 대상의 현재 워커 가동·연결 가능성을 보장하지 않는다. 변경과 삭제는 현재 연결을 먼저 조회하고 revision을 보내며 다른 변경이 있으면 거절된다. 선택적 컨텍스트는 최대 5개의 종류·ID 문자열을 가진 JSON 객체다. 빈 값은 컨텍스트를 지우며 변경 시 기존 구독자는 바꿀 수 없다. `.`와 `..` 연결 ID는 브라우저 URL 경로 정규화 때문에 이 화면에서 변경할 수 없다. API와 권한·대상 등록은 [API README](../../api/rust-api/README.md)의 Discord 바인딩 설명을 따른다. 웹훅 URL 설정 및 Discord 원본 API 호환 화면은 제공하지 않는다.

응답이 불명확하면 자동 재전송하지 않는다. 현재 목록과 서버 감사 기록을 확인한 뒤 화면의 확인 절차로 요청 상태를 지워야 한다. API 0122의 별도 감사 권한이 있으면 화면에서 이력을 조회할 수 있다. 이력은 UUID 순서이고 단일 스냅샷이 아니며 거절된 요청·직접 DB 변경을 기록하지 않으므로 결과를 확정하는 증거가 아닐 수 있다. 화면 이탈이나 새로고침 시 요청 상태와 키는 메모리에서 사라진다. Angular 테스트 249개와 빌드가 통과했다. `scripts/test-discord-endpoints-browser.sh`는 격리 Docker PostgreSQL, 제한 API 로그인, 시험용 환경 API 키 및 실제 브라우저로 대상 목록 선택, 등록·조회·변경·삭제와 감사 이력 표시를 검증한다. 실제 Discord 발송, 원본 Novu 서버 및 운영 HA는 검증하지 않았다. 빌드에는 번들·CSS 크기 경고가 남아 있다.

# Discord 재시도 대기 조사

운영 경보 화면에서 JWT 인증을 선택하면 Discord 429 재시도 대기 집계와 전체 또는 지연 작업 목록을 별도로 조회할 수 있다. 유예 시간은 1–86400초이며 집계·목록은 독립 JWT 권한과 API DB 로그인 `alert_view` 권한이 필요하다. 설정 화면에서 권한 위임·회수와 요청·감사 조회를 지원하며 최초 위임 관리자와 DB 로그인 권한은 DBA가 설정한다. 세부 권한과 API는 [API README](../../api/rust-api/README.md), 경보 구성은 [워커 README](../../worker/rust-queue/README.md)를 따른다. 목록과 집계는 별도 시점이고 UUID 순서이며 실제 발송 여부나 워커 중단의 증거가 아니다.

Angular 테스트 253개와 빌드가 통과했다. `scripts/test-discord-retry-diagnostics-browser.sh`는 격리 Docker PostgreSQL, 제한 API 로그인과 테스트 JWT를 사용해 실제 브라우저에서 집계, 전체·지연 필터, payload 비노출과 권한 없는 사용자의 거절을 확인한다. 실제 외부 IdP, Discord 발송 및 운영 HA는 검증하지 않았다. 기존 번들·CSS 예산 경고가 남아 있다.

권한 위임을 추가한 Angular 테스트 254개와 빌드가 통과했다. 제한 DB 로그인·테스트 JWT·Docker PostgreSQL API 테스트에서 두 권한의 독립 부여·회수와 감사 실패 롤백을 확인했다. `scripts/test-discord-retry-grants-browser.sh`의 기본 모드는 Docker PostgreSQL이며, 이번 브라우저–API 시험은 Docker Desktop 시작 불가로 `SWALLOW_TEST_POSTGRES_MODE=local`을 지정해 임시 로컬 PostgreSQL 17에서 실행했다. 두 권한 부여, 위임 감사, 대상의 진단 조회, 집계 권한 회수와 목록 권한 유지를 확인했다. 로컬 모드는 `initdb`, `pg_ctl`, `psql`이 필요하다. 외부 IdP와 운영 HA는 미검증이다.


### Discord 429 재시도 정책

설정 화면에서 대상 ID별 `off`, `known`, `fallback` 모드와 최대 횟수, 대체 대기 시간을 선택하고 조회·변경·요청 확인·JWT 변경 감사를 할 수 있다. API 0125와 워커 0211, 대상별 JWT 및 API DB 로그인 권한이 각각 필요하다. 변경 전 대상 워커를 중지하고 진행 중인 발송을 정리한 다음 변경하고 동일 설정으로 재시작한다. API는 워커 롤아웃을 조정하거나 이미 진행 중인 발송을 취소하지 않는다. `fallback`은 제공자의 준비 전에 재시도할 수 있다.

변경은 먼저 요청을 준비하고 별도 확인 후 제출한다. 응답 불명 시 원래 토큰·환경·대상·주소로 동일 요청 ID를 재제출하거나 기록을 확인한다. 요청 정보는 메모리에만 보관된다. 변경 없는 요청도 감사되며 revision은 그대로다. 감사는 UUID 순서이고 단일 스냅샷이 아니다. Angular 테스트 258건과 빌드가 통과했다. `scripts/test-discord-retry-policy-browser.sh`는 실제 브라우저·Rust API·제한 DB 로그인·테스트 JWT에서 정책 변경, 요청·감사 조회, 변경 없는 요청, 대상·사용자·환경 격리를 검증한다. 기본은 Docker PostgreSQL이며 `TEST_POSTGRES_MODE=local`은 테스트 전용 임시 로컬 PostgreSQL을 사용한다. Docker Desktop 시작 불가로 로컬 PostgreSQL 17에서 검증했다. 실제 Discord·외부 IdP·운영 HA는 미검증이다. 번들·CSS 크기 경고가 남아 있다. 자세한 권한과 배포 절차는 [API README](../../api/rust-api/README.md)와 [워커 README](../../worker/rust-queue/README.md)를 참고한다.


### Discord 재시도 정책 권한 위임

설정 화면에서 환경·대상·운영자별 정책 조회, 변경, 감사 JWT 권한을 조회하고 위임·회수할 수 있다. API 0126의 위임 관리자 권한과 API 로그인 함수 실행 권한은 DBA가 별도로 설정해야 한다. 위임된 JWT 권한은 정책 접근에 필요한 API DB 로그인 권한을 자동 부여하지 않는다. 자기 자신의 권한 변경은 거절된다. 요청은 확인 후 제출하며 응답이 불명확한 경우 원래 토큰·환경·대상·주소로 동일 요청을 재제출하거나 요청 ID를 조회한다. 요청 정보는 화면 메모리에만 보관되며 감사는 실제 변경만 표시하고 UUID 순서로 조회한다.

Angular 테스트 262건과 빌드가 통과했다. `scripts/test-discord-retry-policy-grants-browser.sh`는 실제 브라우저, Rust API, 제한 DB 로그인과 테스트 JWT를 사용해 조회·부여·요청·감사·회수와 대상의 정책 접근 변경을 검증한다. 기본은 Docker PostgreSQL이며 `TEST_POSTGRES_MODE=local`은 임시 로컬 PostgreSQL을 사용한다. Docker Desktop 시작 불가로 로컬 PostgreSQL 17에서 검증했다. 외부 IdP·운영 HA는 미검증이며 빌드 크기 경고가 남아 있다. [API README](../../api/rust-api/README.md)에 권한과 함수 설정 절차가 있다.

### Experimental SMTP selection

The workflow email-provider screen supports SMTP with the existing API-key and operator JWT authorization flows. Configure and start the SMTP worker separately and drain old generators and queued email jobs before switching; see the worker README. Selection alone does not verify delivery or provide SMTP outcome reconciliation. The SMTP browser fixture is `scripts/test-smtp-provider-browser.sh` (Docker PostgreSQL by default; `TEST_POSTGRES_MODE=local` uses a temporary local PostgreSQL installation). It exercises provider selection and audit through the API, not external email delivery. The browser test passed with a temporary local PostgreSQL instance; the Docker mode was not exercised in this run.

### SMTP failure review

In the alerts screen, use an operator JWT and environment to list and acknowledge SMTP failures. List and review permissions are independent and can be delegated in Settings by an authorized grant administrator; initial administrator and DB login privileges still require DBA provisioning. See the API README. A review does not determine delivery or retry a message. The interface retains the request ID for lookup or exact replay after an uncertain response while the page remains open. `scripts/test-smtp-review-browser.sh` exercises the built UI, real API, synthetic JWT and restricted DB login (Docker PostgreSQL by default, or `TEST_POSTGRES_MODE=local`). The browser test passed with a temporary local PostgreSQL; external IdP and SMTP relay are not covered.

The SMTP permission delegation browser fixture is `scripts/test-smtp-grants-browser.sh` and uses Docker by default or an isolated local PostgreSQL with `TEST_POSTGRES_MODE=local`. The browser test passed with the local mode; Docker was not exercised in this run.

### SMTP worker database scope management

In Settings, an operator with independently provisioned JWT and API DB permissions can inspect a worker DB login's SMTP environment scope, prepare an enable/disable request with a reason, look up an uncertain request and view JWT-path audit records. The interface does not install RLS or provision PostgreSQL roles and permissions; see the API and worker READMEs for prerequisites and limitations. `scripts/test-smtp-scope-browser.sh` exercises enable, request lookup, audit and disable using the real API, synthetic JWT and restricted API login; it passed with temporary local PostgreSQL (`TEST_POSTGRES_MODE=local`). Docker mode, external IdP and production HA were not tested in that run.

The Settings permission delegation form can grant and revoke SMTP scope read, write and audit rights after a DBA provisions the initial independent JWT grant administrator and DB privileges. `scripts/test-smtp-scope-grants-browser.sh` exercises the delegated flow against the real API with synthetic JWTs and disposable local PostgreSQL (`TEST_POSTGRES_MODE=local`). The API DB login's underlying database privileges remain separate.

SMTP attention rows with an unknown attempt support independent relay-acceptance and retry decisions as well as review acknowledgment. The operator supplies an evidence-based reason, confirms the selected action, and can look up the request by ID and decision or resubmit the same request after an uncertain response using the original token, environment and API origin. Acceptance does not mean mailbox delivery; retry can duplicate a message when the external evidence is wrong. After worker 0224 and API 0138, pre-dispatch failures can also be explicitly retried, but cannot be marked accepted. Decision permissions must be provisioned separately from list/review permissions as described in the API README; the dashboard can delegate those JWT grants and display separately authorized DB-level resolution history as described below.

`TEST_POSTGRES_MODE=local scripts/test-smtp-resolution-browser.sh` uses disposable local PostgreSQL, a restricted API DB login, signed test JWT, real Rust API and browser. It checks an acceptance, request lookup and a retry selection, then verifies the job states and two audit records. The first run required correcting an unsupported browser test selector. No actual SMTP worker, relay, external identity provider or production HA is involved in this browser test.

SMTP decision JWT permissions can be granted or revoked independently in Settings, with separate grant request and delegation audit views. Initial delegation administrators, audit viewers and DB login permissions still require DBA provisioning. `TEST_POSTGRES_MODE=local scripts/test-smtp-resolution-grants-browser.sh` runs a restricted API login, signed fixture JWTs, real API and browser against disposable PostgreSQL: it grants both decisions, records an acceptance as the target operator, inspects delegation audit, revokes acceptance and checks that retry remains granted and acceptance lookup is denied. It does not test a real IdP, external relay or production HA.

The SMTP panel displays resolution history for a job UUID and pages through older entries. Separate JWT viewer and DB viewer grants are required. The displayed DB actor and DB request ID remain visible. After API migration 0135, newly recorded JWT decisions also display the JWT issuer, subject, client and original request ID; older records and CLI decisions can have no JWT attribution. `TEST_POSTGRES_MODE=local scripts/test-smtp-resolution-history-browser.sh` validates a decision and its history using the browser, Rust API, fixture JWT and restricted login against disposable PostgreSQL. Angular tests cover pagination, large revisions and stale responses. Real IdP, external delivery and production HA are not covered.

SMTP history viewing can be delegated with `smtp-resolution:history` in Settings after API migration 0136. Existing SMTP decision grant administrators gain this additional authority; review their privileges before migration. The API DB history viewer permission remains separate. The browser test `TEST_POSTGRES_MODE=local scripts/test-smtp-history-grants-browser.sh` verifies granting, viewing and revoking access.

The SMTP panel can find a current failed job by UUID, including an already acknowledged failure, and then prepare a decision using its current revisions. The existing list-view permissions apply; finding a job neither changes its status nor reopens alerts. `TEST_POSTGRES_MODE=local scripts/test-smtp-failure-lookup-browser.sh` validates acknowledgment, lookup and acceptance with the real dashboard and API against disposable PostgreSQL and a fixture JWT. It does not test a real relay or production HA.

`TEST_POSTGRES_MODE=local scripts/test-smtp-predispatch-browser.sh` checks an operator retry of a failed SMTP job with no current attempt, including request lookup. Review the existing retry grants and prior delivery evidence as documented in the worker README before deploying this behavior.

Discord attention now allows authorized operators to prepare an acceptance or retry decision and recover an uncertain response by request ID. For 429 it displays available seconds and a calculated UTC guidance time; this is advisory. Operators can select an immediate retry or, after worker 0226 and API 0140, a retry no earlier than the recorded guidance. Check external delivery evidence and rate limits before choosing retry. Separate JWT and DB decision grants are required. After API 0141 the dashboard can delegate JWT decision grants; initial administrators and DB grants require trusted DBA provisioning. Decision history is available after worker 0227 and API 0142 with independent JWT and DB viewer grants. `TEST_POSTGRES_MODE=local scripts/test-discord-resolution-browser.sh` uses fixture JWTs, restricted API DB login, disposable PostgreSQL and the real browser/API to verify acceptance, retry and 429 display; it does not send to Discord.

`TEST_POSTGRES_MODE=local scripts/test-discord-delay-browser.sh` verifies guided retry selection and the resulting future queue timestamp through the real dashboard and API, using fixture JWTs and disposable PostgreSQL. It does not contact Discord.

`TEST_POSTGRES_MODE=local scripts/test-discord-resolution-grants-browser.sh` exercises JWT decision grant, target operator decision, delegation audit and revocation through the real dashboard and API using fixture JWTs and disposable PostgreSQL. It does not contact Discord or an external IdP.

`TEST_POSTGRES_MODE=local scripts/test-discord-history-browser.sh` verifies a Discord decision, 429 schedule and JWT-attributed history through the real dashboard/API, fixture JWT and disposable PostgreSQL. Earlier or CLI decisions can lack JWT attribution. History viewer JWT grants can be delegated after API 0144; initial administrators and DB viewer grants remain DBA-managed. The test does not contact Discord or a real IdP.

Discord failed jobs can be looked up by UUID even after acknowledgment, using the existing list permissions. The lookup shows the current snapshot and 429 guidance without changing the job or alert. `TEST_POSTGRES_MODE=local scripts/test-discord-failure-lookup-browser.sh` verifies acknowledgment, lookup and guided retry with fixture JWT and disposable PostgreSQL; it does not contact Discord.

Discord history viewing can be delegated with `discord-resolution:history` in Settings after API 0144. Existing Discord decision grant administrators gain this authority; review their privileges before migration. The API DB history viewer permission remains separate. The browser fixture `TEST_POSTGRES_MODE=local scripts/test-discord-history-grants-browser.sh` verifies granting, viewing and revoking access.

With worker migration 0229, Discord retry diagnostics include operator-scheduled and automatic 429 retries; an immediate retry can have zero delay. `TEST_POSTGRES_MODE=local scripts/test-discord-monitor-browser.sh` verifies the operator retry appears in the count and list through the real dashboard and API, using fixture JWT and disposable PostgreSQL.

Discord shared cooldown can be viewed and changed in Settings after worker migrations 0230–0231 and API migration 0145. An authorized operator enters a destination, reads its current state and revision, prepares a change with a reason, and submits the reviewed request. An uncertain response retains the request ID for lookup or an identical retry with the same credentials. The panel also shows JWT-attributed audit records. JWT and API database permissions must be provisioned by a trusted DBA; grant delegation for this setting is not implemented. Stop and drain affected workers before changing the setting. Disabling it does not undo already scheduled queue delays, and the panel does not coordinate a multi-host rollout. See the worker and API READMEs for the complete scope and limitations.

`npm test -- --include='src/app/discord-cooldown.spec.ts' --watch=false` exercises mocked API responses for read, change, no-op, request lookup, audit, uncertain response recovery, credential binding and navigation blocking. The production build succeeded with existing bundle-size warnings. A separate Chromium UI check used a mocked HTTP API. `PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH" TEST_POSTGRES_MODE=local scripts/test-discord-cooldown-browser.sh` starts a disposable local PostgreSQL 17, the Rust API and built dashboard, and uses ego-browser with signed fixture JWTs and a restricted API DB login. It verifies read, enable, no-op, disable, request lookup, JWT audit, and user/environment/destination access denial; SQL checks verify the stored revision, disabled state, and all three audit records. It does not exercise an external IdP, Discord, production HA or multiple hosts; Docker was not used for this run.

Discord shared cooldown health is also shown in the operational alerts view after worker migration 0232 and API migration 0146. It requires separately provisioned JWT health and DB `alert_view` permissions. The displayed aggregates are a point-in-time view, can overlap retry counts, and omit jobs not yet marked by a compatible worker; see the worker README. `npm test -- --include='src/app/discord-cooldown-health.spec.ts' --watch=false` covers response validation and stale requests. `PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH" TEST_POSTGRES_MODE=local scripts/test-discord-cooldown-browser.sh` also checks health counts and denial through the built browser, API and local PostgreSQL with fixture JWTs; it does not use Docker or test real IdP, Discord or production HA.

After API migration 0147, the Settings permission selector includes `Discord 공유 대기 집계` (`discord-retry:cooldown-health`). Existing Discord retry grant administrators gain authority over this permission; review their privileges before rollout. The API database login still needs its independent `alert_view` grant. `PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH" SWALLOW_TEST_POSTGRES_MODE=local scripts/test-discord-cooldown-grants-browser.sh` uses fixture JWTs, a restricted API login, local disposable PostgreSQL, Rust API and the actual dashboard to check grant, audit, target access, revocation and continued separate retry-list access. It does not test a real IdP, Discord or production HA. Docker Desktop could not start for this run.

SMTP password reload health is displayed in the operational alerts view after worker migration 0233 and API migration 0148. An authorized operator selects observation age and minimum healthy workers. The panel reports file-read validation observations and does not prove SMTP authentication or delivery. The JWT and API DB `alert_view` permissions are separate. After API 0149, existing SMTP review grant administrators can delegate the JWT permission; DB permissions remain DBA-provisioned. `npm test -- --include='src/app/smtp-credential-health.spec.ts' --watch=false` checks response/input validation and stale-response handling. `PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH" TEST_POSTGRES_MODE=local scripts/test-discord-cooldown-browser.sh` checks the display and permission denial with fixture-signed JWTs, real API and local PostgreSQL. The browser fixture inserts a synthetic unhealthy observation; it does not run an SMTP worker, contact a real relay or IdP, or test production HA.

After API 0149, Settings includes `SMTP 암호 갱신 상태` (`smtp-review:credential-health`). Existing SMTP review grant administrators gain authority over this permission; review their privileges before rollout. `PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH" TEST_POSTGRES_MODE=local scripts/test-smtp-credential-grants-browser.sh` uses fixture JWTs, a restricted API login, local PostgreSQL, Rust API and the actual dashboard to check grant, audit, target access, revocation and independent SMTP review-list access. It does not test a real IdP, relay or production HA.

SMTP CA reload status is available in the SMTP review panel beside password reload status. It uses the independently authorized `smtp-ca-health` endpoint, with separate age and minimum-healthy controls. A DBA provisions the initial grant administrators and DB permissions; after API migration 0151 the permission panel supports CA health grant and revocation. The status covers file validation and does not prove relay trust or delivery. `TEST_POSTGRES_MODE=local scripts/test-smtp-ca-health-browser.sh` verifies CA access and password-health denial through the built app, real local API, restricted DB login and a synthetic JWT. The 301 Angular unit tests and production build also passed; the build reports initial bundle and CSS budget warnings. A real IdP and production HA were not tested.

The CA health permission can be delegated in the SMTP review permission panel after API migration 0151. Existing SMTP review grant administrators gain this additional scope, so review their permissions before rollout. `TEST_POSTGRES_MODE=local scripts/test-smtp-ca-grants-browser.sh` exercises grant, audit, CA access and revocation against the real API and disposable PostgreSQL using synthetic JWTs. Actual external IdP and multi-host HA remain unverified.

설정 화면의 `경보 대상별 시간 설정`에서 Slack·이메일 대상 ID를 선택하고 PostgreSQL에 저장된 Resend 유예 시간, 집계기 경과 시간, 쿨다운을 조회·변경하고 요청과 JWT 변경 이력을 확인할 수 있다. JWT의 대상별 읽기·쓰기·감사 권한과 API DB 로그인 권한이 각각 필요하며 초기 권한은 DBA가 부여한다. 변경 요청은 revision과 UUID를 고정해 준비한 뒤 별도 확인하여 제출한다. 응답이 불명이면 원래 토큰·환경·채널·대상·API 주소로 동일 요청을 재제출하거나 요청 ID로 조회한다. 요청은 메모리에만 보관되므로 화면 이탈이나 새로고침 전에 내용을 기록해야 한다. 빈 입력은 파일 값이 아니라 팬아웃 명령행 값을 상속한다. 실제 적용에는 각 팬아웃의 `SWALLOW_ALERT_TIMING_SOURCE=postgres`와 별도 DB 읽기 권한이 필요하며, 이미 시작된 실행이나 파일 모드 발송기를 바꾸지 않는다. 대상 주소·시크릿·전체 감시 조건, 권한 위임과 설정 삭제는 이 화면에서 관리하지 않는다. 상세 배포 조건은 [API README](../../api/rust-api/README.md)와 [워커 README](../../worker/rust-queue/README.md)를 참고한다.

Angular 테스트 305건과 빌드가 통과했으며 기존 번들·CSS 크기 예산 경고가 있다. `PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH" TEST_POSTGRES_MODE=local scripts/test-alert-timing-browser.sh`는 실제 브라우저, 제한 API DB 로그인, 테스트 JWT와 격리 로컬 PostgreSQL에서 조회·요청 준비 전 DB 불변·변경 제출·요청 조회·감사·다른 운영자 거절을 확인한다. 실제 IdP·외부 경보 발송·운영 HA는 미검증이다. Docker는 사용하지 않았다.
