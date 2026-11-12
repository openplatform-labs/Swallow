# Swallow

Swallow는 Novu의 알림 기능을 Rust와 PostgreSQL로 재구현하는 실험 프로젝트입니다. 비교 기준은 Novu 커밋 `895388f37efe9e06de0aa333e5727303e6ad6853`입니다.

**운영용이 아니며 Novu와 완전히 호환되지 않습니다.** 원본과의 차이, 미구현 기능과 검증하지 않은 영역은 [구현·검증 현황](RUST_IMPLEMENTATION_STATUS_KO.md)을 확인하세요. MongoDB 데이터 이전과 운영 배포도 준비되지 않았습니다.

## 구성

| 경로 | 역할 |
|---|---|
| [Rust API](apps/api/rust-api/README.md) | 제한적인 트리거·구독자·Inbox API와 웹훅 |
| [Rust 워커 및 PostgreSQL 큐](apps/worker/rust-queue/README.md) | 실험적인 워크플로 처리와 인앱·SendGrid·Twilio·FCM 발송 |
| [Rust WS](apps/ws/rust-ws/README.md) | 제한적인 Socket.IO 연결과 알림 이벤트 |
| [공통 계약](apps/api/rust-contracts) | API·워커·WS에서 공유하는 제한적인 계약 |

루트의 TypeScript 앱·라이브러리·SDK와 기존 Novu 문서는 기준 소스 및 비교 테스트에 사용됩니다. 해당 문서는 원본 Novu의 사용법을 설명하며 Swallow의 구현 범위를 의미하지 않습니다.

## 문서

- [구현·검증 현황](RUST_IMPLEMENTATION_STATUS_KO.md): 현재 기능, 테스트 근거, 운영상 제약과 미구현 사항의 기준 문서.
- [서비스·SDK 계약](NOVU_SERVICE_CONTRACTS_KO.md): 고정된 원본 버전의 서비스 경계와 비교해야 할 계약.
- [Rust·PostgreSQL 설계 기록](RUST_POSTGRES_DESIGN_KO.md): MongoDB·Redis에서 전환하려는 배경과 초기 설계. 과거 제안이므로 현재 구현 현황과 구분합니다.
- 각 Rust 서비스 README: 설정, 마이그레이션, 테스트 명령과 개별 제한.

## 개발 및 검증

API의 Docker PostgreSQL 통합 테스트는 다음과 같습니다. 일회용 컨테이너를 만들고 종료 시 삭제합니다.

```sh
cd apps/api/rust-api
./scripts/test-postgres.sh
```

원본 서버와의 차등 테스트에는 별도로 빌드한 기준 커밋 및 Docker 환경이 필요합니다. 실행 방법과 데이터 삭제 범위는 [API README](apps/api/rust-api/README.md)의 Verification 항목을 확인하세요. 선택된 테스트의 통과는 전체 호환성을 증명하지 않습니다. API와 워커를 실행하기 전에 필요한 실험용 마이그레이션을 순서대로 적용해야 하며, 기본 DB 연결은 인증서와 호스트명을 검증하는 TLS를 요구합니다.

## 출처와 라이선스

이 저장소에는 [Novu](https://github.com/novuhq/novu)의 소스가 포함되어 있습니다. 각 파일 및 패키지에 적용되는 기존 저작권과 라이선스를 유지합니다. 저장소의 [MIT 라이선스](LICENSE-MIT), [Enterprise 라이선스](LICENSE-ENTERPRISE), [Enterprise 패키지 라이선스](EE-PACKAGES-LICENSE)와 각 패키지의 라이선스 정보를 확인하세요.
