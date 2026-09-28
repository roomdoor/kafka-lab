# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Kafka 실무 문제(파티션 순서, 오프셋 커밋, 재시도/DLT, 아웃박스, 멱등성)를 재현·검증하는 학습용 프로젝트(Kotlin / Spring Boot 4 / Kafka KRaft / PostgreSQL / Ktor mock). 문서·주석은 한국어.
전체 흐름·실행 방법·실습 시나리오·mock-pg 설정 API 는 `README.md` 에 있다.

## 명령

```sh
docker compose up -d --build        # Kafka + kafka-ui + PostgreSQL + mock-pg
./gradlew bootRun
./gradlew test                      # Testcontainers. Docker 만 켜져 있으면 됨
./gradlew test --tests 'com.roomdoor.kafkalab.PaymentIdempotencyTest'
./gradlew :mock-pg:run              # mock 직접 실행 (먼저 docker compose stop mock-pg)
```

## 구조에서 놓치기 쉬운 것

- 업무 이벤트는 같은 `@Transactional` 에서 `outbox` 행을 쓰고 `OutboxRelay` 가 발행한다. 컨슈머 결과(`PaymentService`)도 아웃박스를 거친다. 예외는 DLT 로, `DeadLetterPublishingRecoverer` 가 `KafkaTemplate` 으로 직접 보낸다.
- 에러 핸들러(`KafkaConsumerConfig`)는 `DeadLetterConsumer` 를 뺀 모든 리스너 공통이다. 백오프 재시도 후 `<topic>.DLT`, `IllegalArgumentException` 등 비재시도 예외는 바로 DLT.
  `DeadLetterConsumer` 는 전용 팩토리(`deadLetterListenerContainerFactory`)라 2차 DLT 가 없다. 일시적 DB 장애(예외 체인의 `TransientDataAccessException`·`SQLTransientException`·`SQLRecoverableException` 등 타입, 또는 가장 안쪽 SQLState 08/57P0/53/40001/40P01/25006 로 판별)만 최대 10분 재시도하고, 영구 DB 오류를 포함한 나머지는 재시도 없이. 어느 쪽이든 끝나면 ERROR 로그 후 건너뜀.
  무한 재시도는 두지 않는다. 재시도 정책이 첫 실패 때 정해져, 같은 예외 클래스로 영구 오류가 이어지면 DLT 가 멈춘다.
- 결제 결과 분류는 `PaymentGatewayClient` 가 정한다. 2xx + `transactionId` 만 승인, 402 는 예외 없이 거절, 나머지 응답과 통신 실패는 예외 → 재시도 → DLT. 예외는 `outcomeUnknown` 으로 "결제 여부 모름"(타임아웃 등)과 "확실히 안 됨"(커넥션 거부뿐, 5xx 도 모름)을 가른다.
- 결제 중복 방어는 PG 호출 전 `payments` 에 `PENDING` 을 넣고 `schema.sql` 의 부분 유니크 인덱스(`order_id`, PENDING/COMPLETED)로 승자를 정하는 방식이다. 예약 행의 `eventId`(= PG 멱등키)가 같으면 재배달이 자기 예약을 이어받고, 결과를 모르는 PG 실패에서는 예약을 풀지 않는다. 바꾸기 전에 `PaymentConsumer` 전체를 먼저 본다.
- JPA 경유 유니크 위반은 `DuplicateKeyException` 이 아니라 `DataIntegrityViolationException`(원인 Hibernate `ConstraintViolationException`)으로 온다. 제약 이름은 로케일에 따라 null 이 되므로 SQLState `23505` 로 가른다.
- `mock-pg` 는 별도 Gradle 모듈이고 테스트에서만 `testImplementation(project(":mock-pg"))` 로 쓴다. 테스트는 `IntegrationTestBase` 가 같은 JVM 에 띄우고 매번 `MockPgConfig()` 로 되돌린다. `POST /_config` 는 설정 전체를 교체하므로 네 필드를 모두 보낸다.

## 함정

- 스키마는 `ddl-auto: update` + `schema.sql`(테스트에서도 실행). `@Enumerated(STRING)` 컬럼의 CHECK 제약은 `update` 가 갱신하지 않는다.
  `PaymentStatus`·`OrderStatus`·`FailedEventStatus` 는 `schema.sql` 이 값 목록을 고정해 다시 세우므로 enum 에 값을 추가하면 `schema.sql` 목록도 고친다(빠뜨리면 `SchemaEnumCheckTest` 가 잡는다). `failed_events` 유니크 제약도 `schema.sql` 이 다시 세운다.
- compose 브로커는 `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` 라 없는 토픽은 발행이 실패한다. Testcontainers 브로커는 자동 생성이라 테스트로는 못 잡는다.
- Ktor 와 Spring Boot BOM 의 코루틴 버전 충돌 때문에 `build.gradle.kts` 가 버전을 올려둔다(주석 참고). 의존성을 올릴 때 확인한다.
- Kotlin 들여쓰기는 탭. 주석은 "무엇"이 아니라 "왜"를 한국어로 적는다.
