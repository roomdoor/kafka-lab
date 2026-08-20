# kafka-lab

Kafka를 실무에서 쓸 때 실제로 마주치는 문제들을 하나씩 재현하고 검증하는 학습용 프로젝트.

주문 → 결제 → 알림 흐름을 만들고, 그 위에서 파티션·컨슈머 그룹·오프셋 커밋·재시도·DLT·
트랜잭셔널 아웃박스·멱등성을 확인한다. 결제는 **진짜 외부 HTTP 호출**이고, 그 상대인 mock 게이트웨이는
별도 컨테이너로 떠 있어서 죽여볼 수 있다.

**Kotlin 2.2 / Spring Boot 4.0 / Kafka 4.3 (KRaft) / PostgreSQL 17 / Ktor 3.5 / Testcontainers**

최근 작업 내역과 남은 일은 [HANDOFF.md](HANDOFF.md)에 있다.

---

## 전체 흐름

```
POST /api/orders
      │
      ▼
 OrderService  ┌ @Transactional ─────────────────────────┐
      │        │ orders INSERT (status=CREATED)          │
      │        │ outbox INSERT → order.events            │
      │        │ outbox INSERT → notification.requested  │
      │        └─────────────────────────────────────────┘
      ▼
 OutboxRelay (500ms 폴링)
      │
      ├────────────────────────────────┐
      ▼                                ▼
┌────────────────────────┐   ┌──────────────────────────┐
│ order.events (파티션 3) │   │ notification.requested   │
│ key = orderId          │   │ key = customerId (파티션 3)│
│  OrderCreated          │   └──────────────────────────┘
│  PaymentCompleted      │              │
│  PaymentFailed         │              ▼
└────────────────────────┘      NotificationConsumer
   │              │              group=notification-service
   │ group=       │ group=
   │ payment-     │ order-status-
   │ service      │ service
   ▼              ▼
PaymentConsumer   OrderStatusProjector
   │               (orders.status 갱신)
   │  ① 관심 없는 이벤트 걸러내기
   │  ② 이미 결제됐나 조회 (빠른 길. 경합은 못 막는다)
   │  ③ payments 에 PENDING 예약 ← 여기서 유니크 제약이 승자를 정한다
   │  ④ 외부 PG 호출 ──HTTP──▶ mock-pg 컨테이너 (:9090)
   │  ⑤ @Transactional {
   │       예약한 행 확정 + outbox 2건
   │     }
   ▼
 실패 시 → 0.5s → 1s → 2s 재시도 → order.events.DLT
```

**컨슈머가 다시 프로듀서가 된다.** 결제 컨슈머는 결과를 아웃박스에 적고, 같은 릴레이가 발행한다.
발행 경로는 끝까지 하나뿐이다.

---

## 이 프로젝트가 다루는 것

| 주제 | 어디서 볼 수 있나 |
|---|---|
| 파티션 키와 순서 보장 | `OrderService`, `PartitionKeyOrderingTest`, `OrderEventChainTest` |
| 한 토픽에 여러 이벤트 타입 vs 토픽 분리 | `Topics` 주석, `PaymentConsumer` 의 타입 필터 |
| 컨슈머 그룹 fan-out | `PaymentConsumer` ↔ `OrderStatusProjector` (같은 토픽, 다른 그룹) |
| 수동 오프셋 커밋 | `KafkaConsumerConfig`, 각 컨슈머의 `ack.acknowledge()` |
| 재시도 + 지수 백오프 + DLT | `KafkaConsumerConfig`, `PaymentFailureTest` |
| **실패 이력 기록과 재처리** | `FailedEvent`, `DeadLetterConsumer` |
| **재시도 가능 실패 vs 불가능 실패** | `PaymentGatewayClient` |
| 멱등 프로듀서 / `acks=all` | `KafkaProducerConfig` |
| **업무 테이블 제약으로 만드는 멱등 컨슈머** | `Payment`, `schema.sql`, `PaymentIdempotencyTest` |
| **외부 API 멱등키** | `PaymentGatewayClient`, `MockPaymentGateway` |
| 트랜잭셔널 아웃박스 | `OrderService`, `PaymentService`, `OutboxRelay` |
| 자연 멱등이라 중복 검사가 필요 없는 경우 | `OrderStatusProjector` 주석 |

---

## 실행

```sh
docker compose up -d --build        # Kafka + kafka-ui + PostgreSQL + mock-pg
./gradlew bootRun
```

첫 `--build` 는 mock-pg 이미지를 만드느라 몇 분 걸린다. 이후에는 `docker compose up -d` 로 충분하다.

기동 시 `schema.sql` 이 `uk_payments_open_order` 를 만든다. 이 인덱스가 생기기 전에 쌓인
데이터에 같은 주문의 성공 결제가 두 건 있으면 인덱스 생성이 실패하고 **앱이 뜨지 않는다.**
제약을 조용히 건너뛰는 것보다 낫다고 보고 `continue-on-error` 는 켜지 않았다. 이때는 중복을 먼저 지운다.

```sh
docker exec kafka-lab-postgres psql -U kafkalab -d kafkalab -c \
  "select order_id, count(*) from payments where status='COMPLETED' group by 1 having count(*) > 1;"
```

`schema.sql` 은 `payments_status_check` 도 다시 세운다. Hibernate 가 `@Enumerated(STRING)` 컬럼에
값 목록을 CHECK 제약으로 만들어 두는데 **`ddl-auto: update` 는 그 제약을 갱신하지 않기 때문**이다.
enum 에 값을 추가하면 기존 DB 에서만 INSERT 가 거부되고, 테스트는 `create-drop` 이라 이 함정을 못 잡는다.
`PaymentStatus.PENDING` 을 추가했을 때 실제로 겪은 일이다.

`processed_events` 를 쓰던 시절의 DB 라면 그 테이블도 남아 있다. `ddl-auto: update` 는 지우지 않는다.

```sh
docker exec kafka-lab-postgres psql -U kafkalab -d kafkalab -c "drop table if exists processed_events;"
```

- **Swagger UI: http://localhost:8080/swagger-ui/index.html** ← 여기서 바로 호출
- Kafka UI: http://localhost:8081 (토픽·파티션·컨슈머 랙)
- mock 결제 게이트웨이: http://localhost:9090

```sh
# 정상 결제
curl -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' -d '{"customerId":"c-1","amount":25000}'

# 한도 초과 → PG 가 402 로 거절 (재시도 없이 실패 확정)
curl -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' -d '{"customerId":"c-1","amount":2000000}'
```

기본 설정은 **30% 확률로 500, 10% 확률로 8초 지연**이다. 정상 흐름만 보려면 실패율을 끈다.

```sh
curl -X POST http://localhost:9090/_config -H 'Content-Type: application/json' \
  -d '{"failureRate":0,"timeoutRate":0,"delayMillis":8000,"declineAbove":1000000}'
```

mock 코드를 고치는 중이라면 이미지 재빌드 없이 직접 띄우는 편이 빠르다.

```sh
docker compose stop mock-pg
./gradlew :mock-pg:run
```

테스트는 Testcontainers가 Kafka/PostgreSQL을 띄우고 mock 게이트웨이는 JVM 안에서 직접 뜬다.
Docker만 켜져 있으면 `docker compose up` 없이도 돌아간다.

```sh
./gradlew test
```

---

## 실습 시나리오

### 1. 이벤트 체인과 순서 보장

주문을 하나 넣고 로그를 본다. 이벤트 하나가 결제를 부르고, 결제 결과가 다시 이벤트가 되어
상태 갱신과 알림을 부른다.

```sh
docker exec kafka-lab-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order.events --from-beginning \
  --property print.partition=true --property print.key=true
```

**확인할 것**: 같은 `orderId` 의 `ORDER_CREATED` 와 `PAYMENT_COMPLETED` 가 **같은 파티션에 이 순서로** 있다.
파티션 키를 `orderId` 로 잡았기 때문이다. 순서가 뒤집히면 `OrderStatusProjector` 가
존재하지 않는 주문을 갱신하려 든다. (`OrderEventChainTest`)

### 2. 같은 토픽, 다른 그룹 — fan-out

`PaymentConsumer` 와 `OrderStatusProjector` 는 `order.events` 를 각자 읽는다.

```sh
docker exec kafka-lab-broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group payment-service
docker exec kafka-lab-broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group order-status-service
```

**확인할 것**: 두 그룹의 `CURRENT-OFFSET` 이 따로 논다. 그룹이 같았다면 파티션이 나뉘어
한쪽만 받았을 것이다. `groupId` 는 라우팅이 아니라 **오프셋 장부의 이름**이다.

### 3. 외부 서버가 죽으면 — 재시도 후 DLT

```sh
docker compose stop mock-pg
curl -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' -d '{"customerId":"c-outage","amount":30000}'
```

**확인할 것**: 0.5초 → 1초 → 2초 간격으로 3번 재시도한 뒤 `order.events.DLT` 로 넘어간다.
주문 상태는 `CREATED` 그대로고 `payments` 에는 아무것도 남지 않는다 — **결제 실패로 확정하지 않는다.**
장애는 결과가 아니기 때문이다. 나중에 사람이 DLT 를 보고 재처리한다.

```sh
docker compose start mock-pg    # 되살리면 이후 주문은 정상 처리된다
```

**실패는 `failed_events` 테이블에 기록된다.** DLT 토픽만으로는 조사가 안 되기 때문이다 —
보존 기간이 지나면 사라지고, 검색·집계가 안 되고, "이미 처리했는지" 를 표시할 자리가 없다.

```sh
docker exec kafka-lab-postgres psql -U kafkalab -d kafkalab -c \
  "select id, original_topic, original_consumer_group, exception_class, status, failed_at
     from failed_events where status='PENDING' order by id desc;"

# 원인별 집계
docker exec kafka-lab-postgres psql -U kafkalab -d kafkalab -c \
  "select exception_class, count(*) from failed_events group by 1 order by 2 desc;"
```

역할이 나뉜다. **DLT 토픽 = 재발행할 원본 데이터, `failed_events` = 조사·추적용 인덱스.**
둘 다 있어야 한다.

#### 재처리 — 원인을 고친 뒤 원본 토픽에 다시 넣는다

자동 재처리는 넣지 않았다. 원인을 모른 채 되돌리면 같은 실패를 반복하고, 최악의 경우
DLT → 원본 → DLT 로 무한히 돈다. 사람이 판단하는 게 기본이다.

```sh
# 1. DLT 를 키까지 함께 뽑는다. 키가 없으면 다른 파티션으로 흩어져 순서가 깨진다.
docker exec kafka-lab-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order.events.DLT --from-beginning \
  --formatter-property print.key=true --formatter-property key.separator='|' \
  --timeout-ms 8000 > dlt.txt

# 2. 원인을 고친다 (여기서는 mock-pg 를 되살리는 것)
docker compose start mock-pg

# 3. 원본 토픽에 재발행
cat dlt.txt | docker exec -i kafka-lab-broker /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic order.events \
  --property parse.key=true --property key.separator='|'

# 4. 손댄 건은 상태를 바꿔둔다
docker exec kafka-lab-postgres psql -U kafkalab -d kafkalab -c \
  "update failed_events set status='RETRIED', retry_count=retry_count+1, last_retried_at=now()
     where status='PENDING';"
```

**재처리가 안전한 이유는 멱등 컨슈머가 있기 때문이다.** 결제가 성공한 뒤 다른 단계에서 터져 DLT 로 갔다면,
재처리해도 `payments.order_id` 부분 유니크 인덱스가 막아 이중 결제가 나지 않는다. 멱등성 없이 재처리하면 사고다.

주의: 재처리 시점에는 그 주문의 후속 이벤트가 이미 처리됐을 수 있다. **순서는 이미 깨져 있다.**

### 4. 거절은 재시도하지 않는다

```sh
curl -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' -d '{"customerId":"c-1","amount":2000000}'
```

**확인할 것**: 재시도 로그가 없고 DLT 로도 가지 않는다. `payments.status = FAILED`,
`orders.status = PAYMENT_FAILED` 로 즉시 확정된다.

이 구분이 이 프로젝트에서 가장 중요한 부분이다. 거절을 예외로 던지면 재시도 3번을 낭비하고
DLT 를 오염시킨다. 반대로 일시 장애를 확정 실패로 처리하면 멀쩡한 주문이 결제 실패로 굳는다.
`PaymentGatewayClient` 가 그 갈림길이다.

### 5. 중복은 반드시 온다

같은 이벤트를 두 번 흘려보낸다.

```sh
docker exec kafka-lab-broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group payment-service \
  --topic order.events --reset-offsets --to-earliest --execute
```

(먼저 앱을 내려야 한다. 오프셋 되감기는 그룹에 활성 컨슈머가 없을 때만 된다.)

**확인할 것**: 과거 이벤트가 전부 다시 흘러들어오지만 `payments` 는 늘지 않는다.
"처리했다" 는 기록을 따로 두지 않는다. 결제 결과 자체가 증거이고, 최종 방어선은 DB 제약이다.

```sql
select order_id, count(*) from payments where status in ('PENDING', 'COMPLETED') group by 1 having count(*) > 1;
-- 한 건도 나오면 안 된다. uk_payments_open_order 가 애초에 INSERT 를 거부한다.
```

판별 기준이 `eventId` 가 아니라 `orderId` 라서 더 강하다. 버그로 같은 주문에 대해 **다른 eventId 로**
`OrderCreated` 가 두 번 발행돼도 막힌다. eventId 기준이었다면 그대로 통과해 이중 결제가 났을 것이다.

**조회만으로는 부족하다는 게 이 절의 진짜 요점이다.** 조회하고 결제하고 저장하는 사이는 통째로 열려 있다.
같은 주문이 다른 파티션에 실려 오거나 리밸런스가 끼면 두 스레드가 나란히 조회를 통과해
결제를 두 번 하고, 그제서야 한 건이 제약에 걸린다 — 돈은 이미 나간 뒤다.

그래서 **PG 를 부르기 전에 `PENDING` 행을 먼저 넣는다.** 진 쪽은 INSERT 가 거부돼 결제를 시도조차 못 한다.
멱등성을 "이미 했는지 확인" 이 아니라 "자리를 선점" 으로 만드는 것이다.

```sql
-- 결제 중인 주문 (평소엔 비어 있거나 순간적으로만 보인다)
select * from payments where status = 'PENDING';
```

PG 호출이 예외로 끝나면 예약을 지우고 예외를 다시 던진다. 안 지우면 Kafka 재시도가
자기가 남긴 예약에 막혀 영영 결제되지 않는다. 다만 **호출 도중 프로세스가 죽으면 PENDING 이 남고**
그 주문은 막힌다. 학습용이라 청소 배치는 두지 않았다 — 위 쿼리로 확인하고 손으로 지우면 된다.

거절은 이 제약 밖이다(`FAILED` 는 인덱스에 없다). 한도를 올린 뒤 다시 흘리면 결제가 새로 시도된다.

### 6. 타임아웃 — 결제됐는지 모르는 상태

지연 확률을 100%로 올린다.

```sh
curl -X POST http://localhost:9090/_config -H 'Content-Type: application/json' \
  -d '{"failureRate":0,"timeoutRate":1,"delayMillis":8000,"declineAbove":1000000}'
```

**확인할 것**: 호출 측은 3초에 끊지만 **게이트웨이 쪽에서는 결제가 진행된다.**
재시도할 때 같은 `Idempotency-Key`(= eventId) 를 보내기 때문에 게이트웨이가 처음 결과를 그대로 돌려주고,
이중 결제가 나지 않는다. `PaymentGatewayClient` 에서 헤더를 빼고 돌려보면 차이가 보인다.

### 7. 컨슈머를 늘리면 어디까지 빨라지나

`KafkaConsumerConfig` 의 `setConcurrency` 를 5로 올려 재기동하고 로그의 thread 이름을 본다.

**확인할 것**: 파티션이 3개면 스레드를 5개 띄워도 2개는 파티션을 못 받아 논다.
병렬도의 상한은 파티션 수다.

### 8. 파티션은 늘릴 수만 있다

```sh
docker exec kafka-lab-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --alter --topic order.events --partitions 6
```

줄이는 명령은 실패한다. 늘리면 키-파티션 매핑이 바뀌어 **기존 키의 순서 보장이 그 시점에 끊긴다.**

### 9. DLT 는 순서를 깬다

`PaymentConsumer` 에서 예외를 던지도록 잠깐 고쳐두고 같은 주문에 이벤트를 두 건 흘려보면,
앞 이벤트는 DLT 로 빠지고 뒤 이벤트는 정상 처리된다.

**확인할 것**: DLT 는 막힌 파티션을 뚫는 장치지 공짜가 아니다. 재고·잔액처럼 앞 이벤트가 만든 상태 위에
뒤 이벤트가 얹히는 도메인이라면, DLT 대신 그 파티션을 멈추는 선택을 해야 할 수도 있다.

---

## 모듈 구성

```
kafka-lab/
├── src/main/kotlin/com/roomdoor/kafkalab/
│   ├── config/        토픽·프로듀서·컨슈머 설정
│   ├── order/         주문 도메인, 이벤트 모델, 상태 프로젝터
│   ├── payment/       결제 컨슈머, PG 클라이언트, 트랜잭션 경계
│   ├── notification/  알림 컨슈머
│   ├── outbox/        아웃박스 엔티티·릴레이
│   └── dlt/           DLT 감시 + 실패 이력 테이블
└── mock-pg/           별도 모듈. Ktor 로 만든 가짜 결제 게이트웨이 (Docker 이미지)
```

mock-pg 는 앱과 **의존성이 분리**돼 있다. Ktor 는 앱 런타임 클래스패스에 들어가지 않고,
테스트에서만 `testImplementation(project(":mock-pg"))` 로 끌어다 쓴다.

---

## 의도적으로 넣지 않은 것

- **배송 체인** — 결제까지로 범위를 끊었다. 개념이 반복될 뿐이라 우선순위가 낮다.
- **Kafka Streams / ksqlDB** — 집계·윈도우 처리는 별도 주제라 범위를 흐린다.
- **Schema Registry / Avro** — JSON 문자열로 두고, 스키마 호환성 문제는 `OrderEvent` 주석으로만 다뤘다.
- **Kafka 트랜잭션(exactly-once)** — 아웃박스 + 멱등 컨슈머로 같은 목적을 달성했다.
  Kafka 트랜잭션은 "Kafka에서 읽어 Kafka로 쓰는" 경로에서 값어치가 크고, DB가 끼면 어차피 아웃박스가 필요하다.
- **보상 트랜잭션 / Saga** — 결제 실패는 주문 상태 변경까지만 표현한다.
- **재조회(reconciliation) 배치** — 타임아웃 후 결과 불명 상태는 멱등키로만 다룬다.
  실무에서는 PG 와 주기적으로 대사하는 배치가 반드시 있다. `payments.pg_transaction_id` 가 그 연결고리다.
- **멀티 브로커** — 복제·ISR·리더 선출을 보려면 브로커 3대가 필요하다.
  단일 브로커에서는 `acks=all` 도 사실상 `acks=1` 과 같다는 점만 알아두면 된다.
