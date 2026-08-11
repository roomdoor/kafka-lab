# kafka-lab

Kafka를 실무에서 쓸 때 실제로 마주치는 문제들을 하나씩 재현하고 검증하는 학습용 프로젝트.

주문(order) → 결제(payment) / 알림(notification) 흐름을 최소한으로 만들고, 그 위에서
파티션·컨슈머 그룹·오프셋 커밋·재시도·DLT·트랜잭셔널 아웃박스를 확인한다.

**Kotlin 2.2 / Spring Boot 4.0 / Kafka 4.3 (KRaft) / PostgreSQL 17 / Testcontainers**

---

## 이 프로젝트가 다루는 것

| 주제 | 어디서 볼 수 있나 |
|---|---|
| 파티션 키와 순서 보장 | `OrderService`, `PartitionKeyOrderingTest` |
| 컨슈머 그룹 fan-out | `PaymentConsumer`, `NotificationConsumer` |
| 수동 오프셋 커밋 | `KafkaConsumerConfig`, 각 컨슈머의 `ack.acknowledge()` |
| 재시도 + 지수 백오프 + DLT | `KafkaConsumerConfig`, `DeadLetterTest` |
| 멱등 프로듀서 / `acks=all` | `KafkaProducerConfig` |
| 중복 수신 방어(멱등 컨슈머) | `PaymentConsumer` |
| 트랜잭셔널 아웃박스 | `OrderService`, `OutboxRelay`, `OutboxRelayTest` |

---

## 실행

```sh
docker compose up -d          # Kafka + kafka-ui + PostgreSQL
./gradlew bootRun
```

- Kafka UI: http://localhost:8081 (토픽·파티션·컨슈머 랙 확인)
- 애플리케이션: http://localhost:8080

주문 생성:

```sh
curl -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"c-1","amount":25000}'
```

테스트는 Testcontainers로 진짜 브로커를 띄우므로 Docker만 켜져 있으면 된다
(`docker compose up` 은 필요 없다).

```sh
./gradlew test
```

---

## 실습 시나리오

각 항목은 "무엇을 확인하는가"를 먼저 읽고, 직접 돌려서 눈으로 확인하는 순서를 권한다.

### 1. 파티션 키가 순서를 결정한다

같은 `customerId`로 주문을 여러 번 만들어도 파티션은 제각각이다. 파티션 키는 `orderId`이기 때문이다.
Kafka UI의 Messages 탭에서 각 메시지의 partition 값을 보면 된다.

**확인할 것**: 순서 보장은 토픽이 아니라 **파티션 단위**다. 순서가 필요한 이벤트는 같은 키로 묶어야 한다.
(`PartitionKeyOrderingTest`)

### 2. 컨슈머 그룹이 다르면 같은 메시지를 양쪽이 받는다

`payment-service`와 `notification-service`는 같은 토픽을 읽지만 그룹이 다르다.
주문을 하나 만들면 두 컨슈머 로그가 모두 찍힌다.

**확인할 것**: 그룹이 같으면 파티션이 나뉘어 한쪽만 받고(작업 큐), 다르면 양쪽 다 받는다(fan-out).

### 3. 컨슈머를 늘리면 어디까지 빨라지나

`KafkaConsumerConfig`의 `setConcurrency` 값을 4, 5로 올려 재기동한 뒤 로그의 thread 이름을 본다.

**확인할 것**: 파티션이 3개면 컨슈머 스레드를 4개 띄워도 하나는 파티션을 못 받아 논다.
병렬도의 상한은 파티션 수다.

### 4. 실패한 메시지는 DLT로 빠진다

```sh
# customerId 를 FAIL 로 주면 결제 컨슈머가 매번 예외를 던진다
curl -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"FAIL","amount":25000}'
```

0.5초 → 1초 → 2초 간격으로 3번 재시도한 뒤 `order.created.DLT` 로 넘어간다.
Kafka UI에서 DLT 토픽을 열면 원본 토픽·파티션·예외 메시지가 헤더에 남아 있다.

**확인할 것**: 이 장치가 없으면 실패한 메시지 하나가 무한 재시도를 돌며
**같은 파티션 뒤에 쌓인 정상 메시지까지 전부 멈춘다**(poison pill).
`DefaultErrorHandler` 설정을 잠시 지우고 돌려보면 그 상황을 그대로 볼 수 있다.

### 5. 아웃박스가 없으면 무엇이 깨지나

`OrderService`는 주문과 이벤트를 **같은 DB 트랜잭션**에 쓰고, 발행은 `OutboxRelay`가 맡는다.

직접 비교해보려면 `OrderService`에서 `kafkaTemplate.send()`를 바로 호출하도록 바꾼 뒤,
전송 직후 예외를 던져 트랜잭션을 롤백시켜 본다.

**확인할 것**: 주문은 DB에 없는데 "주문 생성" 이벤트만 흘러나간 상태가 만들어진다.
DB 트랜잭션과 메시지 전송은 함께 커밋되지 않는다 — 아웃박스는 이 간극을 메우는 패턴이다.
(`OutboxRelayTest`)

### 6. 중복은 반드시 온다

`OutboxRelay`는 at-least-once다. 전송에 성공하고 `publishedAt`을 기록하기 전에 죽으면 같은 이벤트를 다시 보낸다.

**확인할 것**: `PaymentConsumer`의 `processedEventIds` 검사를 지우고 릴레이를 강제 중복 실행시키면
결제가 두 번 처리된다. Kafka에서 "정확히 한 번"은 컨슈머가 멱등하게 만들어 얻는 것이지 브로커가 주는 게 아니다.

### 7. 컨슈머를 죽여 리밸런싱 보기

애플리케이션을 두 개 띄우고(`--server.port=8082`) 한쪽을 강제 종료한다.

**확인할 것**: 남은 쪽이 죽은 인스턴스의 파티션을 넘겨받는다.
그 사이 처리 중이던 메시지는 커밋되지 않았으므로 다시 소비된다 — 6번의 중복이 실제로 발생하는 지점이다.

### 8. 파티션은 늘릴 수만 있다

```sh
docker exec kafka-lab-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --alter --topic order.created --partitions 6
```

줄이는 명령은 실패한다.

**확인할 것**: 파티션을 늘리면 키-파티션 매핑이 바뀌어, **기존 키의 순서 보장이 그 시점에 끊긴다**.
운영에서 파티션 증설을 함부로 못 하는 이유다.

---

## 유용한 명령

```sh
# 토픽 목록 / 상세
docker exec kafka-lab-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec kafka-lab-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic order.created

# 컨슈머 그룹 상태와 랙 (LAG 이 쌓이면 컨슈머가 못 따라가고 있다는 뜻)
docker exec kafka-lab-broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group payment-service

# 메시지 직접 확인
docker exec kafka-lab-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order.created.DLT --from-beginning
```

---

## 의도적으로 넣지 않은 것

- **Kafka Streams / ksqlDB** — 집계·윈도우 처리는 별도 주제라 범위를 흐린다.
- **Schema Registry / Avro** — 운영에서는 필요하지만, 여기서는 JSON 문자열로 두고
  스키마 호환성 문제 자체는 `OrderEvent` 주석으로만 다뤘다.
- **Kafka 트랜잭션(exactly-once semantics)** — 아웃박스 + 멱등 컨슈머로 같은 목적을 달성했다.
  Kafka 트랜잭션은 "Kafka에서 읽어 Kafka로 쓰는" 경로에서 값어치가 크고, DB가 끼면 어차피 아웃박스가 필요하다.
- **멀티 브로커 구성** — 복제·ISR·리더 선출을 보려면 브로커 3대가 필요하다.
  단일 브로커에서는 `acks=all`도 사실상 `acks=1`과 같다는 점만 알고 있으면 된다.
