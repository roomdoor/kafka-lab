# 작업 인계 문서

다른 컴퓨터에서 이어서 작업하려고 남기는 기록이다. 프로젝트 자체 설명은 [README.md](README.md)에 있고,
이 문서는 **최근 세션에서 무엇을 왜 바꿨는지**와 **아직 안 한 것**만 다룬다.

기준 커밋: `73ae1b0` → `3a93ac4` (main). 17개 파일, +460 / -157.

---

## 1. 이번에 바꾼 것

### 1-1. `processed_events` 테이블 제거 → `payments` 제약으로 대체

**커밋** `afc91aa`, `dd2e36e`

멱등성을 위해 `(consumer_group, event_id)` 유니크를 가진 별도 테이블을 두고 있었다. 그런데 결제 결과가
`payments`에 이미 남는데 "처리했다"는 사실을 또 적는 건 같은 말을 두 번 하는 것이었다. 실제로 그 테이블을
쓰는 컨슈머는 `PaymentConsumer` 하나뿐이었다.

- `idempotency/ProcessedEvent.kt`, `ProcessedEventRepository.kt` 삭제
- `payments`에 부분 유니크 인덱스 도입 (`schema.sql`)
- 판별 기준이 `eventId` → `orderId`로 바뀌었다. **더 강하다** — 같은 주문이 버그로 다른 eventId로
  두 번 발행돼도 막힌다. eventId 기준이었다면 통과해서 이중 결제가 났을 자리다

`OrderStatusProjector`는 상태 덮어쓰기라 자연 멱등, `NotificationConsumer`는 알림 두 번을 감수,
`DeadLetterConsumer`는 `failed_events` 유니크 제약으로 이미 막힌다. 그래서 범용 장치가 필요 없었다.

### 1-2. PG 호출 전 `PENDING` 예약

**커밋** `e1d66f5` — 이번 세션에서 제일 중요한 변경

"이미 결제됐나 조회 → PG 호출 → 결과 저장" 순서는 **조회와 저장 사이가 통째로 열려 있다.** 같은 주문이
다른 파티션에 실려 오거나 리밸런스가 끼면 두 스레드가 나란히 조회를 통과해 결제를 두 번 하고,
그제서야 한 건이 유니크 제약에 걸린다 — 돈은 이미 나간 뒤다.

그래서 **PG를 부르기 전에 `PENDING` 행을 먼저 INSERT**한다. 진 쪽은 DB 제약에 걸려 결제를 시도조차 못 한다.
멱등성을 "이미 했는지 확인"이 아니라 **"자리를 선점"**으로 바꾼 것이다.

```sql
create unique index uk_payments_open_order
	on payments (order_id)
	where status in ('PENDING', 'COMPLETED');
```

`FAILED`가 빠져 있는 게 의도적이다. 거절된 주문은 한도를 올린 뒤 다시 시도할 수 있어야 한다.

**PG 멱등키(`Idempotency-Key`)로는 이 구멍이 안 막힌다.** 키가 `eventId`라서 같은 주문이 다른 eventId로
발행되면 PG는 별개 결제로 본다. 게다가 mock PG의 `IdempotencyStore`는 조회 후 저장이라 동시 요청에
그 자체로 취약하다(실제 PG는 보통 409를 던진다 — 조용히 재생해주지 않는다).

흐름은 이렇게 된다.

```
① 관심 없는 이벤트 걸러내기
② 이미 결제됐나 조회        ← 빠른 길일 뿐. 경합은 못 막는다
③ payments 에 PENDING 예약  ← 여기서 DB 제약이 승자를 정한다
④ 외부 PG 호출              ← 진 쪽은 여기 도달 못 함
⑤ 예약한 행을 COMPLETED / FAILED 로 확정
```

PG 호출이 예외로 끝나면 **예약을 지우고 다시 던진다.** 안 지우면 Kafka 재시도가 자기가 남긴 예약에
막혀 영영 결제되지 않는다.

### 1-3. `payments_status_check` 재생성

**커밋** `8b630f0` — 실제로 겪은 사고

`PaymentStatus`에 `PENDING`을 추가했더니 기존 DB에서 INSERT가 거부됐다. Hibernate가 `@Enumerated(STRING)`
컬럼에 값 목록을 CHECK 제약으로 만들어 두는데, **`ddl-auto: update`는 그 제약을 갱신하지 않는다.**
테스트는 `create-drop`이라 매번 새로 만들어져 이 함정을 못 잡는다.

`schema.sql`에서 drop 후 다시 세운다. **enum에 값을 추가할 때마다 여기를 같이 고쳐야 한다.**

같이 드러난 더 나쁜 문제: 예약 실패를 `DataIntegrityViolationException`으로 통째로 잡고 있어서
**CHECK 제약 위반을 경합으로 착각하고 조용히 ack**했다. 주문이 결제도 DLT도 없이 사라졌다.
경합은 유니크 위반뿐이므로 `DuplicateKeyException`으로 좁혔다.

### 1-4. 호출 로그

**커밋** `9247313`, `3a93ac4`

`config/CallLoggingAspect.kt` — `@RestController`와 `@Service`의 public 메서드 진입/종료/예외를 남긴다.
중첩 깊이만큼 들여쓰고, 로그 패턴의 스레드 이름과 함께 보면 **흐름이 스레드를 건너뛰는 지점**이 보인다.

```
→ OrderController.create(CreateOrderRequest(customerId=c-6, amount=22000))
  → OrderService.createOrder(c-6, 22000)
  ← OrderService.createOrder 124ms
← OrderController.create 133ms
```

컨슈머는 `@Component`라 안 잡힌다 — 이미 자기 흐름을 직접 로그로 남기고 있어서 중복을 피했다.
필요하면 포인트컷에 `@within(org.springframework.stereotype.Component)`를 더하면 된다.

그리고 PG 호출 실패 원인을 예약 해제 직전에 남긴다. 이전에는 재시도가 성공해버리면 실패했다는 사실이
**어디에도 안 남았다** — 클라이언트는 로그 없이 던지기만 하고, 에러 핸들러는 DLT 회수 때만 찍는다.

### 1-5. 주문 요청 임의값

**커밋** `115eb89`, `b1023f0`

파티션 분포를 보려면 주문을 여러 건 넣어야 하는데 매번 값을 지어내기 번거로워서, `{}`만 던져도 되게 했다.

```kotlin
@param:JsonSetter(nulls = Nulls.SKIP)
val customerId: String = "c-${Random.nextInt(0, CUSTOMER_COUNT + 1)}",
```

`@JsonSetter(nulls = Nulls.SKIP)`이 핵심이다. **Kotlin 기본값은 키가 아예 없을 때만 적용**되고,
`{"customerId": null}`처럼 null이 명시되면 그 null이 그대로 들어온다. SKIP이 그걸 "값 없음"으로 취급한다.

금액 상한(1,000,000)은 mock PG의 거절 기준과 같은 값이다. 넘기면 임의 주문이 가끔 거절돼 흐름을 보기 어렵다.

---

## 2. 다른 컴퓨터에서 시작하기

```sh
git clone git@github.com:roomdoor/kafka-lab.git
cd kafka-lab
docker compose up -d --build     # 첫 빌드는 mock-pg 이미지 때문에 몇 분
./gradlew bootRun
```

- Swagger UI: http://localhost:8080/swagger-ui/index.html
- Kafka UI: http://localhost:8081
- mock PG: http://localhost:9090

주문 넣기:

```sh
curl -X POST http://localhost:8080/api/orders -H 'Content-Type: application/json' -d '{}'
```

기본 mock PG 설정은 **실패율 30%, 지연 10%(8초)**다. 정상 흐름만 보려면 끈다.

```sh
curl -X POST http://localhost:9090/_config -H 'Content-Type: application/json' \
  -d '{"failureRate":0,"timeoutRate":0,"delayMillis":8000,"declineAbove":1000000}'
```

테스트는 Testcontainers로 Kafka·PostgreSQL을 직접 띄운다. Docker만 있으면 된다.

```sh
./gradlew test     # 14건, 약 40초
```

### 주의할 것

새 컴퓨터는 DB가 비어 있어서 문제없지만, **기존 DB가 있는 환경**에서는 두 가지가 걸린다.

- 같은 주문의 `COMPLETED`가 두 건 있으면 `uk_payments_open_order` 생성이 실패해 **앱이 안 뜬다.**
  제약이 조용히 없는 채로 도는 것보다 낫다고 보고 `continue-on-error`는 켜지 않았다
- `processed_events` 테이블이 남아 있을 수 있다. `ddl-auto: update`는 지우지 않는다

```sh
docker exec kafka-lab-postgres psql -U kafkalab -d kafkalab -c \
  "select order_id, count(*) from payments where status='COMPLETED' group by 1 having count(*) > 1;"
docker exec kafka-lab-postgres psql -U kafkalab -d kafkalab -c "drop table if exists processed_events;"
```

전부 밀고 싶으면 `docker compose down -v` — postgres에 named volume이 없어서 데이터가 초기화된다.

---

## 3. 아직 안 한 것

### 3-1. DLT 재처리

`DeadLetterConsumer`가 DLT를 읽어 `failed_events`에 **기록만** 한다. 재처리 API도 스케줄러도 없다.
`FailedEventStatus`에 `PENDING / RETRIED / RESOLVED / IGNORED`가 정의돼 있지만 **이 값을 바꾸는 코드가 없다.**

자동 재처리를 안 넣은 이유는 원인을 모른 채 되돌리면 DLT → 원본 → DLT로 무한히 돌 수 있기 때문이다.
만든다면 **조회 + 선택 재발행 API** 쪽이 학습용으로 낫다. 지금은 README 3절의 수동 SQL 절차뿐이다.

### 3-2. `PENDING` 고아 행 청소

PG 호출 **도중** 프로세스가 죽으면 `PENDING` 행이 남아 그 주문이 영구히 막힌다.
`PaymentConsumer`에 `ponytail:` 주석으로 표시해 뒀다.

```sql
select * from payments where status = 'PENDING';   -- 평소엔 비어 있어야 정상
```

일정 시간 지난 `PENDING`을 `FAILED`로 내리는 스케줄러를 붙이면 된다.

### 3-3. 그 밖에 논의만 하고 넘긴 것

- **배송 이벤트 체인** — 확장 계획에서 명시적으로 제외했다
- **거절 경로의 중복 방어** — 동시 중복 거절이면 `FAILED` 2행 + 알림 2번. 돈은 안 움직여서 감수했다.
  다만 승인과 거절이 갈리는 경합은 `OrderStatusProjector`에서 `PAID → PAYMENT_FAILED` 전이를 막아 처리했다
- **CDC(Debezium)** — 아웃박스 릴레이를 대체할 수 있다. 폴링 지연이 없어지는 대신 운영 요소가 늘어난다

---

## 4. 코드 읽을 때 헷갈리기 쉬운 지점

| 지점 | 주의 |
|---|---|
| `payments`의 유니크 | 단순 유니크가 아니라 **`PENDING`/`COMPLETED` 한정 부분 인덱스**. `FAILED`는 여러 건 쌓인다 |
| `findTopByOrderIdOrderByIdDesc` | 한 주문에 행이 여럿일 수 있어 단건 조회를 못 쓴다 |
| `maxAttempts = 3` | **재시도** 3번. 최초 배달까지 합쳐 PG를 **4번** 부른다 |
| 즉시 DLT 대상 | 실패 횟수가 아니라 **예외 타입**으로 사전 판별 (`addNotRetryableExceptions`) |
| 402 거절 | 예외가 아니다. 재시도하지 않고 `FAILED`로 확정 |
| `ack.acknowledge()` 위치 | 맨 마지막. 중간에 ack하면 에러 핸들러의 seek가 무의미해진다 |
