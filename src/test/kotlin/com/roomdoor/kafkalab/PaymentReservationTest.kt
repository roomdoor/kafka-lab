package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.order.OrderEvent
import com.roomdoor.kafkalab.order.OrderEventType
import com.roomdoor.kafkalab.outbox.OutboxRepository
import com.roomdoor.kafkalab.payment.PaymentConsumer
import com.roomdoor.kafkalab.payment.PaymentGatewayException
import com.roomdoor.kafkalab.payment.PaymentRepository
import com.roomdoor.kafkalab.payment.PaymentService
import com.roomdoor.kafkalab.payment.PaymentStatus
import com.roomdoor.kafkalab.support.IntegrationTestBase
import com.roomdoor.mockpg.MockPgConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.support.Acknowledgment
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * PENDING 예약이 **PG 결제 시도(= eventId = 멱등키)** 와 묶여 있는지 확인한다 (#1, #2).
 *
 * 재시도 타이밍을 Kafka 백오프에 맡기면 테스트가 느리고 흔들린다. 여기서는 [PaymentConsumer.consume] 을
 * 직접 불러 "첫 배달 → 재배달" 을 손으로 재현한다. 같은 레코드를 다시 넘기는 게 Kafka 재시도와 같은 상황이다.
 */
class PaymentReservationTest : IntegrationTestBase() {

	@Autowired
	private lateinit var paymentConsumer: PaymentConsumer

	@Autowired
	private lateinit var paymentRepository: PaymentRepository

	@Autowired
	private lateinit var paymentService: PaymentService

	@Autowired
	private lateinit var outboxRepository: OutboxRepository

	@Autowired
	private lateinit var jdbcTemplate: JdbcTemplate

	/**
	 * #1. PG 는 승인했는데 결과 확정 트랜잭션이 롤백됐다. 재배달이 자기가 남긴 예약을 경합으로 착각하면
	 * 돈은 나갔는데 결제는 영원히 PENDING 이고, 이벤트도 DLT 기록도 없이 ack 된다.
	 */
	@Test
	fun `결과 확정이 실패해도 재시도가 자기 예약을 이어받아 결제를 완료한다`() {
		// 트리거가 이 표식이 든 아웃박스 INSERT 만 거부한다. 다른 테스트의 이벤트는 건드리지 않는다.
		val event = orderCreated("order-complete-fails-${UUID.randomUUID()}")
		val record = record(event)

		failOutboxInsertsContaining("complete-fails")
		try {
			// PG 승인 뒤 아웃박스 INSERT 가 터져 completePayment 가 롤백된다. 예약(PENDING)은 이미 커밋돼 있다.
			assertFails { paymentConsumer.consume(record, Acknowledgment { }) }
		} finally {
			restoreOutboxInserts()
		}

		var acked = false
		paymentConsumer.consume(record, Acknowledgment { acked = true })

		assertTrue(acked)
		assertEquals(
			PaymentStatus.COMPLETED,
			paymentRepository.findTopByOrderIdOrderByIdDesc(event.orderId)?.status,
			"재시도가 자기 예약을 경합으로 보고 건너뛰면 PENDING 에 갇힌다",
		)
		assertTrue(
			outboxRepository.findAll().any { it.aggregateId == event.orderId && it.payload.contains("PAYMENT_COMPLETED") },
			"결제 완료 이벤트가 아웃박스에 남아야 주문이 PAID 로 넘어간다",
		)
	}

	/**
	 * #2. 읽기 타임아웃은 "결제됐는지 모름" 이다. 이때 예약을 지우면, 그 사이 같은 주문이 다른 eventId 로
	 * 들어왔을 때 새 멱등키로 PG 를 불러 이중 결제가 난다.
	 */
	@Test
	fun `PG 타임아웃 뒤에도 예약이 남아 다른 eventId 의 같은 주문은 결제하지 못한다`() {
		// 호출 측 read timeout 은 3초. mock 은 4초 뒤에 결제를 끝내고 결과를 멱등키에 저장한다.
		configureMockPg(MockPgConfig(timeoutRate = 1.0, delayMillis = 4_000))
		val original = orderCreated("order-timeout-${UUID.randomUUID()}")

		assertFailsWith<PaymentGatewayException> { paymentConsumer.consume(record(original), Acknowledgment { }) }
		assertEquals(
			PaymentStatus.PENDING,
			paymentRepository.findTopByOrderIdOrderByIdDesc(original.orderId)?.status,
			"결과를 모르는 채로 예약을 풀면 orderId 방어가 사라진다",
		)

		// 버그성 재발행·수동 발행 등으로 같은 주문이 다른 eventId 로 들어온다. PG 는 이제 정상이다.
		configureMockPg(MockPgConfig())
		// PG 를 부르지 않고 던져야 한다. ack 하고 버리면 저 PENDING 이 주인 없이 남았을 때 이 이벤트가 흔적 없이 사라진다.
		var competitorAcked = false
		assertFailsWith<IllegalStateException> {
			paymentConsumer.consume(
				record(original.copy(eventId = UUID.randomUUID().toString())),
				Acknowledgment { competitorAcked = true },
			)
		}

		assertFalse(competitorAcked, "재시도·DLT 로 넘어가야 하므로 ack 하면 안 된다")
		assertEquals(
			0,
			paymentRepository.countByOrderIdAndStatus(original.orderId, PaymentStatus.COMPLETED),
			"새 멱등키로 결제했다면 여기서 이미 COMPLETED 가 생긴다 — 이중 결제",
		)

		// mock 이 첫 요청을 끝내고 결과를 저장할 때까지 기다린다. 그 전에 같은 키로 부르면
		// mock 이 진행 중인 요청을 몰라 새로 결제한다(#3). 이 테스트가 보려는 것과는 별개의 구멍이다.
		// 500 만 내게 해두고 같은 키로 물어본다. mock 은 저장된 결과를 500 판정보다 먼저 재생하므로,
		// 저장 전에는 500(결제·저장 없음), 저장 뒤에는 처음 결과가 온다. 엿보는 동안 결제가 새로 생기지 않는다.
		configureMockPg(MockPgConfig(failureRate = 1.0))
		lateinit var firstTransactionId: String
		await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(200)).until {
			val (status, transactionId) = askMockPg(original)
			if (status == 200 && transactionId != null) firstTransactionId = transactionId
			status == 200
		}

		// 원래 이벤트의 재시도. 같은 멱등키로 PG 를 다시 불러 처음 결과를 받아 확정해야 한다.
		// mock 은 여전히 500 모드라, 재생이 아니라 새로 결제하려 했다면 여기서 실패한다.
		var retryAcked = false
		paymentConsumer.consume(record(original), Acknowledgment { retryAcked = true })

		assertTrue(retryAcked)
		val payment = paymentRepository.findTopByOrderIdOrderByIdDesc(original.orderId)
		assertEquals(PaymentStatus.COMPLETED, payment?.status)
		assertEquals(
			firstTransactionId,
			payment?.pgTransactionId,
			"타임아웃 났던 첫 결제가 기록돼야 한다. 다른 거래번호라면 PG 에서 돈이 두 번 나간 것이다",
		)
	}

	/**
	 * 같은 eventId 배달 둘이 동시에 예약을 이어받은 경우(PG 호출 중 리밸런스로 재배달 등). 둘 다 PG 결과를 받아
	 * 확정하러 오지만 PENDING → COMPLETED 는 한 번만 일어나야 한다. 두 번 일어나면 완료 이벤트·알림이 두 번 나간다.
	 */
	@Test
	fun `같은 예약을 두 배달이 동시에 확정해도 완료 이벤트는 한 번만 나간다`() {
		val event = orderCreated("order-double-complete-${UUID.randomUUID()}")
		val reserved = paymentService.reserve(event)

		// 두 배달이 각자 읽어간 예약. 둘 다 PENDING 을 본 상태로 확정에 들어간다.
		val copies = List(2) { paymentRepository.findById(reserved.id!!).get() }
		val start = CountDownLatch(1)
		// thread{} 안의 예외는 테스트 스레드로 올라오지 않는다. 모아서 확인해야 조용한 실패를 놓치지 않는다.
		val errors = ConcurrentLinkedQueue<Throwable>()
		val threads = copies.map { copy ->
			thread {
				start.await()
				runCatching { paymentService.completePayment(copy, event, "pg_same_key_replay") }
					.onFailure { errors += it }
			}
		}
		start.countDown()
		threads.forEach { it.join() }

		assertTrue(errors.isEmpty(), "같은 결과로 이미 확정된 건 오류가 아니다: $errors")

		assertEquals(
			PaymentStatus.COMPLETED,
			paymentRepository.findTopByOrderIdOrderByIdDesc(event.orderId)?.status,
		)
		assertEquals(
			1,
			outboxRepository.findAll().count { it.aggregateId == event.orderId && it.payload.contains("PAYMENT_COMPLETED") },
			"완료 이벤트는 한 번만 아웃박스에 들어가야 한다",
		)
	}

	/**
	 * 예약 해제는 예약 시점의 사본을 들고 온다. 그 사이 같은 eventId 의 다른 배달이 확정했다면
	 * 사본대로 지우는 순간 결제 기록이 사라진다. PENDING 일 때만 지워야 한다.
	 */
	@Test
	fun `이미 확정된 결제는 늦게 온 예약 해제로 지워지지 않는다`() {
		val event = orderCreated("order-late-release-${UUID.randomUUID()}")
		val stale = paymentService.reserve(event)
		paymentService.completePayment(paymentRepository.findById(stale.id!!).get(), event, "pg_other_delivery")

		paymentService.releaseReservation(stale)

		assertEquals(PaymentStatus.COMPLETED, paymentRepository.findById(stale.id!!).orElse(null)?.status)
	}

	/**
	 * 첫 배달이 커넥션 거부(확실한 실패)로 예약을 풀려는 사이, 같은 eventId 의 재배달이 이미 이어받아 PG 를 부르고 있다.
	 * 여기서 지우면 자리가 비어 다른 eventId 가 새 멱등키로 결제한다. 이어받은 예약은 첫 배달이 풀지 못해야 한다.
	 */
	@Test
	fun `이어받은 예약은 처음 잡은 배달이 풀지 못한다`() {
		val event = orderCreated("order-takeover-release-${UUID.randomUUID()}")
		val reservedByFirst = paymentService.reserve(event)

		assertTrue(paymentService.takeOver(paymentRepository.findById(reservedByFirst.id!!).get()))
		val released = paymentService.releaseReservation(reservedByFirst)

		assertFalse(released)
		assertEquals(PaymentStatus.PENDING, paymentRepository.findById(reservedByFirst.id!!).orElse(null)?.status)
	}

	/**
	 * #11. 거절을 확정한 뒤 ack 전에 죽으면 같은 레코드가 다시 온다. FAILED 는 예약 인덱스 밖이라 재배달이 새로 예약하고,
	 * PG 가 402 를 재생하면 거절 이벤트와 알림이 한 번 더 나간다.
	 */
	@Test
	fun `거절을 확정한 뒤 재배달돼도 거절 이벤트는 한 번만 나간다`() {
		// 기본 설정은 100만 원 초과를 거절한다.
		val event = orderCreated("order-decline-redelivery-${UUID.randomUUID()}").copy(amount = 2_000_000)
		val record = record(event)

		paymentConsumer.consume(record, Acknowledgment { })
		var redeliveryAcked = false
		paymentConsumer.consume(record, Acknowledgment { redeliveryAcked = true })

		assertTrue(redeliveryAcked, "이미 처리된 거절이므로 재시도 없이 ack 해야 한다")
		assertEquals(1, paymentRepository.countByOrderId(event.orderId), "재배달이 FAILED 행을 또 만들면 안 된다")
		assertEquals(
			1,
			outboxRepository.findAll().count { it.aggregateId == event.orderId && it.payload.contains("PAYMENT_FAILED") },
			"거절 이벤트가 두 번 나가면 알림도 두 번 간다",
		)
	}

	/**
	 * 같은 eventId 에 FAILED 와 PENDING 이 함께 있다(두 배달이 동시에 달려 한쪽이 거절, 다른 쪽이 새로 예약한 뒤 결과 모름).
	 * 그 PENDING 은 같은 eventId 의 재배달만 이어받을 수 있다. "이미 거절됨" 으로 건너뛰면 영영 남는다.
	 */
	@Test
	fun `같은 eventId 의 PENDING 이 남아 있으면 거절 기록이 있어도 이어받는다`() {
		// 실제로는 같은 키에 PG 가 402 를 재생하므로 거절 금액으로 둔다. 이어받은 예약도 거절로 끝난다.
		val event = orderCreated("order-failed-and-pending-${UUID.randomUUID()}").copy(amount = 2_000_000)
		paymentService.declinePayment(paymentService.reserve(event), event, "테스트 거절")
		val pending = paymentService.reserve(event)

		var acked = false
		paymentConsumer.consume(record(event), Acknowledgment { acked = true })

		assertTrue(acked)
		assertEquals(
			PaymentStatus.FAILED,
			paymentRepository.findById(pending.id!!).orElse(null)?.status,
			"거절 기록만 보고 건너뛰면 이 예약이 PENDING 에 갇힌다",
		)
	}

	private fun orderCreated(orderId: String) = OrderEvent(
		eventId = UUID.randomUUID().toString(),
		eventType = OrderEventType.ORDER_CREATED,
		orderId = orderId,
		customerId = "customer-reservation",
		amount = 15_000,
		occurredAt = Instant.now(),
	)

	private fun record(event: OrderEvent) =
		ConsumerRecord(Topics.ORDER_EVENTS, 0, 0L, event.orderId, jsonMapper.writeValueAsString(event))

	/** 결과 확정 중 일시적 DB 오류를 흉내낸다. 코드에 테스트용 분기를 넣지 않으려고 DB 트리거를 쓴다. */
	private fun failOutboxInsertsContaining(marker: String) {
		jdbcTemplate.execute(
			"""
			create or replace function fail_outbox_insert() returns trigger as $$
			begin
				if new.payload like '%$marker%' then
					raise exception '테스트: 아웃박스 INSERT 실패';
				end if;
				return new;
			end
			$$ language plpgsql
			""".trimIndent()
		)
		jdbcTemplate.execute(
			"create trigger fail_outbox_insert before insert on outbox_events for each row execute function fail_outbox_insert()"
		)
	}

	private fun restoreOutboxInserts() {
		jdbcTemplate.execute("drop trigger if exists fail_outbox_insert on outbox_events")
		jdbcTemplate.execute("drop function if exists fail_outbox_insert()")
	}

	/** 같은 멱등키로 mock 에 직접 묻는다. 상태 코드와 거래번호를 돌려준다. */
	private fun askMockPg(event: OrderEvent): Pair<Int, String?> {
		val request = HttpRequest.newBuilder(URI.create("http://localhost:$mockPgPort/payments"))
			.header("Content-Type", "application/json")
			.header("Idempotency-Key", event.eventId)
			.POST(HttpRequest.BodyPublishers.ofString("""{"orderId":"${event.orderId}","amount":${event.amount}}"""))
			.build()
		val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
		return response.statusCode() to jsonMapper.readTree(response.body()).get("transactionId")?.asString()
	}
}
