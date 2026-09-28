package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.order.OrderEvent
import com.roomdoor.kafkalab.order.OrderEventType
import com.roomdoor.kafkalab.outbox.OutboxRepository
import com.roomdoor.kafkalab.payment.PaymentConsumer
import com.roomdoor.kafkalab.payment.PaymentGatewayException
import com.roomdoor.kafkalab.payment.PaymentRepository
import com.roomdoor.kafkalab.payment.PaymentStatus
import com.roomdoor.kafkalab.support.IntegrationTestBase
import com.roomdoor.mockpg.MockPgConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.support.Acknowledgment
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFails
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
		var competitorAcked = false
		paymentConsumer.consume(
			record(original.copy(eventId = UUID.randomUUID().toString())),
			Acknowledgment { competitorAcked = true },
		)

		assertTrue(competitorAcked, "경합으로 보고 물러나야 한다")
		assertEquals(
			0,
			paymentRepository.countByOrderIdAndStatus(original.orderId, PaymentStatus.COMPLETED),
			"새 멱등키로 결제했다면 여기서 이미 COMPLETED 가 생긴다 — 이중 결제",
		)

		// mock 이 첫 요청을 끝내고 결과를 저장할 때까지 기다린다. 그 전에 같은 키로 부르면
		// mock 이 진행 중인 요청을 몰라 새로 결제한다(#3). 이 테스트가 보려는 것과는 별개의 구멍이다.
		Thread.sleep(1_500)

		// 원래 이벤트의 재시도. 같은 멱등키로 PG 를 다시 불러 처음 결과를 받아 확정해야 한다.
		var retryAcked = false
		paymentConsumer.consume(record(original), Acknowledgment { retryAcked = true })

		assertTrue(retryAcked)
		val payment = paymentRepository.findTopByOrderIdOrderByIdDesc(original.orderId)
		assertEquals(PaymentStatus.COMPLETED, payment?.status)
		assertEquals(
			replayedTransactionId(original),
			payment?.pgTransactionId,
			"타임아웃 났던 첫 결제가 기록돼야 한다. 다른 거래번호라면 PG 에서 돈이 두 번 나간 것이다",
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

	/** 같은 멱등키로 mock 에 다시 물어 처음 결과의 거래번호를 받는다. 저장된 결과는 재생될 뿐 새 결제가 아니다. */
	private fun replayedTransactionId(event: OrderEvent): String? {
		val request = HttpRequest.newBuilder(URI.create("http://localhost:$mockPgPort/payments"))
			.header("Content-Type", "application/json")
			.header("Idempotency-Key", event.eventId)
			.POST(HttpRequest.BodyPublishers.ofString("""{"orderId":"${event.orderId}","amount":${event.amount}}"""))
			.build()
		val body = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString()).body()
		return jsonMapper.readTree(body).get("transactionId")?.asString()
	}
}
