package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.order.OrderEvent
import com.roomdoor.kafkalab.order.OrderEventType
import com.roomdoor.kafkalab.payment.Payment
import com.roomdoor.kafkalab.payment.PaymentRepository
import com.roomdoor.kafkalab.payment.PaymentStatus
import com.roomdoor.kafkalab.support.IntegrationTestBase
import com.roomdoor.mockpg.MockPgConfig
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.dao.DataIntegrityViolationException
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * 같은 이벤트가 두 번 와도 결제는 한 번만 되는지 확인한다.
 *
 * Kafka 는 at-least-once 라 중복은 예외 상황이 아니라 **정상 동작**이다.
 * 아웃박스 릴레이가 발행 직후 죽거나, 컨슈머가 커밋 전에 죽으면 같은 메시지가 다시 온다.
 *
 * 처리 이력을 따로 적지 않는다. payments 에 **성공한 결제**가 있다는 것 자체가 "이미 처리했다" 는 증거다.
 * 최종 방어선은 `uk_payments_completed_order` 부분 유니크 인덱스다.
 */
class PaymentIdempotencyTest : IntegrationTestBase() {

	@Autowired
	private lateinit var paymentRepository: PaymentRepository

	@Test
	fun `같은 주문 이벤트가 두 번 오면 eventId 가 달라도 결제는 한 번만 처리된다`() {
		val orderId = "order-dup-${UUID.randomUUID()}"
		val event = OrderEvent(
			eventId = UUID.randomUUID().toString(),
			eventType = OrderEventType.ORDER_CREATED,
			orderId = orderId,
			customerId = "customer-dup",
			amount = 12_000,
			occurredAt = Instant.now(),
		)
		// 릴레이가 재발행한 상황을 흉내낸다.
		kafkaTemplate.send(Topics.ORDER_EVENTS, orderId, jsonMapper.writeValueAsString(event)).get()

		// 두 번째는 eventId 만 다르다. 판별 근거가 orderId 라서 이것도 막혀야 한다.
		// eventId 기준이었다면 그대로 통과해 이중 결제가 났을 자리다.
		val republished = event.copy(eventId = UUID.randomUUID().toString())
		kafkaTemplate.send(Topics.ORDER_EVENTS, orderId, jsonMapper.writeValueAsString(republished)).get()

		await().atMost(Duration.ofSeconds(30)).untilAsserted {
			assertEquals(1, paymentRepository.countByOrderId(orderId), "결제는 한 건만 기록돼야 한다")
		}

		// 두 번째 메시지까지 확실히 소비되도록 잠시 더 두고 다시 확인한다.
		Thread.sleep(3_000)
		assertEquals(1, paymentRepository.countByOrderId(orderId), "뒤늦게 중복 결제가 생기면 안 된다")
	}

	/**
	 * 컨슈머의 사전 조회는 빠른 길일 뿐이고, 동시에 들어온 두 스레드는 둘 다 통과한다.
	 * 실제로 막는 것은 DB 제약이므로 그 제약이 살아있는지 직접 확인한다.
	 */
	@Test
	fun `성공한 결제는 주문당 하나만 저장되고 거절은 여러 건 쌓인다`() {
		val orderId = "order-constraint-${UUID.randomUUID()}"

		paymentRepository.saveAndFlush(payment(orderId, PaymentStatus.COMPLETED))

		assertFailsWith<DataIntegrityViolationException>("같은 주문의 두 번째 성공 결제는 DB 가 거부해야 한다") {
			paymentRepository.saveAndFlush(payment(orderId, PaymentStatus.COMPLETED))
		}

		// 거절까지 하나로 막으면 다른 카드로 다시 시도하는 흐름이 영영 막힌다.
		val declinedOrderId = "order-declined-${UUID.randomUUID()}"
		paymentRepository.saveAndFlush(payment(declinedOrderId, PaymentStatus.FAILED))
		paymentRepository.saveAndFlush(payment(declinedOrderId, PaymentStatus.FAILED))

		assertEquals(2, paymentRepository.countByOrderId(declinedOrderId), "거절은 몇 번이든 쌓일 수 있어야 한다")
	}

	/**
	 * 멱등 검사가 성공한 결제만 보는지 확인한다. 거절까지 세면 한도를 올린 뒤 재발행해도
	 * 컨슈머가 조용히 건너뛰어 주문이 실패 상태로 굳는다.
	 */
	@Test
	fun `거절된 주문은 재발행하면 다시 결제된다`() {
		val orderId = "order-retry-${UUID.randomUUID()}"
		val event = OrderEvent(
			eventId = UUID.randomUUID().toString(),
			eventType = OrderEventType.ORDER_CREATED,
			orderId = orderId,
			customerId = "customer-retry",
			amount = 2_000_000,
			occurredAt = Instant.now(),
		)
		val payload = jsonMapper.writeValueAsString(event)

		// mock 은 100만원을 넘으면 402 로 거절한다.
		kafkaTemplate.send(Topics.ORDER_EVENTS, orderId, payload).get()

		await().atMost(Duration.ofSeconds(30)).untilAsserted {
			assertEquals(
				PaymentStatus.FAILED,
				paymentRepository.findTopByOrderIdOrderByIdDesc(orderId)?.status,
				"먼저 거절로 확정돼야 한다",
			)
		}

		// 한도를 올리고 사람이 결제를 다시 시도시킨 상황이다.
		// eventId 를 새로 매기는 게 중요하다. 그대로 재발행하면 PG 가 그 멱등키의 거절을 그대로 재생한다.
		// 재시도는 "새 결제 시도" 이므로 새 멱등키가 필요하다.
		configureMockPg(MockPgConfig(declineAbove = 5_000_000))
		val reattempt = event.copy(eventId = UUID.randomUUID().toString())
		kafkaTemplate.send(Topics.ORDER_EVENTS, orderId, jsonMapper.writeValueAsString(reattempt)).get()

		await().atMost(Duration.ofSeconds(30)).untilAsserted {
			assertEquals(
				1,
				paymentRepository.countByOrderIdAndStatus(orderId, PaymentStatus.COMPLETED),
				"거절은 멱등 검사에 걸리지 않으므로 재결제가 성공해야 한다",
			)
		}
	}

	private fun payment(orderId: String, status: PaymentStatus) = Payment(
		paymentId = UUID.randomUUID().toString(),
		orderId = orderId,
		amount = 1_000,
		status = status,
	)
}
