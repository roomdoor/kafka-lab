package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.order.OrderEvent
import com.roomdoor.kafkalab.order.OrderEventType
import com.roomdoor.kafkalab.payment.Payment
import com.roomdoor.kafkalab.payment.PaymentRepository
import com.roomdoor.kafkalab.payment.PaymentStatus
import com.roomdoor.kafkalab.support.IntegrationTestBase
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
 * 처리 이력을 따로 적지 않는다. payments 에 그 주문 행이 있다는 것 자체가 "이미 처리했다" 는 증거다.
 * 최종 방어선은 `order_id` 부분 유니크 인덱스다.
 */
class PaymentIdempotencyTest : IntegrationTestBase() {

	@Autowired
	private lateinit var paymentRepository: PaymentRepository

	@Test
	fun `같은 이벤트를 두 번 발행해도 결제는 한 번만 처리된다`() {
		val orderId = "order-dup-${UUID.randomUUID()}"
		val event = OrderEvent(
			// eventId 가 아니라 orderId 가 중복을 판별하는 근거다. eventId 가 달라도 결과는 같아야 한다.
			eventId = UUID.randomUUID().toString(),
			eventType = OrderEventType.ORDER_CREATED,
			orderId = orderId,
			customerId = "customer-dup",
			amount = 12_000,
			occurredAt = Instant.now(),
		)
		val payload = jsonMapper.writeValueAsString(event)

		// 릴레이가 재발행한 상황을 흉내낸다.
		kafkaTemplate.send(Topics.ORDER_EVENTS, orderId, payload).get()
		kafkaTemplate.send(Topics.ORDER_EVENTS, orderId, payload).get()

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

	private fun payment(orderId: String, status: PaymentStatus) = Payment(
		paymentId = UUID.randomUUID().toString(),
		orderId = orderId,
		amount = 1_000,
		status = status,
	)
}
