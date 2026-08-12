package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.order.OrderEvent
import com.roomdoor.kafkalab.order.OrderEventType
import com.roomdoor.kafkalab.order.OrderRepository
import com.roomdoor.kafkalab.order.OrderService
import com.roomdoor.kafkalab.order.OrderStatus
import com.roomdoor.kafkalab.payment.PaymentRepository
import com.roomdoor.kafkalab.payment.PaymentStatus
import com.roomdoor.kafkalab.support.IntegrationTestBase
import com.roomdoor.mockpg.MockPgConfig
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 외부 결제 실패를 두 종류로 나눠 처리하는지 확인한다. 이 구분이 이 프로젝트에서 가장 중요한 부분이다.
 *
 * - **일시 장애(500)** → 재시도할 값어치가 있다. 소진하면 DLT 로 보낸다
 * - **결제 거절(402)** → 재시도해도 결과가 같다. 예외를 던지지 않고 실패로 확정한다
 *
 * 이 둘을 섞으면 반드시 하나가 잘못된다. 거절을 재시도하면 3번을 낭비하고 DLT 를 오염시키고,
 * 일시 장애를 확정 실패로 처리하면 멀쩡한 주문이 결제 실패로 굳는다.
 */
class PaymentFailureTest : IntegrationTestBase() {

	@Autowired
	private lateinit var orderService: OrderService

	@Autowired
	private lateinit var orderRepository: OrderRepository

	@Autowired
	private lateinit var paymentRepository: PaymentRepository

	@Test
	fun `게이트웨이 일시 장애는 재시도 후 DLT 로 간다`() {
		// 항상 500 을 돌려주도록 바꾼다. docker compose stop mock-pg 와 같은 상황이다.
		configureMockPg(MockPgConfig(failureRate = 1.0))

		val order = orderService.createOrder("customer-outage", amount = 30_000)

		val dltRecord = consumeMatching(Topics.ORDER_EVENTS_DLT, expectedCount = 1) {
			it.value().contains(order.orderId)
		}.firstOrNull()

		assertNotNull(dltRecord, "재시도를 소진한 뒤 DLT 에 도착해야 한다")

		// 결제는 확정되지 않았다. 나중에 사람이 DLT 를 보고 재처리해야 하는 상태.
		assertNull(paymentRepository.findByOrderId(order.orderId), "결제 기록이 남으면 안 된다")
		assertEquals(
			OrderStatus.CREATED,
			orderRepository.findByOrderId(order.orderId)?.status,
			"상태는 그대로 CREATED 여야 한다",
		)
	}

	@Test
	fun `결제 거절은 재시도 없이 실패로 확정된다`() {
		// mock 은 100만원을 넘으면 402 로 거절한다.
		val order = orderService.createOrder("customer-declined", amount = 2_000_000)

		await().atMost(Duration.ofSeconds(30)).untilAsserted {
			val payment = paymentRepository.findByOrderId(order.orderId)
			assertNotNull(payment, "거절도 결과이므로 기록은 남아야 한다")
			assertEquals(PaymentStatus.FAILED, payment.status)
			assertTrue(payment.failureReason?.contains("한도 초과") == true, "사유가 남아야 한다")
		}

		val events = consumeMatching(Topics.ORDER_EVENTS, expectedCount = 2) { it.key() == order.orderId }
			.map { jsonMapper.readValue(it.value(), OrderEvent::class.java) }

		assertEquals(
			listOf(OrderEventType.ORDER_CREATED, OrderEventType.PAYMENT_FAILED),
			events.map { it.eventType },
		)

		await().atMost(Duration.ofSeconds(20)).untilAsserted {
			assertEquals(OrderStatus.PAYMENT_FAILED, orderRepository.findByOrderId(order.orderId)?.status)
		}

		// 거절은 예외가 아니므로 DLT 로 가면 안 된다.
		val dlt = consumeMatching(Topics.ORDER_EVENTS_DLT, expectedCount = 1, timeout = Duration.ofSeconds(5)) {
			it.value().contains(order.orderId)
		}
		assertTrue(dlt.isEmpty(), "거절은 DLT 대상이 아니다")
	}
}
