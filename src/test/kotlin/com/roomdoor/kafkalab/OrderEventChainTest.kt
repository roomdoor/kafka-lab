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
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * 이벤트 체인 전체를 확인한다.
 *
 * 주문 생성 → OrderCreated 발행 → 결제 컨슈머가 외부 PG 호출 → PaymentCompleted 발행 → 주문 상태 갱신.
 *
 * 여기서 증명하려는 핵심은 **같은 orderId 를 키로 쓴 두 이벤트가 같은 파티션에 발생 순서대로 쌓인다**는 것이다.
 * 이 순서가 깨지면 상태 갱신 컨슈머가 존재하지 않는 주문을 갱신하려 든다.
 */
class OrderEventChainTest : IntegrationTestBase() {

	@Autowired
	private lateinit var orderService: OrderService

	@Autowired
	private lateinit var orderRepository: OrderRepository

	@Autowired
	private lateinit var paymentRepository: PaymentRepository

	@Test
	fun `주문 생성부터 결제 완료까지 이벤트가 한 파티션에 순서대로 흐른다`() {
		val order = orderService.createOrder("customer-chain", amount = 25_000)

		// 결제가 끝날 때까지 기다린다. 아웃박스 폴링(500ms) + 외부 호출이 걸린다.
		await().atMost(Duration.ofSeconds(30)).untilAsserted {
			val payment = paymentRepository.findByOrderId(order.orderId)
			assertNotNull(payment, "결제 기록이 DB 에 남아야 한다")
			assertEquals(PaymentStatus.COMPLETED, payment.status)
			assertNotNull(payment.pgTransactionId, "PG 거래번호가 저장돼야 대사가 가능하다")
		}

		val events = consumeMatching(Topics.ORDER_EVENTS, expectedCount = 2) {
			it.key() == order.orderId
		}.map { record -> record to jsonMapper.readValue(record.value(), OrderEvent::class.java) }

		assertEquals(2, events.size, "OrderCreated 와 PaymentCompleted 두 건이어야 한다")
		assertEquals(
			listOf(OrderEventType.ORDER_CREATED, OrderEventType.PAYMENT_COMPLETED),
			events.map { it.second.eventType },
			"발생 순서대로 도착해야 한다",
		)
		assertEquals(
			1,
			events.map { it.first.partition() }.distinct().size,
			"같은 키를 썼으므로 한 파티션에만 들어가야 한다",
		)
		assertTrue(
			events[0].first.offset() < events[1].first.offset(),
			"파티션 안에서 오프셋이 증가해야 한다",
		)

		// 상태 갱신은 결제 서비스가 아니라 별도 컨슈머 그룹이 이벤트를 받아 처리한다.
		await().atMost(Duration.ofSeconds(20)).untilAsserted {
			assertEquals(OrderStatus.PAID, orderRepository.findByOrderId(order.orderId)?.status)
		}
	}

	@Test
	fun `알림 요청은 별도 토픽으로 나가고 고객 ID 를 키로 쓴다`() {
		val customerId = "customer-notify-${System.nanoTime()}"
		val order = orderService.createOrder(customerId, amount = 15_000)

		// 주문 접수 알림 + 결제 완료 알림
		val notifications = consumeMatching(Topics.NOTIFICATION_REQUESTED, expectedCount = 2) {
			it.key() == customerId
		}

		assertEquals(2, notifications.size, "주문 접수와 결제 완료 두 번 알림이 나가야 한다")
		assertTrue(notifications.all { it.value().contains(order.orderId) })
		assertEquals(
			1,
			notifications.map { it.partition() }.distinct().size,
			"한 고객의 알림은 한 파티션에 모여야 한다",
		)
	}
}
