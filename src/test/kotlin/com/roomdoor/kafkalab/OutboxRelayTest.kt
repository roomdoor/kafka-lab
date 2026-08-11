package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.order.OrderRepository
import com.roomdoor.kafkalab.order.OrderService
import com.roomdoor.kafkalab.outbox.OutboxRepository
import com.roomdoor.kafkalab.support.IntegrationTestBase
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 트랜잭셔널 아웃박스가 실제로 동작하는지 확인한다.
 *
 * 핵심 주장: 업무 데이터가 커밋되면 이벤트도 반드시 남고, 롤백되면 이벤트도 남지 않는다.
 * 발행은 그 뒤에 릴레이가 책임진다.
 */
class OutboxRelayTest : IntegrationTestBase() {

	@Autowired
	private lateinit var orderService: OrderService

	@Autowired
	private lateinit var orderRepository: OrderRepository

	@Autowired
	private lateinit var outboxRepository: OutboxRepository

	@Test
	fun `주문을 저장하면 아웃박스에 이벤트가 함께 남고 릴레이가 Kafka 로 발행한다`() {
		val customerId = "customer-${UUID.randomUUID()}"

		val order = orderService.createOrder(customerId, amount = 25_000)

		val outboxEvent = outboxRepository.findAll().firstOrNull { it.aggregateId == order.orderId }
		assertNotNull(outboxEvent, "주문과 같은 트랜잭션에서 아웃박스 행이 만들어져야 한다")
		assertEquals(Topics.ORDER_CREATED, outboxEvent.topic)

		// 릴레이는 스케줄러라 즉시 실행되지 않는다. 발행 완료 표시가 채워질 때까지 기다린다.
		await().atMost(Duration.ofSeconds(15)).untilAsserted {
			val reloaded = outboxRepository.findById(outboxEvent.id!!).orElseThrow()
			assertNotNull(reloaded.publishedAt, "발행에 성공하면 publishedAt 이 채워져야 한다")
		}

		val published = consumeMatching(Topics.ORDER_CREATED, expectedCount = 1) {
			it.key() == order.orderId
		}.firstOrNull()
		assertNotNull(published, "아웃박스에 남은 이벤트가 Kafka 로 나가야 한다")
		assertTrue(published.value().contains(customerId))
	}

	@Test
	fun `주문 저장이 실패하면 이벤트도 남지 않는다`() {
		val before = outboxRepository.count()

		assertFailsWith<IllegalArgumentException> {
			orderService.createOrder("customer-invalid", amount = 0)
		}

		assertEquals(before, outboxRepository.count(), "실패한 요청은 아웃박스에 흔적을 남기면 안 된다")
		assertNull(orderRepository.findByOrderId("customer-invalid"))
	}
}
