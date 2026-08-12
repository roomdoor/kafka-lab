package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.idempotency.ProcessedEventRepository
import com.roomdoor.kafkalab.order.OrderEvent
import com.roomdoor.kafkalab.order.OrderEventType
import com.roomdoor.kafkalab.payment.PaymentConsumer
import com.roomdoor.kafkalab.payment.PaymentRepository
import com.roomdoor.kafkalab.support.IntegrationTestBase
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 같은 이벤트가 두 번 와도 결제는 한 번만 되는지 확인한다.
 *
 * Kafka 는 at-least-once 라 중복은 예외 상황이 아니라 **정상 동작**이다.
 * 아웃박스 릴레이가 발행 직후 죽거나, 컨슈머가 커밋 전에 죽으면 같은 메시지가 다시 온다.
 *
 * 예전에는 이 기록이 메모리에 있어서 재시작하면 사라졌다. 지금은 processed_events 테이블에 남는다.
 */
class PaymentIdempotencyTest : IntegrationTestBase() {

	@Autowired
	private lateinit var paymentRepository: PaymentRepository

	@Autowired
	private lateinit var processedEventRepository: ProcessedEventRepository

	@Test
	fun `같은 이벤트를 두 번 발행해도 결제는 한 번만 처리된다`() {
		val orderId = "order-dup-${UUID.randomUUID()}"
		val event = OrderEvent(
			// eventId 가 같다는 게 핵심이다. 이게 중복을 판별하는 유일한 근거다.
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

		assertTrue(
			processedEventRepository.existsByConsumerGroupAndEventId(PaymentConsumer.GROUP_ID, event.eventId),
			"처리 이력이 DB 에 남아야 재시작 후에도 중복을 막을 수 있다",
		)
	}
}
