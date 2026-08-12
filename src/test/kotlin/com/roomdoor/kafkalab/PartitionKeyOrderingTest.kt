package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.order.OrderEvent
import com.roomdoor.kafkalab.order.OrderEventType
import com.roomdoor.kafkalab.support.IntegrationTestBase
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Kafka 의 순서 보장 범위를 확인한다.
 *
 * 보장되는 것: 같은 (토픽 + 파티션) 안에서의 순서.
 * 보장되지 않는 것: 토픽 전체의 순서. 그래서 순서가 중요한 이벤트는 같은 키로 묶어 같은 파티션에 보내야 한다.
 *
 * 여기서는 PAYMENT_COMPLETED 타입으로 보낸다. 결제 컨슈머가 건너뛰는 타입이라
 * 순서만 확인하는 이 테스트가 외부 게이트웨이를 건드리지 않는다.
 */
class PartitionKeyOrderingTest : IntegrationTestBase() {

	@Test
	fun `같은 키로 보낸 메시지는 같은 파티션에 순서대로 쌓인다`() {
		val key = "order-${UUID.randomUUID()}"
		val payloads = (1..5).map { seq -> jsonMapper.writeValueAsString(event(key, amount = seq.toLong())) }

		payloads.forEach { kafkaTemplate.send(Topics.ORDER_EVENTS, key, it).get() }

		val mine = consumeMatching(Topics.ORDER_EVENTS, expectedCount = 5) { it.key() == key }

		assertEquals(5, mine.size, "보낸 5건이 모두 도착해야 한다")
		assertEquals(1, mine.map { it.partition() }.distinct().size, "같은 키는 한 파티션에만 들어가야 한다")
		assertEquals(
			(1..5).toList(),
			mine.map { jsonMapper.readValue(it.value(), OrderEvent::class.java).amount.toInt() },
			"파티션 안에서는 보낸 순서가 유지되어야 한다",
		)

		// 오프셋도 단조 증가해야 한다 — 순서 보장의 실체가 이것이다.
		assertTrue(mine.zipWithNext().all { (a, b) -> a.offset() < b.offset() })
	}

	@Test
	fun `키가 다르면 여러 파티션에 나뉘어 들어간다`() {
		val marker = UUID.randomUUID().toString()
		repeat(30) { index ->
			val key = "$marker-$index"
			kafkaTemplate.send(Topics.ORDER_EVENTS, key, jsonMapper.writeValueAsString(event(key))).get()
		}

		val partitions = consumeMatching(Topics.ORDER_EVENTS, expectedCount = 30) {
			it.key()?.startsWith(marker) == true
		}.map { it.partition() }.distinct()

		assertEquals(Topics.PARTITIONS, partitions.size, "키 30개면 파티션 3개에 모두 분배되어야 한다")
	}

	private fun event(orderId: String, amount: Long = 1_000) = OrderEvent(
		eventId = UUID.randomUUID().toString(),
		// 결제 컨슈머가 무시하는 타입이라 이 테스트는 외부 호출을 유발하지 않는다.
		eventType = OrderEventType.PAYMENT_COMPLETED,
		orderId = orderId,
		customerId = "customer-ordering",
		amount = amount,
		occurredAt = Instant.now(),
	)
}
