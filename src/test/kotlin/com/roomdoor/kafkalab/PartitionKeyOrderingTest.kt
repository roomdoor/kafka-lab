package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.support.IntegrationTestBase
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Kafka 의 순서 보장 범위를 확인한다.
 *
 * 보장되는 것: 같은 파티션 안에서의 순서.
 * 보장되지 않는 것: 토픽 전체의 순서. 그래서 순서가 중요한 이벤트는 같은 키로 묶어 같은 파티션에 보내야 한다.
 */
class PartitionKeyOrderingTest : IntegrationTestBase() {

	@Test
	fun `같은 키로 보낸 메시지는 같은 파티션에 순서대로 쌓인다`() {
		val key = "order-${UUID.randomUUID()}"
		val payloads = (1..5).map { """{"seq":$it,"key":"$key"}""" }

		payloads.forEach { kafkaTemplate.send(Topics.ORDER_CREATED, key, it).get() }

		val mine = consumeMatching(Topics.ORDER_CREATED, expectedCount = 5) { it.key() == key }

		assertEquals(5, mine.size, "보낸 5건이 모두 도착해야 한다")
		assertEquals(1, mine.map { it.partition() }.distinct().size, "같은 키는 한 파티션에만 들어가야 한다")
		assertEquals(payloads, mine.map { it.value() }, "파티션 안에서는 보낸 순서가 유지되어야 한다")

		// 오프셋도 단조 증가해야 한다 — 순서 보장의 실체가 이것이다.
		assertTrue(mine.zipWithNext().all { (a, b) -> a.offset() < b.offset() })
	}

	@Test
	fun `키가 다르면 여러 파티션에 나뉘어 들어간다`() {
		val marker = UUID.randomUUID().toString()
		repeat(30) { kafkaTemplate.send(Topics.ORDER_CREATED, "$marker-$it", """{"marker":"$marker"}""").get() }

		val partitions = consumeMatching(Topics.ORDER_CREATED, expectedCount = 30) {
			it.key()?.startsWith(marker) == true
		}.map { it.partition() }.distinct()

		assertEquals(Topics.ORDER_PARTITIONS, partitions.size, "키 30개면 파티션 3개에 모두 분배되어야 한다")
	}
}
