package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.dlt.FailedEventRepository
import com.roomdoor.kafkalab.support.IntegrationTestBase
import org.apache.kafka.clients.producer.ProducerRecord
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.support.KafkaHeaders
import java.nio.ByteBuffer
import java.time.Duration
import java.util.UUID
import kotlin.random.Random
import kotlin.test.assertEquals

/**
 * `order.events` 는 두 그룹(payment-service, order-status-service)이 읽는다. 한 레코드가 두 그룹 모두에서
 * 실패하면 DLT 에는 원본 좌표가 같고 그룹만 다른 레코드가 2건 쌓인다. 둘 다 기록돼야 한다.
 */
class DeadLetterConsumerTest : IntegrationTestBase() {

	@Autowired
	private lateinit var failedEventRepository: FailedEventRepository

	@Test
	fun `같은 원본 레코드라도 실패한 그룹이 다르면 각각 기록되고, 같은 그룹의 재수신은 한 건으로 합쳐진다`() {
		// 다른 테스트가 만든 실패와 좌표가 겹치지 않게 오프셋을 크게 잡는다.
		val offset = Random.nextLong(1_000_000_000, Long.MAX_VALUE / 2)
		val marker = UUID.randomUUID().toString()

		sendToDlt(offset, "payment-service", marker)
		sendToDlt(offset, "order-status-service", marker)
		// DLT 오프셋을 되감아 다시 읽은 상황. 행이 늘면 안 된다.
		sendToDlt(offset, "payment-service", marker)
		// 그룹 헤더가 없는 레코드도 재수신은 한 건으로 합쳐져야 한다 (schema.sql 의 NULLS NOT DISTINCT).
		sendToDlt(offset, null, marker)
		sendToDlt(offset, null, marker)
		// DLT 는 파티션 1개라 순서대로 처리된다. 이게 기록되면 앞의 레코드도 모두 처리된 것이다.
		sendToDlt(offset + 1, "payment-service", marker)

		await().atMost(Duration.ofSeconds(20)).untilAsserted {
			val recorded = failedEventRepository.findAll().filter { it.payload == marker }
			assertEquals(1, recorded.count { it.originalOffset == offset + 1 })
			assertEquals(
				listOf(null, "order-status-service", "payment-service"),
				recorded.filter { it.originalOffset == offset }.map { it.originalConsumerGroup }.sortedBy { it ?: "" },
			)
		}
	}

	private fun sendToDlt(offset: Long, group: String?, payload: String) {
		val record = ProducerRecord<String, String>(Topics.ORDER_EVENTS_DLT, "key", payload)
		// DeadLetterPublishingRecoverer 가 붙이는 헤더를 흉내 낸다. 파티션·오프셋은 4/8바이트 정수다.
		record.headers()
			.add(KafkaHeaders.DLT_ORIGINAL_TOPIC, Topics.ORDER_EVENTS.toByteArray())
			.add(KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer.allocate(4).putInt(0).array())
			.add(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(8).putLong(offset).array())
		group?.let { record.headers().add(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP, it.toByteArray()) }
		kafkaTemplate.send(record).get()
	}
}
