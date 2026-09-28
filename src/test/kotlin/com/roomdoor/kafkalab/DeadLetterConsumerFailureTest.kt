package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.dlt.DeadLetterConsumer
import com.roomdoor.kafkalab.dlt.FailedEventRepository
import com.roomdoor.kafkalab.dlt.FailedEventStatus
import com.roomdoor.kafkalab.support.IntegrationTestBase
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.support.KafkaHeaders
import java.time.Duration
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

/**
 * DLT 컨슈머가 실패해도 `<topic>.DLT.DLT` 로 보내지 않아야 한다(#6).
 *
 * compose 브로커는 토픽 자동 생성이 꺼져 있어 `.DLT.DLT` 발행이 실패하고, 복구 실패로 DLT 파티션이 멈춘다.
 * Testcontainers 브로커는 자동 생성이라 멈추지는 않는다. 대신 `.DLT.DLT` 토픽이 조용히 생기므로
 * "그 토픽이 생기지 않았는가" 로 같은 버그를 잡는다.
 */
class DeadLetterConsumerFailureTest : IntegrationTestBase() {

	@Autowired
	private lateinit var failedEventRepository: FailedEventRepository

	@Test
	fun `DLT 처리 실패는 2차 DLT 로 보내지 않고 건너뛴다`() {
		// 값이 null(tombstone) 이면 non-null 인 FailedEvent.payload 에 넣다가 NPE 가 난다. 재시도해도 같은 실패다.
		// order.events.DLT 가 아니라 이쪽에 넣는다. 다른 테스트가 order.events.DLT 의 value() 를 읽다가 깨진다.
		val topic = Topics.NOTIFICATION_REQUESTED_DLT
		kafkaTemplate.send(topic, "dlt-poison-${UUID.randomUUID()}", null).get()
		val next = kafkaTemplate.send(topic, "dlt-next-${UUID.randomUUID()}", "{}").get().recordMetadata
		val partition = TopicPartition(next.topic(), next.partition())

		// 뒤따른 정상 레코드까지 커밋됐다 = 파티션이 멈추지 않았다.
		await().atMost(Duration.ofSeconds(30)).until {
			committedOffset(DeadLetterConsumer.GROUP_ID, partition) > next.offset()
		}

		val topics = AdminClient.create(mapOf<String, Any>(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers))
			.use { it.listTopics().names().get() }
		assertFalse("$topic.DLT" in topics, "DLT 리스너 실패가 2차 DLT 로 발행됐다")
	}

	@Test
	fun `원본 좌표 헤더가 문자열이어도 실패를 기록한다`() {
		// kafka-ui 등으로 재발행하면 4바이트 정수 대신 문자열 "2" 가 올 수 있다. 읽다가 던지면 기록이 건너뛰어진다.
		val payload = """{"marker":"${UUID.randomUUID()}"}"""
		val record = ProducerRecord<String, String>(Topics.ORDER_EVENTS_DLT, "dlt-string-header", payload)
		record.headers().add(KafkaHeaders.DLT_ORIGINAL_PARTITION, "2".toByteArray())
		record.headers().add(KafkaHeaders.DLT_ORIGINAL_OFFSET, "42".toByteArray())
		val sent = kafkaTemplate.send(record).get().recordMetadata

		await().atMost(Duration.ofSeconds(20)).untilAsserted {
			val recorded = failedEventRepository.findByStatusOrderByIdDesc(FailedEventStatus.PENDING)
				.firstOrNull { it.payload == payload }
			assertNotNull(recorded, "실패가 failed_events 에 기록돼야 한다")
			// 헤더를 못 읽으면 DLT 레코드 자신의 좌표로 대신한다.
			assertEquals(sent.partition(), recorded.originalPartition)
			assertEquals(sent.offset(), recorded.originalOffset)
		}
	}
}
