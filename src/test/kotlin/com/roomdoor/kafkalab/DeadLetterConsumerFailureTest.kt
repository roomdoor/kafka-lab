package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.dlt.DeadLetterConsumer
import com.roomdoor.kafkalab.support.IntegrationTestBase
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.KafkaHeaders
import java.time.Duration
import java.util.UUID
import kotlin.test.assertFalse

/**
 * DLT 컨슈머가 실패해도 `<topic>.DLT.DLT` 로 보내지 않아야 한다(#6).
 *
 * compose 브로커는 토픽 자동 생성이 꺼져 있어 `.DLT.DLT` 발행이 실패하고, 복구 실패로 DLT 파티션이 멈춘다.
 * Testcontainers 브로커는 자동 생성이라 멈추지는 않는다. 대신 `.DLT.DLT` 토픽이 조용히 생기므로
 * "그 토픽이 생기지 않았는가" 로 같은 버그를 잡는다.
 */
class DeadLetterConsumerFailureTest : IntegrationTestBase() {

	@Test
	fun `DLT 처리 실패는 2차 DLT 로 보내지 않고 건너뛴다`() {
		// 원본 파티션 헤더는 4바이트 정수여야 한다. 1바이트면 읽다가 BufferUnderflowException — 재시도해도 같은 실패다.
		// 값을 null 로 만드는 방법도 있지만, DLT 를 같이 읽는 다른 테스트의 value() 를 깨뜨린다.
		val poison = ProducerRecord<String, String>(Topics.ORDER_EVENTS_DLT, "dlt-poison-${UUID.randomUUID()}", "{}")
		poison.headers().add(KafkaHeaders.DLT_ORIGINAL_PARTITION, byteArrayOf(1))
		kafkaTemplate.send(poison).get()
		val next = kafkaTemplate.send(Topics.ORDER_EVENTS_DLT, "dlt-next-${UUID.randomUUID()}", "{}").get().recordMetadata
		val partition = TopicPartition(next.topic(), next.partition())

		// 뒤따른 정상 레코드까지 커밋됐다 = 파티션이 멈추지 않았다.
		await().atMost(Duration.ofSeconds(30)).until {
			committedOffset(DeadLetterConsumer.GROUP_ID, partition) > next.offset()
		}

		val topics = AdminClient.create(mapOf<String, Any>(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers))
			.use { it.listTopics().names().get() }
		assertFalse("${Topics.ORDER_EVENTS_DLT}.DLT" in topics, "DLT 리스너 실패가 2차 DLT 로 발행됐다")
	}
}
