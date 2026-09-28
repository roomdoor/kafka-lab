package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.dlt.DeadLetterConsumer
import com.roomdoor.kafkalab.dlt.FailedEvent
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
import org.springframework.jdbc.core.JdbcTemplate
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

	@Autowired
	private lateinit var jdbcTemplate: JdbcTemplate

	@Test
	fun `영구 DB 오류는 2차 DLT 로 보내지 않고 건너뛴다`() {
		// 스키마 불일치 같은 영구 오류를 트리거로 흉내 낸다. raise exception 은 SQLState P0001 이라
		// 유니크 위반(DataIntegrityViolationException)으로 잡히지 않고, 일시 장애도 아니다. 재시도해도 같은 실패다.
		jdbcTemplate.execute(
			"""
			create or replace function reject_dlt_poison() returns trigger as $$
			begin
				if new.message_key like 'dlt-poison-%' then raise exception 'poison'; end if;
				return new;
			end $$ language plpgsql
			""".trimIndent(),
		)
		jdbcTemplate.execute("drop trigger if exists reject_dlt_poison on failed_events")
		jdbcTemplate.execute("create trigger reject_dlt_poison before insert on failed_events for each row execute function reject_dlt_poison()")

		try {
			// order.events.DLT 가 아니라 이쪽에 넣는다. 다른 테스트가 order.events.DLT 를 읽으며 자기 메시지를 찾는다.
			val topic = Topics.NOTIFICATION_REQUESTED_DLT
			kafkaTemplate.send(topic, "dlt-poison-${UUID.randomUUID()}", "{}").get()
			val next = kafkaTemplate.send(topic, "dlt-next-${UUID.randomUUID()}", "{}").get().recordMetadata
			val partition = TopicPartition(next.topic(), next.partition())

			// 뒤따른 정상 레코드까지 커밋됐다 = 파티션이 멈추지 않았다.
			await().atMost(Duration.ofSeconds(30)).until {
				committedOffset(DeadLetterConsumer.GROUP_ID, partition) > next.offset()
			}

			val topics = AdminClient.create(mapOf<String, Any>(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers))
				.use { it.listTopics().names().get() }
			assertFalse("$topic.DLT" in topics, "DLT 리스너 실패가 2차 DLT 로 발행됐다")
		} finally {
			jdbcTemplate.execute("drop trigger if exists reject_dlt_poison on failed_events")
		}
	}

	@Test
	fun `null 값 DLT 레코드도 실패를 기록한다`() {
		val key = "dlt-tombstone-${UUID.randomUUID()}"
		kafkaTemplate.send(Topics.NOTIFICATION_REQUESTED_DLT, key, null).get()

		val recorded = awaitRecorded { it.messageKey == key }
		assertEquals("", recorded.payload)
	}

	@Test
	fun `문자열로 적힌 원본 좌표 헤더를 읽는다`() {
		// kafka-ui 등으로 재발행하면 4/8바이트 정수 대신 문자열이 온다. 다른 테스트의 진짜 실패와 겹치지 않게 큰 오프셋을 쓴다.
		val payload = """{"marker":"${UUID.randomUUID()}"}"""
		val record = ProducerRecord<String, String>(Topics.ORDER_EVENTS_DLT, "dlt-string-header", payload)
		record.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, Topics.ORDER_EVENTS.toByteArray())
		record.headers().add(KafkaHeaders.DLT_ORIGINAL_PARTITION, "2".toByteArray())
		record.headers().add(KafkaHeaders.DLT_ORIGINAL_OFFSET, "4200000042".toByteArray())
		kafkaTemplate.send(record).get()

		val recorded = awaitRecorded { it.payload == payload }
		assertEquals(Topics.ORDER_EVENTS, recorded.originalTopic)
		assertEquals(2, recorded.originalPartition)
		assertEquals(4_200_000_042, recorded.originalOffset)
	}

	@Test
	fun `읽을 수 없는 좌표 헤더면 세 좌표 모두 DLT 레코드 자신의 것을 쓴다`() {
		val payload = """{"marker":"${UUID.randomUUID()}"}"""
		val record = ProducerRecord<String, String>(Topics.ORDER_EVENTS_DLT, "dlt-broken-header", payload)
		record.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, Topics.ORDER_EVENTS.toByteArray())
		record.headers().add(KafkaHeaders.DLT_ORIGINAL_PARTITION, "abc".toByteArray())
		record.headers().add(KafkaHeaders.DLT_ORIGINAL_OFFSET, "42".toByteArray())
		val sent = kafkaTemplate.send(record).get().recordMetadata

		val recorded = awaitRecorded { it.payload == payload }
		// 원본 토픽 헤더는 읽혔어도 섞지 않는다. 섞으면 order.events 의 엉뚱한 메시지를 가리킨다.
		assertEquals(sent.topic(), recorded.originalTopic)
		assertEquals(sent.partition(), recorded.originalPartition)
		assertEquals(sent.offset(), recorded.originalOffset)
	}

	@Test
	fun `빈 원본 토픽 헤더면 세 좌표 모두 DLT 레코드 자신의 것을 쓴다`() {
		val payload = """{"marker":"${UUID.randomUUID()}"}"""
		val record = ProducerRecord<String, String>(Topics.ORDER_EVENTS_DLT, "dlt-blank-topic", payload)
		record.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, " ".toByteArray())
		record.headers().add(KafkaHeaders.DLT_ORIGINAL_PARTITION, "2".toByteArray())
		record.headers().add(KafkaHeaders.DLT_ORIGINAL_OFFSET, "4200000043".toByteArray())
		val sent = kafkaTemplate.send(record).get().recordMetadata

		val recorded = awaitRecorded { it.payload == payload }
		assertEquals(sent.topic(), recorded.originalTopic)
		assertEquals(sent.partition(), recorded.originalPartition)
		assertEquals(sent.offset(), recorded.originalOffset)
	}

	private fun awaitRecorded(predicate: (FailedEvent) -> Boolean): FailedEvent {
		var found: FailedEvent? = null
		await().atMost(Duration.ofSeconds(20)).untilAsserted {
			found = failedEventRepository.findByStatusOrderByIdDesc(FailedEventStatus.PENDING).firstOrNull(predicate)
			assertNotNull(found, "실패가 failed_events 에 기록돼야 한다")
		}
		return found!!
	}
}
