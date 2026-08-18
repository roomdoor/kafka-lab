package com.roomdoor.kafkalab.dlt

import com.roomdoor.kafkalab.config.Topics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.stereotype.Component
import java.nio.ByteBuffer

/**
 * DLT 를 읽어 실패를 **DB 에 기록**한다.
 *
 * 로그만 남기면 조사할 때 쓸 수가 없다. 로그는 회전되면 사라지고, 집계도 검색도 안 되고,
 * "이 실패를 이미 처리했는지" 를 표시할 자리가 없다. 그래서 [FailedEvent] 테이블에 적는다.
 *
 * 여기서 자동 재처리는 하지 않는다. 원인을 모른 채 되돌리면 같은 실패를 반복하고,
 * 최악의 경우 DLT → 원본 → DLT 로 무한히 돈다. 사람이 원인을 고친 뒤 재발행하는 게 기본이다.
 */
@Component
class DeadLetterConsumer(
	private val failedEventRepository: FailedEventRepository,
) {

	private val log = LoggerFactory.getLogger(javaClass)

	// DLT 는 파티션이 1개라 컨슈머 스레드를 1개만 띄운다. 더 띄워봐야 놀기만 한다.
	// 토픽별 DLT 를 한 리스너로 모아 본다 — 실패는 어느 흐름에서 났든 사람이 봐야 하는 건 같다.
	@KafkaListener(
		topics = [Topics.ORDER_EVENTS_DLT, Topics.NOTIFICATION_REQUESTED_DLT],
		groupId = GROUP_ID,
		concurrency = "1",
	)
	fun consume(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
		// DeadLetterPublishingRecoverer 가 원본 정보와 예외를 헤더에 담아준다.
		val originalTopic = record.headerAsString(KafkaHeaders.DLT_ORIGINAL_TOPIC) ?: record.topic()
		// 파티션·오프셋 헤더는 문자열이 아니라 4/8바이트 정수다. 그냥 String 으로 읽으면 깨진 글자가 나온다.
		val originalPartition = record.headerAsInt(KafkaHeaders.DLT_ORIGINAL_PARTITION) ?: record.partition()
		val originalOffset = record.headerAsLong(KafkaHeaders.DLT_ORIGINAL_OFFSET) ?: record.offset()

		val failed = FailedEvent(
			originalTopic = originalTopic,
			originalPartition = originalPartition,
			originalOffset = originalOffset,
			originalConsumerGroup = record.headerAsString(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP),
			messageKey = record.key(),
			payload = record.value(),
			// 진짜 원인은 cause 쪽에 있다. 스프링이 리스너 예외를 ListenerExecutionFailedException 으로 감싸기 때문이다.
			exceptionClass = record.headerAsString(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)
				?: record.headerAsString(KafkaHeaders.DLT_EXCEPTION_FQCN),
			exceptionMessage = record.headerAsString(KafkaHeaders.DLT_EXCEPTION_MESSAGE),
		)

		try {
			failedEventRepository.save(failed)
			log.error("처리 실패 기록: topic=$originalTopic partition=$originalPartition offset=$originalOffset group=${failed.originalConsumerGroup} 원인=${failed.exceptionClass}")
		} catch (e: DataIntegrityViolationException) {
			// 유니크 제약 위반 = 이미 적어둔 실패다. 오프셋을 되감아 DLT 를 다시 읽으면 여기로 온다.
			log.warn("이미 기록된 실패, 건너뜀: topic=$originalTopic partition=$originalPartition offset=$originalOffset")
		}

		ack.acknowledge()
	}

	private fun ConsumerRecord<String, String>.headerAsString(name: String): String? =
		headers().lastHeader(name)?.value()?.toString(Charsets.UTF_8)

	private fun ConsumerRecord<String, String>.headerAsInt(name: String): Int? =
		headers().lastHeader(name)?.value()?.let { ByteBuffer.wrap(it).int }

	private fun ConsumerRecord<String, String>.headerAsLong(name: String): Long? =
		headers().lastHeader(name)?.value()?.let { ByteBuffer.wrap(it).long }

	companion object {
		const val GROUP_ID = "dead-letter-inspector"
	}
}
