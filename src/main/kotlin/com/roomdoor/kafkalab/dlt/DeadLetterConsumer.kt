package com.roomdoor.kafkalab.dlt

import com.roomdoor.kafkalab.config.Topics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.stereotype.Component
import java.nio.ByteBuffer

/**
 * DLT 를 읽어 실패 원인을 남긴다.
 *
 * 실무에서 DLT 는 보통 자동 재처리하지 않는다. 원인을 모른 채 되돌리면 같은 실패를 반복할 뿐이다.
 * 알림을 띄워 사람이 확인하고, 코드나 데이터를 고친 뒤 수동으로 원본 토픽에 다시 넣는 흐름이 일반적이다.
 */
@Component
class DeadLetterConsumer {

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
		val originalTopic = record.headerAsString(KafkaHeaders.DLT_ORIGINAL_TOPIC)
		// 파티션·오프셋 헤더는 문자열이 아니라 4/8바이트 정수다. 그냥 String 으로 읽으면 깨진 글자가 나온다.
		val originalPartition = record.headerAsInt(KafkaHeaders.DLT_ORIGINAL_PARTITION)
		val exceptionMessage = record.headerAsString(KafkaHeaders.DLT_EXCEPTION_MESSAGE)

		log.error("처리 실패 이벤트 도착: originalTopic=$originalTopic originalPartition=$originalPartition 원인=$exceptionMessage payload=${record.value()}")

		ack.acknowledge()
	}

	private fun ConsumerRecord<String, String>.headerAsString(name: String): String =
		headers().lastHeader(name)?.value()?.toString(Charsets.UTF_8) ?: "-"

	private fun ConsumerRecord<String, String>.headerAsInt(name: String): String =
		headers().lastHeader(name)?.value()?.let { ByteBuffer.wrap(it).int.toString() } ?: "-"

	companion object {
		const val GROUP_ID = "dead-letter-inspector"
	}
}
