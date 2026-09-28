package com.roomdoor.kafkalab.dlt

import com.roomdoor.kafkalab.config.Topics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.hibernate.JDBCException
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
		// 기본 팩토리는 실패를 <topic>.DLT 로 보낸다. 여기서는 .DLT.DLT 가 되므로 전용 팩토리를 쓴다.
		containerFactory = "deadLetterListenerContainerFactory",
	)
	fun consume(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
		// DeadLetterPublishingRecoverer 가 원본 정보와 예외를 헤더에 담아준다.
		val headerTopic = record.headerAsString(KafkaHeaders.DLT_ORIGINAL_TOPIC)
		val headerPartition = record.headerAsInt(KafkaHeaders.DLT_ORIGINAL_PARTITION)
		val headerOffset = record.headerAsLong(KafkaHeaders.DLT_ORIGINAL_OFFSET)
		// 세 좌표는 한 메시지에서 와야 한다. 원본 토픽에 DLT 레코드의 파티션·오프셋을 섞으면 원본 토픽의 엉뚱한 메시지를
		// 가리키고, 진짜 실패 기록과 유니크 키가 부딪칠 수도 있다. 하나라도 못 읽으면 DLT 레코드 자신의 좌표를 쓴다.
		val (originalTopic, originalPartition, originalOffset) =
			// 빈 토픽 헤더도 못 읽은 것으로 본다. 빈 이름으로 기록하면 재발행할 곳이 없다.
			if (!headerTopic.isNullOrBlank() && headerPartition != null && headerOffset != null) {
				Triple(headerTopic, headerPartition, headerOffset)
			} else {
				Triple(record.topic(), record.partition(), record.offset())
			}

		val failed = FailedEvent(
			originalTopic = originalTopic,
			originalPartition = originalPartition,
			originalOffset = originalOffset,
			originalConsumerGroup = record.headerAsString(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP),
			messageKey = record.key(),
			// tombstone(null 값)도 실패는 실패다. null 로 두면 NPE 로 기록이 건너뛰어진다.
			payload = record.value() ?: "",
			// 진짜 원인은 cause 쪽에 있다. 스프링이 리스너 예외를 ListenerExecutionFailedException 으로 감싸기 때문이다.
			exceptionClass = record.headerAsString(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)
				?: record.headerAsString(KafkaHeaders.DLT_EXCEPTION_FQCN),
			exceptionMessage = record.headerAsString(KafkaHeaders.DLT_EXCEPTION_MESSAGE),
		)

		try {
			failedEventRepository.save(failed)
			log.error("처리 실패 기록: topic=$originalTopic partition=$originalPartition offset=$originalOffset group=${failed.originalConsumerGroup} 원인=${failed.exceptionClass}")
		} catch (e: DataIntegrityViolationException) {
			// 유니크 위반(23505)만 중복이다. 다른 위반(CHECK, NOT NULL 등)까지 삼키면 실패 기록이 소리 없이 사라진다.
			// 제약 이름은 Hibernate 가 지역화된 에러 메시지에서 뽑아서 lc_messages 가 영어가 아니면 null 이 된다. SQLState 는 언어와 무관하다.
			// PK 는 identity 라 이 테이블에서 INSERT 가 걸릴 유니크는 uk_failed_events_original_record 하나뿐이다.
			if ((e.cause as? JDBCException)?.sqlState != UNIQUE_VIOLATION) throw e
			// 유니크 제약 위반 = 같은 그룹의 이미 적어둔 실패다. 오프셋을 되감아 DLT 를 다시 읽으면 여기로 온다.
			log.warn("이미 기록된 실패, 건너뜀: topic=$originalTopic partition=$originalPartition offset=$originalOffset group=${failed.originalConsumerGroup}")
		}

		ack.acknowledge()
	}

	private fun ConsumerRecord<String, String>.headerAsString(name: String): String? =
		headers().lastHeader(name)?.value()?.toString(Charsets.UTF_8)

	// 스프링은 파티션·오프셋을 4/8바이트 정수로 쓰지만, kafka-ui 나 스프링이 아닌 프로듀서로 재발행하면 "2" 같은 문자열이 온다.
	// 문자열부터 본다. 숫자 글자만으로 된 바이너리 정수는 파티션 8억·오프셋 3×10^18 이상이라 현실에 없다.
	// 둘 다 아니면 null — 여기서 던지면 기록 자체가 건너뛰어진다.
	private fun ConsumerRecord<String, String>.headerAsInt(name: String): Int? {
		val bytes = headers().lastHeader(name)?.value() ?: return null
		return bytes.toString(Charsets.UTF_8).toIntOrNull()
			?: bytes.takeIf { it.size == Int.SIZE_BYTES }?.let { ByteBuffer.wrap(it).int }
	}

	private fun ConsumerRecord<String, String>.headerAsLong(name: String): Long? {
		val bytes = headers().lastHeader(name)?.value() ?: return null
		return bytes.toString(Charsets.UTF_8).toLongOrNull()
			?: bytes.takeIf { it.size == Long.SIZE_BYTES }?.let { ByteBuffer.wrap(it).long }
	}

	companion object {
		const val GROUP_ID = "dead-letter-inspector"

		/** PostgreSQL unique_violation */
		private const val UNIQUE_VIOLATION = "23505"
	}
}
