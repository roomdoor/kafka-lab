package com.roomdoor.kafkalab.outbox

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.domain.Limit
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.time.Instant

/**
 * 아웃박스 테이블을 주기적으로 훑어 미발행 이벤트를 Kafka 로 내보낸다.
 *
 * 이 구조가 보장하는 건 at-least-once 다. exactly-once 가 아니다.
 * 전송에는 성공했는데 publishedAt 을 기록하기 전에 프로세스가 죽으면 다음 폴링에서 같은 이벤트를 다시 보낸다.
 * 그래서 중복 제거 책임은 컨슈머가 진다 — OrderEvent.eventId 가 그 용도다.
 */
@Component
class OutboxRelay(
	private val outboxRepository: OutboxRepository,
	private val kafkaTemplate: KafkaTemplate<String, String>,
	@param:Value("\${app.outbox.batch-size}") private val batchSize: Int,
) {

	private val log = LoggerFactory.getLogger(javaClass)

	@Scheduled(fixedDelayString = "\${app.outbox.poll-interval-ms}")
	@Transactional
	fun publishPending() {
		val pending = outboxRepository.findByPublishedAtIsNullOrderByIdAsc(Limit.of(batchSize))
		if (pending.isEmpty()) return

		for (event in pending) {
			try {
				// get() 으로 브로커 응답을 기다린다. 기다리지 않으면 실패한 전송까지 발행 성공으로 기록된다.
				val result = kafkaTemplate.send(event.topic, event.aggregateId, event.payload).get()
				event.publishedAt = Instant.now()
				log.debug("발행 완료: id=${event.id} key=${event.aggregateId} partition=${result.recordMetadata.partition()} offset=${result.recordMetadata.offset()}")
			} catch (e: Exception) {
				// 한 건이 막히면 뒤의 이벤트도 보내지 않고 멈춘다. 순서를 지키기 위해서다.
				// 여기서 계속 진행하면 같은 주문의 두 번째 이벤트가 첫 번째보다 먼저 도착할 수 있다.
				log.error("발행 실패, 이번 주기 중단: id=${event.id} 원인=${e.message}")
				break
			}
		}
	}
}
