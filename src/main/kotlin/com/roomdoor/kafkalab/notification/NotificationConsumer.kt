package com.roomdoor.kafkalab.notification

import com.roomdoor.kafkalab.config.Topics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import tools.jackson.databind.json.JsonMapper

/**
 * 알림 서비스. **전용 토픽** `notification.requested` 만 본다.
 *
 * 주문 이벤트를 직접 구독할 수도 있었지만 그러지 않았다.
 * 그렇게 하면 알림 서비스가 주문 도메인의 이벤트 타입을 전부 알아야 하고,
 * 주문 쪽에 새 이벤트가 생길 때마다 알림 쪽도 따라 고쳐야 한다.
 * 지금은 "이 문구를 보내라" 는 요청만 받으므로 주문이 어떻게 바뀌든 영향이 없다.
 *
 * 파티션 키가 customerId 라, 한 고객에게 가는 알림끼리는 순서가 지켜진다.
 * 서로 다른 고객의 알림 순서는 보장되지 않지만 그럴 필요도 없다.
 */
@Component
class NotificationConsumer(
	private val jsonMapper: JsonMapper,
) {

	private val log = LoggerFactory.getLogger(javaClass)

	@KafkaListener(topics = [Topics.NOTIFICATION_REQUESTED], groupId = GROUP_ID)
	fun consume(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
		val event = jsonMapper.readValue(record.value(), NotificationEvent::class.java)

		log.info("알림 발송: customerId=${event.customerId} 내용=${event.message} partition=${record.partition()} offset=${record.offset()} thread=${Thread.currentThread().name}")

		ack.acknowledge()
	}

	companion object {
		const val GROUP_ID = "notification-service"
	}
}
