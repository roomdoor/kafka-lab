package com.roomdoor.kafkalab.notification

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.order.OrderEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import tools.jackson.databind.json.JsonMapper

/**
 * 알림 서비스 역할. PaymentConsumer 와 **같은 토픽**을 **다른 컨슈머 그룹**으로 읽는다.
 *
 * 그룹이 다르면 오프셋도 따로 관리되므로 같은 메시지를 양쪽이 모두 받는다(fan-out).
 * 그룹이 같았다면 파티션이 나뉘어 한쪽만 받았을 것이다 — 이 차이가 Kafka 를 큐가 아닌 로그로 만드는 지점이다.
 */
@Component
class NotificationConsumer(
	private val jsonMapper: JsonMapper,
) {

	private val log = LoggerFactory.getLogger(javaClass)

	@KafkaListener(topics = [Topics.ORDER_CREATED], groupId = GROUP_ID)
	fun consume(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
		val event = jsonMapper.readValue(record.value(), OrderEvent::class.java)

		log.info(
			"알림 발송: orderId=${event.orderId} customerId=${event.customerId} " +
				"partition=${record.partition()} offset=${record.offset()} thread=${Thread.currentThread().name}"
		)

		ack.acknowledge()
	}

	companion object {
		const val GROUP_ID = "notification-service"
	}
}
