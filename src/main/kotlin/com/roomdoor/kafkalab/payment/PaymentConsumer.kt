package com.roomdoor.kafkalab.payment

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.order.OrderEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import tools.jackson.databind.json.JsonMapper
import java.util.concurrent.ConcurrentHashMap

/**
 * 결제 서비스 역할. order.created 를 소비한다.
 *
 * 실습용 실패 트리거:
 * - customerId 가 "FAIL" → 매번 예외. 재시도 3번 후 DLT 로 간다.
 * - amount 가 0 이하 → IllegalArgumentException. 재시도 없이 즉시 DLT (에러 핸들러의 not-retryable 목록).
 */
@Component
class PaymentConsumer(
	private val jsonMapper: JsonMapper,
) {

	private val log = LoggerFactory.getLogger(javaClass)

	/**
	 * 처리한 eventId 기록. Kafka 는 at-least-once 라 같은 메시지가 두 번 올 수 있고,
	 * 결제처럼 두 번 실행되면 안 되는 작업은 반드시 이 검사가 있어야 한다.
	 *
	 * 여기서는 메모리에 두지만 재시작하면 사라진다.
	 * 실무에서는 처리 이력 테이블에 eventId 유니크 제약을 걸고, 중복 삽입 예외를 "이미 처리함" 으로 해석한다.
	 */
	private val processedEventIds = ConcurrentHashMap.newKeySet<String>()

	@KafkaListener(topics = [Topics.ORDER_CREATED], groupId = GROUP_ID)
	fun consume(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
		val event = jsonMapper.readValue(record.value(), OrderEvent::class.java)

		if (event.eventId in processedEventIds) {
			log.warn("중복 수신, 건너뜀: eventId=${event.eventId}")
			ack.acknowledge()
			return
		}

		require(event.amount > 0) { "결제 금액이 잘못됨: ${event.amount}" }
		check(event.customerId != "FAIL") { "결제 게이트웨이 응답 없음: orderId=${event.orderId}" }

		log.info("결제 처리: orderId=${event.orderId} amount=${event.amount} partition=${record.partition()} offset=${record.offset()}")

		// 처리에 성공한 뒤에 기록한다. 이 줄이 위쪽에 있으면 재시도 때 자기 자신을 '중복' 으로 오인해
		// 실패한 메시지가 조용히 성공 처리되고 DLT 로도 가지 않는다.
		processedEventIds.add(event.eventId)

		// 처리가 끝난 뒤에만 커밋한다. 이 줄에 도달하기 전에 예외가 나면 오프셋은 그대로 남는다.
		ack.acknowledge()
	}

	/** 테스트에서 어떤 주문이 실제로 처리됐는지 확인하는 용도. */
	fun processedCount(): Int = processedEventIds.size

	companion object {
		const val GROUP_ID = "payment-service"
	}
}
