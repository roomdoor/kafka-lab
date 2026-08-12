package com.roomdoor.kafkalab.payment

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.idempotency.ProcessedEventRepository
import com.roomdoor.kafkalab.order.OrderEvent
import com.roomdoor.kafkalab.order.OrderEventType
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import tools.jackson.databind.json.JsonMapper

/**
 * 결제 서비스. `order.events` 를 구독해 주문 생성 이벤트에만 반응한다.
 *
 * 처리 순서가 중요하다.
 * 1. 관심 없는 이벤트 걸러내기 — 이 컨슈머는 자기가 발행한 PAYMENT_COMPLETED 도 같은 토픽에서 다시 읽는다
 * 2. 멱등 검사 — 이미 처리한 이벤트면 결제하지 않는다
 * 3. **트랜잭션 밖에서** 외부 게이트웨이 호출 — 느린 HTTP 를 DB 커넥션 잡은 채로 하지 않는다
 * 4. 결과를 한 트랜잭션으로 커밋 ([PaymentService])
 */
@Component
class PaymentConsumer(
	private val jsonMapper: JsonMapper,
	private val paymentGatewayClient: PaymentGatewayClient,
	private val paymentService: PaymentService,
	private val processedEventRepository: ProcessedEventRepository,
) {

	private val log = LoggerFactory.getLogger(javaClass)

	@KafkaListener(topics = [Topics.ORDER_EVENTS], groupId = GROUP_ID)
	fun consume(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
		val event = jsonMapper.readValue(record.value(), OrderEvent::class.java)

		// 한 토픽에 여러 이벤트 타입을 담은 대가. 관심 없는 것도 일단 다 읽고 여기서 버린다.
		// 토픽을 타입별로 나눴다면 이 분기가 없는 대신 순서 보장을 잃었을 것이다.
		if (event.eventType != OrderEventType.ORDER_CREATED) {
			ack.acknowledge()
			return
		}

		// 결제는 두 번 하면 안 되는 작업이다. Kafka 는 같은 메시지를 두 번 줄 수 있다.
		if (processedEventRepository.existsByConsumerGroupAndEventId(GROUP_ID, event.eventId)) {
			log.warn("중복 수신, 건너뜀: eventId=${event.eventId} orderId=${event.orderId}")
			ack.acknowledge()
			return
		}

		// 트랜잭션 밖. 여기서 던지는 PaymentGatewayException 은 재시도 대상이라 잡지 않는다.
		val result = paymentGatewayClient.requestPayment(
			// 멱등키로 이벤트 ID 를 쓴다. 타임아웃 후 재시도해도 게이트웨이가 결제를 또 하지 않는다.
			idempotencyKey = event.eventId,
			orderId = event.orderId,
			amount = event.amount,
		)

		try {
			when (result) {
				is PaymentGatewayResult.Approved -> {
					paymentService.completePayment(event, result.transactionId)
					log.info("결제 완료: orderId=${event.orderId} amount=${event.amount} partition=${record.partition()} offset=${record.offset()}")
				}

				// 거절은 예외가 아니다. 재시도해도 결과가 같으므로 실패로 확정하고 다음으로 넘어간다.
				is PaymentGatewayResult.Declined -> {
					paymentService.declinePayment(event, result.reason)
					log.warn("결제 거절: orderId=${event.orderId} 사유=${result.reason}")
				}
			}
		} catch (e: DataIntegrityViolationException) {
			// processed_events 유니크 제약 위반 = 다른 스레드가 방금 같은 이벤트를 처리했다.
			// 위의 exists 검사는 경합을 막지 못한다. 최종 방어선은 언제나 DB 제약이다.
			log.warn("동시 처리 감지, 건너뜀: eventId=${event.eventId} (${e.javaClass.simpleName})")
		}

		// 처리가 끝난 뒤에만 커밋한다. 이 줄에 도달하기 전에 예외가 나면 오프셋은 그대로 남아 재시도된다.
		ack.acknowledge()
	}

	companion object {
		const val GROUP_ID = "payment-service"
	}
}
