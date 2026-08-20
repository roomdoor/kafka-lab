package com.roomdoor.kafkalab.payment

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.dlt.FailedEvent
import com.roomdoor.kafkalab.dlt.FailedEventRepository
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
	private val paymentRepository: PaymentRepository,
	private val failedEventRepository: FailedEventRepository,
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
		// 처리했다는 사실을 따로 적지 않는다. payments 에 성공한 결제가 있다는 것 자체가 증거다.
		// 판별 기준이 eventId 가 아니라 orderId 라서, 같은 주문이 다른 eventId 로 두 번 발행돼도 막힌다.
		//
		// COMPLETED 만 세는 이유는 `uk_payments_completed_order` 와 기준을 맞추기 위해서다.
		// 거절까지 세면 한도를 올린 뒤 DLT 에서 재발행해도 영영 건너뛰어 주문이 실패로 굳는다.
		// 대가로 같은 거절이 두 번 처리될 수 있다 — 알림이 두 번 나가지만 돈은 움직이지 않는다.
		if (paymentRepository.countByOrderIdAndStatus(event.orderId, PaymentStatus.COMPLETED) > 0) {
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
			// uk_payments_completed_order 위반 = 다른 스레드가 방금 같은 주문을 결제했다.
			// 위의 count 검사는 경합을 막지 못한다. 최종 방어선은 언제나 DB 제약이다.
			//
			// 여기 도달했다는 건 PG 호출이 이미 끝났다는 뜻이다. 멱등키는 eventId 라서,
			// 두 메시지의 eventId 가 같으면 PG 가 막아준다. 하지만 **같은 주문이 다른 eventId 로**
			// 두 번 발행된 경우 PG 는 서로 다른 결제로 보고 실제로 두 번 청구한다.
			// 로그만 남기면 아무도 모른 채 지나간다. 대사할 수 있도록 실패 이력에 적는다.
			recordSuspectedDoubleCharge(record, event, e)
		}

		// 처리가 끝난 뒤에만 커밋한다. 이 줄에 도달하기 전에 예외가 나면 오프셋은 그대로 남아 재시도된다.
		ack.acknowledge()
	}

	/**
	 * 사람이 봐야 하는 상태다. DLT 로 간 실패와 같은 테이블에 적어 조회 경로를 하나로 둔다.
	 * 이 메시지는 재발행하면 안 된다 — 결제는 이미 됐고, 확인해야 할 것은 PG 쪽 청구 건수다.
	 */
	private fun recordSuspectedDoubleCharge(
		record: ConsumerRecord<String, String>,
		event: OrderEvent,
		cause: DataIntegrityViolationException,
	) {
		log.error("이중 청구 의심: orderId=${event.orderId} eventId=${event.eventId} (${cause.javaClass.simpleName})")

		try {
			failedEventRepository.save(
				FailedEvent(
					originalTopic = record.topic(),
					originalPartition = record.partition(),
					originalOffset = record.offset(),
					originalConsumerGroup = GROUP_ID,
					messageKey = record.key(),
					payload = record.value(),
					exceptionClass = cause.javaClass.name,
					exceptionMessage = "같은 주문에 성공한 결제가 이미 있다. PG 에 중복 청구가 없는지 대사가 필요하다.",
				)
			)
		} catch (e: DataIntegrityViolationException) {
			// 이미 적어둔 건이다. 오프셋을 되감아 다시 읽으면 여기로 온다.
			log.warn("이미 기록된 이중 청구 의심, 건너뜀: orderId=${event.orderId} (${e.javaClass.simpleName})")
		}
	}

	companion object {
		const val GROUP_ID = "payment-service"
	}
}
