package com.roomdoor.kafkalab.payment

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.order.OrderEvent
import com.roomdoor.kafkalab.order.OrderEventType
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import tools.jackson.databind.json.JsonMapper

/**
 * 결제 서비스. `order.events` 를 구독해 주문 생성 이벤트에만 반응한다.
 *
 * 처리 순서가 중요하다.
 * 1. 관심 없는 이벤트 걸러내기 — 이 컨슈머는 자기가 발행한 PAYMENT_COMPLETED 도 같은 토픽에서 다시 읽는다
 * 2. 이미 결제된 주문인지 조회 — 빠른 길일 뿐이다. 경합은 이 검사로 못 막는다
 * 3. **PG 호출 전에** PENDING 행으로 자리 예약 — 여기서 DB 제약이 승자를 정한다
 * 4. **트랜잭션 밖에서** 외부 게이트웨이 호출 — 느린 HTTP 를 DB 커넥션 잡은 채로 하지 않는다
 * 5. 예약한 행을 결과로 확정 ([PaymentService])
 *
 * 3번이 없으면 조회와 저장 사이가 통째로 열린다. 두 스레드가 나란히 2번을 통과해 결제를 두 번 하고,
 * 그제서야 한 건이 유니크 제약에 걸린다 — 돈은 이미 나간 뒤다. 조회는 경합을 막지 못한다.
 *
 * ponytail: PG 호출 도중 프로세스가 죽으면 PENDING 행이 남아 그 주문이 막힌다.
 * 학습용이라 청소 배치는 두지 않았다. 필요해지면 일정 시간 지난 PENDING 을 FAILED 로 내리는 스케줄러를 붙인다.
 */
@Component
class PaymentConsumer(
	private val jsonMapper: JsonMapper,
	private val paymentGatewayClient: PaymentGatewayClient,
	private val paymentService: PaymentService,
	private val paymentRepository: PaymentRepository,
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

		// 이미 끝난 주문이면 예약 INSERT 를 시도할 것도 없다. 예외를 던지고 잡는 것보다 조회가 싸다.
		// 여기서 COMPLETED 만 보는 이유는 아래 예약이 PENDING 을 맡기 때문이다.
		if (paymentRepository.countByOrderIdAndStatus(event.orderId, PaymentStatus.COMPLETED) > 0) {
			log.warn("이미 결제된 주문, 건너뜀: eventId=${event.eventId} orderId=${event.orderId}")
			ack.acknowledge()
			return
		}

		// 자리를 먼저 잡는다. 위의 조회는 빠른 길일 뿐 경합을 막지 못한다 —
		// 같은 주문이 다른 파티션에 실려 오거나 리밸런스 중이면 두 스레드가 나란히 통과한다.
		// PG 호출 **전에** DB 제약으로 승자를 정해야 진 쪽이 결제를 못 한다.
		val payment = try {
			paymentService.reserve(event)
		} catch (e: DuplicateKeyException) {
			// 유니크 위반만 경합이다. 다른 쪽이 먼저 잡았고 그쪽이 끝까지 처리하므로 여기서는 물러난다.
			//
			// DataIntegrityViolationException 전체를 잡으면 안 된다. CHECK 제약 위반 같은 스키마 오류까지
			// 경합으로 착각해 조용히 ack 하고, 그 주문은 결제도 DLT 도 없이 사라진다.
			// 나머지 위반은 그대로 던져 재시도와 DLT 로 보낸다.
			log.warn("처리 중인 주문, 건너뜀: eventId=${event.eventId} orderId=${event.orderId} (${e.javaClass.simpleName})")
			ack.acknowledge()
			return
		}

		// 트랜잭션 밖. 여기서 던지는 PaymentGatewayException 은 재시도 대상이라 잡지 않는다.
		val result = try {
			paymentGatewayClient.requestPayment(
				// 멱등키로 이벤트 ID 를 쓴다. 타임아웃 후 재시도해도 게이트웨이가 결제를 또 하지 않는다.
				idempotencyKey = event.eventId,
				orderId = event.orderId,
				amount = event.amount,
			)
		} catch (e: Exception) {
			// 재시도가 성공해버리면 DLT 로 안 가고, 그러면 실패했다는 사실이 어디에도 안 남는다.
			// 원인(타임아웃인지 5xx 인지)은 여기서만 알 수 있으므로 여기서 남긴다.
			log.warn("PG 호출 실패, 예약 해제 후 재시도로 넘김: orderId=${event.orderId} (${e.javaClass.simpleName}: ${e.message})")
			// 예약을 쥔 채 재시도로 넘기면 다음 배달이 자기가 남긴 예약에 막혀 영영 결제되지 않는다.
			paymentService.releaseReservation(payment)
			throw e
		}

		when (result) {
			is PaymentGatewayResult.Approved -> {
				paymentService.completePayment(payment, event, result.transactionId)
				log.info("결제 완료: orderId=${event.orderId} amount=${event.amount} partition=${record.partition()} offset=${record.offset()}")
			}

			// 거절은 예외가 아니다. 재시도해도 결과가 같으므로 실패로 확정하고 다음으로 넘어간다.
			is PaymentGatewayResult.Declined -> {
				paymentService.declinePayment(payment, event, result.reason)
				log.warn("결제 거절: orderId=${event.orderId} 사유=${result.reason}")
			}
		}

		// 처리가 끝난 뒤에만 커밋한다. 이 줄에 도달하기 전에 예외가 나면 오프셋은 그대로 남아 재시도된다.
		ack.acknowledge()
	}

	companion object {
		const val GROUP_ID = "payment-service"
	}
}
