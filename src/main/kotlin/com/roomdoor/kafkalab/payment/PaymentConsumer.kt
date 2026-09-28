package com.roomdoor.kafkalab.payment

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.order.OrderEvent
import com.roomdoor.kafkalab.order.OrderEventType
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.hibernate.exception.ConstraintViolationException
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
 * 2. 이미 결제된 주문인지 조회 — 빠른 길일 뿐이다. 경합은 이 검사로 못 막는다
 * 3. **PG 호출 전에** PENDING 행으로 자리 예약 — 여기서 DB 제약이 승자를 정한다
 * 4. **트랜잭션 밖에서** 외부 게이트웨이 호출 — 느린 HTTP 를 DB 커넥션 잡은 채로 하지 않는다
 * 5. 예약한 행을 결과로 확정 ([PaymentService])
 *
 * 3번이 없으면 조회와 저장 사이가 통째로 열린다. 두 스레드가 나란히 2번을 통과해 결제를 두 번 하고,
 * 그제서야 한 건이 유니크 제약에 걸린다 — 돈은 이미 나간 뒤다. 조회는 경합을 막지 못한다.
 *
 * 예약에는 eventId(= PG 멱등키)가 적혀 있다. 그래서 재배달(재시도, 커밋 전에 죽은 뒤 다시 읽기)은
 * 자기 예약을 알아보고 이어받는다. 예약은 "이 주문은 이 멱등키로 결제한다" 는 약속이 된다.
 *
 * ponytail: 결과를 모른 채 재시도가 소진되면 PENDING 이 남아 DLT 로 간다. 다른 eventId 는 그 주문을 결제하지 못한다.
 * DLT 재처리(같은 eventId)는 예약을 이어받아 끝낸다. 그 밖에는 사람이 PG 와 대사해 정리한다. 자동 대사 배치는 두지 않았다.
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
		} catch (e: DataIntegrityViolationException) {
			// 예약 인덱스 위반만 경합이다. JPA 경유라 DuplicateKeyException 이 아니라 이 예외로 오므로 제약 이름으로 가른다.
			// CHECK 제약 위반 같은 스키마 오류까지 경합으로 착각하면 조용히 ack 하고, 그 주문은 결제도 DLT 도 없이 사라진다.
			// 나머지 위반은 그대로 던져 재시도와 DLT 로 보낸다.
			if ((e.cause as? ConstraintViolationException)?.constraintName != OPEN_ORDER_INDEX) throw e

			// 막았다고 다 남은 아니다. 같은 eventId 의 PENDING 이면 **앞선 배달이 남긴 내 예약**이다 —
			// PG 호출이 결과 모름으로 끝났거나, 승인 뒤 결과 확정이 롤백된 경우다. 이걸 경합으로 보고 물러나면
			// 돈은 나갔는데 결제는 PENDING 에 갇히고, ack 됐으니 DLT 에도 안 남는다.
			// 같은 멱등키로 PG 를 다시 부르면 PG 가 처음 결과를 재생하므로 이어받아도 이중 결제가 없다.
			val mine = paymentRepository.findByOrderIdAndStatus(event.orderId, PaymentStatus.PENDING)
				?.takeIf { it.eventId == event.eventId }
			if (mine == null) {
				// 다른 eventId 가 잡았다. 그쪽이 끝까지 처리하므로 여기서는 물러난다.
				log.warn("처리 중인 주문, 건너뜀: eventId=${event.eventId} orderId=${event.orderId}")
				ack.acknowledge()
				return
			}
			log.info("앞선 시도의 예약을 이어받음: eventId=${event.eventId} orderId=${event.orderId}")
			mine
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
			//
			// 예약은 PG 가 결제하지 않은 게 확실할 때만 푼다. 결과를 모르는데 풀면 그 사이(백오프 중, DLT 로 간 뒤)
			// 같은 주문이 다른 eventId 로 들어와 새 멱등키로 결제한다 — 이중 결제다.
			// 쥔 채로 넘겨도 재시도는 같은 eventId 라 위에서 자기 예약을 이어받는다.
			val release = e is PaymentGatewayException && !e.outcomeUnknown
			log.warn("PG 호출 실패, ${if (release) "예약 해제" else "예약 유지"} 후 재시도로 넘김: orderId=${event.orderId} (${e.javaClass.simpleName}: ${e.message})")
			if (release) paymentService.releaseReservation(payment)
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

		/** `schema.sql` 의 부분 유니크 인덱스. 이 이름의 위반만 경합으로 본다. */
		private const val OPEN_ORDER_INDEX = "uk_payments_open_order"
	}
}
