package com.roomdoor.kafkalab.payment

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.notification.NotificationEvent
import com.roomdoor.kafkalab.order.OrderEvent
import com.roomdoor.kafkalab.order.OrderEventType
import com.roomdoor.kafkalab.outbox.OutboxWriter
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.UUID

/**
 * 결제 결과를 확정하는 트랜잭션 경계.
 *
 * 외부 게이트웨이 호출은 **여기 들어오지 않는다**. 느린 HTTP 호출을 트랜잭션 안에 두면
 * DB 커넥션을 응답 대기 내내 붙잡아 커넥션 풀이 마른다. 호출은 [PaymentConsumer] 가 밖에서 끝내고,
 * 그 결과만 이 안에서 한 번에 커밋한다.
 *
 * 한 트랜잭션으로 묶는 것 세 가지:
 * 1. payments — 결제 결과. 이 행의 존재 자체가 "이미 처리했다" 는 증거라 별도 표시가 필요 없다
 * 2. outbox → order.events — 결제 완료/실패 이벤트
 * 3. outbox → notification.requested — 고객 알림 요청
 *
 * 이 중 하나라도 따로 커밋되면 정합성이 깨진다. 예를 들어 2번만 나가고 1번이 롤백되면
 * "결제 완료" 이벤트는 흘러갔는데 결제 기록은 없는 상태가 된다.
 */
@Service
class PaymentService(
	private val paymentRepository: PaymentRepository,
	private val outboxWriter: OutboxWriter,
) {

	@Transactional
	fun completePayment(event: OrderEvent, transactionId: String): Payment {
		val payment = paymentRepository.save(
			Payment(
				paymentId = UUID.randomUUID().toString(),
				orderId = event.orderId,
				amount = event.amount,
				status = PaymentStatus.COMPLETED,
				pgTransactionId = transactionId,
			)
		)

		outboxWriter.write(
			topic = Topics.ORDER_EVENTS,
			// orderId 를 키로 써야 OrderCreated 와 같은 파티션에 이어 붙는다.
			partitionKey = event.orderId,
			payload = event.copy(
				eventId = UUID.randomUUID().toString(),
				eventType = OrderEventType.PAYMENT_COMPLETED,
				occurredAt = Instant.now(),
				paymentId = payment.paymentId,
			),
		)

		outboxWriter.write(
			topic = Topics.NOTIFICATION_REQUESTED,
			// 알림은 고객 단위로 순서를 지키면 충분하다.
			partitionKey = event.customerId,
			payload = notification(event, "주문 ${event.orderId} 결제가 완료되었습니다."),
		)

		return payment
	}

	@Transactional
	fun declinePayment(event: OrderEvent, reason: String): Payment {
		val payment = paymentRepository.save(
			Payment(
				paymentId = UUID.randomUUID().toString(),
				orderId = event.orderId,
				amount = event.amount,
				status = PaymentStatus.FAILED,
				failureReason = reason,
			)
		)

		outboxWriter.write(
			topic = Topics.ORDER_EVENTS,
			partitionKey = event.orderId,
			payload = event.copy(
				eventId = UUID.randomUUID().toString(),
				eventType = OrderEventType.PAYMENT_FAILED,
				occurredAt = Instant.now(),
				failureReason = reason,
			),
		)

		outboxWriter.write(
			topic = Topics.NOTIFICATION_REQUESTED,
			partitionKey = event.customerId,
			payload = notification(event, "주문 ${event.orderId} 결제에 실패했습니다: $reason"),
		)

		return payment
	}

	private fun notification(event: OrderEvent, message: String) = NotificationEvent(
		eventId = UUID.randomUUID().toString(),
		customerId = event.customerId,
		orderId = event.orderId,
		message = message,
		occurredAt = Instant.now(),
	)
}
