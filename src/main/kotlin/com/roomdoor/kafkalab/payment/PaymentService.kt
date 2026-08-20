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
 *
 * 트랜잭션이 하나 더 있다. [reserve] 는 PG 호출 **전에** 따로 커밋된다.
 * 결과 확정과 같은 트랜잭션에 두면 커밋이 PG 응답 뒤로 밀려 예약의 의미가 사라진다.
 */
@Service
class PaymentService(
	private val paymentRepository: PaymentRepository,
	private val outboxWriter: OutboxWriter,
) {

	/**
	 * 결제 자리를 먼저 잡는다. `uk_payments_open_order` 위반이 나면 다른 쪽이 이미 잡은 것이므로
	 * 호출자는 PG 를 부르지 않고 물러나야 한다.
	 *
	 * `saveAndFlush` 여야 한다. `save` 만 하면 INSERT 가 커밋 시점까지 미뤄져 제약 위반이
	 * 이 메서드 밖에서 터진다 — 그때는 이미 PG 를 부른 뒤다.
	 */
	@Transactional
	fun reserve(event: OrderEvent): Payment = paymentRepository.saveAndFlush(
		Payment(
			paymentId = UUID.randomUUID().toString(),
			orderId = event.orderId,
			amount = event.amount,
			status = PaymentStatus.PENDING,
		)
	)

	/**
	 * PG 호출이 예외로 끝났을 때 예약을 되돌린다. 붙잡은 채로 두면 Kafka 재시도가
	 * 자기가 남긴 예약에 막혀 결제가 영영 안 된다.
	 */
	@Transactional
	fun releaseReservation(payment: Payment) = paymentRepository.delete(payment)

	@Transactional
	fun completePayment(payment: Payment, event: OrderEvent, transactionId: String): Payment {
		payment.status = PaymentStatus.COMPLETED
		payment.pgTransactionId = transactionId
		paymentRepository.save(payment)

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
	fun declinePayment(payment: Payment, event: OrderEvent, reason: String): Payment {
		// FAILED 로 내려가면 인덱스 밖으로 빠진다. 그래서 나중에 다시 시도할 수 있다.
		payment.status = PaymentStatus.FAILED
		payment.failureReason = reason
		paymentRepository.save(payment)

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
