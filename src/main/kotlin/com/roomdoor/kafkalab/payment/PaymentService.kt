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
	 * 결제 자리를 먼저 잡는다. `uk_payments_open_order` 위반이 나면 이미 누가 잡은 것이다.
	 * 잡은 쪽이 다른 eventId 면 호출자는 PG 를 부르지 않고 재시도로 넘기고, 같은 eventId 면 앞선 시도의 예약이니 이어받는다.
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
			eventId = event.eventId,
			status = PaymentStatus.PENDING,
		)
	)

	/**
	 * PG 가 결제하지 않은 게 **확실할 때만** 예약을 되돌린다. 그래야 다른 eventId 의 재시도도 결제할 수 있다.
	 * 결과를 모르는데 풀면 그 사이 다른 eventId 가 새 멱등키로 결제해 이중 결제가 난다.
	 *
	 * PENDING 일 때만 지운다. 들고 있는 엔티티는 예약 시점의 사본이라, 그 사이 같은 eventId 의 다른 배달이
	 * 확정한 COMPLETED 행을 그대로 delete 하면 결제 기록이 사라진다.
	 */
	@Transactional
	fun releaseReservation(payment: Payment) {
		paymentRepository.deletePending(payment.id!!)
	}

	@Transactional
	fun completePayment(payment: Payment, event: OrderEvent, transactionId: String): Payment {
		// 같은 eventId 의 배달 둘이 동시에 예약을 이어받으면 둘 다 여기 온다(PG 호출 중 리밸런스로 재배달 등).
		// 엔티티를 save 하면 둘 다 덮어쓰고 완료 이벤트·알림이 두 번 나간다. PENDING 일 때만 바꾸는 조건부 UPDATE 로
		// 전이를 한 번으로 만든다. 행 락 때문에 늦은 쪽은 앞쪽 커밋을 기다렸다가 0건을 받고, 아웃박스를 쓰지 않는다.
		if (paymentRepository.finalizePending(payment.id!!, PaymentStatus.COMPLETED, transactionId, null) == 0) {
			return alreadyFinalized(payment) { it.status == PaymentStatus.COMPLETED && it.pgTransactionId == transactionId }
		}
		payment.status = PaymentStatus.COMPLETED
		payment.pgTransactionId = transactionId

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
		// 전이를 한 번으로 만드는 이유는 completePayment 와 같다.
		if (paymentRepository.finalizePending(payment.id!!, PaymentStatus.FAILED, null, reason) == 0) {
			return alreadyFinalized(payment) { it.status == PaymentStatus.FAILED && it.failureReason == reason }
		}
		payment.status = PaymentStatus.FAILED
		payment.failureReason = reason

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

	/**
	 * 조건부 UPDATE 가 0건일 때. 같은 결과로 먼저 확정된 거라면 할 일이 없다.
	 * 그게 아니면(행이 지워졌거나 다른 결과로 확정됐다) PG 결과가 어디에도 안 남는다. 조용히 넘기지 않고 던져 DLT 로 보낸다.
	 */
	private fun alreadyFinalized(payment: Payment, sameResult: (Payment) -> Boolean): Payment {
		val current = paymentRepository.findById(payment.id!!).orElse(null)
		check(current != null && sameResult(current)) {
			"PG 결과를 확정할 예약이 없다: orderId=${payment.orderId} paymentId=${payment.paymentId} 현재=$current"
		}
		return current
	}

	private fun notification(event: OrderEvent, message: String) = NotificationEvent(
		eventId = UUID.randomUUID().toString(),
		customerId = event.customerId,
		orderId = event.orderId,
		message = message,
		occurredAt = Instant.now(),
	)
}
