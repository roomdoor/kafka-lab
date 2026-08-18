package com.roomdoor.kafkalab.order

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.notification.NotificationEvent
import com.roomdoor.kafkalab.outbox.OutboxWriter
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.UUID

@Service
class OrderService(
	private val orderRepository: OrderRepository,
	private val outboxWriter: OutboxWriter,
) {

	/**
	 * 주문 저장과 이벤트 기록을 한 트랜잭션으로 묶는다.
	 *
	 * 여기서 kafkaTemplate.send() 를 직접 부르면 안 되는 이유:
	 * DB 트랜잭션과 Kafka 전송은 서로 다른 시스템이라 함께 커밋되지 않는다.
	 * 전송에 성공한 직후 트랜잭션이 롤백되면 "주문은 없는데 주문 생성 이벤트만 나간" 상태가 되고,
	 * 반대로 커밋 직후 애플리케이션이 죽으면 이벤트만 유실된다.
	 * 그래서 같은 DB 에 이벤트를 적어두고(아웃박스), 발행은 [com.roomdoor.kafkalab.outbox.OutboxRelay] 에 맡긴다.
	 *
	 * 이벤트를 **두 토픽**에 적는다. 둘 다 같은 트랜잭션이라 하나만 나가는 일은 없다.
	 */
	@Transactional
	fun createOrder(customerId: String, amount: Long): Order {
		require(amount > 0) { "주문 금액은 0보다 커야 한다: $amount" }

		val order = orderRepository.save(Order(orderId = UUID.randomUUID().toString(), customerId = customerId, amount = amount))

		outboxWriter.write(
			topic = Topics.ORDER_EVENTS,
			// 파티션 키를 orderId 로 잡아 이 주문의 후속 이벤트(결제 완료/실패)가 같은 파티션에 이어 쌓이게 한다.
			// Kafka 의 순서 보장은 파티션 단위이지 토픽 단위가 아니다.
			partitionKey = order.orderId,
			payload = OrderEvent(
				eventId = UUID.randomUUID().toString(),
				eventType = OrderEventType.ORDER_CREATED,
				orderId = order.orderId,
				customerId = order.customerId,
				amount = order.amount,
				occurredAt = Instant.now(),
			),
		)

		outboxWriter.write(
			topic = Topics.NOTIFICATION_REQUESTED,
			partitionKey = order.customerId,
			payload = NotificationEvent(
				eventId = UUID.randomUUID().toString(),
				customerId = order.customerId,
				orderId = order.orderId,
				message = "주문 ${order.orderId} 이 접수되었습니다.",
				occurredAt = Instant.now(),
			),
		)

		return order
	}
}
