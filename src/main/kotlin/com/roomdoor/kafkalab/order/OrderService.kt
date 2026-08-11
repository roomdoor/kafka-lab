package com.roomdoor.kafkalab.order

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.outbox.OutboxEvent
import com.roomdoor.kafkalab.outbox.OutboxRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.json.JsonMapper
import java.time.Instant
import java.util.UUID

@Service
class OrderService(
	private val orderRepository: OrderRepository,
	private val outboxRepository: OutboxRepository,
	private val jsonMapper: JsonMapper,
) {

	/**
	 * 주문 저장과 이벤트 기록을 한 트랜잭션으로 묶는다.
	 *
	 * 여기서 kafkaTemplate.send() 를 직접 부르면 안 되는 이유:
	 * DB 트랜잭션과 Kafka 전송은 서로 다른 시스템이라 함께 커밋되지 않는다.
	 * 전송에 성공한 직후 트랜잭션이 롤백되면 "주문은 없는데 주문 생성 이벤트만 나간" 상태가 되고,
	 * 반대로 커밋 직후 애플리케이션이 죽으면 이벤트만 유실된다.
	 * 그래서 같은 DB 에 이벤트를 적어두고(아웃박스), 발행은 [com.roomdoor.kafkalab.outbox.OutboxRelay] 에 맡긴다.
	 */
	@Transactional
	fun createOrder(customerId: String, amount: Long): Order {
		require(amount > 0) { "주문 금액은 0보다 커야 한다: $amount" }

		val order = orderRepository.save(
			Order(orderId = UUID.randomUUID().toString(), customerId = customerId, amount = amount)
		)

		val event = OrderEvent(
			eventId = UUID.randomUUID().toString(),
			orderId = order.orderId,
			customerId = order.customerId,
			amount = order.amount,
			occurredAt = Instant.now(),
		)

		outboxRepository.save(
			OutboxEvent(
				// 파티션 키를 orderId 로 잡아 같은 주문의 이벤트가 한 파티션에 순서대로 들어가게 한다.
				// Kafka 의 순서 보장은 파티션 단위이지 토픽 단위가 아니다.
				aggregateId = order.orderId,
				topic = Topics.ORDER_CREATED,
				payload = jsonMapper.writeValueAsString(event),
			)
		)

		return order
	}
}
