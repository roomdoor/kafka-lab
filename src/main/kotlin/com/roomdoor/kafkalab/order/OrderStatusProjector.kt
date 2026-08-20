package com.roomdoor.kafkalab.order

import com.roomdoor.kafkalab.config.Topics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.json.JsonMapper

/**
 * 주문 상태를 이벤트로 갱신한다. `order.events` 를 [com.roomdoor.kafkalab.payment.PaymentConsumer] 와
 * **다른 그룹**으로 읽으므로 같은 이벤트를 양쪽이 모두 받는다(fan-out).
 *
 * 결제 서비스가 orders 테이블을 직접 UPDATE 하지 않는다는 게 요점이다.
 * 서비스가 분리되면 남의 테이블을 만질 수 없고, 이벤트만이 유일한 연결 통로가 된다.
 *
 * **이 컨슈머에는 멱등 검사가 없다.** 같은 상태를 두 번 써도 결과가 같기 때문이다(자연 멱등).
 * 결제처럼 "실행할 때마다 돈이 나가는" 작업만 중복 검사가 필요하다.
 * 중복 방어는 공짜가 아니므로 필요한 곳에만 둔다.
 *
 * 다만 상태 전이에는 한 방향 규칙이 하나 있다. PAID 는 PAYMENT_FAILED 로 내려가지 않는다.
 * 자연 멱등은 "같은 이벤트가 두 번" 을 감당할 뿐, "서로 다른 결론이 둘" 을 감당하지는 못한다.
 *
 * 그리고 이 컨슈머는 순서에 의존한다. PAYMENT_COMPLETED 가 ORDER_CREATED 보다 먼저 오면
 * 갱신할 주문이 없다. 같은 orderId 를 키로 써서 한 파티션에 순서대로 넣는 이유가 이것이다.
 */
@Component
class OrderStatusProjector(
	private val jsonMapper: JsonMapper,
	private val orderRepository: OrderRepository,
) {

	private val log = LoggerFactory.getLogger(javaClass)

	@KafkaListener(topics = [Topics.ORDER_EVENTS], groupId = GROUP_ID)
	@Transactional
	fun consume(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
		val event = jsonMapper.readValue(record.value(), OrderEvent::class.java)

		val newStatus = when (event.eventType) {
			// 주문 생성 시 이미 CREATED 로 저장돼 있다.
			OrderEventType.ORDER_CREATED -> null
			OrderEventType.PAYMENT_COMPLETED -> OrderStatus.PAID
			OrderEventType.PAYMENT_FAILED -> OrderStatus.PAYMENT_FAILED
		}

		if (newStatus != null) {
			val order = orderRepository.findByOrderId(event.orderId)
			if (order == null) {
				// 순서가 뒤집혔거나 다른 환경의 이벤트다. 재시도해도 생기지 않으므로 넘긴다.
				log.warn("주문을 찾을 수 없어 상태 갱신 생략: orderId=${event.orderId}")
			} else if (order.status == OrderStatus.PAID && newStatus == OrderStatus.PAYMENT_FAILED) {
				// 중복 처리 경합에서 한쪽은 승인, 한쪽은 거절로 갈리면 두 이벤트가 모두 나간다.
				// 돈이 나간 사실이 우선이다. PAID 를 실패로 덮으면 결제된 주문이 실패로 보인다.
				log.error("결제 완료된 주문에 실패 이벤트 도착, 무시: orderId=${event.orderId} eventId=${event.eventId}")
			} else {
				order.status = newStatus
				log.info("주문 상태 갱신: orderId=${event.orderId} → $newStatus partition=${record.partition()} offset=${record.offset()}")
			}
		}

		ack.acknowledge()
	}

	companion object {
		const val GROUP_ID = "order-status-service"
	}
}
