package com.roomdoor.kafkalab.order

import java.time.Instant

enum class OrderEventType {
	ORDER_CREATED,
	PAYMENT_COMPLETED,
	PAYMENT_FAILED,
}

/**
 * `order.events` 토픽에 실려 나가는 계약(contract). 컨슈머가 다른 팀 서비스일 수도 있다고 가정하고 다룬다.
 *
 * 한 클래스에 [eventType] 을 두고 타입별 필드를 nullable 로 뒀다.
 * 타입마다 클래스를 나누고 다형 역직렬화를 붙이는 방법도 있지만, 이 규모에서는 얻는 것보다 설정이 크다.
 *
 * 스키마 진화 규칙:
 * - 필드 **추가**는 안전하다. 모르는 필드는 컨슈머가 무시하면 그만이다.
 * - 필드 **삭제·타입 변경**은 컨슈머를 깨뜨린다. 옛 컨슈머가 아직 그 필드를 읽고 있기 때문이다.
 * - 그래서 nullable 로 추가하고, 모든 컨슈머가 넘어간 뒤에 정리하는 순서를 지킨다.
 *
 * [eventId] 는 컨슈머가 중복 수신을 걸러내는 열쇠다 — Kafka 는 at-least-once 라 같은 메시지가 두 번 올 수 있다.
 */
data class OrderEvent(
	val eventId: String,
	val eventType: OrderEventType,
	val orderId: String,
	val customerId: String,
	val amount: Long,
	val occurredAt: Instant,

	/** PAYMENT_COMPLETED 에만 채워진다. */
	val paymentId: String? = null,

	/** PAYMENT_FAILED 에만 채워진다. */
	val failureReason: String? = null,
)
