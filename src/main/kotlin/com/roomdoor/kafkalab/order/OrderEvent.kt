package com.roomdoor.kafkalab.order

import java.time.Instant

/**
 * 토픽에 실려 나가는 계약(contract). 컨슈머가 다른 팀 서비스일 수도 있다고 가정하고 다룬다.
 *
 * 필드 추가는 안전하지만(모르는 필드는 무시하면 그만), 필드 삭제와 타입 변경은 컨슈머를 깨뜨린다.
 * eventId 는 컨슈머가 중복 수신을 걸러내는 열쇠다 — Kafka 는 at-least-once 라 같은 메시지가 두 번 올 수 있다.
 */
data class OrderEvent(
	val eventId: String,
	val orderId: String,
	val customerId: String,
	val amount: Long,
	val occurredAt: Instant,
)
