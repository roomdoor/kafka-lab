package com.roomdoor.kafkalab.notification

import java.time.Instant

/**
 * `notification.requested` 토픽 페이로드.
 *
 * 주문 이벤트와 달리 "무슨 일이 있었다" 가 아니라 "이 문구를 보내달라" 는 **요청**이다.
 * 알림 서비스가 주문 도메인을 몰라도 되도록 발행하는 쪽에서 문구까지 만들어 넘긴다.
 */
data class NotificationEvent(
	val eventId: String,
	val customerId: String,
	val orderId: String,
	val message: String,
	val occurredAt: Instant,
)
