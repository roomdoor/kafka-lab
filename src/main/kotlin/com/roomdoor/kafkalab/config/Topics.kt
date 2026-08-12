package com.roomdoor.kafkalab.config

object Topics {

	/**
	 * 주문 생명주기 이벤트를 **한 토픽에 모은다**. OrderCreated / PaymentCompleted / PaymentFailed 가 모두 여기 들어간다.
	 *
	 * 왜 타입별로 토픽을 나누지 않았나:
	 * Kafka 의 순서 보장은 (토픽 + 파티션) 단위다. 토픽을 나누면 같은 주문의 이벤트라도
	 * "생성" 과 "결제완료" 사이의 순서가 보장되지 않는다. 앞 이벤트가 만든 상태 위에 뒤 이벤트가 얹히는
	 * 관계라면 한 토픽에 같은 키로 넣어야 한다.
	 *
	 * 파티션 키는 orderId. 한 주문의 이벤트는 한 파티션에 발생 순서대로 쌓인다.
	 */
	const val ORDER_EVENTS = "order.events"

	/**
	 * 알림 요청은 **별도 토픽**으로 뺐다.
	 *
	 * 주문 상태와 순서를 맞출 필요가 없고(늦게 가도 알림은 알림이다), 소비하는 쪽도 다르다.
	 * 파티션 키는 customerId — 한 고객에게 가는 알림끼리는 순서를 지킨다.
	 *
	 * 순서 결합이 필요 없는 관심사를 굳이 한 토픽에 넣으면, 알림 컨슈머가 느려질 때
	 * 관계없는 주문 이벤트까지 같이 밀린다.
	 */
	const val NOTIFICATION_REQUESTED = "notification.requested"

	/**
	 * 재시도를 모두 소진한 메시지가 떨어지는 곳. 토픽마다 하나씩 둔다.
	 * 접미사 `.DLT` 는 스프링의 [org.springframework.kafka.listener.DeadLetterPublishingRecoverer] 기본 규칙이다.
	 */
	const val ORDER_EVENTS_DLT = "$ORDER_EVENTS.DLT"
	const val NOTIFICATION_REQUESTED_DLT = "$NOTIFICATION_REQUESTED.DLT"

	/**
	 * 파티션 수 = 한 컨슈머 그룹이 가질 수 있는 최대 병렬도.
	 * 파티션보다 컨슈머가 많으면 남는 컨슈머는 놀게 된다.
	 * 늘리는 건 되지만 줄이는 건 안 되므로 처음 잡을 때 여유를 둔다.
	 */
	const val PARTITIONS = 3
}
