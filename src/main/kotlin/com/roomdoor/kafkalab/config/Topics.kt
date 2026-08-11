package com.roomdoor.kafkalab.config

object Topics {

	/** 주문 생성 이벤트. 여러 컨슈머 그룹이 같은 토픽을 각자 소비한다(fan-out). */
	const val ORDER_CREATED = "order.created"

	/**
	 * 재시도를 모두 소진한 메시지가 떨어지는 곳.
	 * 접미사 `.DLT` 는 스프링의 [org.springframework.kafka.listener.DeadLetterPublishingRecoverer] 기본 규칙이다.
	 */
	const val ORDER_CREATED_DLT = "$ORDER_CREATED.DLT"

	/**
	 * 파티션 수 = 한 컨슈머 그룹이 가질 수 있는 최대 병렬도.
	 * 파티션보다 컨슈머가 많으면 남는 컨슈머는 놀게 된다.
	 * 늘리는 건 되지만 줄이는 건 안 되므로 처음 잡을 때 여유를 둔다.
	 */
	const val ORDER_PARTITIONS = 3
}
