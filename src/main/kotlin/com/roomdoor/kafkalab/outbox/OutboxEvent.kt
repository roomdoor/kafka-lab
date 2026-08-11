package com.roomdoor.kafkalab.outbox

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant

/**
 * 아웃박스 한 건 = "아직 Kafka 로 못 보낸 이벤트".
 *
 * 업무 데이터와 같은 트랜잭션으로 이 행을 넣기 때문에, DB 가 커밋되면 이벤트도 반드시 남고
 * 롤백되면 이벤트도 같이 사라진다. 발행은 릴레이가 나중에 책임진다.
 */
@Entity
@Table(name = "outbox_events")
class OutboxEvent(

	/** 파티션 키로 쓴다. 같은 집합체(여기서는 주문)의 이벤트는 같은 파티션에 순서대로 쌓인다. */
	@Column(nullable = false)
	val aggregateId: String,

	@Column(nullable = false)
	val topic: String,

	@Column(nullable = false, columnDefinition = "text")
	val payload: String,

	@Column(nullable = false)
	val createdAt: Instant = Instant.now(),

	/** null 이면 아직 미발행. 릴레이가 발행에 성공하면 채운다. */
	@Column
	var publishedAt: Instant? = null,

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	val id: Long? = null,
)
