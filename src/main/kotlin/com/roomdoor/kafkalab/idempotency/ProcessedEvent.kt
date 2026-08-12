package com.roomdoor.kafkalab.idempotency

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import jakarta.persistence.UniqueConstraint
import java.time.Instant

/**
 * "이 컨슈머 그룹이 이 이벤트를 이미 처리했다" 는 기록.
 *
 * Kafka 는 at-least-once 라 같은 메시지가 두 번 온다. 결제처럼 두 번 실행되면 안 되는 작업은
 * 이 검사가 없으면 그대로 사고가 된다.
 *
 * 그룹을 키에 포함한 이유: 같은 이벤트를 결제 서비스도 받고 상태 갱신 서비스도 받는다.
 * eventId 만으로 막으면 먼저 처리한 쪽이 다른 쪽까지 막아버린다.
 *
 * 유니크 제약이 핵심이다. 애플리케이션에서 SELECT 로 확인하고 INSERT 하는 방식은
 * 동시에 두 스레드가 들어오면 둘 다 통과한다. DB 제약만이 그 경합을 막는다.
 */
@Entity
@Table(
	name = "processed_events",
	uniqueConstraints = [UniqueConstraint(columnNames = ["consumer_group", "event_id"])],
)
class ProcessedEvent(

	@Column(name = "consumer_group", nullable = false, length = 100)
	val consumerGroup: String,

	@Column(name = "event_id", nullable = false, length = 100)
	val eventId: String,

	@Column(nullable = false)
	val processedAt: Instant = Instant.now(),

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	val id: Long? = null,
)
