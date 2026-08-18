package com.roomdoor.kafkalab.dlt

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import jakarta.persistence.UniqueConstraint
import java.time.Instant

enum class FailedEventStatus {
	/** 아직 아무도 손대지 않음. 사람이 봐야 하는 상태 */
	PENDING,

	/** 원본 토픽에 다시 넣었음. 결과는 아직 모름 */
	RETRIED,

	/** 재처리에 성공했거나 다른 방법으로 해결됨 */
	RESOLVED,

	/** 재처리할 값어치가 없다고 판단해 덮음 (테스트 데이터, 이미 취소된 주문 등) */
	IGNORED,
}

/**
 * DLT 로 떨어진 메시지의 **조사·추적용 기록**.
 *
 * DLT 토픽과 역할이 다르다.
 * - DLT 토픽 = 재발행에 쓸 원본 데이터. Kafka 라 소비해도 지워지지 않지만 보존 기간이 지나면 사라지고,
 *   조회·검색·집계가 안 되며, "이미 처리했는지" 를 표시할 방법이 없다.
 * - 이 테이블 = 그 위에 얹는 인덱스. SQL 로 집계하고, 상태를 표시하고, 보존 기간과 무관하게 남는다.
 *
 * 둘 다 있어야 한다. 이 테이블만 있으면 재발행할 원본이 없고, DLT 만 있으면 관리가 안 된다.
 */
@Entity
@Table(
	name = "failed_events",
	// 같은 (원본 토픽, 파티션, 오프셋) 은 세상에 하나뿐이다. DLT 를 다시 읽어도 행이 중복되지 않게 막는다.
	uniqueConstraints = [
		UniqueConstraint(columnNames = ["original_topic", "original_partition", "original_offset"]),
	],
)
class FailedEvent(

	@Column(name = "original_topic", nullable = false)
	val originalTopic: String,

	@Column(name = "original_partition", nullable = false)
	val originalPartition: Int,

	/** 재발행할 때 이 위치를 찾아가면 원본 메시지가 있다. */
	@Column(name = "original_offset", nullable = false)
	val originalOffset: Long,

	/** 어느 컨슈머 그룹이 실패했는지. 같은 토픽을 여러 그룹이 읽으므로 이게 없으면 범인을 못 찾는다. */
	@Column
	val originalConsumerGroup: String? = null,

	/** 파티션 키. 재발행할 때 이 값을 같이 넣어야 원래 파티션으로 돌아간다. */
	@Column
	val messageKey: String? = null,

	@Column(nullable = false, columnDefinition = "text")
	val payload: String,

	@Column
	val exceptionClass: String? = null,

	/** 스택트레이스는 담지 않는다. 필요하면 DLT 토픽 헤더에 그대로 있다. */
	@Column(columnDefinition = "text")
	val exceptionMessage: String? = null,

	@Column(nullable = false)
	val failedAt: Instant = Instant.now(),

	@Enumerated(EnumType.STRING)
	@Column(nullable = false, length = 32)
	var status: FailedEventStatus = FailedEventStatus.PENDING,

	@Column(nullable = false)
	var retryCount: Int = 0,

	@Column
	var lastRetriedAt: Instant? = null,

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	val id: Long? = null,
)
