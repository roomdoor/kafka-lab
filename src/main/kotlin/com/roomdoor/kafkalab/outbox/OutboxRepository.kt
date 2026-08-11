package com.roomdoor.kafkalab.outbox

import org.springframework.data.domain.Limit
import org.springframework.data.jpa.repository.JpaRepository

interface OutboxRepository : JpaRepository<OutboxEvent, Long> {

	/**
	 * 미발행 이벤트를 id 순으로 가져온다. id 순 = 저장 순서라, 같은 주문의 이벤트 순서가 뒤집히지 않는다.
	 *
	 * 릴레이 인스턴스를 여러 대 띄우면 같은 행을 동시에 집어 중복 발행이 난다.
	 * 그때는 여기에 비관적 락(`SELECT ... FOR UPDATE SKIP LOCKED`)을 걸어야 한다.
	 */
	fun findByPublishedAtIsNullOrderByIdAsc(limit: Limit): List<OutboxEvent>
}
