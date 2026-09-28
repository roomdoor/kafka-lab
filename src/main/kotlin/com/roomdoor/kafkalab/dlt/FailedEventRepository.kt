package com.roomdoor.kafkalab.dlt

import org.springframework.data.jpa.repository.JpaRepository

interface FailedEventRepository : JpaRepository<FailedEvent, Long> {
	fun findByStatusOrderByIdDesc(status: FailedEventStatus): List<FailedEvent>
	fun countByStatus(status: FailedEventStatus): Long
}
