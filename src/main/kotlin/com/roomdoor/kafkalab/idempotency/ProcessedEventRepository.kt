package com.roomdoor.kafkalab.idempotency

import org.springframework.data.jpa.repository.JpaRepository

interface ProcessedEventRepository : JpaRepository<ProcessedEvent, Long> {
	fun existsByConsumerGroupAndEventId(consumerGroup: String, eventId: String): Boolean
	fun countByConsumerGroup(consumerGroup: String): Long
}
