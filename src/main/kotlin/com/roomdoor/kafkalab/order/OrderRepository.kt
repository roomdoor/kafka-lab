package com.roomdoor.kafkalab.order

import org.springframework.data.jpa.repository.JpaRepository

interface OrderRepository : JpaRepository<Order, Long> {
	fun findByOrderId(orderId: String): Order?
}
