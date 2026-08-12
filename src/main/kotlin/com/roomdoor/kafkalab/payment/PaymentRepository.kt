package com.roomdoor.kafkalab.payment

import org.springframework.data.jpa.repository.JpaRepository

interface PaymentRepository : JpaRepository<Payment, Long> {
	fun findByOrderId(orderId: String): Payment?
	fun countByOrderId(orderId: String): Long
}
