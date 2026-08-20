package com.roomdoor.kafkalab.payment

import org.springframework.data.jpa.repository.JpaRepository

interface PaymentRepository : JpaRepository<Payment, Long> {

	/**
	 * 한 주문에 행이 여러 개일 수 있다 — 거절은 여러 번 쌓인다. 단건 조회로 선언하면
	 * 두 건째부터 IncorrectResultSizeDataAccessException 이 난다. 가장 최근 결과를 본다.
	 */
	fun findTopByOrderIdOrderByIdDesc(orderId: String): Payment?

	fun countByOrderId(orderId: String): Long

	/** 멱등 검사용. 성공한 결제만 센다 — `uk_payments_completed_order` 와 같은 기준이어야 한다. */
	fun countByOrderIdAndStatus(orderId: String, status: PaymentStatus): Long
}
