package com.roomdoor.kafkalab.payment

import org.springframework.data.jpa.repository.JpaRepository

interface PaymentRepository : JpaRepository<Payment, Long> {

	/**
	 * 한 주문에 행이 여러 개일 수 있다 — 거절은 여러 번 쌓인다. 단건 조회로 선언하면
	 * 두 건째부터 IncorrectResultSizeDataAccessException 이 난다. 가장 최근 결과를 본다.
	 */
	fun findTopByOrderIdOrderByIdDesc(orderId: String): Payment?

	fun countByOrderId(orderId: String): Long

	/** 이미 끝난 주문인지 보는 빠른 길. 처리 중(PENDING)인 경우는 예약 INSERT 가 걸러낸다. */
	fun countByOrderIdAndStatus(orderId: String, status: PaymentStatus): Long
}
