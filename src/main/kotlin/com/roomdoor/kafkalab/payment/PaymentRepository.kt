package com.roomdoor.kafkalab.payment

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import java.time.Instant

interface PaymentRepository : JpaRepository<Payment, Long> {

	/**
	 * 한 주문에 행이 여러 개일 수 있다 — 거절은 여러 번 쌓인다. 단건 조회로 선언하면
	 * 두 건째부터 IncorrectResultSizeDataAccessException 이 난다. 가장 최근 결과를 본다.
	 */
	fun findTopByOrderIdOrderByIdDesc(orderId: String): Payment?

	fun countByOrderId(orderId: String): Long

	/** 이미 끝난 주문인지 보는 빠른 길. 처리 중(PENDING)인 경우는 예약 INSERT 가 걸러낸다. */
	fun countByOrderIdAndStatus(orderId: String, status: PaymentStatus): Long

	/** 예약에 막혔을 때 누가 잡았는지 본다. PENDING 은 인덱스 때문에 주문당 하나뿐이라 단건이다. */
	fun findByOrderIdAndStatus(orderId: String, status: PaymentStatus): Payment?

	/**
	 * PENDING 인 예약만 결과로 확정한다. 이미 다른 배달이 확정했으면 0 을 돌려준다.
	 * "읽고 검사하고 저장" 으로 나누면 두 배달이 나란히 PENDING 을 읽는다. WHERE 절에 조건을 넣어야 DB 가 한 번만 허락한다.
	 */
	@Modifying
	@Query(
		"""
		update Payment p set p.status = :status, p.pgTransactionId = :transactionId, p.failureReason = :failureReason
		where p.id = :id and p.status = com.roomdoor.kafkalab.payment.PaymentStatus.PENDING
		"""
	)
	fun finalizePending(id: Long, status: PaymentStatus, transactionId: String?, failureReason: String?): Int

	/**
	 * 아무도 이어받지 않은 PENDING 예약만 지운다. 같은 eventId 의 다른 배달이 먼저 확정한 결과도,
	 * 지금 이어받아 PG 를 부르는 중인 예약도 지우지 않으려는 것이다.
	 */
	@Modifying
	@Query(
		"""
		delete from Payment p
		where p.id = :id and p.status = com.roomdoor.kafkalab.payment.PaymentStatus.PENDING and p.takenOverAt is null
		"""
	)
	fun deletePending(id: Long): Int

	/**
	 * 이어받기 표시. PENDING 일 때만 된다. 0 이면 그 사이 예약이 풀렸거나 확정된 것이라 이어받으면 안 된다.
	 * 표시와 해제([deletePending])가 같은 행 락으로 줄을 서므로 둘 중 하나만 이긴다.
	 */
	@Modifying
	@Query(
		"""
		update Payment p set p.takenOverAt = :now
		where p.id = :id and p.status = com.roomdoor.kafkalab.payment.PaymentStatus.PENDING
		"""
	)
	fun markTakenOver(id: Long, now: Instant): Int
}
