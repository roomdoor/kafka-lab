package com.roomdoor.kafkalab.payment

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant

enum class PaymentStatus {
	COMPLETED,
	FAILED,
}

/**
 * 결제 결과. 예전에는 메모리에만 있었지만, 재시작하면 사라지는 기록으로는
 * "이 주문 결제됐나?" 라는 질문에 답할 수 없다.
 *
 * [pgTransactionId] 는 나중에 PG 사와 대사(reconciliation)할 때 쓰는 유일한 연결고리다.
 * 이게 없으면 우리 DB 와 PG 기록을 맞춰볼 방법이 없다.
 *
 * 이 테이블이 중복 결제를 막는 멱등 장치이기도 하다. `order_id` 에 걸린 부분 유니크 인덱스가
 * 성공한 결제를 주문당 하나로 제한한다 (`schema.sql`). 거절은 여러 건 쌓일 수 있다.
 */
@Entity
@Table(name = "payments")
class Payment(

	@Column(nullable = false, unique = true)
	val paymentId: String,

	@Column(nullable = false)
	val orderId: String,

	@Column(nullable = false)
	val amount: Long,

	@Enumerated(EnumType.STRING)
	@Column(nullable = false, length = 32)
	val status: PaymentStatus,

	@Column
	val pgTransactionId: String? = null,

	@Column
	val failureReason: String? = null,

	@Column(nullable = false)
	val createdAt: Instant = Instant.now(),

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	val id: Long? = null,
)
