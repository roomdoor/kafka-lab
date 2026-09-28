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
	/** PG 호출 직전에 잡아두는 예약. 이 상태가 있다는 건 누군가 이 주문을 결제하는 중이라는 뜻이다. */
	PENDING,
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
 * **처리 중이거나 성공한** 결제를 주문당 하나로 제한한다 (`schema.sql`). 거절은 여러 건 쌓일 수 있다.
 *
 * PENDING 이 인덱스에 포함되는 게 핵심이다. PG 를 호출하기 전에 이 행을 먼저 INSERT 하므로,
 * 같은 주문이 동시에 두 번 들어와도 진 쪽은 PG 를 아예 부르지 못한다.
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

	/**
	 * 이 결제를 시도한 이벤트 ID. PG 멱등키와 같은 값이라 "이 예약 = 이 PG 결제 시도" 로 묶인다.
	 * 재배달이 예약에 막혔을 때 자기 예약인지(이어받기) 남의 예약인지(물러나기) 이걸로 가른다.
	 * 이 컬럼이 생기기 전 행은 null 이라 누구의 예약도 아닌 것으로 본다.
	 */
	@Column
	val eventId: String? = null,

	/**
	 * 같은 eventId 의 재배달이 이 예약을 이어받은 시각. 한 번이라도 이어받았으면 예약을 잡은 첫 배달이 풀지 못한다.
	 * 첫 배달의 실패가 확실해도 이어받은 쪽이 지금 PG 를 부르고 있을 수 있기 때문이다.
	 */
	@Column
	var takenOverAt: Instant? = null,

	@Enumerated(EnumType.STRING)
	@Column(nullable = false, length = 32)
	var status: PaymentStatus,

	@Column
	var pgTransactionId: String? = null,

	@Column
	var failureReason: String? = null,

	@Column(nullable = false)
	val createdAt: Instant = Instant.now(),

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	val id: Long? = null,
) {

	// 없으면 호출 로그에 Payment@4ffaf635 로 찍혀 아무것도 알 수 없다 (CallLoggingAspect).
	override fun toString() = "Payment(orderId=$orderId, amount=$amount, status=$status)"
}
