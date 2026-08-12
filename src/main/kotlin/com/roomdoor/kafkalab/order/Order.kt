package com.roomdoor.kafkalab.order

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant

enum class OrderStatus {
	CREATED,
	PAID,
	PAYMENT_FAILED,
}

// order 는 SQL 예약어(ORDER BY)라 테이블명은 orders 로 둔다.
@Entity
@Table(name = "orders")
class Order(

	@Column(nullable = false, unique = true)
	val orderId: String,

	@Column(nullable = false)
	val customerId: String,

	@Column(nullable = false)
	val amount: Long,

	/**
	 * 이 필드는 주문을 만든 쪽이 아니라 [OrderStatusProjector] 가 이벤트를 받아 갱신한다.
	 * 결제 서비스가 주문 테이블을 직접 UPDATE 하지 않는다는 뜻이다 — 서비스 간 결합을 이벤트로만 유지한다.
	 *
	 * 저장은 이름(STRING)으로 한다. ORDINAL 로 두면 enum 순서를 바꾸는 순간 기존 데이터의 의미가 뒤바뀐다.
	 */
	@Enumerated(EnumType.STRING)
	@Column(nullable = false, length = 32)
	var status: OrderStatus = OrderStatus.CREATED,

	@Column(nullable = false)
	val createdAt: Instant = Instant.now(),

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	val id: Long? = null,
)
