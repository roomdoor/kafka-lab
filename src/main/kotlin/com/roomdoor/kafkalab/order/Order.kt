package com.roomdoor.kafkalab.order

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant

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

	@Column(nullable = false)
	val createdAt: Instant = Instant.now(),

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	val id: Long? = null,
)
