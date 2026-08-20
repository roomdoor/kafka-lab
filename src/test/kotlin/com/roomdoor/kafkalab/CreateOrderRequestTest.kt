package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.order.OrderController.CreateOrderRequest
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 임의값 채우기는 경계가 전부다. 상한이 하나 어긋나면 PG 한도를 넘겨 주문이 가끔 거절되고,
 * 고객 번호 상한이 어긋나면 c-100 이 영영 안 나온다. 스프링 없이 확인한다.
 */
class CreateOrderRequestTest {

	@Test
	fun `값을 주면 그대로 쓴다`() {
		val request = CreateOrderRequest(customerId = "c-1", amount = 25_000)

		assertEquals("c-1", request.customerIdOrRandom())
		assertEquals(25_000, request.amountOrRandom())
	}

	@Test
	fun `null 이면 정해진 범위 안에서 채운다`() {
		val request = CreateOrderRequest()

		val customerIds = List(2_000) { request.customerIdOrRandom() }
		val amounts = List(2_000) { request.amountOrRandom() }

		val numbers = customerIds.map {
			assertTrue(it.startsWith("c-"), "고객 ID 는 c- 로 시작해야 한다. 실제=$it")
			it.removePrefix("c-").toInt()
		}
		assertTrue(numbers.all { it in 0..CreateOrderRequest.CUSTOMER_COUNT }, "고객 번호가 0~100 을 벗어났다")
		assertTrue(numbers.contains(0) && numbers.contains(CreateOrderRequest.CUSTOMER_COUNT), "양 끝값도 나와야 한다")

		assertTrue(
			amounts.all { it in CreateOrderRequest.MIN_AMOUNT..CreateOrderRequest.MAX_AMOUNT },
			"금액이 10,000~1,000,000 을 벗어났다. 최소=${amounts.min()} 최대=${amounts.max()}",
		)
	}
}
