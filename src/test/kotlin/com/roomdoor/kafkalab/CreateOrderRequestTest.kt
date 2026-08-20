package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.order.OrderController.CreateOrderRequest
import org.junit.jupiter.api.Test
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 임의값 채우기는 경계와 역직렬화가 전부다. 상한이 하나 어긋나면 PG 한도를 넘겨 주문이 가끔 거절되고,
 * `@JsonSetter(nulls = Nulls.SKIP)` 이 빠지면 명시적 null 이 그대로 들어와 주문 생성이 터진다.
 * 스프링 없이 확인한다.
 */
class CreateOrderRequestTest {

	private val mapper = JsonMapper.builder().addModule(KotlinModule.Builder().build()).build()

	@Test
	fun `값을 주면 그대로 쓴다`() {
		val request = mapper.readValue("""{"customerId":"c-1","amount":25000}""", CreateOrderRequest::class.java)

		assertEquals("c-1", request.customerId)
		assertEquals(25_000, request.amount)
	}

	@Test
	fun `키가 없어도 null 이어도 기본값으로 채운다`() {
		// Kotlin 기본값만으로는 두 번째가 null 로 들어온다. Nulls.SKIP 이 그걸 막는다.
		for (json in listOf("{}", """{"customerId":null,"amount":null}""")) {
			val request = mapper.readValue(json, CreateOrderRequest::class.java)

			assertTrue(request.customerId.startsWith("c-"), "입력=$json 실제=${request.customerId}")
			assertTrue(request.amount >= CreateOrderRequest.MIN_AMOUNT, "입력=$json 실제=${request.amount}")
		}
	}

	@Test
	fun `채워지는 값은 정해진 범위 안이다`() {
		val requests = List(2_000) { CreateOrderRequest() }

		val numbers = requests.map { it.customerId.removePrefix("c-").toInt() }
		assertTrue(numbers.all { it in 0..CreateOrderRequest.CUSTOMER_COUNT }, "고객 번호가 0~100 을 벗어났다")
		assertTrue(numbers.contains(0) && numbers.contains(CreateOrderRequest.CUSTOMER_COUNT), "양 끝값도 나와야 한다")

		val amounts = requests.map { it.amount }
		assertTrue(
			amounts.all { it in CreateOrderRequest.MIN_AMOUNT..CreateOrderRequest.MAX_AMOUNT },
			"금액이 10,000~1,000,000 을 벗어났다. 최소=${amounts.min()} 최대=${amounts.max()}",
		)
	}
}
