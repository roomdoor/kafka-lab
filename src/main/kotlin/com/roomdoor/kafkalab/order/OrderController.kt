package com.roomdoor.kafkalab.order

import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@Tag(name = "주문", description = "주문 생성. 저장과 동시에 아웃박스에 이벤트가 기록되고 릴레이가 Kafka 로 발행한다.")
@RestController
@RequestMapping("/api/orders")
class OrderController(
	private val orderService: OrderService,
) {

	@Operation(
		summary = "주문 생성",
		description = """
			주문을 저장하고 order.created 이벤트를 발행한다.

			실습용 입력값:
			- customerId 를 `FAIL` 로 주면 결제 컨슈머가 매번 실패한다. 3번 재시도 후 order.created.DLT 로 넘어간다.
			- amount 를 0 이하로 주면 주문 자체가 거절된다. 주문도 이벤트도 남지 않는다(아웃박스 원자성).
		""",
	)
	@PostMapping
	fun create(@RequestBody request: CreateOrderRequest): ResponseEntity<CreateOrderResponse> {
		val order = orderService.createOrder(request.customerId, request.amount)
		return ResponseEntity.status(HttpStatus.CREATED).body(CreateOrderResponse(order.orderId))
	}

	data class CreateOrderRequest(
		@field:Schema(description = "고객 ID. `FAIL` 을 주면 결제 컨슈머가 실패하도록 만들어져 있다.", example = "c-1")
		val customerId: String,

		@field:Schema(description = "주문 금액. 0 이하는 거절된다.", example = "25000")
		val amount: Long,
	)

	data class CreateOrderResponse(
		@field:Schema(description = "생성된 주문 ID. Kafka 파티션 키로도 쓰인다.")
		val orderId: String,
	)
}
