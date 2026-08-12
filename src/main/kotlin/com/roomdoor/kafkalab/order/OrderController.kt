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
			주문을 저장하고 order.events 토픽에 OrderCreated 를, notification.requested 에 알림 요청을 발행한다.
			이후 결제 컨슈머가 외부 PG(mock-pg:9090)를 호출하고 그 결과를 다시 이벤트로 발행한다.

			실습용 입력값:
			- amount 가 1,000,000 초과 → PG 가 402 로 거절한다. 재시도 없이 PaymentFailed 로 확정되고 주문 상태가 PAYMENT_FAILED 가 된다.
			- amount 가 0 이하 → 주문 자체가 거절된다. 주문도 이벤트도 남지 않는다(아웃박스 원자성).
			- `docker compose stop mock-pg` 로 PG 를 죽인 뒤 주문 → 재시도 3번 후 order.events.DLT 로 넘어간다.
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
