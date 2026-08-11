package com.roomdoor.kafkalab.order

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/orders")
class OrderController(
	private val orderService: OrderService,
) {

	@PostMapping
	fun create(@RequestBody request: CreateOrderRequest): ResponseEntity<CreateOrderResponse> {
		val order = orderService.createOrder(request.customerId, request.amount)
		return ResponseEntity.status(HttpStatus.CREATED).body(CreateOrderResponse(order.orderId))
	}

	data class CreateOrderRequest(val customerId: String, val amount: Long)

	data class CreateOrderResponse(val orderId: String)
}
