package com.roomdoor.kafkalab.payment

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatusCode
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.stereotype.Component
import org.springframework.web.client.RestClient
import org.springframework.web.client.ResourceAccessException
import java.time.Duration

/** 재시도하면 결과가 달라질 수 있는 실패. 이 예외만 Kafka 재시도로 넘긴다. */
class PaymentGatewayException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/** 게이트웨이가 내린 **확정** 결과. 승인이든 거절이든 다시 물어봐야 소용없다. */
sealed interface PaymentGatewayResult {
	data class Approved(val transactionId: String) : PaymentGatewayResult
	data class Declined(val reason: String) : PaymentGatewayResult
}

private data class GatewayRequest(val orderId: String, val amount: Long)

private data class GatewayResponse(
	val status: String? = null,
	val transactionId: String? = null,
	val reason: String? = null,
)

/**
 * 외부 결제 게이트웨이 호출.
 *
 * 이 클래스의 핵심은 요청을 보내는 게 아니라 **실패를 두 갈래로 나누는 것**이다.
 *
 * - 5xx, 타임아웃, 커넥션 거부 → [PaymentGatewayException]. 잠시 후 다시 하면 될 수도 있다
 * - 402 거절 → [PaymentGatewayResult.Declined]. **예외가 아니다.** 100번 더 물어봐도 거절이다
 *
 * 이 구분을 안 하면 둘 중 하나가 반드시 잘못된다. 거절을 예외로 던지면 재시도 3번을 낭비하고 DLT 를 오염시키고,
 * 일시 장애를 정상 결과로 취급하면 멀쩡한 주문이 결제 실패로 확정된다.
 */
@Component
class PaymentGatewayClient(
	@param:Value("\${app.payment-gateway.base-url}") baseUrl: String,
	@param:Value("\${app.payment-gateway.connect-timeout-ms}") connectTimeoutMs: Long,
	@param:Value("\${app.payment-gateway.read-timeout-ms}") readTimeoutMs: Long,
) {

	private val log = LoggerFactory.getLogger(javaClass)

	private val restClient: RestClient = RestClient.builder()
		.baseUrl(baseUrl)
		.requestFactory(
			SimpleClientHttpRequestFactory().apply {
				// 타임아웃을 안 걸면 응답 없는 서버 하나가 컨슈머 스레드를 영원히 붙잡는다.
				// 그러면 max.poll.interval.ms 를 넘겨 리밸런싱까지 끌려간다.
				setConnectTimeout(Duration.ofMillis(connectTimeoutMs))
				setReadTimeout(Duration.ofMillis(readTimeoutMs))
			}
		)
		.build()

	/**
	 * @param idempotencyKey 이벤트 ID 를 그대로 쓴다. 타임아웃 후 재시도할 때 게이트웨이가
	 *   "아까 그 요청" 임을 알아보고 결제를 또 하지 않고 처음 결과를 돌려준다.
	 *   이 헤더가 없으면 타임아웃 한 번에 이중 결제가 난다.
	 */
	fun requestPayment(idempotencyKey: String, orderId: String, amount: Long): PaymentGatewayResult {
		val response = try {
			restClient.post()
				.uri("/payments")
				.header("Idempotency-Key", idempotencyKey)
				.body(GatewayRequest(orderId, amount))
				.retrieve()
				// 기본 동작은 4xx/5xx 에서 예외를 던지는 것이다. 상태 코드별로 직접 분류하려고 꺼둔다.
				.onStatus({ true }) { _, _ -> }
				.toEntity(GatewayResponse::class.java)
		} catch (e: ResourceAccessException) {
			// 커넥션 거부, 읽기 타임아웃 등. 결제가 됐는지 안 됐는지 알 수 없는 상태다.
			throw PaymentGatewayException("게이트웨이 통신 실패: orderId=$orderId", e)
		}

		val status: HttpStatusCode = response.statusCode
		val body = response.body

		return when {
			status.is2xxSuccessful && body?.transactionId != null -> {
				log.debug("결제 승인: orderId=$orderId transactionId=${body.transactionId}")
				PaymentGatewayResult.Approved(body.transactionId)
			}

			status.value() == 402 ->
				PaymentGatewayResult.Declined(body?.reason ?: "결제 거절")

			else ->
				throw PaymentGatewayException("게이트웨이 오류 응답: status=$status body=$body")
		}
	}
}
