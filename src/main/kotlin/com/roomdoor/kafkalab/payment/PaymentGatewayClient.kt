package com.roomdoor.kafkalab.payment

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatusCode
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.stereotype.Component
import org.springframework.web.client.RestClient
import org.springframework.web.client.RestClientException
import org.springframework.web.client.toEntity
import java.net.ConnectException
import java.time.Duration

/**
 * 재시도하면 결과가 달라질 수 있는 PG 실패. 거절처럼 다시 물어도 같은 결과는 이 예외가 아니라 [PaymentGatewayResult] 로 돌려준다.
 * (Kafka 에러 핸들러는 IllegalArgumentException 말고는 다 재시도한다. 결제 컨슈머의 IllegalStateException 경로도 재시도에 기댄다.)
 *
 * [outcomeUnknown] 이 true 면 PG 가 결제했는지 모른다. 호출자는 예약을 풀면 안 된다 —
 * 풀면 같은 주문이 다른 멱등키로 다시 결제될 수 있다. 기본값이 true 인 건 모를 때는 모른다고 보는 게 안전해서다.
 */
class PaymentGatewayException(
	message: String,
	cause: Throwable? = null,
	val outcomeUnknown: Boolean = true,
) : RuntimeException(message, cause)

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
	 *   "아까 그 요청" 임을 알아보고 결제를 또 하지 않는다. 처음 요청이 아직 처리 중이면 409 를 받고
	 *   (예외 → 재시도), 끝난 뒤에는 처음 결과를 돌려받는다. 이 헤더가 없으면 타임아웃 한 번에 이중 결제가 난다.
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
				.toEntity<GatewayResponse>()
		} catch (e: RestClientException) {
			// ResourceAccessException 만 잡으면 안 된다. RestClient 는 상태·헤더를 본문 읽을 때 늦게 읽어서,
			// 읽기 타임아웃이 ResourceAccessException 이 아닌 RestClientException 으로 감싸져 나온다.
			//
			// 커넥션 거부만 "결제 안 됨" 이 확실하다. 요청이 PG 에 닿지도 않았다. 이 클래스에서 확정 실패는 이것 하나다.
			// 읽기 타임아웃·응답 도중 끊김은 PG 가 처리했는지 알 수 없다. 연결 타임아웃도 구분이 어려워 모름으로 둔다.
			throw PaymentGatewayException(
				"게이트웨이 통신 실패: orderId=$orderId",
				e,
				outcomeUnknown = e.rootCause !is ConnectException,
			)
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

			// 응답이 와도 결제 여부는 모른다(outcomeUnknown 기본값). 5xx 도 마찬가지다 — 앞단 프록시의 502/504 는
			// PG 가 처리한 뒤에도 나고, 본문이 JSON 이 아니면 위에서 읽기 실패로 빠져 어차피 구분이 흔들린다.
			// 예약을 쥔 채 재시도하면 같은 키로 다시 묻게 되고, PG 가 결과를 저장하지 않은 실패(mock-pg 의 500)라면
			// 그 재시도가 새 시도로 처리된다. 풀어서 얻는 건 없고 잃을 수 있는 건 이중 결제다.
			else ->
				throw PaymentGatewayException("게이트웨이 오류 응답: status=$status body=$body")
		}
	}
}
