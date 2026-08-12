package com.roomdoor.mockpg

import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.install
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import kotlinx.serialization.Serializable
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.random.Random

/**
 * 결제 게이트웨이 mock. 진짜 PG 사가 그렇듯 간헐적으로 실패한다.
 *
 * 여기서 재현하려는 건 세 가지다.
 * 1. 일시적 장애(500) — 재시도하면 성공할 수 있다
 * 2. 응답 지연 — 호출 측 타임아웃을 유발한다. 결제가 됐는지 안 됐는지 알 수 없는 최악의 상태
 * 3. 결제 거절(402) — 재시도해도 결과가 같다. 실패로 확정하고 넘어가야 한다
 *
 * 그리고 Idempotency-Key 를 지원한다. 호출 측이 타임아웃 후 같은 키로 재시도하면
 * 결제를 또 하는 대신 처음 결과를 그대로 돌려준다. 실제 PG(스트라이프·토스 등)가 쓰는 방식이다.
 */

@Serializable
data class PaymentRequest(val orderId: String, val amount: Long)

@Serializable
data class PaymentResponse(
	val status: String,
	val transactionId: String? = null,
	val reason: String? = null,
)

@Serializable
data class MockPgConfig(
	/** 500 을 돌려줄 확률. 0.0 이면 항상 정상 */
	val failureRate: Double = 0.0,
	/** 응답을 지연시킬 확률 */
	val timeoutRate: Double = 0.0,
	/** 지연 시 대기 시간(ms). 호출 측 read timeout 보다 길어야 의미가 있다 */
	val delayMillis: Long = 8_000,
	/** 이 금액을 넘으면 한도 초과로 거절한다 */
	val declineAbove: Long = 1_000_000,
)

/** 멱등키별로 확정된 결과를 보관한다. 500 은 확정이 아니므로 저장하지 않는다. */
private class IdempotencyStore {
	private val results = ConcurrentHashMap<String, Pair<HttpStatusCode, PaymentResponse>>()

	fun find(key: String?): Pair<HttpStatusCode, PaymentResponse>? = key?.let { results[it] }

	fun remember(key: String?, status: HttpStatusCode, response: PaymentResponse) {
		key?.let { results[it] = status to response }
	}
}

fun startMockPaymentGateway(
	port: Int,
	initialConfig: MockPgConfig = MockPgConfig(),
): EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration> {
	val store = IdempotencyStore()
	val log = org.slf4j.LoggerFactory.getLogger("MockPaymentGateway")

	// 재기동 없이 동작을 바꿀 수 있게 둔다. 테스트가 시나리오마다 실패율을 갈아끼운다.
	val configRef = java.util.concurrent.atomic.AtomicReference(initialConfig)

	return embeddedServer(Netty, port = port) {
		install(ContentNegotiation) { json() }

		routing {
			get("/health") { call.respond(mapOf("status" to "UP")) }

			/** 실습·테스트용. 실제 PG 에는 당연히 이런 게 없다. */
			post("/_config") {
				val updated = call.receive<MockPgConfig>()
				configRef.set(updated)
				log.info("설정 변경: $updated")
				call.respond(updated)
			}

			post("/payments") {
				val config = configRef.get()
				val idempotencyKey = call.request.headers["Idempotency-Key"]
				val request = call.receive<PaymentRequest>()

				// 이미 확정된 결과가 있으면 결제를 다시 하지 않고 그대로 재생한다.
				store.find(idempotencyKey)?.let { (status, response) ->
					log.info("멱등키 재사용: key=$idempotencyKey → ${response.status} 재생")
					call.respond(status, response)
					return@post
				}

				// 일시적 장애. 확정 결과가 아니므로 저장하지 않는다 — 재시도하면 다시 굴린다.
				if (Random.nextDouble() < config.failureRate) {
					log.warn("일시 장애 응답: orderId=${request.orderId}")
					call.respond(
						HttpStatusCode.InternalServerError,
						PaymentResponse(status = "ERROR", reason = "게이트웨이 일시 장애"),
					)
					return@post
				}

				// 응답 지연. 호출 측은 타임아웃으로 끊지만 여기서는 결제가 진행된다.
				if (Random.nextDouble() < config.timeoutRate) {
					log.warn("응답 지연 ${config.delayMillis}ms: orderId=${request.orderId}")
					kotlinx.coroutines.delay(config.delayMillis)
				}

				val (status, response) = if (request.amount > config.declineAbove) {
					HttpStatusCode.PaymentRequired to
						PaymentResponse(status = "DECLINED", reason = "한도 초과: ${request.amount}")
				} else {
					HttpStatusCode.OK to
						PaymentResponse(status = "APPROVED", transactionId = "pg_${UUID.randomUUID()}")
				}

				store.remember(idempotencyKey, status, response)
				log.info("결제 응답: orderId=${request.orderId} amount=${request.amount} → ${response.status}")
				call.respond(status, response)
			}
		}
	}
}

fun main() {
	val port = System.getenv("MOCK_PG_PORT")?.toInt() ?: 9090
	val config = MockPgConfig(
		failureRate = System.getenv("MOCK_PG_FAILURE_RATE")?.toDouble() ?: 0.3,
		timeoutRate = System.getenv("MOCK_PG_TIMEOUT_RATE")?.toDouble() ?: 0.1,
		delayMillis = System.getenv("MOCK_PG_DELAY_MS")?.toLong() ?: 8_000,
		declineAbove = System.getenv("MOCK_PG_DECLINE_ABOVE")?.toLong() ?: 1_000_000,
	)

	println("mock payment gateway 시작: port=$port config=$config")
	startMockPaymentGateway(port, config).start(wait = true)
}
