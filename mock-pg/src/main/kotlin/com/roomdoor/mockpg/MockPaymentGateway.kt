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
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
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
 * 그리고 Idempotency-Key 를 지원한다. 호출 측이 타임아웃 후 같은 키로 재시도하면 결제를 또 하지 않는다.
 * 처음 요청이 아직 처리 중이면 409 로 거절하고, 끝난 뒤에는 처음 결과를 그대로 돌려준다.
 * 실제 PG(스트라이프·토스 등)가 쓰는 방식이다.
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

/**
 * 멱등키별로 확정된 결과를 보관한다. 500 은 확정이 아니므로 저장하지 않는다.
 *
 * 결과가 나오기 전(지연 중)에도 키를 "처리 중" 으로 먼저 잡아둔다. 조회만 하고 결과를 나중에 저장하면
 * 지연 창 안에 들어온 재시도가 빈 자리를 보고 새 결제를 해버린다(#3).
 *
 * public 인 건 앱 테스트가 선점 주인 규칙을 직접 검증하려고서다(다른 Gradle 모듈이라 internal 은 안 보인다).
 */
class IdempotencyStore {
	/**
	 * data class 가 아니라 equals 가 동일성이다. 그래서 remove(key, 표시) 는 "내가 넣은 그 표시" 만 지운다.
	 * 값 비교였다면 500 으로 끝난 앞 요청이 뒤 재시도의 처리 중 표시를 지워, 그 틈에 온 요청이 또 결제한다.
	 */
	class Entry(val status: HttpStatusCode, val response: PaymentResponse)

	private val results = ConcurrentHashMap<String, Entry>()

	/** 요청마다 새로 만드는 처리 중 표시. 선점의 주인을 가린다. */
	fun newClaim() = Entry(
		HttpStatusCode.Conflict,
		PaymentResponse(status = "IN_PROGRESS", reason = "같은 Idempotency-Key 요청이 처리 중"),
	)

	/**
	 * 키를 원자적으로 선점한다. 이미 있으면 그 값(확정 결과 또는 남의 처리 중 표시)을 돌려주고, 선점에 성공하면 null.
	 * 처리 중 응답은 409 다. 스트라이프처럼 "같은 키가 아직 처리 중이니 나중에 다시" 라는 뜻이고,
	 * 호출 측은 2xx·402 가 아니면 재시도하므로 끝난 뒤의 재시도가 확정 결과를 받아간다.
	 */
	fun claim(key: String?, claim: Entry): Entry? = key?.let { results.putIfAbsent(it, claim) }

	fun remember(key: String?, status: HttpStatusCode, response: PaymentResponse) {
		key?.let { results[it] = Entry(status, response) }
	}

	/** 확정 결과 없이 끝났으면 자기 표시만 지운다. 이미 지웠거나 결과로 바뀌었으면 아무것도 안 한다. */
	fun release(key: String?, claim: Entry) {
		key?.let { results.remove(it, claim) }
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

				// 이미 확정된 결과가 있으면 결제를 다시 하지 않고 그대로 재생한다. 처리 중이면 409.
				val claim = store.newClaim()
				store.claim(idempotencyKey, claim)?.let { existing ->
					if (existing.status == HttpStatusCode.Conflict) {
						log.info("멱등키 처리 중: key=$idempotencyKey → 409 거절")
					} else {
						log.info("멱등키 재사용: key=$idempotencyKey → ${existing.response.status} 재생")
					}
					call.respond(existing.status, existing.response)
					return@post
				}

				// 확정 결과 없이 빠져나가는 모든 길(500, 예외, 취소)에서 선점을 푼다. 안 풀면 그 키는 영영 409 다.
				// 이미 풀었거나 결과를 저장한 뒤라면 release 는 아무것도 지우지 않는다.
				try {
					// 일시적 장애. 확정 결과가 아니므로 저장하지 않는다 — 재시도하면 다시 굴린다.
					if (Random.nextDouble() < config.failureRate) {
						// 응답보다 먼저 푼다. 응답 뒤에 풀면 곧바로 온 재시도가 아직 남은 처리 중 표시에 409 를 받는다.
						store.release(idempotencyKey, claim)
						log.warn("일시 장애 응답: orderId=${request.orderId}")
						call.respond(
							HttpStatusCode.InternalServerError,
							PaymentResponse(status = "ERROR", reason = "게이트웨이 일시 장애"),
						)
						return@post
					}

					// 응답 지연. 호출 측은 타임아웃으로 끊지만 여기서는 결제가 진행된다.
					// 호출 측이 끊어 엔진이 핸들러를 취소해도 결제는 끝까지 가야 하므로 결과 저장까지 취소 불가로 돈다.
					// 취소되면 finally 가 키를 풀어버려, 진행 중이던 결제를 재시도가 새 결제로 또 하게 된다.
					val (status, response) = withContext(NonCancellable) {
						if (Random.nextDouble() < config.timeoutRate) {
							log.warn("응답 지연 ${config.delayMillis}ms: orderId=${request.orderId}")
							delay(config.delayMillis)
						}

						val result = if (request.amount > config.declineAbove) {
							HttpStatusCode.PaymentRequired to
								PaymentResponse(status = "DECLINED", reason = "한도 초과: ${request.amount}")
						} else {
							HttpStatusCode.OK to
								PaymentResponse(status = "APPROVED", transactionId = "pg_${UUID.randomUUID()}")
						}
						store.remember(idempotencyKey, result.first, result.second)
						result
					}

					log.info("결제 응답: orderId=${request.orderId} amount=${request.amount} → ${response.status}")
					call.respond(status, response)
				} finally {
					store.release(idempotencyKey, claim)
				}
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
