package com.roomdoor.kafkalab

import com.roomdoor.mockpg.IdempotencyStore
import com.roomdoor.mockpg.MockPgConfig
import com.roomdoor.mockpg.startMockPaymentGateway
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame

/**
 * mock 게이트웨이 멱등키 동작만 본다. 컨테이너 없이 같은 JVM 에 게이트웨이만 띄운다.
 *
 * 지연 중인 요청과 같은 키의 재시도가 겹치는 창이 핵심이다(#3).
 * 이 창에서 재시도를 새 결제로 받아주면 한 키에 승인이 여러 건 생긴다.
 */
class MockPaymentGatewayIdempotencyTest {

	private val port = ServerSocket(0).use { it.localPort }
	private val http = HttpClient.newHttpClient()
	// /_config 는 설정 전체를 교체하므로 기본값 필드도 빠짐없이 보낸다.
	private val json = Json { encodeDefaults = true }
	private val server = startMockPaymentGateway(port, MockPgConfig()).also { it.start(wait = false) }

	@AfterEach
	fun stop() = server.stop(0, 0)

	@Test
	fun `지연 중에 같은 키로 재시도하면 409 를 받고, 끝난 뒤에는 처음 결과만 재생된다`() {
		// 첫 커넥션·직렬화 초기화가 아래 짧은 타임아웃을 잡아먹지 않게 먼저 한 번 데운다.
		assertEquals(200, pay(UUID.randomUUID().toString()).statusCode())
		configure(MockPgConfig(timeoutRate = 1.0, delayMillis = 2_000))
		val key = UUID.randomUUID().toString()

		// 호출 측 read timeout 을 흉내낸다. 호출 측은 끊지만 게이트웨이는 결제를 계속 진행한다.
		assertFailsWith<HttpTimeoutException> { pay(key, timeout = Duration.ofMillis(1_000)) }

		val retryInFlight = pay(key)
		assertEquals(409, retryInFlight.statusCode())

		val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos()
		var first = pay(key)
		while (first.statusCode() == 409 && System.nanoTime() < deadline) {
			Thread.sleep(100)
			first = pay(key)
		}
		val second = pay(key)
		assertEquals(200, first.statusCode())
		assertEquals(first.body(), second.body())
	}

	@Test
	fun `500 으로 끝난 키는 비워서 재시도가 다시 결제를 시도한다`() {
		configure(MockPgConfig(failureRate = 1.0))
		val key = UUID.randomUUID().toString()
		assertEquals(500, pay(key).statusCode())

		configure(MockPgConfig())
		assertEquals(200, pay(key).statusCode())
	}

	@Test
	fun `500 뒤 같은 키로 들어온 재시도의 선점은 앞 요청이 풀지 못한다`() {
		assertEquals(200, pay(UUID.randomUUID().toString()).statusCode())
		val key = UUID.randomUUID().toString()

		configure(MockPgConfig(failureRate = 1.0))
		assertEquals(500, pay(key).statusCode())

		// B 가 지연에 들어가 키를 잡고 있는 동안 C 가 온다. 앞의 500 요청이 B 의 선점을 지웠다면 C 가 새 결제를 한다.
		configure(MockPgConfig(timeoutRate = 1.0, delayMillis = 2_000))
		val b = http.sendAsync(payRequest(key), HttpResponse.BodyHandlers.ofString())
		Thread.sleep(300)
		// C 가 B 보다 먼저 선점하면 역할만 바뀐다. 어느 쪽이든 승인 하나, 409 하나여야 한다.
		val responses = listOf(pay(key), b.get(10, TimeUnit.SECONDS))

		assertEquals(listOf(200, 409), responses.map { it.statusCode() }.sorted())
		val approved = responses.single { it.statusCode() == 200 }.body()
		assertEquals(approved, pay(key).body())
	}

	@Test
	fun `앞 요청이 늦게 한 번 더 풀어도 뒤 요청의 선점은 남는다`() {
		// 위 HTTP 테스트로는 "앞 요청의 finally 가 뒤 요청의 선점 뒤에 도는" 틈을 밖에서 맞출 수 없어 저장소를 직접 본다.
		val store = IdempotencyStore()
		val key = "k"
		val claimA = store.newClaim()
		assertNull(store.claim(key, claimA))
		store.release(key, claimA)

		val claimB = store.newClaim()
		assertNull(store.claim(key, claimB))
		store.release(key, claimA)

		assertSame(claimB, store.claim(key, store.newClaim()))
	}

	private fun pay(key: String, timeout: Duration = Duration.ofSeconds(10)): HttpResponse<String> =
		http.send(payRequest(key, timeout), HttpResponse.BodyHandlers.ofString())

	private fun payRequest(key: String, timeout: Duration = Duration.ofSeconds(10)): HttpRequest =
		HttpRequest.newBuilder(URI.create("http://localhost:$port/payments"))
			.header("Content-Type", "application/json")
			.header("Idempotency-Key", key)
			.timeout(timeout)
			.POST(HttpRequest.BodyPublishers.ofString("""{"orderId":"o-1","amount":1000}"""))
			.build()

	private fun configure(config: MockPgConfig) {
		val request = HttpRequest.newBuilder(URI.create("http://localhost:$port/_config"))
			.header("Content-Type", "application/json")
			.POST(HttpRequest.BodyPublishers.ofString(json.encodeToString(MockPgConfig.serializer(), config)))
			.build()
		http.send(request, HttpResponse.BodyHandlers.discarding())
	}
}
