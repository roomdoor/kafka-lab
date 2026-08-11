package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.order.OrderEvent
import com.roomdoor.kafkalab.support.IntegrationTestBase
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.support.KafkaHeaders
import tools.jackson.databind.json.JsonMapper
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * 처리에 실패한 메시지가 파티션을 막지 않고 DLT 로 빠지는지 확인한다.
 *
 * 이 안전장치가 없으면 잘못된 메시지 한 건이 무한 재시도를 돌면서
 * 같은 파티션 뒤에 쌓인 정상 메시지까지 전부 멈춘다(poison pill).
 */
class DeadLetterTest : IntegrationTestBase() {

	@Autowired
	private lateinit var jsonMapper: JsonMapper

	@Test
	fun `재시도를 모두 소진한 메시지는 DLT 로 이동한다`() {
		val orderId = "fail-${UUID.randomUUID()}"
		val event = OrderEvent(
			eventId = UUID.randomUUID().toString(),
			orderId = orderId,
			// PaymentConsumer 가 이 값을 보면 매번 예외를 던진다.
			customerId = "FAIL",
			amount = 10_000,
			occurredAt = Instant.now(),
		)

		kafkaTemplate.send(Topics.ORDER_CREATED, orderId, jsonMapper.writeValueAsString(event)).get()

		val dltRecord = consumeMatching(Topics.ORDER_CREATED_DLT, expectedCount = 1) {
			it.value().contains(orderId)
		}.firstOrNull()

		assertNotNull(dltRecord, "재시도 소진 후 DLT 에 도착해야 한다")

		val originalTopic = dltRecord.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)
			?.value()?.toString(Charsets.UTF_8)
		assertEquals(Topics.ORDER_CREATED, originalTopic, "원본 토픽이 헤더에 남아야 재처리가 가능하다")

		val exceptionMessage = dltRecord.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)
			?.value()?.toString(Charsets.UTF_8)
		assertTrue(
			exceptionMessage?.contains("결제 게이트웨이") == true,
			"실패 원인이 헤더에 남아야 한다. 실제 값=$exceptionMessage",
		)
	}

	@Test
	fun `재시도해도 소용없는 예외는 즉시 DLT 로 간다`() {
		val orderId = "invalid-${UUID.randomUUID()}"
		// amount 가 0 이면 PaymentConsumer 가 IllegalArgumentException 을 던지고,
		// 에러 핸들러의 not-retryable 목록에 있으므로 백오프 없이 바로 넘어간다.
		val payload = """
			{"eventId":"${UUID.randomUUID()}","orderId":"$orderId","customerId":"c1","amount":0,"occurredAt":"${Instant.now()}"}
		""".trimIndent()

		kafkaTemplate.send(Topics.ORDER_CREATED, orderId, payload).get()

		val dltRecord = consumeMatching(Topics.ORDER_CREATED_DLT, expectedCount = 1) {
			it.value().contains(orderId)
		}.firstOrNull()

		assertNotNull(dltRecord, "재시도 없이 DLT 로 이동해야 한다")
	}
}
