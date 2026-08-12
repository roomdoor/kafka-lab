package com.roomdoor.kafkalab.support

import com.roomdoor.mockpg.MockPgConfig
import com.roomdoor.mockpg.startMockPaymentGateway
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import tools.jackson.databind.json.JsonMapper
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID

/**
 * 실제 Kafka 브로커와 PostgreSQL 을 컨테이너로 띄우고, mock 결제 게이트웨이는 **같은 JVM 안에서** 임의 포트로 띄운다.
 *
 * mock 을 컨테이너로 올리면 테스트가 이미지 빌드에 묶인다. 클래스로 직접 띄우면 즉시 뜨고,
 * 시나리오마다 실패율을 바꿔가며 쓸 수 있다.
 */
@SpringBootTest
@Testcontainers
abstract class IntegrationTestBase {

	@Autowired
	protected lateinit var kafkaTemplate: KafkaTemplate<String, String>

	@Autowired
	protected lateinit var jsonMapper: JsonMapper

	@BeforeEach
	fun resetEnvironment() {
		// 앞선 테스트가 바꿔둔 실패율이 남아 다음 테스트를 오염시키지 않게 매번 되돌린다.
		configureMockPg(MockPgConfig())
		// 컨슈머가 파티션을 배정받기 전에 발행하면 테스트가 먼저 끝나버릴 수 있다.
		Thread.sleep(200)
	}

	/** mock 게이트웨이 동작을 런타임에 바꾼다. 실제 PG 에는 없는 실습용 엔드포인트다. */
	protected fun configureMockPg(config: MockPgConfig) {
		val body = """
			{"failureRate":${config.failureRate},"timeoutRate":${config.timeoutRate},
			 "delayMillis":${config.delayMillis},"declineAbove":${config.declineAbove}}
		""".trimIndent()

		val request = HttpRequest.newBuilder(URI.create("http://localhost:$mockPgPort/_config"))
			.header("Content-Type", "application/json")
			.POST(HttpRequest.BodyPublishers.ofString(body))
			.build()

		HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding())
	}

	/**
	 * 테스트 전용 컨슈머. 그룹을 매번 새로 만들어 다른 테스트의 오프셋에 영향을 주지 않는다.
	 *
	 * 테스트끼리 같은 토픽을 공유하므로 "총 N건" 이 아니라 "조건에 맞는 N건" 을 기다린다.
	 * 총 건수로 기다리면 남의 메시지로 정원이 차버려 정작 내 메시지를 못 본 채 끝난다.
	 */
	protected fun consumeMatching(
		topic: String,
		expectedCount: Int,
		timeout: Duration = Duration.ofSeconds(30),
		predicate: (ConsumerRecord<String, String>) -> Boolean,
	): List<ConsumerRecord<String, String>> {
		val props = mapOf<String, Any>(
			ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
			ConsumerConfig.GROUP_ID_CONFIG to "test-${UUID.randomUUID()}",
			ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
		)

		KafkaConsumer<String, String>(props).use { consumer ->
			consumer.subscribe(listOf(topic))
			val collected = mutableListOf<ConsumerRecord<String, String>>()
			val deadline = System.nanoTime() + timeout.toNanos()

			while (System.nanoTime() < deadline && collected.size < expectedCount) {
				consumer.poll(Duration.ofMillis(500)).forEach { if (predicate(it)) collected += it }
			}
			return collected
		}
	}

	companion object {

		@JvmStatic
		val kafka: KafkaContainer = KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"))

		@JvmStatic
		val postgres: PostgreSQLContainer = PostgreSQLContainer(DockerImageName.parse("postgres:17"))

		@JvmStatic
		val mockPgPort: Int = ServerSocket(0).use { it.localPort }

		init {
			kafka.start()
			postgres.start()
			// 기본값은 '항상 성공'. 실패 시나리오는 각 테스트가 /_config 로 바꿔 쓴다.
			startMockPaymentGateway(mockPgPort, MockPgConfig()).start(wait = false)
		}

		@JvmStatic
		@DynamicPropertySource
		fun properties(registry: DynamicPropertyRegistry) {
			registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
			registry.add("spring.datasource.url") { postgres.jdbcUrl }
			registry.add("spring.datasource.username") { postgres.username }
			registry.add("spring.datasource.password") { postgres.password }
			registry.add("spring.jpa.hibernate.ddl-auto") { "create-drop" }
			registry.add("app.payment-gateway.base-url") { "http://localhost:$mockPgPort" }
		}
	}
}
