package com.roomdoor.kafkalab.support

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
import java.time.Duration
import java.util.UUID

/**
 * 실제 Kafka 브로커와 PostgreSQL 을 컨테이너로 띄우고 테스트한다.
 *
 * EmbeddedKafka 로도 되지만, 리스너 설정·리밸런스·DLT 처럼 이 프로젝트에서 확인하려는 동작은
 * 진짜 브로커에서 검증해야 의미가 있다. 컨테이너는 클래스 간에 재사용되어 한 번만 뜬다.
 */
@SpringBootTest
@Testcontainers
abstract class IntegrationTestBase {

	@Autowired
	protected lateinit var kafkaTemplate: KafkaTemplate<String, String>

	@BeforeEach
	fun waitForListeners() {
		// 컨슈머가 파티션을 배정받기 전에 발행하면 auto.offset.reset=earliest 라도
		// 테스트가 먼저 끝나버릴 수 있다. 컨테이너 기동 직후 한 번만 여유를 준다.
		Thread.sleep(200)
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

		init {
			kafka.start()
			postgres.start()
		}

		@JvmStatic
		@DynamicPropertySource
		fun properties(registry: DynamicPropertyRegistry) {
			registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
			registry.add("spring.datasource.url") { postgres.jdbcUrl }
			registry.add("spring.datasource.username") { postgres.username }
			registry.add("spring.datasource.password") { postgres.password }
			registry.add("spring.jpa.hibernate.ddl-auto") { "create-drop" }
		}
	}
}
