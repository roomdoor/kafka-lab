package com.roomdoor.kafkalab.config

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory

/**
 * 페이로드는 JSON 문자열 그대로 보낸다.
 *
 * spring-kafka 의 JacksonJsonSerializer 를 쓰면 편하지만, 메시지 헤더에 producer 쪽 클래스명(`__TypeId__`)이
 * 박혀서 컨슈머가 같은 패키지 구조를 갖도록 강제된다. 서비스가 분리되면 그 결합이 그대로 장애 지점이 된다.
 * 아웃박스 테이블에도 JSON 문자열로 저장되므로, 문자열로 통일하는 편이 저장·전송 경로가 하나로 유지된다.
 */
@Configuration
class KafkaProducerConfig {

	@Bean
	fun producerFactory(
		@Value("\${spring.kafka.bootstrap-servers}") bootstrapServers: String,
	): ProducerFactory<String, String> {
		val props = mutableMapOf<String, Any>(
			ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
			ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
			ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,

			// acks=all: 리더뿐 아니라 ISR(동기화된 복제본) 전부가 받았을 때 성공으로 친다.
			// acks=1 이면 리더가 죽는 순간 방금 성공 응답한 메시지가 사라질 수 있다.
			ProducerConfig.ACKS_CONFIG to "all",

			// 멱등 프로듀서. 네트워크 오류로 내부 재시도가 일어나도 브로커가 시퀀스 번호로 중복을 걸러낸다.
			// 이게 없으면 "재시도 = 중복 발행" 이 된다.
			ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,

			// 멱등성이 켜져 있으면 5 이하에서 순서가 보장된다. 끄면 재시도 시 순서가 뒤집힐 수 있다.
			ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to 5,

			// 전송 실패를 얼마나 오래 견딜지. 이 시간을 넘기면 예외로 올라온다.
			ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to 120_000,

			// 같은 파티션으로 갈 메시지를 5ms 모아 한 번에 보낸다. 처리량이 오르는 대신 지연이 그만큼 는다.
			ProducerConfig.LINGER_MS_CONFIG to 5,
			ProducerConfig.COMPRESSION_TYPE_CONFIG to "lz4",
		)
		return DefaultKafkaProducerFactory(props)
	}

	@Bean
	fun kafkaTemplate(producerFactory: ProducerFactory<String, String>): KafkaTemplate<String, String> =
		KafkaTemplate(producerFactory)
}
