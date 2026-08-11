package com.roomdoor.kafkalab.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.ExponentialBackOff

@Configuration
class KafkaConsumerConfig {

	private val log = LoggerFactory.getLogger(javaClass)

	@Bean
	fun consumerFactory(
		@Value("\${spring.kafka.bootstrap-servers}") bootstrapServers: String,
	): ConsumerFactory<String, String> {
		val props = mutableMapOf<String, Any>(
			ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,

			// 오프셋이 없는 새 컨슈머 그룹이 어디서부터 읽을지. earliest = 토픽 처음부터.
			// latest 로 두면 그룹을 새로 만든 시점 이전 메시지를 전부 건너뛴다.
			ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",

			// 자동 커밋을 끈다. 켜두면 "처리 전에 커밋" 이 일어나 장애 시 메시지가 조용히 사라진다.
			ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,

			// 한 번의 poll 로 가져올 최대 건수. 이 배치를 max.poll.interval.ms 안에 처리하지 못하면
			// 브로커가 죽은 컨슈머로 판단해 리밸런스를 일으킨다 — 느린 처리에서 가장 흔한 사고.
			ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 10,
			ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG to 300_000,
		)
		return DefaultKafkaConsumerFactory(props)
	}

	@Bean
	fun kafkaListenerContainerFactory(
		consumerFactory: ConsumerFactory<String, String>,
		kafkaTemplate: KafkaTemplate<String, String>,
	): ConcurrentKafkaListenerContainerFactory<String, String> {
		val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
		factory.setConsumerFactory(consumerFactory)

		// 컨슈머 스레드 수. 파티션 수보다 크게 잡아도 남는 스레드는 파티션을 못 받아 논다.
		factory.setConcurrency(Topics.ORDER_PARTITIONS)

		// 리스너가 ack.acknowledge() 를 호출해야 오프셋이 커밋된다. 처리 성공 후에만 커밋하기 위함.
		factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE

		factory.setCommonErrorHandler(deadLetterErrorHandler(kafkaTemplate))
		return factory
	}

	/**
	 * 실패 처리 정책: 지수 백오프로 3번 더 시도하고, 그래도 실패하면 DLT 로 보낸 뒤 오프셋을 커밋한다.
	 * 커밋까지 해야 뒤에 쌓인 정상 메시지가 막히지 않는다 — 이 처리가 없으면 poison pill 하나가 파티션 전체를 세운다.
	 */
	private fun deadLetterErrorHandler(kafkaTemplate: KafkaTemplate<String, String>): DefaultErrorHandler {
		val recoverer = DeadLetterPublishingRecoverer(kafkaTemplate) { record, exception ->
			log.error("DLT 로 보냄: topic=${record.topic()} partition=${record.partition()} offset=${record.offset()} 원인=${exception.message}")
			// 파티션을 -1 로 넘겨 브로커가 고르게 한다.
			// 기본 동작은 '원본과 같은 파티션 번호' 인데, DLT 는 파티션이 1개라 2번 파티션에서 온 메시지는 전송이 실패한다.
			TopicPartition(Topics.ORDER_CREATED_DLT, -1)
		}

		// 0.5초 → 1초 → 2초 로 벌어지며 최대 3번 재시도. 실패가 몰릴 때 브로커와 하위 시스템을 함께 두들기지 않으려는 것.
		val backOff = ExponentialBackOff().apply {
			initialInterval = 500
			multiplier = 2.0
			maxInterval = 5_000
			maxAttempts = 3
			// 여러 컨슈머가 동시에 실패했을 때 재시도 시각이 겹치지 않게 흩뿌린다.
			jitter = 100
		}

		return DefaultErrorHandler(recoverer, backOff).apply {
			// 재시도해도 결과가 같은 예외는 즉시 DLT 로 보낸다. 잘못된 데이터를 3번 더 씹어봐야 시간만 버린다.
			addNotRetryableExceptions(IllegalArgumentException::class.java)
		}
	}
}
