package com.roomdoor.kafkalab.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

/**
 * 브로커의 auto.create.topics.enable 을 꺼둔 상태이므로 토픽은 여기서 명시적으로 만든다.
 * KafkaAdmin 이 애플리케이션 기동 시 없는 토픽만 생성한다(이미 있으면 건드리지 않는다).
 * 주의: 파티션 수를 줄이는 변경은 반영되지 않는다 — Kafka 자체가 파티션 축소를 지원하지 않는다.
 */
@Configuration
class KafkaTopicConfig {

	@Bean
	fun orderCreatedTopic(): NewTopic =
		TopicBuilder.name(Topics.ORDER_CREATED)
			.partitions(Topics.ORDER_PARTITIONS)
			.replicas(1)
			.build()

	/**
	 * DLT 는 병렬 처리보다 "빠짐없이 모아두는 것" 이 목적이라 파티션 1개로 충분하다.
	 * 실무에서는 보존 기간을 본 토픽보다 길게 잡는 경우가 많다(조사할 시간이 필요하므로).
	 */
	@Bean
	fun orderCreatedDltTopic(): NewTopic =
		TopicBuilder.name(Topics.ORDER_CREATED_DLT)
			.partitions(1)
			.replicas(1)
			.build()
}
