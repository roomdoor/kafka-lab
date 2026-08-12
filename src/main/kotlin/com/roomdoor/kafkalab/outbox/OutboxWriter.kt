package com.roomdoor.kafkalab.outbox

import org.springframework.stereotype.Component
import tools.jackson.databind.json.JsonMapper

/**
 * 아웃박스에 이벤트를 적는다. **반드시 업무 데이터와 같은 트랜잭션 안에서 호출해야** 의미가 있다.
 *
 * 발행은 [OutboxRelay] 가 나중에 맡는다. 여기서는 DB 에 쓰기만 한다.
 */
@Component
class OutboxWriter(
	private val outboxRepository: OutboxRepository,
	private val jsonMapper: JsonMapper,
) {

	/**
	 * @param partitionKey Kafka 파티션 키. 순서를 지켜야 하는 것끼리 같은 값을 줘야 한다.
	 *   주문 생명주기는 orderId, 알림은 customerId 를 쓴다.
	 */
	fun write(topic: String, partitionKey: String, payload: Any) {
		outboxRepository.save(
			OutboxEvent(
				aggregateId = partitionKey,
				topic = topic,
				payload = jsonMapper.writeValueAsString(payload),
			)
		)
	}
}
