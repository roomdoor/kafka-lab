package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.KafkaConsumerConfig
import com.roomdoor.kafkalab.config.Topics
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.dao.InvalidDataAccessResourceUsageException
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.ListenerExecutionFailedException
import org.springframework.kafka.listener.MessageListenerContainer
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * DLT 리스너 에러 핸들러의 분류(#6). 영구 DB 오류까지 무한 재시도하면 DLT 가 다시 멈춘다.
 *
 * DB 를 실제로 영구 오류 상태로 만들기는 어려워 핸들러에 예외를 직접 넣는다.
 * handleOne 이 true 면 복구(건너뜀), false 면 재시도다.
 */
class DeadLetterErrorHandlerTest {

	private val handler = KafkaConsumerConfig()
		.deadLetterListenerContainerFactory(mock(ConsumerFactory::class.java) as ConsumerFactory<String, String>)
		.createContainer(Topics.ORDER_EVENTS_DLT)
		.commonErrorHandler!!

	private val consumer = mock(Consumer::class.java)
	private val container = mock(MessageListenerContainer::class.java)

	@Test
	fun `영구 DB 오류는 재시도하지 않고 건너뛴다`() {
		assertTrue(handle(InvalidDataAccessResourceUsageException("column does not exist"), offset = 0))
	}

	@Test
	fun `일시적 DB 장애는 재시도한다`() {
		assertFalse(handle(DataAccessResourceFailureException("connection refused"), offset = 1))
	}

	// 스프링은 리스너 예외를 ListenerExecutionFailedException 으로 감싸 넘긴다. 실제와 같게 감싼다.
	private fun handle(cause: Exception, offset: Long): Boolean =
		handler.handleOne(
			ListenerExecutionFailedException("listener failed", cause),
			ConsumerRecord(Topics.ORDER_EVENTS_DLT, 0, offset, "key", "value"),
			consumer,
			container,
		)
}
