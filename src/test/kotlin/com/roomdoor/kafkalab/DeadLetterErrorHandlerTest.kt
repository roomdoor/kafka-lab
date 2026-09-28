package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.KafkaConsumerConfig
import com.roomdoor.kafkalab.config.Topics
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.hibernate.exception.GenericJDBCException
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.dao.InvalidDataAccessResourceUsageException
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.ListenerExecutionFailedException
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.orm.jpa.JpaSystemException
import java.sql.SQLException
import java.sql.SQLTransientException
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

	@Test
	fun `타입이 뭉개져도 SQLState 가 DB 재시작이면 재시도한다`() {
		// DB 재시작 시 PSQLException(57P01) → Hibernate GenericJDBCException → JpaSystemException 으로 온다.
		val restart = JpaSystemException(GenericJDBCException("terminating connection", SQLException("terminating connection", "57P01")))
		assertFalse(handle(restart, offset = 2))
	}

	@Test
	fun `같은 래핑이라도 SQLState 가 문법 오류면 건너뛴다`() {
		val grammar = JpaSystemException(GenericJDBCException("no such column", SQLException("no such column", "42703")))
		assertTrue(handle(grammar, offset = 3))
	}

	@Test
	fun `페일오버 중 읽기 전용 트랜잭션 오류는 재시도한다`() {
		val readOnly = JpaSystemException(GenericJDBCException("read-only transaction", SQLException("read-only transaction", "25006")))
		assertFalse(handle(readOnly, offset = 4))
	}

	@Test
	fun `SQLTransientException 은 SQLState 가 없어도 재시도한다`() {
		val timeout = JpaSystemException(GenericJDBCException("timeout", SQLTransientException("timeout")))
		assertFalse(handle(timeout, offset = 5))
	}

	@Test
	fun `일시 장애 뒤 같은 예외 클래스의 영구 오류가 이어져도 결국 건너뛴다`() {
		// 재시도 정책은 첫 실패(57P01) 때 정해지고, 예외 클래스가 같으면 42703 이 와도 다시 정해지지 않는다.
		// 무한 재시도였다면 여기서 영원히 false 다.
		val restart = JpaSystemException(GenericJDBCException("terminating connection", SQLException("terminating connection", "57P01")))
		val grammar = JpaSystemException(GenericJDBCException("no such column", SQLException("no such column", "42703")))
		assertFalse(handle(restart, offset = 7))

		// 한도는 벽시계가 아니라 백오프 간격의 합(10분)으로 잰다. mock 컨테이너는 멈춘 상태라 대기가 바로 끝난다.
		// 1+2+4+8+16 초 뒤로 30초씩이면 25번 안쪽에서 한도에 닿는다.
		val skipped = (1..100).any { handle(grammar, offset = 7) }
		assertTrue(skipped, "재시도 한도가 끝나면 건너뛰어야 한다")
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
