package com.roomdoor.kafkalab.dlt

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.hibernate.exception.ConstraintViolationException
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.kafka.support.Acknowledgment
import java.sql.SQLException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** 중복으로 보고 삼켜도 되는 건 우리 유니크 제약 위반뿐이다. 다른 무결성 위반까지 삼키면 실패 기록이 소리 없이 사라진다. */
class DeadLetterConsumerDuplicateTest {

	@Test
	fun `유니크 제약 위반은 이미 기록된 실패로 보고 ack 한다`() {
		assertEquals(1, consumeWhenSaveThrows(sqlState = "23505"))
	}

	@Test
	fun `다른 무결성 위반은 삼키지 않고 던진다`() {
		// 23514 = check_violation
		assertFailsWith<DataIntegrityViolationException> { consumeWhenSaveThrows(sqlState = "23514") }
	}

	/**
	 * save 가 [sqlState] 위반으로 실패하게 만들고 consume 을 부른다. ack 횟수를 돌려준다.
	 * 제약 이름은 null 로 둔다. lc_messages 가 영어가 아니면 Hibernate 가 이름을 못 뽑아 실제로 이렇게 온다.
	 */
	private fun consumeWhenSaveThrows(sqlState: String): Int {
		val repository = Mockito.mock(FailedEventRepository::class.java)
		val cause = ConstraintViolationException("violation", SQLException("violation", sqlState), null)
		Mockito.doThrow(DataIntegrityViolationException("violation", cause)).`when`(repository).save(Mockito.any())

		var acks = 0
		DeadLetterConsumer(repository).consume(ConsumerRecord("order.events.DLT", 0, 0L, "key", "{}"), Acknowledgment { acks++ })
		return acks
	}
}
