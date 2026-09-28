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
		assertEquals(1, consumeWhenSaveThrows("uk_failed_events_original_record"))
	}

	@Test
	fun `다른 무결성 위반은 삼키지 않고 던진다`() {
		assertFailsWith<DataIntegrityViolationException> { consumeWhenSaveThrows("failed_events_status_check") }
	}

	/** save 가 [constraintName] 위반으로 실패하게 만들고 consume 을 부른다. ack 횟수를 돌려준다. */
	private fun consumeWhenSaveThrows(constraintName: String): Int {
		val repository = Mockito.mock(FailedEventRepository::class.java)
		val cause = ConstraintViolationException("violation", SQLException("violation"), constraintName)
		Mockito.doThrow(DataIntegrityViolationException("violation", cause)).`when`(repository).save(Mockito.any())

		var acks = 0
		DeadLetterConsumer(repository).consume(ConsumerRecord("order.events.DLT", 0, 0L, "key", "{}"), Acknowledgment { acks++ })
		return acks
	}
}
