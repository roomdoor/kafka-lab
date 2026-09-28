package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.dlt.FailedEvent
import com.roomdoor.kafkalab.dlt.FailedEventRepository
import com.roomdoor.kafkalab.dlt.FailedEventStatus
import com.roomdoor.kafkalab.order.Order
import com.roomdoor.kafkalab.order.OrderRepository
import com.roomdoor.kafkalab.order.OrderStatus
import com.roomdoor.kafkalab.payment.Payment
import com.roomdoor.kafkalab.payment.PaymentRepository
import com.roomdoor.kafkalab.payment.PaymentStatus
import com.roomdoor.kafkalab.support.IntegrationTestBase
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.io.ClassPathResource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator
import java.util.UUID
import javax.sql.DataSource

/**
 * `ddl-auto: update` 는 이미 있는 CHECK 제약을 갱신하지 않는다. enum 에 값을 추가하면 기존 DB 에서만 거부된다.
 * 테스트 DB 는 매번 새로 만들어지므로, 옛 enum 으로 만든 제약이 남은 DB 를 직접 흉내 낸 뒤
 * `schema.sql` 을 다시 돌려 모든 enum 값이 들어가는지 본다.
 * `schema.sql` 이 그 테이블을 다시 세우지 않거나, 하드코딩한 목록에 값이 빠지면 여기서 걸린다.
 */
class SchemaEnumCheckTest : IntegrationTestBase() {

	@Autowired
	private lateinit var jdbc: JdbcTemplate

	@Autowired
	private lateinit var dataSource: DataSource

	@Autowired
	private lateinit var orderRepository: OrderRepository

	@Autowired
	private lateinit var paymentRepository: PaymentRepository

	@Autowired
	private lateinit var failedEventRepository: FailedEventRepository

	@Test
	fun `orders 는 모든 OrderStatus 를 받는다`() {
		val id = orderRepository.save(Order(UUID.randomUUID().toString(), "customer-schema", 1_000)).id!!
		assertAllStatusesAccepted("orders", id, OrderStatus.entries.map { it.name })
	}

	@Test
	fun `payments 는 모든 PaymentStatus 를 받는다`() {
		val payment = Payment(UUID.randomUUID().toString(), UUID.randomUUID().toString(), 1_000, PaymentStatus.entries.first())
		assertAllStatusesAccepted("payments", paymentRepository.save(payment).id!!, PaymentStatus.entries.map { it.name })
	}

	@Test
	fun `failed_events 는 모든 FailedEventStatus 를 받는다`() {
		val failed = FailedEvent(
			originalTopic = "schema-test",
			originalPartition = 0,
			originalOffset = System.nanoTime(),
			payload = "{}",
			status = FailedEventStatus.entries.first(),
		)
		assertAllStatusesAccepted("failed_events", failedEventRepository.save(failed).id!!, FailedEventStatus.entries.map { it.name })
	}

	/** 행은 첫 값으로 넣어 두고, 첫 값만 허용하는 "옛 제약" 을 만든 뒤 schema.sql 로 복구되는지 본다. */
	private fun assertAllStatusesAccepted(table: String, id: Long, values: List<String>) {
		val constraint = "${table}_status_check"
		// 공유 DB 라 옛 제약이 남으면 다른 테스트 리스너가 깨진다. schema.sql 이 복구를 못 해도 원래 정의로 되돌린다.
		val original = jdbc.queryForObject(
			"select pg_get_constraintdef(oid) from pg_constraint where conname = ?", String::class.java, constraint,
		)
		try {
			// 옛 제약을 걸고 곧바로 schema.sql 을 돌려 그 사이 창을 줄인다.
			// NOT VALID: 다른 테스트가 남긴 행은 검사하지 않고, 이후 INSERT·UPDATE 에만 적용한다.
			jdbc.execute("alter table $table drop constraint $constraint")
			jdbc.execute("alter table $table add constraint $constraint check (status in ('${values.first()}')) not valid")
			ResourceDatabasePopulator(ClassPathResource("schema.sql")).execute(dataSource)

			values.forEach { jdbc.update("update $table set status = ? where id = ?", it, id) }
		} finally {
			jdbc.execute("alter table $table drop constraint if exists $constraint")
			jdbc.execute("alter table $table add constraint $constraint $original")
		}
	}
}
