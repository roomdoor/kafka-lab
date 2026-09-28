package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.config.Topics
import com.roomdoor.kafkalab.order.Order
import com.roomdoor.kafkalab.order.OrderEvent
import com.roomdoor.kafkalab.order.OrderEventType
import com.roomdoor.kafkalab.order.OrderRepository
import com.roomdoor.kafkalab.order.OrderStatus
import com.roomdoor.kafkalab.order.OrderStatusProjector
import com.roomdoor.kafkalab.support.IntegrationTestBase
import org.apache.kafka.common.TopicPartition
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Duration
import java.time.Instant
import java.util.UUID
import javax.sql.DataSource
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 오프셋은 DB 커밋이 **끝난 뒤에** 커밋돼야 한다(#4).
 *
 * 순서가 거꾸로면 DB 커밋이 실패한 사이 재시작·리밸런스가 났을 때 새 소유자는 커밋된 오프셋 다음부터 읽는다.
 * 상태 갱신이 사라지고 DLT 에도 남지 않는다.
 *
 * 커밋을 실패시키는 대신 **멈춰 세운다**. 다른 연결로 주문 행을 FOR UPDATE 로 잡아두면
 * 프로젝터의 UPDATE(트랜잭션 커밋 때 flush 된다)가 락을 기다린다. 그 순간 = DB 커밋 직전이다.
 * 이때 오프셋이 이미 커밋돼 있으면 버그다. 실패 타이밍을 맞출 필요가 없어 결과가 흔들리지 않는다.
 */
class OrderStatusCommitOrderTest : IntegrationTestBase() {

	@Autowired
	private lateinit var orderRepository: OrderRepository

	@Autowired
	private lateinit var dataSource: DataSource

	@Autowired
	private lateinit var jdbcTemplate: JdbcTemplate

	@Test
	fun `DB 커밋 전에는 오프셋을 커밋하지 않는다`() {
		val order = orderRepository.save(
			Order(orderId = "order-${UUID.randomUUID()}", customerId = "customer-commit-order", amount = 10_000),
		)
		val event = OrderEvent(
			eventId = UUID.randomUUID().toString(),
			eventType = OrderEventType.PAYMENT_COMPLETED,
			orderId = order.orderId,
			customerId = order.customerId,
			amount = order.amount,
			occurredAt = Instant.now(),
			paymentId = "payment-commit-order",
		)

		val partition: TopicPartition
		val offset: Long

		dataSource.connection.use { lock ->
			lock.autoCommit = false
			val holderPid = lock.createStatement().use { st ->
				st.executeQuery("select pg_backend_pid()").use { it.next(); it.getInt(1) }
			}
			lock.prepareStatement("select 1 from orders where order_id = ? for update").use {
				it.setString(1, order.orderId)
				it.executeQuery()
			}

			val sent = kafkaTemplate.send(Topics.ORDER_EVENTS, order.orderId, jsonMapper.writeValueAsString(event))
				.get().recordMetadata
			partition = TopicPartition(sent.topic(), sent.partition())
			offset = sent.offset()

			await().atMost(Duration.ofSeconds(20)).until { updateBlockedBy(holderPid) }

			assertTrue(
				committedOffset(OrderStatusProjector.GROUP_ID, partition) <= offset,
				"DB 커밋이 끝나지 않았는데 오프셋이 이미 커밋됐다",
			)

			// 락을 풀면 프로젝터의 트랜잭션이 커밋된다.
			lock.rollback()
		}

		await().atMost(Duration.ofSeconds(20)).untilAsserted {
			assertEquals(OrderStatus.PAID, orderRepository.findByOrderId(order.orderId)?.status)
			assertTrue(committedOffset(OrderStatusProjector.GROUP_ID, partition) > offset, "DB 커밋 뒤에는 오프셋이 커밋돼야 한다")
		}
	}

	// 이 테스트의 락 때문에 막힌 UPDATE 만 센다. 다른 세션의 락 대기를 잘못 세면 버그가 있어도 통과할 수 있다.
	private fun updateBlockedBy(holderPid: Int): Boolean =
		jdbcTemplate.queryForObject(
			"select count(*) from pg_stat_activity where ? = any(pg_blocking_pids(pid)) and query ilike 'update orders%'",
			Long::class.java,
			holderPid,
		)!! > 0
}
