package com.roomdoor.kafkalab

import com.roomdoor.kafkalab.support.IntegrationTestBase
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.io.ClassPathResource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator
import javax.sql.DataSource
import kotlin.test.assertEquals

/**
 * 기존 DB 의 옛 3컬럼 유니크 제약은 `ddl-auto: update` 가 `UK<해시>` 이름으로 붙인 것이라,
 * 테스트(create-drop)에서 Postgres 가 붙이는 이름과 다르다. schema.sql 이 이름이 아니라 컬럼 구성으로 찾아 지우는지 본다.
 */
class FailedEventUniqueKeySchemaTest : IntegrationTestBase() {

	@Autowired
	private lateinit var jdbc: JdbcTemplate

	@Autowired
	private lateinit var dataSource: DataSource

	@Test
	fun `update 모드가 만든 이름의 옛 3컬럼 유니크도 schema_sql 이 지우고 4컬럼 제약으로 바꾼다`() {
		try {
			// 다른 테스트가 좌표는 같고 그룹만 다른 행을 남겼을 수 있다. 그러면 3컬럼 유니크를 만들 수 없어 비운다.
			jdbc.execute("delete from failed_events")
			jdbc.execute("alter table failed_events drop constraint uk_failed_events_original_record")
			jdbc.execute("alter table failed_events add constraint uk_test_update_mode unique (original_topic, original_partition, original_offset)")

			ResourceDatabasePopulator(ClassPathResource("schema.sql")).execute(dataSource)

			val uniques = jdbc.queryForList(
				"select conname, pg_get_constraintdef(oid) as def from pg_constraint where conrelid = 'failed_events'::regclass and contype = 'u'",
			).associate { it["conname"] as String to it["def"] as String }
			assertEquals(
				mapOf("uk_failed_events_original_record" to "UNIQUE NULLS NOT DISTINCT (original_topic, original_partition, original_offset, original_consumer_group)"),
				uniques,
			)
		} finally {
			jdbc.execute("alter table failed_events drop constraint if exists uk_test_update_mode")
			ResourceDatabasePopulator(ClassPathResource("schema.sql")).execute(dataSource)
		}
	}
}
